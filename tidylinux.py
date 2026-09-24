#!/usr/bin/env python3
'''tidy — Linux upgrade + cleanup TUI, the Linux sibling of tidymac.

Runs everything in parallel with per-task progress bars.

  --dry-run        size everything up, delete nothing, run no commands
  --headless       one line per task instead of the TUI (for systemd/cron)
  --no-sudo        skip every task that needs root instead of asking once
  --no-report      skip the JSON run report
  --install-agent  write a weekly systemd user timer and exit
'''
from __future__ import annotations
import argparse, asyncio, contextlib, json, os, re, shutil, signal, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence, TextIO
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, ScrollableContainer, Vertical
from textual.theme import Theme
from textual.widgets import Footer, Header, Label, ProgressBar, RichLog

HOME = Path.home()
UID = os.getuid()
HOME_STR = str(HOME)

# Run modes, set from the command line before any task starts. Every deletion
# and every child process checks DRY_RUN, so a dry run is inert by construction
# rather than by each task remembering to ask.
DRY_RUN = False
WRITE_REPORT = True
REPORT_DIR = HOME / '.local/state/tidy'
KEEP_REPORTS = 20

# Bounded pool so ~40 filesystem walkers don't thrash the disk or starve
# asyncio's default executor (which subprocess plumbing also uses).
_IO_POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix='fsclean')

# Directories never worth descending into during whole-home sweeps: either
# enormous (package caches, VCS metadata) or handled by a dedicated task.
_WALK_SKIP = frozenset({
    '.Trash', '.bun', '.bundle', '.cache', '.cargo', '.git', '.gradle', '.hg',
    '.m2', '.mypy_cache', '.npm', '.nvm', '.pub-cache', '.pyenv', '.pytest_cache',
    '.rustup', '.rvm', '.svn', '.tox', '.venv', '.local', '.var', '.steam',
    '.config', '.mozilla', 'snap', 'Pods', '__pycache__', 'node_modules', 'venv',
})

# Regenerable build/test droppings swept out of the whole home tree. Every one
# of these is rebuilt on the next run of the tool that made it.
_CRUFT_DIRS = frozenset({
    '.ipynb_checkpoints', '.mypy_cache', '.pytest_cache', '.ruff_cache',
    '__pycache__',
})

LOG_TIMEOUT = 600.0   # filesystem tasks
CMD_TIMEOUT = 1800.0  # ordinary tool upgrades
SLOW_TIMEOUT = 3600.0 # apt / snap / brew

# Non-interactive, low-noise environment for every child process.
_CHILD_ENV = {
    **os.environ,
    'NONINTERACTIVE': '1',
    'HOMEBREW_NO_AUTO_UPDATE': '1',
    'HOMEBREW_NO_ENV_HINTS': '1',
    'HOMEBREW_NO_INSTALL_CLEANUP': '1',
    'PIP_DISABLE_PIP_VERSION_CHECK': '1',
    'PYTHONUNBUFFERED': '1',
    'GIT_TERMINAL_PROMPT': '0',
    'DEBIAN_FRONTEND': 'noninteractive',
    'NEEDRESTART_MODE': 'a',  # Ubuntu's needrestart otherwise waits on a dialog
    'TERM': 'dumb',
}

# Whether root-only tasks (apt, snap, journal) can run. Settled once in main():
# either we are root, sudo needs no password, or the user authenticated up
# front. Children can never prompt — their stdin is /dev/null.
SUDO_OK = os.geteuid() == 0


def _root(cmd: Sequence[str]) -> list[str]:
    '''Prefix cmd with a non-interactive sudo unless we already are root.'''
    return list(cmd) if os.geteuid() == 0 else ['sudo', '-n', *cmd]


def _disk_free(path: str = '/') -> int:
    '''Free bytes on the volume holding path.

    The per-file byte tally can't see hardlinks, reflinks or btrfs/zfs
    compression, so the delta across a whole run is the honest number — even
    if other processes nudge it while we work.
    '''
    try:
        st = os.statvfs(path)
        return st.f_bavail * st.f_frsize
    except OSError:
        return 0


def _size_str(n: int) -> str:
    for unit, div in (('GB', 1073741824), ('MB', 1048576), ('KB', 1024)):
        if abs(n) >= div:
            return f'{n / div:.1f} {unit}'
    return f'{n} B'


# ── Result protocol ──────────────────────────────────────────────────────────
@dataclass(slots=True)
class Result:
    '''What a task accomplished. Replaces the old tuple/int/None/sentinel mix.'''
    freed: int = 0
    files: int = 0
    pkgs: int = 0
    skipped: bool = False
    failed: bool = False
    note: str = ''
    # Which command failed, and the tail of what it printed. Without these a
    # failed task says only that something failed, and the output that would
    # explain it is gone: the TUI log is capped and dies with the app.
    failed_cmd: str = ''
    failed_tail: list[str] = field(default_factory=list)

    def label(self) -> str:
        if self.skipped:
            return ICON_SKIP
        parts = []
        if self.pkgs:
            parts.append(f'{self.pkgs:,} upgraded')
        if self.freed:
            parts.append(f'freed {_size_str(self.freed)}')
        if self.files:
            parts.append(f'{self.files:,} files')
        if self.note:
            parts.append(self.note)
        return ' · '.join(parts) if parts else 'done'


SKIPPED = Result(skipped=True)


class _Count:
    '''Mutable counter threaded through a streaming output matcher.'''
    __slots__ = ('n',)

    def __init__(self) -> None:
        self.n = 0


Matcher = Callable[[str, _Count], None]


# ── Subprocess plumbing ──────────────────────────────────────────────────────
_LINE_SPLIT = re.compile(rb'[\r\n]')
_MAX_LINE = 16384


async def _run_cmd(cmd: Sequence[str], log: Callable[[str], None]) -> int:
    '''Run cmd, streaming its output to log. Returns the exit code.

    stdin is /dev/null so a child can never steal keystrokes from the TUI or
    block forever on a prompt, and the child gets its own session so a
    cancellation (timeout, quit) can kill the whole process tree, not just the
    immediate child.
    '''
    if DRY_RUN:
        log(f'[dry-run] would run: {" ".join(cmd)}')
        return 0
    # sudo caches credentials per terminal session, so a sudo child must stay
    # in ours to reuse the ticket from main(). It still gets its own process
    # group, which is all the teardown below needs.
    detach = ({'process_group': 0} if cmd[0] == 'sudo'
              else {'start_new_session': True})
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=_CHILD_ENV,
            **detach,
        )
    except (FileNotFoundError, NotADirectoryError):
        log(f'command not found: {cmd[0]}')
        return 127
    except (PermissionError, OSError) as exc:
        log(f'could not run {cmd[0]}: {exc}')
        return 126

    assert proc.stdout
    try:
        # Chunked reads rather than readline(): a tool emitting a huge line, or
        # a \r-based progress bar with no newline at all, would otherwise
        # overrun StreamReader's 64 KiB limit and raise.
        buf = b''
        while chunk := await proc.stdout.read(65536):
            buf += chunk
            pieces = _LINE_SPLIT.split(buf)
            buf = pieces.pop()
            for raw in pieces:
                if line := raw.decode(errors='replace').rstrip():
                    log(line)
            if len(buf) > _MAX_LINE:
                log(buf.decode(errors='replace').rstrip())
                buf = b''
        if buf and (line := buf.decode(errors='replace').rstrip()):
            log(line)
        return await proc.wait()
    finally:
        if proc.returncode is None:
            # Cancelled or timed out: tear down the child's whole process group.
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=5)
            if proc.returncode is None:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            with contextlib.suppress(Exception):
                await proc.wait()


FAIL_TAIL_LINES = 20  # of a failing command's output, kept for the report


@dataclass(slots=True)
class _Series:
    '''How a run of commands went: how many failed, and detail on the first.'''
    failures: int = 0
    cmd: str = ''
    tail: list[str] = field(default_factory=list)


def _fail_note(series: _Series) -> str:
    '''Name the command that failed. A bare count sends you log-digging.'''
    if not series.failures:
        return ''
    more = f' (+{series.failures - 1} more)' if series.failures > 1 else ''
    # The note lands in a one-line TUI label; the untruncated command line and
    # its output are in the run report.
    cmd = series.cmd if len(series.cmd) <= 60 else series.cmd[:59] + '\u2026'
    return f'failed: {cmd}{more}'


async def _run_series(
    cmds: Iterable[Sequence[str]],
    log: Callable[[str], None],
    counted: Callable[[str], None] | None = None,
    count_only: frozenset[int] | None = None,
) -> _Series:
    '''Run commands in order, reporting how many exited non-zero.

    The first failure keeps its command line and the tail of its output, so a
    run report can say what broke instead of just how many things did.

    count_only restricts package counting to the given command indices, so
    diagnostics like `brew doctor` can't inflate the upgrade tally.
    '''
    outcome = _Series()
    for i, cmd in enumerate(cmds):
        log(f'$ {" ".join(cmd)}')
        sink = counted if counted and (count_only is None or i in count_only) else log
        tail: list[str] = []

        # Tee every line to its usual sink and keep a rolling tail, which is
        # only read if this command turns out to have failed.
        def keep(msg: str) -> None:
            sink(msg)
            tail.append(msg)
            if len(tail) > FAIL_TAIL_LINES:
                del tail[0]

        if await _run_cmd(cmd, keep) != 0:
            outcome.failures += 1
            if not outcome.cmd:
                outcome.cmd = ' '.join(cmd)
                outcome.tail = list(tail)
    return outcome


# ── Filesystem helpers ───────────────────────────────────────────────────────
def _purge_tree(root: str) -> tuple[int, int]:
    '''Delete a directory tree in a single pass, returning (bytes, files).

    One traversal that stats and unlinks together, instead of rglob-then-rmtree
    (which walked everything twice). Sizes are credited only for files actually
    removed, so a tree we lack permission on no longer inflates the totals.
    '''
    freed = files = 0
    stack, seen = [root], []
    while stack:
        cur = stack.pop()
        seen.append(cur)
        try:
            with os.scandir(cur) as it:
                for entry in it:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                        else:
                            size = entry.stat(follow_symlinks=False).st_size
                            if not DRY_RUN:
                                os.unlink(entry.path)
                            freed += size
                            files += 1
                    except OSError:
                        continue
        except OSError:
            continue
    if not DRY_RUN:
        for path in reversed(seen):  # deepest first
            with contextlib.suppress(OSError):
                os.rmdir(path)
    return freed, files


def _remove_entry(entry: Path) -> tuple[int, int]:
    try:
        if entry.is_dir() and not entry.is_symlink():
            return _purge_tree(str(entry))
        size = entry.lstat().st_size  # lstat: never bill a symlink for its target
        if not DRY_RUN:
            entry.unlink()
        return size, 1
    except OSError:
        return 0, 0


def _expand(pattern: str) -> Path:
    '''Expand a leading ~ only — str.replace('~', HOME) also mangled ~ mid-path.'''
    return Path(os.path.expanduser(pattern))


_GLOB_CHARS = re.compile(r'[*?\[]')


def _iter_matches(pattern: str) -> Iterator[Path]:
    '''Expand a pattern with wildcards in *any* component, not just the last.

    Browser caches live one level below a profile directory
    (Chrome/*/Code Cache/*), which the old parent.glob(name) could never reach.
    pathlib rather than the glob module on purpose: glob.glob skips dotfiles,
    which would quietly spare half of ~/.Trash and every hidden cache dir.
    '''
    path = _expand(pattern)
    parts = path.parts
    for i, part in enumerate(parts):
        if not _GLOB_CHARS.search(part):
            continue
        base = Path(*parts[:i]) if i else Path('.')
        try:
            yield from base.glob(str(Path(*parts[i:])))
        except (OSError, ValueError, IndexError):
            pass
        return
    if path.exists() or path.is_symlink():  # literal path, no wildcards
        yield path


async def _to_thread(fn: Callable[[], tuple[int, int]]) -> tuple[int, int]:
    return await asyncio.get_running_loop().run_in_executor(_IO_POOL, fn)


async def _clean_paths(
    paths: Sequence[str], log: Callable[[str], None], min_age: float = 0.0
) -> Result:
    def _do() -> tuple[int, int]:
        freed = count = 0
        cutoff = time.time() - min_age if min_age else 0.0
        for pattern in paths:
            try:
                entries = list(_iter_matches(pattern))
            except OSError:
                continue
            for entry in entries:
                if cutoff:
                    try:
                        if entry.lstat().st_mtime > cutoff:
                            continue  # in active use — leave it alone
                    except OSError:
                        continue
                f, c = _remove_entry(entry)
                freed += f
                count += c
        return freed, count

    freed, count = await _to_thread(_do)
    if freed or count:
        log(f'freed {_size_str(freed)} · {count:,} files')
    return Result(freed=freed, files=count)


async def _empty_trash(log: Callable[[str], None]) -> Result:
    '''The freedesktop trash in $HOME, plus the per-user one on every mounted
    volume — files deleted from a USB disk land there, not in $HOME.'''
    roots = [HOME / '.local/share/Trash']
    user = HOME.name
    for mounts in (Path('/media') / user, Path('/run/media') / user, Path('/mnt')):
        try:
            for vol in mounts.iterdir():
                trash = vol / f'.Trash-{UID}'
                if trash.is_dir():
                    roots.append(trash)
        except OSError:
            pass
    return await _clean_paths(
        [str(r / sub / '*') for r in roots for sub in ('files', 'info', 'expunged')], log)


# ~/.cache entries the blanket sweep leaves alone: either a dedicated task
# cleans them more carefully (uv prunes, pip purges, hf/torch hold models that
# take hours to re-download), or has its own row — sweeping those here too
# would bill the same bytes twice in a dry run — or they are live state that
# merely happens to live under .cache.
_DOT_CACHE_KEEP = frozenset({
    'uv', 'pip', 'huggingface', 'torch', 'pre-commit', 'lm-studio',
    'thumbnails', 'mozilla', 'zen', 'librewolf', 'BraveSoftware',
    'google-chrome', 'google-chrome-beta', 'chromium', 'microsoft-edge',
    'vivaldi', 'opera', 'yarn', 'node-gyp', 'electron', 'JetBrains', 'spotify',
    'keyring', 'gnome-software', 'ibus', 'ibus-table', 'tracker3',
    'evolution', 'ubuntu-pro', 'update-manager-core', 'claude', 'opencode',
    'claude-cli-nodejs', 'motd.legal-displayed',
})


async def _dot_cache(log: Callable[[str], None]) -> Result:
    '''~/.cache is the XDG cache root — the Linux ~/Library/Caches.'''
    base = HOME / '.cache'
    try:
        names = [e.name for e in os.scandir(base) if e.name not in _DOT_CACHE_KEEP]
    except OSError:
        return Result()
    return await _clean_paths([str(base / n) for n in names], log)


# Snaps and flatpaks each get a private $HOME, so their caches never land in
# ~/.cache. Apps with a dedicated row are left to it.
_SNAP_OWN_ROW = frozenset({'firefox', 'chromium', 'brave', 'opera', 'code', 'slack',
                           'discord', 'signal-desktop', 'obsidian', 'postman',
                           'insomnia', 'teams-for-linux', 'spotify', 'telegram-desktop',
                           'libreoffice'})
_FLATPAK_OWN_ROW = frozenset({'org.mozilla.firefox', 'org.chromium.Chromium',
                              'com.brave.Browser', 'com.google.Chrome', 'com.microsoft.Edge',
                              'com.vivaldi.Vivaldi', 'app.zen_browser.zen',
                              'io.gitlab.librewolf-community', 'com.visualstudio.code',
                              'com.slack.Slack', 'com.discordapp.Discord', 'org.signal.Signal',
                              'md.obsidian.Obsidian', 'com.spotify.Client',
                              'org.telegram.desktop', 'com.valvesoftware.Steam',
                              'org.libreoffice.LibreOffice'})


async def _sandbox_caches(log: Callable[[str], None]) -> Result:
    paths: list[str] = []
    for base, own, subs in ((HOME / 'snap', _SNAP_OWN_ROW, ('common/.cache', 'current/.cache')),
                            (HOME / '.var/app', _FLATPAK_OWN_ROW, ('cache',))):
        with contextlib.suppress(OSError):
            for app in os.scandir(base):
                if app.name not in own and app.is_dir(follow_symlinks=False):
                    paths += [str(Path(app.path, sub, '*')) for sub in subs]
    return await _clean_paths(paths, log)


# ── Root-only tasks ──────────────────────────────────────────────────────────
def _needs_root(fn: Callable) -> Callable:
    async def run(log: Callable[[str], None]) -> Result:
        if not SUDO_OK and not DRY_RUN:
            log('needs root — run tidy from a terminal (it asks once) or as root')
            return Result(skipped=True, note='needs sudo')
        return await fn(log)
    return run


_RE_APT = re.compile(r'^(\d+) upgraded, ')
_RE_APT_FREED = re.compile(r'After this operation, ([\d.,]+) ([kMG]B) disk space will be freed')
_UNITS = {'kB': 1000, 'MB': 1000 ** 2, 'GB': 1000 ** 3}


async def _apt(log: Callable[[str], None]) -> Result:
    pkgs = freed = 0

    def counted(msg: str) -> None:
        nonlocal pkgs, freed
        log(msg)
        if m := _RE_APT.match(msg):
            pkgs = max(pkgs, int(m.group(1)))
        if m := _RE_APT_FREED.search(msg):
            with contextlib.suppress(ValueError):
                freed += int(float(m.group(1).replace(',', '')) * _UNITS[m.group(2)])

    opts = ['-y', '-o', 'Dpkg::Options::=--force-confdef', '-o', 'Dpkg::Options::=--force-confold']
    cmds = [
        _root(['apt-get', 'update']),
        # full-upgrade, not upgrade: plain upgrade holds back anything whose
        # dependencies changed, which on Ubuntu is every kernel update.
        _root(['apt-get', 'full-upgrade', *opts]),
        _root(['apt-get', 'autoremove', '--purge', *opts]),
    ]
    # Packages removed without --purge leave their config behind in state
    # "rc"; nothing uses it, and it clutters every dpkg listing.
    if residual := await asyncio.to_thread(_dpkg_residual):
        cmds.append(_root(['apt-get', 'purge', *opts, *residual]))
    series = await _run_series(cmds, log, counted)
    freed += _dir_size('/var/cache/apt/archives')
    # clean, not autoclean: the .debs are re-downloadable, and autoclean keeps
    # every one still in the archive, which is nearly all of them.
    clean = await _run_series([_root(['apt-get', 'clean'])], log)
    first = series if series.failures else clean
    notes = [_fail_note(first)] if first.failures else []
    if residual:
        notes.append(f'{len(residual)} residual configs')
    if Path('/run/reboot-required').exists():
        notes.append('reboot required')
    return Result(pkgs=pkgs, freed=freed if not clean.failures else 0,
                  failed=bool(series.failures or clean.failures),
                  note=' · '.join(n for n in notes if n), failed_cmd=first.cmd,
                  failed_tail=first.tail)


def _dpkg_residual() -> list[str]:
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        out = subprocess.run(['dpkg-query', '-W', '-f', '${db:Status-Abbrev} ${Package}\\n'],
                             capture_output=True, text=True, timeout=60).stdout
        return [line.split()[1] for line in out.splitlines()
                if line.startswith('rc') and len(line.split()) == 2]
    return []


def _dir_size(path: str) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            with contextlib.suppress(OSError):
                total += os.lstat(os.path.join(root, f)).st_size
    return total


# fwupdmgr exits 2 for "nothing to do", which is the usual, healthy outcome.
_FWUPD_NOTHING = 2


async def _firmware(log: Callable[[str], None]) -> Result:
    counter = _Count()

    def counted(msg: str) -> None:
        log(msg)
        if re.match(r'^\s*(?:Successfully installed|Downloading .* for )', msg):
            counter.n += 1

    failures: list[tuple[str, list[str]]] = []
    for cmd in (['fwupdmgr', 'refresh', '--force'],
                ['fwupdmgr', 'update', '-y', '--no-reboot-check', '--offline']):
        full = _root(cmd)
        log(f'$ {" ".join(full)}')
        tail: list[str] = []

        def keep(msg: str) -> None:
            counted(msg)
            tail.append(msg)
            del tail[:-FAIL_TAIL_LINES]

        if await _run_cmd(full, keep) not in (0, _FWUPD_NOTHING):
            failures.append((' '.join(full), list(tail)))
    if failures:
        cmd, tail = failures[0]
        return Result(pkgs=counter.n, failed=True, note=f'failed: {cmd}',
                      failed_cmd=cmd, failed_tail=tail)
    note = 'staged for next boot' if counter.n else ''
    return Result(pkgs=counter.n, note=note)


# Root-owned leftovers: rotated logs (logrotate's .1/.gz generations),
# systemd coredumps, apport crash files of every user, and snapd's download
# cache (hardlinks to installed snaps — dropping them frees what nothing else
# still links to). find prints each size before deleting it, so the task can
# bill exactly what went.
_ROOT_SWEEPS = [
    ['/var/log', '-type', 'f', '(', '-name', '*.gz', '-o', '-name', '*.xz', '-o', '-name', '*.old',
     '-o', '-regex', r'.*\.[0-9]+', ')'],
    ['/var/lib/systemd/coredump', '-type', 'f'],
    ['/var/crash', '-type', 'f'],
    ['/var/lib/snapd/cache', '-type', 'f', '-links', '1'],
]


async def _root_sweep(log: Callable[[str], None]) -> Result:
    freed = files = 0

    def tally(msg: str) -> None:
        nonlocal freed, files
        if msg.isdigit():
            freed += int(msg)
            files += 1
        else:
            log(msg)

    cmds = [_root(['find', *args, '-printf', '%s\\n', '-delete'])
            for args in _ROOT_SWEEPS if Path(args[0]).is_dir()]
    series = await _run_series(cmds, log, tally)
    if freed or files:
        log(f'freed {_size_str(freed)} · {files:,} files')
    return Result(freed=freed, files=files, failed=bool(series.failures),
                  note=_fail_note(series), failed_cmd=series.cmd,
                  failed_tail=series.tail)


_RE_SNAP = re.compile(r'^(\S+) .*\brefreshed\b')
_RE_SNAP_DISABLED = re.compile(r'^(\S+)\s+\S+\s+(\d+)\s+.*\bdisabled\b')


async def _snap(log: Callable[[str], None]) -> Result:
    '''Refresh snaps, then drop the disabled revisions snapd keeps as
    rollback copies — two per snap by default, often gigabytes in total.'''
    counter = _Count()

    def counted(msg: str) -> None:
        log(msg)
        if _RE_SNAP.match(msg):
            counter.n += 1

    series = await _run_series([_root(['snap', 'refresh'])], log, counted)
    # Listing is read-only, so it runs even in a dry run: the log should say
    # what would go.
    out = ''
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        out = (await asyncio.to_thread(
            subprocess.run, ['snap', 'list', '--all'],
            capture_output=True, text=True, timeout=60)).stdout
    old = [m.groups() for line in out.splitlines()
           if (m := _RE_SNAP_DISABLED.match(line))]
    freed = 0
    for name, rev in old:
        with contextlib.suppress(OSError):
            freed += os.stat(f'/var/lib/snapd/snaps/{name}_{rev}.snap').st_size
    removed = await _run_series(
        [_root(['snap', 'remove', name, f'--revision={rev}']) for name, rev in old], log)
    failures = series.failures + removed.failures
    first = series if series.failures else removed
    note = _fail_note(first) if failures else (f'{len(old)} old revisions' if old else '')
    return Result(pkgs=counter.n, freed=freed if not removed.failures else 0,
                  failed=bool(failures), note=note, failed_cmd=first.cmd,
                  failed_tail=first.tail)


_RE_JOURNAL = re.compile(r'Vacuuming done, freed ([\d.]+)([BKMGT])')
_JOURNAL_UNITS = {'B': 1, 'K': 1 << 10, 'M': 1 << 20, 'G': 1 << 30, 'T': 1 << 40}


async def _journal(log: Callable[[str], None]) -> Result:
    freed = 0

    def _log(msg: str) -> None:
        nonlocal freed
        log(msg)
        if m := _RE_JOURNAL.search(msg):
            freed += int(float(m.group(1)) * _JOURNAL_UNITS[m.group(2)])

    series = await _run_series(
        [_root(['journalctl', '--vacuum-time=30d', '--vacuum-size=500M'])], log, _log)
    return Result(freed=freed, failed=bool(series.failures),
                  note=_fail_note(series), failed_cmd=series.cmd,
                  failed_tail=series.tail)


async def _home_sweep(log: Callable[[str], None]) -> Result:
    '''One pass over $HOME for .DS_Store droppings and regenerable build cruft.

    These used to be two tasks walking the same tree concurrently, which
    doubled the slowest part of the whole run for no benefit.
    '''
    def _do() -> tuple[int, int]:
        freed = count = 0
        for root, dirnames, files in os.walk(HOME):
            targets = [d for d in dirnames if d in _CRUFT_DIRS]
            # Prune caches/VCS/app state — walking node_modules and .local
            # dominated the runtime — and the cruft dirs themselves, which are
            # about to be purged wholesale. Notably venvs stay skipped: their
            # __pycache__ belongs to the venv, not to us.
            dirnames[:] = [
                d for d in dirnames
                if d not in _WALK_SKIP and d not in _CRUFT_DIRS
            ]
            for name in targets:
                f, c = _purge_tree(os.path.join(root, name))
                freed += f
                count += c
            if '.DS_Store' in files:
                path = os.path.join(root, '.DS_Store')
                try:
                    size = os.lstat(path).st_size
                    if not DRY_RUN:
                        os.unlink(path)
                    freed += size
                    count += 1
                except OSError:
                    pass
        return freed, count

    freed, count = await _to_thread(_do)
    if count:
        log(f'freed {_size_str(freed)} · {count:,} files')
    return Result(freed=freed, files=count)


# ── Package-count matchers ───────────────────────────────────────────────────
# Each tool reports upgrades differently; loose heuristics over-counted badly
# (the old ' -> ' in msg and 'v' in msg rule matched almost any version string).
_RE_BREW = re.compile(r'^==> Upgrading (?!\d+\s+outdated)(\S+)')
_RE_PIPX = re.compile(r'^upgraded package (\S+)', re.I)
_RE_UV = re.compile(r'^updated (\S+) v\S+ -> v', re.I)
_RE_PIPUPGRADE = re.compile(r'^successfully installed ', re.I)
_RE_RUSTUP = re.compile(r'^\s*(\S+) updated - ')
_RE_NPM = re.compile(r'^(?:changed|added|updated) (\d+) packages?', re.I)
_RE_MAS = re.compile(r'^upgrading (\d+) outdated application', re.I)
_RE_GEM_SUM = re.compile(r'^gems updated:\s*(.+)$', re.I)
_RE_GEM_INST = re.compile(r'^successfully installed ', re.I)
_RE_SWU = re.compile(r'^\s*(?:installing|installed):?\s+\S', re.I)


def _m_brew(line: str, c: _Count) -> None:
    if _RE_BREW.match(line):
        c.n += 1


def _m_python(line: str, c: _Count) -> None:
    if _RE_PIPX.match(line) or _RE_UV.match(line) or _RE_PIPUPGRADE.match(line):
        c.n += 1


def _m_rustup(line: str, c: _Count) -> None:
    if _RE_RUSTUP.match(line):
        c.n += 1


def _m_npm(line: str, c: _Count) -> None:
    if m := _RE_NPM.match(line):
        c.n = max(c.n, int(m.group(1)))  # npm prints a total, not per-package


def _m_mas(line: str, c: _Count) -> None:
    if m := _RE_MAS.match(line):
        c.n = max(c.n, int(m.group(1)))


def _m_gem(line: str, c: _Count) -> None:
    if m := _RE_GEM_SUM.match(line):
        c.n = max(c.n, len(m.group(1).split()))  # authoritative summary line
    elif _RE_GEM_INST.match(line) and not c.n:
        c.n += 1


def _m_swu(line: str, c: _Count) -> None:
    if _RE_SWU.match(line):
        c.n += 1


def _m_omz(line: str, c: _Count) -> None:
    low = line.lower()
    if 'hooray' in low or 'oh my zsh has been updated' in low:
        c.n = 1


# ── Upgrade tasks ────────────────────────────────────────────────────────────
def _upgrade_runner(
    cmds: Sequence[Sequence[str]],
    matcher: Matcher | None = None,
    count_only: frozenset[int] | None = None,
    require: Sequence[str] = (),
) -> Callable:
    async def run(log: Callable[[str], None]) -> Result:
        for tool in require or [cmds[0][0]]:
            if not _has(tool):
                log(f'{tool} not found, skipping')
                return SKIPPED
        counter = _Count()

        def counted(msg: str) -> None:
            log(msg)
            if matcher:
                matcher(msg, counter)

        series = await _run_series(cmds, log, counted, count_only)
        return Result(
            pkgs=counter.n,
            failed=bool(series.failures),
            note=_fail_note(series),
            failed_cmd=series.cmd,
            failed_tail=series.tail,
        )

    return run


async def _brew_upgrade(log: Callable[[str], None]) -> Result:
    cmds = [
        ['brew', 'update'],
        ['brew', 'upgrade'],
        # --greedy: without it brew skips every cask that self-updates, which
        # is most of them, and the task reports success having done nothing.
        ['brew', 'upgrade', '--cask', '--greedy'],
        ['brew', 'autoremove'],
        ['brew', 'cleanup', '-s'],
        ['brew', 'doctor'],
        ['brew', 'missing'],
    ]
    counter = _Count()

    def counted(msg: str) -> None:
        log(msg)
        _m_brew(msg, counter)

    # brew doctor/missing exit non-zero routinely; don't call that a failure.
    series = await _run_series(cmds[:5], log, counted, frozenset({1, 2}))
    await _run_series(cmds[5:], log)
    return Result(pkgs=counter.n, failed=bool(series.failures),
                  note=_fail_note(series), failed_cmd=series.cmd,
                  failed_tail=series.tail)


async def _docker_prune(log: Callable[[str], None]) -> Result:
    if await _run_cmd(['docker', 'info'], lambda _: None) != 0:
        log('docker daemon not running, skipping')
        return SKIPPED
    freed = 0

    def _log(msg: str) -> None:
        nonlocal freed
        log(msg)
        if 'total reclaimed space:' in msg.lower():
            with contextlib.suppress(ValueError, IndexError):
                raw = msg.split(':', 1)[1].strip().lower()
                for suffix, mult in (('gb', 1 << 30), ('mb', 1 << 20),
                                     ('kb', 1 << 10), ('b', 1)):
                    if raw.endswith(suffix):
                        freed = int(float(raw[: -len(suffix)].strip()) * mult)
                        break

    await _run_cmd(['docker', 'system', 'prune', '-f'], _log)
    return Result(freed=freed)


async def _pip_clean(log: Callable[[str], None]) -> Result:
    files = 0

    def _log(msg: str) -> None:
        nonlocal files
        log(msg)
        if 'files removed:' in msg.lower() or 'file removed:' in msg.lower():
            with contextlib.suppress(ValueError, IndexError):
                files = int(msg.split(':', 1)[1].strip())

    pip = 'pip' if _has('pip') else 'pip3'
    await _run_cmd([pip, 'cache', 'purge'], _log)
    return Result(files=files)


_RE_PNPM_FILES = re.compile(r'(\d[\d,]*)\s+files?\s+removed', re.I)


async def _pnpm_prune(log: Callable[[str], None]) -> Result:
    files = 0

    def _log(msg: str) -> None:
        nonlocal files
        log(msg)
        if m := _RE_PNPM_FILES.search(msg):
            files = max(files, int(m.group(1).replace(',', '')))

    rc = await _run_cmd(['pnpm', 'store', 'prune'], _log)
    return Result(files=files, failed=rc != 0,
                  note='prune failed' if rc else '')


async def _nuget_clean(log: Callable[[str], None]) -> Result:
    '''Clear NuGet's *caches*.

    ~/.nuget/packages is the global packages folder every project resolves
    against, not a cache — emptying it forces a full restore of every solution
    on the machine. http-cache/temp/plugins-cache are the real caches.
    '''
    if _has('dotnet'):
        base = ['dotnet', 'nuget', 'locals']
        return await _simple_cmds(
            [[*base, kind, '--clear'] for kind in
             ('http-cache', 'temp', 'plugins-cache')], log)
    return await _simple_cmds(
        [['nuget', 'locals', kind, '-clear'] for kind in ('http-cache', 'temp')], log)


async def _simple_cmds(cmds: Sequence[Sequence[str]], log: Callable[[str], None]) -> Result:
    series = await _run_series(cmds, log)
    return Result(failed=bool(series.failures), note=_fail_note(series),
                  failed_cmd=series.cmd, failed_tail=series.tail)


# ── Task table ───────────────────────────────────────────────────────────────
@dataclass(slots=True)
class Task:
    id: str
    name: str
    subcategory: str
    fn: Callable
    timeout: float = LOG_TIMEOUT
    locks: tuple[str, ...] = field(default_factory=tuple)


class _LockSet:
    '''Named async locks, created on first use.'''
    __slots__ = ('_locks',)

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}

    @contextlib.asynccontextmanager
    async def hold(self, names: tuple[str, ...]):
        # Sorted acquisition order: a task holding two locks can't deadlock
        # against one taking them in the opposite order.
        async with contextlib.AsyncExitStack() as stack:
            for name in names:
                lock = self._locks.setdefault(name, asyncio.Lock())
                await stack.enter_async_context(lock)
            yield


async def _execute(
    task: Task,
    locks: _LockSet,
    log: Callable[[str], None],
    on_start: Callable[[], None] | None = None,
) -> tuple[Result, float, str]:
    '''Run one task under its locks. Never raises; returns (result, seconds, status).

    Shared by the TUI and the headless runner so the two can't drift apart on
    timeouts, locking or error handling.
    '''
    async with locks.hold(task.locks):
        if on_start:
            on_start()
        start = time.monotonic()
        try:
            res = await asyncio.wait_for(task.fn(log), timeout=task.timeout)
        except asyncio.TimeoutError:
            log(f'timed out after {task.timeout / 60:.0f} min')
            return Result(failed=True, note='timeout'), time.monotonic() - start, 'timeout'
        except Exception as exc:
            log(repr(exc))
            return Result(failed=True, note='error'), time.monotonic() - start, 'error'
        if not isinstance(res, Result):
            res = Result()
        elapsed = time.monotonic() - start
        status = 'skipped' if res.skipped else 'failed' if res.failed else 'ok'
        return res, elapsed, status


_WHICH_CACHE: dict[str, bool] = {}


def _has(cmd: str) -> bool:
    '''Cached PATH lookup — the task table probes ~100 tools at import.'''
    if cmd not in _WHICH_CACHE:
        import shutil
        _WHICH_CACHE[cmd] = shutil.which(cmd) is not None
    return _WHICH_CACHE[cmd]


# Tasks writing the same tree or driving the same package manager must not run
# concurrently: parallel `brew upgrade` + `brew cleanup` deadlocks on Homebrew's
# lock, and the blanket ~/.cache sweep raced every per-app cache task,
# double-counting bytes and spraying ENOENT.
_PATH_LOCKS = (
    ('~/.cache', 'dot-cache'),
    ('~/.config', 'dot-config'),
    ('~/.local/share', 'dot-share'),
    ('~/.gradle', 'gradle'),
    ('~/.cargo', 'rust'),
    ('~/.rustup', 'rust'),
    ('~/go', 'go'),
    ('~/.gem', 'gem'),
)


def _locks_for(paths: Sequence[str]) -> tuple[str, ...]:
    found = {name for path in paths for prefix, name in _PATH_LOCKS
             if path.startswith(prefix)}
    return tuple(sorted(found))


def _path_task(tid: str, name: str, subcat: str, paths: list[str],
               min_age: float = 0.0) -> Task:
    async def run(log: Callable[[str], None]) -> Result:
        return await _clean_paths(paths, log, min_age)

    return Task(tid, name, subcat, run, locks=_locks_for(paths))


def _cmd_task(tid: str, name: str, subcat: str, paths: list[str], *cmds: str) -> Task | None:
    '''Path task included only if any of the given commands exist in PATH.'''
    return _path_task(tid, name, subcat, paths) if any(_has(c) for c in cmds) else None


def _dir_task(tid: str, name: str, subcat: str, paths: list[str], *checks: str) -> Task | None:
    '''Path task included only if one of the given directories exists.'''
    found = any(_expand(c).exists() for c in checks)
    return _path_task(tid, name, subcat, paths) if found else None


def _app_task(tid: str, name: str, subcat: str,
              roots: Sequence[str], patterns: Callable[[str], list[str]]) -> Task | None:
    '''Path task over whichever install roots exist.

    One app can live natively (~/.config/X), as a snap (~/snap/<name>/...) and
    as a flatpak (~/.var/app/<id>/...) — each keeps its own copy of the same
    cache layout, so the patterns are stamped out per root that is present.
    '''
    present = [r for r in roots if _expand(r).is_dir()]
    if not present:
        return None
    return _path_task(tid, name, subcat, [p for r in present for p in patterns(r)])


def _snap_root(snap: str, rel: str) -> str:
    return f'~/snap/{snap}/current/{rel}'


def _flatpak_root(app_id: str, rel: str) -> str:
    return f'~/.var/app/{app_id}/{rel}'


def _chromium_task(tid: str, name: str, config: str, *,
                   snap: str | None = None, flatpak: str | None = None) -> Task | None:
    '''Cache dirs of a Chromium-family browser, across every profile.

    Chromium splits its caches on Linux just as on macOS: the HTTP and code
    caches go to ~/.cache/<browser>, everything else stays beside the profile
    in ~/.config. The `*` matches Default / "Profile 1" / "System Profile"
    alike; a profile lacking a given cache just contributes no matches.
    '''
    pairs = [(f'~/.config/{config}', f'~/.cache/{config}')]
    if snap:
        pairs.append((f'~/snap/{snap}/common/{snap}', f'~/snap/{snap}/common/.cache/{snap}'))
    if flatpak:
        pairs.append((_flatpak_root(flatpak, f'config/{config}'),
                      _flatpak_root(flatpak, f'cache/{config}')))
    paths = [p for support, cache in pairs if _expand(support).is_dir()
             for p in _chromium(support, cache)]
    return _path_task(tid, name, 'Browsers', paths) if paths else None


def _chromium(support: str, cache: str) -> list[str]:
    return [
        f'{cache}/*/Cache/*',
        f'{cache}/*/Code Cache/*',
        f'{cache}/ShaderCache/*',
        f'{support}/*/GPUCache/*',
        f'{support}/*/DawnGraphiteCache/*',
        f'{support}/*/DawnWebGPUCache/*',
        f'{support}/*/Service Worker/CacheStorage/*',
        f'{support}/*/Service Worker/ScriptCache/*',
        f'{support}/GPUPersistentCache/*',
        f'{support}/GraphiteDawnCache/*',
        f'{support}/GrShaderCache/*',
        f'{support}/ShaderCache/*',
        f'{support}/component_crx_cache/*',
        f'{support}/extensions_crx_cache/*',
    ]


def _gecko(cache: str) -> list[str]:
    '''Gecko keeps its disk cache per profile, outside the profile itself.'''
    return [f'{cache}/*/cache2/*', f'{cache}/*/startupCache/*']


def _electron(base: str) -> list[str]:
    '''Cache dirs every Electron app inherits from Chromium.'''
    return [
        f'{base}/Cache/*',
        f'{base}/Code Cache/*',
        f'{base}/GPUCache/*',
        f'{base}/DawnGraphiteCache/*',
        f'{base}/DawnWebGPUCache/*',
        f'{base}/Service Worker/CacheStorage/*',
    ]


def _electron_task(tid: str, name: str, subcat: str, config: str, *,
                   snap: str | None = None, flatpak: str | None = None,
                   extra: Sequence[str] = ()) -> Task | None:
    roots = [f'~/.config/{config}']
    if snap:
        roots.append(_snap_root(snap, f'.config/{config}'))
    if flatpak:
        roots.append(_flatpak_root(flatpak, f'config/{config}'))
    return _app_task(tid, name, subcat, roots,
                     lambda r: [*_electron(r), *(f'{r}/{e}' for e in extra)])


def _upgrade(tid: str, name: str, cmds: list[list[str]], *, matcher: Matcher | None = None,
             require: Sequence[str] = (), locks: tuple[str, ...] = (),
             timeout: float = CMD_TIMEOUT, when: bool = True) -> Task | None:
    if not when:
        return None
    probes = require or [cmds[0][0]]
    if not any(_has(p) for p in probes):
        return None
    return Task(tid, name, 'Upgrades',
                _upgrade_runner(cmds, matcher, require=probes),
                timeout=timeout, locks=locks)


def _collect(*tasks: Task | None) -> list[Task]:
    return [t for t in tasks if t is not None]


# One row per ref in flatpak's plan table: "1. [✓] org.x.App  stable  u  flathub".
_RE_FLATPAK = re.compile(r'^\s*\d+\.\s+(?:\[.\]\s+)?\S+\s+\S+\s+[ui]\s')


def _user_owned(cmd: str) -> bool:
    path = shutil.which(cmd)
    return bool(path) and Path(path).resolve().is_relative_to(HOME)


_RE_CARGO_UPD = re.compile(r'^Overall updated (\d+) packages?', re.I)


def _m_cargo(line: str, c: _Count) -> None:
    if m := _RE_CARGO_UPD.match(line):
        c.n = int(m.group(1))


def _m_flatpak(line: str, c: _Count) -> None:
    if _RE_FLATPAK.match(line):
        c.n += 1


# oh-my-zsh ships `omz` as a *shell function*, so shutil.which never finds it and
# the task silently vanished from the list. Drive the upgrade script directly.
_OMZ = HOME / '.oh-my-zsh/tools/upgrade.sh'

ALL_TASKS: list[Task] = [
    # ── Upgrades ─────────────────────────────────────────────────────────────
    *_collect(
        Task('apt', 'APT packages', 'Upgrades', _needs_root(_apt),
             timeout=SLOW_TIMEOUT, locks=('apt',)) if _has('apt-get') else None,
        Task('snap', 'Snaps', 'Upgrades', _needs_root(_snap),
             timeout=SLOW_TIMEOUT, locks=('snap',)) if _has('snap') else None,
        _upgrade('flatpak', 'Flatpak', [['flatpak', 'update', '-y', '--noninteractive']],
                 matcher=_m_flatpak, locks=('flatpak',), timeout=SLOW_TIMEOUT),
        Task('firmware', 'Firmware (fwupd)', 'Upgrades', _needs_root(_firmware),
             timeout=SLOW_TIMEOUT) if _has('fwupdmgr') else None,
        Task('brew', 'Homebrew', 'Upgrades', _brew_upgrade,
             timeout=SLOW_TIMEOUT, locks=('brew',)) if _has('brew') else None,
        _upgrade('cargo_bins', 'Cargo binaries', [['cargo', 'install-update', '-a']],
                 matcher=_m_cargo, require=('cargo-install-update',), locks=('rust',)),
        _upgrade('gh_ext', 'gh extensions', [['gh', 'extension', 'upgrade', '--all']],
                 when=_has('gh') and any((HOME / '.local/share/gh/extensions').glob('*'))),
        _upgrade('omz', 'Oh My Zsh', [['zsh', '-f', str(_OMZ), '-v', 'minimal']],
                 matcher=_m_omz, require=('zsh',), when=_OMZ.is_file()),
        _upgrade('python', 'Python tools',
                 [c for c in ([['pipupgrade', '-y', '-u']] if _has('pipupgrade') else [])
                  + ([['pipx', 'upgrade-all']] if _has('pipx') else [])
                  + ([['uv', 'tool', 'upgrade', '--all']] if _has('uv') else [])],
                 matcher=_m_python, require=('pipx', 'uv', 'pipupgrade'),
                 locks=('python',)),
        # A distro-packaged npm or ruby installs globals under /usr, where an
        # unprivileged update can only fail — only drive the ones living in
        # $HOME (nvm, fnm, rbenv, mise, …); apt upgrades the rest.
        _upgrade('node', 'Node globals', [['npm', '-g', 'update']],
                 matcher=_m_npm, locks=('npm',), when=_user_owned('npm')),
        _upgrade('rust', 'Rust', [['rustup', 'update']],
                 matcher=_m_rustup, locks=('rust',)),
        _upgrade('ruby', 'Ruby gems', [['gem', 'update', '--system'], ['gem', 'update']],
                 matcher=_m_gem, locks=('gem',), when=_user_owned('gem')),
    ),
    # ── System ───────────────────────────────────────────────────────────────
    Task('dotcache', 'User Caches (~/.cache)', 'System', _dot_cache, locks=('dot-cache',)),
    _path_task('thumbs',     'Thumbnails',             'System', ['~/.cache/thumbnails/*', '~/.thumbnails/*']),
    _path_task('crash',      'Crash Reports',          'System', [f'/var/crash/*.{UID}.crash', f'/var/crash/*.{UID}.upload*']),
    # min_age: don't yank temp files out from under processes running right now.
    _path_task('tmp',        'Temp Dirs',              'System', ['/tmp/*', '/var/tmp/*'], min_age=86400),
    _path_task('xsession',   'X Session Logs',         'System', ['~/.xsession-errors.old', '~/.local/share/xorg/*.old']),
    _path_task('incomplete', 'Incomplete Downloads',   'System', ['~/Downloads/*.crdownload', '~/Downloads/*.part']),
    _path_task('shellres',   'Shell History Residue',  'System', ['~/.zsh_history.bak*', '~/.zcompdump*']),
    Task('trash',     'Trash',                  'System', _empty_trash, locks=('dot-share',)),
    Task('sandbox',   'Snap & Flatpak Caches',  'System', _sandbox_caches),
    Task('homesweep', '.DS_Store & Cruft',      'System', _home_sweep),
    *_collect(
        Task('journal', 'systemd Journal (30d)', 'System', _needs_root(_journal),
             timeout=CMD_TIMEOUT) if _has('journalctl') else None,
        Task('rootsweep', 'Old Logs, Cores & Crashes', 'System', _needs_root(_root_sweep),
             timeout=CMD_TIMEOUT) if _has('find') else None,
        Task('flatpak_cl', 'Flatpak Unused', 'System',
             lambda log: _simple_cmds([['flatpak', 'uninstall', '--unused', '-y',
                                        '--noninteractive']], log),
             timeout=CMD_TIMEOUT, locks=('flatpak',)) if _has('flatpak') else None,
        Task('brew_cl', 'Homebrew Cleanup', 'System',
             lambda log: _simple_cmds([['brew', 'cleanup', '--prune=all'],
                                       ['brew', 'autoremove']], log),
             timeout=CMD_TIMEOUT, locks=('brew',)) if _has('brew') else None,
        Task('docker', 'Docker Prune', 'System', _docker_prune,
             timeout=CMD_TIMEOUT) if _has('docker') else None,
        Task('podman', 'Podman Prune', 'System',
             lambda log: _simple_cmds([['podman', 'system', 'prune', '-f']], log),
             timeout=CMD_TIMEOUT) if _has('podman') else None,
        Task('dns', 'DNS Cache', 'System',
             lambda log: _simple_cmds([['resolvectl', 'flush-caches']], log))
        if _has('resolvectl') else None,
    ),
    # ── Browsers ─────────────────────────────────────────────────────────────
    *_collect(
        _chromium_task('chrome',   'Chrome',        'google-chrome', flatpak='com.google.Chrome'),
        _chromium_task('canary',   'Chrome Beta',   'google-chrome-beta'),
        _chromium_task('chromium', 'Chromium',      'chromium', snap='chromium', flatpak='org.chromium.Chromium'),
        _chromium_task('brave',    'Brave',         'BraveSoftware/Brave-Browser', snap='brave', flatpak='com.brave.Browser'),
        _chromium_task('edge',     'Edge',          'microsoft-edge', flatpak='com.microsoft.Edge'),
        _chromium_task('vivaldi',  'Vivaldi',       'vivaldi', flatpak='com.vivaldi.Vivaldi'),
        _chromium_task('opera',    'Opera',         'opera', snap='opera'),
        _app_task('firefox', 'Firefox', 'Browsers',
                  ['~/.cache/mozilla/firefox', '~/snap/firefox/common/.cache/mozilla/firefox',
                   '~/.var/app/org.mozilla.firefox/cache/mozilla/firefox'], _gecko),
        _app_task('zen', 'Zen', 'Browsers',
                  ['~/.cache/zen', '~/.var/app/app.zen_browser.zen/cache/zen'], _gecko),
        _app_task('librewolf', 'LibreWolf', 'Browsers',
                  ['~/.cache/librewolf', '~/.var/app/io.gitlab.librewolf-community/cache/librewolf'], _gecko),
        _dir_task('tor', 'Tor Browser', 'Browsers',
                  ['~/.local/share/torbrowser/tbb/*/tor-browser/Browser/TorBrowser/Data/Browser/Caches/*'],
                  '~/.local/share/torbrowser'),
    ),
    # ── Dev — JS/Node ────────────────────────────────────────────────────────
    *_collect(
        Task('npm', 'npm', 'Dev — JS/Node',
             lambda log: _simple_cmds([['npm', 'cache', 'clean', '--force']], log),
             timeout=CMD_TIMEOUT, locks=('npm',)) if _has('npm') else None,
        _cmd_task('yarn',      'Yarn',      'Dev — JS/Node', ['~/.cache/yarn/*'],             'yarn'),
        # Never delete the store directly: every node_modules on the machine
        # hardlinks into it, so a wipe silently guts installed projects.
        # `store prune` drops only the packages nothing references any more.
        Task('pnpm', 'pnpm store', 'Dev — JS/Node', _pnpm_prune,
             timeout=CMD_TIMEOUT) if _has('pnpm') else None,
        _cmd_task('bun',       'Bun',       'Dev — JS/Node', ['~/.bun/install/cache/*'],      'bun'),
        _dir_task('nodegyp',   'node-gyp',  'Dev — JS/Node', ['~/.cache/node-gyp/*', '~/.node-gyp/*'], '~/.cache/node-gyp', '~/.node-gyp'),
        _dir_task('turbo',     'Turbo',     'Dev — JS/Node', ['~/.turbo/*'],                  '~/.turbo'),
        _dir_task('electron',  'Electron',  'Dev — JS/Node', ['~/.cache/electron/*', '~/.electron/*'], '~/.cache/electron', '~/.electron'),
        _cmd_task('npx',       'npx cache', 'Dev — JS/Node', ['~/.npm/_npx/*'],               'npm'),
        _dir_task('nvmcache',  'nvm cache', 'Dev — JS/Node', ['~/.nvm/.cache/*'],             '~/.nvm/.cache'),
    ),
    # ── Dev — JVM ────────────────────────────────────────────────────────────
    *_collect(
        _cmd_task('gradle', 'Gradle', 'Dev — JVM', ['~/.gradle/caches/*', '~/.gradle/daemon/*'], 'gradle'),
        # ~/.m2/repository is the local repository, not a cache: wiping it
        # re-downloads every dependency of every project. These are the stale
        # resolution markers and partial downloads, which are pure junk.
        _cmd_task('maven', 'Maven metadata', 'Dev — JVM',
                  ['~/.m2/repository/**/*.lastUpdated',
                   '~/.m2/repository/**/_remote.repositories',
                   '~/.m2/repository/**/resolver-status.properties',
                   '~/.m2/repository/**/*.part',
                   '~/.m2/repository/**/*.lock'], 'mvn'),
    ),
    # ── Dev — Python ─────────────────────────────────────────────────────────
    *_collect(
        Task('pip', 'pip', 'Dev — Python', _pip_clean, timeout=CMD_TIMEOUT,
             locks=('python',)) if any(_has(c) for c in ('pip', 'pip3')) else None,
        _cmd_task('pyenv',  'pyenv',  'Dev — Python', ['~/.pyenv/cache/*'],             'pyenv'),
        _cmd_task('conda',  'Conda',  'Dev — Python', ['~/.conda/pkgs/*'],              'conda', 'mamba'),
        # `prune` drops only what nothing links to; `clean` would force every
        # project to re-download its whole dependency set.
        Task('uvcache', 'uv cache', 'Dev — Python',
             lambda log: _simple_cmds([['uv', 'cache', 'prune']], log),
             timeout=CMD_TIMEOUT, locks=('python',)) if _has('uv') else None,
        _dir_task('precommit', 'pre-commit', 'Dev — Python', ['~/.cache/pre-commit/*'],     '~/.cache/pre-commit'),
        _dir_task('jupyter',   'Jupyter runtime', 'Dev — Python', ['~/.local/share/jupyter/runtime/*'], '~/.local/share/jupyter/runtime'),
    ),
    # ── Dev — Go ─────────────────────────────────────────────────────────────
    *_collect(
        Task('gobuild', 'Go build cache', 'Dev — Go',
             lambda log: _simple_cmds([['go', 'clean', '-cache'],
                                       ['go', 'clean', '-testcache']], log),
             timeout=CMD_TIMEOUT, locks=('go',)) if _has('go') else None,
        _cmd_task('gomod', 'Go module cache', 'Dev — Go', ['~/go/pkg/mod/cache/*'],       'go'),
    ),
    # ── Dev — Rust ───────────────────────────────────────────────────────────
    *_collect(
        _cmd_task('cargo',  'Cargo registry',   'Dev — Rust', ['~/.cargo/registry/cache/*'],                'cargo'),
        _cmd_task('rustup', 'Rustup downloads', 'Dev — Rust', ['~/.rustup/downloads/*', '~/.rustup/tmp/*'], 'rustup'),
        _cmd_task('cargosrc', 'Cargo sources',  'Dev — Rust', ['~/.cargo/registry/src/*'],                   'cargo'),
    ),
    # ── Dev — Ruby/PHP ───────────────────────────────────────────────────────
    *_collect(
        # ~/.gem/ruby/<ver>/gems holds *installed* gems — only the downloaded
        # .gem archives and the remote spec cache are safe to drop.
        _cmd_task('gem',      'Gem cache', 'Dev — Ruby/PHP', ['~/.gem/ruby/*/cache/*', '~/.gem/specs/*', '~/.local/share/gem/specs/*', '~/.local/share/gem/ruby/*/cache/*'], 'gem'),
        _cmd_task('bundler',  'Bundler',   'Dev — Ruby/PHP', ['~/.bundle/cache/*'],           'bundle'),
    ),
    # ── Dev — Other ──────────────────────────────────────────────────────────
    *_collect(
        Task('nuget', 'NuGet caches', 'Dev — Other', _nuget_clean,
             timeout=CMD_TIMEOUT) if any(_has(c) for c in ('dotnet', 'nuget')) else None,
        _dir_task('torch',   'PyTorch',      'Dev — Other', ['~/.cache/torch/*'],             '~/.cache/torch'),
        _dir_task('hf',      'Hugging Face', 'Dev — Other', ['~/.cache/huggingface/*'],       '~/.cache/huggingface'),
        _cmd_task('kubectl', 'kubectl',      'Dev — Other', ['~/.kube/cache/*'],              'kubectl'),
        _cmd_task('aws',     'AWS CLI',      'Dev — Other', ['~/.aws/cli/cache/*'],           'aws'),
        _cmd_task('terraform', 'Terraform',   'Dev — Other', ['~/.terraform.d/plugin-cache/*'],          'terraform', 'tofu'),
        _cmd_task('ccache',    'ccache',      'Dev — Other', ['~/.ccache/*'],                            'ccache'),
        _dir_task('gcloud',    'gcloud logs', 'Dev — Other', ['~/.config/gcloud/logs/*'],                '~/.config/gcloud/logs'),
        _dir_task('ansible',   'Ansible tmp', 'Dev — Other', ['~/.ansible/tmp/*'],                       '~/.ansible/tmp'),
        _dir_task('ollamalog', 'Ollama logs', 'Dev — Other', ['~/.ollama/logs/*'],                       '~/.ollama/logs'),
        _dir_task('lxd',       'LXD images',  'Dev — Other', ['~/snap/lxd/common/config/cache/*'],       '~/snap/lxd/common/config/cache'),
    ),
    # ── IDEs & Editors ───────────────────────────────────────────────────────
    *_collect(
        _dir_task('jetbrains', 'JetBrains logs', 'IDEs & Editors', ['~/.cache/JetBrains/*/log/*'], '~/.cache/JetBrains'),
        _electron_task('vscode',   'VS Code',  'IDEs & Editors', 'Code', snap='code',
                       flatpak='com.visualstudio.code', extra=('CachedData/*', 'logs/*')),
        _electron_task('cursor',   'Cursor',   'IDEs & Editors', 'Cursor', extra=('CachedData/*', 'logs/*')),
        _electron_task('windsurf', 'Windsurf', 'IDEs & Editors', 'Windsurf', extra=('CachedData/*', 'logs/*')),
        _dir_task('zedlogs',   'Zed logs',    'IDEs & Editors', ['~/.local/share/zed/logs/*'],             '~/.local/share/zed/logs'),
        _dir_task('androidsdk', 'Android SDK', 'IDEs & Editors', ['~/.android/cache/*', '~/.android/build-cache/*'], '~/.android'),
        # ~/.pub-cache/bin and global_packages hold `pub global activate`
        # tools; only the package sources, which `pub get` re-fetches, go.
        _cmd_task('pubcache', 'Pub cache', 'IDEs & Editors',
                  ['~/.pub-cache/hosted/*/*', '~/.pub-cache/git/*', '~/.pub-cache/_temp/*'],
                  'flutter', 'dart'),
    ),
    # ── Apps ─────────────────────────────────────────────────────────────────
    *_collect(
        _electron_task('slack',     'Slack',          'Apps', 'Slack', snap='slack', flatpak='com.slack.Slack'),
        _electron_task('discord',   'Discord',        'Apps', 'discord', snap='discord', flatpak='com.discordapp.Discord'),
        _electron_task('signal',    'Signal',         'Apps', 'Signal', snap='signal-desktop', flatpak='org.signal.Signal'),
        _electron_task('obsidian',  'Obsidian',       'Apps', 'obsidian', snap='obsidian', flatpak='md.obsidian.Obsidian'),
        _electron_task('postman',   'Postman',        'Apps', 'Postman', snap='postman'),
        _electron_task('insomnia',  'Insomnia',       'Apps', 'Insomnia', snap='insomnia'),
        _electron_task('claudeapp', 'Claude',         'Apps', 'Claude'),
        _electron_task('ghdesktop', 'GitHub Desktop', 'Apps', 'GitHub Desktop'),
        _electron_task('teams',     'Teams',          'Apps', 'teams-for-linux', snap='teams-for-linux'),
        # Media caches: both apps re-download whatever they need on demand.
        _app_task('telegram', 'Telegram', 'Apps',
                  ['~/.local/share/TelegramDesktop', _snap_root('telegram-desktop', '.local/share/TelegramDesktop'),
                   _flatpak_root('org.telegram.desktop', 'data/TelegramDesktop')],
                  lambda r: [f'{r}/tdata/user_data*/cache/*', f'{r}/tdata/user_data*/media_cache/*']),
        _app_task('spotify', 'Spotify', 'Apps',
                  ['~/.cache/spotify', _snap_root('spotify', '.cache/spotify'),
                   _flatpak_root('com.spotify.Client', 'cache/spotify')],
                  lambda r: [f'{r}/Data/*', f'{r}/Storage/*']),
        _app_task('steam', 'Steam', 'Apps',
                  ['~/.local/share/Steam', _flatpak_root('com.valvesoftware.Steam', '.local/share/Steam')],
                  lambda r: [f'{r}/appcache/httpcache/*', f'{r}/depotcache/*', f'{r}/logs/*']),
        _dir_task('dropbox', 'Dropbox', 'Apps', ['~/Dropbox/.dropbox.cache/*'], '~/Dropbox/.dropbox.cache'),
        _app_task('libreoffice', 'LibreOffice', 'Apps',
                  ['~/.config/libreoffice', _snap_root('libreoffice', '.config/libreoffice'),
                   _flatpak_root('org.libreoffice.LibreOffice', 'config/libreoffice')],
                  lambda r: [f'{r}/4/cache/*']),
    ),
]

# ── Theme ────────────────────────────────────────────────────────────────────
# gruvbox dark, medium contrast (github.com/morhetz/gruvbox). Kept as constants
# so the row icons use the same palette as the CSS instead of falling back to
# whatever "green" and "yellow" mean in the host terminal.
GB = {
    'bg0':    '#282828',
    'bg1':    '#3c3836',
    'bg2':    '#504945',
    'bg3':    '#665c54',
    'fg0':    '#fbf1c7',
    'fg1':    '#ebdbb2',
    'fg4':    '#a89984',
    'gray':   '#928374',
    'red':    '#fb4934',
    'green':  '#b8bb26',
    'yellow': '#fabd2f',
    'blue':   '#83a598',
    'purple': '#d3869b',
    'aqua':   '#8ec07c',
    'orange': '#fe8019',
}

GRUVBOX = Theme(
    name='gruvbox-dark',
    primary=GB['blue'],
    secondary=GB['aqua'],
    accent=GB['purple'],
    warning=GB['yellow'],
    error=GB['red'],
    success=GB['green'],
    foreground=GB['fg1'],
    background=GB['bg0'],
    surface=GB['bg1'],
    panel=GB['bg2'],
    dark=True,
    variables={
        'text-muted': GB['fg4'],
        'text-disabled': GB['gray'],
        'border': GB['bg2'],
        'border-blurred': GB['bg1'],
        'block-cursor-foreground': GB['bg0'],
        'block-cursor-background': GB['yellow'],
        'input-selection-background': GB['blue'] + '40',
        'footer-key-foreground': GB['orange'],
        'footer-description-foreground': GB['fg4'],
        'footer-background': GB['bg1'],
        'scrollbar': GB['bg2'],
        'scrollbar-hover': GB['bg3'],
        'scrollbar-active': GB['orange'],
        'scrollbar-background': GB['bg0'],
    },
)

# Status glyphs, coloured from the same palette.
ICON_IDLE = f"[{GB['gray']}]○[/]"
ICON_RUN = f"[{GB['yellow']}]↻[/]"
ICON_DONE = f"[{GB['green']}]✓[/]"
ICON_WARN = f"[{GB['orange']}]⚠[/]"
ICON_FAIL = f"[{GB['red']}]✗[/]"
ICON_SKIP = f"[{GB['gray']}]—[/]"

CSS = '''
Screen {
    background: $background;
    color: $foreground;
}

#overall-section {
    height: 5;
    padding: 1 2 0 2;
}

#overall-bar {
    width: 100%;
    height: 1;
}

#overall-bar Bar > .bar--bar {
    color: $success;
}

#overall-bar Bar > .bar--complete {
    color: $success;
}

#overall-bar PercentageStatus {
    color: $accent;
}

#totals-label {
    color: $text-muted;
    margin-top: 0;
}

#task-scroll {
    height: 1fr;
    border: round $panel;
    border-title-color: $primary;
    background: $surface;
    margin: 1 2;
}

.subcat-header {
    background: $panel;
    color: $accent;
    text-style: bold;
    width: 100%;
    padding: 0 1;
    margin-top: 1;
}

.subcat-header:first-of-type {
    margin-top: 0;
}

.task-row {
    height: 1;
    padding: 0 1;
}

.task-icon {
    width: 3;
}

.task-name {
    width: 28;
    overflow: hidden hidden;
    color: $foreground;
}

.task-bar {
    display: none;
    width: 1fr;
    height: 1;
}

.task-bar Bar > .bar--indeterminate {
    color: $warning;
    background: $panel;
}

.task-bar Bar > .bar--bar {
    color: $primary;
    background: $panel;
}

.task-result {
    width: 1fr;
    text-align: right;
    color: $text-muted;
}

#log {
    height: 10;
    border: round $panel;
    background: $surface;
    margin: 0 2 1 2;
}

Header {
    background: $panel;
    color: $accent;
}

Footer {
    background: $panel;
}
'''

class TidyApp(App[None]):
    CSS = CSS
    TITLE = 'tidy'
    BINDINGS = [('q', 'quit', 'Quit')]

    def __init__(self) -> None:
        super().__init__()
        self._total = len(ALL_TASKS)
        self._completed = 0
        self._total_freed = 0
        self._total_files = 0
        self._total_pkgs = 0
        self._started = time.strftime('%Y-%m-%dT%H:%M:%S')
        self._disk_before = _disk_free()
        self._records: list[dict] = []
        self._locks = _LockSet()
        self._stream = _open_run_log()

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id='overall-section'):
            yield ProgressBar(total=self._total, show_percentage=True, show_eta=False,
                              id='overall-bar')
            yield Label(f'0 / {self._total} tasks done', id='totals-label')
        with ScrollableContainer(id='task-scroll'):
            subcats: dict[str, list[Task]] = {}
            for t in ALL_TASKS:
                subcats.setdefault(t.subcategory, []).append(t)
            for subcat, tasks in subcats.items():
                yield Label(subcat, classes='subcat-header')
                for t in tasks:
                    with Horizontal(classes='task-row'):
                        yield Label(ICON_IDLE, id=f'icon_{t.id}', classes='task-icon')
                        yield Label(t.name, classes='task-name')
                        yield ProgressBar(total=None, show_percentage=False, show_eta=False,
                                          id=f'bar_{t.id}', classes='task-bar')
                        yield Label('', id=f'result_{t.id}', classes='task-result')
        # max_lines caps memory: ~40 tasks streaming build output adds up fast.
        yield RichLog(id='log', highlight=True, markup=False, wrap=True, max_lines=2000)
        yield Footer()

    def on_unmount(self) -> None:
        # Quitting mid-run still leaves what ran on disk, which is the point.
        if self._stream:
            self._stream.close()
            self._stream = None

    def on_mount(self) -> None:
        self.register_theme(GRUVBOX)
        self.theme = GRUVBOX.name
        if DRY_RUN:
            self.query_one('#log', RichLog).write(
                'DRY RUN — nothing will be deleted and no commands will run')
        self.run_all()

    def _summary_parts(self) -> list[str]:
        parts = []
        if self._total_pkgs:
            parts.append(f'{self._total_pkgs:,} libs upgraded')
        if self._total_files:
            parts.append(f'{self._total_files:,} files removed')
        if self._total_freed:
            parts.append(f'{_size_str(self._total_freed)} freed')
        return parts

    def _refresh_totals(self) -> None:
        parts = [f'{self._completed} / {self._total} tasks done', *self._summary_parts()]
        self.query_one('#totals-label', Label).update('  ·  '.join(parts))
        pkgs = f'  ·  {self._total_pkgs} libs' if self._total_pkgs else ''
        prefix = 'Dry run' if DRY_RUN else 'tidy'
        self.title = f'{prefix}  ·  {self._completed} / {self._total}{pkgs}'

    @work
    async def run_all(self) -> None:
        log_widget = self.query_one('#log', RichLog)
        overall = self.query_one('#overall-bar', ProgressBar)

        async def run_one(t: Task) -> None:
            icon = self.query_one(f'#icon_{t.id}', Label)
            bar = self.query_one(f'#bar_{t.id}', ProgressBar)
            result_lbl = self.query_one(f'#result_{t.id}', Label)

            def task_log(msg: str) -> None:
                log_widget.write(f'{t.name}: {msg}')
                if self._stream:
                    self._stream.write(f'{t.name}: {msg}\n')

            def on_start() -> None:  # only once the task actually holds its locks
                icon.update(ICON_RUN)
                bar.display = True
                result_lbl.display = False

            try:
                res, elapsed, status = await _execute(t, self._locks, task_log, on_start)
                self._records.append(_record(t, res, elapsed, status))
                self._total_freed += res.freed
                self._total_files += res.files
                self._total_pkgs += res.pkgs
                suffix = f' · {elapsed:.0f}s' if elapsed >= 1 and not res.skipped else ''
                result_lbl.update(res.label() + suffix)
                icon.update(ICON_SKIP if status == 'skipped'
                            else ICON_DONE if status == 'ok'
                            else ICON_WARN if status == 'failed'
                            else ICON_FAIL)
            finally:
                bar.display = False
                result_lbl.display = True
                self._completed += 1
                overall.advance(1)
                self._refresh_totals()

        await asyncio.gather(*(run_one(t) for t in ALL_TASKS), return_exceptions=True)

        if self._completed < self._total:  # 100% even if something died unexpectedly
            overall.advance(self._total - self._completed)
            self._completed = self._total
        self._refresh_totals()

        totals = _totals(self._records, self._started,
                         max(0, _disk_free() - self._disk_before))
        summary = _summary_line(totals)
        done = 'Dry run done' if DRY_RUN else 'Done ✓'
        self.title = f'{done}  ·  {summary}' if summary else done
        log_widget.write(f'\n{done}' + (f'  ·  {summary}' if summary else ''))
        if self._stream:
            self._stream.close()
            self._stream = None
        if report := _write_report(self._records, totals):
            log_widget.write(f'report: {report}')
        await _notify(summary)


# ── Reporting ────────────────────────────────────────────────────────────────
def _open_run_log() -> TextIO | None:
    '''Tee task output to a file both front ends share.

    The TUI's RichLog is capped and dies with the app, so a failure you only
    watched scroll past used to leave nothing behind to read afterwards.
    '''
    with contextlib.suppress(OSError):
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        return (REPORT_DIR / 'last-run.log').open('w')
    except OSError:
        return None


def _write_report(records: list[dict], totals: dict) -> Path | None:
    '''Persist one run as JSON. The TUI log is capped and dies with the app;
    this is what lets you compare a run against the last one.'''
    if not WRITE_REPORT:
        return None
    payload = {
        'started': totals['started'],
        'finished': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'dry_run': DRY_RUN,
        'totals': totals,
        'tasks': records,
    }
    try:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime('%Y%m%d-%H%M%S')
        path = REPORT_DIR / f'{stamp}{"-dryrun" if DRY_RUN else ""}.json'
        text = json.dumps(payload, indent=2)
        path.write_text(text)
        (REPORT_DIR / 'last-run.json').write_text(text)
        # Keep the directory from growing without bound.
        runs = sorted(REPORT_DIR.glob('20*.json'))
        for stale in runs[:-KEEP_REPORTS]:
            with contextlib.suppress(OSError):
                stale.unlink()
        return path
    except OSError:
        return None


def _totals(records: list[dict], started: str, freed_disk: int) -> dict:
    return {
        'started': started,
        'tasks': len(records),
        'failed': sum(1 for r in records if r['status'] in ('failed', 'error', 'timeout')),
        'skipped': sum(1 for r in records if r['status'] == 'skipped'),
        'freed': sum(r['freed'] for r in records),
        'files': sum(r['files'] for r in records),
        'pkgs': sum(r['pkgs'] for r in records),
        # A dry run deletes nothing, so any statvfs delta is other processes
        # moving the needle — reporting it would be a lie.
        'disk_freed': 0 if DRY_RUN else freed_disk,
    }


def _record(task: Task, res: Result, elapsed: float, status: str) -> dict:
    rec = {
        'id': task.id, 'name': task.name, 'subcategory': task.subcategory,
        'status': status, 'freed': res.freed, 'files': res.files,
        'pkgs': res.pkgs, 'note': res.note, 'seconds': round(elapsed, 1),
    }
    # Only a failure carries detail; every other record stays as terse as before.
    if res.failed_cmd:
        rec['failed_cmd'] = res.failed_cmd
        rec['failed_tail'] = res.failed_tail
    return rec


def _summary_line(totals: dict) -> str:
    parts = []
    if totals['pkgs']:
        parts.append(f'{totals["pkgs"]:,} libs upgraded')
    if totals['files']:
        parts.append(f'{totals["files"]:,} files removed')
    if totals['freed']:
        parts.append(f'{_size_str(totals["freed"])} freed')
    if totals['disk_freed']:
        parts.append(f'{_size_str(totals["disk_freed"])} disk reclaimed')
    return '  ·  '.join(parts)


async def _notify(text: str) -> None:
    '''Desktop notification, when there is a desktop session to show it in.'''
    if not _has('notify-send') or not os.environ.get('DBUS_SESSION_BUS_ADDRESS'):
        return
    title = 'tidy' + (' (dry run)' if DRY_RUN else '')
    # Directly, not via _run_cmd: a dry run should still tell you it finished.
    with contextlib.suppress(Exception):
        proc = await asyncio.create_subprocess_exec(
            'notify-send', '-a', 'tidy', title, text or 'Finished',
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await proc.wait()


# ── Headless runner ──────────────────────────────────────────────────────────
_STATUS_MARK = {'ok': '\u2713', 'skipped': '\u2014', 'failed': '\u26a0',
                'error': '\u2717', 'timeout': '\u2717'}


async def run_headless() -> int:
    '''Same tasks, no TUI: one line per task on stdout, detail to a log file.

    This is what the systemd timer runs — a Textual app needs a terminal.
    '''
    started = time.strftime('%Y-%m-%dT%H:%M:%S')
    before = _disk_free()
    locks = _LockSet()
    records: list[dict] = []
    stream = _open_run_log()

    print(f'tidy · {len(ALL_TASKS)} tasks'
          + ('  ·  DRY RUN' if DRY_RUN else ''), flush=True)

    async def run_one(task: Task) -> None:
        def task_log(msg: str) -> None:
            if stream:
                stream.write(f'{task.name}: {msg}\n')

        res, elapsed, status = await _execute(task, locks, task_log)
        records.append(_record(task, res, elapsed, status))
        detail = res.label()
        suffix = f' · {elapsed:.0f}s' if elapsed >= 1 and not res.skipped else ''
        print(f'  {_STATUS_MARK.get(status, "?")} {task.name:<24} {detail}{suffix}',
              flush=True)

    try:
        await asyncio.gather(*(run_one(t) for t in ALL_TASKS), return_exceptions=True)
    finally:
        if stream:
            stream.close()

    totals = _totals(records, started, max(0, _disk_free() - before))
    summary = _summary_line(totals)
    print('\nDone' + (f'  ·  {summary}' if summary else ''))
    if report := _write_report(records, totals):
        print(f'report: {report}')
    await _notify(summary)
    return 1 if totals['failed'] else 0


# ── systemd user timer ───────────────────────────────────────────────────────
AGENT_UNIT = 'tidy'
_SERVICE = '''[Unit]
Description=tidy — weekly upgrade + cleanup

[Service]
Type=oneshot
ExecStart={python} {script} --headless
Nice=5
IOSchedulingClass=idle
StandardOutput=append:{logdir}/agent.out.log
StandardError=append:{logdir}/agent.err.log
'''
_TIMER = '''[Unit]
Description=Run tidy every Sunday at 11:00

[Timer]
OnCalendar=Sun 11:00
Persistent=true

[Install]
WantedBy=timers.target
'''


def _install_agent() -> None:
    '''Write a weekly systemd user timer. Deliberately does not enable it —
    that is the user's call, and the command to do it is printed below.'''
    unit_dir = HOME / '.config/systemd/user'
    unit_dir.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    service = unit_dir / f'{AGENT_UNIT}.service'
    timer = unit_dir / f'{AGENT_UNIT}.timer'
    # A zipapp's __file__ points inside the archive; sys.argv[0] is the archive.
    script = Path(sys.argv[0]).resolve()
    service.write_text(_SERVICE.format(python=sys.executable, script=script,
                                       logdir=REPORT_DIR))
    timer.write_text(_TIMER)
    print(f'wrote {service}\nwrote {timer}')
    print('\nIt is not enabled yet. To enable the Sunday 11:00 run:')
    print(f'  systemctl --user daemon-reload && systemctl --user enable --now {AGENT_UNIT}.timer')
    print('To disable it again:')
    print(f'  systemctl --user disable --now {AGENT_UNIT}.timer')
    print('Root-only tasks (apt, snap, journal) skip under the timer unless sudo needs no password.')


def _authenticate() -> bool:
    '''Settle SUDO_OK once, before any task starts.

    Children run with stdin on /dev/null and can never prompt, so the one
    chance to type a password is here, in the foreground. sudo caches the
    ticket per terminal session, which the sudo children stay in.
    '''
    if os.geteuid() == 0:
        return True
    if not _has('sudo'):
        return False
    quiet = {'stdin': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL,
             'stderr': subprocess.DEVNULL}
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        if subprocess.run(['sudo', '-n', 'true'], timeout=10, **quiet).returncode == 0:
            return True
    if not sys.stdin.isatty():
        return False
    print('tidy: apt, snap and the journal need root — sudo asks once, '
          'or Ctrl-C to skip them.')
    try:
        return subprocess.run(['sudo', '-v']).returncode == 0
    except KeyboardInterrupt:
        print()
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description='tidy — Linux upgrade + cleanup.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run', action='store_true',
                        help='report what would be freed; delete nothing, run no commands')
    parser.add_argument('--headless', action='store_true',
                        help='no TUI, one line per task (for systemd/cron)')
    parser.add_argument('--no-sudo', action='store_true',
                        help='skip every root-only task instead of asking for a password')
    parser.add_argument('--no-report', action='store_true',
                        help='do not write a JSON run report')
    parser.add_argument('--install-agent', action='store_true',
                        help='write a weekly systemd user timer and exit')
    args = parser.parse_args()

    global DRY_RUN, WRITE_REPORT, SUDO_OK
    DRY_RUN = args.dry_run
    WRITE_REPORT = not args.no_report

    if args.install_agent:
        _install_agent()
        return
    if not DRY_RUN and not args.no_sudo:
        SUDO_OK = _authenticate()

    try:
        if args.headless:
            raise SystemExit(asyncio.run(run_headless()))
        TidyApp().run()
    finally:
        _IO_POOL.shutdown(wait=False, cancel_futures=True)


if __name__ == '__main__':
    main()
