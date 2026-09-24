#!/usr/bin/env python3
'''tidymac — macOS upgrade + cleanup TUI. Runs everything in parallel with per-task progress bars.

  --dry-run        size everything up, delete nothing, run no commands
  --headless       one line per task instead of the TUI (for launchd/cron)
  --snapshots      also thin Time Machine local snapshots (needs sudo)
  --no-report      skip the JSON run report
  --install-agent  write a weekly launchd agent and exit
  --apps           report installed apps by size, last use and ~/Library data
  --uninstall APP  move an app and everything it left in Library to the Trash
  --orphans        find Library data whose app is no longer installed
  --claude-history DAYS  also delete Claude Code transcripts older than DAYS
'''
from __future__ import annotations
import argparse, asyncio, contextlib, json, os, plistlib, re, shlex, signal, subprocess, sys, time
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
REPORT_DIR = HOME / '.local/state/tidymac'
KEEP_REPORTS = 20

# Bounded pool so ~40 filesystem walkers don't thrash the disk or starve
# asyncio's default executor (which subprocess plumbing also uses).
_IO_POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix='fsclean')

# Directories never worth descending into during whole-home sweeps: either
# enormous (package caches, VCS metadata) or handled by a dedicated task.
_WALK_SKIP = frozenset({
    '.Trash', '.bun', '.bundle', '.cache', '.cargo', '.git', '.gradle', '.hg',
    '.m2', '.mypy_cache', '.npm', '.nvm', '.pub-cache', '.pyenv', '.pytest_cache',
    '.rustup', '.rvm', '.svn', '.tox', '.venv', 'DerivedData', 'Library',
    'Pods', '__pycache__', 'node_modules', 'venv',
})

# Regenerable build/test droppings swept out of the whole home tree. Every one
# of these is rebuilt on the next run of the tool that made it.
# Signed bundles. Deleting anything inside one breaks its seal, and real
# .app/.bundle payloads (updaters, helpers) live under Application Support.
_BUNDLE_SUFFIXES = ('.app', '.bundle', '.framework', '.plugin', '.kext')

_CRUFT_DIRS = frozenset({
    '.ipynb_checkpoints', '.mypy_cache', '.pytest_cache', '.ruff_cache',
    '__pycache__',
})

LOG_TIMEOUT = 600.0   # filesystem tasks
CMD_TIMEOUT = 1800.0  # ordinary tool upgrades
SLOW_TIMEOUT = 3600.0 # brew / macOS updates

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
    'TERM': 'dumb',
}


def _disk_free(path: str = '/') -> int:
    '''Free bytes on the volume holding path.

    The per-file byte tally can't see APFS compression, clones or purgeable
    space, so the delta across a whole run is the honest number — even if other
    processes nudge it while we work.
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
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=_CHILD_ENV,
            start_new_session=True,
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


async def _clean_old_logs(log: Callable[[str], None]) -> Result:
    cutoff = time.time() - 2592000  # 30 days

    # /var/log is root-owned: without sudo every unlink there just EPERMs, so
    # the walk was pure cost. Include it only when we can actually act on it.
    roots = [HOME / 'Library/Logs']
    if os.geteuid() == 0:
        roots.append(Path('/var/log'))

    def _do() -> tuple[int, int]:
        freed = count = 0
        for base in roots:
            if not base.is_dir():
                continue
            for root, _, files in os.walk(base):
                for fname in files:
                    path = os.path.join(root, fname)
                    try:
                        st = os.lstat(path)  # one stat, not two
                        if st.st_mtime < cutoff:
                            if not DRY_RUN:
                                os.unlink(path)
                            freed += st.st_size
                            count += 1
                    except OSError:
                        continue
        return freed, count

    freed, count = await _to_thread(_do)
    if freed or count:
        log(f'freed {_size_str(freed)} · {count:,} files')
    return Result(freed=freed, files=count)


async def _clean_lang_packs(log: Callable[[str], None]) -> Result:
    keep = {'en.lproj', 'en_GB.lproj', 'Base.lproj', 'en_US.lproj'}
    base = HOME / 'Library/Application Support'

    def _do() -> tuple[int, int]:
        freed = count = 0
        if not base.is_dir():
            return freed, count
        for root, dirnames, _ in os.walk(base):
            # Bundles first: their Resources hold .lproj we must not touch.
            dirnames[:] = [d for d in dirnames if not d.endswith(_BUNDLE_SUFFIXES)]
            targets = [d for d in dirnames if d.endswith('.lproj') and d not in keep]
            # Prune: purged trees are gone, kept ones hold no nested .lproj.
            dirnames[:] = [d for d in dirnames if not d.endswith('.lproj')]
            for name in targets:
                f, c = _purge_tree(os.path.join(root, name))
                freed += f
                count += c
        return freed, count

    freed, count = await _to_thread(_do)
    if freed or count:
        log(f'freed {_size_str(freed)} · {count:,} files')
    return Result(freed=freed, files=count)


async def _empty_trash(log: Callable[[str], None]) -> Result:
    roots = [HOME / '.Trash']
    try:
        for vol in Path('/Volumes').iterdir():
            trash = vol / '.Trashes' / str(UID)
            if trash.is_dir():
                roots.append(trash)
    except OSError:
        pass
    return await _clean_paths([str(r / '*') for r in roots], log)


async def _home_sweep(log: Callable[[str], None]) -> Result:
    '''One pass over $HOME for .DS_Store droppings and regenerable build cruft.

    These used to be two tasks walking the same tree concurrently, which
    doubled the slowest part of the whole run for no benefit.
    '''
    def _do() -> tuple[int, int]:
        freed = count = 0
        for root, dirnames, files in os.walk(HOME):
            targets = [d for d in dirnames if d in _CRUFT_DIRS]
            # Prune caches/VCS/bundles — walking node_modules and Library
            # dominated the runtime — and the cruft dirs themselves, which are
            # about to be purged wholesale. Notably venvs stay skipped: their
            # __pycache__ belongs to the venv, not to us.
            dirnames[:] = [
                d for d in dirnames
                if d not in _WALK_SKIP and d not in _CRUFT_DIRS
                and not d.endswith(_BUNDLE_SUFFIXES)
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


async def _container_prune(tool: str, log: Callable[[str], None]) -> Result:
    '''`<tool> system prune -f` for docker or podman, crediting what it reports.

    Both print the same "Total reclaimed space:" line. `info` doubles as the
    liveness probe: with no daemon (or no podman machine) up, prune would
    just fail.
    '''
    if await _run_cmd([tool, 'info'], lambda _: None) != 0:
        log(f'{tool} is not running, skipping')
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

    await _run_cmd([tool, 'system', 'prune', '-f'], _log)
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


# ── Guarded sweeps & report-only scans ───────────────────────────────────────
_DAY = 86400.0
# --claude-history DAYS. Zero leaves Claude Code's session transcripts alone:
# they are --resume history, not cache.
CLAUDE_HISTORY_DAYS = 0


async def _in_pool(fn: Callable):
    return await asyncio.get_running_loop().run_in_executor(_IO_POOL, fn)


def _newest_use(root: str, stop: float) -> float:
    '''Latest mtime/atime anywhere under root, bailing out once past stop.

    A directory's own mtime only moves when a direct child is added or
    removed, so a cache busy three levels down still looks untouched from the
    top. atime counts too: a model or video that is read but never rewritten
    is still in use.
    '''
    newest = 0.0
    stack = [root]
    while stack:
        cur = stack.pop()
        try:
            st = os.lstat(cur)
        except OSError:
            continue
        newest = max(newest, st.st_mtime, st.st_atime)
        if newest > stop:
            return newest
        if os.path.isdir(cur) and not os.path.islink(cur):
            with contextlib.suppress(OSError), os.scandir(cur) as it:
                stack.extend(e.path for e in it)
    return newest


def _dir_bytes(root: str) -> int:
    total = 0
    for dirpath, _, files in os.walk(root):
        for name in files:
            with contextlib.suppress(OSError):
                total += os.lstat(os.path.join(dirpath, name)).st_size
    return total


def _found(items: list[tuple[int, str]], log: Callable[[str], None],
           what: str, show: int = 10) -> Result:
    '''Result for a report-only scan: log the biggest hits, delete nothing.'''
    items.sort(reverse=True)
    for size, label in items[:show]:
        log(f'{_size_str(size):>9}  {label}')
    if len(items) > show:
        log(f'… and {len(items) - show} more')
    if not items:
        return Result(note='nothing found')
    total = sum(size for size, _ in items)
    return Result(note=f'{len(items)} {what} · {_size_str(total)} (not deleted)')


# Editor extension roots → the app whose running process rewrites .obsolete.
_EDITOR_EXT_DIRS = (
    ('~/.vscode/extensions', 'Visual Studio Code.app'),
    ('~/.vscode-insiders/extensions', 'Visual Studio Code - Insiders.app'),
    ('~/.vscode-oss/extensions', 'VSCodium.app'),
    ('~/.cursor/extensions', 'Cursor.app'),
    ('~/.windsurf/extensions', 'Windsurf.app'),
)


def _app_running(bundle: str) -> bool:
    import subprocess
    try:
        return subprocess.run(['pgrep', '-f', f'/{bundle}/'], capture_output=True,
                              timeout=10).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return True  # can't tell: assume it is, and leave its files alone


async def _clean_obsolete_extensions(log: Callable[[str], None]) -> Result:
    '''Extension versions VS Code and its forks have themselves marked obsolete.

    Each update leaves the old folder behind, listed in extensions/.obsolete
    for deletion at some later start that often never comes. Only names in
    that list go — guessing "older" from version strings would also hit
    side-by-side builds the editor still loads — and never while the editor
    runs, since it rewrites that file on exit.
    '''
    def _do() -> tuple[int, int, list[str]]:
        freed = count = 0
        notes: list[str] = []
        for root, bundle in _EDITOR_EXT_DIRS:
            base = _expand(root)
            try:
                names = json.loads((base / '.obsolete').read_text())
            except (OSError, ValueError):
                continue
            if not isinstance(names, dict):
                continue
            if _app_running(bundle):
                notes.append(f'{bundle} is running, skipped {root}')
                continue
            for name in names:
                target = base / name
                # Keys are bare folder names; anything else is not ours to follow.
                if '/' in name or name.startswith('.') or target.is_symlink() \
                        or not target.is_dir():
                    continue
                f, c = _purge_tree(str(target))
                freed += f
                count += c
                notes.append(f'{root}/{name}')
        return freed, count, notes

    freed, count, notes = await _in_pool(_do)
    for line in notes:
        log(line)
    return Result(freed=freed, files=count)


_WALLPAPER = HOME / 'Library/Application Support/com.apple.wallpaper'


async def _clean_aerials(log: Callable[[str], None]) -> Result:
    '''Downloaded aerial videos (~500 MB each) no wallpaper or screen saver uses.

    Kept: any video whose asset ID the wallpaper store mentions, and any
    played in the last 30 days, which covers "default" choices that name no
    asset. If the store can't be read, nothing goes. macOS downloads an
    aerial again whenever it is next picked.
    '''
    def _do() -> tuple[int, int, list[str]]:
        store = _WALLPAPER / 'Store/Index.plist'
        try:
            # Raw bytes on purpose: the asset IDs sit inside nested binary
            # plists, and a substring test survives format changes that a
            # structural walk would not.
            referenced = store.read_bytes().upper()
        except OSError:
            return 0, 0, ['wallpaper store unreadable, keeping every aerial']
        cutoff = time.time() - 30 * _DAY
        freed = count = 0
        kept: list[str] = []
        for video in (_WALLPAPER / 'aerials/videos').glob('*.mov'):
            try:
                st = video.lstat()
            except OSError:
                continue
            if video.stem.upper().encode() in referenced:
                kept.append(f'kept {video.name} (selected)')
                continue
            if max(st.st_atime, st.st_mtime) > cutoff:
                kept.append(f'kept {video.name} (played recently)')
                continue
            f, c = _remove_entry(video)
            freed += f
            count += c
        return freed, count, kept

    freed, count, notes = await _in_pool(_do)
    for line in notes:
        log(line)
    return Result(freed=freed, files=count)


# ~/.cache entries a dedicated task already handles (some deliberately gently:
# uv is pruned, never wiped), plus model stores — downloaded weights are data
# that costs gigabytes to fetch again, not cache.
_DOT_CACHE_KEEP = frozenset({
    'esbuild', 'gh', 'huggingface', 'lm-studio', 'nvim', 'nx', 'ollama',
    'org.swift.swiftpm', 'pre-commit', 'puppeteer', 'torch', 'uv', 'vite',
    'webpack', 'whisper', 'yt-dlp', 'zed', 'zig',
})


async def _clean_stale_dot_cache(log: Callable[[str], None]) -> Result:
    '''~/.cache directories nothing has touched in 30 days, judged deep.'''
    def _do() -> tuple[int, int, list[str]]:
        base = HOME / '.cache'
        cutoff = time.time() - 30 * _DAY
        freed = count = 0
        gone: list[str] = []
        entries: list[os.DirEntry] = []
        with contextlib.suppress(OSError), os.scandir(base) as it:
            entries = [e for e in it if e.name not in _DOT_CACHE_KEEP
                       and e.is_dir(follow_symlinks=False)]
        for e in entries:
            if _newest_use(e.path, cutoff) > cutoff:
                continue
            f, c = _purge_tree(e.path)
            freed += f
            count += c
            gone.append(f'{e.name}: {_size_str(f)}')
        return freed, count, gone

    freed, count, notes = await _in_pool(_do)
    for line in notes:
        log(line)
    return Result(freed=freed, files=count)


_CLAUDE = HOME / '.claude'
_RE_SESSION = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')


def _claude_recent_sessions(cutoff: float) -> set[str]:
    '''IDs of sessions whose transcript moved since cutoff — possibly live.'''
    recent = set()
    for jsonl in (_CLAUDE / 'projects').glob('*/*.jsonl'):
        with contextlib.suppress(OSError):
            if jsonl.stat().st_mtime > cutoff:
                recent.add(jsonl.stem)
    return recent


async def _clean_claude_state(log: Callable[[str], None]) -> Result:
    '''Claude Code's per-session scratch: debug logs, shell snapshots, paste
    cache, todos and /rewind file history.

    Live sessions read these, so anything tied to a session whose transcript
    moved in the last 30 days stays, and the rest must itself be 30 days old.
    '''
    def _do() -> tuple[int, int]:
        cutoff = time.time() - 30 * _DAY
        recent = _claude_recent_sessions(cutoff)
        freed = count = 0
        for sub in ('debug', 'file-history', 'paste-cache', 'shell-snapshots', 'todos'):
            with contextlib.suppress(OSError), os.scandir(_CLAUDE / sub) as it:
                for e in list(it):
                    m = _RE_SESSION.match(e.name)
                    if m and m.group(0) in recent:
                        continue
                    if _newest_use(e.path, cutoff) > cutoff:
                        continue
                    f, c = _remove_entry(Path(e.path))
                    freed += f
                    count += c
        return freed, count

    freed, count = await _to_thread(_do)
    if freed or count:
        log(f'freed {_size_str(freed)} · {count:,} files')
    return Result(freed=freed, files=count)


async def _clean_claude_history(log: Callable[[str], None]) -> Result:
    '''Session transcripts (--resume history) older than --claude-history days.

    Only session-named entries go — each project's memory/ directory stays.
    Claude Code has its own knob for this too: cleanupPeriodDays in
    ~/.claude/settings.json.
    '''
    def _do() -> tuple[int, int]:
        cutoff = time.time() - CLAUDE_HISTORY_DAYS * _DAY
        freed = count = 0
        for entry in (_CLAUDE / 'projects').glob('*/*'):
            if not _RE_SESSION.match(entry.name):
                continue
            if _newest_use(str(entry), cutoff) > cutoff:
                continue
            f, c = _remove_entry(entry)
            freed += f
            count += c
        return freed, count

    freed, count = await _to_thread(_do)
    if freed or count:
        log(f'freed {_size_str(freed)} · {count:,} files')
    return Result(freed=freed, files=count)


def _claude_history_task() -> Task:
    return Task('claudehist', f'Claude Transcripts ({CLAUDE_HISTORY_DAYS}d+)',
                'Dev — Other', _clean_claude_history)


async def _sim_runtimes(log: Callable[[str], None]) -> Result:
    '''Unavailable simulators, then runtime images unused for 90 days.'''
    res = await _simple_cmds([['xcrun', 'simctl', 'delete', 'unavailable']], log)
    quiet = False

    def _log(msg: str) -> None:
        nonlocal quiet
        log(msg)
        quiet |= 'no matching images' in msg.lower()

    rc = await _run_cmd(['xcrun', 'simctl', 'runtime', 'delete',
                         '--notUsedSinceDays', '90'], _log)
    # Exit 2 with "No matching images" just means nothing was old enough.
    if rc and not (rc == 2 and quiet):
        res.failed = True
        res.note = res.note or 'failed: simctl runtime delete'
    return res


def _self_updating(tool: str) -> bool:
    '''True if tool is on PATH and did not come from Homebrew.

    The Homebrew task already upgrades brew-installed copies, and a
    self-update would overwrite a file brew owns and thinks it knows.
    '''
    import shutil
    path = shutil.which(tool)
    if not path:
        return False
    real = os.path.realpath(path)
    return '/Cellar/' not in real and '/Caskroom/' not in real


def _editor_clis() -> list[list[str]]:
    '''`<editor> --update-extensions` for each installed VS Code-family app.'''
    cmds = []
    for app, cli in (('Visual Studio Code.app', 'code'), ('Cursor.app', 'cursor'),
                     ('Windsurf.app', 'windsurf'), ('VSCodium.app', 'codium')):
        for d in _APP_DIRS:
            path = Path(d, app, 'Contents/Resources/app/bin', cli)
            if os.access(path, os.X_OK):
                cmds.append([str(path), '--update-extensions'])
                break
    return cmds


async def _report_node_modules(log: Callable[[str], None]) -> Result:
    '''node_modules in projects nobody has installed into for 90 days.'''
    def _do() -> list[tuple[int, str]]:
        cutoff = time.time() - 90 * _DAY
        hits = []
        skip = _WALK_SKIP - {'node_modules'}
        for root, dirnames, _ in os.walk(HOME):
            if 'node_modules' in dirnames:
                nm = os.path.join(root, 'node_modules')
                stamps = [nm] + [os.path.join(root, f) for f in
                                 ('package.json', 'package-lock.json',
                                  'pnpm-lock.yaml', 'yarn.lock', 'bun.lockb')]
                newest = 0.0
                for p in stamps:
                    with contextlib.suppress(OSError):
                        newest = max(newest, os.lstat(p).st_mtime)
                if newest and newest < cutoff:
                    hits.append((_dir_bytes(nm), root.replace(HOME_STR, '~', 1)))
            # Hidden dirs hold tools, not projects: editor extensions ship
            # their own node_modules, and those are the editor's business.
            dirnames[:] = [d for d in dirnames if d not in skip and not d.startswith('.')
                           and d != 'node_modules' and not d.endswith(_BUNDLE_SUFFIXES)]
        return hits

    return _found(await _in_pool(_do), log, 'stale node_modules')


async def _report_ios_backups(log: Callable[[str], None]) -> Result:
    '''iPhone/iPad backups. Never deleted: they may be the only copy.'''
    base = HOME / 'Library/Application Support/MobileSync/Backup'

    def _do() -> list[tuple[int, str]] | None:
        try:
            entries = list(base.iterdir())
        except PermissionError:
            return None
        except OSError:
            return []
        return [(_dir_bytes(str(e)),
                 f'{e.name[:12]}…  last backup {time.strftime("%Y-%m-%d", time.localtime(e.stat().st_mtime))}')
                for e in entries if e.is_dir()]

    items = await _in_pool(_do)
    if items is None:
        return Result(note='needs Full Disk Access to size')
    return _found(items, log, 'device backups')


async def _report_downloads(log: Callable[[str], None]) -> Result:
    '''Files over 500 MB in ~/Downloads untouched for 30 days.'''
    def _do() -> list[tuple[int, str]]:
        cutoff = time.time() - 30 * _DAY
        hits = []
        for root, dirnames, files in os.walk(HOME / 'Downloads'):
            dirnames[:] = [d for d in dirnames if not d.endswith(_BUNDLE_SUFFIXES)]
            for name in files:
                with contextlib.suppress(OSError):
                    st = os.lstat(os.path.join(root, name))
                    if st.st_size >= 500 << 20 and max(st.st_mtime, st.st_atime) < cutoff:
                        hits.append((st.st_size, os.path.join(root, name)
                                     .replace(HOME_STR, '~', 1)))
        return hits

    return _found(await _in_pool(_do), log, 'big old downloads')


async def _report_installers(log: Callable[[str], None]) -> Result:
    '''macOS installer apps. Often kept on purpose for bootable USB installers.'''
    def _do() -> list[tuple[int, str]]:
        return [(_dir_bytes(str(p)), str(p)) for d in _APP_DIRS
                for p in Path(d).glob('Install macOS *.app')]

    return _found(await _in_pool(_do), log, 'macOS installers')


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
# lock, and the blanket ~/Library/Caches sweep raced every per-app cache task,
# double-counting bytes and spraying ENOENT.
_PATH_LOCKS = (
    ('~/Library/Caches', 'lib-caches'),
    ('~/Library/Logs', 'lib-logs'),
    ('~/Library/Application Support', 'app-support'),
    ('~/.gradle', 'gradle'),
    ('~/.cargo', 'rust'),
    ('~/.rustup', 'rust'),
    ('~/go', 'go'),
    ('~/.gem', 'gem'),
    ('~/Library/Containers', 'containers'),
    ('~/Library/Group Containers', 'group-containers'),
    ('~/.cache', 'dot-cache'),
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


_APP_DIRS = ('/Applications', '/System/Applications', str(HOME / 'Applications'))


def _installed(app: str) -> bool:
    '''True if the bundle exists — user-installed apps also live in ~/Applications.'''
    bundle = Path(app).name
    return Path(app).exists() or any(Path(d, bundle).exists() for d in _APP_DIRS)


def _app_task(tid: str, name: str, subcat: str, paths: list[str], *apps: str) -> Task | None:
    '''Path task included only if one of the .app bundles is installed.'''
    return _path_task(tid, name, subcat, paths) if any(map(_installed, apps)) else None


def _dir_task(tid: str, name: str, subcat: str, paths: list[str], check: str) -> Task | None:
    '''Path task included only if the given directory exists.'''
    return _path_task(tid, name, subcat, paths) if _expand(check).exists() else None


def _chromium(support: str, cache: str) -> list[str]:
    '''Cache dirs of a Chromium-family browser, across every profile.

    Chromium splits its caches on macOS: the HTTP and code caches go to
    ~/Library/Caches/<vendor>/<browser>, everything else stays beside the
    profile in Application Support. The `*` matches Default / "Profile 1" /
    "System Profile" alike; a profile lacking a given cache just contributes
    no matches.
    '''
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


_RE_SNAPSHOT = re.compile(r'com\.apple\.TimeMachine\.([\d-]+)\.local')


async def _thin_snapshots(log: Callable[[str], None]) -> Result:
    '''Delete Time Machine's local snapshots — often the biggest single win.

    Opt-in (--snapshots) because it throws away the local restore points
    "Enter Time Machine" offers between backups.
    '''
    if os.geteuid() != 0 and not DRY_RUN:
        log('tmutil deletelocalsnapshots is root-only — re-run under sudo')
        return SKIPPED
    dates: list[str] = []

    def _collect_dates(msg: str) -> None:
        log(msg)
        if m := _RE_SNAPSHOT.search(msg):
            dates.append(m.group(1))

    await _run_cmd(['tmutil', 'listlocalsnapshots', '/'], _collect_dates)
    if not dates:
        return Result(note='none found')
    failures = 0
    for date in dates:
        if await _run_cmd(['tmutil', 'deletelocalsnapshots', date], log) != 0:
            failures += 1
    deleted = len(dates) - failures
    return Result(failed=bool(failures),
                  note=f'{deleted}/{len(dates)} snapshots deleted')


async def _snapshot_hint(totals: dict) -> str:
    '''Explain a run that freed gigabytes but left the free-space number flat.

    Blocks a local snapshot still references stay allocated after the files
    are deleted, so the disk only gives the space back once the snapshots age
    out (macOS thins them within about a day, or sooner under pressure).
    '''
    if DRY_RUN or totals['freed'] < 1073741824 or totals['disk_freed'] * 2 > totals['freed']:
        return ''
    dates: list[str] = []

    def _collect(msg: str) -> None:
        if m := _RE_SNAPSHOT.search(msg):
            dates.append(m.group(1))

    await _run_cmd(['tmutil', 'listlocalsnapshots', '/'], _collect)
    if not dates:
        return ''
    return (f'{len(dates)} Time Machine local snapshots still hold the freed space; '
            'macOS releases it within about a day, or re-run with --snapshots')


def _snapshots_task() -> Task:
    return Task('snapshots', 'TM Local Snapshots', 'System', _thin_snapshots,
                timeout=SLOW_TIMEOUT)


def _collect(*tasks: Task | None) -> list[Task]:
    return [t for t in tasks if t is not None]


# oh-my-zsh ships `omz` as a *shell function*, so shutil.which never finds it and
# the task silently vanished from the list. Drive the upgrade script directly.
_OMZ = HOME / '.oh-my-zsh/tools/upgrade.sh'

ALL_TASKS: list[Task] = [
    # ── Upgrades ─────────────────────────────────────────────────────────────
    *_collect(
        Task('brew', 'Homebrew', 'Upgrades', _brew_upgrade,
             timeout=SLOW_TIMEOUT, locks=('brew',)) if _has('brew') else None,
        _upgrade('omz', 'Oh My Zsh', [['zsh', '-f', str(_OMZ), '-v', 'minimal']],
                 matcher=_m_omz, require=('zsh',), when=_OMZ.is_file()),
        _upgrade('mas', 'Mac App Store', [['mas', 'upgrade']], matcher=_m_mas),
        _upgrade('python', 'Python tools',
                 [c for c in ([['pipupgrade', '-y', '-u']] if _has('pipupgrade') else [])
                  + ([['pipx', 'upgrade-all']] if _has('pipx') else [])
                  + ([['uv', 'tool', 'upgrade', '--all']] if _has('uv') else [])],
                 matcher=_m_python, require=('pipx', 'uv', 'pipupgrade'),
                 locks=('python',)),
        _upgrade('node', 'Node globals', [['npm', '-g', 'update']],
                 matcher=_m_npm, locks=('npm',)),
        _upgrade('rust', 'Rust', [['rustup', 'update']],
                 matcher=_m_rustup, locks=('rust',)),
        _upgrade('ruby', 'Ruby gems', [['gem', 'update', '--system'], ['gem', 'update']],
                 matcher=_m_gem, locks=('gem',)),
        _upgrade('macos', 'macOS updates', [['softwareupdate', '-ia']],
                 matcher=_m_swu, timeout=SLOW_TIMEOUT),
        # Self-updaters, only for copies Homebrew doesn't own. `claude update`
        # swaps the binary under running sessions; they keep the old one
        # until restarted, which is fine.
        _upgrade('claude', 'Claude Code CLI', [['claude', 'update']],
                 when=_self_updating('claude')),
        _upgrade('bunup', 'Bun', [['bun', 'upgrade']], when=_self_updating('bun')),
        _upgrade('denoup', 'Deno', [['deno', 'upgrade']], when=_self_updating('deno')),
        _upgrade('mise', 'mise tools',
                 [['mise', 'upgrade']]
                 + ([['mise', 'self-update', '--yes']] if _self_updating('mise') else []),
                 locks=('mise',)),
        _upgrade('asdf', 'asdf plugins', [['asdf', 'plugin', 'update', '--all']]),
        _upgrade('gcloud', 'gcloud components', [['gcloud', 'components', 'update', '--quiet']]),
        # Left out on purpose: `flutter upgrade` can break projects pinned to
        # a channel or version.
        _upgrade('editorext', 'Editor extensions', _editor_clis(),
                 require=tuple(c[0] for c in _editor_clis()),
                 when=bool(_editor_clis())),
    ),
    # ── System ───────────────────────────────────────────────────────────────
    _path_task('caches',     'User Caches',           'System', ['~/Library/Caches/*']),
    _path_task('crash',      'Crash Reports',          'System', ['~/Library/Application Support/CrashReporter/*']),
    # min_age: don't yank temp files out from under processes running right now.
    _path_task('tmp',        'Temp Dirs',              'System', ['/tmp/*', '/var/tmp/*'], min_age=86400),
    _path_task('quicklook',  'QuickLook Cache',        'System', ['~/Library/Caches/com.apple.QuickLook.thumbnailcache/*']),
    _path_task('savedstate', 'Saved App State',        'System', ['~/Library/Saved Application State/*']),
    _path_task('diag',       'Diagnostic Reports',     'System', ['~/Library/DiagnosticReports/*']),
    _path_task('identity',   'Identity Caches',        'System', ['~/Library/IdentityCaches/*']),
    _path_task('maildown',   'Mail Downloads',         'System', ['~/Library/Mail Downloads/*']),
    _path_task('incomplete', 'Incomplete Downloads',   'System', ['~/Downloads/*.download', '~/Downloads/*.crdownload', '~/Downloads/*.part']),
    _path_task('shellres',   'Shell History Residue',  'System', ['~/.zsh_history.bak*', '~/.zcompdump*']),
    Task('oldlogs',   'Old Logs (30d+)',        'System', _clean_old_logs, locks=('lib-logs',)),
    Task('trash',     'Trash',                  'System', _empty_trash),
    Task('langpacks', 'Non-English Lang Packs', 'System', _clean_lang_packs, locks=('app-support',)),
    Task('homesweep', '.DS_Store & Cruft',      'System', _home_sweep),
    # Sandboxed apps keep their caches out of ~/Library/Caches, so the sweep
    # above never saw them — on a busy Mac this is usually the biggest single
    # win after Xcode.
    _path_task('containers',  'Sandboxed App Caches',  'System', ['~/Library/Containers/*/Data/Library/Caches/*']),
    _path_task('groupcache',  'Group Container Caches','System', ['~/Library/Group Containers/*/Library/Caches/*']),
    _path_task('appsupcache', 'App Support Caches',    'System', ['~/Library/Application Support/Caches/*']),
    _path_task('ipsw',        'Device Software Updates','System', ['~/Library/iTunes/iPhone Software Updates/*', '~/Library/iTunes/iPad Software Updates/*']),
    *_collect(
        Task('aerials', 'Unused Aerial Videos', 'System', _clean_aerials,
             locks=('app-support',)) if (_WALLPAPER / 'aerials/videos').is_dir() else None,
        Task('dotcache', 'Stale ~/.cache (30d+)', 'System', _clean_stale_dot_cache,
             locks=('dot-cache',)) if (HOME / '.cache').is_dir() else None,
    ),
    *_collect(
        Task('brew_cl', 'Homebrew Cleanup', 'System',
             lambda log: _simple_cmds([['brew', 'cleanup', '--prune=all'],
                                       ['brew', 'autoremove']], log),
             timeout=CMD_TIMEOUT, locks=('brew',)) if _has('brew') else None,
        Task('docker', 'Docker Prune', 'System',
             lambda log: _container_prune('docker', log),
             timeout=CMD_TIMEOUT) if _has('docker') else None,
        Task('podman', 'Podman Prune', 'System',
             lambda log: _container_prune('podman', log),
             timeout=CMD_TIMEOUT) if _has('podman') else None,
    ),
    Task('dns', 'DNS Cache', 'System',
         lambda log: _simple_cmds([['dscacheutil', '-flushcache']], log)),
    *_collect(
        Task('fontcache', 'Font Caches', 'System',
             lambda log: _simple_cmds([['atsutil', 'databases', '-removeUser']], log),
             timeout=CMD_TIMEOUT) if _has('atsutil') else None,
    ),
    # ── Browsers ─────────────────────────────────────────────────────────────
    *_collect(
        _app_task('safari',  'Safari',  'Browsers', ['~/Library/Caches/com.apple.Safari/*', '~/Library/Containers/com.apple.Safari/Data/Library/Caches/*', '~/Library/Safari/History.db-shm'], '/Applications/Safari.app'),
        _app_task('chrome',   'Chrome',   'Browsers', _chromium('~/Library/Application Support/Google/Chrome', '~/Library/Caches/Google/Chrome'),                      '/Applications/Google Chrome.app'),
        _app_task('canary',   'Chrome Canary', 'Browsers', _chromium('~/Library/Application Support/Google/Chrome Canary', '~/Library/Caches/Google/Chrome Canary'),          '/Applications/Google Chrome Canary.app'),
        _app_task('chromium', 'Chromium', 'Browsers', _chromium('~/Library/Application Support/Chromium', '~/Library/Caches/Chromium'),                           '/Applications/Chromium.app'),
        _app_task('brave',    'Brave',    'Browsers', _chromium('~/Library/Application Support/BraveSoftware/Brave-Browser', '~/Library/Caches/BraveSoftware/Brave-Browser'),        '/Applications/Brave Browser.app'),
        _app_task('edge',     'Edge',     'Browsers', _chromium('~/Library/Application Support/Microsoft Edge', '~/Library/Caches/Microsoft Edge'),                     '/Applications/Microsoft Edge.app'),
        _app_task('vivaldi',  'Vivaldi',  'Browsers', _chromium('~/Library/Application Support/Vivaldi', '~/Library/Caches/Vivaldi'),                            '/Applications/Vivaldi.app'),
        _app_task('opera',    'Opera',    'Browsers', _chromium('~/Library/Application Support/com.operasoftware.Opera', '~/Library/Caches/com.operasoftware.Opera'),            '/Applications/Opera.app'),
        _app_task('dia',      'Dia',      'Browsers', _chromium('~/Library/Application Support/Dia', '~/Library/Caches/Dia'),                                '/Applications/Dia.app'),
        _app_task('arc',      'Arc',      'Browsers', _chromium('~/Library/Application Support/Arc/User Data', '~/Library/Caches/Arc'), '/Applications/Arc.app'),
        # Gecko keeps its disk cache per profile under ~/Library/Caches.
        _app_task('firefox',  'Firefox',  'Browsers', ['~/Library/Caches/Firefox/Profiles/*/cache2/*', '~/Library/Caches/Firefox/Profiles/*/startupCache/*'], '/Applications/Firefox.app'),
        _app_task('zen',      'Zen',      'Browsers', ['~/Library/Caches/zen/Profiles/*/cache2/*', '~/Library/Caches/zen/Profiles/*/startupCache/*'],         '/Applications/Zen Browser.app', '/Applications/Zen.app'),
        _app_task('librewolf','LibreWolf','Browsers', ['~/Library/Caches/LibreWolf/Profiles/*/cache2/*'],                            '/Applications/LibreWolf.app'),
        _app_task('tor',      'Tor Browser','Browsers', ['~/Library/Caches/TorBrowser-Data/Browser/Caches/*'],                       '/Applications/Tor Browser.app'),
        _app_task('orion',    'Orion',    'Browsers', ['~/Library/Caches/com.kagi.kagimacOS/*'],                                     '/Applications/Orion.app'),
        # Chrome's updater keeps every extension/component download it has
        # fetched; losing it only means full rather than delta downloads.
        _dir_task('gupdater', 'Google Updater cache', 'Browsers', ['~/Library/Application Support/Google/GoogleUpdater/crx_cache/*'], '~/Library/Application Support/Google/GoogleUpdater/crx_cache'),
    ),
    # ── Dev — JS/Node ────────────────────────────────────────────────────────
    *_collect(
        Task('npm', 'npm', 'Dev — JS/Node',
             lambda log: _simple_cmds([['npm', 'cache', 'clean', '--force']], log),
             timeout=CMD_TIMEOUT, locks=('npm',)) if _has('npm') else None,
        _cmd_task('yarn',      'Yarn',      'Dev — JS/Node', ['~/Library/Caches/Yarn/*'],     'yarn'),
        # Never delete the store directly: every node_modules on the machine
        # hardlinks into it, so a wipe silently guts installed projects.
        # `store prune` drops only the packages nothing references any more.
        Task('pnpm', 'pnpm store', 'Dev — JS/Node', _pnpm_prune,
             timeout=CMD_TIMEOUT) if _has('pnpm') else None,
        _cmd_task('bun',       'Bun',       'Dev — JS/Node', ['~/.bun/install/cache/*'],      'bun'),
        _dir_task('nodegyp',   'node-gyp',  'Dev — JS/Node', ['~/.node-gyp/*'],               '~/.node-gyp'),
        _dir_task('webpack',   'Webpack',   'Dev — JS/Node', ['~/.cache/webpack/*'],          '~/.cache/webpack'),
        _dir_task('vite',      'Vite',      'Dev — JS/Node', ['~/.cache/vite/*'],             '~/.cache/vite'),
        _dir_task('turbo',     'Turbo',     'Dev — JS/Node', ['~/.turbo/*'],                  '~/.turbo'),
        _dir_task('puppeteer', 'Puppeteer', 'Dev — JS/Node', ['~/.cache/puppeteer/*'],        '~/.cache/puppeteer'),
        _dir_task('electron',  'Electron',  'Dev — JS/Node', ['~/.electron/*'],               '~/.electron'),
        _cmd_task('npx',       'npx cache', 'Dev — JS/Node', ['~/.npm/_npx/*'],               'npm'),
        _dir_task('nvmcache',  'nvm cache', 'Dev — JS/Node', ['~/.nvm/.cache/*'],             '~/.nvm/.cache'),
        _dir_task('cypress',   'Cypress',   'Dev — JS/Node', ['~/Library/Caches/Cypress/*'],  '~/Library/Caches/Cypress'),
        _dir_task('esbuild',   'esbuild',   'Dev — JS/Node', ['~/.cache/esbuild/*'],          '~/.cache/esbuild'),
        _dir_task('nx',        'Nx',        'Dev — JS/Node', ['~/.cache/nx/*'],               '~/.cache/nx'),
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
        _cmd_task('poetry', 'Poetry', 'Dev — Python', ['~/Library/Caches/pypoetry/*'], 'poetry'),
        _cmd_task('pyenv',  'pyenv',  'Dev — Python', ['~/.pyenv/cache/*'],             'pyenv'),
        _cmd_task('conda',  'Conda',  'Dev — Python', ['~/.conda/pkgs/*'],              'conda', 'mamba'),
        # `prune` drops only what nothing links to; `clean` would force every
        # project to re-download its whole dependency set.
        Task('uvcache', 'uv cache', 'Dev — Python',
             lambda log: _simple_cmds([['uv', 'cache', 'prune']], log),
             timeout=CMD_TIMEOUT, locks=('python',)) if _has('uv') else None,
        _dir_task('precommit', 'pre-commit', 'Dev — Python', ['~/.cache/pre-commit/*'],     '~/.cache/pre-commit'),
        _dir_task('pipxcache', 'pipx cache', 'Dev — Python', ['~/Library/Caches/pipx/*'],   '~/Library/Caches/pipx'),
        _dir_task('jupyter',   'Jupyter runtime', 'Dev — Python', ['~/Library/Jupyter/runtime/*'], '~/Library/Jupyter/runtime'),
    ),
    # ── Dev — Go ─────────────────────────────────────────────────────────────
    *_collect(
        Task('gobuild', 'Go build cache', 'Dev — Go',
             lambda log: _simple_cmds([['go', 'clean', '-cache'],
                                       ['go', 'clean', '-testcache']], log),
             timeout=CMD_TIMEOUT, locks=('go',)) if _has('go') else None,
        _cmd_task('gomod', 'Go module cache', 'Dev — Go', ['~/go/pkg/mod/cache/*'],       'go'),
        _cmd_task('gopls', 'gopls cache',     'Dev — Go', ['~/Library/Caches/gopls/*'],   'gopls'),
    ),
    # ── Dev — Rust ───────────────────────────────────────────────────────────
    *_collect(
        _cmd_task('cargo',  'Cargo registry',   'Dev — Rust', ['~/.cargo/registry/cache/*'],                'cargo'),
        _cmd_task('rustup', 'Rustup downloads', 'Dev — Rust', ['~/.rustup/downloads/*', '~/.rustup/tmp/*'], 'rustup'),
        _cmd_task('cargosrc', 'Cargo sources',  'Dev — Rust', ['~/.cargo/registry/src/*'],                   'cargo'),
        _dir_task('sccache',  'sccache',        'Dev — Rust', ['~/Library/Caches/Mozilla.sccache/*'],        '~/Library/Caches/Mozilla.sccache'),
    ),
    # ── Dev — Ruby/PHP ───────────────────────────────────────────────────────
    *_collect(
        # ~/.gem/ruby/<ver>/gems holds *installed* gems — only the downloaded
        # .gem archives and the remote spec cache are safe to drop.
        _cmd_task('gem',      'Gem cache', 'Dev — Ruby/PHP', ['~/.gem/ruby/*/cache/*', '~/.gem/specs/*', '~/.gem/cache/*'], 'gem'),
        _cmd_task('bundler',  'Bundler',   'Dev — Ruby/PHP', ['~/.bundle/cache/*'],           'bundle'),
        _cmd_task('composer', 'Composer',  'Dev — Ruby/PHP', ['~/Library/Caches/composer/*'], 'composer'),
    ),
    # ── Dev — Other ──────────────────────────────────────────────────────────
    *_collect(
        Task('nuget', 'NuGet caches', 'Dev — Other', _nuget_clean,
             timeout=CMD_TIMEOUT) if any(_has(c) for c in ('dotnet', 'nuget')) else None,
        _cmd_task('swiftpm', 'Swift PM',     'Dev — Other', ['~/.cache/org.swift.swiftpm/*'], 'swift'),
        _cmd_task('deno',    'Deno',         'Dev — Other', ['~/Library/Caches/deno/*'],      'deno'),
        _dir_task('torch',   'PyTorch',      'Dev — Other', ['~/.cache/torch/*'],             '~/.cache/torch'),
        _dir_task('hf',      'Hugging Face', 'Dev — Other', ['~/.cache/huggingface/*'],       '~/.cache/huggingface'),
        _cmd_task('kubectl', 'kubectl',      'Dev — Other', ['~/.kube/cache/*'],              'kubectl'),
        _cmd_task('aws',     'AWS CLI',      'Dev — Other', ['~/.aws/cli/cache/*'],           'aws'),
        _cmd_task('gh',        'GitHub CLI',  'Dev — Other', ['~/.cache/gh/*'],                          'gh'),
        _cmd_task('helm',      'Helm',        'Dev — Other', ['~/Library/Caches/helm/*'],                'helm'),
        _cmd_task('terraform', 'Terraform',   'Dev — Other', ['~/.terraform.d/plugin-cache/*'],          'terraform', 'tofu'),
        _cmd_task('ccache',    'ccache',      'Dev — Other', ['~/.ccache/*', '~/Library/Caches/ccache/*'], 'ccache'),
        _dir_task('gcloud',    'gcloud logs', 'Dev — Other', ['~/.config/gcloud/logs/*'],                '~/.config/gcloud/logs'),
        _dir_task('ansible',   'Ansible tmp', 'Dev — Other', ['~/.ansible/tmp/*'],                       '~/.ansible/tmp'),
        _dir_task('zig',       'Zig',         'Dev — Other', ['~/.cache/zig/*'],                         '~/.cache/zig'),
        _dir_task('ytdlp',     'yt-dlp',      'Dev — Other', ['~/.cache/yt-dlp/*'],                      '~/.cache/yt-dlp'),
        _dir_task('ollamalog', 'Ollama logs', 'Dev — Other', ['~/.ollama/logs/*'],                       '~/.ollama/logs'),
        _dir_task('claudecli', 'Claude Code', 'Dev — Other', ['~/Library/Caches/claude-cli-nodejs/*'],   '~/Library/Caches/claude-cli-nodejs'),
        Task('claudestate', 'Claude Code state (30d+)', 'Dev — Other',
             _clean_claude_state) if _CLAUDE.is_dir() else None,
    ),
    # ── IDEs & Editors ───────────────────────────────────────────────────────
    *_collect(
        _app_task('xcodedev',  'Xcode DerivedData', 'IDEs & Editors', ['~/Library/Developer/Xcode/DerivedData/*'],        '/Applications/Xcode.app'),
        _app_task('xcodearch', 'Xcode Archives',    'IDEs & Editors', ['~/Library/Developer/Xcode/Archives/*'],           '/Applications/Xcode.app'),
        _app_task('xcodedoc',  'Xcode DocCache',    'IDEs & Editors', ['~/Library/Developer/Xcode/DocumentationCache/*'], '/Applications/Xcode.app'),
        _app_task('xcodelogs', 'Xcode Device Logs', 'IDEs & Editors', ['~/Library/Developer/Xcode/iOS Device Logs/*'],    '/Applications/Xcode.app'),
        # Symbol dumps re-fetched from the device the next time you attach it.
        _app_task('xcodedevsup','Xcode Device Support','IDEs & Editors', ['~/Library/Developer/Xcode/iOS DeviceSupport/*', '~/Library/Developer/Xcode/watchOS DeviceSupport/*', '~/Library/Developer/Xcode/tvOS DeviceSupport/*'], '/Applications/Xcode.app'),
        _app_task('xcodecache','Xcode Caches',       'IDEs & Editors', ['~/Library/Caches/com.apple.dt.Xcode/*'],          '/Applications/Xcode.app'),
        _app_task('simulator', 'iOS Simulator',     'IDEs & Editors', ['~/Library/Developer/CoreSimulator/Caches/*'],     '/Applications/Xcode.app'),
        Task('sim_runtimes', 'Sim Runtimes (unused)', 'IDEs & Editors', _sim_runtimes,
             timeout=CMD_TIMEOUT) if _has('xcrun') else None,
        Task('extobsolete', 'Obsolete Extensions', 'IDEs & Editors', _clean_obsolete_extensions)
        if any(_expand(root).joinpath('.obsolete').is_file() for root, _ in _EDITOR_EXT_DIRS) else None,
        Task('androidstudio', 'Android Studio', 'IDEs & Editors',
             lambda log: _clean_paths(
                 [str(p / '*') for p in (HOME / 'Library/Caches/Google').glob('AndroidStudio*/')]
                 if (HOME / 'Library/Caches/Google').is_dir() else [],
                 log,
             ), locks=('lib-caches',)) if Path('/Applications/Android Studio.app').exists() else None,
        _dir_task('jetbrains',  'JetBrains',   'IDEs & Editors', ['~/Library/Caches/JetBrains/*', '~/Library/Logs/JetBrains/*'],                                        '~/Library/Caches/JetBrains'),
        _app_task('vscode',   'VS Code',  'IDEs & Editors', [*_electron('~/Library/Application Support/Code'),     '~/Library/Application Support/Code/CachedData/*',     '~/Library/Application Support/Code/logs/*'],     '/Applications/Visual Studio Code.app'),
        _app_task('cursor',   'Cursor',   'IDEs & Editors', [*_electron('~/Library/Application Support/Cursor'),   '~/Library/Application Support/Cursor/CachedData/*',   '~/Library/Application Support/Cursor/logs/*'],   '/Applications/Cursor.app'),
        _app_task('windsurf', 'Windsurf', 'IDEs & Editors', [*_electron('~/Library/Application Support/Windsurf'), '~/Library/Application Support/Windsurf/CachedData/*', '~/Library/Application Support/Windsurf/logs/*'], '/Applications/Windsurf.app'),
        _app_task('zed',      'Zed',      'IDEs & Editors', ['~/.cache/zed/*', '~/Library/Logs/Zed/*'],                                                                  '/Applications/Zed.app'),
        _app_task('sublime',  'Sublime Text', 'IDEs & Editors', ['~/Library/Caches/com.sublimetext.4/*', '~/Library/Caches/com.sublimetext.3/*'],                        '/Applications/Sublime Text.app'),
        _dir_task('nvimcache','Neovim',   'IDEs & Editors', ['~/.cache/nvim/*'],                                                                                         '~/.cache/nvim'),
        _dir_task('androidsdk', 'Android SDK', 'IDEs & Editors', ['~/.android/cache/*', '~/.android/build-cache/*'],                                                    '~/.android'),
        _cmd_task('cocoapods',  'CocoaPods',   'IDEs & Editors', ['~/Library/Caches/CocoaPods/*'],                                                                      'pod'),
        _cmd_task('flutter',  'Flutter engine', 'IDEs & Editors', ['~/Library/Caches/flutter_engine/*'], 'flutter'),
        # ~/.pub-cache/bin and global_packages hold `pub global activate`
        # tools; only the package sources, which `pub get` re-fetches, go.
        _cmd_task('pubcache', 'Pub cache', 'IDEs & Editors',
                  ['~/.pub-cache/hosted/*/*', '~/.pub-cache/git/*', '~/.pub-cache/_temp/*'],
                  'flutter', 'dart'),
    ),
    # ── Apps ─────────────────────────────────────────────────────────────────
    *_collect(
        _app_task('slack',         'Slack',            'Apps', _electron('~/Library/Application Support/Slack'),                                  '/Applications/Slack.app'),
        _app_task('discord',       'Discord',          'Apps', _electron('~/Library/Application Support/discord'),                                '/Applications/Discord.app'),
        _app_task('signal',        'Signal',           'Apps', _electron('~/Library/Application Support/Signal'),                                 '/Applications/Signal.app'),
        _app_task('notion',        'Notion',           'Apps', _electron('~/Library/Application Support/Notion'),                                 '/Applications/Notion.app'),
        _app_task('obsidian',      'Obsidian',         'Apps', _electron('~/Library/Application Support/obsidian'),                               '/Applications/Obsidian.app'),
        _app_task('postman',       'Postman',          'Apps', _electron('~/Library/Application Support/Postman'),                                '/Applications/Postman.app'),
        _app_task('insomnia',      'Insomnia',         'Apps', _electron('~/Library/Application Support/Insomnia'),                               '/Applications/Insomnia.app'),
        _app_task('claudeapp',     'Claude',           'Apps', _electron('~/Library/Application Support/Claude'),                                 '/Applications/Claude.app'),
        _app_task('ghdesktop',     'GitHub Desktop',   'Apps', _electron('~/Library/Application Support/GitHub Desktop'),                          '/Applications/GitHub Desktop.app'),
        _app_task('lmstudio',      'LM Studio',        'Apps', [*_electron('~/Library/Application Support/LM Studio'), '~/.cache/lm-studio/*'],   '/Applications/LM Studio.app'),
        # Media caches: both apps re-download whatever they need on demand.
        _app_task('telegram',      'Telegram',         'Apps', ['~/Library/Application Support/Telegram Desktop/tdata/user_data*/cache/*', '~/Library/Application Support/Telegram Desktop/tdata/user_data*/media_cache/*'], '/Applications/Telegram.app', '/Applications/Telegram Desktop.app'),
        _app_task('spotify',       'Spotify',          'Apps', ['~/Library/Caches/com.spotify.client/*', '~/Library/Application Support/Spotify/PersistentCache/*'],   '/Applications/Spotify.app'),
        _app_task('zoom',          'Zoom',             'Apps', ['~/Library/Caches/us.zoom.xos/*', '~/Library/Caches/us.zoom.xos.AutoUpdater/*'],   '/Applications/zoom.us.app'),
        _app_task('teams_classic', 'Teams (Classic)',  'Apps', _electron('~/Library/Application Support/Microsoft/Teams'),                         '/Applications/Microsoft Teams classic.app'),
        _app_task('teams_new',     'Teams (New)',      'Apps', ['~/Library/Group Containers/UBF8T346G9.com.microsoft.teams/Cache/*'],              '/Applications/Microsoft Teams.app'),
        _app_task('steam',         'Steam',            'Apps', ['~/Library/Application Support/Steam/appcache/*', '~/Library/Application Support/Steam/depotcache/*', '~/Library/Application Support/Steam/logs/*'], '/Applications/Steam.app'),
        _app_task('dropbox',       'Dropbox',          'Apps', ['~/Dropbox/.dropbox.cache/*'],                                                     '/Applications/Dropbox.app'),
        _app_task('adobe',         'Adobe Media Cache','Apps', ['~/Library/Application Support/Adobe/Common/Media Cache Files/*', '~/Library/Application Support/Adobe/Common/Peak Files/*'], '/Applications/Adobe Creative Cloud.app'),
        _app_task('dockerlogs',    'Docker Desktop',   'Apps', ['~/Library/Containers/com.docker.docker/Data/log/*'],                              '/Applications/Docker.app'),
        _app_task('vlc',           'VLC',              'Apps', ['~/Library/Caches/org.videolan.vlc/*'],                                            '/Applications/VLC.app'),
        _app_task('transmission',  'Transmission',     'Apps', ['~/Library/Caches/org.m0k.transmission/*'],                                        '/Applications/Transmission.app'),
        _app_task('whatsapp',      'WhatsApp',         'Apps', ['~/Library/Containers/net.whatsapp.WhatsApp/Data/Library/Caches/*'],                '/Applications/WhatsApp.app'),
        _app_task('utm',           'UTM',              'Apps', ['~/Library/Containers/com.utmapp.UTM/Data/Library/Caches/*'],                       '/Applications/UTM.app'),
        _app_task('libreoffice',   'LibreOffice',      'Apps', ['~/Library/Application Support/LibreOffice/4/cache/*'],                             '/Applications/LibreOffice.app'),
    ),
    # ── Report only ──────────────────────────────────────────────────────────
    # Big, and not ours to delete: listed in the log and report, left in place.
    Task('rep_nodemods',  'Stale node_modules (90d)', 'Report only', _report_node_modules),
    Task('rep_backups',   'iOS Device Backups',       'Report only', _report_ios_backups),
    Task('rep_downloads', 'Big Old Downloads',        'Report only', _report_downloads),
    Task('rep_installers','macOS Installers',         'Report only', _report_installers),
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

class TidymacApp(App[None]):
    CSS = CSS
    TITLE = 'tidymac'
    BINDINGS = [('q', 'quit', 'Quit')]

    def __init__(self) -> None:
        super().__init__()
        # Read once: --snapshots appends to ALL_TASKS before the app is built.
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
        prefix = 'Dry run' if DRY_RUN else 'tidymac'
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
        for line in _vs_last_run(self._records, report):
            log_widget.write(line)
        if hint := await _snapshot_hint(totals):
            log_widget.write(hint)
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


def _vs_last_run(records: list[dict], current: Path | None) -> list[str]:
    '''Compare this run with the previous one of the same kind.

    Dry runs compare only with dry runs: a real run always "frees" less than
    the dry run before it, which says nothing. The biggest growers are what
    fills the disk between runs, so they're worth naming.
    '''
    prev = None
    for path in sorted(REPORT_DIR.glob('20*.json'), reverse=True):
        if path == current:
            continue
        with contextlib.suppress(OSError, ValueError):
            data = json.loads(path.read_text())
            if data.get('dry_run') == DRY_RUN:
                prev = data
                break
    if not prev:
        return []
    before = {t['id']: t.get('freed', 0) for t in prev.get('tasks', [])}
    freed = sum(r['freed'] for r in records)
    was = prev.get('totals', {}).get('freed', 0)
    delta = freed - was
    sign = '+' if delta >= 0 else '-'
    lines = [f'vs last run ({prev.get("started", "?")[:10]}): '
             f'{_size_str(freed)} freed, {sign}{_size_str(abs(delta))}']
    growers = sorted(((r['freed'] - before.get(r['id'], 0), r['name'])
                      for r in records), reverse=True)[:3]
    grew = [f'{name} +{_size_str(d)}' for d, name in growers if d >= 50 << 20]
    if grew:
        lines.append('grew most: ' + ', '.join(grew))
    return lines


async def _notify(text: str) -> None:
    safe = (text or 'Finished').replace('"', "'").replace('\\', '')
    title = 'tidymac' + (' (dry run)' if DRY_RUN else '')
    # osascript directly, not via _run_cmd: a dry run should still tell you it
    # finished.
    with contextlib.suppress(Exception):
        proc = await asyncio.create_subprocess_exec(
            'osascript', '-e', f'display notification "{safe}" with title "{title}"',
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await proc.wait()


# ── Headless runner ──────────────────────────────────────────────────────────
_STATUS_MARK = {'ok': '\u2713', 'skipped': '\u2014', 'failed': '\u26a0',
                'error': '\u2717', 'timeout': '\u2717'}


async def run_headless() -> int:
    '''Same tasks, no TUI: one line per task on stdout, detail to a log file.

    This is what a launchd agent runs — a Textual app needs a terminal.
    '''
    started = time.strftime('%Y-%m-%dT%H:%M:%S')
    before = _disk_free()
    locks = _LockSet()
    records: list[dict] = []
    stream = _open_run_log()

    print(f'upgrade-and-clean · {len(ALL_TASKS)} tasks'
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
    for line in _vs_last_run(records, report):
        print(line)
    if hint := await _snapshot_hint(totals):
        print(hint)
    await _notify(summary)
    return 1 if totals['failed'] else 0


# ── Apps: report, uninstall, orphans ─────────────────────────────────────────
# Dragging an app to the Trash leaves its Library data behind, often more than
# the app itself. These modes find that data by bundle id and move it to the
# Trash (never unlink: Put Back is the undo), one-shot rather than a parallel
# task because each one asks before it touches anything.
UNUSED_DAYS = 90

# (directory, may an entry there be named after the app instead of its id)
_LEFTOVER_DIRS: tuple[tuple[str, bool], ...] = (
    ('~/Library/Application Support', True),
    ('~/Library/Application Support/FileProvider', False),
    ('~/Library/Application Support/com.apple.sharedfilelist/'
     'com.apple.LSSharedFileList.ApplicationRecentDocuments', False),
    ('~/Library/Application Scripts', False),
    ('~/Library/Caches', True),
    ('~/Library/Containers', False),
    ('~/Library/Cookies', False),
    ('~/Library/Group Containers', False),
    ('~/Library/HTTPStorages', False),
    ('~/Library/LaunchAgents', False),
    ('~/Library/Logs', True),
    ('~/Library/Preferences', False),
    ('~/Library/Preferences/ByHost', False),
    ('~/Library/Saved Application State', False),
    ('~/Library/WebKit', False),
    ('/Library/Application Support', True),
    ('/Library/Caches', False),
    ('/Library/LaunchAgents', False),
    ('/Library/LaunchDaemons', False),
    ('/Library/Logs', True),
    ('/Library/Preferences', False),
    ('/Library/PrivilegedHelperTools', False),
)

# Only an app or its extension ever writes here. Preferences and Caches also
# collect CLI tools' ids, so an unowned id there proves nothing; an unowned id
# here is an app that's gone. HTTPStorages and WebKit are out for the same
# reason: CLI tools with an embedded bundle id (Playwright's WebKit, compiled
# Bun/Swift binaries) write there too.
_ORPHAN_EVIDENCE = frozenset({
    '~/Library/Application Scripts',
    '~/Library/Application Support/com.apple.sharedfilelist/'
    'com.apple.LSSharedFileList.ApplicationRecentDocuments',
    '~/Library/Containers',
    '~/Library/Saved Application State',
})

# Editors that keep extensions in a home dot-dir rather than Library, keyed by
# the app name (lowercase) that owns it. ~/.cursor alone can run to gigabytes.
_DOT_DIRS = {
    '.cursor': 'cursor',
    '.vscode': 'visual studio code',
    '.vscode-insiders': 'visual studio code - insiders',
    '.windsurf': 'windsurf',
}

_ID_SUFFIXES = ('.plist', '.binarycookies', '.savedstate', '.sfl2', '.sfl3', '.sfl4')
_RE_TEAM = re.compile(r'^[A-Z0-9]{10}\.')
_RE_BUNDLE_ID = re.compile(r'^[a-z0-9-]+(\.[a-z0-9_-]+){2,}$')


@dataclass(slots=True)
class _App:
    path: Path
    bid: str                 # lowercase
    names: frozenset[str]    # lowercase bundle stem and CFBundleName
    size: int = 0
    data: int = 0            # bytes of leftovers in Library
    last_used: float | None = None


def _read_app(path: Path) -> _App | None:
    try:
        with (path / 'Contents/Info.plist').open('rb') as fh:
            info = plistlib.load(fh)
    except (OSError, plistlib.InvalidFileException, ValueError):
        return None
    # Not CFBundleExecutable: Claude Code's URL handler runs a binary called
    # `claude`, which would claim the Claude app's Application Support folder.
    names = {path.stem, info.get('CFBundleName')}
    return _App(path, str(info.get('CFBundleIdentifier') or '').lower(),
                frozenset(n.lower() for n in names if isinstance(n, str) and len(n) >= 3))


def _user_apps() -> list[_App]:
    '''Removable apps: /Applications and ~/Applications, one folder deep.'''
    found: list[_App] = []
    for base in (Path('/Applications'), HOME / 'Applications'):
        try:
            entries = list(base.iterdir())
        except OSError:
            continue
        for entry in entries:
            if entry.suffix == '.app':
                candidates = [entry]
            elif entry.is_dir() and not entry.is_symlink():
                candidates = [p for p in entry.glob('*.app')]
            else:
                continue
            for path in candidates:
                # Safari and friends are firmlinks into the sealed system volume.
                if path.is_symlink() or not (app := _read_app(path)) or not app.bid:
                    continue
                found.append(app)
    return found


_NESTED_BUNDLES = (
    'Contents/*/*.app', 'Contents/*/*/*.app', 'Contents/*/*/*/*/*.app',
    'Contents/PlugIns/*.appex', 'Contents/Library/*/*.appex',
)

# An id whose app-only data changed this recently is still in use by
# something, even if that something isn't a bundle we can find.
ORPHAN_QUIET_DAYS = 7


def _installed_ids() -> set[str]:
    '''Bundle ids of every app on the machine, not just the removable ones.

    Spotlight finds apps anywhere (~/Downloads, other volumes, helpers);
    the app folders are walked too in case indexing is off for them.
    '''
    paths = {str(a.path) for a in _user_apps()}
    # Helpers and extensions nested in an app have ids of their own (Docker's
    # login item is com.docker.helper), and Spotlight doesn't index inside bundles.
    for app in list(paths):
        for pattern in _NESTED_BUNDLES:
            with contextlib.suppress(OSError, ValueError):
                paths.update(str(p) for p in Path(app).glob(pattern))
    for base in ('/System/Applications', '/System/Library/CoreServices'):
        with contextlib.suppress(OSError):
            paths.update(str(p) for p in Path(base).rglob('*.app'))
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        out = subprocess.run(
            ['mdfind', 'kMDItemContentType == "com.apple.application-bundle"'],
            capture_output=True, text=True, timeout=60).stdout
        paths.update(line for line in out.splitlines() if line.endswith('.app'))
    ids = set()
    for p in paths:
        if (app := _read_app(Path(p))) and app.bid:
            ids.add(app.bid)
    return ids


def _entry_key(name: str) -> tuple[str, bool]:
    '''(the bundle id an entry is named for, whether it carried a team prefix).'''
    key = name.lower()
    for suffix in _ID_SUFFIXES:
        if key.endswith(suffix):
            key = key[:-len(suffix)]
            break
    team = bool(_RE_TEAM.match(name))
    if team:
        key = key[11:]
    return key.removeprefix('group.'), team


# macOS's own data, including ids Apple kept from apps it bought (Workflow
# became Shortcuts). Never an orphan, never a leftover.
_APPLE_IDS = ('com.apple.', 'is.workflow.', 'systemgroup.')


def _product_owned(key: str, ids: Iterable[str], names: Iterable[str]) -> bool:
    '''Whether an installed app plausibly owns key, by id or by product name.

    Far looser than _owner, on purpose: calling an app gone is the costly
    mistake. Microsoft AutoUpdate is com.microsoft.autoupdate2 yet caches as
    com.microsoft.autoupdate.fba; MonitorControl moved from me.guillaumeb to
    app.monitorcontrol; Docker Desktop also writes com.electron.dockerdesktop.
    '''
    parts = key.split('.')
    root = '.'.join(parts[:3])
    if any(bid.startswith(root) or key.startswith(bid) for bid in ids):
        return True
    product = parts[2] if len(parts) > 2 else ''
    return any(product.startswith(n) for n in names)


def _owner(key: str, ids: Iterable[str]) -> str:
    '''The most specific bundle id that key belongs to, or ''.

    Prefix, not equality: com.microsoft.OneDriveUpdater and
    com.microsoft.OneDrive.FileProvider are OneDrive's. Longest wins, so
    com.google.Chrome.canary stays Canary's while Canary is installed.
    '''
    best = ''
    for bid in ids:
        if len(bid) > len(best) and key.startswith(bid):
            best = bid
    return best


_NESTED = '~/Library/Application Support/*'  # vendor folders: Google/Chrome


def _library_entries() -> list[tuple[Path, str, bool]]:
    '''Every candidate leftover: (path, its leftover dir, may it match by name).

    Application Support is also read one level down, where vendors nest their
    apps (Google/Chrome); those entries match by app name only.
    '''
    out = []
    for base, by_name in _LEFTOVER_DIRS:
        try:
            for entry in _expand(base).iterdir():
                out.append((entry, base, by_name))
        except OSError:
            continue
    for vendor in list(_expand('~/Library/Application Support').iterdir()):
        if vendor.is_dir() and not vendor.is_symlink():
            with contextlib.suppress(OSError):
                out.extend((entry, _NESTED, True) for entry in vendor.iterdir())
    return out


def _leftovers(app: _App, ids: set[str], entries: list[tuple[Path, str, bool]],
               other_names: set[str] = frozenset()) -> list[Path]:
    '''What app left in Library. other_names are the names of every other
    installed app: a folder two apps could both claim by name is neither's.'''
    others = ids - {app.bid}
    names = app.names - other_names
    nospace = {n.replace(' ', '') for n in names if len(n) >= 5}
    found = []
    for path, base, by_name in entries:
        key, team = _entry_key(path.name)
        if key.startswith(_APPLE_IDS):
            continue
        owner = '' if base == _NESTED else _owner(key, others | {app.bid})
        if owner == app.bid:
            found.append(path)
        elif owner:
            continue  # another installed app's, however the name reads
        elif by_name and path.name.lower() in names:
            found.append(path)
        elif team and any(key.startswith(n) for n in nospace):
            found.append(path)  # UBF8T346G9.OneDriveStandaloneSuite
    for dot, owner_name in _DOT_DIRS.items():
        if owner_name in app.names and (HOME / dot).is_dir():
            found.append(HOME / dot)
    # A nested match inside a folder already taken would be counted twice.
    return [p for p in found if not any(q != p and q in p.parents for q in found)]


def _tree_size(path: Path) -> int:
    '''Allocated bytes, like du: Docker's sparse disk image advertises
    hundreds of GB it doesn't use, and APFS compression shrinks the rest.'''
    try:
        if path.is_symlink() or not path.is_dir():
            return path.lstat().st_blocks * 512
    except OSError:
        return 0
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            with contextlib.suppress(OSError):
                total += os.lstat(os.path.join(root, name)).st_blocks * 512
    return total


def _label(app: _App) -> str:
    '''App name, with its folder when it lives in one (Python 3.13/IDLE).'''
    parent = app.path.parent
    if parent in (Path('/Applications'), HOME / 'Applications'):
        return app.path.stem
    return f'{parent.name.removesuffix(".localized")}/{app.path.stem}'


def _other_names(app: _App, apps: Sequence[_App]) -> set[str]:
    return {n for a in apps if a is not app for n in a.names}


def _sizes(paths: Sequence[Path]) -> list[int]:
    return list(_IO_POOL.map(_tree_size, paths))


def _last_used(paths: Sequence[Path]) -> list[float | None]:
    '''Spotlight's last-opened date per path; None when never recorded.'''
    if not paths:
        return []
    try:
        out = subprocess.run(
            ['mdls', '-raw', '-name', 'kMDItemLastUsedDate', *map(str, paths)],
            capture_output=True, text=True, timeout=60).stdout
    except (OSError, subprocess.SubprocessError):
        return [None] * len(paths)
    from datetime import datetime
    values: list[float | None] = []
    for raw in out.split('\0')[:len(paths)]:
        try:
            values.append(datetime.strptime(raw.strip(), '%Y-%m-%d %H:%M:%S %z').timestamp())
        except ValueError:
            values.append(None)
    return values + [None] * (len(paths) - len(values))


def _age(ts: float | None) -> str:
    if ts is None:
        return 'never recorded'
    days = int((time.time() - ts) // 86400)
    return 'today' if days == 0 else f'{days}d ago'


def _needs_root(path: Path) -> bool:
    '''Moving a directory rewrites its "..", so it must be writable itself.'''
    try:
        writable_self = not path.is_dir() or path.is_symlink() or os.access(path, os.W_OK)
        return not (writable_self and os.access(path.parent, os.W_OK))
    except OSError:
        return True


def _trash(paths: Sequence[Path]) -> list[Path]:
    '''Move paths to the Trash, returning the ones that failed.

    Finder first, because only Finder records where an item came from and so
    offers Put Back; a plain rename into ~/.Trash is the fallback when
    Automation access to Finder is denied.
    '''
    failed = []
    for path in paths:
        script = f'tell application "Finder" to delete (POSIX file {json.dumps(str(path))} as alias)'
        ok = subprocess.run(['osascript', '-e', script], capture_output=True).returncode == 0
        if not ok:
            target = HOME / '.Trash' / path.name
            if target.exists():
                target = target.with_name(f'{path.name} {time.strftime("%H.%M.%S")}')
            try:
                os.rename(path, target)
            except OSError:
                failed.append(path)
    return failed


def _sudo_command(paths: Sequence[Path]) -> str:
    '''The one command to run for root-owned leftovers: unload, then trash.'''
    steps = [f'launchctl bootout system {shlex.quote(str(p))} 2>/dev/null'
             for p in paths if p.parent == Path('/Library/LaunchDaemons')]
    steps.append('mv ' + ' '.join(shlex.quote(str(p)) for p in paths)
                 + f' {shlex.quote(str(HOME / ".Trash"))}/')
    return f'sudo sh -c {shlex.quote("; ".join(steps))}'


def _confirm(question: str, assume_yes: bool) -> bool:
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        print('not a terminal — pass --yes to proceed without asking')
        return False
    return input(f'{question} [y/N] ').strip().lower() in ('y', 'yes')


def _remove_items(paths: list[Path], assume_yes: bool) -> int:
    '''Show paths with sizes, ask, then trash what we can and print the rest.'''
    sizes = _sizes(paths)
    for path, size in sorted(zip(paths, sizes), key=lambda ps: -ps[1]):
        mark = '  (needs sudo)' if _needs_root(path) else ''
        print(f'  {_size_str(size):>9}  {str(path).replace(HOME_STR, "~")}{mark}')
    print(f'  {_size_str(sum(sizes)):>9}  total, {len(paths)} items')
    if DRY_RUN:
        print('\ndry run — nothing moved')
        return 0
    if not _confirm(f'\nMove these {len(paths)} items to the Trash?', assume_yes):
        print('nothing moved')
        return 1
    rooted = [p for p in paths if _needs_root(p)]
    failed = _trash([p for p in paths if p not in rooted])
    moved = len(paths) - len(rooted) - len(failed)
    print(f'\nmoved {moved} items to the Trash — Finder\'s Put Back undoes it')
    for path in failed:
        print(f'  could not move {path}')
    if rooted:
        print('\nRoot-owned items left. To finish, run:')
        print(f'  {_sudo_command(rooted)}')
    return 1 if failed else 0


def _apps_report() -> int:
    '''Removable apps, biggest first, with last use and their Library data.'''
    apps = _user_apps()
    if not apps:
        print('no apps found in /Applications or ~/Applications')
        return 0
    ids = _installed_ids()
    entries = _library_entries()
    for app, size, used in zip(apps, _sizes([a.path for a in apps]),
                               _last_used([a.path for a in apps])):
        app.size, app.last_used = size, used
        app.data = sum(_sizes(_leftovers(app, ids, entries, _other_names(app, apps))))
    cutoff = time.time() - UNUSED_DAYS * 86400
    apps.sort(key=lambda a: -(a.size + a.data))
    width = max(len(_label(a)) for a in apps)
    print(f'  {"app":<{width}}  {"size":>9}  {"data":>9}  last used')
    unused = []
    for a in apps:
        stale = a.last_used is None or a.last_used < cutoff
        if stale:
            unused.append(a)
        print(f'{"*" if stale else " "} {_label(a):<{width}}  {_size_str(a.size):>9}  '
              f'{_size_str(a.data):>9}  {_age(a.last_used)}')
    if unused:
        total = sum(a.size + a.data for a in unused)
        print(f'\n* {len(unused)} apps unused for {UNUSED_DAYS}+ days, or never opened '
              f'as far as Spotlight knows: {_size_str(total)} with their data')
    print('\nRemove with: tidymac --uninstall "App Name" [...]')
    return 0


def _xcode_guard(app: _App) -> str:
    '''Refuse to pull Xcode out from under xcode-select — git and cc go with it.'''
    try:
        dev = subprocess.run(['xcode-select', '-p'], capture_output=True,
                             text=True, timeout=10).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ''
    if dev.startswith(str(app.path) + '/'):
        fix = ('sudo xcode-select -s /Library/Developer/CommandLineTools'
               if Path('/Library/Developer/CommandLineTools').is_dir()
               else 'xcode-select --install   # then: sudo xcode-select -s /Library/Developer/CommandLineTools')
        return f'{app.path.name} is the active developer directory. First run:\n  {fix}'
    return ''


def _uninstall(queries: Sequence[str], assume_yes: bool) -> int:
    apps = _user_apps()
    chosen: list[_App] = []
    for query in queries:
        q = query.lower().removesuffix('.app')
        matches = [a for a in apps if q in (a.path.stem.lower(), _label(a).lower(), a.bid)
                   or q in a.names]
        if not matches:
            matches = [a for a in apps if q in _label(a).lower()]
        if len(matches) != 1:
            names = ', '.join(_label(a) for a in matches) or 'nothing'
            print(f'"{query}" matches {names} — give the exact app name')
            return 2
        if warning := _xcode_guard(matches[0]):
            print(warning)
            return 2
        chosen.append(matches[0])

    ids = _installed_ids()
    entries = _library_entries()
    paths: list[Path] = []
    for app in chosen:
        paths.append(app.path)
        paths.extend(p for p in _leftovers(app, ids, entries, _other_names(app, apps))
                     if p not in paths)
    print(f'Uninstall {", ".join(_label(a) for a in chosen)}:')
    if not DRY_RUN:
        # Quit first: a running app rewrites its prefs and caches on the way out.
        for app in chosen:
            subprocess.run(['osascript', '-e', f'quit app id {json.dumps(app.bid)}'],
                           capture_output=True, timeout=30)
    return _remove_items(paths, assume_yes)


def _orphans(assume_yes: bool) -> int:
    '''Library data for bundle ids no installed app owns.'''
    ids = _installed_ids()
    if len(ids) < 20:
        print('could not list installed apps (is Spotlight indexing off?) — '
              'refusing to guess what is orphaned')
        return 2
    entries = _library_entries()
    installed_names = {n for a in _user_apps() for n in a.names}
    products = {n.replace(' ', '') for n in installed_names if len(n) >= 4}
    gone: set[str] = set()
    active: set[str] = set()
    quiet = time.time() - ORPHAN_QUIET_DAYS * 86400
    for path, base, _ in entries:
        key, team = _entry_key(path.name)
        if (base in _ORPHAN_EVIDENCE and not team and _RE_BUNDLE_ID.match(key)
                and not key.startswith(_APPLE_IDS)
                and not _product_owned(key, ids, products)):
            gone.add(key)
    # terminal-browser keeps its bundle somewhere no scan reaches, yet writes
    # its preferences daily: fresh activity anywhere overrules "not installed".
    for path, base, _ in entries:
        key, _ = _entry_key(path.name)
        if base != _NESTED and _owner(key, gone):
            with contextlib.suppress(OSError):
                if path.lstat().st_mtime > quiet:
                    active.add(_owner(key, gone))
    gone -= active
    # A dead app's extensions and helpers live under its id; fold them into
    # the shortest id they extend, so OneDrive reads as one app, not six.
    roots = {k for k in gone if not any(k != o and k.startswith(o) for o in gone)}
    paths = []
    for path, base, _ in entries:
        key, _ = _entry_key(path.name)
        if (base != _NESTED and not key.startswith(_APPLE_IDS)
                and not _product_owned(key, ids, products) and _owner(key, roots)):
            paths.append(path)
    paths += [HOME / dot for dot, name in _DOT_DIRS.items()
              if name not in installed_names and (HOME / dot).is_dir()]
    if not paths:
        print('no orphaned app data found')
        return 0
    print(f'Data left by {len(roots)} apps that are no longer installed:')
    return _remove_items(paths, assume_yes)


# ── launchd agent ────────────────────────────────────────────────────────────
AGENT_LABEL = 'local.tidymac'
_AGENT_PLIST = '''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{python}</string>
        <string>{script}</string>
        <string>--headless</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Weekday</key><integer>0</integer>
        <key>Hour</key><integer>11</integer>
        <key>Minute</key><integer>0</integer>
    </dict>
    <key>RunAtLoad</key><false/>
    <key>LowPriorityIO</key><true/>
    <key>Nice</key><integer>5</integer>
    <key>StandardOutPath</key><string>{logdir}/agent.out.log</string>
    <key>StandardErrorPath</key><string>{logdir}/agent.err.log</string>
</dict>
</plist>
'''


def _install_agent() -> None:
    '''Write a weekly launchd agent. Deliberately does not load it — that is
    the user's call, and the command to do it is printed below.'''
    plist_dir = HOME / 'Library/LaunchAgents'
    plist_dir.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    target = plist_dir / f'{AGENT_LABEL}.plist'
    target.write_text(_AGENT_PLIST.format(
        label=AGENT_LABEL,
        python=sys.executable,
        script=str(Path(__file__).resolve()),
        logdir=str(REPORT_DIR),
    ))
    print(f'wrote {target}')
    print('\nIt is not loaded yet. To enable the Sunday 11:00 run:')
    print(f'  launchctl bootstrap gui/{UID} {target}')
    print('To disable it again:')
    print(f'  launchctl bootout gui/{UID}/{AGENT_LABEL}')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='tidymac — macOS upgrade + cleanup.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run', action='store_true',
                        help='report what would be freed; delete nothing, run no commands')
    parser.add_argument('--headless', action='store_true',
                        help='no TUI, one line per task (for launchd/cron)')
    parser.add_argument('--snapshots', action='store_true',
                        help='also delete Time Machine local snapshots (needs sudo)')
    parser.add_argument('--no-report', action='store_true',
                        help='do not write a JSON run report')
    parser.add_argument('--install-agent', action='store_true',
                        help='write a weekly launchd agent and exit')
    parser.add_argument('--apps', action='store_true',
                        help='report installed apps by size, last use and Library data')
    parser.add_argument('--uninstall', nargs='+', metavar='APP',
                        help='move apps and their Library data to the Trash')
    parser.add_argument('--orphans', action='store_true',
                        help='move Library data of apps no longer installed to the Trash')
    parser.add_argument('--yes', action='store_true',
                        help='do not ask before --uninstall / --orphans move anything')
    parser.add_argument('--claude-history', type=int, metavar='DAYS',
                        help='also delete Claude Code session transcripts older than DAYS')
    args = parser.parse_args()

    global DRY_RUN, WRITE_REPORT, CLAUDE_HISTORY_DAYS
    DRY_RUN = args.dry_run
    WRITE_REPORT = not args.no_report

    if args.install_agent:
        _install_agent()
        return
    try:
        if args.apps:
            raise SystemExit(_apps_report())
        if args.uninstall:
            raise SystemExit(_uninstall(args.uninstall, args.yes))
        if args.orphans:
            raise SystemExit(_orphans(args.yes))
    finally:
        if args.apps or args.uninstall or args.orphans:
            _IO_POOL.shutdown(wait=False, cancel_futures=True)
    if args.snapshots:
        ALL_TASKS.append(_snapshots_task())
    if args.claude_history is not None:
        if args.claude_history < 1:
            parser.error('--claude-history needs at least 1 day')
        CLAUDE_HISTORY_DAYS = args.claude_history
        ALL_TASKS.append(_claude_history_task())

    try:
        if args.headless:
            raise SystemExit(asyncio.run(run_headless()))
        TidymacApp().run()
    finally:
        _IO_POOL.shutdown(wait=False, cancel_futures=True)


if __name__ == '__main__':
    main()
