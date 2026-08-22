#!/usr/bin/env python3
'''tidymac — macOS upgrade + cleanup TUI. Runs everything in parallel with per-task progress bars.

  --dry-run        size everything up, delete nothing, run no commands
  --headless       one line per task instead of the TUI (for launchd/cron)
  --snapshots      also thin Time Machine local snapshots (needs sudo)
  --no-report      skip the JSON run report
  --install-agent  write a weekly launchd agent and exit
'''
from __future__ import annotations
import argparse, asyncio, contextlib, json, os, re, signal, sys, time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence
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


async def _run_series(
    cmds: Iterable[Sequence[str]],
    log: Callable[[str], None],
    counted: Callable[[str], None] | None = None,
    count_only: frozenset[int] | None = None,
) -> int:
    '''Run commands in order; returns how many exited non-zero.

    count_only restricts package counting to the given command indices, so
    diagnostics like `brew doctor` can't inflate the upgrade tally.
    '''
    failures = 0
    for i, cmd in enumerate(cmds):
        log(f'$ {" ".join(cmd)}')
        sink = counted if counted and (count_only is None or i in count_only) else log
        if await _run_cmd(cmd, sink) != 0:
            failures += 1
    return failures


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
                and not d.endswith('.app')
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

        failures = await _run_series(cmds, log, counted, count_only)
        return Result(
            pkgs=counter.n,
            failed=bool(failures),
            note=f'{failures} cmd failed' if failures else '',
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
    failures = await _run_series(cmds[:5], log, counted, frozenset({1, 2}))
    await _run_series(cmds[5:], log)
    return Result(pkgs=counter.n, failed=bool(failures),
                  note=f'{failures} cmd failed' if failures else '')


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
    failures = await _run_series(cmds, log)
    return Result(failed=bool(failures), note=f'{failures} cmd failed' if failures else '')


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
        Task('brew_cl', 'Homebrew Cleanup', 'System',
             lambda log: _simple_cmds([['brew', 'cleanup', '--prune=all'],
                                       ['brew', 'autoremove']], log),
             timeout=CMD_TIMEOUT, locks=('brew',)) if _has('brew') else None,
        Task('docker', 'Docker Prune', 'System', _docker_prune,
             timeout=CMD_TIMEOUT) if _has('docker') else None,
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
        Task('sim_runtimes', 'Sim Runtimes (unused)', 'IDEs & Editors',
             lambda log: _simple_cmds([['xcrun', 'simctl', 'delete', 'unavailable']], log),
             timeout=CMD_TIMEOUT) if _has('xcrun') else None,
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
        if report := _write_report(self._records, totals):
            log_widget.write(f'report: {report}')
        await _notify(summary)


# ── Reporting ────────────────────────────────────────────────────────────────
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
    return {
        'id': task.id, 'name': task.name, 'subcategory': task.subcategory,
        'status': status, 'freed': res.freed, 'files': res.files,
        'pkgs': res.pkgs, 'note': res.note, 'seconds': round(elapsed, 1),
    }


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
    log_path = REPORT_DIR / 'last-run.log'
    with contextlib.suppress(OSError):
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        stream = log_path.open('w')
    except OSError:
        stream = None

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
    await _notify(summary)
    return 1 if totals['failed'] else 0


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
    args = parser.parse_args()

    global DRY_RUN, WRITE_REPORT
    DRY_RUN = args.dry_run
    WRITE_REPORT = not args.no_report

    if args.install_agent:
        _install_agent()
        return
    if args.snapshots:
        ALL_TASKS.append(_snapshots_task())

    try:
        if args.headless:
            raise SystemExit(asyncio.run(run_headless()))
        TidymacApp().run()
    finally:
        _IO_POOL.shutdown(wait=False, cancel_futures=True)


if __name__ == '__main__':
    main()
