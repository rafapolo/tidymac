# tidymac

A macOS upgrade + cleanup TUI. It upgrades your package managers and sweeps
every cache it can find — dozens of tasks, all in **parallel**, with a live
progress bar per task and a running total of what it freed.

![tidymac running](docs/screenshot.png)

Every task is auto-detected: tidymac only shows Cargo if you have `cargo`,
only shows Brave if Brave is installed. Nothing is hardcoded to a machine.

---

## Setup

Requires **macOS** and **Python 3.11+**.

### With uv (recommended)

```sh
git clone https://github.com/rafapolo/tidymac.git
cd tidymac
uv tool install .
```

### With pipx

```sh
git clone https://github.com/rafapolo/tidymac.git
cd tidymac
pipx install .
```

Either one puts a `tidymac` command on your PATH.

### Without installing

```sh
git clone https://github.com/rafapolo/tidymac.git
cd tidymac
pip install textual
./tidymac.py
```

**First run: use `--dry-run`.** It sizes everything up and deletes nothing, so
you can see exactly what tidymac would touch on your machine before it does.

```sh
tidymac --dry-run
```

---

## Commands

```sh
tidymac                  # the TUI — upgrade everything, clean everything
tidymac --dry-run        # size it all up, delete nothing, run no commands
tidymac --headless       # one line per task, no TUI (for launchd/cron)
tidymac --snapshots      # also thin Time Machine local snapshots (needs sudo)
tidymac --no-report      # skip the JSON run report
tidymac --install-agent  # write a weekly launchd agent, then exit
tidymac --claude-history 90  # also delete Claude Code transcripts older than 90 days
tidymac --apps           # installed apps by size, Library data and last use
tidymac --uninstall Discord "Microsoft Teams"   # app + its Library data → Trash
tidymac --orphans        # Library data of apps that are no longer installed → Trash
tidymac --help
```

Flags combine: `tidymac --headless --dry-run` is a safe way to see what a
scheduled run would do, and `--dry-run --uninstall X` lists what would go.

In the TUI, press `q` to quit.

---

## What it does

**Upgrades** — Homebrew (update, upgrade, casks, autoremove, cleanup, doctor),
Oh My Zsh, Mac App Store (`mas`), Python tools (`pipx` / `uv` / `pipupgrade`),
npm globals, `rustup`, Ruby gems, and macOS software updates. Also
self-updates Claude Code, Bun and Deno (only copies Homebrew doesn't own),
mise tools, asdf plugins, gcloud components and VS Code-family extensions.
`flutter upgrade` is left out on purpose: it can break pinned projects.

**System** — user caches, sandboxed app + group container caches, crash and
diagnostic reports, saved app state, Trash (including per-volume trashes),
QuickLook thumbnails, logs older than 30 days, non-English `.lproj` language
packs, `.DS_Store` files, build cruft (`__pycache__`, `.pytest_cache`,
`.ruff_cache`, `.mypy_cache`, `.ipynb_checkpoints`), temp dirs, DNS and font
caches, Docker and Podman prune, downloaded aerial wallpaper videos nothing
uses, and `~/.cache` directories untouched for 30 days (judged by the newest
file inside, not the folder date).

**Browsers** — Safari, Chrome, Chrome Canary, Chromium, Brave, Edge, Vivaldi,
Opera, Arc, Dia, Firefox, Zen, LibreWolf, Tor, Orion. Chromium-family browsers
are cleaned across *every* profile. Google Updater's download cache too.

**Dev** — JS/Node (npm, yarn, pnpm store prune, bun, Vite, Webpack, Turbo,
Cypress, Puppeteer, esbuild, Nx, nvm), JVM (Gradle, Maven), Python (pip,
Poetry, pyenv, conda, uv, pre-commit, Jupyter), Go, Rust (Cargo registry,
rustup downloads, sccache), Ruby/PHP (gem cache, Bundler, Composer), plus
NuGet, Swift PM, Deno, kubectl, AWS CLI, gh, Helm, Terraform, ccache, gcloud,
Ansible, Zig, yt-dlp, Ollama logs, PyTorch, Hugging Face, and Claude Code's
per-session scratch (debug logs, shell snapshots, `/rewind` history) once
30 days stale.

**IDEs & Editors** — Xcode (DerivedData, Archives, DocCache, device logs and
device support), iOS Simulator, Android Studio + SDK, JetBrains, VS Code,
Cursor, Windsurf, Zed, Sublime Text, Neovim, CocoaPods, Flutter, simulator
runtimes unused for 90 days, and the old extension versions VS Code-family
editors have marked obsolete but never deleted.

**Apps** — Slack, Discord, Signal, Notion, Obsidian, Postman, Insomnia, Claude,
GitHub Desktop, LM Studio, Telegram, Spotify, Zoom, Teams, Steam, Dropbox,
Adobe media cache, Docker Desktop, VLC, Transmission, WhatsApp, UTM,
LibreOffice.

**Report only** — listed, never deleted: `node_modules` in projects untouched
for 90 days, iPhone/iPad backups, files over 500 MB in `~/Downloads` unopened
for 30 days, and `Install macOS` installer apps.

---

## Apps

Dragging an app to the Trash leaves its data behind in `~/Library`, often
more than the app itself: Teams' container outweighs half its bundle, and
Cursor leaves gigabytes of extensions in `~/.cursor`.

- **`--apps`** lists every app in `/Applications` and `~/Applications`,
  biggest first, with its size, what it keeps in Library, and when Spotlight
  last saw it opened. Anything unused for 90 days or never opened gets a `*`.
- **`--uninstall APP…`** finds the app's data by bundle id (containers,
  group containers, preferences, caches, logs, launch agents, WebKit and
  HTTP storage, saved state, recent-documents lists, editor dot-dirs), shows
  it with sizes, asks, then quits the app and moves it all to the Trash.
- **`--orphans`** does the same for apps already gone.

Both move things to the Trash through Finder, so **Put Back** is the undo.
Root-owned pieces (the bundle itself, `/Library/LaunchDaemons`) aren't
touched: tidymac prints the single `sudo` command that unloads and trashes
them. `--yes` skips the question; `--dry-run` only lists.

Deciding what counts as someone else's is the careful part:

- A folder matching an app's **name** is only claimed when no other installed
  app has that name too. The executable name isn't used at all: Claude
  Code's URL handler runs a binary called `claude`, and would otherwise claim
  the Claude app's data.
- An id belongs to the **most specific** installed bundle id it extends, so
  uninstalling Chrome leaves Chrome Canary's data alone.
- `--orphans` only calls an app gone on evidence nothing else writes:
  sandbox containers, extension scripts, saved window state, recent-documents
  lists. It then spares any id whose vendor and product match something
  installed, including helpers nested inside other apps
  (`com.docker.helper` lives in `Docker.app`), renamed vendors
  (MonitorControl, from `me.guillaumeb` to `app.monitorcontrol`), and any
  id with files changed in the last 7 days, since something is still using it.
- Uninstalling Xcode is refused while `xcode-select` points into it, since
  `git` and the compilers would go with it. The fix is printed.

After a big cleanup, the free-space number can stay flat. Deleted files that
a Time Machine local snapshot still references keep their blocks until the
snapshot ages out. When that's the case, tidymac says so at the end of a run.

---

## Run reports

Each run writes JSON to `~/.local/state/tidymac/`, with `last-run.json` always
pointing at the most recent one. The TUI log is capped and dies with the app —
this is what lets you compare one run against the last.

```sh
jq .totals ~/.local/state/tidymac/last-run.json
```

Each run ends by comparing itself with the previous run of the same kind
(dry runs only against dry runs) and naming the tasks that grew the most:
that's where the disk fills up between runs.

Old reports are pruned automatically.

---

## Weekly schedule

```sh
tidymac --install-agent
```

This writes `~/Library/LaunchAgents/local.tidymac.plist` for a Sunday 11:00
headless run, at low I/O priority. It deliberately does **not** load it — the
command to do that is printed for you:

```sh
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/local.tidymac.plist
launchctl bootout   gui/$UID/local.tidymac    # to disable again
```

Agent output lands in `~/.local/state/tidymac/agent.{out,err}.log`.

---

## Notes on safety

tidymac deletes regenerable data only, and a few deliberate choices back that:

- **pnpm** uses `store prune`, never a store wipe — every `node_modules` on the
  machine hardlinks into that store, so deleting it outright would silently gut
  your installed projects.
- **uv** uses `cache prune` rather than `cache clean`, which would force every
  project to re-download its full dependency set.
- **Ruby gems**: only downloaded `.gem` archives and the remote spec cache are
  dropped — installed gems stay.
- **Temp dirs** skip anything modified in the last 24 hours, so files still in
  use by running processes aren't pulled out from under them.
- Tasks touching the same tree or package manager hold a lock, so a
  `brew upgrade` can't race a `brew cleanup`, and the blanket cache sweep can't
  race a per-app cache task.
- **Editor extensions**: only folders the editor itself listed in
  `extensions/.obsolete` go, and never while that editor is running.
- **Aerial videos**: any video the wallpaper store references, or that played
  in the last 30 days, stays. If the store can't be read, nothing is deleted.
- **Claude Code**: scratch tied to a session active in the last 30 days stays.
  Transcripts (your `--resume` history) are only touched with
  `--claude-history DAYS`.
- Child processes get `/dev/null` on stdin and their own session, so nothing
  can steal keystrokes from the TUI or hang forever on a prompt.

`--snapshots` is the one flag that needs `sudo`, and it's opt-in for that
reason. Time Machine local snapshots are not a substitute for your backups, but
deleting them does reduce what you can roll back to locally.

---

## Linux: `tidy`

`tidylinux.py` is the Linux sibling, tuned for Ubuntu: same engine and TUI,
with a Linux task table.

- **Upgrades** — APT (`full-upgrade`, `autoremove --purge`, residual `rc`
  configs purged, `apt-get clean`; flags when a reboot is required), snaps,
  Flatpak, firmware via `fwupdmgr` (staged for next boot), `cargo
  install-update`, `gh` extensions, plus the user-level tools tidymac knows.
- **Cleanup** — snapd's disabled revisions, the systemd journal (30 days /
  500 MB), rotated `/var/log` generations, coredumps, apport crashes, snapd's
  download cache, `~/.cache`, per-snap and per-flatpak caches, freedesktop
  Trash (also on mounted volumes), and the native/snap/flatpak locations of
  browsers, editors and Electron apps.

APT, snaps, firmware, the journal and the `/var` sweep need root. Run from a terminal, `tidy` asks for the
sudo password once before the TUI starts; `--no-sudo` skips those tasks
instead. `--install-agent` writes a weekly systemd user timer.

It ships as one self-contained file (a zipapp with Textual vendored in), so
the target needs only `python3`:

```sh
uv pip install --target build/src --python-version 3.14 \
    --python-platform x86_64-manylinux_2_28 --only-binary :all: 'textual>=8.2'
cp tidylinux.py build/src/
python3 -m zipapp build/src -m tidylinux:main -p '/usr/bin/env python3' -c -o build/tidy
scp build/tidy host:.local/bin/tidy
```

---

## License

MIT
