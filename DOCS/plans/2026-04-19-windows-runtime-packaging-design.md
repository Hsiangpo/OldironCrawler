# Windows Runtime and Portable EXE Design

## Problem

The crawler currently runs on the developer machine only through an injected launch workaround:

- `src/oldironcrawler/app.py` imports `resource`, which does not exist on Windows.
- The interactive flow prints Chinese text and file names, but the process does not configure UTF-8 output on Windows terminals.
- The runtime assumes source-tree execution from the repository root, which is not enough for a portable packaged EXE.

The result is simple: `python run.py` fails on Windows, and a naive PyInstaller build would package the current failure modes.

## Goals

- Make `python run.py` start normally on Windows without injected shims or environment variables.
- Preserve the current console interaction model.
- Support Chinese file names and Chinese terminal output reliably.
- Package a portable Windows console EXE that can run on machines without Python installed.
- Keep `.env`, `websites`, and `output` external to the EXE.

## Non-Goals

- Do not change crawl strategy or extraction quality.
- Do not add a GUI.
- Do not embed secrets into the EXE.
- Do not redesign the project layout beyond what is required for runtime portability.

## Chosen Approach

Use a small runtime bootstrap layer plus PyInstaller packaging support.

This approach fixes both source execution and packaged execution with the same rules:

1. Detect the runtime root safely.
2. Configure console streams for UTF-8 early.
3. Treat missing POSIX-only features as optional on Windows.
4. Package the application as a console EXE with external `.env` and data folders.

This is preferable to a PyInstaller-only workaround because the source entrypoint and the EXE will share the same behavior.

## Runtime Design

### 1. Bootstrap module

Create a dedicated bootstrap module under `src/oldironcrawler/` with three responsibilities:

- configure console encoding
- resolve runtime root
- raise file descriptor soft limit only when supported

The bootstrap layer should be called from `run.py` before importing the main app module.

### 2. Windows-safe file limit handling

`resource` must become optional.

The crawler benefits from raising the `RLIMIT_NOFILE` soft limit on POSIX systems, but that optimization is not a startup requirement on Windows. The bootstrap should detect whether `resource` is importable. If it is missing, the function becomes a no-op. If it exists, keep the current logic.

This preserves current Linux behavior and removes the Windows startup crash.

### 3. UTF-8 console handling

The entrypoint should reconfigure `stdin`, `stdout`, and `stderr` to UTF-8 when the stream supports `reconfigure()`.

This must happen before interactive prompts print Chinese text. The logic should be defensive:

- do nothing if the stream is missing
- do nothing if `reconfigure()` is unavailable
- do not crash on reconfiguration errors

This keeps the application readable in normal terminals and avoids `UnicodeEncodeError` during menu rendering and error reporting.

### 4. Runtime root resolution

The runtime root should be computed differently for source and frozen execution:

- source run: repository root resolved from `run.py`
- frozen run: directory containing the EXE

`AppConfig.load()` should receive the resolved runtime root instead of recomputing it from `__file__`.

That makes these paths stable in both modes:

- `.env`
- `websites/`
- `output/runtime/`
- `output/delivery/`

## Packaging Design

### Packaging format

Build a Windows console EXE with PyInstaller.

Use a two-step validation flow:

1. verify a directory build first
2. produce the final single-file EXE

The final user-facing artifact should be a console EXE plus external working files beside it.

### External runtime layout

The EXE should expect this layout:

- `OldIronCrawler.exe`
- `.env`
- `websites/`
- `output/`

The application should create missing output directories automatically, as it already does.

### Build assets

Add packaging assets under a dedicated packaging directory, not mixed into crawler logic. The packaging assets should include:

- a PyInstaller spec file
- a PowerShell build script
- short usage notes in the script or spec comments

## Test Strategy

The implementation should follow TDD with real Windows execution checks.

### Automated checks

- subprocess test: `python run.py` can import and start on Windows without a `resource` shim
- subprocess test: Chinese menu output no longer crashes under a non-UTF-8 `PYTHONIOENCODING`
- unit test: runtime root resolver returns repo root for source mode
- unit test: runtime root resolver returns EXE directory for frozen mode

### Packaging checks

- build a PyInstaller directory bundle successfully
- run the built EXE from a temp working directory with a real `.env` and `websites` folder
- confirm the EXE creates `output` and reaches the interactive file-selection step

## Risks and Controls

- PyInstaller may miss hidden imports from dynamic dependencies.
  - Control: keep build in two phases and add hidden imports only when a real build proves they are necessary.
- Console encoding may vary across Windows terminals.
  - Control: configure streams explicitly and verify with a subprocess test.
- The packaged runtime may accidentally read the repo `.env` instead of the EXE-local `.env`.
  - Control: resolve runtime root centrally and test the frozen path contract.

## Acceptance Criteria

- `python run.py` starts on Windows with no injected compatibility shim.
- Chinese prompts and Chinese file names print correctly.
- The crawler still uses repository-local `.env`, `websites`, and `output` during source execution.
- A packaged Windows console EXE builds successfully.
- The packaged EXE runs on a machine without Python installed when a valid `.env` is provided beside the EXE.
