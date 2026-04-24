# Windows Runtime and Portable EXE Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the crawler run natively on Windows and ship a portable console EXE that works without Python preinstalled.

**Architecture:** Add a small bootstrap layer that normalizes console encoding, makes POSIX-only limits optional, and resolves runtime root for both source and frozen execution. Then wire the entrypoint and config through that layer, add regression tests, and package the app with PyInstaller using an EXE-local working directory contract.

**Tech Stack:** Python 3.11, pytest, sqlite3, python-dotenv, PyInstaller, PowerShell

---

### Task 1: Add a testable runtime bootstrap module

**Files:**
- Create: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\bootstrap.py`
- Create: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py`

**Step 1: Write the failing test**

```python
from pathlib import Path

from oldironcrawler.bootstrap import resolve_runtime_root


def test_resolve_runtime_root_uses_source_parent_when_not_frozen() -> None:
    entry = Path(r"E:\Develop\Masterpiece\System\OldIronCrawler\run.py")

    root = resolve_runtime_root(entry_file=entry, frozen=False, executable_path=None)

    assert root == entry.parent


def test_resolve_runtime_root_uses_exe_parent_when_frozen() -> None:
    entry = Path(r"E:\ignored\run.py")
    exe = Path(r"D:\Apps\OldIronCrawler\OldIronCrawler.exe")

    root = resolve_runtime_root(entry_file=entry, frozen=True, executable_path=exe)

    assert root == exe.parent
```

**Step 2: Run test to verify it fails**

Run: `pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py -v`

Expected: FAIL with `ModuleNotFoundError` or missing `resolve_runtime_root`.

**Step 3: Write minimal implementation**

```python
from pathlib import Path


def resolve_runtime_root(*, entry_file: Path, frozen: bool, executable_path: Path | None) -> Path:
    if frozen and executable_path is not None:
        return executable_path.resolve().parent
    return entry_file.resolve().parent
```

**Step 4: Run test to verify it passes**

Run: `pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add src/oldironcrawler/bootstrap.py tests/test_windows_runtime.py
git commit -m "test: add runtime root bootstrap coverage"
```

### Task 2: Make the entrypoint safe on Windows

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\run.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\bootstrap.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py`

**Step 1: Write the failing test**

```python
import os
import subprocess
import sys
from pathlib import Path


def test_run_entrypoint_starts_without_resource_shim(tmp_path: Path) -> None:
    project_root = Path(r"E:\Develop\Masterpiece\System\OldIronCrawler")
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    proc = subprocess.run(
        [sys.executable, "-c", "import builtins, run; builtins.input=lambda _='': '1'; raise SystemExit(run.main())"],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
    )

    assert "ModuleNotFoundError: No module named 'resource'" not in proc.stderr
```

**Step 2: Run test to verify it fails**

Run: `pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py::test_run_entrypoint_starts_without_resource_shim -v`

Expected: FAIL because `run.py` still imports the app module path that crashes on `resource`.

**Step 3: Write minimal implementation**

```python
# bootstrap.py
def raise_nofile_soft_limit(target: int = 65536) -> None:
    try:
        import resource
    except ImportError:
        return
    ...

# run.py
from oldironcrawler.bootstrap import configure_stdio_utf8, resolve_runtime_root

configure_stdio_utf8()
project_root = resolve_runtime_root(...)
```

**Step 4: Run test to verify it passes**

Run: `pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py::test_run_entrypoint_starts_without_resource_shim -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add run.py src/oldironcrawler/bootstrap.py tests/test_windows_runtime.py
git commit -m "fix: make entrypoint start on windows"
```

### Task 3: Lock UTF-8 console behavior for Chinese prompts

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\bootstrap.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py`

**Step 1: Write the failing test**

```python
import os
import subprocess
import sys
from pathlib import Path


def test_run_entrypoint_handles_chinese_output_under_cp1252() -> None:
    project_root = Path(r"E:\Develop\Masterpiece\System\OldIronCrawler")
    env = os.environ.copy()
    env["PYTHONUTF8"] = "0"
    env["PYTHONIOENCODING"] = "cp1252"
    proc = subprocess.run(
        [sys.executable, "-c", "import builtins, run; builtins.input=lambda _='': '1'; raise SystemExit(run.main())"],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
    )

    assert "UnicodeEncodeError" not in proc.stderr
```

**Step 2: Run test to verify it fails**

Run: `pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py::test_run_entrypoint_handles_chinese_output_under_cp1252 -v`

Expected: FAIL with `UnicodeEncodeError`.

**Step 3: Write minimal implementation**

```python
import sys


def configure_stdio_utf8() -> None:
    for stream_name in ("stdin", "stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass
```

**Step 4: Run test to verify it passes**

Run: `pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py::test_run_entrypoint_handles_chinese_output_under_cp1252 -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add src/oldironcrawler/bootstrap.py tests/test_windows_runtime.py
git commit -m "fix: force utf8 console streams on windows"
```

### Task 4: Route config through the runtime root contract

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\run.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\app.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\config.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py`

**Step 1: Write the failing test**

```python
from pathlib import Path

from oldironcrawler.config import AppConfig


def test_app_config_uses_explicit_runtime_root(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    root.mkdir()
    (root / ".env").write_text("LLM_BASE_URL=https://example.com\nLLM_KEY=test\nLLM_MODEL=gpt-5.4-mini\n", encoding="utf-8")

    config = AppConfig.load(root)

    assert config.project_root == root
    assert config.websites_dir == root / "websites"
    assert config.runtime_dir == root / "output" / "runtime"
```

**Step 2: Run test to verify it fails**

Run: `pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py::test_app_config_uses_explicit_runtime_root -v`

Expected: FAIL if the runtime still recomputes the root indirectly.

**Step 3: Write minimal implementation**

```python
# run.py passes resolved project_root into the app
# app.py accepts project_root and stops recomputing from __file__
# config.py remains the single source of directory derivation
```

**Step 4: Run test to verify it passes**

Run: `pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py::test_app_config_uses_explicit_runtime_root -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add run.py src/oldironcrawler/app.py src/oldironcrawler/config.py tests/test_windows_runtime.py
git commit -m "refactor: use explicit runtime root"
```

### Task 5: Add packaging assets for a portable console EXE

**Files:**
- Create: `E:\Develop\Masterpiece\System\OldIronCrawler\packaging\build_exe.ps1`
- Create: `E:\Develop\Masterpiece\System\OldIronCrawler\packaging\OldIronCrawler.spec`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\requirements.txt`

**Step 1: Write the failing test**

```python
from pathlib import Path


def test_packaging_assets_exist() -> None:
    root = Path(r"E:\Develop\Masterpiece\System\OldIronCrawler")
    assert (root / "packaging" / "build_exe.ps1").exists()
    assert (root / "packaging" / "OldIronCrawler.spec").exists()
```

**Step 2: Run test to verify it fails**

Run: `pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py::test_packaging_assets_exist -v`

Expected: FAIL because the packaging files do not exist yet.

**Step 3: Write minimal implementation**

```powershell
# build_exe.ps1
python -m PyInstaller --noconfirm .\packaging\OldIronCrawler.spec
```

```python
# OldIronCrawler.spec
console = True
name = "OldIronCrawler"
```

Add `pyinstaller` to the build requirements path that the script will use.

**Step 4: Run test to verify it passes**

Run: `pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py::test_packaging_assets_exist -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add packaging/build_exe.ps1 packaging/OldIronCrawler.spec requirements.txt tests/test_windows_runtime.py
git commit -m "build: add pyinstaller packaging assets"
```

### Task 6: Verify source run, packaged build, and packaged startup

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py`

**Step 1: Write the failing test**

```python
def test_placeholder() -> None:
    assert False
```

Replace the placeholder with real packaging verification only after the build assets exist.

**Step 2: Run test to verify it fails**

Run: `pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py::test_placeholder -v`

Expected: FAIL.

**Step 3: Write minimal implementation**

Use real verification commands instead of a permanent placeholder:

```powershell
python -m pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py -v
python E:\Develop\Masterpiece\System\OldIronCrawler\run.py
powershell -ExecutionPolicy Bypass -File E:\Develop\Masterpiece\System\OldIronCrawler\packaging\build_exe.ps1
```

Then run the built `OldIronCrawler.exe` from a clean folder containing:

- `.env`
- `websites\`

Confirm that:

- the process starts
- Chinese menu text prints
- `output\` is created beside the EXE

**Step 4: Run verification to confirm it passes**

Run:

```powershell
pytest E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py -v
python -X utf8 E:\Develop\Masterpiece\System\OldIronCrawler\run.py
powershell -ExecutionPolicy Bypass -File E:\Develop\Masterpiece\System\OldIronCrawler\packaging\build_exe.ps1
```

Expected:

- test suite PASS
- source run reaches interactive selection
- PyInstaller build succeeds
- built EXE reaches interactive selection

**Step 5: Commit**

```bash
git add tests/test_windows_runtime.py
git commit -m "test: verify windows runtime and packaging flow"
```
