# Runtime LLM Key Prompt Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Prompt for a per-run LLM key at startup, keep it in memory only, and remove interactive `Ctrl+C` tracebacks.

**Architecture:** Add a console input helper for masked secret entry, pass the key into config as an explicit override, and handle cancellation at the entrypoint. Then verify the same flow in both source execution and the packaged EXE.

**Tech Stack:** Python 3.11, pytest, PowerShell, PyInstaller

---

### Task 1: Add failing tests for masked key input

**Files:**
- Create: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\console.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py`

**Step 1: Write the failing test**

```python
def test_prompt_runtime_llm_key_masks_and_retries() -> None:
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_windows_runtime.py::test_prompt_runtime_llm_key_masks_and_retries -v`

Expected: FAIL because the prompt helper does not exist yet.

**Step 3: Write minimal implementation**

Add a prompt helper that accepts injected char-reader and writer dependencies for testing.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_windows_runtime.py::test_prompt_runtime_llm_key_masks_and_retries -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add src/oldironcrawler/console.py tests/test_windows_runtime.py
git commit -m "feat: add masked runtime key prompt"
```

### Task 2: Add failing tests for key override and clean cancellation

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\run.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\config.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py`

**Step 1: Write the failing test**

```python
def test_app_config_prefers_runtime_llm_key_override(tmp_path: Path) -> None:
    ...

def test_run_main_returns_clean_cancel_when_key_prompt_is_interrupted(monkeypatch) -> None:
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_windows_runtime.py::test_app_config_prefers_runtime_llm_key_override tests/test_windows_runtime.py::test_run_main_returns_clean_cancel_when_key_prompt_is_interrupted -v`

Expected: FAIL because override and cancellation handling do not exist yet.

**Step 3: Write minimal implementation**

Add:

- `llm_key_override` to `AppConfig.load()`
- entrypoint key prompt wrapper
- `KeyboardInterrupt` handling in `run.main()`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_windows_runtime.py::test_app_config_prefers_runtime_llm_key_override tests/test_windows_runtime.py::test_run_main_returns_clean_cancel_when_key_prompt_is_interrupted -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add run.py src/oldironcrawler/config.py tests/test_windows_runtime.py
git commit -m "fix: support runtime key override and clean cancel"
```

### Task 3: Thread the runtime key into the app flow

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\app.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py`

**Step 1: Write the failing test**

```python
def test_run_interactive_uses_runtime_llm_key_override(...) -> None:
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_windows_runtime.py::test_run_interactive_uses_runtime_llm_key_override -v`

Expected: FAIL because the app does not yet accept and forward the override.

**Step 3: Write minimal implementation**

Thread `llm_key_override` from `run.py` into `run_interactive()` and then into `AppConfig.load()`.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_windows_runtime.py::test_run_interactive_uses_runtime_llm_key_override -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add src/oldironcrawler/app.py tests/test_windows_runtime.py
git commit -m "refactor: pass runtime key through app startup"
```

### Task 4: Verify source runtime and rebuild the EXE

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\dist\OldIronCrawler.exe`

**Step 1: Run the targeted test suite**

Run:

```bash
pytest tests/test_windows_runtime.py tests/test_core.py::test_raise_nofile_soft_limit_raises_soft_limit -v
```

Expected: PASS.

**Step 2: Verify source startup**

Run:

```bash
python run.py
```

Expected:

- prompt for LLM key first
- empty input retries
- file menu appears only after a key is entered

**Step 3: Rebuild the EXE**

Run:

```powershell
.\packaging\build_exe.ps1
```

Expected: successful one-file build.

**Step 4: Verify EXE startup**

Run the rebuilt EXE from a clean folder with `.env` and `websites`.

Expected:

- prompt for LLM key first
- clean `Ctrl+C`
- `output` created beside the EXE

**Step 5: Commit**

```bash
git add dist/OldIronCrawler.exe
git commit -m "build: refresh portable exe with runtime key prompt"
```
