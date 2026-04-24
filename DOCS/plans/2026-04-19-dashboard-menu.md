# Interactive Dashboard Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the linear command flow with a resilient business-style dashboard that validates the runtime Key before entering the panel and keeps recoverable errors inside the program.

**Architecture:** Split the interaction layer into a reusable dashboard/menu module plus small app orchestration helpers. Persist only in-memory session state for the current Key, selected workbook, concurrency, and single-site timeout, then project those settings into `AppConfig` for each crawl run.

**Tech Stack:** Python, pytest, pathlib, sqlite runtime store, Windows shell integration

---

### Task 1: Lock the new dashboard behavior with failing tests

**Files:**
- Modify: `tests/test_llm_error_handling.py`
- Modify: `tests/test_windows_runtime.py`

**Step 1: Add tests for dashboard requirements**
- startup Key prompt text changes to `请输入本次运行的 Key:`
- invalid Key never drops into the panel
- dashboard menu routes recoverable errors back to the right screen
- `output` root is used for deliveries instead of `output\delivery`

**Step 2: Run focused tests to verify they fail**
Run: `& '.\.venv\Scripts\python.exe' -m pytest tests\test_llm_error_handling.py tests\test_windows_runtime.py -v`
Expected: FAIL

### Task 2: Implement dashboard/session state and menu flow

**Files:**
- Create: `src/oldironcrawler/dashboard.py`
- Modify: `src/oldironcrawler/console.py`
- Modify: `src/oldironcrawler/app.py`
- Modify: `run.py`

**Step 1: Add session state for current Key, selected file, concurrency, timeout**

**Step 2: Implement a business-style main panel**
- start crawl
- open websites folder
- open output folder
- system config
- exit

**Step 3: Keep recoverable errors inside the program**
- wrong Key -> stay on Key page
- wrong file selection -> stay on file page
- crawl finish -> wait for enter then return menu
- temporary runtime outage -> pause then continue

**Step 4: Re-run focused tests**
Run: `& '.\.venv\Scripts\python.exe' -m pytest tests\test_llm_error_handling.py tests\test_windows_runtime.py -v`
Expected: PASS

### Task 3: Move delivery output to `output\` and refresh packaging behavior

**Files:**
- Modify: `src/oldironcrawler/config.py`
- Modify: `src/oldironcrawler/package_layout.py`
- Modify: `tests/test_windows_runtime.py`

**Step 1: Point delivery files to `output\` while keeping runtime cache in `output\runtime`**

**Step 2: Update portable folder placeholders and checks**

**Step 3: Re-run regression tests**
Run: `& '.\.venv\Scripts\python.exe' -m pytest tests\test_llm_error_handling.py tests\test_windows_runtime.py tests\test_core.py -v`
Expected: PASS

### Task 4: Rebuild and smoke-check the packaged exe

**Files:**
- Output: `dist\OldIronCrawler\`

**Step 1: Rebuild**
Run: `& '.\packaging\build_exe.ps1'`
Expected: `dist\OldIronCrawler\OldIronCrawler.exe` refreshed

**Step 2: Smoke-check invalid Key path on packaged exe**
Expected: invalid Key stays on Key page and never shows the file menu
