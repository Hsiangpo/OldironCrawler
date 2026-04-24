# LLM Website Column Pick and Portable Folder Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Excel imports choose the real website column with LLM assistance and ship a ready-to-send `dist\OldIronCrawler\` folder.

**Architecture:** Extend the importer to build compact column summaries, call the existing LLM client once per CSV/XLSX file, and blend the result with strong local penalties for social/email/note columns. Then change the packaging script to assemble a clean portable folder that contains the EXE, runnable `.env`, and empty input/output directories.

**Tech Stack:** Python 3.11, openpyxl, pytest, PowerShell, PyInstaller

---

### Task 1: Add failing tests for mixed-column spreadsheet detection

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_core.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\importer.py`

**Step 1: Write the failing test**

Add tests that show:

- a sheet with `company_name`, `linkedin_url`, `email`, and `company website` should pick `company website`
- a sheet with several URL-like columns should still reject the social URL column

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_core.py::test_xlsx_loader_prefers_llm_selected_company_website_column -v`

Expected: FAIL because the importer still chooses by weak header/first-match logic.

**Step 3: Write minimal implementation**

Add column summary helpers, LLM-based column pick, and guarded final scoring.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_core.py::test_xlsx_loader_prefers_llm_selected_company_website_column -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add tests/test_core.py src/oldironcrawler/importer.py
git commit -m "feat: use llm to identify website column"
```

### Task 2: Thread the LLM picker into app startup

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\app.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\extractor\llm_client.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py`

**Step 1: Write the failing test**

Add a test that proves the importer can receive an LLM picker from the startup flow and that the selected column reason is printed.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_windows_runtime.py::test_import_flow_logs_selected_website_column -v`

Expected: FAIL because no such wiring exists yet.

**Step 3: Write minimal implementation**

Expose a focused LLM method for website-column selection and pass it into the importer from `run_interactive()`.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_windows_runtime.py::test_import_flow_logs_selected_website_column -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add src/oldironcrawler/app.py src/oldironcrawler/extractor/llm_client.py tests/test_windows_runtime.py
git commit -m "refactor: wire website-column llm selector into import flow"
```

### Task 3: Add failing tests for portable folder packaging

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\packaging\build_exe.ps1`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py`

**Step 1: Write the failing test**

Add a test that expects the build output structure to be:

- `dist\OldIronCrawler\OldIronCrawler.exe`
- `dist\OldIronCrawler\.env`
- `dist\OldIronCrawler\websites\`
- `dist\OldIronCrawler\output\delivery\`
- `dist\OldIronCrawler\output\runtime\`

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_windows_runtime.py::test_packaging_targets_portable_folder_layout -v`

Expected: FAIL because the build still writes the EXE directly into `dist\`.

**Step 3: Write minimal implementation**

Change the build script to:

- build the EXE
- create `dist\OldIronCrawler\`
- move/copy the EXE into that folder
- generate the packaged `.env`
- create `websites\` and `output\` subdirectories with placeholder files

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_windows_runtime.py::test_packaging_targets_portable_folder_layout -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add packaging/build_exe.ps1 tests/test_windows_runtime.py
git commit -m "build: produce portable dist folder"
```

### Task 4: Rebuild and verify the portable folder

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\dist\OldIronCrawler\OldIronCrawler.exe`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\dist\OldIronCrawler\.env`

**Step 1: Run the targeted tests**

Run:

```bash
pytest tests/test_windows_runtime.py tests/test_core.py -k "website_column or portable_folder or nofile" -v
```

Expected: PASS.

**Step 2: Rebuild the package**

Run:

```powershell
.\packaging\build_exe.ps1
```

Expected: build succeeds and outputs `dist\OldIronCrawler\`.

**Step 3: Verify folder contents**

Check:

- `dist\OldIronCrawler\OldIronCrawler.exe`
- `dist\OldIronCrawler\.env`
- `dist\OldIronCrawler\websites\`
- `dist\OldIronCrawler\output\delivery\`
- `dist\OldIronCrawler\output\runtime\`

**Step 4: Real launch verification**

Run `dist\OldIronCrawler\OldIronCrawler.exe`.

Expected:

- prompts for LLM key first
- uses the packaged folder layout
- does not require extra manual setup before the user drops a workbook into `websites\`

**Step 5: Commit**

```bash
git add dist/OldIronCrawler packaging/build_exe.ps1
git commit -m "build: refresh portable delivery folder"
```
