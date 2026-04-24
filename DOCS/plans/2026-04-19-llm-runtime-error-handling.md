# LLM Runtime Error Handling Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make OldIronCrawler classify LLM proxy errors correctly, pause safely, and resume work with clear Chinese prompts.

**Architecture:** Add a dedicated LLM error-classification layer that inspects status code, error code, error type, message, and Retry-After headers. Route both startup validation and in-run failures through one intervention loop so the same selected workbook and sqlite progress can continue without restarting the whole job.

**Tech Stack:** Python, pytest, OpenAI Python SDK, httpx, sqlite runtime store

---

### Task 1: Add failing tests for LLM error classification and runtime recovery

**Files:**
- Create: `tests/test_llm_error_handling.py`
- Modify: `src/oldironcrawler/app.py`
- Modify: `src/oldironcrawler/extractor/llm_client.py`

**Step 1: Write the failing tests**
- Cover `401 invalid_api_key` -> require new key
- Cover `403 budget_exhausted` -> require new key
- Cover `403 ip_not_allowed` -> require new key with restriction message
- Cover `503 service_temporarily_unavailable` -> pause and retry current key
- Cover app resume loop keeping the same workbook/runtime store path while replacing only the key

**Step 2: Run the focused tests to verify they fail**

Run: `& '.\.venv\Scripts\python.exe' -m pytest tests\test_llm_error_handling.py -v`
Expected: FAIL because the new classifier and recovery loop do not exist yet

### Task 2: Implement the classifier and intervention exceptions

**Files:**
- Create: `src/oldironcrawler/llm_errors.py`
- Modify: `src/oldironcrawler/extractor/llm_client.py`

**Step 1: Add a structured error model**
- Define category, prompt mode, retry-after seconds, and Chinese user message
- Keep parsing logic independent from crawler business logic

**Step 2: Teach the LLM client to raise structured intervention exceptions**
- Prefer OpenAI/httpx exception structure over plain string matching
- Keep transient auto-retry for short-lived 429/5xx/network failures
- Escalate to intervention only after retry budget is exhausted or when the error is clearly credential/quota related

**Step 3: Re-run focused tests**

Run: `& '.\.venv\Scripts\python.exe' -m pytest tests\test_llm_error_handling.py -v`
Expected: Some tests still fail because app-level recovery is not implemented yet

### Task 3: Implement startup/runtime pause-and-resume flow

**Files:**
- Modify: `src/oldironcrawler/app.py`
- Modify: `src/oldironcrawler/console.py`
- Modify: `src/oldironcrawler/runner.py`
- Modify: `run.py`

**Step 1: Wrap startup validation and column-detection in a key-recovery loop**
- Pick workbook once
- Reuse the same workbook while prompting for a new key only when needed

**Step 2: Wrap crawl execution in a runtime recovery loop**
- Preserve sqlite progress
- Reset `running` rows back to `pending` on resume
- Retry with current key for temporary service outages
- Prompt for a new masked key for auth/quota/access failures

**Step 3: Add clear Chinese console prompts**
- Wrong key
- Quota exhausted
- IP/permission restricted
- Temporary service unavailable / retry-after guidance

**Step 4: Re-run focused tests**

Run: `& '.\.venv\Scripts\python.exe' -m pytest tests\test_llm_error_handling.py -v`
Expected: PASS

### Task 4: Run regression verification

**Files:**
- Modify: `tests/test_windows_runtime.py`
- Modify: `tests/test_core.py`

**Step 1: Add or update small regression tests only if current suites expose gaps**

**Step 2: Run targeted regression suites**

Run: `& '.\.venv\Scripts\python.exe' -m pytest tests\test_llm_error_handling.py tests\test_windows_runtime.py tests\test_core.py -v`
Expected: PASS

### Task 5: Rebuild the portable folder and verify packaged behavior

**Files:**
- Modify: `packaging/build_exe.ps1`
- Output: `dist/OldIronCrawler/`

**Step 1: Rebuild the exe**

Run: `& '.\packaging\build_exe.ps1'`
Expected: `dist\OldIronCrawler\OldIronCrawler.exe` refreshed successfully

**Step 2: Smoke-check the packaged layout**
- Verify `dist\OldIronCrawler\.env`
- Verify `dist\OldIronCrawler\websites\`
- Verify `dist\OldIronCrawler\output\delivery\`
- Verify `dist\OldIronCrawler\output\runtime\`

**Step 3: Record verification result in final report**
