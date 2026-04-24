# High-Concurrency Stability and Phones Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix the main high-concurrency stall/failure paths, improve German `impressum`-driven extraction quality, and add rule-based `phones` extraction end-to-end without lowering concurrency.

**Architecture:** Keep the existing crawler architecture, but repair the runner/pool/protocol control flow so failures are bounded and observable. Extend the contact-rule extraction layer from `emails` only to `emails + phones`, then carry the new field through runtime storage, reporting, and delivery.

**Tech Stack:** Python 3, SQLite, pytest, BeautifulSoup, httpx, ThreadPoolExecutor, PowerShell build tooling

---

### Task 1: Add regression tests for runner shutdown and page-pool starvation

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_core.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_fetch_failures.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\runner.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\extractor\page_pool.py`

**Step 1: Write the failing tests**

- Add a test where one running site raises `LlmConfigurationError` while another site finishes later; assert the finished result is not lost and the shutdown path does not wait invisibly for a full site budget.
- Add a test where a timed-out page batch does not block a later batch from acquiring slots after timeout cleanup.

**Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_core.py -k "llm_configuration or heartbeat" -v
pytest tests/test_fetch_failures.py -k "page_pool or timeout" -v
```

Expected: FAIL on current runner/page-pool behavior.

**Step 3: Write the minimal implementation**

- In `runner.py`, separate fatal-error detection from final drain/persist behavior.
- Ensure already-finished futures are persisted before exit.
- Stop scheduling new work once fatal config error is known.
- In `page_pool.py`, cancel not-started futures and release batch pressure when a batch closes on timeout.

**Step 4: Run the targeted tests**

Run:

```bash
pytest tests/test_core.py -k "llm_configuration or heartbeat" -v
pytest tests/test_fetch_failures.py -k "page_pool or timeout" -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add tests/test_core.py tests/test_fetch_failures.py src/oldironcrawler/runner.py src/oldironcrawler/extractor/page_pool.py
git commit -m "fix: harden fatal runner shutdown and page pool timeout cleanup"
```

### Task 2: Fix retry classification and restart-recovery coverage

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\app.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\runner.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_llm_error_handling.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_core.py`

**Step 1: Write the failing tests**

- Add a real-SQLite recovery test: claim a row as `running`, reopen the same DB, call restart recovery, and assert the task becomes runnable again.
- Add a test that `site_deadline_exceeded` is treated as terminal and not retried.
- Add a test that temporary LLM retry path uses bounded backoff instead of a hot loop.

**Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_llm_error_handling.py -v
pytest tests/test_core.py -k "deadline or recovery" -v
```

Expected: FAIL on current behavior.

**Step 3: Write the minimal implementation**

- Add bounded retry/backoff in `_recover_runtime_llm_key()` or its caller.
- Make `site_deadline_exceeded` a terminal classification.
- Strengthen restart recovery to be covered by real SQLite tests.

**Step 4: Run the targeted tests**

Run:

```bash
pytest tests/test_llm_error_handling.py -v
pytest tests/test_core.py -k "deadline or recovery" -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add tests/test_llm_error_handling.py tests/test_core.py src/oldironcrawler/app.py src/oldironcrawler/runner.py
git commit -m "fix: bound retry recovery and harden restart state handling"
```

### Task 3: Repair protocol and challenge classification without reducing concurrency

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\extractor\protocol_client.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\challenge_solver.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_protocol_regressions.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_core.py`

**Step 1: Write the failing tests**

- Add a test that plain non-challenge `403` becomes blocked/permanent instead of successful HTML.
- Add a test that temporary homepage fetch failure still allows common probe fallback.
- Add a test that SGCaptcha/Imperva pages do not enter Cloudflare CapSolver path.
- Add a test that challenge polling respects real elapsed time accounting.

**Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_protocol_regressions.py -v
pytest tests/test_core.py -k "challenge or 403" -v
```

Expected: FAIL on current protocol/challenge logic.

**Step 3: Write the minimal implementation**

- In `protocol_client.py`, classify plain `403` correctly and keep temporary homepage errors from bypassing common probes.
- In `challenge_solver.py`, separate challenge families, validate fallback status codes, and account for real elapsed time.

**Step 4: Run the targeted tests**

Run:

```bash
pytest tests/test_protocol_regressions.py -v
pytest tests/test_core.py -k "challenge or 403" -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add tests/test_protocol_regressions.py tests/test_core.py src/oldironcrawler/extractor/protocol_client.py src/oldironcrawler/challenge_solver.py
git commit -m "fix: correct blocked-page and challenge fallback handling"
```

### Task 4: Improve German value-page scoring and shell-page enrichment

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\extractor\value_rules.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\extractor\service.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\extractor\shell_page.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\extractor\company_rules.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_shell_regressions.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_discovery_optimization.py`

**Step 1: Write the failing tests**

- Add a test that `impressum` and `kontakt` are selected as high-value representative/contact pages.
- Add a test that overflow shell pages are enriched before contact extraction.
- Add a test that synthetic shell recovery headers are ignored by company fallback.
- Add a test that shell detection still triggers when a root-container page includes cookie/banner text.

**Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_shell_regressions.py -v
pytest tests/test_discovery_optimization.py -v
```

Expected: FAIL on current extraction heuristics.

**Step 3: Write the minimal implementation**

- Add German tokens and `impressum`-heavy weighting to representative/contact scoring.
- Re-run shell enrichment after overflow pages are fetched.
- Ignore synthetic shell recovery headers in company fallback.
- Relax shell detection so root-container evidence is checked before blunt text-length rejection.

**Step 4: Run the targeted tests**

Run:

```bash
pytest tests/test_shell_regressions.py -v
pytest tests/test_discovery_optimization.py -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add tests/test_shell_regressions.py tests/test_discovery_optimization.py src/oldironcrawler/extractor/value_rules.py src/oldironcrawler/extractor/service.py src/oldironcrawler/extractor/shell_page.py src/oldironcrawler/extractor/company_rules.py
git commit -m "fix: improve german page scoring and shell enrichment coverage"
```

### Task 5: Add rule-based phones extraction beside emails

**Files:**
- Create: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\extractor\phone_rules.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\extractor\service.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\extractor\shell_page.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_core.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_shell_regressions.py`

**Step 1: Write the failing tests**

- Add a test for multiple `tel:` values joined with `; `.
- Add a test for visible phone extraction and deduplication across formatting variants.
- Add a test for phone recovery from JSON/script or shell evidence.
- Add a test that obvious numeric noise is rejected.

**Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_core.py -k "phone" -v
pytest tests/test_shell_regressions.py -k "phone" -v
```

Expected: FAIL because phone extraction does not exist yet.

**Step 3: Write the minimal implementation**

- Create `phone_rules.py` with:
  - split/join helpers
  - HTML extraction
  - embedded-content extraction
  - normalization and dedupe
  - noise rejection
- In `service.py`, extract phones from the same contact-rule pages used for emails and include them in the result.
- Ensure shell evidence lines preserve phone-bearing signals.

**Step 4: Run the targeted tests**

Run:

```bash
pytest tests/test_core.py -k "phone" -v
pytest tests/test_shell_regressions.py -k "phone" -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add tests/test_core.py tests/test_shell_regressions.py src/oldironcrawler/extractor/phone_rules.py src/oldironcrawler/extractor/service.py src/oldironcrawler/extractor/shell_page.py
git commit -m "feat: add rule-based phone extraction"
```

### Task 6: Carry `phones` through runtime storage, terminal output, and delivery CSV

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\runtime\store.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\reporter.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\runner.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_core.py`

**Step 1: Write the failing tests**

- Add a runtime-store test that persists and reloads `phones`.
- Add a CSV writer test that outputs `phones`.
- Add a terminal formatting test that prints the phone line with `未找到` when empty.

**Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_core.py -k "phones or delivery" -v
```

Expected: FAIL because runtime/reporter still use the 4-column contract.

**Step 3: Write the minimal implementation**

- Add `phones` to `SiteResult`, SQLite schema management, reset paths, `mark_done`, and `delivery_rows()`.
- Add phone display to `print_site_result()`.
- Update `write_delivery_csv()` field order.

**Step 4: Run the targeted tests**

Run:

```bash
pytest tests/test_core.py -k "phones or delivery" -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add tests/test_core.py src/oldironcrawler/runtime/store.py src/oldironcrawler/reporter.py src/oldironcrawler/runner.py
git commit -m "feat: deliver phones through runtime and csv output"
```

### Task 7: Fix packaged/runtime config divergence and artifact naming collisions

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\config.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\package_layout.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\bootstrap.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\src\oldironcrawler\app.py`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\tests\test_windows_runtime.py`

**Step 1: Write the failing tests**

- Add a test that packaged `.env` preserves challenge-related keys.
- Add a test that config can read from `os.environ`.
- Add a test that invalid local proxy ports fail closed without crashing.
- Add a test that different input suffixes do not collide on runtime DB/delivery artifact naming.

**Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_windows_runtime.py -v
pytest tests/test_core.py -k "artifact or collision" -v
```

Expected: FAIL on current packaging/config behavior.

**Step 3: Write the minimal implementation**

- Preserve challenge keys in package layout.
- Stop forcing concurrency overrides in the portable package unless intentionally required.
- Merge `.env` with process environment values safely.
- Sanitize persisted keys against newline injection.
- Make runtime artifact names unique per input file identity.

**Step 4: Run the targeted tests**

Run:

```bash
pytest tests/test_windows_runtime.py -v
pytest tests/test_core.py -k "artifact or collision" -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add tests/test_windows_runtime.py tests/test_core.py src/oldironcrawler/config.py src/oldironcrawler/package_layout.py src/oldironcrawler/bootstrap.py src/oldironcrawler/app.py
git commit -m "fix: align packaged runtime config with source behavior"
```

### Task 8: Run the full regression sweep and update product docs

**Files:**
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\DOCS\PRD.MD`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\DOCS\plans\2026-04-22-high-concurrency-stability-and-phones-design.md`
- Modify: `E:\Develop\Masterpiece\System\OldIronCrawler\DOCS\plans\2026-04-22-high-concurrency-stability-and-phones.md`

**Step 1: Update docs**

- Change the delivery contract in `DOCS/PRD.MD` from 4 columns to 5 columns.
- Document that phones are rule-extracted, not LLM-generated.
- Document that German `impressum` is an explicit strong signal.

**Step 2: Run the full test suite**

Run:

```bash
pytest -q
```

Expected: PASS with the environment dependencies installed.

**Step 3: Run a focused manual verification**

Run at least one real German input and verify:

- `Impressum` contributes representative/contact results
- multiple phones are preserved
- no long heartbeat-only starvation appears under configured high concurrency

**Step 4: Record final verification notes**

- Save exact commands and observed outcomes in the current task notes or commit message body.

**Step 5: Commit**

```bash
git add DOCS/PRD.MD DOCS/plans/2026-04-22-high-concurrency-stability-and-phones-design.md DOCS/plans/2026-04-22-high-concurrency-stability-and-phones.md
git commit -m "docs: update contact extraction and stability plan"
```
