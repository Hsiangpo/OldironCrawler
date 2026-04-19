# OldIronCrawler High-Concurrency Value Budget Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve per-site completion speed under high concurrency without losing representative, email, or company-name coverage.

**Architecture:** Keep the current protocol-first crawler and 3-stage pipeline, but replace the flat “merge all target URLs and fetch together” behavior with a value-aware page budget: representative pages first, homepage HTML reuse, email pages second, and early-stop when enough evidence is already present. In parallel, clean the LLM-as-Teacher feedback loop so successful runs stop poisoning useful page families.

**Tech Stack:** Python 3.11, curl_cffi/httpx, SQLite, pytest

---

## Freeze Status (2026-04-19)

- Chunk 1: Completed
- Chunk 2: Completed
- Chunk 3: Completed
- Chunk 4 / Task 8: Deferred, not executed
- Benchmark gate:
  - `c44`: `221.03s` / `27.15 sites_per_minute` / `all_three=42` / `dropped=10` / `fake_done=0`
  - `c52`: `207.23s` / `28.95 sites_per_minute` / `all_three=42` / `dropped=10` / `fake_done=0`
- Final decision:
  - default concurrency stays `52`
  - current bottleneck remains page fetching
  - Task 8 is deferred because it attacks a secondary bottleneck, not the current primary bottleneck

---

## Context Snapshot

- Verified clean benchmark on the current node:
  - `40` concurrency: `12.66 sites/min`, `dropped=16`, `fake_done=0`
  - `44` concurrency: `14.89 sites/min`, `dropped=8`, `fake_done=0`
  - `52` concurrency: `16.0 sites/min`, `dropped=8`, `fake_done=0`
- Verified post-Chunk-3 benchmark freeze on the same node:
  - `44` concurrency: `221.03s`, `27.15 sites/min`, `all_three=42`, `dropped=10`, `fake_done=0`
  - `52` concurrency: `207.23s`, `28.95 sites/min`, `all_three=42`, `dropped=10`, `fake_done=0`
- Current runtime bottleneck remains page fetching, not discovery or LLM latency.
- Frozen default knobs on the current branch:
  - concurrency: `52`
  - `REP_PAGE_LIMIT=5`
  - `EMAIL_PAGE_SOFT_LIMIT=8`
  - `EMAIL_PAGE_HARD_LIMIT=16`
  - `PAGE_TOTAL_HARD_LIMIT=20`
  - `EMAIL_STOP_SAME_DOMAIN_COUNT=2`
- Landed behavior on the frozen branch:
  - representative extraction now runs after primary fetch and before deciding email overflow
  - homepage gets an explicit primary slot and can reuse discovery HTML instead of being re-fetched
  - representative pages are scanned by email rules and successful runs no longer write broad negative-token pollution

## Design Decision

Do **not** implement a dumb global “max pages = 12” cutoff.

Implement a **budgeted fetch planner** instead:

- Representative page budget stays strong and stable.
- Email page budget starts small and only expands if needed.
- Homepage HTML from discovery is reused instead of re-fetching.
- Representative pages are also scanned by email rules.
- Once enough evidence exists, stop expanding fetch scope.

This keeps value density high. It reduces waste without throwing away leader pages or hidden email pages too early.

---

## File Map

**Modify**
- [config.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/config.py)
- [llm_client.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/llm_client.py)
- [page_pool.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/page_pool.py)
- [protocol_client.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/protocol_client.py)
- [service.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/service.py)
- [value_rules.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/value_rules.py)
- [global_learning.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/runtime/global_learning.py)
- [.env.example](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/.env.example)

**Reuse existing tests**
- [test_core.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_core.py)
- [test_global_learning.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_global_learning.py)
- [test_discovery_optimization.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_discovery_optimization.py)

**Create**
- [test_value_budgeting.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_value_budgeting.py)
- [test_fetch_failures.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_fetch_failures.py)

---

## Chunk 1: Page Budget And Fetch Priority

### Task 1: Add explicit budget config

**Files:**
- Modify: [config.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/config.py)
- Modify: [.env.example](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/.env.example)
- Test: [test_value_budgeting.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_value_budgeting.py)

- [x] **Step 1: Add failing config tests**

Add tests for default values and env overrides for:
- `REP_PAGE_LIMIT=5`
- `EMAIL_PAGE_SOFT_LIMIT=8`
- `EMAIL_PAGE_HARD_LIMIT=16`
- `PAGE_TOTAL_HARD_LIMIT=20`
- `EMAIL_STOP_SAME_DOMAIN_COUNT=2`

- [x] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler
python -m pytest tests/test_value_budgeting.py -q
```

Expected: missing config fields / assertion failure.

- [x] **Step 3: Add minimal config fields**

Add new `AppConfig` fields and env parsing in [config.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/config.py).

- [x] **Step 4: Update example config**

Document the new knobs in [.env.example](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/.env.example).

- [x] **Step 5: Run config tests again**

Expected: PASS.


### Task 2: Build a value-aware fetch plan

**Files:**
- Modify: [value_rules.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/value_rules.py)
- Create: [test_value_budgeting.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_value_budgeting.py)

- [x] **Step 1: Write failing planner tests**

Cover these cases:
- rep pages always preserved up to limit
- email pages are capped by soft budget first
- total target URLs never exceed hard limit
- homepage keeps priority if selected
- duplicate rep/email URL only counted once

- [x] **Step 2: Run planner tests and confirm failure**

Run:

```bash
cd /Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler
python -m pytest tests/test_value_budgeting.py -q
```

- [x] **Step 3: Add planner helper**

In [value_rules.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/value_rules.py), add a helper with a clear boundary, for example:

```python
def build_fetch_plan(
    start_url: str,
    rep_urls: list[str],
    email_urls: list[str],
    *,
    rep_limit: int,
    email_soft_limit: int,
    email_hard_limit: int,
    total_hard_limit: int,
) -> dict[str, list[str]]:
    ...
```

Expected output shape:
- `rep_urls`
- `email_primary_urls`
- `email_overflow_urls`
- `all_primary_urls`

- [x] **Step 4: Keep implementation DRY**

Do not add multiple parallel planners. One planner only.

- [x] **Step 5: Run planner tests**

Expected: PASS.


### Task 3: Fetch representative pages first, then email overflow pages

**Files:**
- Modify: [service.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/service.py)
- Test: [test_core.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_core.py)
- Test: [test_value_budgeting.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_value_budgeting.py)

- [x] **Step 1: Write failing service tests**

Add tests for:
- rep fetch happens before email overflow fetch
- rep pages are available to LLM even when email overflow is skipped
- early-stop prevents phase-2 email fetch when enough evidence already exists

- [x] **Step 2: Refactor `process()` into smaller helpers**

Split the current long path in [service.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/service.py) into helpers such as:
- `_plan_fetch_targets(...)`
- `_fetch_primary_pages(...)`
- `_fetch_email_overflow_pages(...)`
- `_should_stop_after_primary_fetch(...)`

- [x] **Step 3: Implement two-phase fetch**

Rules:
- fetch rep pages + homepage + primary email pages first
- run representative extraction after phase 1
- only fetch email overflow pages if same-domain email evidence is still weak

- [x] **Step 4: Protect total budget**

Never exceed `PAGE_TOTAL_HARD_LIMIT`.

- [x] **Step 5: Run focused tests**

Run:

```bash
cd /Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler
python -m pytest tests/test_value_budgeting.py tests/test_core.py -q
```

Expected: PASS.

---

## Chunk 2: Reuse Existing HTML And Recover More Value

### Task 4: Reuse homepage HTML from discovery

**Files:**
- Modify: [service.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/service.py)
- Test: [test_core.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_core.py)

- [x] **Step 1: Write failing test**

Test that when homepage HTML already exists from discovery, it is injected into the page map and not re-fetched.

- [x] **Step 2: Implement homepage reuse**

Reuse the discovery-stage homepage HTML before calling `protocol.fetch_pages(...)`.

- [x] **Step 3: Recompute metrics correctly**

`target_url_count` stays logical, but `fetched_page_count` should reflect actual network-fetched pages plus reused cached pages if they are consumed downstream.

- [x] **Step 4: Run test**

Expected: PASS.


### Task 5: Scan representative pages for emails too

**Files:**
- Modify: [service.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/service.py)
- Modify: [email_rules.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/email_rules.py)
- Test: [test_core.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_core.py)

- [x] **Step 1: Write failing test**

Test that an email present only on a representative page is still kept by rules.

- [x] **Step 2: Merge email source pages safely**

Use:
- `email_pages`
- plus `rep_pages` not already in `email_pages`

Keep rule extraction only. Do not use LLM for emails.

- [x] **Step 3: Add same-domain early-stop**

If same-domain email count already reaches `EMAIL_STOP_SAME_DOMAIN_COUNT`, skip email overflow fetch.

- [x] **Step 4: Run test**

Expected: PASS.

---

## Chunk 3: Clean The Learning Loop

### Task 6: Remove negative-token pollution from successful runs

**Files:**
- Modify: [service.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/service.py)
- Test: [test_global_learning.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_global_learning.py)

- [x] **Step 1: Write failing tests**

Cover:
- successful representative extraction should not mark sibling fetched pages as negative
- failed extraction may still record negative evidence
- email success should not penalize other fetched email pages from the same successful run

- [x] **Step 2: Change feedback semantics**

Recommended rule:
- if there is positive evidence, write positive tokens only
- only write broad negative tokens on full failure

- [x] **Step 3: Make email source typing explicit**

Pass `list(email_sources.keys())` instead of relying on dict iteration side effects.

- [x] **Step 4: Run tests**

Expected: PASS.


### Task 7: Add optional learning decay

**Files:**
- Modify: [global_learning.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/runtime/global_learning.py)
- Test: [test_global_learning.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_global_learning.py)

- [x] **Step 1: Write failing tests**

Test that stale high-score features decay after the chosen window and that recent features stay untouched.

- [x] **Step 2: Implement a minimal decay rule**

Keep it simple:
- decay only positive scores
- run lazily inside `load_scores()`
- do not add a background job

- [x] **Step 3: Run tests**

Expected: PASS.

---

## Chunk 4: Discovery Overlap And Final Tuning

**Status:** Deferred after benchmark gate. Task 8 is not executed in the frozen solution because benchmark evidence shows page fetching is still the primary bottleneck.

### Task 8: Overlap sitemap and related discovery work

**Files:**
- Modify: [service.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/extractor/service.py)
- Test: [test_discovery_optimization.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/tests/test_discovery_optimization.py)

- [ ] **Step 1: Write failing discovery tests**

Cover:
- sitemap and related-subdomain work can overlap after homepage HTML is available
- enough homepage coverage cancels extra work
- final URL merge still preserves determinism

- [ ] **Step 2: Implement bounded overlap**

Do not launch unbounded futures. One small executor or one shared background future is enough.

- [ ] **Step 3: Run tests**

Expected: PASS.


### Task 9: Rerun clean benchmark and decide default values

**Files:**
- Modify: [config.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/config.py)
- Modify: [.env.example](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/.env.example)
- Output: `output/runtime/benchmark_after_budget_top100/`

- [x] **Step 1: Benchmark `44` and `52` on the same current node**

Use the fixed code only. Same 100-row input slice. Same learning seed.

- [x] **Step 2: Record these metrics**

For both:
- `wall_seconds`
- `sites_per_minute`
- `company_hit`
- `rep_hit`
- `email_hit`
- `all_three`
- `dropped`
- `fake_done`

- [x] **Step 3: Acceptance rule**

Select the new default only if:
- `fake_done=0`
- `all_three` is not worse than current best by more than `2`
- `dropped` does not regress by more than `2`
- throughput is measurably higher

- [x] **Step 4: Freeze the new defaults**

Update default concurrency and page-budget knobs in:
- [config.py](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/src/oldironcrawler/config.py)
- [.env.example](/Users/Zhuanz1/Develop/Masterpiece/System/OldIronCrawler/.env.example)

---

## Recommended Execution Order

1. Chunk 1
2. Chunk 2
3. Chunk 3
4. Benchmark once
5. Chunk 4 only if speed is still not good enough
6. Freeze the solution when benchmark shows `52` is still best without quality regression

## What Not To Do In This Plan

- Do not add browser-based anti-bot bypass logic for rare sites.
- Do not add DNS prefetch now.
- Do not add a giant hard-coded page blacklist system.
- Do not collapse this into one huge refactor; each chunk must stay independently testable.

## Expected Outcome

Frozen outcome on the current branch:
- fewer low-value page fetches per site
- less representative-page starvation
- faster average completion time under `52` concurrency
- better email recall from representative pages
- no more learning pollution from successful runs
- no fake done in the benchmark gate

The key target is not “fewer requests at all costs”.  
The key target is “more value per request”.
