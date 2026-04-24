# High-Concurrency Stability and Phones Extraction Design

## Problem

The crawler currently has two classes of failures:

1. Runtime stability failures under high concurrency:
   - runs can degrade into heartbeat-only periods with no new site results
   - fatal LLM configuration failures do not stop cleanly
   - temporary protocol failures and challenge handling can consume the entire site budget
   - packaged runtime behavior diverges from source runtime

2. Extraction quality gaps:
   - German official sites rely heavily on `impressum` and `kontakt`, but current scoring is still too English-centric
   - shell-page enrichment does not cover overflow email pages
   - same-domain contact signals hidden in embedded content can be skipped
   - the product currently extracts only emails, not phones

The user requirement is explicit:

- keep high concurrency
- fix the backend instead of lowering concurrency
- treat `Impressum` as a first-class representative/contact signal for German sites
- add a `phones` field and keep all rule-valid public phones, similar to `emails`

## Goals

- Remove the main causes of heartbeat-only stalls and restart-to-recover behavior.
- Preserve high concurrency while making failure handling bounded and observable.
- Make German `impressum` / `kontakt` / related legal-contact pages rank correctly.
- Add rule-based phone extraction end-to-end:
  - SQLite
  - terminal output
  - CSV delivery
- Keep LLM limited to company name and representative extraction.

## Non-Goals

- Do not switch production crawling to browser automation.
- Do not reduce configured concurrency as the primary fix.
- Do not ask the LLM to invent or supplement phones.
- Do not add country-specific adapters beyond path/token weighting and general rules.

## Chosen Approach

Use a layered repair strategy:

1. Fix runtime state and pool behavior so the worker system can fail fast without losing completed work.
2. Tighten protocol and challenge handling so blocked pages are classified correctly and fallback paths stay inside the site budget.
3. Extend the rule-extraction pipeline from "emails only" to "contact signals", adding `phones` with the same join-and-keep-all policy.
4. Strengthen German-path scoring, with `impressum` treated as both representative and contact evidence.
5. Align packaged runtime with source runtime so the `dist` build preserves challenge-solving and operator configuration.

This approach keeps the current architecture intact, avoids product sprawl, and fixes the specific field failures the user reported.

## Runtime Design

### 1. Fatal LLM failure handling

`run_crawl_session()` should stop accepting new work immediately when a fatal LLM configuration failure is detected, but it must also:

- persist already-finished futures before exiting
- mark interrupted work deterministically
- avoid waiting silently until every started site exhausts its full deadline

The shutdown path must remain observable to the operator. If a fatal key/configuration error happens, the user should get a promptable recovery path quickly instead of a long fake hang.

### 2. Page pool timeout behavior

Timed-out batches must not keep occupying shared page-pool capacity after the caller has already given up on them.

The pool should:

- close the batch
- stop scheduling more work for the batch
- cancel queued futures that have not started
- stop stale completions from blocking later sites

This directly targets the "heartbeat but no new output" symptom.

### 3. Retry classification and bounded recovery

Temporary LLM and protocol failures need bounded, explicit recovery rules:

- honor site deadlines and do not re-run `site_deadline_exceeded`
- add retry backoff for temporary LLM startup/runtime failures
- keep progress heartbeat visible during long waits
- classify hard `403` pages as blocked pages, not usable content

## Protocol and Challenge Design

### 1. 403 handling

Plain `403 Forbidden` or access-denied pages that are not recognized challenge pages must surface as blocked responses, not as normal HTML.

### 2. Discovery fallback

If the homepage fetch fails temporarily, discovery should still attempt common-value probes such as:

- `impressum`
- `kontakt`
- `contact`
- `about`
- `privacy`
- `legal`

The fallback should be skipped only when the failure is truly permanent.

### 3. Challenge solver budget accounting

Challenge-solving waits must account for real request time, not only polling intervals. The total challenge path must stay bounded by the configured solver maximum and the per-site deadline.

### 4. Challenge-type separation

Cloudflare-only solving paths must not be attempted for other challenge families such as SGCaptcha or Imperva.

## Extraction Design

### 1. German representative and contact weighting

The value rules should explicitly score German legal/contact paths, with `impressum` elevated as a top signal because many German sites place the legal entity and managing director there.

Representative-side strong signals should include:

- `impressum`
- `kontakt`
- `ueber`
- `ueber-uns`
- `uber`
- `uber-uns`
- `geschaeftsfuehrer`
- `geschaftsfuhrer`
- `managing-director`

Contact/email/phone-side strong signals should include:

- `impressum`
- `kontakt`
- `datenschutz`
- `privacy`
- `legal`
- `terms`
- `imprint`

### 2. Shell enrichment coverage

Shell-page enrichment must run for overflow contact pages too, not only the first primary batch.

### 3. Company-name fallback safety

Synthetic shell recovery headers must be ignored by company fallback extraction.

### 4. Email recovery fix

Embedded same-domain email extraction should be additive, not fallback-only. A visible offsite email must not prevent recovery of a same-domain email embedded in script/JSON/template content.

## Phones Design

### 1. Output policy

Add a new `phones` field with the same product semantics as `emails`:

- keep all rule-valid public phones
- multiple values are joined by `; `
- terminal shows `未找到` when empty
- CSV stores empty string when empty

Final delivery format becomes:

- `company_name`
- `representative`
- `emails`
- `phones`
- `website`

### 2. Extraction policy

Phones are extracted by rules only.

Primary sources:

- `tel:` links
- visible phone patterns in HTML text
- `schema.org telephone`
- JSON/script/template content
- shell-page recovered evidence

Cleaning rules:

- strip obvious separators while preserving a readable normalized output
- reject too-short numeric noise
- reject IDs, timestamps, and address/postcode artifacts
- deduplicate by normalized digit signature

The rule set should preserve international formatting where possible, while deduplicating equivalent representations.

### 3. Pipeline placement

Phone extraction should sit beside email extraction in the service pipeline:

- build the same contact-rule page set
- extract emails and phones from the same selected pages
- update stage metrics and result assembly once per contact-rule phase

This avoids a second independent crawl path.

## Storage and Delivery Design

### 1. Runtime store

Add `phones` to `SiteResult`, SQLite schema, reset paths, mark-done persistence, and delivery reads.

### 2. Reporter

Add phone display in terminal output and include `phones` in CSV writing.

### 3. Input identity safety

Use an input-specific artifact naming rule that does not collide on `input_path.stem` alone. Different suffixes or equivalent unique identifiers must produce separate runtime DB and delivery CSV paths.

## Packaging and Runtime Config Design

The portable packaging path must preserve the runtime features needed in source mode:

- `CAPSOLVER_API_KEY`
- `CAPSOLVER_PROXY`
- `CLOUDFLARE_PROXY_URL`
- operator-selected concurrency settings unless intentionally overridden

Configuration loading should accept both `.env` and process environment variables, with process environment values allowed to override file values when present.

## Test Strategy

### 1. Stability tests

- fatal LLM failure with already-running workers
- page-pool timeout does not starve later batches
- `site_deadline_exceeded` is terminal
- restart recovery reopens a real SQLite file and resets `running` rows

### 2. Protocol tests

- plain `403` returns blocked classification
- temporary homepage failure still triggers common probe discovery
- non-Cloudflare challenge does not enter Cloudflare CapSolver path
- challenge polling respects real elapsed time

### 3. Extraction tests

- German `impressum` and `kontakt` are selected
- overflow shell contact pages are enriched
- embedded same-domain email is kept even when an offsite email is visible
- synthetic shell header is not used as company name
- phone extraction keeps multiple phones and deduplicates equivalent variants

### 4. Packaging tests

- packaged `.env` keeps challenge-related keys
- config loader accepts `os.environ`
- frozen runtime smoke checks are at least covered by a stronger packaging test path

## Acceptance Criteria

- High-concurrency runs no longer degrade into long heartbeat-only gaps caused by stale page-pool work.
- Fatal LLM configuration failures surface promptly without silently waiting for the full remaining site budget.
- German sites with key data in `impressum` can select and extract representative/contact evidence more reliably.
- Output contains a new `phones` field with all rule-valid public phones.
- Packaged runtime behavior is materially aligned with source runtime for challenge solving and operator configuration.
