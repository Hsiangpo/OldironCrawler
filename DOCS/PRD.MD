# OldIronCrawler PRD

## 1. Product Goal

OldIronCrawler is a lightweight global official-website crawler.

The user provides a file that contains many company websites. The software reads the file, crawls each official website through protocol requests, extracts:

- company name
- representative
- all public emails
- website

and finally delivers a CSV.

This product is not a country-specific crawler. It is a generic website-input software built on top of the existing shared `protocol + LLM` capabilities already implemented in:

- `/Users/Zhuanz1/Develop/Masterpiece/Spider/Website/OldIron/shared/oldiron_core/protocol_crawler`
- `/Users/Zhuanz1/Develop/Masterpiece/Spider/Website/OldIron/shared/oldiron_core/fc_email`

The new product must reuse that logic instead of inventing a separate extraction philosophy.

## 2. Product Scope

### 2.1 In Scope

- Import website lists from `txt`, `csv`, or `xlsx`
- Auto-detect the website column
- Crawl official websites with `pc`
- Extract company name from website content
- Extract one highest-value representative from website content
- Extract all rule-valid public emails from website content
- Resume from SQLite state
- Deliver one CSV for one input file
- Show terminal progress in a human-readable format

### 2.2 Out of Scope

- Browser automation as the primary crawler
- Country-specific site adapters
- Google Maps completion
- Company directory sources
- CRM-like UI
- Extra delivery columns beyond the agreed 4 columns

## 3. Hard Product Decisions

These decisions are fixed and should be treated as product rules.

### 3.1 Crawl Strategy

- Use `pc` only
- Do not use browser automation for normal collection
- Browser tooling may only be used later for debugging or inspection, not as the production path

### 3.2 Data Source

- Input files are placed under `/websites`
- The software runs against one selected input file at a time
- The input may contain only websites and no company names

### 3.3 Extraction Strategy

- Company name comes from website content
- If the website does not show a formal legal entity name, use the brand name
- Representative is selected by `LLM`
- Representative must be a single highest-value person only
- Emails are extracted by rules only
- `LLM` must not be used to generate or supplement emails
- Representative selection must continue to use `html -> markdown`
- Representative prompt should keep the current shared strictness:
  - no guessing
  - no name splitting from company name
  - must use website evidence
  - if uncertain, leave empty

### 3.4 Delivery Policy

- Deliver all records
- Do not require all fields to be non-empty before delivery
- Final CSV contains exactly 4 columns:
  - `company_name`
  - `representative`
  - `emails`
  - `website`

### 3.5 Display Policy

When a field is not found:

- terminal displays `未找到`
- CSV stores an empty string

This applies to:

- company name
- representative
- emails

### 3.6 Email Output Format

- Multiple emails are joined by `; `
- No upper limit on the number of kept emails
- Continue using the current shared email filtering philosophy:
  - keep real personal emails
  - keep real company emails
  - keep real offsite but meaningful emails
  - remove only obviously fake / placeholder / script-noise / directory-noise emails

### 3.7 Deduplication Policy

- Deduplicate by website first
- If only a bare domain is available, deduplicate by domain
- Different websites are treated as different records, even if the company appears related
- Do not merge records only because the company name looks the same

### 3.8 Output Order

- Final CSV must follow input order
- Do not reorder by completion time

## 4. Runtime UX

### 4.1 Startup Style

The first version uses interactive startup.

Example expectation:

1. start program
2. show files under `/websites`
3. user chooses one file
4. software runs against that file

Non-interactive CLI may be added later, but interactive mode is the first-class UX for now.

### 4.2 Terminal Output Format

The terminal output must follow this style:

```text
[378/97094] fntl.co.uk
  公司名: FNTL
  姓名: NADEEM KHAN
  邮箱: office@fntl.co.uk
  ------------------------------------------------------------
```

If a field is missing:

```text
[380/97094] 3-sphere.co.uk
  公司名: 未找到
  姓名: 未找到
  邮箱: enquiries@3-sphere.co.uk
  ------------------------------------------------------------
```

## 5. Input Rules

### 5.1 Supported Input Types

- `.txt`
- `.csv`
- `.xlsx`

### 5.2 Website Column Detection

For structured files:

1. Prefer headers named:
   - `website`
   - `url`
   - `domain`
   - `homepage`
2. If no header matches, choose the first column that looks like websites

For text files:

- each non-empty line is treated as one website candidate

### 5.3 Input Assumption

- The file may contain websites only
- The software must not require company name as input

## 6. State And Resume Model

### 6.1 One Input File, One SQLite

Each input file has its own exclusive SQLite runtime database.

Paths:

- input file: `/websites/xxx.csv`
- runtime db: `/output/runtime/xxx.sqlite3`
- delivery file: `/output/delivery/xxx.csv`

### 6.2 File Fingerprint Rule

The runtime DB belongs to both:

- the input filename
- the input file content fingerprint

If the filename is the same but the file content has changed:

- the old runtime DB is treated as stale
- the system rebuilds a fresh DB for the new content

### 6.3 Resume Requirement

Resume is a hard requirement.

The system must support:

- stopping and continuing later
- not reprocessing completed records
- preserving input order identity
- retrying temporary failures

### 6.4 Internal Metadata

The final CSV only contains 4 columns, but the SQLite database may store internal fields such as:

- normalized website
- evidence url
- evidence quote
- raw extracted company name
- status
- retry count
- input row number

These internal fields exist for resume and debugging only.

## 7. Concurrency

### 7.1 Website-Level Concurrency

- Run up to `64` websites in parallel

### 7.2 Intra-Website Page Concurrency

- Run up to `8` page fetches in parallel inside one website

### 7.3 Lightweight Principle

The product should stay lightweight.

Do not add heavy coordination systems, unnecessary background services, or overdesigned patch layers.

## 8. Failure And Retry Rules

### 8.1 Temporary Failures

The following are temporary failures:

- `429`
- `5xx`
- `timeout`
- `TLS EOF`

Handling:

- do not discard data immediately
- use exponential backoff
- keep retrying within the current wait budget

### 8.2 Long-Failure Handling

If total wait budget is exhausted:

- mark the record as `failed_temp`
- move it to the queue tail
- retry it one more time later
- if it fails again, discard it from active processing

This means:

- one delayed retry after queue tail
- no endless hot-loop retry for hopeless targets

### 8.3 Dead Site Cases

Examples:

- domain does not exist
- website remains unavailable for a long period
- impossible connection after the delayed retry

These can be treated as final dropped records.

## 9. Proxy Rule

The software should remain simple and automatic.

Expected behavior:

- if the system proxy is available, use proxy
- if no VPN / proxy is active, use direct connection

Recommended runtime behavior:

1. if `.env` explicitly defines proxy, use it
2. otherwise probe the common local proxy port
3. if proxy is reachable, use it
4. if proxy is not reachable, use direct mode

## 10. LLM Configuration

### 10.1 Active Provider

Current agreed runtime configuration:

- `LLM_BASE_URL=https://api.gpteamservices.com/v1`
- `LLM_MODEL=gpt-5.4-mini`
- `LLM_REASONING_EFFORT=low`
- `LLM_API_STYLE=responses`

### 10.2 Secret Storage

Sensitive values must stay in project root `.env`.

The real key must not be committed into tracked source files or docs.

### 10.3 Environment Boundary

`.env` is the source of truth for:

- `LLM` base url
- `LLM` key
- model
- reasoning effort
- proxy
- concurrency
- timeout

Docs only describe variable names and behavior.

## 11. Representative Page Selection

Representative extraction uses a two-stage page selection system.

### 11.1 Goal

Select the 5 most valuable pages for:

- company name
- representative

Then:

- fetch full HTML
- convert to markdown
- trim total prompt size below `250K`
- send the selected pages to `LLM` in one request

### 11.2 Rule-First Strategy

First use rules to rank sitemap / discovered links.

The current direction should continue:

- start from sitemap if available
- otherwise extract links from homepage
- score links by known valuable path tokens

### 11.3 LLM-As-Teacher Strategy

If the rules select fewer than 5 strong pages:

- let `LLM` choose additional high-value pages from a larger candidate set
- write the newly useful path tokens back into the learned rule pool

### 11.4 Weighted Learning Strategy

The learned rule pool should be weighted, not just a flat set.

Each learned token stores a score, for example:

```json
{
  "about": 17,
  "company": 15,
  "team": 23,
  "leadership": 29
}
```

When a token is confirmed useful:

- increase its weight by `+1`

This lets rule selection improve over time.

### 11.5 Do Not Over-Prune

Do not shrink the learned token pool down to only 5 items.

The pool should remain larger, for example top 100 or top 200 learned tokens, so long-tail useful paths are not destroyed by short-term samples.

## 12. Email Page Selection

Email extraction is different from representative extraction.

### 12.1 Rule-Only Strategy

- Email extraction does not use `LLM`
- Email extraction uses all matched high-value pages

### 12.2 No Artificial 5-Page Limit

If a website has:

- 140 discovered links
- 20 links that match email-value rules

then all 20 matched pages should be fetched for email extraction.

### 12.3 Separate Rule Pool

Representative pages and email pages should not share exactly the same learned rule pool.

They overlap, but they are not identical.

Recommended separation:

- representative token pool
- email token pool

Typical examples:

- representative pages:
  - `about`
  - `company`
  - `team`
  - `leadership`
  - `management`
  - `board`
  - `founder`
- email pages:
  - `contact`
  - `support`
  - `help`
  - `privacy`
  - `legal`
  - `terms`
  - `about`

`privacy / legal / terms` are explicitly allowed for email extraction.

## 13. Extraction Semantics

### 13.1 Company Name

- extract from official website content
- if no formal legal name is found, use the brand name

### 13.2 Representative

- choose one single highest-value person only
- do not return multiple executives
- must not hallucinate
- if uncertain, leave empty

### 13.3 Emails

- extract all valid public emails from all matched email-value pages
- do not cap the number
- join with `; `

## 14. Delivery File

Final CSV schema:

```csv
company_name,representative,emails,website
```

Rules:

- output follows input order
- empty fields remain empty strings in CSV
- final file path: `/output/delivery/xxx.csv`

## 15. Non-Goals For V1

To keep the product lightweight, V1 should not include:

- multi-user accounts
- web dashboard
- country adapters
- distributed job orchestration
- extra evidence columns in delivery CSV
- complicated rule editors
- browser automation fallback collection

## 16. Implementation Principle

The implementation should copy the proven shared logic from the existing OldIron codebase and adapt it into a generic website-input product.

The new product may improve the current shared logic, but it must not weaken these already-agreed behaviors:

- `pc` first
- representative via `LLM`
- emails via rules
- strict anti-hallucination for representative
- lightweight runtime
- resumable SQLite state
- simple terminal UX

