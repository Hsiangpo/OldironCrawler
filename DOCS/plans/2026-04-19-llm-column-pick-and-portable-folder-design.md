# LLM Website Column Pick and Portable Folder Design

## Problem

Two delivery gaps remain:

- Excel imports still rely on weak column guessing. Mixed sheets can contain website, social, email, and note columns together.
- Packaging currently produces a single EXE in `dist/`, but the required handoff is a ready-to-send folder under `dist\OldIronCrawler\`.

The user wants the crawler to identify the real website column automatically, even in messy sheets, and to ship a folder that can be zipped manually and sent directly to non-technical users.

## Goals

- Use one LLM call per imported table to identify the most likely website column.
- Keep local guardrails so obvious social/email/note columns are not selected by mistake.
- Automatically select the best column without prompting the user.
- Produce a distributable folder at `dist\OldIronCrawler\` that already contains the EXE, `.env`, `websites\`, and `output\` structure.

## Non-Goals

- Do not ask the user to resolve ambiguous columns manually.
- Do not generate the final zip automatically.
- Do not ship the current local website input files to the boss.
- Do not change crawl extraction behavior beyond import-column selection.

## Chosen Approach

Use a hybrid selector:

1. summarize each column locally
2. ask the LLM to choose the website column from those summaries
3. combine the LLM answer with strong local penalties for social, email, and note-like columns
4. select the highest final score automatically

This keeps the selection smarter than rules alone while still protecting against obvious bad picks.

## Column Selection Design

### 1. Column summaries

For each column in the first worksheet:

- header text
- sample non-empty values
- counts for values that look like website, email, social URL, or plain text
- share of rows that normalize into a website

The LLM should only see compact summaries, not the full workbook.

### 2. LLM decision

The LLM should return:

- selected column index
- confidence
- short reason

The prompt should state clearly that:

- the target is the company official website column
- social URLs, email columns, and notes must be rejected
- when several URL-like columns exist, the company main site wins over social/profile/support links

### 3. Local guardrails

Local scoring should still apply after the LLM returns:

- strong penalty for email-heavy columns
- strong penalty for social-domain-heavy columns
- moderate penalty for note-like text columns
- positive score for normalized homepage-like values

If the LLM picks a bad-looking column, the local penalties can still prevent it from winning.

### 4. Runtime feedback

When the selected source is CSV/XLSX, print a short line like:

- selected column header
- confidence
- reason

This gives immediate visibility without blocking the run.

## Packaging Folder Design

The build output should end as:

- `dist\OldIronCrawler\OldIronCrawler.exe`
- `dist\OldIronCrawler\.env`
- `dist\OldIronCrawler\websites\`
- `dist\OldIronCrawler\output\delivery\`
- `dist\OldIronCrawler\output\runtime\`

The packaged `.env` should be runnable but should not carry a fixed LLM key, because the program now asks for one at startup.

The package folder should not include the developer's local input workbooks. It should contain empty-but-preserved directories, using small placeholder files where needed so manual zipping does not drop them.

## Verification

- unit tests for website-column summaries and ranking
- unit test for LLM-selected column surviving mixed-column sheets
- unit test for social/email columns losing even when URL-like
- packaging verification that `dist\OldIronCrawler\` contains the expected files and directories
- real run verification using the packaged folder

## Acceptance Criteria

- mixed Excel sheets choose the correct company website column automatically
- obvious LinkedIn/email/note columns are not selected as the import source
- the importer logs which column was selected and why
- build output is a ready-to-zip folder at `dist\OldIronCrawler\`
- the folder is runnable after sending, with users only needing to input their LLM key
