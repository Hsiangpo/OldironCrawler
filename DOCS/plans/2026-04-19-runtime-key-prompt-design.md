# Runtime LLM Key Prompt Design

## Problem

The current runtime still reads the LLM key from `.env`. That is no longer desired.

The new behavior must be:

- every launch prompts for an LLM key first
- the key is valid only for the current run
- the key is not written back to disk
- the input should be masked with `*`
- empty input should loop until a value is entered
- `Ctrl+C` should exit cleanly without a traceback

This behavior must work in both source execution and the packaged console EXE.

## Goals

- Prompt for the LLM key before the file-selection menu appears.
- Keep the key in memory only for the current process.
- Mask input as `*`, including basic backspace support.
- Remove `KeyboardInterrupt` tracebacks from interactive cancellation paths.
- Keep `.env` for all other runtime settings.

## Non-Goals

- Do not save the key to `.env`.
- Do not add a GUI credential dialog.
- Do not change crawl logic or extraction rules.
- Do not add key validation logic beyond existing runtime validation.

## Chosen Approach

Use a small interactive console helper plus an explicit config override.

The runtime flow becomes:

1. bootstrap stdio and runtime root
2. prompt for LLM key
3. import the app and run with an in-memory key override

This is better than silently falling back to `.env` because the startup behavior stays consistent across both `python run.py` and `OldIronCrawler.exe`.

## Runtime Design

### 1. Console secret input helper

Add a dedicated console helper module that reads one character at a time and writes `*` for each accepted character.

Required behavior:

- printable character -> append and print `*`
- backspace -> remove one stored character and erase one `*`
- enter -> submit
- `Ctrl+C` -> raise `KeyboardInterrupt`
- empty or whitespace-only input -> show a short Chinese retry message and prompt again

The helper should support Windows and non-Windows terminals:

- Windows: `msvcrt.getwch()`
- POSIX fallback: `tty` + `termios`

### 2. In-memory key override

`AppConfig.load()` should accept an optional `llm_key_override`.

Resolution order should become:

1. explicit runtime override
2. `.env` `LLM_KEY`
3. `.env` `LLM_API_KEY`

That keeps current compatibility while ensuring the prompted key wins.

### 3. Startup order

`run.py` should prompt for the key before calling the interactive app.

The main entrypoint should:

- bootstrap runtime root and UTF-8
- prompt for the key
- call `run_interactive(project_root, llm_key_override=...)`

This guarantees the file menu and all crawler work happen only after the run-specific key is available.

### 4. Clean cancellation

`run.py` should catch `KeyboardInterrupt` and print a short Chinese cancellation message instead of showing a traceback.

This must apply to:

- key prompt cancellation
- file-selection cancellation
- later interactive cancellation in the same process

## Test Strategy

### Automated tests

- prompt helper masks characters with `*`
- prompt helper supports backspace
- prompt helper loops on empty input
- `AppConfig.load()` prefers explicit key override
- `run.main()` returns a clean cancellation code when key prompt raises `KeyboardInterrupt`

### Real verification

- run `python run.py`, confirm the key prompt appears before the file menu
- run the EXE, confirm the same startup order
- confirm `Ctrl+C` exits without a traceback in both modes

## Acceptance Criteria

- Startup always asks for an LLM key first.
- Empty key input loops until a non-empty key is entered.
- Input displays `*` characters while typing.
- The key is not written to `.env`.
- `Ctrl+C` exits cleanly with a short message and no traceback.
- The packaged EXE keeps the same behavior.
