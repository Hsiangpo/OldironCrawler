# EXE Icon Design

## Goal

Replace the default PyInstaller EXE icon with the user-provided image at:

- `DOCS\webwxgetmsgimg.jpg`

The final packaged app should keep using the same image every time the build runs.

## Chosen Approach

Use the JPG only as the source asset, then generate and commit a real Windows `.ico` file under `packaging\`.

This is the most stable option because:

- Windows EXE icons are expected to be `.ico`
- PyInstaller handles `.ico` reliably
- the build stays deterministic and does not depend on runtime image conversion support on other machines

## Design

- Keep `DOCS\webwxgetmsgimg.jpg` as the visual source
- Generate `packaging\OldIronCrawler.ico` as the build resource
- Update the PyInstaller spec to reference that `.ico`
- Update the validation `--onedir` build step to use the same icon
- Rebuild `dist\OldIronCrawler\OldIronCrawler.exe`

## Verification

- test that `packaging\OldIronCrawler.ico` exists
- test that `packaging\OldIronCrawler.spec` references the icon
- rebuild the packaged app
- verify the built EXE contains icon resources

## Acceptance Criteria

- `packaging\OldIronCrawler.ico` exists in the repo
- PyInstaller uses that icon for the EXE
- `dist\OldIronCrawler\OldIronCrawler.exe` is rebuilt with the custom icon
