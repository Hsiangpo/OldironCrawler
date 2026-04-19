from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from openpyxl import load_workbook


_SUPPORTED_SUFFIXES = {".txt", ".csv", ".xlsx"}
_HEADER_CANDIDATES = ("website", "url", "domain", "homepage")


@dataclass
class ImportedWebsite:
    input_index: int
    raw_website: str
    website: str
    dedupe_key: str


def list_input_files(websites_dir: Path) -> list[Path]:
    if not websites_dir.exists():
        return []
    items = [path for path in websites_dir.iterdir() if path.is_file() and path.suffix.lower() in _SUPPORTED_SUFFIXES]
    return sorted(items, key=lambda item: item.name.lower())


def choose_input_file(websites_dir: Path) -> Path:
    files = list_input_files(websites_dir)
    if not files:
        raise RuntimeError(f"没有找到输入文件，请把 txt/csv/xlsx 放到 {websites_dir}")
    name_map = {path.name.lower(): path for path in files}
    print("可用输入文件：")
    for index, path in enumerate(files, start=1):
        print(f"  {index}. {path.name}")
    while True:
        raw = input("请输入文件序号: ").strip()
        matched = name_map.get(raw.lower())
        if matched is not None:
            return matched
        try:
            choice = int(raw)
        except ValueError:
            print("输入无效，请重新输入序号或文件名。")
            continue
        if 1 <= choice <= len(files):
            return files[choice - 1]
        print("序号超出范围，请重新输入。")


def compute_file_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def compute_rows_fingerprint(rows: list[ImportedWebsite]) -> str:
    payload = [
        {
            "input_index": row.input_index,
            "website": row.website,
            "dedupe_key": row.dedupe_key,
        }
        for row in rows
    ]
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()


def load_websites(path: Path) -> list[ImportedWebsite]:
    suffix = path.suffix.lower()
    if suffix == ".txt":
        rows = _load_from_txt(path)
    elif suffix == ".csv":
        rows = _load_from_csv(path)
    elif suffix == ".xlsx":
        rows = _load_from_xlsx(path)
    else:
        raise RuntimeError(f"不支持的文件类型: {path.suffix}")
    return _dedupe_websites(rows)


def _load_from_txt(path: Path) -> list[str]:
    rows: list[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        text = str(line or "").strip()
        if text:
            rows.append(text)
    return rows


def _load_from_csv(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    return _load_from_matrix(rows)


def _load_from_xlsx(path: Path) -> list[str]:
    workbook = load_workbook(filename=path, read_only=True, data_only=True)
    try:
        sheet = workbook.active
        rows = [list(row) for row in sheet.iter_rows(values_only=True)]
    finally:
        workbook.close()
    return _load_from_matrix(rows)


def _load_from_matrix(rows: list[list[object]]) -> list[str]:
    if not rows:
        return []
    header_index = _find_header_index(rows[0])
    if header_index is not None:
        return _load_from_column(rows[1:], header_index)
    guess_index = _find_first_website_like_column(rows)
    if guess_index is None:
        return []
    return _load_from_column(rows, guess_index)


def _find_header_index(first_row: list[object]) -> int | None:
    for index, value in enumerate(first_row):
        lowered = str(value or "").strip().lower()
        if lowered in _HEADER_CANDIDATES:
            return index
    return None


def _find_first_website_like_column(rows: list[list[object]]) -> int | None:
    max_width = max((len(row) for row in rows), default=0)
    best_index: int | None = None
    best_score = 0
    for column in range(max_width):
        score = 0
        for row in rows[:50]:
            if column >= len(row):
                continue
            if _normalize_website(str(row[column] or "")):
                score += 1
        if score > best_score:
            best_score = score
            best_index = column
    return best_index if best_score > 0 else None


def _load_from_column(rows: list[list[object]], column: int) -> list[str]:
    result: list[str] = []
    for row in rows:
        if column >= len(row):
            continue
        text = str(row[column] or "").strip()
        if text:
            result.append(text)
    return result


def _dedupe_websites(rows: list[str]) -> list[ImportedWebsite]:
    results: list[ImportedWebsite] = []
    seen: set[str] = set()
    for index, raw in enumerate(rows, start=1):
        website = _normalize_website(raw)
        if not website:
            continue
        dedupe_key = _build_dedupe_key(website)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        results.append(
            ImportedWebsite(
                input_index=index,
                raw_website=str(raw).strip(),
                website=website,
                dedupe_key=dedupe_key,
            )
        )
    return results


def _normalize_website(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "://" not in text:
        text = f"https://{text}"
    parsed = urlparse(text)
    host = str(parsed.netloc or parsed.path or "").strip().lower()
    if not host:
        return ""
    if host.startswith("www."):
        host = host[4:]
    path = str(parsed.path or "").strip()
    normalized = f"{parsed.scheme or 'https'}://{host}{path}"
    return normalized.rstrip("/") or ""


def _build_dedupe_key(website: str) -> str:
    parsed = urlparse(website)
    host = str(parsed.netloc or "").strip().lower()
    path = str(parsed.path or "").strip()
    if host and path not in {"", "/"}:
        return website.lower()
    if host:
        return host
    return website.lower()
