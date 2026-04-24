from __future__ import annotations

import html
import re
from collections.abc import Iterable
from urllib.parse import unquote


_SCRIPT_BLOCK_RE = re.compile(r"(?is)<(script|style|template)\b[^>]*>.*?</\1>")
_TEL_HREF_RE = re.compile(r"(?is)tel:\s*([^\"'<>\s]+)")
_PHONE_CONTEXT_RE = re.compile(
    r"(?is)(?:\b(?:tel(?:ephone)?|telefon|phone|hotline|mobile|mob|call us|call)\b\s*(?:[:=]|is|号码|电话)?\s*|tel:\s*)([+]?\d[\d\s()./\-]{5,}\d)"
)
_PHONE_JSON_RE = re.compile(
    r"(?is)['\"]?(?:telephone|phone|tel|mobile|mob|contact_number|contactNumber)['\"]?\s*[:=]\s*['\"]?([+]?\d[\d\s()./\-]{5,}\d)"
)
_FAX_HINT_RE = re.compile(r"(?i)\bfax\b")
_EXTENSION_RE = re.compile(r"(?i)(?:ext\.?|extension|durchwahl)\s*\d+")
_SENTINEL_NUMBERS = {
    "2147483647",
}


def split_phones(values: Iterable[str] | str) -> list[str]:
    items = re.split(r"[;,]", values) if isinstance(values, str) else list(values)
    result: list[str] = []
    seen: set[str] = set()
    for raw in items:
        phone = normalize_phone_candidate(raw)
        if not phone:
            continue
        signature = _phone_signature(phone)
        if signature in seen:
            continue
        seen.add(signature)
        result.append(phone)
    return _drop_prefixed_extension_variants(result)


def join_phones(values: Iterable[str] | str) -> str:
    return "; ".join(split_phones(values))


def collect_phones_for_pages(pages: list[tuple[str, str]]) -> tuple[list[str], dict[str, list[str]]]:
    collected: list[str] = []
    seen: set[str] = set()
    page_hits: dict[str, list[str]] = {}
    for url, html_text in pages:
        page_phones = split_phones(
            [
                *extract_phones_from_html(html_text),
                *extract_phones_from_embedded_content(html_text),
            ]
        )
        if page_phones:
            page_hits[url] = page_phones
        for phone in page_phones:
            signature = _phone_signature(phone)
            if signature in seen:
                continue
            seen.add(signature)
            collected.append(phone)
    return collected, page_hits


def extract_phones_from_html(raw_html: str) -> list[str]:
    text = str(raw_html or "")
    if not text.strip():
        return []
    visible = html.unescape(_SCRIPT_BLOCK_RE.sub(" ", text))
    found: list[str] = []
    for match in _TEL_HREF_RE.findall(text):
        _append_phone(found, match)
    for match in _PHONE_CONTEXT_RE.findall(visible):
        _append_phone(found, match)
    return found


def extract_phones_from_embedded_content(raw_html: str) -> list[str]:
    text = html.unescape(str(raw_html or ""))
    if not text.strip():
        return []
    found: list[str] = []
    for match in _TEL_HREF_RE.findall(text):
        _append_phone(found, match)
    for match in _PHONE_JSON_RE.findall(text):
        _append_phone(found, match)
    return found


def normalize_phone_candidate(value: object) -> str:
    text = html.unescape(unquote(str(value or ""))).strip()
    if not text:
        return ""
    text = re.sub(r"(?i)^tel:\s*", "", text)
    text = text.split("?", 1)[0].split("#", 1)[0]
    text = _EXTENSION_RE.split(text, maxsplit=1)[0]
    text = re.sub(r"\s+", " ", text).strip(" ,.;")
    if not text or _FAX_HINT_RE.search(text):
        return ""
    has_plus = text.startswith("+") or text.startswith("00")
    digits = re.sub(r"\D", "", text)
    if has_plus and digits.startswith("00"):
        digits = digits[2:]
    if not digits or digits in _SENTINEL_NUMBERS:
        return ""
    if len(digits) < 7 or len(digits) > 15:
        return ""
    if re.fullmatch(r"(?:19|20)\d{11,}", digits):
        return ""
    if len(digits) >= 13 and not has_plus and not digits.startswith("0"):
        return ""
    if len(set(digits)) == 1:
        return ""
    return f"+{digits}" if has_plus else digits


def _append_phone(result: list[str], value: object) -> None:
    phone = normalize_phone_candidate(value)
    if not phone:
        return
    signature = _phone_signature(phone)
    if signature in {_phone_signature(item) for item in result}:
        return
    result.append(phone)


def _drop_prefixed_extension_variants(values: list[str]) -> list[str]:
    signatures = {_phone_signature(value): value for value in values}
    drop_signatures: set[str] = set()
    for left in signatures:
        for right in signatures:
            if left == right:
                continue
            if right.startswith(left) and 1 <= len(right) - len(left) <= 4:
                drop_signatures.add(right)
    return [value for value in values if _phone_signature(value) not in drop_signatures]


def _phone_signature(value: str) -> str:
    return re.sub(r"\D", "", str(value or "").strip())
