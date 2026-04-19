from __future__ import annotations

import html
import json
import re
from urllib.parse import urlparse

from oldironcrawler.extractor.email_rules import extract_registrable_domain


_META_TAG_RE = re.compile(r"(?is)<meta\b[^>]*>")
_ATTR_RE = re.compile(r"([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*=\s*(['\"])(.*?)\2", re.DOTALL)
_TITLE_RE = re.compile(r"(?is)<title[^>]*>(.*?)</title>")
_H1_RE = re.compile(r"(?is)<h1[^>]*>(.*?)</h1>")
_JSON_LD_RE = re.compile(r'(?is)<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>')
_CORP_TYPES = {
    "airline", "corporation", "hotel", "hotelroom", "localbusiness", "lodgingbusiness",
    "medicalbusiness", "organization", "professionalservice", "resort", "touristaccommodation",
    "travelagency",
}
_GENERIC_PREFIXES = (
    "welcome to ",
    "about ",
)
_GENERIC_COMPANY_PHRASES = (
    "company leadership",
    "contact us",
    "privacy policy",
    "terms and conditions",
    "cookie policy",
)
_INVALID_COMPANY_NAMES = {
    "account suspended",
}
_TRADING_AS_RE = re.compile(r"(?i)\btrading as\b")
_REGISTRATION_SUFFIX_RE = re.compile(
    r"\b((?:limited|ltd|llp|plc|inc|corp|corporation|company|group|kg|gmbh))\b[\s,.:;-]*\d{6,}$",
    re.IGNORECASE,
)


def extract_company_name_fallback(website: str, pages: list[tuple[str, str]]) -> str:
    site_token = _extract_site_token(website)
    candidates: list[tuple[int, str]] = []
    for url, html_text in pages:
        page_weight = _page_company_weight(url)
        candidates.extend(_extract_company_candidates_from_html(html_text, site_token, page_weight))
    if not candidates:
        return ""
    ordered = sorted(candidates, key=lambda item: (-item[0], len(item[1]), item[1].lower()))
    seen: set[str] = set()
    for _score, value in ordered:
        normalized = _normalize_company_text(value)
        if not normalized:
            continue
        dedupe = re.sub(r"[^a-z0-9]+", "", normalized.lower())
        if dedupe in seen:
            continue
        seen.add(dedupe)
        return normalized
    return ""


def clean_company_name_candidate(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html.unescape(str(value or ""))).strip()
    text = re.sub(r"\s+", " ", text)
    if not text:
        return ""
    text = _clean_trading_as_text(text)
    lowered = text.lower()
    if lowered in _INVALID_COMPANY_NAMES:
        return ""
    text = _REGISTRATION_SUFFIX_RE.sub(r"\1", text).strip(" ,.-")
    lowered = text.lower()
    if lowered in _INVALID_COMPANY_NAMES:
        return ""
    return text


def _extract_company_candidates_from_html(html_text: str, site_token: str, page_weight: int) -> list[tuple[int, str]]:
    text = str(html_text or "")
    if not text.strip():
        return []
    candidates: list[tuple[int, str]] = []
    candidates.extend(_extract_json_ld_candidates(text, site_token, page_weight))
    candidates.extend(_extract_meta_candidates(text, site_token, page_weight))
    title_match = _TITLE_RE.search(text)
    if title_match:
        candidates.append((_score_company_candidate(title_match.group(1), site_token, 10 + page_weight), title_match.group(1)))
    h1_match = _H1_RE.search(text)
    if h1_match:
        candidates.append((_score_company_candidate(h1_match.group(1), site_token, 14 + page_weight), h1_match.group(1)))
    return candidates


def _extract_json_ld_candidates(html_text: str, site_token: str, page_weight: int) -> list[tuple[int, str]]:
    candidates: list[tuple[int, str]] = []
    for raw_block in _JSON_LD_RE.findall(html_text):
        for name in _extract_names_from_json_ld(raw_block):
            candidates.append((_score_company_candidate(name, site_token, 22 + page_weight), name))
    return candidates


def _extract_meta_candidates(html_text: str, site_token: str, page_weight: int) -> list[tuple[int, str]]:
    result: list[tuple[int, str]] = []
    for raw_tag in _META_TAG_RE.findall(html_text):
        attrs = {
            key.strip().lower(): value
            for key, _quote, value in _ATTR_RE.findall(raw_tag)
        }
        content = str(attrs.get("content", "") or "").strip()
        if not content:
            continue
        prop = str(attrs.get("property", "") or attrs.get("name", "")).strip().lower()
        if prop == "og:site_name":
            result.append((_score_company_candidate(content, site_token, 20 + page_weight), content))
        elif prop in {"application-name", "twitter:title", "og:title"}:
            result.append((_score_company_candidate(content, site_token, 8 + page_weight), content))
    return result


def _extract_names_from_json_ld(raw_block: str) -> list[str]:
    payload = html.unescape(str(raw_block or "").strip())
    if not payload:
        return []
    try:
        data = json.loads(payload)
    except Exception:  # noqa: BLE001
        return []
    results: list[str] = []
    _collect_names_from_json_ld_node(data, results)
    return results


def _collect_names_from_json_ld_node(node: object, results: list[str]) -> None:
    if isinstance(node, list):
        for item in node:
            _collect_names_from_json_ld_node(item, results)
        return
    if not isinstance(node, dict):
        return
    raw_type = node.get("@type")
    types = [str(raw_type).lower()] if isinstance(raw_type, str) else [str(item).lower() for item in raw_type or []]
    if any(item in _CORP_TYPES for item in types):
        name = str(node.get("name", "") or "").strip()
        if name:
            results.append(name)
    for value in node.values():
        _collect_names_from_json_ld_node(value, results)


def _extract_site_token(website: str) -> str:
    domain = extract_registrable_domain(website)
    if not domain:
        return ""
    host = domain.split(".", 1)[0].lower()
    return re.sub(r"[^a-z0-9]+", "", host)


def _page_company_weight(url: str) -> int:
    lowered = str(url or "").lower()
    score = 0
    if any(token in lowered for token in ("imprint", "impressum", "privacy", "about", "contact")):
        score += 4
    if lowered.endswith("/") or urlparse(lowered).path in {"", "/"}:
        score += 2
    return score


def _score_company_candidate(value: str, site_token: str, base_score: int) -> int:
    normalized = _normalize_company_text(value)
    if not normalized:
        return -100
    lowered = normalized.lower()
    score = base_score
    if site_token and site_token in re.sub(r"[^a-z0-9]+", "", lowered):
        score += 20
    if any(phrase == lowered for phrase in _GENERIC_COMPANY_PHRASES):
        score -= 30
    if len(normalized.split()) <= 1 and not site_token:
        score -= 8
    return score


def _normalize_company_text(value: str) -> str:
    text = clean_company_name_candidate(value)
    lowered = text.lower()
    for prefix in _GENERIC_PREFIXES:
        if lowered.startswith(prefix):
            text = text[len(prefix):].strip(" :-|")
            lowered = text.lower()
    if " | " in text:
        parts = [part.strip() for part in text.split("|") if part.strip()]
        if parts:
            text = parts[0]
            lowered = text.lower()
    if " - " in text and len(text.split(" - ", 1)[0].split()) >= 2:
        text = text.split(" - ", 1)[0].strip()
        lowered = text.lower()
    if not text or lowered in _GENERIC_COMPANY_PHRASES:
        return ""
    return text


def _clean_trading_as_text(text: str) -> str:
    match = re.search(r"\(\s*trading as\s+(.+?)\s*\)$", text, flags=re.IGNORECASE)
    if match is not None:
        return str(match.group(1) or "").strip(" ,.-")
    parts = _TRADING_AS_RE.split(text, maxsplit=1)
    if len(parts) == 2:
        trailing = str(parts[1] or "").strip(" ,.-")
        if trailing:
            return trailing
    return text
