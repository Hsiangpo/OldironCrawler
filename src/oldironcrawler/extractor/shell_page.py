from __future__ import annotations

import html
import hashlib
import re
import time
import unicodedata
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from oldironcrawler.extractor.email_rules import email_matches_website, extract_registrable_domain, normalize_email_candidate

_ROOT_IDS = {"root", "app", "__next", "__nuxt", "svelte"}
_ASSET_SUFFIXES = (".js", ".mjs", ".json")
_EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_DOUBLE_QUOTED_RE = re.compile(r'"((?:\\.|[^"\\]){3,180})"')
_SINGLE_QUOTED_RE = re.compile(r"'((?:\\\\.|[^'\\\\]){3,180})'")
_ASSET_ANCHOR_RE = re.compile(
    r"mailto:|tel:|impressum|imprint|contact|gmbh|geschäft|director|founder|ceo|leadership|management|partner|owner|phone|president|telefon|telephone",
    re.IGNORECASE,
)
_PHONE_SIGNAL_RE = re.compile(r"(?:\+?\d[\d()./\-\s]{5,}\d)")
_PERSON_NAME_RE = re.compile(r"[A-Z][A-Za-z.-]+(?: [A-Z][A-Za-z.-]+){1,3}")
_ROLE_KEYWORDS = {
    "ceo",
    "chief executive",
    "contact",
    "director",
    "founder",
    "geschaftsfuhrer",
    "geschaftsfuhrender",
    "impressum",
    "imprint",
    "legal",
    "managing director",
    "owner",
    "partner",
    "president",
}
_COMPANY_KEYWORDS = {
    " inc",
    " llc",
    " ltd",
    " ag",
    " bv",
    " corp",
    " gmbh",
    " limited",
}
_HEAD_META_FIELDS = ("author", "description", "og:title", "og:description", "twitter:title", "twitter:description")
_SHELL_EMAIL_KEEP_HINTS = {
    "contact",
    "director",
    "email",
    "founder",
    "geschaftsfuhrer",
    "geschaftsfuhrender",
    "impressum",
    "kontakt",
    "legal",
    "mail",
    "mailto",
    "managing director",
    "owner",
    "partner",
    "phone",
    "president",
    "representative",
    "telefon",
}
_SHELL_EMAIL_DROP_HINTS = {
    "aufsicht",
    "authority",
    "beispiel",
    "compliance",
    "datenschutz",
    "example",
    "placeholder",
    "privacy",
    "regulator",
    "regierung",
    "terms",
}


def looks_like_shell_page(page_html: str) -> bool:
    soup = BeautifulSoup(str(page_html or ""), "lxml")
    body = soup.body or soup
    text = re.sub(r"\s+", " ", body.get_text(" ", strip=True)).strip()
    if not soup.find_all("script", src=True):
        return False
    if _has_root_container(soup):
        return True
    lowered = str(page_html or "").lower()
    if "data-reactroot" in lowered or "id=\"root\"" in lowered or "id='root'" in lowered:
        return True
    if len(text) > 160:
        return False
    return False


def extract_first_party_asset_urls(page_url: str, page_html: str, *, limit: int = 6) -> list[str]:
    soup = BeautifulSoup(str(page_html or ""), "lxml")
    page_domain = extract_registrable_domain(page_url)
    urls: list[str] = []
    seen: set[str] = set()
    for script in soup.find_all("script", src=True):
        src = str(script.get("src") or "").strip()
        if not src:
            continue
        resolved = urljoin(page_url, src)
        if resolved in seen:
            continue
        if not _is_first_party_asset(page_domain, resolved):
            continue
        seen.add(resolved)
        urls.append(resolved)
        if len(urls) >= limit:
            break
    return urls


def build_shell_fingerprint(page_url: str, page_html: str) -> str:
    if not looks_like_shell_page(page_html):
        return ""
    asset_urls = extract_first_party_asset_urls(page_url, page_html, limit=12)
    fingerprint_parts = [_normalize_fingerprint_asset_url(asset_url) for asset_url in asset_urls]
    fingerprint_parts.extend(_collect_head_lines(page_html)[:3])
    if not any(fingerprint_parts):
        return ""
    payload = "\n".join(part for part in fingerprint_parts if part)
    return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()


def fetch_first_party_asset_texts(
    asset_urls: list[str],
    *,
    proxy_url: str,
    timeout_seconds: float,
    deadline_monotonic: float | None = None,
) -> dict[str, str]:
    if not asset_urls:
        return {}
    client_kwargs: dict[str, object] = {
        "follow_redirects": True,
        "timeout": max(float(timeout_seconds or 0), 1.0),
        "trust_env": False,
        "headers": {"User-Agent": "Mozilla/5.0", "Accept": "*/*"},
    }
    if proxy_url:
        client_kwargs["proxy"] = proxy_url
    result: dict[str, str] = {}
    with httpx.Client(**client_kwargs) as client:
        for asset_url in asset_urls:
            request_timeout = _resolve_deadline_timeout(
                timeout_seconds=timeout_seconds,
                deadline_monotonic=deadline_monotonic,
            )
            if request_timeout is None:
                break
            try:
                response = client.get(asset_url, timeout=request_timeout)
                if response.status_code != 200:
                    continue
                if not _looks_like_text_asset(asset_url, str(response.headers.get("Content-Type", "") or "")):
                    continue
                text = str(response.text or "").strip()
                if not text:
                    continue
                result[asset_url] = text[:2_500_000]
            except Exception:  # noqa: BLE001
                continue
    return result


def build_shell_evidence_html(page_url: str, page_html: str, asset_texts: dict[str, str]) -> str:
    evidence_lines = _collect_head_lines(page_html)
    for asset_url, asset_text in asset_texts.items():
        signals = _extract_asset_signal_lines(asset_text, page_url=page_url)
        if not signals:
            continue
        evidence_lines.append(f"Source Asset: {asset_url}")
        evidence_lines.extend(signals)
    deduped = _dedupe_lines(evidence_lines)
    if not deduped:
        return str(page_html or "")
    parts = [
        "<html><body><section data-oldiron-shell='1'>",
        f"<h1>Recovered shell evidence for {html.escape(page_url)}</h1>",
    ]
    for line in deduped[:180]:
        parts.append(f"<p>{html.escape(line)}</p>")
    parts.append("</section></body></html>")
    return "".join(parts)


def enrich_shell_page_html(
    page_url: str,
    page_html: str,
    *,
    proxy_url: str,
    timeout_seconds: float,
    deadline_monotonic: float | None = None,
) -> str:
    if not looks_like_shell_page(page_html):
        return str(page_html or "")
    asset_urls = extract_first_party_asset_urls(page_url, page_html)
    asset_texts = fetch_first_party_asset_texts(
        asset_urls,
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
        deadline_monotonic=deadline_monotonic,
    )
    return build_shell_evidence_html(page_url, page_html, asset_texts)


def _resolve_deadline_timeout(*, timeout_seconds: float, deadline_monotonic: float | None) -> float | None:
    base_timeout = max(float(timeout_seconds or 0), 1.0)
    if deadline_monotonic is None:
        return base_timeout
    remaining = deadline_monotonic - time.monotonic()
    if remaining <= 0:
        return None
    return max(min(base_timeout, remaining), 0.01)


def _has_root_container(soup: BeautifulSoup) -> bool:
    body = soup.body or soup
    for element in body.find_all(True, recursive=False):
        if str(element.get("id") or "").strip().lower() in _ROOT_IDS:
            return True
    return False


def _is_first_party_asset(page_domain: str, asset_url: str) -> bool:
    if not page_domain:
        return False
    parsed = urlparse(asset_url)
    host = str(parsed.netloc or "").strip().lower()
    if not host:
        return False
    asset_domain = extract_registrable_domain(host)
    if asset_domain != page_domain:
        return False
    lowered_path = str(parsed.path or "").strip().lower()
    return lowered_path.endswith(_ASSET_SUFFIXES) or "/assets/" in lowered_path


def _looks_like_text_asset(asset_url: str, content_type: str) -> bool:
    lowered = str(content_type or "").lower()
    if any(token in lowered for token in ("javascript", "json", "text/plain", "ecmascript")):
        return True
    return str(urlparse(asset_url).path or "").lower().endswith(_ASSET_SUFFIXES)


def _collect_head_lines(page_html: str) -> list[str]:
    soup = BeautifulSoup(str(page_html or ""), "lxml")
    lines: list[str] = []
    if soup.title and soup.title.string:
        title = re.sub(r"\s+", " ", str(soup.title.string or "")).strip()
        if title:
            lines.append(title)
    for meta in soup.find_all("meta"):
        key = str(meta.get("property") or meta.get("name") or "").strip().lower()
        if key not in _HEAD_META_FIELDS:
            continue
        content = re.sub(r"\s+", " ", str(meta.get("content") or "")).strip()
        if content:
            lines.append(content)
    return _dedupe_lines(lines)


def _extract_asset_signal_lines(asset_text: str, *, page_url: str) -> list[str]:
    scoped_text = _slice_asset_signal_windows(str(asset_text or ""))
    general_candidates: list[tuple[int, str]] = []
    person_candidates: list[tuple[int, str]] = []
    role_candidates: list[tuple[int, str]] = []
    company_candidates: list[tuple[int, str]] = []
    email_candidates: list[tuple[int, str]] = []
    for match in _EMAIL_RE.finditer(scoped_text):
        email = normalize_email_candidate(match.group(0))
        if not email:
            continue
        context = _extract_email_context_fragment(scoped_text, match.start(), match.end())
        if not _should_keep_shell_email(page_url, email, context):
            continue
        score = 90 if email_matches_website(page_url, email) else 48
        email_candidates.append((score, email.lower()))
    for name in _PERSON_NAME_RE.findall(scoped_text):
        if _looks_like_person_name(name):
            person_candidates.append((78, name))
    for fragment in _extract_context_fragments(scoped_text):
        cleaned_fragment = _sanitize_signal_line(page_url, fragment)
        score = _signal_score(cleaned_fragment)
        if score > 0:
            _bucket_signal_line(
                cleaned_fragment,
                score,
                general_candidates,
                person_candidates,
                role_candidates,
                company_candidates,
            )
    for raw_value in _DOUBLE_QUOTED_RE.findall(scoped_text):
        decoded = _sanitize_signal_line(page_url, _decode_js_string(raw_value))
        score = _signal_score(decoded)
        if score > 0:
            _bucket_signal_line(
                decoded,
                score,
                general_candidates,
                person_candidates,
                role_candidates,
                company_candidates,
            )
    for raw_value in _SINGLE_QUOTED_RE.findall(scoped_text):
        decoded = _sanitize_signal_line(page_url, _decode_js_string(raw_value))
        score = _signal_score(decoded)
        if score > 0:
            _bucket_signal_line(
                decoded,
                score,
                general_candidates,
                person_candidates,
                role_candidates,
                company_candidates,
            )
    ordered = (
        sorted(email_candidates, key=lambda item: (-item[0], len(item[1]), item[1]))[:20]
        + sorted(person_candidates, key=lambda item: (-item[0], len(item[1]), item[1]))[:20]
        + sorted(role_candidates, key=lambda item: (-item[0], len(item[1]), item[1]))[:24]
        + sorted(company_candidates, key=lambda item: (-item[0], len(item[1]), item[1]))[:20]
        + sorted(general_candidates, key=lambda item: (-item[0], len(item[1]), item[1]))[:120]
    )
    result: list[str] = []
    seen: set[str] = set()
    for _score, line in ordered:
        key = line.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(line)
        if len(result) >= 120:
            break
    return result


def _bucket_signal_line(
    line: str,
    score: int,
    general_candidates: list[tuple[int, str]],
    person_candidates: list[tuple[int, str]],
    role_candidates: list[tuple[int, str]],
    company_candidates: list[tuple[int, str]],
) -> None:
    general_candidates.append((score, line))
    lowered = _normalize_signal_text(line)
    if _looks_like_person_name(line):
        person_candidates.append((score + 20, line))
    if any(keyword in lowered for keyword in _ROLE_KEYWORDS):
        role_candidates.append((score + 12, line))
    if any(keyword in f" {lowered} " for keyword in _COMPANY_KEYWORDS):
        company_candidates.append((score + 10, line))


def _decode_js_string(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("\\/", "/")
    text = text.encode("utf-8", errors="ignore").decode("unicode_escape", errors="ignore")
    text = re.sub(r"\s+", " ", text).strip().strip(".,;")
    return text


def _extract_context_fragments(scoped_text: str) -> list[str]:
    fragments: list[str] = []
    for match in _ASSET_ANCHOR_RE.finditer(scoped_text):
        fragment = scoped_text[max(match.start() - 120, 0) : min(match.end() + 160, len(scoped_text))]
        fragment = re.sub(r"[{}\[\](),:+*/]", " ", fragment)
        fragment = re.sub(r"\s+", " ", fragment).strip().strip(".,;")
        if len(fragment) < 12 or len(fragment) > 180:
            continue
        fragments.append(fragment)
        if len(fragments) >= 40:
            break
    return fragments


def _slice_asset_signal_windows(asset_text: str) -> str:
    text = str(asset_text or "")
    windows: list[tuple[int, int]] = []
    for match in _EMAIL_RE.finditer(text):
        windows.append((max(match.start() - 600, 0), min(match.end() + 600, len(text))))
    for match in _ASSET_ANCHOR_RE.finditer(text):
        windows.append((max(match.start() - 600, 0), min(match.end() + 600, len(text))))
        if len(windows) >= 80:
            break
    if not windows:
        return text[:200_000]
    merged: list[tuple[int, int]] = []
    start, end = sorted(windows)[0]
    for next_start, next_end in sorted(windows)[1:]:
        if next_start <= end + 200:
            end = max(end, next_end)
            continue
        merged.append((start, end))
        start, end = next_start, next_end
    merged.append((start, end))
    return "\n".join(text[left:right] for left, right in merged[:24])


def _signal_score(value: str) -> int:
    text = str(value or "").strip()
    if len(text) < 3 or len(text) > 180:
        return 0
    if text.startswith(("http://", "https://")):
        return 0
    if text.count("{") or text.count("function(") or text.count("=>"):
        return 0
    lowered = _normalize_signal_text(text)
    score = 0
    if _EMAIL_RE.search(text):
        score += 90
    if _PHONE_SIGNAL_RE.search(text):
        score += 36
    if any(keyword in lowered for keyword in _ROLE_KEYWORDS):
        score += 48
    if any(keyword in f" {lowered} " for keyword in _COMPANY_KEYWORDS):
        score += 38
    if _looks_like_person_name(text):
        score += 58
    if "mailto:" in lowered:
        score += 12
    if re.search(r"[A-Za-z].*[A-Za-z]", text) and " " in text:
        score += 4
    return score


def _looks_like_person_name(value: str) -> bool:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text or len(text) > 80:
        return False
    return bool(re.fullmatch(r"[A-Z][A-Za-z.-]+(?: [A-Z][A-Za-z.-]+){1,3}", text))


def _normalize_signal_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", text).strip().lower()


def _dedupe_lines(lines: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw_line in lines:
        line = re.sub(r"\s+", " ", str(raw_line or "")).strip()
        if not line:
            continue
        key = line.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(line)
    return result


def _normalize_fingerprint_asset_url(asset_url: str) -> str:
    parsed = urlparse(asset_url)
    host = str(parsed.netloc or "").strip().lower()
    path = str(parsed.path or "").strip()
    if not host and not path:
        return ""
    return f"{host}{path}"


def _extract_email_context_fragment(text: str, start: int, end: int) -> str:
    left_boundaries = [
        text.rfind("\n", 0, start),
        text.rfind('"', 0, start),
        text.rfind("'", 0, start),
    ]
    right_boundaries = [
        index
        for index in (
            text.find("\n", end),
            text.find('"', end),
            text.find("'", end),
        )
        if index != -1
    ]
    left = max(max(left_boundaries), start - 120)
    right = min(right_boundaries) if right_boundaries else min(end + 120, len(text))
    return text[max(left, 0) : min(right, len(text))]


def _sanitize_signal_line(page_url: str, line: str) -> str:
    cleaned = str(line or "")
    for match in list(_EMAIL_RE.finditer(cleaned)):
        email = normalize_email_candidate(match.group(0))
        if not email:
            continue
        if _should_keep_shell_email(page_url, email, cleaned):
            continue
        cleaned = cleaned.replace(match.group(0), " ")
    return re.sub(r"\s+", " ", cleaned).strip(" ,;")


def _should_keep_shell_email(page_url: str, email: str, context: str) -> bool:
    if email_matches_website(page_url, email):
        return True
    lowered = _normalize_signal_text(context)
    registrable_domain = extract_registrable_domain(email.split("@", 1)[1])
    if not registrable_domain:
        return False
    if any(token in registrable_domain for token in _SHELL_EMAIL_DROP_HINTS):
        return False
    if any(token in lowered for token in _SHELL_EMAIL_DROP_HINTS):
        return False
    if not any(token in lowered for token in _SHELL_EMAIL_KEEP_HINTS):
        return False
    if "mailto" in lowered:
        return True
    if _looks_like_person_name(context):
        return True
    if any(keyword in lowered for keyword in _ROLE_KEYWORDS):
        return True
    if any(keyword in f" {lowered} " for keyword in _COMPANY_KEYWORDS):
        return True
    return False
