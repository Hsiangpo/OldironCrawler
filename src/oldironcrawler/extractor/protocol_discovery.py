from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

_SKIP_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".webp",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
    ".mp3", ".mp4", ".avi", ".mov", ".wmv",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".tar", ".gz", ".rar", ".7z", ".exe", ".dmg", ".apk",
}
_RELATED_SUBDOMAIN_HOST_TOKENS = {
    "about", "career", "careers", "company", "contact", "help", "jobs",
    "leadership", "people", "support", "team",
}
_RELATED_SUBDOMAIN_PATH_TOKENS = {
    "about", "board", "career", "careers", "company", "contact", "director",
    "executive", "founder", "governance", "jobs", "leadership", "management",
    "officers", "people", "president", "privacy", "support", "team", "terms",
}
_SUBDOMAIN_SCAN_PAGE_TOKENS = {
    "about", "contact", "company", "help", "people", "privacy", "support",
    "team",
}
_TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "msclkid",
    "ref_src",
    "srsltid",
}
_COMMON_VALUE_PATHS = (
    "/about",
    "/about-us",
    "/about.html",
    "/company",
    "/company-leadership",
    "/contact",
    "/contact-us",
    "/contact.html",
    "/executive-team",
    "/impressum",
    "/imprint",
    "/legal-notice",
    "/our-team",
    "/team",
    "/team-members",
    "/leadership",
    "/management",
    "/people",
    "/privacy-policy",
    "/privacy",
    "/terms",
)


def extract_same_site_links(html_text: str, page_url: str, *, limit: int) -> list[str]:
    base_host = (urlparse(page_url).netloc or "").strip().lower()
    if not base_host:
        return []
    result: list[str] = []
    seen: set[str] = set()
    for raw_href in re.findall(r'<a\s[^>]*href=["\']([^"\']+)["\']', html_text, re.IGNORECASE):
        href = raw_href.strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        absolute = urljoin(page_url, href)
        parsed = urlparse(absolute)
        link_host = (parsed.netloc or "").strip().lower()
        if not link_host or not (link_host == base_host or link_host.endswith(f".{base_host}") or base_host.endswith(f".{link_host}")):
            continue
        normalized = normalize_discovery_url(absolute)
        if normalized not in seen and is_supported_url(normalized):
            seen.add(normalized)
            result.append(normalized)
            if len(result) >= limit:
                break
    return result


def extract_same_org_seed_urls(html_text: str, page_url: str, *, site_domain: str, limit: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    page_host = (urlparse(page_url).netloc or "").strip().lower()
    for raw_href in re.findall(r'<a\s[^>]*href=["\']([^"\']+)["\']', html_text, re.IGNORECASE):
        href = raw_href.strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        absolute = urljoin(page_url, href)
        parsed = urlparse(absolute)
        host = (parsed.netloc or "").strip().lower()
        if not host or host == page_host:
            continue
        if extract_registrable_domain(host) != site_domain:
            continue
        normalized = normalize_discovery_url(absolute)
        if normalized in seen or not looks_related_subdomain_seed(normalized):
            continue
        if not is_supported_url(normalized):
            continue
        seen.add(normalized)
        result.append(normalized)
        if len(result) >= limit:
            break
    return result


def is_supported_url(url: str) -> bool:
    text = str(url or "").strip()
    lowered = text.lower()
    if "{" in text or "}" in text or "itemdataobject." in lowered:
        return False
    path = (urlparse(text).path or "").lower()
    return not any(path.endswith(suffix) for suffix in _SKIP_EXTENSIONS)


def merge_unique_urls(left: list[str], right: list[str], *, limit: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for url in [*left, *right]:
        value = normalize_discovery_url(str(url or "").strip())
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
        if len(result) >= limit:
            break
    return result


def normalize_discovery_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    if not parsed.scheme or not parsed.netloc:
        return text
    kept_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        clean_key = str(key or "").strip().lower()
        if not clean_key:
            continue
        if clean_key.startswith("utm_") or clean_key in _TRACKING_QUERY_KEYS:
            continue
        kept_pairs.append((key, value))
    return parsed._replace(query=urlencode(kept_pairs, doseq=True), fragment="").geturl()


def build_common_probe_urls(start_url: str) -> list[str]:
    parsed = urlparse(start_url)
    if not parsed.scheme or not parsed.netloc:
        return []
    locale_prefix = extract_path_locale_prefix(parsed.path)
    result: list[str] = []
    seen: set[str] = set()
    hosts = [parsed.netloc]
    if parsed.netloc and not parsed.netloc.lower().startswith("www."):
        hosts.append(f"www.{parsed.netloc}")
    for host in hosts:
        for base_prefix in ([locale_prefix] if locale_prefix else []) + [""]:
            for path in _COMMON_VALUE_PATHS:
                joined_path = f"{base_prefix}{path}" if base_prefix else path
                probe_url = parsed._replace(netloc=host, path=joined_path, query="", fragment="").geturl()
                if probe_url not in seen:
                    seen.add(probe_url)
                    result.append(probe_url)
    return result


def extract_path_locale_prefix(path: str) -> str:
    cleaned = str(path or "").strip("/")
    if not cleaned:
        return ""
    first = cleaned.split("/", 1)[0].strip().lower()
    if re.fullmatch(r"[a-z]{2}(?:-[a-z]{2})?", first):
        return f"/{first}"
    return ""


def pick_subdomain_probe_urls(start_url: str, direct_urls: list[str]) -> list[str]:
    start_domain = extract_registrable_domain(start_url)
    picked: list[str] = []
    for url in direct_urls:
        parsed = urlparse(url)
        host = (parsed.netloc or "").strip().lower()
        if not host or extract_registrable_domain(host) != start_domain:
            continue
        tokens = extract_url_hint_tokens(url)
        if not any(token in _SUBDOMAIN_SCAN_PAGE_TOKENS for token in tokens):
            continue
        if url not in picked:
            picked.append(url)
        if len(picked) >= 4:
            break
    return picked


def looks_related_subdomain_seed(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return False
    host_tokens = [token for token in re.split(r"[\W_]+", host) if len(token) >= 3]
    path_tokens = extract_url_hint_tokens(url)
    if any(token in _RELATED_SUBDOMAIN_HOST_TOKENS for token in host_tokens):
        return True
    if any(token in _RELATED_SUBDOMAIN_PATH_TOKENS for token in path_tokens):
        return True
    return False


def extract_url_hint_tokens(url: str) -> list[str]:
    parsed = urlparse(str(url or ""))
    tokens: list[str] = []
    for part in parsed.path.split("/"):
        for token in re.split(r"[\W_]+", part.strip().lower()):
            clean = token.strip().lower()
            if len(clean) < 3 or clean in tokens:
                continue
            tokens.append(clean)
    return tokens


def extract_registrable_domain(value: str) -> str:
    host = str(value or "").strip().lower()
    if not host:
        return ""
    if "://" in host or "/" in host:
        parsed = urlparse(host if "://" in host else f"https://{host}")
        host = str(parsed.netloc or parsed.path or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    labels = [label for label in host.split(".") if label]
    if len(labels) < 2:
        return host
    suffix2 = ".".join(labels[-2:])
    if suffix2 in {"ac.jp", "co.jp", "go.jp", "ne.jp", "or.jp", "ac.uk", "co.uk", "gov.uk", "org.uk", "com.au", "net.au", "org.au", "com.br", "net.br", "org.br", "co.nz", "org.nz", "com.mx", "org.mx"} and len(labels) >= 3:
        return ".".join(labels[-3:])
    return suffix2
