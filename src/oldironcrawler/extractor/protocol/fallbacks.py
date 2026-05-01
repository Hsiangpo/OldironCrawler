from __future__ import annotations

from urllib.parse import urlparse

from oldironcrawler.extractor.protocol.content import TEXT_HINTS
from oldironcrawler.extractor.protocol_discovery import is_supported_url


def should_try_http_fallback(url: str, lowered_error: str) -> bool:
    if not url.lower().startswith("https://"):
        return False
    return any(
        token in lowered_error
        for token in (
            "ssl certificate",
            "certificate has expired",
            "certificate subject name",
        )
    )


def should_try_httpx_fallback(lowered_error: str) -> bool:
    return any(
        token in lowered_error
        for token in (
            "getaddrinfo() thread failed to start",
            "thread failed to start",
            "couldn't create thread",
            "failed to create thread",
            "empty reply from server",
            "timed out",
            "timeout",
            "connection reset",
            "recv failure",
            "connection closed abruptly",
        )
    )


def should_try_httpx_status_fallback(url: str, status_code: int, response_text: str) -> bool:
    if status_code == 202:
        return True
    if status_code != 404:
        return False
    if _is_root_like_url(url):
        return True
    lowered = str(response_text or "").lower()
    return any(token in lowered for token in ("wixerrorpagesapp", "page not found", "not found"))


def replace_https_with_http(url: str) -> str:
    if url.lower().startswith("https://"):
        return f"http://{url[8:]}"
    return url


def build_www_fallback_url(url: str, lowered_error: str) -> str:
    if not any(token in lowered_error for token in ("connection closed abruptly", "empty reply from server", "connection reset", "recv failure")):
        return ""
    parsed = urlparse(str(url or ""))
    host = str(parsed.netloc or "").strip()
    if not host or host.lower().startswith("www."):
        return ""
    return parsed._replace(netloc=f"www.{host}").geturl()


def build_empty_page_batch_error(urls: list[str]) -> str:
    if not urls:
        return "empty_page_batch"
    preview = ", ".join(urls[:2])
    if len(urls) > 2:
        preview = f"{preview}, ..."
    return f"empty_page_batch: {preview}"


def is_supported_response(url: str, content_type: str) -> bool:
    if not is_supported_url(url):
        return False
    return any(hint in content_type for hint in TEXT_HINTS) or not content_type


def _is_root_like_url(url: str) -> bool:
    parsed = urlparse(str(url or ""))
    path = str(parsed.path or "").strip()
    return not path or path == "/"
