from __future__ import annotations

import gzip
import html
import os
import re
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree

import httpx
from curl_cffi import requests as cffi_requests

from oldironcrawler.challenge_solver import resolve_cloudflare_challenge
from oldironcrawler.extractor.page_pool import PageFetchPool
from oldironcrawler.extractor.protocol_discovery import (
    build_common_probe_urls as _build_common_probe_urls,
    extract_registrable_domain as _extract_registrable_domain,
    extract_same_org_seed_urls as _extract_same_org_seed_urls,
    extract_same_site_links as _extract_same_site_links,
    is_supported_url as _is_supported_url,
    merge_unique_urls as _merge_unique_urls,
    pick_subdomain_probe_urls as _pick_subdomain_probe_urls,
)

_HREF_RE = re.compile(r'<a\s[^>]*href=["\']([^"\']+)["\']', re.IGNORECASE)
_ROBOTS_SITEMAP_RE = re.compile(r"^Sitemap:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
_CHARSET_RE = re.compile(r"charset\s*=\s*[\"']?\s*([a-zA-Z0-9._-]+)", re.IGNORECASE)
_HTML_META_CHARSET_RE = re.compile(br"<meta[^>]+charset=[\"']?\s*([a-zA-Z0-9._-]+)", re.IGNORECASE)
_XML_ENCODING_RE = re.compile(br"<\?xml[^>]+encoding=[\"']\s*([a-zA-Z0-9._-]+)", re.IGNORECASE)
_EMAIL_SIGNAL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_HTML_SIGNAL_PATTERNS = (
    re.compile(r"<h[1-4][^>]*>.*?</h[1-4]>", re.IGNORECASE | re.DOTALL),
    re.compile(
        r"(founder|co-founder|owner|chairman|chief executive|managing director|group chief executive|president|principal solicitor|director|lead guide|leadership|executive team)",
        re.IGNORECASE,
    ),
    _EMAIL_SIGNAL_RE,
)
_TEMP_ERROR_HINTS = (
    "timeout",
    "timed out",
    "429",
    "503",
    "504",
    "500",
    "502",
    "ssl",
    "tls",
    "eof",
    "getaddrinfo() thread failed to start",
    "thread failed to start",
    "couldn't create thread",
    "failed to create thread",
    "resource temporarily unavailable",
    "[errno 35]",
)
_PERMANENT_ERROR_HINTS = (
    "could not resolve host",
    "name or service not known",
    "nodename nor servname",
    "certificate has expired",
    "ssl certificate problem",
    "no alternative certificate subject name matches",
    "certificate subject name",
    "failed to connect",
    "couldn't connect to server",
    "connection refused",
    "no route to host",
    "network is unreachable",
    "host is down",
    "could not connect to server",
)
_TEXT_HINTS = ("text/html", "application/xhtml+xml", "application/xml", "text/xml", "text/plain")
_CLOUDFLARE_CHALLENGE_HINTS = (
    "just a moment...",
    "cf-browser-verification",
    "challenge-platform",
    "cf-challenge",
    "attention required! | cloudflare",
)
_INCAPSULA_CHALLENGE_HINTS = (
    "_incapsula_resource",
    "incapsula incident id",
    "imperva",
)
_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
_REQUEST_SLOT_LIMIT = max(int(os.getenv("PROTOCOL_REQUEST_SLOTS", "52") or "52"), 1)
_REQUEST_SLOT_SEMAPHORE = threading.BoundedSemaphore(_REQUEST_SLOT_LIMIT)
_SITE_DEADLINE_SAFETY_SECONDS = 8.0
class ProtocolTemporaryError(RuntimeError):
    pass
class ProtocolPermanentError(RuntimeError):
    pass
@dataclass
class HtmlPage:
    url: str
    html: str


@dataclass
class DiscoveryStageResult:
    urls: list[str]
    homepage_html: str


@dataclass
class SiteProtocolConfig:
    timeout_seconds: float = 10.0
    max_retries: int = 2
    proxy_url: str = ""
    capsolver_api_key: str = ""
    capsolver_api_base_url: str = "https://api.capsolver.com"
    capsolver_proxy: str = ""
    capsolver_poll_seconds: float = 3.0
    capsolver_max_wait_seconds: float = 40.0
    cloudflare_proxy_url: str = ""
    impersonate: str = "chrome110"
    max_html_chars: int = 250_000
    page_batch_timeout_seconds: float = 45.0
    deadline_monotonic: float | None = None
    common_probe_target: int = 8
    common_probe_concurrency: int = 52
    common_probe_patience_batches: int = 2
    common_probe_min_hits_after_patience: int = 2
    related_seed_limit: int = 2
    default_headers: dict[str, str] = field(default_factory=lambda: {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
class SiteProtocolClient:
    def __init__(self, config: SiteProtocolConfig) -> None:
        self._config = config
        self._http_client = self._build_httpx_client()
        self._probe_executor = ThreadPoolExecutor(max_workers=max(config.common_probe_concurrency, 1))
        self._session_lock = threading.Lock()
        self._thread_sessions: dict[int, cffi_requests.Session] = {}

    def close(self) -> None:
        with self._session_lock:
            sessions = list(self._thread_sessions.values())
            self._thread_sessions.clear()
        for session in sessions:
            try:
                session.close()
            except Exception:  # noqa: BLE001
                pass
        try:
            self._probe_executor.shutdown(wait=False, cancel_futures=True)
        except Exception:  # noqa: BLE001
            pass
        try:
            self._http_client.close()
        except Exception:  # noqa: BLE001
            return None
    def discover_urls(self, start_url: str, *, limit: int = 200) -> list[str]:
        session = self._get_or_create_session()
        urls, homepage_html = self._discover_direct_urls(session, start_url, limit=limit)
        extra_urls = self._discover_related_subdomain_urls(
            session,
            start_url=start_url,
            homepage_html=homepage_html,
            direct_urls=urls,
            limit=limit,
        )
        return _merge_unique_urls(extra_urls, urls, limit=limit)

    def discover_primary_urls(self, start_url: str, *, limit: int = 80) -> DiscoveryStageResult:
        session = self._get_or_create_session()
        urls, homepage_html = self._discover_primary_urls(session, start_url, limit=limit)
        return DiscoveryStageResult(urls=urls, homepage_html=homepage_html)

    def discover_sitemap_urls(self, start_url: str, *, limit: int = 80) -> list[str]:
        session = self._get_or_create_session()
        return self._discover_sitemap_urls(session, start_url, limit=limit)

    def discover_related_subdomain_urls(
        self,
        start_url: str,
        *,
        homepage_html: str,
        direct_urls: list[str],
        limit: int = 40,
    ) -> list[str]:
        session = self._get_or_create_session()
        return self._discover_related_subdomain_urls(
            session,
            start_url=start_url,
            homepage_html=homepage_html,
            direct_urls=direct_urls,
            limit=limit,
        )
    def fetch_page(self, url: str) -> HtmlPage:
        session = self._get_or_create_session()
        html_text = self._fetch_html(session, url, required=True)
        return HtmlPage(url=url, html=html_text)
    def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool: PageFetchPool | None = None) -> list[HtmlPage]:
        pages: list[HtmlPage] = []
        last_error: Exception | None = None
        timed_out = False
        filtered = [url for url in urls if _is_supported_url(url)]
        deadline = time.monotonic() + max(self._config.page_batch_timeout_seconds, 0.01)
        if self._config.deadline_monotonic is not None:
            deadline = min(deadline, self._config.deadline_monotonic)
        if page_pool is not None and filtered:
            return page_pool.fetch_pages(
                urls=filtered,
                fetch_one=lambda url: self._fetch_page_optional(url, timeout_seconds=min(self._config.timeout_seconds, max(deadline - time.monotonic(), 0.01))),
                deadline_monotonic=deadline,
            )
        for url in filtered:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                break
            try:
                page = self._fetch_page_optional(url, timeout_seconds=min(self._config.timeout_seconds, remaining))
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                continue
            if page is not None and page.html.strip():
                pages.append(page)
        if not pages and last_error is not None:
            raise last_error
        if not pages and timed_out:
            raise TimeoutError("page_batch_timeout")
        return pages
    def _fetch_page_optional(self, url: str, *, timeout_seconds: float | None = None) -> HtmlPage | None:
        session = self._get_or_create_session()
        html_text = self._fetch_html(session, url, required=False, timeout_seconds=timeout_seconds)
        if not html_text.strip():
            return None
        return HtmlPage(url=url, html=html_text)
    def _get_or_create_session(self) -> cffi_requests.Session:
        thread_id = threading.get_ident()
        with self._session_lock:
            session = self._thread_sessions.get(thread_id)
            if session is None:
                session = self._build_session()
                self._thread_sessions[thread_id] = session
            return session
    def _build_session(self) -> cffi_requests.Session:
        proxies = {}
        if self._config.proxy_url:
            proxies = {"http": self._config.proxy_url, "https": self._config.proxy_url}
        session = cffi_requests.Session(impersonate=self._config.impersonate, proxies=proxies)
        session.trust_env = False
        session.headers.update(self._config.default_headers)
        return session
    def _build_httpx_client(self) -> httpx.Client:
        client_kwargs: dict[str, object] = {
            "follow_redirects": True,
            "headers": dict(self._config.default_headers),
            "timeout": self._config.timeout_seconds,
            "limits": httpx.Limits(max_connections=128, max_keepalive_connections=32, keepalive_expiry=30.0),
            "trust_env": False,
        }
        if self._config.proxy_url:
            client_kwargs["proxy"] = self._config.proxy_url
        return httpx.Client(**client_kwargs)
    def _fetch_html(
        self,
        session: cffi_requests.Session,
        url: str,
        *,
        required: bool,
        timeout_seconds: float | None = None,
        max_retries_override: int | None = None,
    ) -> str:
        retries = self._config.max_retries if max_retries_override is None else max(max_retries_override, 0)
        attempts = retries + 1
        last_error: Exception | None = None
        for _ in range(attempts):
            response = None
            try:
                request_timeout = self._resolve_timeout(timeout_seconds)
                with _request_slot(timeout_seconds=request_timeout):
                    response = session.get(url, timeout=request_timeout)
                status = int(response.status_code)
                if status == 200:
                    content_type = str(response.headers.get("Content-Type", "") or "").lower()
                    if not _is_supported_response(url, content_type):
                        return ""
                    html_text = _truncate_html(_decode_response_text(response), self._config.max_html_chars)
                    html_text = self._maybe_challenge_fallback(session, url, html_text, request_timeout)
                    _raise_if_challenge_page(url, html_text)
                    return html_text
                if status in {429, 500, 502, 503, 504}:
                    raise ProtocolTemporaryError(f"temporary_http_{status}: {url}")
                if status == 403:
                    challenge_text = _truncate_html(_decode_response_text(response), self._config.max_html_chars)
                    challenge_text = self._maybe_challenge_fallback(session, url, challenge_text, request_timeout)
                    _raise_if_challenge_page(url, challenge_text)
                    return challenge_text
                if status == 404:
                    return ""
                if required:
                    raise ProtocolPermanentError(f"http_{status}: {url}")
                return ""
            except ProtocolTemporaryError:
                last_error = ProtocolTemporaryError(f"temporary_request: {url}")
            except ProtocolPermanentError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                lowered = str(exc).lower()
                httpx_html = self._try_httpx_fallback(url, lowered)
                if httpx_html is not None:
                    return httpx_html
                insecure_html = self._try_insecure_https_fallback(url, lowered)
                if insecure_html is not None:
                    return insecure_html
                fallback_html = self._try_http_fallback(session, url, lowered)
                if fallback_html is not None:
                    return fallback_html
                www_html = self._try_www_fallback(session, url, lowered)
                if www_html is not None:
                    return www_html
                if any(hint in lowered for hint in _PERMANENT_ERROR_HINTS):
                    raise ProtocolPermanentError(str(exc)) from exc
                if any(hint in lowered for hint in _TEMP_ERROR_HINTS):
                    continue
                if required:
                    raise ProtocolPermanentError(str(exc)) from exc
                return ""
            finally:
                if response is not None:
                    try:
                        response.close()
                    except Exception:  # noqa: BLE001
                        pass
        if last_error is not None:
            raise ProtocolTemporaryError(str(last_error or f"temporary_request: {url}"))
        return ""

    def _resolve_timeout(self, timeout_seconds: float | None = None) -> float:
        base_timeout = timeout_seconds if timeout_seconds is not None else self._config.timeout_seconds
        remaining = self._remaining_deadline_seconds()
        if remaining is None:
            return max(base_timeout, 0.05)
        if remaining <= 0:
            raise ProtocolTemporaryError("site_deadline_exceeded")
        return max(min(base_timeout, remaining), 0.05)

    def _remaining_deadline_seconds(self) -> float | None:
        deadline = self._config.deadline_monotonic
        if deadline is None:
            return None
        return deadline - time.monotonic() - _SITE_DEADLINE_SAFETY_SECONDS

    def _maybe_challenge_fallback(
        self,
        session: cffi_requests.Session,
        url: str,
        html_text: str,
        timeout_seconds: float,
    ) -> str:
        capped_wait = self._cap_challenge_wait_seconds()
        return resolve_cloudflare_challenge(
            url=url,
            timeout_seconds=timeout_seconds,
            html_text=html_text,
            proxy_url=self._config.proxy_url,
            max_html_chars=self._config.max_html_chars,
            session_headers=getattr(session, "headers", None) or {},
            cookie_jar=getattr(session, "cookies", None),
            detect_challenge=lambda value: bool(_detect_challenge_kind(value)),
            refetch_html=lambda: self._refetch_challenge_html(session, url, timeout_seconds),
            capsolver_api_key=self._config.capsolver_api_key,
            capsolver_api_base_url=self._config.capsolver_api_base_url,
            capsolver_proxy=self._config.capsolver_proxy,
            capsolver_poll_seconds=self._config.capsolver_poll_seconds,
            capsolver_max_wait_seconds=capped_wait,
            cloudflare_proxy_url=self._config.cloudflare_proxy_url,
            impersonate=self._config.impersonate,
        )

    def _cap_challenge_wait_seconds(self) -> float:
        remaining = self._remaining_deadline_seconds()
        if remaining is None:
            return self._config.capsolver_max_wait_seconds
        return max(min(self._config.capsolver_max_wait_seconds, remaining - 1.0), 0.0)

    def _refetch_challenge_html(self, session: cffi_requests.Session, url: str, timeout_seconds: float) -> str:
        response = None
        try:
            request_timeout = self._resolve_timeout(timeout_seconds)
            with _request_slot(timeout_seconds=request_timeout):
                response = session.get(url, timeout=request_timeout)
            if int(response.status_code) != 200:
                return ""
            content_type = str(response.headers.get("Content-Type", "") or "").lower()
            if not _is_supported_response(url, content_type):
                return ""
            return _truncate_html(_decode_response_text(response), self._config.max_html_chars)
        except Exception:  # noqa: BLE001
            return ""
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:  # noqa: BLE001
                    pass

    def _try_httpx_fallback(self, url: str, lowered_error: str) -> str | None:
        if not _should_try_httpx_fallback(lowered_error):
            return None
        try:
            request_timeout = self._resolve_timeout()
            with _request_slot(timeout_seconds=request_timeout):
                response = self._http_client.get(url, timeout=request_timeout)
            if int(response.status_code) != 200:
                return None
            content_type = str(response.headers.get("Content-Type", "") or "").lower()
            if not _is_supported_response(url, content_type):
                return None
            html_text = _truncate_html(response.text, self._config.max_html_chars)
            _raise_if_challenge_page(url, html_text)
            return html_text
        except Exception:  # noqa: BLE001
            return None

    def _try_insecure_https_fallback(self, url: str, lowered_error: str) -> str | None:
        if not _should_try_http_fallback(url, lowered_error):
            return None
        request_timeout = self._resolve_timeout()
        client_kwargs: dict[str, object] = {
            "follow_redirects": True,
            "headers": dict(self._config.default_headers),
            "timeout": request_timeout,
            "verify": False,
            "trust_env": False,
        }
        if self._config.proxy_url:
            client_kwargs["proxy"] = self._config.proxy_url
        try:
            with httpx.Client(**client_kwargs) as client:
                with _request_slot(timeout_seconds=request_timeout):
                    response = client.get(url, timeout=request_timeout)
                if int(response.status_code) != 200:
                    return None
                content_type = str(response.headers.get("Content-Type", "") or "").lower()
                if not _is_supported_response(url, content_type):
                    return None
                html_text = _truncate_html(response.text, self._config.max_html_chars)
                _raise_if_challenge_page(url, html_text)
                return html_text
        except Exception:  # noqa: BLE001
            return None

    def _try_http_fallback(self, session: cffi_requests.Session, url: str, lowered_error: str) -> str | None:
        if not _should_try_http_fallback(url, lowered_error):
            return None
        fallback_url = _replace_https_with_http(url)
        response = None
        try:
            request_timeout = self._resolve_timeout()
            with _request_slot(timeout_seconds=request_timeout):
                response = session.get(fallback_url, timeout=request_timeout)
            if int(response.status_code) != 200:
                return None
            content_type = str(response.headers.get("Content-Type", "") or "").lower()
            if not _is_supported_response(fallback_url, content_type):
                return None
            html_text = _truncate_html(_decode_response_text(response), self._config.max_html_chars)
            _raise_if_challenge_page(fallback_url, html_text)
            return html_text
        except Exception:  # noqa: BLE001
            return None
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:  # noqa: BLE001
                    pass

    def _try_www_fallback(self, session: cffi_requests.Session, url: str, lowered_error: str) -> str | None:
        fallback_url = _build_www_fallback_url(url, lowered_error)
        if not fallback_url:
            return None
        response = None
        try:
            request_timeout = self._resolve_timeout()
            with _request_slot(timeout_seconds=request_timeout):
                response = session.get(fallback_url, timeout=request_timeout)
            if int(response.status_code) != 200:
                return None
            content_type = str(response.headers.get("Content-Type", "") or "").lower()
            if not _is_supported_response(fallback_url, content_type):
                return None
            html_text = _truncate_html(_decode_response_text(response), self._config.max_html_chars)
            _raise_if_challenge_page(fallback_url, html_text)
            return html_text
        except Exception:  # noqa: BLE001
            return None
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:  # noqa: BLE001
                    pass

    def _discover_sitemap_urls(self, session: cffi_requests.Session, base_url: str, *, limit: int) -> list[str]:
        locations = self._find_sitemap_locations(session, base_url)
        if not locations:
            locations = [urljoin(base_url, "/sitemap.xml")]
        urls: list[str] = []
        visited: set[str] = set()
        base_host = (urlparse(base_url).netloc or "").strip().lower()
        for location in locations:
            if len(urls) >= limit:
                break
            self._parse_sitemap_recursive(session, location, urls, visited, base_host=base_host, limit=limit, depth=0)
        return urls[:limit]

    def _find_sitemap_locations(self, session: cffi_requests.Session, base_url: str) -> list[str]:
        robots_url = urljoin(base_url, "/robots.txt")
        try:
            request_timeout = self._resolve_timeout()
            with _request_slot(timeout_seconds=request_timeout):
                response = session.get(robots_url, timeout=request_timeout)
            if int(response.status_code) != 200:
                return []
            text = _decode_response_text(response)
            return [item.strip() for item in _ROBOTS_SITEMAP_RE.findall(text) if item.strip()]
        except Exception:  # noqa: BLE001
            return []

    def _parse_sitemap_recursive(
        self,
        session: cffi_requests.Session,
        sitemap_url: str,
        result: list[str],
        visited: set[str],
        *,
        base_host: str,
        limit: int,
        depth: int,
    ) -> None:
        if depth > 3 or sitemap_url in visited or len(result) >= limit:
            return
        visited.add(sitemap_url)
        xml_text = self._fetch_sitemap_text(session, sitemap_url)
        if not xml_text:
            return
        try:
            root = ElementTree.fromstring(xml_text)
        except ElementTree.ParseError:
            return
        tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag
        if tag == "sitemapindex":
            for child_loc in root.findall(".//sm:sitemap/sm:loc", _NS):
                child_url = str(child_loc.text or "").strip()
                if child_url:
                    self._parse_sitemap_recursive(
                        session,
                        child_url,
                        result,
                        visited,
                        base_host=base_host,
                        limit=limit,
                        depth=depth + 1,
                    )
            return
        for loc in root.findall(".//sm:url/sm:loc", _NS):
            page_url = str(loc.text or "").strip()
            if not page_url or page_url in visited or not _is_supported_url(page_url):
                continue
            host = (urlparse(page_url).netloc or "").strip().lower()
            if host == base_host or host.endswith(f".{base_host}") or base_host.endswith(f".{host}"):
                visited.add(page_url)
                result.append(page_url)
                if len(result) >= limit:
                    return

    def _discover_direct_urls(
        self,
        session: cffi_requests.Session,
        start_url: str,
        *,
        limit: int,
    ) -> tuple[list[str], str]:
        merged, homepage_html = self._discover_primary_urls(session, start_url, limit=limit)
        if self._has_enough_discovery_hits(merged):
            return merged, homepage_html
        sitemap_urls = self._discover_sitemap_urls(session, start_url, limit=limit)
        if sitemap_urls:
            merged = _merge_unique_urls(merged, sitemap_urls, limit=limit)
        return merged, homepage_html

    def _discover_primary_urls(
        self,
        session: cffi_requests.Session,
        start_url: str,
        *,
        limit: int,
    ) -> tuple[list[str], str]:
        homepage_html = ""
        homepage_error: Exception | None = None
        try:
            homepage_html = self._fetch_html(session, start_url, required=False)
        except ProtocolPermanentError as exc:
            homepage_error = exc
        guessed_urls = self._probe_common_value_urls(session, start_url, limit=limit)
        homepage_links = _extract_same_site_links(homepage_html, start_url, limit=limit) if homepage_html else []
        merged = _merge_unique_urls(homepage_links, guessed_urls, limit=limit)
        if homepage_html:
            return merged, homepage_html
        if guessed_urls:
            return guessed_urls, ""
        if homepage_error is not None:
            raise homepage_error
        return [], ""

    def _probe_common_value_urls(
        self,
        session: cffi_requests.Session,
        start_url: str,
        *,
        limit: int,
    ) -> list[str]:
        probe_urls = _build_common_probe_urls(start_url)
        if not probe_urls:
            return []
        result: list[str] = []
        probe_target = min(max(self._config.common_probe_target, 1), max(limit, 1), len(probe_urls))
        batch_size = min(max(self._config.common_probe_concurrency, 1), len(probe_urls))
        start_index = 0
        empty_batches = 0
        while start_index < len(probe_urls) and len(result) < probe_target:
            batch = probe_urls[start_index : start_index + batch_size]
            start_index += batch_size
            batch_hits = self._probe_common_value_batch(batch)
            result = _merge_unique_urls(
                result,
                batch_hits,
                limit=probe_target,
            )
            if batch_hits:
                empty_batches = 0
            else:
                empty_batches += 1
            if self._should_stop_common_probe_scan(
                batch_count=max(start_index // max(batch_size, 1), 1),
                hit_count=len(result),
                empty_batches=empty_batches,
            ):
                break
        return result

    def _probe_common_value_batch(self, probe_urls: list[str]) -> list[str]:
        if not probe_urls:
            return []
        futures: dict[Future, str] = {}
        results: list[str] = []
        wait_deadline = time.monotonic() + self._resolve_timeout()
        for probe_url in probe_urls:
            futures[self._probe_executor.submit(self._probe_common_value_url, probe_url)] = probe_url
        while futures:
            remaining = wait_deadline - time.monotonic()
            if remaining <= 0:
                break
            done, _ = wait(futures.keys(), timeout=remaining, return_when=FIRST_COMPLETED)
            if not done:
                break
            for future in done:
                futures.pop(future, None)
                try:
                    keep = future.result()
                except Exception:  # noqa: BLE001
                    continue
                if keep:
                    results.append(str(keep))
        for future in futures:
            future.cancel()
        return results

    def _probe_common_value_url(self, probe_url: str) -> str | None:
        session = self._get_or_create_session()
        try:
            html_text = self._fetch_html(
                session,
                probe_url,
                required=False,
                timeout_seconds=min(self._config.timeout_seconds, 4.0),
                max_retries_override=0,
            )
        except ProtocolPermanentError as exc:
            return probe_url if "challenge" in str(exc).lower() else None
        return probe_url if html_text.strip() else None

    def _has_enough_discovery_hits(self, urls: list[str]) -> bool:
        return len(urls) >= max(self._config.common_probe_target, 1)

    def _should_stop_common_probe_scan(self, *, batch_count: int, hit_count: int, empty_batches: int) -> bool:
        if hit_count >= max(self._config.common_probe_target, 1):
            return True
        if empty_batches >= max(self._config.common_probe_patience_batches, 1):
            return True
        if batch_count < max(self._config.common_probe_patience_batches, 1):
            return False
        return hit_count < max(self._config.common_probe_min_hits_after_patience, 1)

    def _discover_related_subdomain_urls(
        self,
        session: cffi_requests.Session,
        *,
        start_url: str,
        homepage_html: str,
        direct_urls: list[str],
        limit: int,
    ) -> list[str]:
        site_domain = _extract_registrable_domain(start_url)
        if not site_domain:
            return []
        probe_urls = [start_url, *_pick_subdomain_probe_urls(start_url, direct_urls)]
        related_seeds: list[str] = []
        for probe_url in probe_urls:
            if len(related_seeds) >= self._config.related_seed_limit:
                break
            try:
                html_text = homepage_html if probe_url == start_url else self._fetch_html(session, probe_url, required=False)
            except ProtocolPermanentError:
                continue
            if not html_text.strip():
                continue
            related_seeds = _merge_unique_urls(
                related_seeds,
                _extract_same_org_seed_urls(html_text, probe_url, site_domain=site_domain, limit=8),
                limit=8,
            )
        if not related_seeds:
            return []
        result: list[str] = []
        per_seed_limit = max(min(limit // max(len(related_seeds), 1), 60), 20)
        for seed_url in related_seeds[: self._config.related_seed_limit]:
            result = _merge_unique_urls(result, [seed_url], limit=limit)
            try:
                extra_urls, _ = self._discover_direct_urls(session, seed_url, limit=per_seed_limit)
            except ProtocolPermanentError:
                continue
            result = _merge_unique_urls(result, extra_urls, limit=limit)
            if len(result) >= limit:
                break
        return result

    def _fetch_sitemap_text(self, session: cffi_requests.Session, url: str) -> str:
        try:
            request_timeout = self._resolve_timeout()
            with _request_slot(timeout_seconds=request_timeout):
                response = session.get(url, timeout=request_timeout)
            if int(response.status_code) != 200:
                return ""
            content = response.content or b""
            if url.endswith(".gz") or content[:2] == b"\x1f\x8b":
                try:
                    content = gzip.decompress(content)
                except Exception:  # noqa: BLE001
                    pass
            return _decode_bytes(content, str(response.headers.get("Content-Type", "") or ""))
        except Exception:  # noqa: BLE001
            return ""

def _should_try_http_fallback(url: str, lowered_error: str) -> bool:
    if not url.lower().startswith("https://"):
        return False
    return any(
        token in lowered_error
        for token in (
            "ssl certificate",
            "certificate has expired",
            "certificate subject name",
            "tls connect error",
            "ssl:",
            "tlsv1_alert",
            "openssl_internal",
        )
    )


def _should_try_httpx_fallback(lowered_error: str) -> bool:
    return any(
        token in lowered_error
        for token in (
            "getaddrinfo() thread failed to start",
            "thread failed to start",
            "couldn't create thread",
            "failed to create thread",
        )
    )


def _replace_https_with_http(url: str) -> str:
    if url.lower().startswith("https://"):
        return f"http://{url[8:]}"
    return url


def _build_www_fallback_url(url: str, lowered_error: str) -> str:
    if not any(token in lowered_error for token in ("connection closed abruptly", "empty reply from server", "connection reset", "recv failure")):
        return ""
    parsed = urlparse(str(url or ""))
    host = str(parsed.netloc or "").strip()
    if not host or host.lower().startswith("www."):
        return ""
    return parsed._replace(netloc=f"www.{host}").geturl()


def _is_supported_response(url: str, content_type: str) -> bool:
    if not _is_supported_url(url):
        return False
    return any(hint in content_type for hint in _TEXT_HINTS) or not content_type

def _truncate_html(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    head_keep = max(max_chars // 3, 1)
    tail_keep = max(max_chars // 3, 1)
    middle_budget = max(max_chars - head_keep - tail_keep - 96, 0)
    middle = _collect_signal_html_windows(text, middle_budget)
    if middle:
        parts = [middle, "\n<!-- 页面内容过长已截断，已保留中部重点片段 -->\n", text[:head_keep]]
    else:
        parts = [text[:head_keep]]
    parts.extend(["\n<!-- 页面内容过长已截断 -->\n", text[-tail_keep:]])
    return "".join(parts)[:max_chars]


def _collect_signal_html_windows(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    windows = _merge_html_signal_windows(_find_html_signal_windows(text))
    if not windows:
        return ""
    lines: list[str] = []
    for start, end in windows:
        fragment_lines = _extract_signal_lines(text[start:end])
        if not fragment_lines:
            continue
        for line in fragment_lines:
            if line not in lines:
                lines.append(line)
        summary = _render_signal_summary(lines)
        if len(summary) >= max_chars:
            return summary[:max_chars]
    return _render_signal_summary(lines)[:max_chars]


def _find_html_signal_windows(text: str) -> list[tuple[int, int]]:
    windows: list[tuple[int, int]] = []
    for pattern in _HTML_SIGNAL_PATTERNS:
        for match in pattern.finditer(text):
            start = max(match.start() - 900, 0)
            end = min(match.end() + 1600, len(text))
            windows.append((start, end))
            if len(windows) >= 24:
                return windows
    return windows


def _merge_html_signal_windows(windows: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not windows:
        return []
    ordered = sorted(windows)
    merged: list[tuple[int, int]] = []
    start, end = ordered[0]
    for next_start, next_end in ordered[1:]:
        if next_start <= end + 256:
            end = max(end, next_end)
            continue
        merged.append((start, end))
        start, end = next_start, next_end
    merged.append((start, end))
    return merged[:8]


def _extract_signal_lines(fragment: str) -> list[str]:
    text = html.unescape(re.sub(r"<[^>]+>", "\n", str(fragment or "")))
    raw_lines = []
    for raw_line in text.splitlines():
        clean = re.sub(r"\s+", " ", raw_line).strip()
        if len(clean) >= 2:
            raw_lines.append(clean)
    if not raw_lines:
        return []
    picked: list[str] = []
    for index, line in enumerate(raw_lines):
        lowered = line.lower()
        if not (_EMAIL_SIGNAL_RE.search(line) or any(token in lowered for token in (
            "founder", "co-founder", "owner", "chairman", "chief executive", "managing director",
            "group chief executive", "president", "principal solicitor", "director", "lead guide",
            "leadership", "executive team",
        ))):
            continue
        start = max(index - 1, 0)
        end = min(index + 3, len(raw_lines))
        for candidate in raw_lines[start:end]:
            if candidate not in picked:
                picked.append(candidate)
    if picked:
        return picked[:24]
    fallback: list[str] = []
    for line in raw_lines:
        if line not in fallback:
            fallback.append(line)
        if len(fallback) >= 12:
            break
    return fallback


def _render_signal_summary(lines: list[str]) -> str:
    if not lines:
        return ""
    body = "".join(f"<p>{html.escape(line)}</p>" for line in lines)
    return f"<section data-oldiron-signal='1'><h2>重点正文片段</h2>{body}</section>"


def _raise_if_challenge_page(url: str, html_text: str) -> None:
    challenge_kind = _detect_challenge_kind(html_text)
    if not challenge_kind:
        return
    raise ProtocolPermanentError(f"{challenge_kind}: {url}")


def _detect_challenge_kind(html_text: str) -> str:
    lowered = str(html_text or "").lower()
    if not lowered:
        return ""
    if any(hint in lowered for hint in _CLOUDFLARE_CHALLENGE_HINTS):
        return "cloudflare_challenge"
    if any(hint in lowered for hint in _INCAPSULA_CHALLENGE_HINTS):
        return "imperva_challenge"
    return ""

@contextmanager
def _request_slot(*, timeout_seconds: float | None = None):
    wait_timeout = None if timeout_seconds is None else max(timeout_seconds, 0.01)
    acquired = _REQUEST_SLOT_SEMAPHORE.acquire(timeout=wait_timeout)
    if not acquired:
        raise ProtocolTemporaryError("request_slot_timeout")
    try:
        yield
    finally:
        _REQUEST_SLOT_SEMAPHORE.release()


def _decode_response_text(response: object) -> str:
    content = getattr(response, "content", b"")
    headers = getattr(response, "headers", {})
    return _decode_bytes(bytes(content or b""), str(headers.get("Content-Type", "") or ""))


def _decode_bytes(content: bytes, content_type: str) -> str:
    if not content:
        return ""
    encodings = _candidate_encodings(content_type, content)
    for encoding in encodings:
        try:
            return content.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue
    return content.decode("utf-8", errors="replace")


def _candidate_encodings(content_type: str, content: bytes) -> list[str]:
    values: list[str] = []
    match = _CHARSET_RE.search(str(content_type or ""))
    if match is not None:
        values.append(str(match.group(1) or "").strip().lower())
    head = bytes(content[:4096])
    for pattern in (_HTML_META_CHARSET_RE, _XML_ENCODING_RE):
        match = pattern.search(head)
        if match is None:
            continue
        values.append(match.group(1).decode("ascii", errors="ignore").strip().lower())
    for fallback in ("utf-8", "utf-8-sig", "cp932", "shift_jis", "euc_jp", "latin-1"):
        if fallback not in values:
            values.append(fallback)
    return values
