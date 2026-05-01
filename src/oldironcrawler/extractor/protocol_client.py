from __future__ import annotations

import gzip
import re
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, wait
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree

import httpx
from curl_cffi import requests as cffi_requests

from oldironcrawler.challenge_solver import resolve_cloudflare_challenge
from oldironcrawler.extractor.page_pool import PageFetchPool
from oldironcrawler.extractor.protocol.content import (
    decode_bytes as _decode_bytes,
    decode_response_text as _decode_response_text,
    detect_challenge_kind as _detect_challenge_kind,
    raise_if_challenge_page as _raise_if_challenge_page,
    truncate_html as _truncate_html,
)
from oldironcrawler.extractor.protocol.errors import (
    PERMANENT_ERROR_HINTS as _PERMANENT_ERROR_HINTS,
    TEMP_ERROR_HINTS as _TEMP_ERROR_HINTS,
    ProtocolPermanentError,
    ProtocolTemporaryError,
    is_fast_fail_tls_handshake_error as _is_fast_fail_tls_handshake_error,
    normalize_homepage_open_error as _normalize_homepage_open_error,
    should_abort_common_probe_after_homepage_error as _should_abort_common_probe_after_homepage_error,
)
from oldironcrawler.extractor.protocol.fallbacks import (
    build_empty_page_batch_error as _build_empty_page_batch_error,
    build_www_fallback_url as _build_www_fallback_url,
    is_supported_response as _is_supported_response,
    replace_https_with_http as _replace_https_with_http,
    should_try_http_fallback as _should_try_http_fallback,
    should_try_httpx_fallback as _should_try_httpx_fallback,
    should_try_httpx_status_fallback as _should_try_httpx_status_fallback,
)
from oldironcrawler.extractor.protocol.types import DiscoveryStageResult, HtmlPage, SiteProtocolConfig
from oldironcrawler.extractor.protocol_discovery import (
    build_common_probe_urls as _build_common_probe_urls,
    extract_registrable_domain as _extract_registrable_domain,
    extract_same_org_seed_urls as _extract_same_org_seed_urls,
    extract_same_site_links as _extract_same_site_links,
    is_supported_url as _is_supported_url,
    merge_unique_urls as _merge_unique_urls,
    pick_subdomain_probe_urls as _pick_subdomain_probe_urls,
    prioritize_discovery_urls as _prioritize_discovery_urls,
)
from oldironcrawler.extractor.protocol_runtime import configure_protocol_runtime, get_probe_executor, request_slot

_ROBOTS_SITEMAP_RE = re.compile(r"^Sitemap:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
_SITE_DEADLINE_SAFETY_SECONDS = 8.0
_REQUEST_SLOT_WAIT_FLOOR_SECONDS = 20.0
_REQUEST_SLOT_WAIT_CAP_SECONDS = 45.0
_REQUEST_SLOT_WAIT_MULTIPLIER = 4.0
_DISCOVERY_HOMEPAGE_TIMEOUT_CAP_SECONDS = 20.0
_COMMON_PROBE_REQUEST_TIMEOUT_SECONDS = 3.0
_COMMON_PROBE_BATCH_WAIT_CAP_SECONDS = 6.0
_COMMON_PROBE_SLOT_WAIT_SECONDS = 2.0


class SiteProtocolClient:
    def __init__(self, config: SiteProtocolConfig) -> None:
        self._config = config
        configure_protocol_runtime(
            probe_workers=max(config.probe_worker_count, 1),
            request_slots=max(config.request_slot_limit, 1),
        )
        self._http_client = self._build_httpx_client()
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
            pages = page_pool.fetch_pages(
                urls=filtered,
                fetch_one=lambda url: self._call_fetch_page_optional(
                    url,
                    timeout_seconds=min(self._config.timeout_seconds, max(deadline - time.monotonic(), 0.01)),
                    request_deadline_monotonic=deadline,
                ),
                deadline_monotonic=deadline,
            )
            if pages:
                return pages
            raise ProtocolTemporaryError(_build_empty_page_batch_error(filtered))
        for url in filtered:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                break
            try:
                page = self._call_fetch_page_optional(
                    url,
                    timeout_seconds=min(self._config.timeout_seconds, remaining),
                    request_deadline_monotonic=deadline,
                )
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                continue
            if page is not None and page.html.strip():
                pages.append(page)
        if not pages and last_error is not None:
            raise last_error
        if not pages and timed_out:
            raise TimeoutError("page_batch_timeout")
        if not pages and filtered:
            raise ProtocolTemporaryError(_build_empty_page_batch_error(filtered))
        return pages
    def _fetch_page_optional(
        self,
        url: str,
        *,
        timeout_seconds: float | None = None,
        request_deadline_monotonic: float | None = None,
    ) -> HtmlPage | None:
        session = self._get_or_create_session()
        html_text = self._fetch_html(
            session,
            url,
            required=False,
            timeout_seconds=timeout_seconds,
            request_deadline_monotonic=request_deadline_monotonic,
        )
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
        return httpx.Client(**self._build_httpx_client_kwargs(self._config.timeout_seconds))

    def _build_httpx_client_kwargs(self, timeout_seconds: float) -> dict[str, object]:
        client_kwargs: dict[str, object] = {
            "follow_redirects": True,
            "headers": dict(self._config.default_headers),
            "timeout": timeout_seconds,
            "limits": httpx.Limits(max_connections=128, max_keepalive_connections=32, keepalive_expiry=30.0),
            "trust_env": False,
        }
        if self._config.proxy_url:
            client_kwargs["proxy"] = self._config.proxy_url
        return client_kwargs
    def _fetch_html(
        self,
        session: cffi_requests.Session,
        url: str,
        *,
        required: bool,
        timeout_seconds: float | None = None,
        max_retries_override: int | None = None,
        request_slot_wait_seconds: float | None = None,
        request_deadline_monotonic: float | None = None,
        allow_httpx_fallback: bool = True,
    ) -> str:
        retries = self._config.max_retries if max_retries_override is None else max(max_retries_override, 0)
        attempts = retries + 1
        last_error: Exception | None = None
        for _ in range(attempts):
            response = None
            try:
                request_timeout = self._resolve_timeout(
                    timeout_seconds,
                    deadline_monotonic=request_deadline_monotonic,
                )
                with request_slot(
                    timeout_seconds=request_timeout,
                    wait_timeout_seconds=self._bounded_request_slot_wait_timeout(
                        request_timeout,
                        request_slot_wait_seconds,
                        deadline_monotonic=request_deadline_monotonic,
                    ),
                ):
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
                    original_text = _truncate_html(_decode_response_text(response), self._config.max_html_chars)
                    original_kind = _detect_challenge_kind(original_text)
                    challenge_text = self._maybe_challenge_fallback(session, url, original_text, request_timeout)
                    _raise_if_challenge_page(url, challenge_text)
                    if original_kind and challenge_text.strip():
                        return challenge_text
                    raise ProtocolPermanentError(f"http_403: {url}")
                if status in {202, 404}:
                    response_text = _truncate_html(_decode_response_text(response), self._config.max_html_chars)
                    httpx_html = self._try_httpx_status_fallback(
                        url,
                        status_code=status,
                        response_text=response_text,
                        timeout_seconds=request_timeout,
                        request_deadline_monotonic=request_deadline_monotonic,
                    )
                    if httpx_html is not None:
                        return httpx_html
                    _raise_if_challenge_page(url, response_text)
                if status == 404:
                    return ""
                if required:
                    raise ProtocolPermanentError(f"http_{status}: {url}")
                return ""
            except ProtocolTemporaryError as exc:
                last_error = exc
            except ProtocolPermanentError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                lowered = str(exc).lower()
                if _is_fast_fail_tls_handshake_error(lowered):
                    raise ProtocolPermanentError(str(exc)) from exc
                insecure_html = self._call_optional_fallback(
                    self._try_insecure_https_fallback,
                    url,
                    lowered,
                    request_deadline_monotonic=request_deadline_monotonic,
                )
                if insecure_html is not None:
                    return insecure_html
                fallback_html = self._call_optional_fallback(
                    self._try_http_fallback,
                    session,
                    url,
                    lowered,
                    request_deadline_monotonic=request_deadline_monotonic,
                )
                if fallback_html is not None:
                    return fallback_html
                www_html = self._call_optional_fallback(
                    self._try_www_fallback,
                    session,
                    url,
                    lowered,
                    request_deadline_monotonic=request_deadline_monotonic,
                )
                if www_html is not None:
                    return www_html
                if allow_httpx_fallback:
                    httpx_html = self._call_optional_fallback(
                        self._try_httpx_fallback,
                        url,
                        lowered,
                        timeout_seconds=request_timeout,
                        request_slot_wait_seconds=request_slot_wait_seconds,
                        request_deadline_monotonic=request_deadline_monotonic,
                    )
                    if httpx_html is not None:
                        return httpx_html
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

    def _resolve_timeout(
        self,
        timeout_seconds: float | None = None,
        *,
        deadline_monotonic: float | None = None,
    ) -> float:
        base_timeout = timeout_seconds if timeout_seconds is not None else self._config.timeout_seconds
        remaining = self._remaining_deadline_seconds(deadline_monotonic=deadline_monotonic)
        if remaining is None:
            return max(base_timeout, 0.05)
        if remaining <= 0:
            raise ProtocolTemporaryError("site_deadline_exceeded")
        return max(min(base_timeout, remaining), 0.05)

    def _remaining_deadline_seconds(self, *, deadline_monotonic: float | None = None) -> float | None:
        if deadline_monotonic is not None:
            return deadline_monotonic - time.monotonic()
        if self._config.deadline_monotonic is None:
            return None
        return self._config.deadline_monotonic - time.monotonic() - _SITE_DEADLINE_SAFETY_SECONDS

    def _resolve_request_slot_wait_timeout(
        self,
        request_timeout_seconds: float,
        *,
        deadline_monotonic: float | None = None,
    ) -> float:
        base_timeout = max(float(request_timeout_seconds or self._config.timeout_seconds or 0.0), 0.05)
        wait_timeout = min(
            max(base_timeout * _REQUEST_SLOT_WAIT_MULTIPLIER, _REQUEST_SLOT_WAIT_FLOOR_SECONDS),
            _REQUEST_SLOT_WAIT_CAP_SECONDS,
        )
        remaining = self._remaining_deadline_seconds(deadline_monotonic=deadline_monotonic)
        if remaining is None:
            return wait_timeout
        if remaining <= 0:
            raise ProtocolTemporaryError("site_deadline_exceeded")
        return max(min(wait_timeout, remaining), 0.05)

    def _bounded_request_slot_wait_timeout(
        self,
        request_timeout_seconds: float,
        wait_timeout_seconds: float | None,
        *,
        deadline_monotonic: float | None = None,
    ) -> float:
        resolved = self._resolve_request_slot_wait_timeout(
            request_timeout_seconds,
            deadline_monotonic=deadline_monotonic,
        )
        if wait_timeout_seconds is None:
            return resolved
        return max(min(resolved, float(wait_timeout_seconds)), 0.01)

    def _call_optional_fallback(self, func, *args, **kwargs):
        pending_kwargs = dict(kwargs)
        optional_keys = ("request_deadline_monotonic", "request_slot_wait_seconds", "timeout_seconds")
        while True:
            try:
                return func(*args, **pending_kwargs)
            except TypeError as exc:
                lowered = str(exc).lower()
                if "unexpected keyword" not in lowered:
                    raise
                removed = False
                for key in optional_keys:
                    if key in pending_kwargs:
                        pending_kwargs.pop(key, None)
                        removed = True
                        break
                if not removed:
                    raise

    def _call_fetch_page_optional(self, url: str, **kwargs):
        try:
            return self._fetch_page_optional(url, **kwargs)
        except TypeError as exc:
            if "unexpected keyword" not in str(exc).lower():
                raise
            fallback_kwargs = dict(kwargs)
            fallback_kwargs.pop("request_deadline_monotonic", None)
            return self._fetch_page_optional(url, **fallback_kwargs)

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
            detect_challenge_kind=_detect_challenge_kind,
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
            with request_slot(
                timeout_seconds=request_timeout,
                wait_timeout_seconds=self._resolve_request_slot_wait_timeout(request_timeout),
            ):
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

    def _try_httpx_fallback(
        self,
        url: str,
        lowered_error: str,
        *,
        timeout_seconds: float | None = None,
        request_slot_wait_seconds: float | None = None,
        request_deadline_monotonic: float | None = None,
    ) -> str | None:
        if not _should_try_httpx_fallback(lowered_error):
            return None
        try:
            return self._request_httpx_html(
                url,
                timeout_seconds=timeout_seconds,
                request_slot_wait_seconds=request_slot_wait_seconds,
                request_deadline_monotonic=request_deadline_monotonic,
            )
        except Exception:  # noqa: BLE001
            return None

    def _try_httpx_status_fallback(
        self,
        url: str,
        *,
        status_code: int,
        response_text: str,
        timeout_seconds: float | None = None,
        request_deadline_monotonic: float | None = None,
    ) -> str | None:
        if not _should_try_httpx_status_fallback(url, status_code, response_text):
            return None
        try:
            return self._request_httpx_html(
                url,
                timeout_seconds=timeout_seconds,
                request_deadline_monotonic=request_deadline_monotonic,
            )
        except Exception:  # noqa: BLE001
            return None

    def _request_httpx_html(
        self,
        url: str,
        *,
        timeout_seconds: float | None = None,
        request_slot_wait_seconds: float | None = None,
        request_deadline_monotonic: float | None = None,
    ) -> str | None:
        request_timeout = self._resolve_timeout(
            timeout_seconds,
            deadline_monotonic=request_deadline_monotonic,
        )
        last_challenge_html = ""
        for _attempt in range(2):
            status, content_type, response_text = self._fetch_httpx_snapshot(
                url,
                timeout_seconds=request_timeout,
                fresh_client=bool(_attempt),
                request_slot_wait_seconds=request_slot_wait_seconds,
                request_deadline_monotonic=request_deadline_monotonic,
            )
            if status == 200:
                if not _is_supported_response(url, content_type):
                    return None
                challenge_kind = _detect_challenge_kind(response_text)
                if challenge_kind:
                    last_challenge_html = response_text
                    continue
                return response_text
            if status in {202, 403} and _detect_challenge_kind(response_text):
                last_challenge_html = response_text
                continue
            return None
        if last_challenge_html:
            _raise_if_challenge_page(url, last_challenge_html)
        return None

    def _fetch_httpx_snapshot(
        self,
        url: str,
        *,
        timeout_seconds: float,
        fresh_client: bool,
        request_slot_wait_seconds: float | None = None,
        request_deadline_monotonic: float | None = None,
    ) -> tuple[int, str, str]:
        if fresh_client:
            with httpx.Client(**self._build_httpx_client_kwargs(timeout_seconds)) as client:
                with request_slot(
                    timeout_seconds=timeout_seconds,
                    wait_timeout_seconds=self._bounded_request_slot_wait_timeout(
                        timeout_seconds,
                        request_slot_wait_seconds,
                        deadline_monotonic=request_deadline_monotonic,
                    ),
                ):
                    response = client.get(url, timeout=timeout_seconds)
                return (
                    int(response.status_code),
                    str(response.headers.get("Content-Type", "") or "").lower(),
                    _truncate_html(str(response.text or ""), self._config.max_html_chars),
                )
        with request_slot(
            timeout_seconds=timeout_seconds,
            wait_timeout_seconds=self._bounded_request_slot_wait_timeout(
                timeout_seconds,
                request_slot_wait_seconds,
                deadline_monotonic=request_deadline_monotonic,
            ),
        ):
            response = self._http_client.get(url, timeout=timeout_seconds)
        return (
            int(response.status_code),
            str(response.headers.get("Content-Type", "") or "").lower(),
            _truncate_html(str(response.text or ""), self._config.max_html_chars),
        )

    def _try_insecure_https_fallback(
        self,
        url: str,
        lowered_error: str,
        *,
        request_deadline_monotonic: float | None = None,
    ) -> str | None:
        if not _should_try_http_fallback(url, lowered_error):
            return None
        request_timeout = self._resolve_timeout(deadline_monotonic=request_deadline_monotonic)
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
                with request_slot(
                    timeout_seconds=request_timeout,
                    wait_timeout_seconds=self._resolve_request_slot_wait_timeout(
                        request_timeout,
                        deadline_monotonic=request_deadline_monotonic,
                    ),
                ):
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

    def _try_http_fallback(
        self,
        session: cffi_requests.Session,
        url: str,
        lowered_error: str,
        *,
        request_deadline_monotonic: float | None = None,
    ) -> str | None:
        if not _should_try_http_fallback(url, lowered_error):
            return None
        fallback_url = _replace_https_with_http(url)
        response = None
        try:
            request_timeout = self._resolve_timeout(deadline_monotonic=request_deadline_monotonic)
            with request_slot(
                timeout_seconds=request_timeout,
                wait_timeout_seconds=self._resolve_request_slot_wait_timeout(
                    request_timeout,
                    deadline_monotonic=request_deadline_monotonic,
                ),
            ):
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

    def _try_www_fallback(
        self,
        session: cffi_requests.Session,
        url: str,
        lowered_error: str,
        *,
        request_deadline_monotonic: float | None = None,
    ) -> str | None:
        fallback_url = _build_www_fallback_url(url, lowered_error)
        if not fallback_url:
            return None
        response = None
        try:
            request_timeout = self._resolve_timeout(deadline_monotonic=request_deadline_monotonic)
            with request_slot(
                timeout_seconds=request_timeout,
                wait_timeout_seconds=self._resolve_request_slot_wait_timeout(
                    request_timeout,
                    deadline_monotonic=request_deadline_monotonic,
                ),
            ):
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
        scan_limit = min(max(limit * 4, limit), 400)
        urls: list[str] = []
        visited: set[str] = set()
        base_host = (urlparse(base_url).netloc or "").strip().lower()
        for location in locations:
            if len(urls) >= scan_limit:
                break
            self._parse_sitemap_recursive(session, location, urls, visited, base_host=base_host, limit=scan_limit, depth=0)
        return _prioritize_discovery_urls(base_url, urls, limit=limit)

    def _find_sitemap_locations(self, session: cffi_requests.Session, base_url: str) -> list[str]:
        robots_url = urljoin(base_url, "/robots.txt")
        try:
            request_timeout = self._resolve_timeout()
            with request_slot(
                timeout_seconds=request_timeout,
                wait_timeout_seconds=self._resolve_request_slot_wait_timeout(request_timeout),
            ):
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
            homepage_html = self._fetch_discovery_homepage(session, start_url)
        except (ProtocolPermanentError, ProtocolTemporaryError) as exc:
            homepage_error = exc
        if homepage_error is not None and _should_abort_common_probe_after_homepage_error(homepage_error):
            raise _normalize_homepage_open_error(start_url, homepage_error) from homepage_error
        guessed_urls = self._probe_common_value_urls(session, start_url, limit=limit)
        homepage_links = _extract_same_site_links(homepage_html, start_url, limit=limit) if homepage_html else []
        merged = _merge_unique_urls(guessed_urls, homepage_links, limit=limit)
        if homepage_html:
            return merged, homepage_html
        if guessed_urls:
            return guessed_urls, ""
        if homepage_error is not None:
            raise homepage_error
        return [], ""

    def _fetch_discovery_homepage(self, session: cffi_requests.Session, start_url: str) -> str:
        timeout_seconds = min(self._config.timeout_seconds, _DISCOVERY_HOMEPAGE_TIMEOUT_CAP_SECONDS)
        request_deadline = time.monotonic() + timeout_seconds
        try:
            return self._fetch_html(
                session,
                start_url,
                required=False,
                timeout_seconds=timeout_seconds,
                max_retries_override=0,
                request_deadline_monotonic=request_deadline,
                allow_httpx_fallback=False,
            )
        except TypeError as exc:
            if "unexpected keyword" not in str(exc).lower():
                raise
            return self._fetch_html(session, start_url, required=False, timeout_seconds=timeout_seconds)

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
        batch_timeout = min(self._config.timeout_seconds, _COMMON_PROBE_BATCH_WAIT_CAP_SECONDS)
        wait_deadline = time.monotonic() + self._resolve_timeout(batch_timeout)
        for probe_url in probe_urls:
            futures[get_probe_executor().submit(self._probe_common_value_url, probe_url)] = probe_url
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
                timeout_seconds=min(self._config.timeout_seconds, _COMMON_PROBE_REQUEST_TIMEOUT_SECONDS),
                max_retries_override=0,
                request_slot_wait_seconds=_COMMON_PROBE_SLOT_WAIT_SECONDS,
                allow_httpx_fallback=False,
            )
        except ProtocolPermanentError:
            # 公共探测阶段只保留真实正文页，挑战页不再当成“命中页”。
            return None
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
            with request_slot(
                timeout_seconds=request_timeout,
                wait_timeout_seconds=self._resolve_request_slot_wait_timeout(request_timeout),
            ):
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
