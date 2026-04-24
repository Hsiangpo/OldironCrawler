from __future__ import annotations

from dataclasses import dataclass
import importlib
import time
from typing import Any, Callable
from urllib.parse import urlparse

import httpx
from curl_cffi import requests as cffi_requests


@dataclass
class CookieRecord:
    name: str
    value: str
    domain: str
    path: str
    secure: bool
    expires: int | None


@dataclass
class CloudflareFallbackResult:
    html: str
    cookies: list[CookieRecord]
    status_code: int


@dataclass
class CapSolverResult:
    cookies: list[CookieRecord]
    user_agent: str
    token: str


def export_cookie_records(cookie_jar: Any) -> list[CookieRecord]:
    records: list[CookieRecord] = []
    for cookie in cookie_jar or []:
        records.append(
            CookieRecord(
                name=str(getattr(cookie, "name", "") or ""),
                value=str(getattr(cookie, "value", "") or ""),
                domain=str(getattr(cookie, "domain", "") or ""),
                path=str(getattr(cookie, "path", "") or "/"),
                secure=bool(getattr(cookie, "secure", False)),
                expires=_coerce_optional_int(getattr(cookie, "expires", None)),
            )
        )
    return [record for record in records if record.name]


def apply_cookie_records(cookie_jar: Any, cookies: list[CookieRecord]) -> None:
    for cookie in cookies:
        kwargs: dict[str, Any] = {"path": cookie.path or "/"}
        if cookie.domain:
            kwargs["domain"] = cookie.domain
        if cookie.secure:
            kwargs["secure"] = True
        if cookie.expires is not None:
            kwargs["expires"] = cookie.expires
        try:
            cookie_jar.set(cookie.name, cookie.value, **kwargs)
        except TypeError:
            kwargs.pop("expires", None)
            cookie_jar.set(cookie.name, cookie.value, **kwargs)


def fetch_with_cloudscraper(
    *,
    url: str,
    timeout_seconds: float,
    proxy_url: str,
    headers: dict[str, str],
    cookies: list[CookieRecord],
) -> CloudflareFallbackResult | None:
    module = _import_cloudscraper_module()
    if module is None:
        return None
    scraper = module.create_scraper(delay=5, doubleDown=True, interpreter="native")
    try:
        scraper.headers.update({str(key): str(value) for key, value in (headers or {}).items() if value})
        if proxy_url:
            scraper.proxies.update({"http": proxy_url, "https": proxy_url})
        apply_cookie_records(scraper.cookies, cookies)
        response = scraper.get(url, timeout=timeout_seconds, allow_redirects=True)
        return CloudflareFallbackResult(
            html=str(getattr(response, "text", "") or ""),
            cookies=export_cookie_records(scraper.cookies),
            status_code=int(getattr(response, "status_code", 0) or 0),
        )
    except Exception:  # noqa: BLE001
        return None
    finally:
        try:
            scraper.close()
        except Exception:  # noqa: BLE001
            pass


def solve_cloudflare_challenge(
    *,
    api_key: str,
    api_base_url: str,
    api_proxy_url: str,
    challenge_url: str,
    challenge_html: str,
    user_agent: str,
    proxy: str,
    poll_seconds: float,
    max_wait_seconds: float,
) -> CapSolverResult | None:
    if not str(api_key or "").strip() or not str(proxy or "").strip():
        return None
    client = _build_http_client(api_proxy_url, timeout_seconds=max_wait_seconds)
    try:
        task_id = _create_capsolver_task(
            client=client,
            api_key=api_key,
            api_base_url=api_base_url,
            challenge_url=challenge_url,
            challenge_html=challenge_html,
            user_agent=user_agent,
            proxy=proxy,
        )
        if not task_id:
            return None
        return _poll_capsolver_result(
            client=client,
            api_key=api_key,
            api_base_url=api_base_url,
            task_id=task_id,
            challenge_url=challenge_url,
            poll_seconds=poll_seconds,
            max_wait_seconds=max_wait_seconds,
        )
    except Exception:  # noqa: BLE001
        return None
    finally:
        client.close()


def resolve_cloudflare_challenge(
    *,
    url: str,
    html_text: str,
    timeout_seconds: float,
    proxy_url: str,
    cloudflare_proxy_url: str,
    max_html_chars: int,
    session_headers: dict[str, str],
    cookie_jar: Any,
    detect_challenge_kind: Callable[[str], str],
    refetch_html: Callable[[], str],
    impersonate: str,
    capsolver_api_key: str,
    capsolver_api_base_url: str,
    capsolver_proxy: str,
    capsolver_poll_seconds: float,
    capsolver_max_wait_seconds: float,
) -> str:
    initial_kind = str(detect_challenge_kind(html_text) or "").strip()
    if initial_kind != "cloudflare_challenge":
        return html_text
    challenge_proxy_url = _pick_challenge_proxy_url(cloudflare_proxy_url, proxy_url)
    challenge_html = _run_cloudscraper_fallback(
        url=url,
        html_text=html_text,
        timeout_seconds=timeout_seconds,
        proxy_url=challenge_proxy_url,
        headers=session_headers,
        cookie_jar=cookie_jar,
        max_html_chars=max_html_chars,
    )
    if not str(detect_challenge_kind(challenge_html) or "").strip():
        return challenge_html
    solved_html = _run_capsolver_fallback(
        url=url,
        challenge_html=challenge_html,
        cookie_jar=cookie_jar,
        session_headers=session_headers,
        refetch_html=refetch_html,
        capsolver_api_key=capsolver_api_key,
        capsolver_api_base_url=capsolver_api_base_url,
        capsolver_proxy=capsolver_proxy,
        capsolver_poll_seconds=capsolver_poll_seconds,
        capsolver_max_wait_seconds=capsolver_max_wait_seconds,
        api_proxy_url=proxy_url,
        challenge_proxy_url=challenge_proxy_url,
        timeout_seconds=timeout_seconds,
        max_html_chars=max_html_chars,
        impersonate=impersonate,
    )
    return solved_html or challenge_html


def normalize_capsolver_proxy(proxy_url: str) -> str:
    raw = str(proxy_url or "").strip()
    if not raw:
        return ""
    if raw.count(":") in {1, 3} and "://" not in raw and "@" not in raw:
        host = raw.split(":", 1)[0].strip().lower()
        return "" if host in {"127.0.0.1", "localhost"} else raw
    parsed = urlparse(raw if "://" in raw else f"http://{raw}")
    host = str(parsed.hostname or "").strip()
    try:
        port = parsed.port
    except ValueError:
        return ""
    if not host or not port:
        return ""
    if host in {"127.0.0.1", "localhost"}:
        return ""
    if parsed.username or parsed.password:
        return f"{host}:{port}:{parsed.username or ''}:{parsed.password or ''}".rstrip(":")
    return f"{host}:{port}"


def build_capsolver_cookie_records(url: str, cookies: dict[str, Any]) -> list[CookieRecord]:
    host = str(urlparse(url).hostname or "").strip()
    if not host:
        return []
    records: list[CookieRecord] = []
    for name, value in (cookies or {}).items():
        if not str(name or "").strip() or value in {None, ""}:
            continue
        records.append(
            CookieRecord(
                name=str(name),
                value=str(value),
                domain=host,
                path="/",
                secure=url.lower().startswith("https://"),
                expires=None,
            )
        )
    return records


def _create_capsolver_task(
    *,
    client: httpx.Client,
    api_key: str,
    api_base_url: str,
    challenge_url: str,
    challenge_html: str,
    user_agent: str,
    proxy: str,
) -> str:
    response = client.post(
        f"{api_base_url.rstrip('/')}/createTask",
        json={
            "clientKey": api_key,
            "task": {
                "type": "AntiCloudflareTask",
                "websiteURL": challenge_url,
                "proxy": proxy,
                "userAgent": str(user_agent or "").strip(),
                "html": str(challenge_html or "").strip(),
            },
        },
    )
    payload = response.json()
    if int(payload.get("errorId", 1)) != 0:
        return ""
    return str(payload.get("taskId", "") or "").strip()


def _poll_capsolver_result(
    *,
    client: httpx.Client,
    api_key: str,
    api_base_url: str,
    task_id: str,
    challenge_url: str,
    poll_seconds: float,
    max_wait_seconds: float,
) -> CapSolverResult | None:
    deadline = time.monotonic() + max(max_wait_seconds, 0.0)
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        response = client.post(
            f"{api_base_url.rstrip('/')}/getTaskResult",
            json={"clientKey": api_key, "taskId": task_id},
        )
        payload = response.json()
        if int(payload.get("errorId", 1)) != 0:
            return None
        status = str(payload.get("status", "") or "").strip().lower()
        if status == "ready":
            solution = payload.get("solution") or {}
            cookie_records = build_capsolver_cookie_records(challenge_url, solution.get("cookies") or {})
            token = str(solution.get("token", "") or "").strip()
            if token and not any(cookie.name == "cf_clearance" for cookie in cookie_records):
                cookie_records.append(
                    CookieRecord(
                        name="cf_clearance",
                        value=token,
                        domain=str(urlparse(challenge_url).hostname or "").strip(),
                        path="/",
                        secure=challenge_url.lower().startswith("https://"),
                        expires=None,
                    )
                )
            return CapSolverResult(
                cookies=cookie_records,
                user_agent=str(solution.get("userAgent", "") or "").strip(),
                token=token,
            )
        if status not in {"idle", "processing"}:
            return None
        time.sleep(min(poll_seconds, max(remaining, 0.0)))


def _build_http_client(api_proxy_url: str, timeout_seconds: float) -> httpx.Client:
    kwargs: dict[str, Any] = {
        "timeout": max(min(float(timeout_seconds or 0.0), 30.0), 5.0),
        "follow_redirects": True,
        "trust_env": False,
    }
    if str(api_proxy_url or "").strip():
        kwargs["proxy"] = api_proxy_url
    return httpx.Client(**kwargs)


def _import_cloudscraper_module():
    try:
        return importlib.import_module("cloudscraper")
    except Exception:  # noqa: BLE001
        return None


def _coerce_optional_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _run_cloudscraper_fallback(
    *,
    url: str,
    html_text: str,
    timeout_seconds: float,
    proxy_url: str,
    headers: dict[str, str],
    cookie_jar: Any,
    max_html_chars: int,
) -> str:
    result = fetch_with_cloudscraper(
        url=url,
        timeout_seconds=timeout_seconds,
        proxy_url=proxy_url,
        headers=headers,
        cookies=export_cookie_records(cookie_jar),
    )
    if result is None or int(result.status_code or 0) != 200 or not str(result.html or "").strip():
        return html_text
    if cookie_jar is not None and result.cookies:
        apply_cookie_records(cookie_jar, result.cookies)
    return str(result.html or "")[:max_html_chars]


def _run_capsolver_fallback(
    *,
    url: str,
    challenge_html: str,
    cookie_jar: Any,
    session_headers: dict[str, str],
    refetch_html: Callable[[], str],
    capsolver_api_key: str,
    capsolver_api_base_url: str,
    capsolver_proxy: str,
    capsolver_poll_seconds: float,
    capsolver_max_wait_seconds: float,
    api_proxy_url: str,
    challenge_proxy_url: str,
    timeout_seconds: float,
    max_html_chars: int,
    impersonate: str,
) -> str:
    proxy = normalize_capsolver_proxy(capsolver_proxy) or normalize_capsolver_proxy(challenge_proxy_url)
    if not proxy or not str(capsolver_api_key or "").strip():
        return ""
    result = solve_cloudflare_challenge(
        api_key=capsolver_api_key,
        api_base_url=capsolver_api_base_url,
        api_proxy_url=api_proxy_url or challenge_proxy_url,
        challenge_url=url,
        challenge_html=challenge_html,
        user_agent=str((session_headers or {}).get("User-Agent", "") or "").strip(),
        proxy=proxy,
        poll_seconds=capsolver_poll_seconds,
        max_wait_seconds=capsolver_max_wait_seconds,
    )
    if result is None or not result.cookies:
        return ""
    if result.user_agent:
        session_headers["User-Agent"] = result.user_agent
    if cookie_jar is not None:
        apply_cookie_records(cookie_jar, result.cookies)
    time.sleep(1.0)
    if challenge_proxy_url:
        solved_html = _refetch_with_temp_proxy(
            url=url,
            timeout_seconds=timeout_seconds,
            proxy_url=challenge_proxy_url,
            impersonate=impersonate,
            session_headers=session_headers,
            cookie_jar=cookie_jar,
            max_html_chars=max_html_chars,
        )
        if solved_html:
            return solved_html
    solved_html = refetch_html()
    if solved_html:
        return solved_html
    raise RuntimeError("challenge_refetch_failed")


def _pick_challenge_proxy_url(cloudflare_proxy_url: str, proxy_url: str) -> str:
    return str(cloudflare_proxy_url or proxy_url or "").strip()


def _refetch_with_temp_proxy(
    *,
    url: str,
    timeout_seconds: float,
    proxy_url: str,
    impersonate: str,
    session_headers: dict[str, str],
    cookie_jar: Any,
    max_html_chars: int,
) -> str:
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else {}
    session = cffi_requests.Session(impersonate=impersonate or "chrome110", proxies=proxies)
    response = None
    try:
        session.trust_env = False
        session.headers.update({str(key): str(value) for key, value in (session_headers or {}).items() if value})
        if cookie_jar is not None:
            apply_cookie_records(session.cookies, export_cookie_records(cookie_jar))
        response = session.get(url, timeout=timeout_seconds)
        if int(getattr(response, "status_code", 0) or 0) != 200:
            return ""
        if cookie_jar is not None:
            apply_cookie_records(cookie_jar, export_cookie_records(session.cookies))
        return str(getattr(response, "text", "") or "")[:max_html_chars]
    except Exception:  # noqa: BLE001
        return ""
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:  # noqa: BLE001
                pass
        try:
            session.close()
        except Exception:  # noqa: BLE001
            pass
