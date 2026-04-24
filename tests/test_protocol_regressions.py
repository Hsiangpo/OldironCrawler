from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.extractor.protocol_client import ProtocolPermanentError, ProtocolTemporaryError, SiteProtocolClient, SiteProtocolConfig
from oldironcrawler.extractor.service import _build_site_protocol_config
from oldironcrawler import challenge_solver as challenge_module


def test_detect_challenge_kind_supports_sgcaptcha_pages() -> None:
    html_text = (
        '<html><head><meta http-equiv="refresh" '
        'content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>'
    )

    assert SiteProtocolClient.__module__  # 保持导入使用，避免静态检查误报
    from oldironcrawler.extractor import protocol_client as protocol_module

    assert protocol_module._detect_challenge_kind(html_text) == "sgcaptcha_challenge"


def test_protocol_fetch_html_uses_httpx_fallback_when_curl_gets_sgcaptcha(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        def __init__(self, status_code: int, html_text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self.headers = {"Content-Type": content_type}
            self.text = html_text
            self.content = html_text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            return FakeResponse(
                202,
                '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>',
            )

    monkeypatch.setattr(
        client,
        "_fetch_httpx_snapshot",
        lambda *_args, **_kwargs: (200, "text/html", "<html><body>httpx fallback ok</body></html>"),
    )

    html = client._fetch_html(FakeSession(), "https://example.com", required=False)

    assert "httpx fallback ok" in html
    client.close()


def test_protocol_fetch_html_uses_httpx_fallback_when_curl_returns_false_404(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        def __init__(self, status_code: int, html_text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self.headers = {"Content-Type": content_type}
            self.text = html_text
            self.content = html_text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            return FakeResponse(404, "<html><body>wix error page</body></html>")

    monkeypatch.setattr(
        client,
        "_fetch_httpx_snapshot",
        lambda *_args, **_kwargs: (200, "text/html", "<html><body>real homepage</body></html>"),
    )

    html = client._fetch_html(FakeSession(), "https://example.com", required=False)

    assert "real homepage" in html
    client.close()


def test_protocol_fetch_html_marks_plain_403_as_blocked(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        def __init__(self) -> None:
            self.status_code = 403
            self.headers = {"Content-Type": "text/html"}
            self.text = "<html><body>Access denied</body></html>"
            self.content = self.text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            return FakeResponse()

    monkeypatch.setattr(client, "_maybe_challenge_fallback", lambda *_args, **_kwargs: "<html><body>Access denied</body></html>")

    with pytest.raises(ProtocolPermanentError, match="http_403"):
        client._fetch_html(FakeSession(), "https://example.com", required=False)

    client.close()


def test_protocol_fetch_html_uses_httpx_fallback_when_curl_times_out(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            raise RuntimeError("Failed to perform, curl: (28) Operation timed out after 5004 milliseconds")

    monkeypatch.setattr(
        client,
        "_fetch_httpx_snapshot",
        lambda *_args, **_kwargs: (200, "text/html", "<html><body>timeout fallback ok</body></html>"),
    )

    html = client._fetch_html(FakeSession(), "https://example.com", required=False)

    assert "timeout fallback ok" in html
    client.close()


def test_protocol_client_fetch_pages_raises_when_all_optional_fetches_return_empty() -> None:
    class EmptyClient(SiteProtocolClient):
        def _fetch_page_optional(self, url: str, *, timeout_seconds: float | None = None):
            return None

    client = EmptyClient(SiteProtocolConfig(page_batch_timeout_seconds=0.2))

    try:
        client.fetch_pages(["https://example.com/a", "https://example.com/b"], max_workers=2)
        raise AssertionError("expected ProtocolTemporaryError")
    except ProtocolTemporaryError as exc:
        assert "empty_page_batch" in str(exc)
    finally:
        client.close()


def test_protocol_fetch_html_retries_httpx_fallback_after_soft_challenge(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        def __init__(self, status_code: int, html_text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self.headers = {"Content-Type": content_type}
            self.text = html_text
            self.content = html_text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            return FakeResponse(
                202,
                '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>',
            )

    responses = iter(
        [
            (202, "text/html", '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>'),
            (200, "text/html", "<html><body>retry success</body></html>"),
        ]
    )

    def fake_httpx_snapshot(*_args, **_kwargs):
        return next(responses)

    monkeypatch.setattr(client, "_fetch_httpx_snapshot", fake_httpx_snapshot)

    html = client._fetch_html(FakeSession(), "https://example.com", required=False)

    assert "retry success" in html
    client.close()


def test_protocol_fetch_html_raises_sgcaptcha_when_httpx_fallback_is_still_challenged(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        def __init__(self, status_code: int, html_text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self.headers = {"Content-Type": content_type}
            self.content = html_text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            return FakeResponse(
                202,
                '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>',
            )

    monkeypatch.setattr(
        client,
        "_fetch_httpx_snapshot",
        lambda *_args, **_kwargs: (
            202,
            "text/html",
            '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>',
        ),
    )

    with pytest.raises(ProtocolPermanentError, match="sgcaptcha_challenge"):
        client._fetch_html(FakeSession(), "https://example.com", required=False)

    client.close()


def test_protocol_common_probe_does_not_keep_challenge_pages(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    def fake_fetch_html(*_args, **_kwargs):
        raise ProtocolPermanentError("sgcaptcha_challenge: https://example.com/executive-team")

    monkeypatch.setattr(client, "_fetch_html", fake_fetch_html)

    assert client._probe_common_value_url("https://example.com/executive-team") is None

    client.close()


def test_discover_primary_urls_still_probes_when_homepage_is_temporary_failure(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    def fake_fetch_html(_session, url: str, *, required: bool, timeout_seconds=None, max_retries_override=None):
        assert url == "https://example.com"
        raise ProtocolTemporaryError("temporary_http_503: https://example.com")

    monkeypatch.setattr(client, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr(client, "_probe_common_value_urls", lambda *_args, **_kwargs: ["https://example.com/impressum"])

    urls, homepage_html = client._discover_primary_urls(object(), "https://example.com", limit=20)

    assert urls == ["https://example.com/impressum"]
    assert homepage_html == ""
    client.close()


def test_resolve_cloudflare_challenge_skips_non_cloudflare_pages(monkeypatch) -> None:
    monkeypatch.setattr(
        challenge_module,
        "_run_cloudscraper_fallback",
        lambda **_kwargs: pytest.fail("non-cloudflare pages must not enter cloudscraper fallback"),
    )
    monkeypatch.setattr(
        challenge_module,
        "_run_capsolver_fallback",
        lambda **_kwargs: pytest.fail("non-cloudflare pages must not enter capsolver fallback"),
    )

    html_text = '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>'
    result = challenge_module.resolve_cloudflare_challenge(
        url="https://example.com",
        html_text=html_text,
        timeout_seconds=20.0,
        proxy_url="http://127.0.0.1:7897",
        cloudflare_proxy_url="",
        max_html_chars=250000,
        session_headers={"User-Agent": "UA-1"},
        cookie_jar=None,
        detect_challenge_kind=lambda _value: "sgcaptcha_challenge",
        refetch_html=lambda: "",
        impersonate="chrome110",
        capsolver_api_key="capsolver-key",
        capsolver_api_base_url="https://api.capsolver.com",
        capsolver_proxy="1.2.3.4:8080:user:pass",
        capsolver_poll_seconds=1.0,
        capsolver_max_wait_seconds=5.0,
    )

    assert result == html_text


def test_build_site_protocol_config_caps_page_batch_timeout() -> None:
    config = SimpleNamespace(
        request_timeout_seconds=10.0,
        total_wait_seconds=180.0,
        proxy_url="",
        capsolver_api_key="",
        capsolver_api_base_url="https://api.capsolver.com",
        capsolver_proxy="",
        capsolver_poll_seconds=3.0,
        capsolver_max_wait_seconds=40.0,
        cloudflare_proxy_url="",
        page_concurrency=32,
    )

    protocol_config = _build_site_protocol_config(config, None)

    assert protocol_config.page_batch_timeout_seconds == 40.0
