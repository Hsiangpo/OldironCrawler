from __future__ import annotations

import sys
import time
import resource
import threading
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.app import _raise_nofile_soft_limit
from oldironcrawler import challenge_solver as challenge_module
from oldironcrawler.extractor.company_rules import extract_company_name_fallback
from oldironcrawler.extractor.email_rules import collect_emails_for_pages
from oldironcrawler.extractor import llm_client as llm_module
from oldironcrawler.extractor import protocol_client as protocol_module
from oldironcrawler.challenge_solver import CloudflareFallbackResult, CapSolverResult
from oldironcrawler.extractor.llm_client import WebsiteLlmClient
from oldironcrawler.extractor.page_pool import PageFetchPool, PageFetchPoolConfig
from oldironcrawler.extractor.protocol_client import ProtocolPermanentError, ProtocolTemporaryError, SiteProtocolClient, SiteProtocolConfig
from oldironcrawler.extractor.protocol_client import HtmlPage
from oldironcrawler.extractor.umbraco_people import UmbracoBio, maybe_build_umbraco_people_page
from oldironcrawler.extractor.service import _merge_page_targets
from oldironcrawler.extractor.service import SiteProfileService
from oldironcrawler.extractor.value_rules import build_candidates, extract_path_tokens, select_email_urls, select_representative_urls
from oldironcrawler.importer import ImportedWebsite, choose_input_file, compute_rows_fingerprint, load_websites
from oldironcrawler.runtime.global_learning import GlobalLearningStore
from oldironcrawler.runner import _describe_error_reason, _describe_missing_reason, _looks_temporary_error
from oldironcrawler.runtime.store import RuntimeStore, SiteResult, SiteStageMetrics


def test_txt_loader_dedupes_by_full_website_then_domain(tmp_path: Path) -> None:
    input_file = tmp_path / "sites.txt"
    input_file.write_text(
        "\n".join(
            [
                "example.com",
                "https://example.com",
                "https://example.com/about",
                "https://example.com/contact",
                "sub.example.com/path",
            ]
        ),
        encoding="utf-8",
    )

    rows = load_websites(input_file)

    assert [row.website for row in rows] == [
        "https://example.com",
        "https://example.com/about",
        "https://example.com/contact",
        "https://sub.example.com/path",
    ]


def test_choose_input_file_accepts_exact_filename(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "工作簿1.xlsx"
    target.write_text("", encoding="utf-8")
    (tmp_path / "smoke.txt").write_text("example.com", encoding="utf-8")
    monkeypatch.setattr("builtins.input", lambda _prompt: "工作簿1.xlsx")

    chosen = choose_input_file(tmp_path)

    assert chosen == target


def test_rows_fingerprint_uses_normalized_rows() -> None:
    rows = [
        ImportedWebsite(input_index=1, raw_website="a.com", website="https://a.com", dedupe_key="a.com"),
        ImportedWebsite(input_index=2, raw_website="b.com/team", website="https://b.com/team", dedupe_key="https://b.com/team"),
    ]

    left = compute_rows_fingerprint(rows)
    right = compute_rows_fingerprint(list(rows))

    assert left == right


def test_store_failed_temp_goes_queue_tail_then_drops(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime.sqlite3"
    store = RuntimeStore(db_path)
    rows = [
        ImportedWebsite(input_index=1, raw_website="a.com", website="https://a.com", dedupe_key="a.com"),
        ImportedWebsite(input_index=2, raw_website="b.com", website="https://b.com", dedupe_key="b.com"),
    ]
    store.prepare_job(input_name="sites.txt", fingerprint="abc", rows=rows)
    first = store.claim_next_site()
    assert first is not None
    assert first.website == "https://a.com"

    store.mark_failed(first.id, "timeout")
    second = store.claim_next_site()
    assert second is not None
    assert second.website == "https://b.com"

    store.mark_done(
        second.id,
        SiteResult(company_name="B", representative="", emails="", website="https://b.com"),
    )
    retry = store.claim_next_site()
    assert retry is not None
    assert retry.website == "https://a.com"
    with store._connect() as conn:
        row = conn.execute("SELECT status, last_error, finished_at FROM sites WHERE id = ?", (retry.id,)).fetchone()
    assert row is not None
    assert row["status"] == "running"
    assert row["last_error"] == ""
    assert row["finished_at"] == ""

    store.mark_failed(retry.id, "timeout again")
    progress = store.progress()
    assert progress["dropped"] == 1


def test_completed_job_can_be_reset_for_rerun(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime.sqlite3"
    store = RuntimeStore(db_path)
    rows = [
        ImportedWebsite(input_index=1, raw_website="a.com", website="https://a.com", dedupe_key="a.com"),
        ImportedWebsite(input_index=2, raw_website="b.com", website="https://b.com", dedupe_key="b.com"),
    ]
    store.prepare_job(input_name="sites.txt", fingerprint="abc", rows=rows)
    first = store.claim_next_site()
    second = store.claim_next_site()
    assert first is not None and second is not None
    store.mark_done(first.id, SiteResult(company_name="A", representative="Alice Smith", emails="a@a.com", website="https://a.com"))
    store.mark_dropped(second.id, "http_401")

    reset = store.reset_completed_job_for_rerun()
    progress = store.progress()

    assert reset is True
    assert progress["pending"] == 2
    assert progress["done"] == 0
    assert progress["dropped"] == 0


def test_runtime_store_reuses_connection_in_same_thread(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")

    left = store._connect()
    right = store._connect()

    assert left is right
    store.close()


def test_runtime_store_updates_and_loads_stage_metrics(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    rows = [
        ImportedWebsite(input_index=1, raw_website="a.com", website="https://a.com", dedupe_key="a.com"),
    ]
    store.prepare_job(input_name="sites.txt", fingerprint="abc", rows=rows)
    task = store.claim_next_site()

    assert task is not None
    store.update_stage_metrics(
        task.id,
        SiteStageMetrics(
            discover_ms=1200,
            llm_pick_ms=300,
            fetch_pages_ms=4500,
            llm_extract_ms=900,
            email_rule_ms=100,
            company_rule_ms=50,
            discovered_url_count=18,
            rep_url_count=5,
            email_url_count=7,
            target_url_count=9,
            fetched_page_count=6,
        ),
    )

    loaded = store.load_stage_metrics(task.id)

    assert loaded.discover_ms == 1200
    assert loaded.llm_pick_ms == 300
    assert loaded.fetch_pages_ms == 4500
    assert loaded.discovered_url_count == 18
    assert loaded.fetched_page_count == 6


def test_raise_nofile_soft_limit_raises_soft_limit(monkeypatch) -> None:
    calls: list[tuple[int, tuple[int, int]]] = []

    def fake_getrlimit(_kind: int):
        return (256, 1_000_000)

    def fake_setrlimit(kind: int, value: tuple[int, int]) -> None:
        calls.append((kind, value))

    monkeypatch.setattr("oldironcrawler.app.resource.getrlimit", fake_getrlimit)
    monkeypatch.setattr("oldironcrawler.app.resource.setrlimit", fake_setrlimit)

    _raise_nofile_soft_limit(65536)

    assert calls == [(resource.RLIMIT_NOFILE, (65536, 1_000_000))]


def test_page_fetch_pool_preserves_input_order() -> None:
    pool = PageFetchPool(PageFetchPoolConfig(worker_count=4, per_host_limit=2))

    def fetch_one(url: str):
        if url.endswith("/2"):
            time.sleep(0.02)
        return url

    pages = pool.fetch_pages(
        urls=["https://a.com/1", "https://a.com/2", "https://a.com/3"],
        fetch_one=fetch_one,
        deadline_monotonic=time.monotonic() + 1.0,
    )

    assert pages == ["https://a.com/1", "https://a.com/2", "https://a.com/3"]
    pool.close()


def test_page_fetch_pool_respects_per_host_limit() -> None:
    pool = PageFetchPool(PageFetchPoolConfig(worker_count=4, per_host_limit=1))
    active = 0
    max_active = 0
    lock = __import__("threading").Lock()

    def fetch_one(url: str):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.02)
        with lock:
            active -= 1
        return url

    pool.fetch_pages(
        urls=["https://a.com/1", "https://a.com/2", "https://a.com/3"],
        fetch_one=fetch_one,
        deadline_monotonic=time.monotonic() + 1.0,
    )

    assert max_active == 1
    pool.close()


def test_page_fetch_pool_limits_submissions_under_short_deadline(monkeypatch) -> None:
    pool = PageFetchPool(PageFetchPoolConfig(worker_count=16, per_host_limit=2))
    submit_count = 0
    original_submit = pool._executor.submit

    def counted_submit(*args, **kwargs):
        nonlocal submit_count
        submit_count += 1
        return original_submit(*args, **kwargs)

    monkeypatch.setattr(pool._executor, "submit", counted_submit)

    def fetch_one(url: str):
        time.sleep(0.2)
        return url

    try:
        pool.fetch_pages(
            urls=[f"https://a.com/{index}" for index in range(20)],
            fetch_one=fetch_one,
            deadline_monotonic=time.monotonic() + 0.05,
        )
        raise AssertionError("expected TimeoutError")
    except TimeoutError as exc:
        assert "page_batch_timeout" in str(exc)
    assert submit_count <= 2
    pool.close()


def test_page_fetch_pool_inflight_limit_scales_with_unique_hosts() -> None:
    pool = PageFetchPool(PageFetchPoolConfig(worker_count=16, per_host_limit=2))

    single_host_limit = pool._inflight_limit([f"https://a.com/{index}" for index in range(20)])
    mixed_host_limit = pool._inflight_limit(
        [f"https://{host}.com/{index}" for host in ("a", "b", "c", "d") for index in range(3)]
    )

    assert single_host_limit == 2
    assert mixed_host_limit == 8
    pool.close()


def test_protocol_client_falls_back_to_http_on_https_tls_errors() -> None:
    class FakeResponse:
        def __init__(self, status_code: int, text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self._text = text
            self.headers = {"Content-Type": content_type}
            self.content = text.encode("utf-8")

        @property
        def text(self) -> str:
            return self._text

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, url: str, timeout: float):
            if url == "https://example.com":
                raise RuntimeError("curl: (60) SSL certificate problem: certificate has expired")
            if url == "http://example.com":
                return FakeResponse(200, "<html>fallback ok</html>")
            raise AssertionError(url)

    client = SiteProtocolClient(SiteProtocolConfig())
    client._try_httpx_fallback = lambda _url, _lowered_error: None
    client._try_insecure_https_fallback = lambda _url, _lowered_error: None
    client._try_www_fallback = lambda _session, _url, _lowered_error: None

    html = client._fetch_html(FakeSession(), "https://example.com", required=True)

    assert "fallback ok" in html


def test_protocol_client_falls_back_to_insecure_https_on_expired_cert(monkeypatch) -> None:
    class FakeResponse:
        def __init__(self, status_code: int, text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self.text = text
            self.headers = {"Content-Type": content_type}

        def close(self) -> None:
            return None

    class FakeHttpxClient:
        def __init__(self, **kwargs) -> None:
            assert kwargs["verify"] is False

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, url: str, timeout: float):
            assert url == "https://example.com"
            assert timeout == 10.0
            return FakeResponse(200, "<html>insecure ok</html>")

    class FakeSession:
        def get(self, url: str, timeout: float):
            raise RuntimeError("curl: (60) SSL certificate problem: certificate has expired")

    client = SiteProtocolClient(SiteProtocolConfig())
    monkeypatch.setattr(protocol_module.httpx, "Client", FakeHttpxClient)

    html = client._fetch_html(FakeSession(), "https://example.com", required=True)

    assert "insecure ok" in html
    client.close()


def test_protocol_client_falls_back_to_www_on_connection_closed() -> None:
    class FakeResponse:
        def __init__(self, status_code: int, text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self._text = text
            self.headers = {"Content-Type": content_type}
            self.content = text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, url: str, timeout: float):
            if url == "https://example.com":
                raise RuntimeError("Failed to perform, curl: (56) Connection closed abruptly")
            if url == "https://www.example.com":
                return FakeResponse(200, "<html>www ok</html>")
            raise AssertionError(url)

    client = SiteProtocolClient(SiteProtocolConfig())

    html = client._fetch_html(FakeSession(), "https://example.com", required=True)

    assert "www ok" in html


def test_protocol_client_falls_back_to_httpx_on_dns_thread_error() -> None:
    class FakeHttpxResponse:
        def __init__(self, status_code: int, text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self.text = text
            self.headers = {"Content-Type": content_type}

    class FakeHttpxClient:
        def get(self, url: str, timeout: float):
            assert url == "https://example.com"
            assert timeout == 10.0
            return FakeHttpxResponse(200, "<html>httpx fallback ok</html>")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, url: str, timeout: float):
            raise RuntimeError("curl: (6) getaddrinfo() thread failed to start")

    client = SiteProtocolClient(SiteProtocolConfig())
    client._http_client = FakeHttpxClient()

    html = client._fetch_html(FakeSession(), "https://example.com", required=True)

    assert "httpx fallback ok" in html


def test_protocol_client_marks_cloudflare_challenge_as_permanent(monkeypatch) -> None:
    class FakeResponse:
        def __init__(self) -> None:
            self.status_code = 403
            self.headers = {"Content-Type": "text/html; charset=UTF-8"}
            self.content = b'<!DOCTYPE html><html><head><title>Just a moment...</title></head><body>Cloudflare challenge-platform</body></html>'

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, url: str, timeout: float):
            return FakeResponse()

    client = SiteProtocolClient(SiteProtocolConfig())
    monkeypatch.setattr(protocol_module, "resolve_cloudflare_challenge", lambda **_kwargs: "<html>Just a moment... cloudflare</html>")

    try:
        client._fetch_html(FakeSession(), "https://example.com", required=True)
    except ProtocolPermanentError as exc:
        assert "cloudflare_challenge" in str(exc)
    else:
        raise AssertionError("expected ProtocolPermanentError")


def test_detect_challenge_kind_ignores_normal_cloudflare_cdn_reference() -> None:
    html = '<html><head><script src="https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.5/gsap.min.js"></script></head></html>'
    assert protocol_module._detect_challenge_kind(html) == ""


def test_protocol_client_discovers_related_company_subdomain_pages(monkeypatch) -> None:
    class FakeSession:
        pass

    client = SiteProtocolClient(SiteProtocolConfig())
    fake_session = FakeSession()

    monkeypatch.setattr(client, "_get_or_create_session", lambda: fake_session)

    def fake_fetch_html(_session, url: str, *, required: bool, timeout_seconds=None) -> str:
        pages = {
            "https://atomlearning.com": '<a href="https://www.atomlearning.com/about">About</a>',
            "https://www.atomlearning.com/about": '<a href="https://careers.atomlearning.com/">Careers</a>',
            "https://careers.atomlearning.com/": '<a href="/pages/about-us">Our story</a><a href="/people">People</a>',
        }
        return pages.get(url, "")

    def fake_discover_sitemap_urls(_session, base_url: str, *, limit: int) -> list[str]:
        if base_url == "https://atomlearning.com":
            return ["https://www.atomlearning.com/about"]
        if base_url == "https://careers.atomlearning.com/":
            return [
                "https://careers.atomlearning.com/pages/about-us",
                "https://careers.atomlearning.com/people",
            ]
        return []

    monkeypatch.setattr(client, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr(client, "_discover_sitemap_urls", fake_discover_sitemap_urls)

    urls = client.discover_urls("https://atomlearning.com", limit=20)

    assert "https://www.atomlearning.com/about" in urls
    assert "https://careers.atomlearning.com/" in urls
    assert "https://careers.atomlearning.com/pages/about-us" in urls
    assert "https://careers.atomlearning.com/people" in urls


def test_protocol_client_skips_broken_probe_pages_during_related_discovery(monkeypatch) -> None:
    class FakeSession:
        pass

    client = SiteProtocolClient(SiteProtocolConfig())
    fake_session = FakeSession()
    monkeypatch.setattr(client, "_get_or_create_session", lambda: fake_session)

    def fake_fetch_html(_session, url: str, *, required: bool, timeout_seconds=None) -> str:
        if url == "https://example.com":
            return '<a href="https://www.example.com/about">About</a>'
        if url == "https://www.example.com/about":
            raise ProtocolPermanentError("certificate has expired")
        return ""

    def fake_discover_sitemap_urls(_session, base_url: str, *, limit: int) -> list[str]:
        if base_url == "https://example.com":
            return ["https://www.example.com/about"]
        return []

    monkeypatch.setattr(client, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr(client, "_discover_sitemap_urls", fake_discover_sitemap_urls)

    urls = client.discover_urls("https://example.com", limit=20)

    assert urls == ["https://www.example.com/about"]


def test_prioritize_representative_content_keeps_signal_without_dropping_body() -> None:
    content = """
    This website uses cookies to ensure you get the best experience.
    Accept all cookies
    Decline all non-necessary cookies
    Who we are
    Senior Leadership Team
    * ### Alex Hatvany
    Co-Founder
    * ### Jake O'Keeffe
    Co-Founder
    * ### Flo Simpson
    Chief Operating Officer
    Career site by Teamtailor
    Atom Learning was founded in 2018 and keeps growing.
    """

    prioritized = llm_module._prioritize_representative_content(content)

    assert "Alex Hatvany" in prioritized
    assert "Chief Operating Officer" in prioritized
    assert "Atom Learning was founded in 2018 and keeps growing." in prioritized
    assert "Accept all cookies" not in prioritized
    assert "Career site by Teamtailor" not in prioritized


def test_prepare_representative_pages_keeps_top_page_full_when_budget_tight() -> None:
    top_page = {
        "url": "https://example.com/about",
        "content": "\n".join(
            [
                "Who we are",
                "Senior Leadership Team",
                "Alex Hatvany",
                "Co-Founder",
                "Paragraph A",
                "Paragraph B",
                "Paragraph C",
            ]
        ),
    }
    lower_page = {
        "url": "https://example.com/legal",
        "content": "\n".join(f"Legal line {index}" for index in range(1, 80)),
    }

    prepared = llm_module._fit_representative_pages_to_budget(
        [top_page, lower_page],
        budget=len(top_page["content"]) + 120,
    )

    assert prepared[0]["content"] == top_page["content"]
    assert len(prepared[1]["content"]) < len(lower_page["content"])


def test_collect_emails_for_pages_falls_back_to_same_domain_embedded_emails() -> None:
    html_text = """
    <html>
      <head><script>
        window.__WIX_DATA__ = {"html":"<p><a href=\\"mailto:grant@audiowaveltd.com\\">grant@audiowaveltd.com</a></p><p><a href=\\"mailto:other@partner.com\\">other@partner.com</a></p>"}
      </script></head>
      <body><div>Contact page without visible email text</div></body>
    </html>
    """

    emails, sources = collect_emails_for_pages("https://audiowaveltd.com", [("https://www.audiowaveltd.com/contact", html_text)])

    assert emails == ["grant@audiowaveltd.com"]
    assert sources == {"https://www.audiowaveltd.com/contact": ["grant@audiowaveltd.com"]}


def test_select_email_urls_includes_drive_for_us_pages() -> None:
    candidates = build_candidates(
        "https://airportexecutive.com",
        ["https://www.airportexecutive.com/drive-for-us"],
        {},
        {},
    )

    urls = select_email_urls(candidates)

    assert "https://www.airportexecutive.com/drive-for-us" in urls


def test_llm_call_with_retry_retries_server_disconnected(monkeypatch) -> None:
    client = WebsiteLlmClient(
        api_key="test-key",
        base_url="https://example.com/v1",
        model="test-model",
        api_style="responses",
        reasoning_effort="",
        proxy_url="",
        timeout_seconds=1,
        concurrency_limit=1,
    )
    attempts = {"count": 0}

    def fake_responses(_kwargs, **_extra):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("RemoteProtocolError: Server disconnected")
        return '{"ok": true}'

    monkeypatch.setattr(client, "_call_responses_streaming_api", fake_responses)
    monkeypatch.setattr(llm_module, "_sleep_with_jitter", lambda *_args, **_kwargs: None)

    result = client._call_with_retry({"model": "test-model", "input": []}, max_retries=3)

    assert result == '{"ok": true}'
    assert attempts["count"] == 2
    client.close()


def test_protocol_client_marks_failed_to_connect_as_permanent() -> None:
    class FakeSession:
        def get(self, url: str, timeout: float):
            raise RuntimeError("Failed to perform, curl: (7) Failed to connect to example.com port 443 after 0 ms: Couldn't connect to server")

    client = SiteProtocolClient(SiteProtocolConfig())

    try:
        client._fetch_html(FakeSession(), "https://example.com", required=True)
    except ProtocolPermanentError as exc:
        assert "couldn't connect to server" in str(exc).lower()
    else:
        raise AssertionError("expected ProtocolPermanentError")


def test_site_profile_service_skips_llm_when_rep_pages_are_empty(tmp_path: Path, monkeypatch) -> None:
    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=["https://example.com/about"], homepage_html="")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            return []

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, **_kwargs):
            raise AssertionError("LLM should not be called when rep_pages is empty")

    monkeypatch.setattr("oldironcrawler.extractor.service.SiteProtocolClient", FakeProtocolClient)

    config = SimpleNamespace(
        request_timeout_seconds=10.0,
        proxy_url="",
        capsolver_api_key="",
        capsolver_api_base_url="https://api.capsolver.com",
        capsolver_proxy="",
        capsolver_poll_seconds=3.0,
        capsolver_max_wait_seconds=40.0,
        cloudflare_proxy_url="",
        page_concurrency=8,
    )
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    learning_store = GlobalLearningStore(tmp_path / "global_learning.sqlite3")
    store.prepare_job(
        input_name="sites.txt",
        fingerprint="abc",
        rows=[ImportedWebsite(input_index=1, raw_website="example.com", website="https://example.com", dedupe_key="example.com")],
    )
    task = store.claim_next_site()

    assert task is not None
    service = SiteProfileService(config, store, learning_store, FakeLlmClient(), page_pool=None)
    result = service.process(task.id, task.website)

    assert result.result.company_name == ""
    assert result.result.representative == ""
    learning_store.close()
    store.close()


def test_describe_error_reason_for_cloudflare_challenge() -> None:
    message = _describe_error_reason("cloudflare_challenge: https://example.com")

    assert "Cloudflare" in message


def test_protocol_client_reuses_thread_session() -> None:
    created: list[str] = []

    class ReuseClient(SiteProtocolClient):
        def _build_session(self):
            created.append("session")

            class FakeSession:
                def close(self) -> None:
                    return None

            return FakeSession()

    client = ReuseClient(SiteProtocolConfig())

    left = client._get_or_create_session()
    right = client._get_or_create_session()

    assert left is right
    assert len(created) == 1


def test_fetch_pages_batch_timeout_does_not_wait_forever() -> None:
    class SlowClient(SiteProtocolClient):
        def _fetch_page_optional(self, url: str, *, timeout_seconds: float | None = None):
            if url.endswith("/fast"):
                return type("Page", (), {"url": url, "html": "<html>ok</html>"})()
            time.sleep(min(timeout_seconds or 0.2, 0.06))
            return None

    client = SlowClient(SiteProtocolConfig(page_batch_timeout_seconds=0.05))
    started = time.time()
    pages = client.fetch_pages(
        ["https://example.com/fast", "https://example.com/slow"],
        max_workers=2,
    )
    elapsed = time.time() - started

    assert elapsed < 0.15
    assert [page.url for page in pages] == ["https://example.com/fast"]


def test_protocol_client_default_headers_allow_keepalive() -> None:
    config = SiteProtocolConfig()

    assert "Connection" not in config.default_headers


def test_merge_page_targets_dedupes_cross_pipeline_urls() -> None:
    merged = _merge_page_targets(
        ["https://example.com/about", "https://example.com/team"],
        ["https://example.com/team", "https://example.com/contact"],
    )

    assert merged == [
        "https://example.com/about",
        "https://example.com/team",
        "https://example.com/contact",
    ]


def test_llm_semaphore_updates_to_new_limit() -> None:
    llm_module._LLM_SEMAPHORE = None
    llm_module._LLM_SEMAPHORE_LIMIT = 0

    client = WebsiteLlmClient.__new__(WebsiteLlmClient)
    client._set_global_concurrency_limit(16)

    assert llm_module._LLM_SEMAPHORE is not None
    assert llm_module._LLM_SEMAPHORE_LIMIT == 16


def test_missing_reason_describes_empty_fields() -> None:
    reason = _describe_missing_reason(
        SiteResult(company_name="", representative="", emails="", website="https://example.com")
    )

    assert reason == "官网页面里未识别到明确公司名；官网页面里未识别到负责人姓名；价值页里未命中有效邮箱"


def test_normalize_representative_name_allows_single_name_but_rejects_role_word() -> None:
    assert llm_module._normalize_representative_name("Anton") == "Anton"
    assert llm_module._normalize_representative_name("Director") == ""


def test_error_reason_maps_http_and_dns_thread_errors() -> None:
    assert _describe_error_reason("http_401: https://example.com") == "站点返回 HTTP 401，页面拒绝访问"
    assert _describe_error_reason(
        "Failed to perform, curl: (6) getaddrinfo() thread failed to start."
    ) == "本地高并发 DNS 解析资源不足，当前请求未成功"
    assert _describe_error_reason("[Errno 35] Resource temporarily unavailable") == "本地高并发网络资源暂时不足，当前请求未成功"
    assert _describe_error_reason("curl: (7) Failed to connect to host: Couldn't connect to server") == "站点当前明显无法连通，已直接停止重试"
    assert _describe_error_reason("imperva_challenge: https://example.com") == "站点被 Imperva/Incapsula 风控挑战页拦截，协议抓取当前拿不到真实正文"
    assert _describe_error_reason("site_deadline_exceeded") == "单站已达到 180 秒时间上限，当前直接停止，不再重试"


def test_temporary_error_detection_includes_resource_temporarily_unavailable() -> None:
    assert _looks_temporary_error(RuntimeError("[Errno 35] Resource temporarily unavailable")) is True


def test_email_rules_keep_real_offsite_and_personal_mail() -> None:
    emails, page_hits = collect_emails_for_pages(
        "https://alpha.co.jp",
        [
            (
                "https://alpha.co.jp/contact",
                """
                <html>
                  info@alpha.co.jp
                  owner@template.com
                  contact@vendor-support.com
                  ceo.personal@gmail.com
                  donna.boynton@partner.org
                </html>
                """,
            )
        ],
    )

    assert emails == [
        "info@alpha.co.jp",
        "contact@vendor-support.com",
        "ceo.personal@gmail.com",
        "donna.boynton@partner.org",
    ]
    assert "https://alpha.co.jp/contact" in page_hits


def test_email_rules_drop_broken_offsite_variant_when_same_local_exists_on_site_domain() -> None:
    emails, _page_hits = collect_emails_for_pages(
        "https://ganco.co.uk",
        [
            (
                "https://ganco.co.uk/contact",
                """
                <html>
                  info@ganco.co.uk
                  info@ganco.co.ukk
                  sam.bosworth@tqr.co.uk
                  sam.bosworth@tqr.co.ukdocument
                </html>
                """,
            )
        ],
    )

    assert emails == ["info@ganco.co.uk", "sam.bosworth@tqr.co.uk"]


def test_email_rules_drop_www_and_placeholder_vendor_noise() -> None:
    emails, _page_hits = collect_emails_for_pages(
        "https://airportexecutive.com",
        [
            (
                "https://airportexecutive.com",
                """
                <html>
                  bookings@airportexecutive.com
                  available@www.airportexecutive.com
                  abc@xyz.com
                  filler@godaddy.com
                </html>
                """,
            )
        ],
    )

    assert emails == ["bookings@airportexecutive.com"]


def test_email_rules_drop_site_typo_domain_and_obfuscated_noise() -> None:
    emails, _page_hits = collect_emails_for_pages(
        "https://fglp.co.uk",
        [
            (
                "https://fglp.co.uk",
                """
                <html>
                  info@fglp.co.uk
                  vasb@styc.pb.hx
                  y.srygba@styc.pb.hx
                </html>
                """,
            )
        ],
    )

    assert emails == ["info@fglp.co.uk"]


def test_email_rules_drop_address_like_local_part_noise() -> None:
    emails, _page_hits = collect_emails_for_pages(
        "https://blueoceanservicesuk.com",
        [
            (
                "https://blueoceanservicesuk.com",
                """
                <html>
                  info@blueoceanservicesuk.com
                  admin@blueoceanservicesuk.com
                  339+stanstead+road+catford+london+se6+4ue+admin@blueoceanservicesuk.com
                </html>
                """,
            )
        ],
    )

    assert emails == ["info@blueoceanservicesuk.com", "admin@blueoceanservicesuk.com"]


def test_value_rules_split_representative_and_email_targets() -> None:
    discovered = [
        "https://example.com/about",
        "https://example.com/team",
        "https://example.com/leadership",
        "https://example.com/contact",
        "https://example.com/privacy",
        "https://example.com/legal",
        "https://example.com/blog/post-1",
    ]

    candidates = build_candidates(
        "https://example.com",
        discovered,
        rep_learned={"leadership": 4},
        email_learned={"privacy": 3},
    )

    rep_urls, teacher_pool = select_representative_urls(candidates, target_count=5)
    email_urls = select_email_urls(candidates)

    assert "https://example.com/contact" in email_urls
    assert "https://example.com/privacy" in email_urls
    assert "https://example.com/leadership" in rep_urls
    assert "https://example.com/blog/post-1" in teacher_pool or "https://example.com/blog/post-1" not in rep_urls


def test_representative_selection_deprioritizes_article_like_pages() -> None:
    discovered = [
        "https://example.com/about",
        "https://example.com/executive-team",
        "https://example.com/whg-pledges-to-bring-diversity-and-inclusion-into-the-boardroom",
        "https://example.com/news/leadership-update",
    ]

    candidates = build_candidates(
        "https://example.com",
        discovered,
        rep_learned={},
        email_learned={},
    )

    rep_urls, teacher_pool = select_representative_urls(candidates, target_count=5)

    assert "https://example.com/executive-team" in rep_urls
    assert "https://example.com/whg-pledges-to-bring-diversity-and-inclusion-into-the-boardroom" not in rep_urls
    assert "https://example.com/news/leadership-update" not in rep_urls
    assert "https://example.com/whg-pledges-to-bring-diversity-and-inclusion-into-the-boardroom" not in teacher_pool


def test_representative_selection_keeps_contact_page_as_candidate() -> None:
    discovered = [
        "https://example.com/contact-us/",
        "https://example.com/our-services/management-accounts/",
    ]

    candidates = build_candidates(
        "https://example.com",
        discovered,
        rep_learned={},
        email_learned={},
    )

    rep_urls, _teacher_pool = select_representative_urls(candidates, target_count=5)

    assert "https://example.com/contact-us/" in rep_urls


def test_representative_selection_keeps_one_person_detail_page_per_team_family() -> None:
    discovered = [
        "https://example.com/team-members/",
        "https://example.com/team_members/raheel-khan/",
        "https://example.com/team_members/andrew-young/",
        "https://example.com/about/",
    ]

    candidates = build_candidates(
        "https://example.com",
        discovered,
        rep_learned={},
        email_learned={},
    )

    rep_urls, _teacher_pool = select_representative_urls(candidates, target_count=5)

    assert "https://example.com/team-members/" in rep_urls
    assert "https://example.com/team_members/raheel-khan/" in rep_urls or "https://example.com/team_members/andrew-young/" in rep_urls


def test_extract_path_tokens_strips_file_extension_suffix() -> None:
    assert extract_path_tokens("https://example.com/andy-maggs-referrals.html") == ["andy", "maggs", "referrals"]


def test_representative_selection_prefers_named_referral_page_over_service_page() -> None:
    discovered = [
        "https://example.com/executive-coaching.html",
        "https://example.com/andy-maggs-referrals.html",
        "https://example.com/about.html",
        "https://example.com/contact.html",
    ]

    candidates = build_candidates(
        "https://example.com",
        discovered,
        rep_learned={},
        email_learned={},
    )

    rep_urls, _teacher_pool = select_representative_urls(candidates, target_count=5)

    assert "https://example.com/andy-maggs-referrals.html" in rep_urls


def test_representative_selection_prefers_same_locale_imprint_page() -> None:
    discovered = [
        "https://www.example.com/de/impressum",
        "https://www.example.com/en/imprint",
        "https://www.example.com/de/ueber-uns/unser-team",
    ]

    candidates = build_candidates(
        "https://example.com/en",
        discovered,
        rep_learned={},
        email_learned={},
    )

    rep_urls, _teacher_pool = select_representative_urls(candidates, target_count=5)

    assert "https://www.example.com/en/imprint" in rep_urls


def test_representative_selection_prefers_executive_team_and_about_us_pages() -> None:
    discovered = [
        "https://example.com/about",
        "https://example.com/about-us",
        "https://example.com/executive-team",
        "https://example.com/company/news/leadership-update",
    ]

    candidates = build_candidates(
        "https://example.com",
        discovered,
        rep_learned={},
        email_learned={},
    )

    rep_urls, _teacher_pool = select_representative_urls(candidates, target_count=5)

    assert "https://example.com/executive-team" in rep_urls
    assert "https://example.com/about-us" in rep_urls


def test_selection_deprioritizes_forum_and_sponsored_pages() -> None:
    discovered = [
        "https://example.com/info/about",
        "https://example.com/info/contact-us",
        "https://example.com/discount-partners",
        "https://example.com/forums/sponsored_discussions/ask-the-expert",
        "https://example.com/member/email-options",
    ]

    candidates = build_candidates(
        "https://example.com",
        discovered,
        rep_learned={},
        email_learned={},
    )

    rep_urls, _teacher_pool = select_representative_urls(candidates, target_count=5)
    email_urls = select_email_urls(candidates)

    assert "https://example.com/info/about" in rep_urls
    assert "https://example.com/discount-partners" not in rep_urls
    assert "https://example.com/forums/sponsored_discussions/ask-the-expert" not in rep_urls
    assert "https://example.com/member/email-options" not in email_urls
    assert "https://example.com/info/contact-us" in email_urls


def test_protocol_discovery_probes_common_value_paths_when_homepage_is_blocked(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    def fake_fetch_html(_session, url: str, *, required: bool, timeout_seconds=None, max_retries_override=None) -> str:
        if url == "https://example.com":
            raise ProtocolPermanentError("cloudflare_challenge: https://example.com")
        if url == "https://example.com/contact-us":
            return "<html>contact</html>"
        return ""

    monkeypatch.setattr(client, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr(client, "_discover_sitemap_urls", lambda *_args, **_kwargs: [])

    urls, homepage_html = client._discover_direct_urls(object(), "https://example.com", limit=20)

    assert homepage_html == ""
    assert "https://example.com/contact-us" in urls
    client.close()


def test_protocol_discovery_keeps_challenged_common_value_paths(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    def fake_fetch_html(_session, url: str, *, required: bool, timeout_seconds=None, max_retries_override=None) -> str:
        if url == "https://example.com":
            return "<html>home</html>"
        if url == "https://example.com/executive-team":
            raise ProtocolPermanentError("cloudflare_challenge: https://example.com/executive-team")
        return ""

    monkeypatch.setattr(client, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr(client, "_discover_sitemap_urls", lambda *_args, **_kwargs: [])

    urls, _homepage_html = client._discover_direct_urls(object(), "https://example.com", limit=20)

    assert "https://example.com/executive-team" in urls
    client.close()


def test_protocol_common_probe_stops_after_target_hits(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig(common_probe_target=3, common_probe_concurrency=4))
    called: list[str] = []

    def fake_probe(url: str) -> str | None:
        called.append(url)
        return url

    monkeypatch.setattr(client, "_probe_common_value_url", fake_probe)

    urls = client._probe_common_value_urls(object(), "https://example.com", limit=20)

    assert len(urls) == 3
    assert len(called) == 4
    client.close()


def test_protocol_discovery_skips_sitemap_when_probe_hits_are_enough(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig(common_probe_target=2))

    monkeypatch.setattr(client, "_fetch_html", lambda *_args, **_kwargs: "<html>home</html>")
    monkeypatch.setattr(
        client,
        "_probe_common_value_urls",
        lambda *_args, **_kwargs: ["https://example.com/about", "https://example.com/contact"],
    )
    sitemap_called = {"value": False}

    def fake_sitemap(*_args, **_kwargs):
        sitemap_called["value"] = True
        return ["https://example.com/sitemap-team"]

    monkeypatch.setattr(client, "_discover_sitemap_urls", fake_sitemap)

    urls, homepage_html = client._discover_direct_urls(object(), "https://example.com", limit=20)

    assert homepage_html == "<html>home</html>"
    assert sitemap_called["value"] is False
    assert "https://example.com/about" in urls
    client.close()


def test_build_common_probe_urls_prefers_locale_prefixed_paths() -> None:
    urls = protocol_module._build_common_probe_urls("https://example.com/en")

    assert "https://example.com/en/imprint" in urls
    assert urls.index("https://example.com/en/imprint") < urls.index("https://example.com/imprint")
    assert "https://example.com/en/company-leadership" in urls
    assert "https://www.example.com/en/executive-team" in urls


def test_company_name_fallback_prefers_site_name_and_strips_welcome_prefix() -> None:
    html_text = """
    <html>
      <head>
        <meta property="og:site_name" content="Corse Lawn House Hotel" />
        <title>Cotswold Hotel, Restaurant, Bar &amp; Events Venue.</title>
      </head>
      <body>
        <h1>Welcome To Corse Lawn House</h1>
      </body>
    </html>
    """

    company = extract_company_name_fallback(
        "https://corselawn.com",
        [("https://corselawn.com", html_text)],
    )

    assert company == "Corse Lawn House Hotel"

def test_detect_challenge_kind_supports_imperva_pages() -> None:
    html_text = '<html><body><iframe src=\"/_Incapsula_Resource?abc=1\"></iframe>Request unsuccessful. Incapsula incident ID: 123</body></html>'

    assert protocol_module._detect_challenge_kind(html_text) == "imperva_challenge"


def test_supported_url_rejects_template_placeholders() -> None:
    assert protocol_module._is_supported_url("https://example.com/about-us/{{{data.link}}}") is False
    assert protocol_module._is_supported_url("https://example.com/about-us/itemDataObject.url") is False


def test_truncate_html_keeps_middle_representative_signal() -> None:
    head = "<html><body>" + ("A" * 120000)
    middle = "<h3>David Garcia</h3><p>Founder &amp; Lead Guide – Bespoke England Tours</p>"
    tail = ("B" * 120000) + "</body></html>"

    shortened = protocol_module._truncate_html(head + middle + tail, 250000)

    assert len(shortened) <= 250000
    assert "David Garcia" in shortened
    assert "Founder &amp; Lead Guide" in shortened


def test_truncate_html_keeps_middle_email_signal() -> None:
    head = "<html><body>" + ("C" * 120000)
    middle = "<div>Contact us at info@example.com for quotes.</div>"
    tail = ("D" * 120000) + "</body></html>"

    shortened = protocol_module._truncate_html(head + middle + tail, 250000)

    assert len(shortened) <= 250000
    assert "info@example.com" in shortened


def test_fetch_with_cloudscraper_returns_html_and_cookies(monkeypatch) -> None:
    class FakeCookieJar:
        def __init__(self) -> None:
            self._items = []

        def set(self, name, value, **kwargs) -> None:
            self._items.append(type("Cookie", (), {"name": name, "value": value, **kwargs})())

        def __iter__(self):
            return iter(self._items)

    class FakeResponse:
        status_code = 200
        text = "<html>ok</html>"

    class FakeScraper:
        def __init__(self) -> None:
            self.headers = {}
            self.proxies = {}
            self.cookies = FakeCookieJar()

        def get(self, url, timeout, allow_redirects):
            assert url == "https://example.com"
            assert timeout == 10
            assert allow_redirects is True
            self.cookies.set("cf_clearance", "token", domain="example.com", path="/", secure=True)
            return FakeResponse()

        def close(self) -> None:
            return None

    class FakeModule:
        @staticmethod
        def create_scraper(**_kwargs):
            return FakeScraper()

    monkeypatch.setattr(challenge_module, "_import_cloudscraper_module", lambda: FakeModule())

    result = challenge_module.fetch_with_cloudscraper(
        url="https://example.com",
        timeout_seconds=10,
        proxy_url="http://127.0.0.1:7897",
        headers={"User-Agent": "UA"},
        cookies=[],
    )

    assert result is not None
    assert result.html == "<html>ok</html>"
    assert any(cookie.name == "cf_clearance" for cookie in result.cookies)


def test_protocol_fetch_html_uses_cloudscraper_when_challenged(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "text/html"}
        content = b"<html>Just a moment... cloudflare</html>"

        def close(self) -> None:
            return None

    class FakeSession:
        def __init__(self) -> None:
            self.headers = {"User-Agent": "UA"}
            self.cookies = challenge_module._import_cloudscraper_module

        def get(self, url, timeout):
            assert url == "https://example.com"
            assert timeout == 10.0
            return FakeResponse()

    class CookieJar:
        def __init__(self) -> None:
            self.values = {}

        def set(self, name, value, **_kwargs) -> None:
            self.values[name] = value

        def __iter__(self):
            return iter([])

    session = FakeSession()
    session.cookies = CookieJar()
    monkeypatch.setattr(
        protocol_module,
        "resolve_cloudflare_challenge",
        lambda **_kwargs: "<html>real body</html>",
    )

    html = client._fetch_html(session, "https://example.com", required=True)

    assert html == "<html>real body</html>"


def test_normalize_capsolver_proxy_rejects_loopback() -> None:
    assert challenge_module.normalize_capsolver_proxy("http://127.0.0.1:7897") == ""
    assert challenge_module.normalize_capsolver_proxy("localhost:9000") == ""
    assert challenge_module.normalize_capsolver_proxy("http://user:pass@1.2.3.4:8080") == "1.2.3.4:8080:user:pass"


def test_solve_cloudflare_challenge_polls_until_ready(monkeypatch) -> None:
    class FakeResponse:
        def __init__(self, payload) -> None:
            self._payload = payload

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self) -> None:
            self.calls = []

        def post(self, url, json):
            self.calls.append((url, json))
            if url.endswith("/createTask"):
                return FakeResponse({"errorId": 0, "taskId": "task-1"})
            return FakeResponse(
                {
                    "errorId": 0,
                    "status": "ready",
                    "solution": {
                        "cookies": {"cf_clearance": "token-1"},
                        "userAgent": "UA-2",
                        "token": "token-1",
                    },
                }
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr(challenge_module, "_build_http_client", lambda _proxy: FakeClient())

    result = challenge_module.solve_cloudflare_challenge(
        api_key="capsolver-key",
        api_base_url="https://api.capsolver.com",
        api_proxy_url="http://127.0.0.1:7897",
        challenge_url="https://example.com/challenge",
        challenge_html="<html>Just a moment...</html>",
        user_agent="UA-1",
        proxy="1.2.3.4:8080:user:pass",
        poll_seconds=1.0,
        max_wait_seconds=5.0,
    )

    assert result is not None
    assert result.user_agent == "UA-2"
    assert any(cookie.name == "cf_clearance" for cookie in result.cookies)


def test_resolve_cloudflare_challenge_prefers_cloudflare_proxy(monkeypatch) -> None:
    seen = {}

    def fake_cloudscraper(**kwargs):
        seen["cloudscraper_proxy_url"] = kwargs["proxy_url"]
        return "<html>Just a moment... cloudflare</html>"

    def fake_capsolver(**kwargs):
        seen["challenge_proxy_url"] = kwargs["challenge_proxy_url"]
        seen["api_proxy_url"] = kwargs["api_proxy_url"]
        return ""

    monkeypatch.setattr(challenge_module, "_run_cloudscraper_fallback", fake_cloudscraper)
    monkeypatch.setattr(challenge_module, "_run_capsolver_fallback", fake_capsolver)

    result = challenge_module.resolve_cloudflare_challenge(
        url="https://example.com",
        html_text="<html>Just a moment... cloudflare</html>",
        timeout_seconds=20.0,
        proxy_url="http://127.0.0.1:7897",
        cloudflare_proxy_url="http://user:pass@154.37.218.32:31281",
        max_html_chars=250000,
        session_headers={"User-Agent": "UA-1"},
        cookie_jar=None,
        detect_challenge=lambda value: "cloudflare" in value.lower(),
        refetch_html=lambda: "",
        impersonate="chrome110",
        capsolver_api_key="capsolver-key",
        capsolver_api_base_url="https://api.capsolver.com",
        capsolver_proxy="",
        capsolver_poll_seconds=1.0,
        capsolver_max_wait_seconds=5.0,
    )

    assert "cloudflare" in result.lower()
    assert seen["cloudscraper_proxy_url"] == "http://user:pass@154.37.218.32:31281"
    assert seen["challenge_proxy_url"] == "http://user:pass@154.37.218.32:31281"
    assert seen["api_proxy_url"] == "http://127.0.0.1:7897"


def test_run_capsolver_fallback_uses_challenge_proxy_when_capsolver_proxy_missing(monkeypatch) -> None:
    class CookieJar:
        def __init__(self) -> None:
            self.values = {}

        def set(self, name, value, **_kwargs) -> None:
            self.values[name] = value

        def __iter__(self):
            return iter([])

    seen = {}

    def fake_solver(**kwargs):
        seen["proxy"] = kwargs["proxy"]
        seen["api_proxy_url"] = kwargs["api_proxy_url"]
        return challenge_module.CapSolverResult(
            cookies=[
                challenge_module.CookieRecord(
                    name="cf_clearance",
                    value="cf-token",
                    domain="example.com",
                    path="/",
                    secure=True,
                    expires=None,
                )
            ],
            user_agent="UA-2",
            token="cf-token",
        )

    monkeypatch.setattr(challenge_module, "solve_cloudflare_challenge", fake_solver)
    monkeypatch.setattr(challenge_module, "_refetch_with_temp_proxy", lambda **kwargs: "<html>real body</html>")
    monkeypatch.setattr(challenge_module.time, "sleep", lambda _seconds: None)

    body = challenge_module._run_capsolver_fallback(
        url="https://example.com",
        challenge_html="<html>Just a moment...</html>",
        cookie_jar=CookieJar(),
        session_headers={"User-Agent": "UA-1"},
        refetch_html=lambda: "",
        capsolver_api_key="capsolver-key",
        capsolver_api_base_url="https://api.capsolver.com",
        capsolver_proxy="",
        capsolver_poll_seconds=1.0,
        capsolver_max_wait_seconds=5.0,
        api_proxy_url="",
        challenge_proxy_url="http://oldironproxy:pass@154.37.218.32:31281",
        timeout_seconds=20.0,
        max_html_chars=250000,
        impersonate="chrome110",
    )

    assert body == "<html>real body</html>"
    assert seen["proxy"] == "154.37.218.32:31281:oldironproxy:pass"
    assert seen["api_proxy_url"] == "http://oldironproxy:pass@154.37.218.32:31281"


def test_protocol_fetch_html_uses_capsolver_when_cloudscraper_still_challenged(monkeypatch) -> None:
    client = SiteProtocolClient(
        SiteProtocolConfig(
            capsolver_api_key="capsolver-key",
            capsolver_proxy="1.2.3.4:8080:user:pass",
            cloudflare_proxy_url="http://oldironproxy:pass@154.37.218.32:31281",
        )
    )

    class FakeResponse:
        def __init__(self, status_code: int, html_text: str) -> None:
            self.status_code = status_code
            self.headers = {"Content-Type": "text/html"}
            self.content = html_text.encode("utf-8")

        def close(self) -> None:
            return None

    class CookieJar:
        def __init__(self) -> None:
            self.values = {}

        def set(self, name, value, **_kwargs) -> None:
            self.values[name] = value

        def __iter__(self):
            return iter([])

    class FakeSession:
        def __init__(self) -> None:
            self.headers = {"User-Agent": "UA-1"}
            self.cookies = CookieJar()
            self.calls = 0

        def get(self, url, timeout):
            self.calls += 1
            assert url == "https://example.com"
            assert timeout == 10.0
            if self.calls == 1:
                return FakeResponse(403, "<html>Just a moment... cloudflare</html>")
            assert self.cookies.values["cf_clearance"] == "token-1"
            return FakeResponse(200, "<html>real body</html>")

    monkeypatch.setattr(
        challenge_module,
        "_run_cloudscraper_fallback",
        lambda **_kwargs: "<html>Just a moment... cloudflare</html>",
    )

    def fake_capsolver(**kwargs):
        assert kwargs["challenge_proxy_url"] == "http://oldironproxy:pass@154.37.218.32:31281"
        assert kwargs["impersonate"] == "chrome110"
        kwargs["session_headers"]["User-Agent"] = "UA-2"
        kwargs["cookie_jar"].set("cf_clearance", "token-1")
        return kwargs["refetch_html"]()

    monkeypatch.setattr(challenge_module, "_run_capsolver_fallback", fake_capsolver)
    monkeypatch.setattr(challenge_module.time, "sleep", lambda _seconds: None)

    session = FakeSession()
    html = client._fetch_html(session, "https://example.com", required=True)

    assert html == "<html>real body</html>"
    assert session.headers["User-Agent"] == "UA-2"
    client.close()


def test_maybe_build_umbraco_people_page_extracts_alias_and_builds_people_html(monkeypatch) -> None:
    def fake_fetch(*, project_alias: str, website: str, proxy_url: str, timeout_seconds: float):
        assert project_alias == "canaccord"
        assert website == "https://www.example.com"
        return [
            UmbracoBio(
                name="David Esfandi",
                url="https://www.example.com/people/david-esfandi/",
                job_title="Chief Executive Officer",
                email_address="david.esfandi@example.com",
                departments=["Leadership"],
                location="London",
            )
        ]

    monkeypatch.setattr("oldironcrawler.extractor.umbraco_people._fetch_umbraco_bios", lambda **kwargs: fake_fetch(**kwargs))

    page = maybe_build_umbraco_people_page(
        website="https://www.example.com",
        pages=[
            HtmlPage(
                url="https://www.example.com/about-us/our-people",
                html='<html><img src="https://media.umbraco.io/canaccord/sample/image.webp" /></html>',
            )
        ],
        proxy_url="",
        timeout_seconds=10.0,
    )

    assert page is not None
    assert page.url == "https://www.example.com/about-us/our-people"
    assert "David Esfandi" in page.html
    assert "Chief Executive Officer" in page.html


def test_maybe_build_umbraco_people_page_returns_none_without_people_page() -> None:
    page = maybe_build_umbraco_people_page(
        website="https://www.example.com",
        pages=[HtmlPage(url="https://www.example.com/about-us", html="<html></html>")],
        proxy_url="",
        timeout_seconds=10.0,
    )

    assert page is None


def test_request_slot_times_out_when_global_slots_are_exhausted(monkeypatch) -> None:
    semaphore = threading.BoundedSemaphore(1)
    assert semaphore.acquire(timeout=0.01) is True
    monkeypatch.setattr(protocol_module, "_REQUEST_SLOT_SEMAPHORE", semaphore)

    try:
        with protocol_module._request_slot(timeout_seconds=0.01):
            raise AssertionError("should not enter request slot")
    except ProtocolTemporaryError as exc:
        assert "request_slot_timeout" in str(exc)
