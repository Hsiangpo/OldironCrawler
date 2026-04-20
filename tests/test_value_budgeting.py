from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.config import AppConfig
from oldironcrawler.extractor.llm_client import LlmExtractionResult
from oldironcrawler.extractor.protocol_client import HtmlPage
from oldironcrawler.extractor.service import DiscoverySnapshot, SiteProfileService
from oldironcrawler.extractor import service as service_module
from oldironcrawler.extractor import value_rules as value_rules_module
from oldironcrawler.importer import ImportedWebsite
from oldironcrawler.runtime.global_learning import GlobalLearningStore
from oldironcrawler.runtime.store import RuntimeStore


def test_app_config_loads_value_budget_defaults(tmp_path: Path, monkeypatch) -> None:
    for name in (
        "REP_PAGE_LIMIT",
        "EMAIL_PAGE_SOFT_LIMIT",
        "EMAIL_PAGE_HARD_LIMIT",
        "PAGE_TOTAL_HARD_LIMIT",
        "EMAIL_STOP_SAME_DOMAIN_COUNT",
    ):
        monkeypatch.delenv(name, raising=False)

    config = AppConfig.load(tmp_path)

    assert config.rep_page_limit == 5
    assert config.email_page_soft_limit == 8
    assert config.email_page_hard_limit == 16
    assert config.page_total_hard_limit == 20
    assert config.email_stop_same_domain_count == 2


def test_app_config_supports_value_budget_dotenv_override(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "REP_PAGE_LIMIT=6",
                "EMAIL_PAGE_SOFT_LIMIT=9",
                "EMAIL_PAGE_HARD_LIMIT=18",
                "PAGE_TOTAL_HARD_LIMIT=22",
                "EMAIL_STOP_SAME_DOMAIN_COUNT=3",
            ]
        ),
        encoding="utf-8",
    )

    config = AppConfig.load(tmp_path)

    assert config.rep_page_limit == 6
    assert config.email_page_soft_limit == 9
    assert config.email_page_hard_limit == 18
    assert config.page_total_hard_limit == 22
    assert config.email_stop_same_domain_count == 3


def test_build_fetch_plan_preserves_rep_pages_and_total_budget() -> None:
    plan = value_rules_module.build_fetch_plan(
        "https://example.com",
        [
            "https://example.com",
            "https://example.com/about",
            "https://example.com/team",
            "https://example.com/leadership",
            "https://example.com/founder",
            "https://example.com/board",
        ],
        [
            "https://example.com/contact",
            "https://example.com/privacy",
            "https://example.com/support",
        ],
        rep_limit=5,
        email_soft_limit=8,
        email_hard_limit=16,
        total_hard_limit=6,
    )

    assert plan["rep_urls"] == [
        "https://example.com",
        "https://example.com/about",
        "https://example.com/team",
        "https://example.com/leadership",
        "https://example.com/founder",
    ]
    assert plan["all_primary_urls"] == [
        "https://example.com",
        "https://example.com/about",
        "https://example.com/team",
        "https://example.com/leadership",
        "https://example.com/founder",
        "https://example.com/contact",
    ]
    assert len(set(plan["rep_urls"] + plan["email_primary_urls"] + plan["email_overflow_urls"])) == 6


def test_build_fetch_plan_splits_email_budget_into_primary_and_overflow() -> None:
    plan = value_rules_module.build_fetch_plan(
        "https://example.com",
        [
            "https://example.com",
            "https://example.com/about",
            "https://example.com/team",
        ],
        [
            "https://example.com/contact",
            "https://example.com/privacy",
            "https://example.com/legal",
            "https://example.com/support",
            "https://example.com/help",
            "https://example.com/careers",
        ],
        rep_limit=5,
        email_soft_limit=2,
        email_hard_limit=5,
        total_hard_limit=8,
    )

    assert plan["email_primary_urls"] == [
        "https://example.com/contact",
        "https://example.com/privacy",
    ]
    assert plan["email_overflow_urls"] == [
        "https://example.com/legal",
        "https://example.com/support",
        "https://example.com/help",
    ]


def test_build_fetch_plan_keeps_selected_homepage_in_primary_phase() -> None:
    plan = value_rules_module.build_fetch_plan(
        "https://example.com",
        [
            "https://example.com/about",
            "https://example.com/team",
        ],
        [
            "https://example.com/contact",
            "https://example.com",
            "https://example.com/privacy",
        ],
        rep_limit=2,
        email_soft_limit=2,
        email_hard_limit=3,
        total_hard_limit=4,
    )

    assert "https://example.com" in plan["all_primary_urls"]
    assert "https://example.com" not in plan["email_overflow_urls"]


def test_build_fetch_plan_can_include_homepage_even_when_homepage_not_in_rep_or_email_candidates() -> None:
    plan = value_rules_module.build_fetch_plan(
        "https://example.com",
        [
            "https://example.com/about",
            "https://example.com/team",
        ],
        [
            "https://example.com/contact",
            "https://example.com/privacy",
            "https://example.com/legal",
        ],
        rep_limit=2,
        email_soft_limit=1,
        email_hard_limit=2,
        total_hard_limit=4,
    )

    assert plan["all_primary_urls"] == [
        "https://example.com/about",
        "https://example.com/team",
        "https://example.com",
        "https://example.com/contact",
    ]
    assert len(plan["all_primary_urls"]) + len(plan["email_overflow_urls"]) <= 4


def test_build_fetch_plan_dedupes_rep_and_email_overlap() -> None:
    plan = value_rules_module.build_fetch_plan(
        "https://example.com",
        [
            "https://example.com",
            "https://example.com/about",
            "https://example.com/contact",
        ],
        [
            "https://example.com/contact",
            "https://example.com/privacy",
            "https://example.com",
            "https://example.com/legal",
        ],
        rep_limit=3,
        email_soft_limit=2,
        email_hard_limit=3,
        total_hard_limit=5,
    )

    assert plan["all_primary_urls"] == [
        "https://example.com",
        "https://example.com/about",
        "https://example.com/contact",
        "https://example.com/privacy",
        "https://example.com/legal",
    ]
    assert plan["email_primary_urls"].count("https://example.com/contact") == 0


def test_site_profile_service_fetches_email_overflow_after_primary_phase_when_needed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    about_url = f"{website}/about"
    team_url = f"{website}/team"
    contact_url = f"{website}/contact"
    privacy_url = f"{website}/privacy"
    fetch_calls: list[list[str]] = []
    llm_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=[about_url, team_url, contact_url, privacy_url], homepage_html="")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            fetch_calls.append(list(urls))
            html_map = {
                about_url: "<html><h1>About</h1><p>Alice Example</p></html>",
                team_url: "<html><h1>Team</h1><p>Alice Example</p></html>",
                contact_url: "<html>info@acmeholdings.co.uk</html>",
                privacy_url: "<html>privacy@acmeholdings.co.uk</html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            llm_calls.append([page["url"] for page in pages])
            return LlmExtractionResult(
                company_name="Example Co",
                representative="Alice Example",
                evidence_url=about_url,
                evidence_quote="Alice Example",
            )

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[about_url, team_url, contact_url, privacy_url],
            candidates=[],
            rep_urls=[about_url, team_url],
            teacher_pool=[],
            email_urls=[contact_url, privacy_url],
        ),
    )

    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(_build_service_config(), store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert fetch_calls == [
        [about_url, team_url, website, contact_url],
        [privacy_url],
    ]
    assert llm_calls == [[about_url, team_url]]
    assert result.result.representative == "Alice Example"
    assert result.result.emails == "info@acmeholdings.co.uk; privacy@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_site_profile_service_reuses_discovery_homepage_html_in_primary_phase(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    about_url = f"{website}/about"
    contact_url = f"{website}/contact"
    fetch_calls: list[list[str]] = []
    llm_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=[website, about_url, contact_url], homepage_html="<html><h1>Acme Holdings</h1><p>Alice Example alice@acmeholdings.co.uk</p></html>")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            fetch_calls.append(list(urls))
            html_map = {
                about_url: "<html><h1>About</h1><p>Alice Example</p></html>",
                contact_url: "<html>info@acmeholdings.co.uk</html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            llm_calls.append([page["url"] for page in pages])
            homepage_page = next(page for page in pages if page["url"] == website)
            assert "alice@acmeholdings.co.uk" in homepage_page["html"]
            return LlmExtractionResult(
                company_name="Acme Holdings",
                representative="Alice Example",
                evidence_url=website,
                evidence_quote="Alice Example",
            )

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[website, about_url, contact_url],
            candidates=[],
            rep_urls=[website, about_url],
            teacher_pool=[],
            email_urls=[contact_url],
            homepage_html="<html><h1>Acme Holdings</h1><p>Alice Example alice@acmeholdings.co.uk</p></html>",
        ),
    )

    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(_build_service_config(), store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert fetch_calls == [[about_url, contact_url]]
    assert llm_calls == [[website, about_url]]
    assert result.stage_metrics.fetched_page_count == 3
    assert result.result.company_name == "Acme Holdings"
    learning_store.close()
    store.close()


def test_site_profile_service_skips_email_overflow_when_representative_and_primary_email_are_enough(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    about_url = f"{website}/about"
    team_url = f"{website}/team"
    contact_url = f"{website}/contact"
    privacy_url = f"{website}/privacy"
    events: list[str] = []
    fetch_calls: list[list[str]] = []
    llm_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=[about_url, team_url, contact_url, privacy_url], homepage_html="")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            events.append("fetch_primary" if privacy_url not in urls else "fetch_overflow")
            fetch_calls.append(list(urls))
            html_map = {
                about_url: "<html><h1>About</h1><p>Alice Example</p></html>",
                team_url: "<html><h1>Team</h1><p>Alice Example</p></html>",
                contact_url: "<html>info@acmeholdings.co.uk support@acmeholdings.co.uk</html>",
                privacy_url: "<html>privacy@acmeholdings.co.uk</html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            events.append("extract_representative")
            llm_calls.append([page["url"] for page in pages])
            return LlmExtractionResult(
                company_name="Example Co",
                representative="Alice Example",
                evidence_url=about_url,
                evidence_quote="Alice Example",
            )

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[about_url, team_url, contact_url, privacy_url],
            candidates=[],
            rep_urls=[about_url, team_url],
            teacher_pool=[],
            email_urls=[contact_url, privacy_url],
        ),
    )

    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(_build_service_config(), store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert events == ["fetch_primary", "extract_representative"]
    assert fetch_calls == [[about_url, team_url, website, contact_url]]
    assert llm_calls == [[about_url, team_url]]
    assert result.result.emails == "info@acmeholdings.co.uk; support@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_site_profile_service_extracts_emails_from_representative_pages(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    about_url = f"{website}/about"
    contact_url = f"{website}/contact"

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=[website, about_url, contact_url], homepage_html="")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            html_map = {
                website: "<html><h1>Home</h1></html>",
                about_url: "<html><h1>About</h1><p>Alice Example founder@acmeholdings.co.uk</p></html>",
                contact_url: "<html><p>Contact us</p></html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            return LlmExtractionResult(
                company_name="Acme Holdings",
                representative="Alice Example",
                evidence_url=about_url,
                evidence_quote="Alice Example",
            )

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[website, about_url, contact_url],
            candidates=[],
            rep_urls=[about_url],
            teacher_pool=[],
            email_urls=[contact_url],
        ),
    )

    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(_build_service_config(), store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert result.result.emails == "founder@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_site_profile_service_fetches_email_overflow_when_primary_email_is_enough_but_representative_is_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    about_url = f"{website}/about"
    team_url = f"{website}/team"
    contact_url = f"{website}/contact"
    privacy_url = f"{website}/privacy"
    events: list[str] = []
    fetch_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=[about_url, team_url, contact_url, privacy_url], homepage_html="")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            events.append("fetch_primary" if privacy_url not in urls else "fetch_overflow")
            fetch_calls.append(list(urls))
            html_map = {
                about_url: "<html><h1>About</h1><p>About us</p></html>",
                team_url: "<html><h1>Team</h1><p>Our team</p></html>",
                contact_url: "<html>info@acmeholdings.co.uk support@acmeholdings.co.uk</html>",
                privacy_url: "<html>privacy@acmeholdings.co.uk</html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            events.append("extract_representative")
            return LlmExtractionResult(
                company_name="Example Co",
                representative="",
                evidence_url="",
                evidence_quote="",
            )

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[about_url, team_url, contact_url, privacy_url],
            candidates=[],
            rep_urls=[about_url, team_url],
            teacher_pool=[],
            email_urls=[contact_url, privacy_url],
        ),
    )

    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(_build_service_config(), store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert events == ["fetch_primary", "extract_representative", "fetch_overflow"]
    assert fetch_calls == [
        [about_url, team_url, website, contact_url],
        [privacy_url],
    ]
    assert result.result.emails == "info@acmeholdings.co.uk; support@acmeholdings.co.uk; privacy@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_site_profile_service_skips_email_overflow_when_representative_page_emails_are_enough(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    about_url = f"{website}/about"
    contact_url = f"{website}/contact"
    privacy_url = f"{website}/privacy"
    events: list[str] = []
    fetch_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=[website, about_url, contact_url, privacy_url], homepage_html="")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            events.append("fetch_primary" if privacy_url not in urls else "fetch_overflow")
            fetch_calls.append(list(urls))
            html_map = {
                website: "<html><h1>Home</h1></html>",
                about_url: "<html><h1>About</h1><p>founder@acmeholdings.co.uk support@acmeholdings.co.uk</p></html>",
                contact_url: "<html><p>Contact us</p></html>",
                privacy_url: "<html><p>privacy@acmeholdings.co.uk</p></html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            events.append("extract_representative")
            return LlmExtractionResult(
                company_name="Acme Holdings",
                representative="Alice Example",
                evidence_url=about_url,
                evidence_quote="Alice Example",
            )

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[website, about_url, contact_url, privacy_url],
            candidates=[],
            rep_urls=[about_url],
            teacher_pool=[],
            email_urls=[contact_url, privacy_url],
        ),
    )

    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(_build_service_config(), store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert events == ["fetch_primary", "extract_representative"]
    assert fetch_calls == [[about_url, website, contact_url]]
    assert result.result.emails == "support@acmeholdings.co.uk; founder@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def _build_service_config() -> SimpleNamespace:
    return SimpleNamespace(
        request_timeout_seconds=10.0,
        total_wait_seconds=180.0,
        proxy_url="",
        capsolver_api_key="",
        capsolver_api_base_url="https://api.capsolver.com",
        capsolver_proxy="",
        capsolver_poll_seconds=3.0,
        capsolver_max_wait_seconds=40.0,
        cloudflare_proxy_url="",
        page_concurrency=8,
        rep_page_limit=2,
        email_page_soft_limit=1,
        email_page_hard_limit=2,
        page_total_hard_limit=5,
        email_stop_same_domain_count=2,
    )


def _prepare_service_task(tmp_path: Path, *, website: str) -> tuple[RuntimeStore, GlobalLearningStore, object]:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    learning_store = GlobalLearningStore(tmp_path / "global_learning.sqlite3")
    store.prepare_job(
        input_name="sites.txt",
        fingerprint="budgeting",
        rows=[
            ImportedWebsite(
                input_index=1,
                raw_website=website,
                website=website,
                dedupe_key=website,
            )
        ],
    )
    task = store.claim_next_site()
    assert task is not None
    return store, learning_store, task
