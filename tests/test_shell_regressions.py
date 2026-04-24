from __future__ import annotations

import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.extractor.email_rules import collect_emails_for_pages
from oldironcrawler.extractor.phone_rules import collect_phones_for_pages
from oldironcrawler.extractor.protocol_client import HtmlPage
from oldironcrawler.extractor import shell_page as shell_module
from oldironcrawler.extractor.service import _build_shell_alias_map, _canonicalize_target_urls
from oldironcrawler.extractor.shell_page import build_shell_evidence_html


def test_shell_evidence_keeps_site_emails_but_drops_placeholder_and_regulator_noise() -> None:
    shell_html = """
    <html>
      <head>
        <script src="/assets/index-xPWHrNF8.js"></script>
      </head>
      <body>
        <div id="root"></div>
      </body>
    </html>
    """
    asset_texts = {
        "https://0xam.de/assets/index-xPWHrNF8.js": """
        "E-Mail: post@0xam.de"
        "mailto:m@0xam.de"
        "placeholder:\\"mail@beispiel.de\\""
        "E-Mail: poststelle@lda.bayern.de"
        "Geschaeftsfuehrer: Marcel Uhlmann"
        """,
    }

    enriched_html = build_shell_evidence_html("https://0xam.de/", shell_html, asset_texts)
    emails, _page_hits = collect_emails_for_pages("https://0xam.de/", [("https://0xam.de/", enriched_html)])

    assert len(emails) == 2
    assert set(emails) == {"post@0xam.de", "m@0xam.de"}


def test_shell_alias_map_collapses_same_shell_fallback_routes() -> None:
    shell_html = """
    <html>
      <head>
        <script src="/assets/app.js"></script>
      </head>
      <body>
        <div id="root"></div>
      </body>
    </html>
    """
    homepage = "https://0xam.de/"
    fake_people = "https://0xam.de/about-us/our-people"
    fake_leadership = "https://0xam.de/executive-team"
    page_map = {
        homepage: HtmlPage(url=homepage, html=shell_html),
        fake_people: HtmlPage(url=fake_people, html=shell_html),
        fake_leadership: HtmlPage(url=fake_leadership, html=shell_html),
    }

    alias_map = _build_shell_alias_map(
        start_url=homepage,
        page_map=page_map,
        target_urls=[homepage, fake_people, fake_leadership],
    )
    canonical_urls = _canonicalize_target_urls(
        [fake_people, fake_leadership, homepage],
        alias_map,
    )

    assert fake_people not in canonical_urls
    assert fake_leadership not in canonical_urls
    assert homepage in canonical_urls


def test_shell_asset_fetch_respects_deadline_and_stops_extra_requests(monkeypatch) -> None:
    calls: list[str] = []

    class FakeResponse:
        status_code = 200

        def __init__(self, url: str) -> None:
            self.url = url
            self.headers = {"Content-Type": "application/javascript"}
            self.text = 'console.log("ok")'

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, url: str, timeout=None):
            calls.append(url)
            time.sleep(0.03)
            return FakeResponse(url)

    monkeypatch.setattr(shell_module.httpx, "Client", lambda **_kwargs: FakeClient())

    result = shell_module.fetch_first_party_asset_texts(
        ["https://0xam.de/assets/a.js", "https://0xam.de/assets/b.js"],
        proxy_url="",
        timeout_seconds=1.0,
        deadline_monotonic=time.monotonic() + 0.01,
    )

    assert list(result.keys()) == ["https://0xam.de/assets/a.js"]
    assert calls == ["https://0xam.de/assets/a.js"]


def test_shell_page_detects_root_container_even_with_cookie_text() -> None:
    html_text = """
    <html>
      <head>
        <script src="/assets/app.js"></script>
      </head>
      <body>
        <div id="root"></div>
        <div>
          This website uses cookies to improve your browsing experience and provide detailed analytics.
          This website uses cookies to improve your browsing experience and provide detailed analytics.
          This website uses cookies to improve your browsing experience and provide detailed analytics.
        </div>
      </body>
    </html>
    """

    assert shell_module.looks_like_shell_page(html_text) is True


def test_shell_evidence_recovers_phone_signals() -> None:
    shell_html = """
    <html>
      <head>
        <script src="/assets/index.js"></script>
      </head>
      <body>
        <div id="root"></div>
      </body>
    </html>
    """
    asset_texts = {
        "https://0xam.de/assets/index.js": """
        "Telefon: +49 30 123 4567"
        "tel:+49 30 123 4568"
        """,
    }

    enriched_html = build_shell_evidence_html("https://0xam.de/", shell_html, asset_texts)
    phones, _page_hits = collect_phones_for_pages([("https://0xam.de/", enriched_html)])

    assert set(phones) == {"+49301234567", "+49301234568"}
