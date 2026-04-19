from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.extractor.protocol_client import SiteProtocolClient, SiteProtocolConfig
from oldironcrawler.extractor.protocol_discovery import extract_same_site_links
from oldironcrawler.extractor.service import _build_discovery_snapshot, _has_enough_discovery_coverage
from oldironcrawler.extractor.value_rules import build_candidates, select_email_urls


def test_extract_same_site_links_strips_tracking_query_duplicates() -> None:
    html = """
    <a href="/contact?utm_source=google">Contact A</a>
    <a href="/contact?fbclid=123">Contact B</a>
    <a href="/contact">Contact C</a>
    """

    urls = extract_same_site_links(html, "https://example.com", limit=10)

    assert urls == ["https://example.com/contact"]


def test_common_probe_scan_stops_after_low_yield_batches(monkeypatch) -> None:
    client = SiteProtocolClient(
        SiteProtocolConfig(
            common_probe_target=8,
            common_probe_concurrency=4,
            common_probe_patience_batches=2,
            common_probe_min_hits_after_patience=2,
        )
    )
    calls: list[list[str]] = []

    monkeypatch.setattr(
        "oldironcrawler.extractor.protocol_client._build_common_probe_urls",
        lambda _start_url: [f"https://example.com/path-{index}" for index in range(20)],
    )

    def fake_probe_common_value_batch(batch: list[str]) -> list[str]:
        calls.append(batch)
        return []

    monkeypatch.setattr(client, "_probe_common_value_batch", fake_probe_common_value_batch)

    urls = client._probe_common_value_urls(client._get_or_create_session(), "https://example.com", limit=40)

    assert urls == []
    assert len(calls) == 2
    client.close()


def test_select_email_urls_limits_family_sprawl_and_total_count() -> None:
    discovered_urls = [
        "https://example.com/contact-us",
        "https://example.com/privacy-policy",
        "https://example.com/about-us",
        "https://example.com/support-alpha/team",
        "https://example.com/support-alpha/contact",
        "https://example.com/support-alpha/office",
    ]
    for token in (
        "beta",
        "gamma",
        "delta",
        "epsilon",
        "zeta",
        "theta",
        "iota",
        "kappa",
        "lambda",
        "mu",
        "nu",
        "xi",
        "omicron",
        "pi",
        "rho",
        "sigma",
        "tau",
        "upsilon",
        "phi",
        "chi",
        "psi",
        "omega",
        "atlas",
        "nova",
        "orion",
        "luna",
        "terra",
        "aurora",
        "solaris",
        "vector",
        "zenith",
        "meridian",
        "horizon",
        "quantum",
        "cosmos",
    ):
        discovered_urls.append(f"https://example.com/support-{token}")

    snapshot = _build_discovery_snapshot("https://example.com", discovered_urls, {}, {})
    urls = select_email_urls(snapshot.candidates)
    alpha_urls = [url for url in urls if "/support-alpha/" in url]

    assert len(alpha_urls) <= 2
    assert len(urls) <= 32


def test_discovery_coverage_stops_when_rep_and_email_families_are_enough() -> None:
    discovered_urls = [
        "https://example.com/about-us",
        "https://example.com/team",
        "https://example.com/leadership",
        "https://example.com/founder",
        "https://example.com/board",
        "https://example.com/contact-us",
        "https://example.com/privacy-policy",
        "https://example.com/support",
        "https://example.com/legal",
        "https://example.com/careers",
    ]

    snapshot = _build_discovery_snapshot("https://example.com", discovered_urls, {}, {})

    assert _has_enough_discovery_coverage(snapshot) is True


def test_build_candidates_does_not_substring_match_directory_contacts() -> None:
    candidates = build_candidates(
        "https://example.com",
        ["https://example.com/directory/contacts"],
        {},
        {},
    )

    candidate = next(item for item in candidates if item.url.endswith("/directory/contacts"))

    assert candidate.rep_rule_score <= 0
    assert candidate.email_rule_score <= 0
