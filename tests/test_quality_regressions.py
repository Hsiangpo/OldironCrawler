from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.extractor import llm_client as llm_module
from oldironcrawler.extractor import protocol_client as protocol_module
from oldironcrawler.extractor.company_rules import clean_company_name_candidate
from oldironcrawler.extractor.service import _build_discovery_snapshot, _has_enough_discovery_coverage
from oldironcrawler.extractor.value_rules import build_candidates, build_fetch_plan, merge_representative_urls, select_representative_urls
from oldironcrawler.extractor.protocol_client import SiteProtocolClient, SiteProtocolConfig


def test_company_name_cleanup_drops_account_suspended() -> None:
    assert clean_company_name_candidate("Account Suspended") == ""


def test_company_name_cleanup_strips_registration_number_suffix() -> None:
    assert clean_company_name_candidate("The Baby Surrogacy LTD 14038878") == "The Baby Surrogacy LTD"


def test_company_name_cleanup_prefers_trading_as_name() -> None:
    assert clean_company_name_candidate("Daniel Mead (trading as Mead Electrical Services)") == "Mead Electrical Services"


def test_convert_pages_to_markdown_keeps_head_signals_for_representative_pages() -> None:
    client = object.__new__(llm_module.WebsiteLlmClient)
    converted = client._convert_pages_to_markdown(
        [
            {
                "url": "https://www.watsonco.co.uk/contact-us/",
                "html": """
                <html>
                  <head>
                    <title>Contact Us - WatsonCo</title>
                    <meta property="og:title" content="Contact Us - WatsonCo" />
                    <meta name="description" content="Contact Mark Watson on 07949 646565 or email markwatson@watsonco.co.uk." />
                  </head>
                  <body>
                    <h1>WatsonCo Chartered Accountants</h1>
                    <p>Phone: 07949 64 65 65</p>
                  </body>
                </html>
                """,
            }
        ]
    )

    content = converted[0]["content"]

    assert "Mark Watson" in content
    assert "markwatson@watsonco.co.uk" in content
    assert "Contact Us - WatsonCo" in content


def test_representative_selection_prefers_named_people_page_over_generic_business_pages() -> None:
    candidates = build_candidates(
        "https://www.canaccord-wealth.com",
        [
            "https://www.canaccord-wealth.com/about-us/our-people",
            "https://www.canaccord-wealth.com/about-us/our-people/david-esfandi",
            "https://www.canaccord-wealth.com/about-us/charitable-foundation",
            "https://www.canaccord-wealth.com/about-us/corporate-responsibility",
            "https://www.canaccord-wealth.com/our-services/investment-management/funds",
            "https://www.canaccord-wealth.com",
        ],
        rep_learned={},
        email_learned={},
    )

    rep_urls, _teacher_pool = select_representative_urls(candidates, target_count=5)

    assert "https://www.canaccord-wealth.com/about-us/our-people/david-esfandi" in rep_urls
    assert "https://www.canaccord-wealth.com/about-us/charitable-foundation" not in rep_urls
    assert "https://www.canaccord-wealth.com/about-us/corporate-responsibility" not in rep_urls
    assert "https://www.canaccord-wealth.com/our-services/investment-management/funds" not in rep_urls


def test_representative_selection_breaks_same_score_people_ties_by_discovery_order() -> None:
    candidates = build_candidates(
        "https://www.canaccord-wealth.com",
        [
            "https://www.canaccord-wealth.com/about-us/our-people",
            "https://www.canaccord-wealth.com/about-us/our-people/david-esfandi",
            "https://www.canaccord-wealth.com/about-us/our-people/andy-finch",
            "https://www.canaccord-wealth.com/about-us",
        ],
        rep_learned={},
        email_learned={},
    )

    rep_urls, _teacher_pool = select_representative_urls(candidates, target_count=5)

    assert "https://www.canaccord-wealth.com/about-us/our-people/david-esfandi" in rep_urls
    assert "https://www.canaccord-wealth.com/about-us/our-people/andy-finch" not in rep_urls


def test_generic_audience_about_page_is_not_misclassified_as_person_detail() -> None:
    candidates = build_candidates(
        "https://www.canaccord-wealth.com",
        [
            "https://www.canaccord-wealth.com/financial-advisers/about-us",
        ],
        rep_learned={},
        email_learned={},
    )

    candidate = next(item for item in candidates if item.url.endswith("/financial-advisers/about-us"))

    assert candidate.is_person_detail_page is False


def test_representative_scoring_keeps_named_profile_ahead_of_overlearned_about_page() -> None:
    candidates = build_candidates(
        "https://www.arema.co.uk",
        [
            "https://www.arema.co.uk/about",
            "https://www.arema.co.uk/team-coaching.html",
            "https://www.arema.co.uk/andy-maggs-referrals.html",
        ],
        rep_learned={
            "about": 120,
            "family:about": 120,
            "team": 52,
            "coaching": 52,
            "family:team/coaching": 52,
        },
        email_learned={},
    )

    candidate_map = {candidate.url: candidate for candidate in candidates}

    assert candidate_map["https://www.arema.co.uk/andy-maggs-referrals.html"].rep_final_score > candidate_map["https://www.arema.co.uk/about"].rep_final_score


def test_representative_url_merge_dedupes_www_and_trailing_slash_variants() -> None:
    urls = merge_representative_urls(
        ["https://watsonco.co.uk/contact-us"],
        [
            "https://www.watsonco.co.uk/contact-us/",
            "https://www.watsonco.co.uk/",
        ],
        limit=5,
    )

    assert urls == [
        "https://watsonco.co.uk/contact-us",
        "https://www.watsonco.co.uk/",
    ]


def test_fetch_plan_dedupes_www_variants_from_rep_and_email_targets() -> None:
    plan = build_fetch_plan(
        "https://watsonco.co.uk",
        [
            "https://watsonco.co.uk/contact-us",
            "https://www.watsonco.co.uk/contact-us/",
        ],
        [
            "https://www.watsonco.co.uk/contact-us",
            "https://watsonco.co.uk",
        ],
        rep_limit=5,
        email_soft_limit=2,
        email_hard_limit=4,
        total_hard_limit=5,
    )

    assert plan["rep_urls"] == ["https://watsonco.co.uk/contact-us"]
    assert plan["all_primary_urls"] == [
        "https://watsonco.co.uk/contact-us",
        "https://watsonco.co.uk",
    ]


def test_build_common_probe_urls_prioritizes_imprint_and_people_paths() -> None:
    urls = protocol_module._build_common_probe_urls("https://example.com/en")

    assert "https://example.com/en/imprint" in urls
    assert "https://example.com/en/about-us/our-people" in urls
    assert urls.index("https://example.com/en/imprint") < urls.index("https://example.com/en/contact")
    assert urls.index("https://example.com/en/about-us/our-people") < urls.index("https://example.com/en/about")


def test_discover_sitemap_urls_prioritizes_value_urls_before_limit(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    low_value = [f"https://example.com/your-needs/topic-{index}" for index in range(8)]
    high_value = [
        "https://example.com/about-us/our-people",
        "https://example.com/about-us/our-people/david-esfandi",
        "https://example.com/en/imprint",
    ]

    monkeypatch.setattr(client, "_find_sitemap_locations", lambda *_args, **_kwargs: ["https://example.com/sitemap.xml"])

    def fake_parse(_session, _sitemap_url, result, visited, *, base_host: str, limit: int, depth: int) -> None:
        for url in [*low_value, *high_value]:
            result.append(url)

    monkeypatch.setattr(client, "_parse_sitemap_recursive", fake_parse)

    urls = client._discover_sitemap_urls(object(), "https://example.com/en", limit=5)

    assert "https://example.com/about-us/our-people" in urls
    assert "https://example.com/about-us/our-people/david-esfandi" in urls
    assert "https://example.com/en/imprint" in urls
    client.close()


def test_discovery_coverage_requires_stronger_representative_signal_before_early_stop() -> None:
    snapshot = _build_discovery_snapshot(
        "https://www.canaccord-wealth.com",
        [
            "https://www.canaccord-wealth.com/about-us",
            "https://www.canaccord-wealth.com/about-us/our-people",
            "https://www.canaccord-wealth.com/about-us/careers",
            "https://www.canaccord-wealth.com/people",
            "https://www.canaccord-wealth.com/contact-us",
            "https://www.canaccord-wealth.com/help-and-contact/our-offices",
            "https://www.canaccord-wealth.com/privacy-policy",
            "https://www.canaccord-wealth.com/support",
            "https://www.canaccord-wealth.com/legal-and-regulatory-information",
            "https://www.canaccord-wealth.com/office-locations",
        ],
        rep_learned={},
        email_learned={},
        rep_target_count=5,
    )

    assert len(snapshot.rep_urls) == 5
    assert _has_enough_discovery_coverage(snapshot, rep_target_count=5) is False
