from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.extractor.service import build_learning_feedback
from oldironcrawler.extractor.value_rules import extract_learning_tokens
from oldironcrawler.runtime.global_learning import GlobalLearningStore


def test_extract_learning_tokens_includes_family_feature() -> None:
    tokens = extract_learning_tokens("https://example.com/pages/about-us")

    assert "pages" in tokens
    assert "about" in tokens
    assert "family:pages/about" in tokens


def test_extract_learning_tokens_filters_noise_words_from_family_feature() -> None:
    tokens = extract_learning_tokens("https://example.com/terms-and-conditions")

    assert "and" not in tokens
    assert "family:terms/conditions" in tokens


def test_extract_learning_tokens_skips_root_family_feature() -> None:
    tokens = extract_learning_tokens("https://example.com")

    assert "family:root" not in tokens


def test_global_learning_store_persists_scores_across_instances(tmp_path: Path) -> None:
    db_path = tmp_path / "global_learning.sqlite3"
    first = GlobalLearningStore(db_path)
    try:
        first.record_success("representative", ["about", "family:pages/about"])
        first.record_failure("representative", ["contact", "family:contact/team"])
    finally:
        first.close()

    second = GlobalLearningStore(db_path)
    try:
        scores = second.load_scores("representative")
    finally:
        second.close()

    assert scores["about"] > 0
    assert scores["family:pages/about"] > 0
    assert scores["contact"] < 0
    assert scores["family:contact/team"] < 0


def test_build_learning_feedback_keeps_missing_fields_neutral() -> None:
    feedback = build_learning_feedback(
        representative="",
        evidence_url="",
        rep_urls=[
            "https://example.com/about",
            "https://example.com/team",
        ],
        rep_fetched_urls=[],
        emails="",
        email_sources=[],
        email_urls=[
            "https://example.com/contact",
            "https://example.com/privacy-policy",
        ],
        email_fetched_urls=[],
    )

    assert feedback.rep_negative_tokens == []
    assert feedback.email_negative_tokens == []


def test_build_learning_feedback_adds_contrastive_negatives_when_winner_exists() -> None:
    feedback = build_learning_feedback(
        representative="William Goodman",
        evidence_url="https://example.com/about",
        rep_urls=[
            "https://example.com/about",
            "https://example.com/team",
            "https://example.com/leadership",
        ],
        rep_fetched_urls=[
            "https://example.com/about",
            "https://example.com/team",
        ],
        emails="sales@example.com",
        email_sources=[
            "https://example.com/contact",
        ],
        email_urls=[
            "https://example.com/contact",
            "https://example.com/privacy-policy",
            "https://example.com/support",
        ],
        email_fetched_urls=[
            "https://example.com/contact",
            "https://example.com/privacy-policy",
        ],
    )

    assert "team" in feedback.rep_negative_tokens
    assert "family:team" in feedback.rep_negative_tokens
    assert "leadership" not in feedback.rep_negative_tokens
    assert "privacy" in feedback.email_negative_tokens
    assert "family:privacy/policy" in feedback.email_negative_tokens
    assert "support" not in feedback.email_negative_tokens


def test_build_learning_feedback_ignores_hallucinated_evidence_url() -> None:
    feedback = build_learning_feedback(
        representative="Alex Hatvany",
        evidence_url="https://example.com/hallucinated-founder",
        rep_urls=[
            "https://example.com/about",
            "https://example.com/team",
        ],
        rep_fetched_urls=[
            "https://example.com/about",
            "https://example.com/team",
        ],
        emails="",
        email_sources=[],
        email_urls=[],
        email_fetched_urls=[],
    )

    assert feedback.rep_positive_tokens == []
    assert feedback.rep_negative_tokens == []
