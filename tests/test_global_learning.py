from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.extractor.service import build_learning_feedback
from oldironcrawler.extractor.value_rules import extract_learning_tokens
from oldironcrawler.runtime.global_learning import (
    _DECAY_STEP,
    _DECAY_WINDOW_DAYS,
    _POSITIVE_DECAY_FLOOR,
    GlobalLearningStore,
)


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


def test_extract_learning_tokens_drops_person_name_tokens_for_profile_pages() -> None:
    tokens = extract_learning_tokens("https://example.com/about-us/our-people/david-esfandi")

    assert "about" in tokens
    assert "people" in tokens
    assert "family:about/people" in tokens
    assert "david" not in tokens
    assert "esfandi" not in tokens


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


def test_build_learning_feedback_successful_representative_keeps_other_rep_pages_neutral() -> None:
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

    assert "about" in feedback.rep_positive_tokens
    assert feedback.rep_negative_tokens == []


def test_build_learning_feedback_representative_failure_stays_neutral() -> None:
    feedback = build_learning_feedback(
        representative="",
        evidence_url="",
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

    assert feedback.rep_negative_tokens == []
    assert feedback.rep_positive_tokens == []


def test_build_learning_feedback_successful_email_keeps_other_email_pages_neutral() -> None:
    feedback = build_learning_feedback(
        representative="",
        evidence_url="",
        rep_urls=[],
        rep_fetched_urls=[],
        emails="sales@example.com",
        email_sources=[
            "https://example.com/contact",
        ],
        email_urls=[
            "https://example.com/contact",
            "https://example.com/privacy-policy",
        ],
        email_fetched_urls=[
            "https://example.com/contact",
            "https://example.com/privacy-policy",
        ],
    )

    assert "contact" in feedback.email_positive_tokens
    assert feedback.email_negative_tokens == []


def test_build_learning_feedback_email_failure_stays_neutral() -> None:
    feedback = build_learning_feedback(
        representative="",
        evidence_url="",
        rep_urls=[],
        rep_fetched_urls=[],
        emails="",
        email_sources=[],
        email_urls=[
            "https://example.com/contact",
            "https://example.com/privacy-policy",
        ],
        email_fetched_urls=[
            "https://example.com/contact",
            "https://example.com/privacy-policy",
        ],
    )

    assert feedback.email_negative_tokens == []
    assert feedback.email_positive_tokens == []


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


def test_global_learning_store_decay_reduces_old_positive_scores(tmp_path: Path) -> None:
    db_path = tmp_path / "global_learning.sqlite3"
    store = GlobalLearningStore(db_path)
    try:
        store.record_success("representative", ["about"])
        store.record_success("representative", ["about"])
        _set_feature_updated_at(
            store,
            "representative",
            "about",
            datetime.now(timezone.utc) - timedelta(days=(_DECAY_WINDOW_DAYS * 2) + 1),
        )
        scores = store.load_scores("representative")
    finally:
        store.close()

    assert scores["about"] == max((3 * 2) - (_DECAY_STEP * 2), _POSITIVE_DECAY_FLOOR)


def test_global_learning_store_decay_skips_recent_positive_scores(tmp_path: Path) -> None:
    db_path = tmp_path / "global_learning.sqlite3"
    store = GlobalLearningStore(db_path)
    try:
        store.record_success("representative", ["about"])
        _set_feature_updated_at(
            store,
            "representative",
            "about",
            datetime.now(timezone.utc) - timedelta(days=max(_DECAY_WINDOW_DAYS - 1, 0)),
        )
        scores = store.load_scores("representative")
    finally:
        store.close()

    assert scores["about"] == 3


def test_global_learning_store_decay_keeps_negative_scores_unchanged(tmp_path: Path) -> None:
    db_path = tmp_path / "global_learning.sqlite3"
    store = GlobalLearningStore(db_path)
    try:
        store.record_failure("representative", ["about"])
        store.record_failure("representative", ["about"])
        _set_feature_updated_at(
            store,
            "representative",
            "about",
            datetime.now(timezone.utc) - timedelta(days=(_DECAY_WINDOW_DAYS * 3) + 1),
        )
        scores = store.load_scores("representative")
    finally:
        store.close()

    assert scores["about"] == -2


def _set_feature_updated_at(
    store: GlobalLearningStore,
    kind: str,
    feature: str,
    updated_at: datetime,
) -> None:
    text = updated_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with store._connect() as conn:
        conn.execute(
            """
            UPDATE learning_features
            SET updated_at = ?
            WHERE kind = ? AND feature = ?
            """,
            (text, kind, feature),
        )
