from __future__ import annotations

from hardcover_tools.core.audit_reporting import (
    bucket_sort_key,
    build_compact_audit_actions,
    build_edition_manual_review_queue,
    build_same_id_edition_write_candidates,
    build_write_plan,
    classify_manual_review_bucket,
    filter_compact_write_plan_rows,
)
from hardcover_tools.core.models import AuditRow


def _build_row(
    calibre_book_id: int,
    *,
    title: str,
    authors: str,
    action: str,
    confidence_score: float,
    confidence_tier: str,
    calibre_hardcover_id: str,
    suggested_hardcover_id: str = "",
    current_hardcover_match_ok: str = "yes",
    current_hardcover_edition_id: str = "",
    preferred_edition_id: str = "",
    preferred_edition_title: str = "",
    preferred_edition_language: str = "English",
    preferred_edition_format_normalized: str = "ebook",
    preferred_edition_reading_format: str = "Ebook",
    preferred_edition_edition_format: str = "Kindle",
    suggested_calibre_title: str = "",
    suggested_calibre_authors: str = "",
    suggested_hardcover_title: str = "",
    suggested_hardcover_authors: str = "",
    suggested_hardcover_slug: str = "",
    calibre_hardcover_slug: str = "",
    reason: str = "fixture reason",
    fix_basis: str = "fixture_basis",
) -> AuditRow:
    return AuditRow(
        calibre_book_id=calibre_book_id,
        calibre_title=title,
        calibre_authors=authors,
        calibre_series="",
        calibre_series_index=None,
        calibre_language="eng",
        calibre_hardcover_id=calibre_hardcover_id,
        calibre_hardcover_slug=calibre_hardcover_slug,
        current_hardcover_edition_id=current_hardcover_edition_id,
        file_path=f"Author/Book {calibre_book_id}/book.epub",
        file_format="EPUB",
        file_work_title=title,
        file_work_authors=authors,
        hardcover_title=suggested_hardcover_title or title,
        hardcover_authors=suggested_hardcover_authors or authors,
        hardcover_slug=suggested_hardcover_slug or calibre_hardcover_slug,
        current_hardcover_title=title,
        current_hardcover_authors=authors,
        suggested_hardcover_title=suggested_hardcover_title or title,
        suggested_hardcover_authors=suggested_hardcover_authors or authors,
        preferred_edition_id=preferred_edition_id,
        preferred_edition_title=preferred_edition_title or title,
        preferred_edition_reading_format=preferred_edition_reading_format,
        preferred_edition_edition_format=preferred_edition_edition_format,
        preferred_edition_format_normalized=preferred_edition_format_normalized,
        preferred_edition_is_ebookish=preferred_edition_format_normalized == "ebook",
        preferred_edition_language=preferred_edition_language,
        preferred_edition_reason="preferred fixture reason",
        preferred_edition_score=100,
        runner_up_edition_id="",
        runner_up_edition_reason="",
        default_ebook_edition_id=preferred_edition_id,
        default_ebook_edition_score=100.0,
        preferred_matches_default_ebook=True,
        preferred_vs_default_ebook_score_gap=0.0,
        edition_choice_score=100.0,
        edition_runner_up_score=0.0,
        edition_choice_score_gap=25.0,
        edition_candidates_considered=1,
        confidence_score=confidence_score,
        confidence_tier=confidence_tier,
        recommended_action=action,
        reason=reason,
        current_hardcover_match_ok=current_hardcover_match_ok,
        suggested_calibre_title=suggested_calibre_title,
        suggested_calibre_authors=suggested_calibre_authors,
        suggested_hardcover_id=suggested_hardcover_id,
        suggested_hardcover_slug=suggested_hardcover_slug,
        suggested_hardcover_edition_id=preferred_edition_id,
        suggested_hardcover_edition_title=preferred_edition_title or title,
        suggested_hardcover_edition_format=preferred_edition_edition_format,
        suggested_hardcover_reading_format=preferred_edition_reading_format,
        suggested_hardcover_edition_format_raw=preferred_edition_edition_format,
        suggested_hardcover_edition_format_normalized=preferred_edition_format_normalized,
        suggested_hardcover_edition_is_ebookish=preferred_edition_format_normalized == "ebook",
        suggested_hardcover_edition_language=preferred_edition_language,
        fix_basis=fix_basis,
    )


def _parity_rows() -> list[AuditRow]:
    return [
        _build_row(
            1,
            title="Shared Title",
            authors="Hobb, Robin",
            action="keep_hardcover_id",
            confidence_score=96.0,
            confidence_tier="high",
            calibre_hardcover_id="100",
            suggested_hardcover_id="100",
            current_hardcover_match_ok="yes",
            current_hardcover_edition_id="",
            preferred_edition_id="5001",
            preferred_edition_title="Shared Title",
            calibre_hardcover_slug="shared-title",
            suggested_hardcover_slug="shared-title",
        ),
        _build_row(
            2,
            title="Royal Assassin (The Illustrated Edition)",
            authors="Hobb, Robin",
            action="replace_hardcover_id",
            confidence_score=92.0,
            confidence_tier="high",
            calibre_hardcover_id="999",
            suggested_hardcover_id="200",
            current_hardcover_match_ok="no",
            preferred_edition_id="6002",
            preferred_edition_title="Royal Assassin",
            suggested_calibre_title="Royal Assassin",
            suggested_calibre_authors="Robin Hobb",
            suggested_hardcover_title="Royal Assassin",
            suggested_hardcover_authors="Robin Hobb",
            suggested_hardcover_slug="royal-assassin",
            calibre_hardcover_slug="stale-royal-assassin",
            reason="canonical clean replacement",
        ),
        _build_row(
            3,
            title="Royal Assassin",
            authors="Robin Hobb",
            action="update_calibre_metadata",
            confidence_score=89.0,
            confidence_tier="medium",
            calibre_hardcover_id="200",
            suggested_hardcover_id="200",
            current_hardcover_match_ok="yes",
            preferred_edition_id="6002",
            preferred_edition_title="Royal Assassin",
            suggested_hardcover_title="Royal Assassin",
            suggested_hardcover_authors="Robin Hobb",
            suggested_hardcover_slug="royal-assassin",
            calibre_hardcover_slug="royal-assassin",
            reason="metadata cleanup",
        ),
        _build_row(
            4,
            title="Manual Review Book",
            authors="Jane Doe",
            action="manual_review",
            confidence_score=80.0,
            confidence_tier="medium",
            calibre_hardcover_id="300",
            suggested_hardcover_id="301",
            current_hardcover_match_ok="",
            preferred_edition_id="7004",
            preferred_edition_title="Manual Review Book",
            suggested_hardcover_title="Manual Review Book",
            suggested_hardcover_authors="Jane Doe",
            suggested_hardcover_slug="manual-review-book",
            reason="needs confirmation",
        ),
    ]


def test_audit_reporting_preserves_bucket_order_and_review_classification() -> None:
    rows = _parity_rows()

    assert [bucket_sort_key(row) for row in rows] == [
        (9, -96.0, 1),
        (0, -92.0, 2),
        (2, -89.0, 3),
        (4, -80.0, 4),
    ]
    assert [classify_manual_review_bucket(row) for row in rows] == [
        "manual_review_strong_candidate",
        "manual_review_strong_candidate",
        "manual_review_strong_candidate",
        "manual_review_unresolved_current_id",
    ]


def test_write_plan_and_queues_preserve_expected_guardrails() -> None:
    rows = _parity_rows()

    write_plan = build_write_plan(rows)
    assert [row["action_type"] for row in write_plan] == [
        "keep_hardcover_id",
        "replace_hardcover_id",
        "update_calibre_metadata",
        "manual_review",
    ]
    assert [row["safe_to_apply_boolean"] for row in write_plan] == [True, True, True, False]
    assert write_plan[1]["new_hardcover_id"] == "200"
    assert write_plan[1]["new_calibre_title"] == "Royal Assassin"
    assert write_plan[1]["new_calibre_author"] == "Robin Hobb"
    assert write_plan[1]["suggested_hardcover_edition_format_normalized"] == "ebook"
    assert filter_compact_write_plan_rows(write_plan) == write_plan

    same_id_candidates = build_same_id_edition_write_candidates(rows)
    assert [row["calibre_book_id"] for row in same_id_candidates] == [1, 3]
    assert all(row["safe_for_current_id_write_pass"] for row in same_id_candidates)
    assert all(row["write_guardrail_reason"] == "ok" for row in same_id_candidates)
    assert all(row["needs_hardcover_edition_write"] for row in same_id_candidates)

    review_queue = build_edition_manual_review_queue(rows)
    assert [row["calibre_book_id"] for row in review_queue] == [2, 4]
    assert [row["write_guardrail_reason"] for row in review_queue] == [
        "suggested hardcover-id differs from current hardcover-id",
        "suggested hardcover-id differs from current hardcover-id",
    ]


def test_compact_actions_include_duplicate_review_rows() -> None:
    rows = _parity_rows()

    compact_actions = build_compact_audit_actions(rows)

    assert [row["review_source"] for row in compact_actions] == [
        "book_audit",
        "book_audit",
        "book_audit",
        "duplicate_review",
        "duplicate_review",
    ]
    assert [row["action_bucket"] for row in compact_actions] == [
        "replace_hardcover_id",
        "update_calibre_metadata",
        "manual_review_unresolved_current_id",
        "duplicate_review",
        "duplicate_review",
    ]
    assert [row["calibre_book_id"] for row in compact_actions[-2:]] == [2, 3]
    assert {row["duplicate_group_id"] for row in compact_actions[-2:]} == {"D0001"}
    assert all(not row["safe_to_apply_boolean"] for row in compact_actions[-2:])
