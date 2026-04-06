from __future__ import annotations

from hardcover_tools.core.audit_pipeline import row_from_result
from hardcover_tools.core.models import (
    BookRecord,
    ContentSignals,
    Decision,
    EditionChoiceInfo,
    EmbeddedMeta,
    FileWork,
    HardcoverBook,
    HardcoverEdition,
    MatchScores,
)


def test_row_from_result_preserves_native_audit_row_contract() -> None:
    record = BookRecord(
        calibre_book_id=1,
        calibre_title="Royal Assassin (The Illustrated Edition)",
        calibre_authors="Hobb, Robin",
        calibre_series="Farseer Trilogy",
        calibre_series_index=2.0,
        calibre_language="eng",
        calibre_hardcover_id="999",
        calibre_hardcover_slug="stale-royal-assassin",
        calibre_hardcover_edition_id="111",
        file_path="Author/Book/book.epub",
        file_format="EPUB",
    )
    file_work = FileWork(
        title="Royal Assassin",
        authors="Robin Hobb",
        language="English",
        title_basis="embedded",
        authors_basis="embedded",
    )
    embedded = EmbeddedMeta(
        embedded_title="Royal Assassin",
        embedded_authors="Robin Hobb",
        embedded_language="English",
        tool_used="host-ebook-meta",
    )
    content = ContentSignals(
        inferred_title_from_content="Royal Assassin",
        inferred_author_from_content="Robin Hobb",
        inferred_language_from_content="English",
        first_heading_excerpt="Royal Assassin",
    )
    current_book = HardcoverBook(
        id=999,
        title="Royal Assassin (Illustrated Edition)",
        subtitle="",
        authors="Robin Hobb",
        series="Farseer Trilogy [2]",
        release_date="1996-01-01",
        slug="stale-royal-assassin",
    )
    best_book = HardcoverBook(
        id=200,
        title="Royal Assassin",
        subtitle="",
        authors="Robin Hobb",
        series="Farseer Trilogy [2]",
        release_date="1996-01-01",
        slug="royal-assassin",
    )
    preferred = HardcoverEdition(
        id=6002,
        book_id=200,
        title="Royal Assassin",
        subtitle="",
        authors="Robin Hobb",
        edition_format="Kindle",
        reading_format="Ebook",
        language="English",
        users_count=10,
        users_read_count=5,
    )
    runner_up = HardcoverEdition(
        id=6003,
        book_id=200,
        title="Royal Assassin",
        subtitle="",
        authors="Robin Hobb",
        edition_format="Paperback",
        reading_format="Read",
        language="English",
    )
    default_ebook = HardcoverEdition(
        id=6002,
        book_id=200,
        title="Royal Assassin",
        subtitle="",
        authors="Robin Hobb",
        edition_format="Kindle",
        reading_format="Ebook",
        language="English",
    )
    choice = EditionChoiceInfo(
        chosen=preferred,
        runner_up=runner_up,
        default_ebook=default_ebook,
        chosen_score=120.0,
        runner_up_score=80.0,
        default_ebook_score=120.0,
        score_gap=40.0,
        chosen_vs_default_ebook_score_gap=0.0,
        count_considered=2,
        chosen_reason="fixture chosen",
        runner_up_reason="fixture runner",
        default_ebook_reason="fixture default",
        chosen_matches_default_ebook=True,
    )
    decision = Decision(
        action="replace_hardcover_id",
        confidence_score=92.0,
        confidence_tier="high",
        reason="canonical clean replacement",
        issue_category="hardcover_link",
        suggested_calibre_title="Royal Assassin",
        suggested_calibre_authors="Robin Hobb",
        suggested_hardcover_id="200",
        suggested_hardcover_slug="royal-assassin",
        suggested_hardcover_edition_id="6002",
        suggested_hardcover_edition_title="Royal Assassin",
        suggested_hardcover_edition_format="Kindle",
        suggested_hardcover_reading_format="Ebook",
        suggested_hardcover_edition_format_raw="Kindle",
        suggested_hardcover_edition_format_normalized="ebook",
        suggested_hardcover_edition_is_ebookish=True,
        suggested_hardcover_edition_language="English",
        fix_basis="file_first_best_match",
    )
    breakdown = MatchScores(title_score=1.0, author_score=1.0, series_score=10.0, total_score=100.0)

    row = row_from_result(
        record,
        file_work,
        embedded,
        content,
        current_book,
        best_book,
        choice,
        "search",
        False,
        decision,
        breakdown,
    )

    assert row.preferred_edition_format_normalized == "ebook"
    assert row.preferred_edition_is_ebookish is True
    assert row.runner_up_edition_format_normalized == "paperback"
    assert row.runner_up_edition_is_ebookish is False
    assert row.default_ebook_edition_format_normalized == "ebook"
    assert row.calibre_author_normalized == "robin hobb"
    assert row.file_author_normalized == "robin hobb"
    assert row.hardcover_primary_author_normalized == "robin hobb"
    assert row.embedded_authors_mismatch_to_calibre_canonical is False
    assert row.embedded_authors_mismatch_to_suggested_canonical is False
    assert row.suggested_hardcover_id == "200"
    assert row.hardcover_title == "Royal Assassin"
    assert row.same_hardcover_id_as_suggestion is False
