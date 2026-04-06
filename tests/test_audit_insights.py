from __future__ import annotations

from hardcover_tools.core.audit_pipeline import _build_verbose_detail_lines, choose_best_candidate
from hardcover_tools.core.audit_insights import (
    build_progress_line,
    format_action_family_counts,
    metadata_probe_diagnostic,
)
from hardcover_tools.core.edition_selection import EditionChoiceInfo
from hardcover_tools.core.models import BookRecord, ContentSignals, EmbeddedMeta, FileWork, HardcoverBook, HardcoverEdition
from tests.test_audit_reporting import _build_row, _parity_rows


def test_format_action_family_counts_reports_compact_operator_mix() -> None:
    rows = _parity_rows()

    assert format_action_family_counts(rows) == "keep:1, meta:1, id:1, review:1"


def test_build_progress_line_surfaces_guardrails_and_metadata_probe_alerts() -> None:
    probe_row = _build_row(
        5,
        title="Reaper",
        authors="Will Wight",
        action="manual_review",
        confidence_score=81.0,
        confidence_tier="medium",
        calibre_hardcover_id="446337",
        suggested_hardcover_id="446337",
        preferred_edition_id="30407439",
        preferred_edition_title="Reaper",
        suggested_hardcover_title="Reaper",
        suggested_hardcover_authors="Will Wight",
        suggested_hardcover_slug="reaper",
        calibre_hardcover_slug="reaper",
        reason="preferred_edition_blank_language; manual review required before any edition write",
    )
    probe_row.file_work_title = "Reape"
    probe_row.file_work_authors = "Will Wight"
    probe_row.file_work_title_basis = "embedded"
    probe_row.file_work_authors_basis = "embedded"
    probe_row.ebook_meta_tool_used = "host-ebook-meta"

    warning, details = metadata_probe_diagnostic(probe_row)
    progress_line = build_progress_line(
        _parity_rows() + [probe_row],
        current=5,
        total=10,
        elapsed_s=2.5,
        hardcover_delta_text="net=1 cache=9 throttle=0/0.00s",
    )

    assert warning == "possible_file_work_title_truncation"
    assert 'file_work_title="Reape"' in details
    assert "[PROGRESS] books=5/10" in progress_line
    assert "pct=50%" in progress_line
    assert "actions=keep:1, meta:1, id:1, review:2" in progress_line
    assert "alerts=blank_lang:1, probe:1" in progress_line
    assert "hc=net=1 cache=9 throttle=0/0.00s" in progress_line


class _StubHardcoverClient:
    def __init__(self, book: HardcoverBook, editions: list[HardcoverEdition]) -> None:
        self._book = book
        self._editions = editions

    def find_book_ids_by_identifier(self, _token: str) -> list[int]:
        return []

    def search_book_ids(self, _query: str, per_page: int = 5, page: int = 1) -> list[int]:
        assert per_page == 5
        assert page == 1
        return [self._book.id]

    def fetch_books(self, ids: list[int]) -> dict[int, HardcoverBook]:
        return {self._book.id: self._book} if self._book.id in ids else {}

    def fetch_editions_for_books(self, ids: list[int]) -> dict[int, list[HardcoverEdition]]:
        return {self._book.id: list(self._editions)} if self._book.id in ids else {}


def test_choose_best_candidate_verbose_path_keeps_alternative_preview_available(capsys) -> None:
    record = BookRecord(
        calibre_book_id=2,
        calibre_title="The Great Change",
        calibre_authors="Joe Abercrombie",
        calibre_series="",
        calibre_series_index=None,
        calibre_language="eng",
        calibre_hardcover_id="",
        calibre_hardcover_slug="",
    )
    file_work = FileWork(
        title="The Great Change",
        authors="Joe Abercrombie",
        language="English",
        title_basis="embedded",
        authors_basis="embedded",
    )
    embedded = EmbeddedMeta(embedded_title="The Great Change", embedded_authors="Joe Abercrombie")
    content = ContentSignals()
    book = HardcoverBook(
        id=1118100,
        title="The Great Change",
        subtitle="",
        authors="Joe Abercrombie",
        series="",
        release_date="",
        slug="the-great-change",
        default_ebook_edition_id=31142083,
    )
    editions = [
        HardcoverEdition(
            id=31142083,
            book_id=1118100,
            title="The Great Change",
            subtitle="",
            authors="Joe Abercrombie",
            users_count=3,
            edition_format="ebook",
            reading_format="Ebook",
            language="English",
        ),
        HardcoverEdition(
            id=30455072,
            book_id=1118100,
            title="The Great Change",
            subtitle="",
            authors="Joe Abercrombie",
            users_count=1,
            edition_format="hardcover",
            reading_format="Physical",
            language="English",
        ),
    ]

    best_book, edition_choice, best_score, _breakdown, why = choose_best_candidate(
        record,
        file_work,
        embedded,
        content,
        _StubHardcoverClient(book, editions),
        verbose=True,
    )

    output = capsys.readouterr().out
    assert best_book is not None
    assert best_book.id == 1118100
    assert edition_choice.chosen is not None
    assert best_score > 0
    assert why
    assert "search candidates=" in output
    assert "search best preferred=" in output


def test_build_verbose_detail_lines_handles_single_candidate_gap_tier() -> None:
    row = _build_row(
        7,
        title="Reaper",
        authors="Will Wight",
        action="keep_hardcover_id",
        confidence_score=103.06,
        confidence_tier="high",
        calibre_hardcover_id="446337",
        suggested_hardcover_id="446337",
        preferred_edition_id="30407439",
        preferred_edition_title="Reaper",
        suggested_hardcover_title="Reaper",
        suggested_hardcover_authors="Will Wight",
        suggested_hardcover_slug="reaper",
        calibre_hardcover_slug="reaper",
        reason="Current Hardcover link verified against the actual ebook file",
    )
    file_work = FileWork(
        title="Reape",
        authors="Will Wight",
        language="English",
        title_basis="embedded",
        authors_basis="embedded",
    )
    current_book = HardcoverBook(
        id=446337,
        title="Reaper",
        subtitle="",
        authors="Will Wight",
        series="Cradle",
        release_date="",
        slug="reaper",
        default_ebook_edition_id=30407439,
    )
    best_edition = HardcoverEdition(
        id=30407439,
        book_id=446337,
        title="Reaper",
        subtitle="",
        authors="Will Wight",
        users_count=256,
        edition_format="ebook",
        reading_format="Ebook",
        language="English",
    )
    best_choice = EditionChoiceInfo(
        chosen=best_edition,
        runner_up=None,
        chosen_score=2371.0,
        score_gap=1390.0,
    )

    lines = _build_verbose_detail_lines(
        row,
        file_work=file_work,
        current_book=current_book,
        current_score=103.06,
        current_ok=True,
        current_why="close-title,author,series",
        best_choice=best_choice,
        best_book=current_book,
        best_edition=best_edition,
        search_beyond_current=False,
        search_reason="",
        metadata_probe_warning="possible_file_work_title_truncation",
        metadata_probe_details='file_work_title="Reape" vs calibre_title="Reaper" source=embedded tool=host-ebook-meta',
        suggest_text='hc="Reaper" [446337] by Will Wight edition="Reaper" [30407439]',
    )

    assert any("preferred=" in line and "single_candidate" in line for line in lines)
    assert any("warning=possible_file_work_title_truncation" in line for line in lines)
