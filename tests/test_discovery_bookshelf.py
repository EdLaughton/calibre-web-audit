from __future__ import annotations

import csv

from hardcover_tools.core.bookshelf_export import (
    BookshelfIntegrationResult,
    build_bookshelf_lookup_attempts,
    build_bookshelf_queue,
    detect_bookshelf_metadata_backend,
    run_bookshelf_integration,
)
from hardcover_tools.core.config import parse_discovery_args
from hardcover_tools.core.models import HardcoverBook, HardcoverEdition
from hardcover_tools.core.output import build_discovery_outputs


class FakeHardcoverClient:
    def __init__(self, books_by_id: dict[int, HardcoverBook], editions_by_book_id: dict[int, list[HardcoverEdition]]) -> None:
        self.books_by_id = books_by_id
        self.editions_by_book_id = editions_by_book_id
        self.calls: list[list[int]] = []

    def fetch_books_and_editions_for_books(self, ids, **kwargs):
        self.calls.append(list(ids))
        return self.books_by_id, self.editions_by_book_id


class FakeBookshelfClient:
    def __init__(self, *, metadata_source: str, search_results: dict[str, list[dict]], add_response: dict | None = None) -> None:
        self.metadata_source = metadata_source
        self.search_results = search_results
        self.add_response = add_response or {"id": 501}
        self.added_books: list[dict] = []
        self.added_authors: list[dict] = []
        self.commands: list[tuple[str, dict]] = []

    def get_development_config(self) -> dict:
        return {"metadataSource": self.metadata_source}

    def search(self, term: str) -> list[dict]:
        return list(self.search_results.get(term, []))

    def add_book(self, payload: dict) -> dict:
        self.added_books.append(payload)
        response = dict(self.add_response)
        response.setdefault("foreignBookId", payload.get("foreignBookId"))
        response.setdefault("foreignEditionId", payload.get("foreignEditionId"))
        return response

    def add_author(self, payload: dict) -> dict:
        self.added_authors.append(payload)
        response = dict(self.add_response)
        response.setdefault("foreignAuthorId", payload.get("foreignAuthorId"))
        return response

    def enqueue_command(self, name: str, body: dict) -> dict:
        self.commands.append((name, dict(body)))
        return {"id": len(self.commands), "name": name}


def _book(book_id: int = 101, *, slug: str = "the-last-kingdom", title: str = "The Last Kingdom") -> HardcoverBook:
    return HardcoverBook(
        id=book_id,
        title=title,
        subtitle="",
        authors="Bernard Cornwell",
        series="",
        release_date="2004-01-01",
        slug=slug,
    )


def _edition(
    edition_id: int = 501,
    *,
    book_id: int = 101,
    isbn13: str = "9780000000001",
    asin: str = "B000TEST01",
) -> HardcoverEdition:
    return HardcoverEdition(
        id=edition_id,
        book_id=book_id,
        title="The Last Kingdom",
        subtitle="",
        authors="Bernard Cornwell",
        isbn_13=isbn13,
        asin=asin,
        edition_format="Ebook",
        reading_format="ebook",
        language="English",
    )


def _candidate(
    *,
    title: str = "The Last Kingdom",
    authors: str = "Bernard Cornwell",
    bucket: str = "shortlist",
    eligible: bool = True,
    discovery_bucket: str = "missing_series",
    display_book_id: str = "101",
    preferred_edition_id: str = "501",
) -> dict[str, object]:
    return {
        "discovery_bucket": discovery_bucket,
        "discovery_priority_bucket": bucket,
        "eligible_for_shortlist_boolean": eligible,
        "shortlist_reason": "ok" if eligible else "review",
        "display_title": title,
        "display_authors": authors,
        "display_series": "The Saxon Stories",
        "display_book_id": display_book_id,
        "preferred_edition_id": preferred_edition_id,
        "preferred_edition_language": "English",
        "preferred_edition_format_normalized": "ebook",
        "gap_kind": "",
        "owned_author_names": authors,
    }


def _search_book_payload(*, local_id: int = 0) -> dict:
    return {
        "id": local_id,
        "foreignBookId": "101",
        "foreignEditionId": "501",
        "title": "The Last Kingdom",
        "author": {
            "id": 0,
            "authorName": "Bernard Cornwell",
            "foreignAuthorId": "9001",
        },
        "editions": [
            {
                "foreignEditionId": "501",
                "isbn13": "9780000000001",
                "asin": "B000TEST01",
                "monitored": True,
            }
        ],
    }


def test_build_bookshelf_queue_defaults_to_shortlist_only() -> None:
    hardcover_client = FakeHardcoverClient(
        {101: _book(), 102: _book(102, slug="other-book", title="Other Book")},
        {
            101: [_edition()],
            102: [_edition(edition_id=502, book_id=102, isbn13="9780000000002", asin="B000TEST02")],
        },
    )
    candidates = [
        _candidate(bucket="shortlist", eligible=True, display_book_id="101", preferred_edition_id="501"),
        _candidate(
            title="Blank Language Pick",
            bucket="shortlist_blank_language_likely_english",
            eligible=True,
            display_book_id="102",
            preferred_edition_id="502",
        ),
        _candidate(title="Review Row", bucket="manual_review", eligible=False, discovery_bucket="unowned_standalone"),
        _candidate(title="Suppressed Row", bucket="suppressed_non_english", eligible=False),
    ]

    queue_rows = build_bookshelf_queue(
        candidates,
        hardcover_client=hardcover_client,
        approval_mode="shortlist-only",
        requested_mode="book",
    )

    assert [row["display_title"] for row in queue_rows] == ["The Last Kingdom", "Blank Language Pick"]
    assert [row["approval_bucket"] for row in queue_rows] == [
        "shortlist",
        "shortlist_blank_language_likely_english",
    ]


def test_parse_discovery_args_supports_bookshelf_export_flags() -> None:
    config = parse_discovery_args(
        [
            "--library-root",
            "/tmp/library",
            "--export-bookshelf",
            "--bookshelf-mode",
            "author",
            "--bookshelf-approval",
            "safe-only",
            "--dry-run",
        ]
    )

    assert config.export_bookshelf is True
    assert config.push_bookshelf is False
    assert config.dry_run is True
    assert config.bookshelf_mode == "author"
    assert config.bookshelf_approval == "safe-only"


def test_parse_discovery_args_requires_connection_and_profiles_for_live_push() -> None:
    try:
        parse_discovery_args(["--library-root", "/tmp/library", "--push-bookshelf"])
    except SystemExit:
        pass
    else:
        raise AssertionError("expected parse_discovery_args to reject incomplete --push-bookshelf configuration")


def test_build_bookshelf_queue_safe_only_excludes_blank_language_shortlist() -> None:
    hardcover_client = FakeHardcoverClient({101: _book()}, {101: [_edition()]})
    candidates = [
        _candidate(bucket="shortlist", eligible=True),
        _candidate(title="Blank Language Pick", bucket="shortlist_blank_language_likely_english", eligible=True),
    ]

    queue_rows = build_bookshelf_queue(
        candidates,
        hardcover_client=hardcover_client,
        approval_mode="safe-only",
        requested_mode="book",
    )

    assert [row["display_title"] for row in queue_rows] == ["The Last Kingdom"]


def test_direct_id_lookup_is_only_used_when_metadata_backend_is_hardcover() -> None:
    hardcover_client = FakeHardcoverClient({101: _book()}, {101: [_edition()]})
    queue_row = build_bookshelf_queue([_candidate()], hardcover_client=hardcover_client)[0]

    backend, allow_direct_ids, _ = detect_bookshelf_metadata_backend("https://api.hardcover.app/v1")
    attempts = build_bookshelf_lookup_attempts(
        queue_row,
        metadata_backend=backend,
        hardcover_direct_ids_allowed=allow_direct_ids,
    )

    assert [attempt.strategy for attempt in attempts] == [
        "direct_edition_id",
        "direct_work_id",
        "isbn",
        "asin",
        "title_author",
    ]


def test_non_hardcover_lookup_falls_back_to_identifier_and_title_author_terms() -> None:
    hardcover_client = FakeHardcoverClient({101: _book()}, {101: [_edition()]})
    queue_row = build_bookshelf_queue([_candidate()], hardcover_client=hardcover_client)[0]

    backend, allow_direct_ids, _ = detect_bookshelf_metadata_backend("https://metadata.example.invalid")
    attempts = build_bookshelf_lookup_attempts(
        queue_row,
        metadata_backend=backend,
        hardcover_direct_ids_allowed=allow_direct_ids,
    )

    assert [attempt.strategy for attempt in attempts] == ["isbn", "asin", "title_author"]


def test_bookshelf_push_dry_run_matches_without_posting() -> None:
    hardcover_client = FakeHardcoverClient({101: _book()}, {101: [_edition()]})
    bookshelf_client = FakeBookshelfClient(
        metadata_source="https://api.hardcover.app/v1",
        search_results={"edition:501": [{"book": _search_book_payload()}]},
    )

    result = run_bookshelf_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_bookshelf=True,
        push_bookshelf=True,
        dry_run=True,
        approval_mode="shortlist-only",
        requested_mode="book",
        bookshelf_root_folder="/books",
        bookshelf_quality_profile_id=1,
        bookshelf_metadata_profile_id=2,
        bookshelf_client=bookshelf_client,
    )

    actions = [row["action"] for row in result.push_log_rows]

    assert bookshelf_client.added_books == []
    assert actions == ["dry-run exported", "looked_up", "matched", "search_skipped"]


def test_bookshelf_push_skips_duplicates_when_search_result_has_local_id() -> None:
    hardcover_client = FakeHardcoverClient({101: _book()}, {101: [_edition()]})
    bookshelf_client = FakeBookshelfClient(
        metadata_source="https://api.hardcover.app/v1",
        search_results={"edition:501": [{"book": _search_book_payload(local_id=44)}]},
    )

    result = run_bookshelf_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_bookshelf=True,
        push_bookshelf=True,
        dry_run=False,
        approval_mode="shortlist-only",
        requested_mode="book",
        bookshelf_root_folder="/books",
        bookshelf_quality_profile_id=1,
        bookshelf_metadata_profile_id=2,
        bookshelf_client=bookshelf_client,
    )

    assert bookshelf_client.added_books == []
    assert result.push_log_rows[-1]["action"] == "duplicate_skipped"
    assert result.push_log_rows[-1]["bookshelf_resource_id"] == "44"


def test_bookshelf_push_skips_ambiguous_title_author_matches() -> None:
    hardcover_client = FakeHardcoverClient(
        {101: _book()},
        {101: [_edition(isbn13="", asin="")]},
    )
    ambiguous_results = [
        {
            "book": {
                "id": 0,
                "foreignBookId": "999",
                "foreignEditionId": "1001",
                "title": "The Last Kingdom",
                "author": {"authorName": "Bernard Cornwell"},
                "editions": [{"foreignEditionId": "1001", "isbn13": "", "asin": "", "monitored": True}],
            }
        },
        {
            "book": {
                "id": 0,
                "foreignBookId": "998",
                "foreignEditionId": "1002",
                "title": "The Last Kingdom",
                "author": {"authorName": "Bernard Cornwell"},
                "editions": [{"foreignEditionId": "1002", "isbn13": "", "asin": "", "monitored": True}],
            }
        },
    ]
    bookshelf_client = FakeBookshelfClient(
        metadata_source="https://metadata.example.invalid",
        search_results={"The Last Kingdom Bernard Cornwell": ambiguous_results},
    )

    result = run_bookshelf_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_bookshelf=True,
        push_bookshelf=True,
        dry_run=False,
        approval_mode="shortlist-only",
        requested_mode="book",
        bookshelf_root_folder="/books",
        bookshelf_quality_profile_id=1,
        bookshelf_metadata_profile_id=2,
        bookshelf_client=bookshelf_client,
    )

    assert bookshelf_client.added_books == []
    assert result.push_log_rows[-1]["action"] == "ambiguous_skipped"
    assert "multiple strong Bookshelf book matches" in result.push_log_rows[-1]["reason"]


def test_discovery_outputs_write_bookshelf_artifacts_with_stable_headers(tmp_path) -> None:
    candidates = [_candidate()]
    bookshelf_result = BookshelfIntegrationResult(
        queue_rows=[
            {
                "row_id": "1",
                "source_row_number": "1",
                "approval_mode": "shortlist-only",
                "approval_bucket": "shortlist",
                "discovery_bucket": "missing_series",
                "discovery_priority_bucket": "shortlist",
                "shortlist_reason": "ok",
                "eligible_for_shortlist_boolean": True,
                "bookshelf_requested_mode": "book",
                "bookshelf_target_kind": "book",
                "display_title": "The Last Kingdom",
                "display_authors": "Bernard Cornwell",
                "display_series": "The Saxon Stories",
                "owned_author_names": "Bernard Cornwell",
                "gap_kind": "",
                "hardcover-id": "101",
                "hardcover-slug": "the-last-kingdom",
                "hardcover-edition": "501",
                "isbn13": "9780000000001",
                "asin": "B000TEST01",
                "preferred_edition_language": "English",
                "preferred_edition_format_normalized": "ebook",
                "direct_edition_term": "edition:501",
                "direct_work_term": "work:101",
                "isbn_term": "isbn:9780000000001",
                "asin_term": "asin:B000TEST01",
                "title_author_term": "The Last Kingdom Bernard Cornwell",
            }
        ],
        push_log_rows=[
            {
                "log_index": "1",
                "row_id": "1",
                "source_row_number": "1",
                "approval_mode": "shortlist-only",
                "approval_bucket": "shortlist",
                "discovery_bucket": "missing_series",
                "bookshelf_target_kind": "book",
                "display_title": "The Last Kingdom",
                "display_authors": "Bernard Cornwell",
                "hardcover-id": "101",
                "hardcover-slug": "the-last-kingdom",
                "hardcover-edition": "501",
                "metadata_backend": "hardcover",
                "metadata_source": "https://api.hardcover.app/v1",
                "lookup_strategy": "",
                "lookup_term": "",
                "match_confidence": "",
                "ambiguity_status": "",
                "action": "exported",
                "bookshelf_resource_type": "",
                "bookshelf_resource_id": "",
                "bookshelf_foreign_book_id": "",
                "bookshelf_foreign_edition_id": "",
                "bookshelf_foreign_author_id": "",
                "dry_run": False,
                "reason": "queued for Bookshelf integration",
            }
        ],
        summary_lines=["# Bookshelf summary", "", "- Queue rows written: **1**"],
        metadata_backend="hardcover",
        metadata_source="https://api.hardcover.app/v1",
    )

    output_paths = build_discovery_outputs(candidates, tmp_path, bookshelf_result=bookshelf_result)

    assert output_paths.bookshelf_queue is not None
    assert output_paths.bookshelf_queue_json is not None
    assert output_paths.bookshelf_push_log is not None
    assert output_paths.bookshelf_summary is not None
    assert output_paths.bookshelf_queue.exists()
    assert output_paths.bookshelf_queue_json.exists()
    assert output_paths.bookshelf_push_log.exists()
    assert output_paths.bookshelf_summary.exists()
    summary_text = output_paths.summary.read_text(encoding="utf-8")
    assert "## Bookshelf" in summary_text

    with output_paths.bookshelf_queue.open(newline="", encoding="utf-8") as handle:
        queue_header = next(csv.reader(handle))
    with output_paths.bookshelf_push_log.open(newline="", encoding="utf-8") as handle:
        push_log_header = next(csv.reader(handle))

    assert queue_header[:6] == [
        "row_id",
        "source_row_number",
        "approval_mode",
        "approval_bucket",
        "discovery_bucket",
        "discovery_priority_bucket",
    ]
    assert push_log_header[:6] == [
        "log_index",
        "row_id",
        "source_row_number",
        "approval_mode",
        "approval_bucket",
        "discovery_bucket",
    ]
