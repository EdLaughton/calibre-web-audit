from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Tuple

from .bookshelf_client import BookshelfApiError, BookshelfClient
from .discovery_export_common import (
    discovery_row_is_export_eligible,
    find_preferred_edition,
    ordered_export_row,
    row_hardcover_slug,
    to_bool,
)
from .matching import author_similarity, bare_title_similarity, primary_author_overlap, title_similarity
from .models import HardcoverBook, HardcoverEdition
from .text_normalization import canonical_author_set, normalize_search_query_title, norm, primary_author

BOOKSHELF_QUEUE_COLUMNS = [
    "row_id",
    "source_row_number",
    "approval_mode",
    "approval_bucket",
    "discovery_bucket",
    "discovery_priority_bucket",
    "shortlist_reason",
    "eligible_for_shortlist_boolean",
    "bookshelf_requested_mode",
    "bookshelf_target_kind",
    "display_title",
    "display_authors",
    "display_series",
    "owned_author_names",
    "gap_kind",
    "hardcover-id",
    "hardcover-slug",
    "hardcover-edition",
    "isbn13",
    "asin",
    "preferred_edition_language",
    "preferred_edition_format_normalized",
    "direct_edition_term",
    "direct_work_term",
    "isbn_term",
    "asin_term",
    "title_author_term",
]

BOOKSHELF_PUSH_LOG_COLUMNS = [
    "log_index",
    "row_id",
    "source_row_number",
    "approval_mode",
    "approval_bucket",
    "discovery_bucket",
    "bookshelf_target_kind",
    "display_title",
    "display_authors",
    "hardcover-id",
    "hardcover-slug",
    "hardcover-edition",
    "metadata_backend",
    "metadata_source",
    "lookup_strategy",
    "lookup_term",
    "match_confidence",
    "ambiguity_status",
    "action",
    "bookshelf_resource_type",
    "bookshelf_resource_id",
    "bookshelf_foreign_book_id",
    "bookshelf_foreign_edition_id",
    "bookshelf_foreign_author_id",
    "dry_run",
    "reason",
]

HARDCOVER_METADATA_HINTS = (
    "api.hardcover.app",
    "hardcover.app",
    "hardcover",
)
@dataclass(frozen=True)
class BookshelfIntegrationResult:
    queue_rows: list[dict[str, Any]]
    push_log_rows: list[dict[str, Any]]
    summary_lines: list[str]
    metadata_backend: str = "not_checked"
    metadata_source: str = ""


@dataclass(frozen=True)
class LookupAttempt:
    strategy: str
    term: str


@dataclass(frozen=True)
class MatchDecision:
    status: str
    confidence: str
    ambiguity_status: str
    reason: str
    resource_type: str = ""
    payload: Optional[dict[str, Any]] = None


def detect_bookshelf_metadata_backend(metadata_source: str) -> tuple[str, bool, str]:
    value = str(metadata_source or "").strip()
    if not value:
        return "unknown", False, "metadataSource missing or empty"
    lowered = value.lower()
    if any(hint in lowered for hint in HARDCOVER_METADATA_HINTS):
        return "hardcover", True, "metadataSource clearly references Hardcover"
    return "non_hardcover", False, "metadataSource does not clearly reference Hardcover"


def build_bookshelf_queue(
    candidates: Sequence[Mapping[str, Any]],
    *,
    hardcover_client: Any,
    approval_mode: str = "shortlist-only",
    requested_mode: str = "book",
    verbose: bool = False,
) -> list[dict[str, Any]]:
    book_ids = sorted({int(row.get("display_book_id") or 0) for row in candidates if int(row.get("display_book_id") or 0)})
    books_by_id: dict[int, HardcoverBook] = {}
    editions_by_book_id: dict[int, list[HardcoverEdition]] = {}
    if book_ids:
        books_by_id, editions_by_book_id = hardcover_client.fetch_books_and_editions_for_books(
            book_ids,
            verbose=verbose,
            progress_label="bookshelf-enrich",
        )

    queue_rows: list[dict[str, Any]] = []
    for index, row in enumerate(candidates, start=1):
        approval_allowed, approval_reason = row_is_bookshelf_eligible(row, approval_mode=approval_mode)
        if not approval_allowed:
            continue
        display_book_id = str(row.get("display_book_id") or "").strip()
        preferred_edition_id = str(row.get("preferred_edition_id") or "").strip()
        book_id = int(display_book_id or 0)
        preferred_edition = find_preferred_edition(
            editions_by_book_id.get(book_id) or [],
            preferred_edition_id=preferred_edition_id,
        )
        bookshelf_target_kind = resolve_bookshelf_target_kind(row, requested_mode=requested_mode)
        row_payload = {
            "row_id": str(len(queue_rows) + 1),
            "source_row_number": str(index),
            "approval_mode": approval_mode,
            "approval_bucket": str(row.get("discovery_priority_bucket") or "unknown"),
            "discovery_bucket": str(row.get("discovery_bucket") or ""),
            "discovery_priority_bucket": str(row.get("discovery_priority_bucket") or ""),
            "shortlist_reason": str(row.get("shortlist_reason") or approval_reason or ""),
            "eligible_for_shortlist_boolean": to_bool(row.get("eligible_for_shortlist_boolean")),
            "bookshelf_requested_mode": requested_mode,
            "bookshelf_target_kind": bookshelf_target_kind,
            "display_title": str(row.get("display_title") or row.get("title") or ""),
            "display_authors": str(row.get("display_authors") or row.get("authors") or ""),
            "display_series": str(row.get("display_series") or row.get("series_name") or ""),
            "owned_author_names": str(row.get("owned_author_names") or ""),
            "gap_kind": str(row.get("gap_kind") or row.get("reason") or ""),
            "hardcover-id": display_book_id,
            "hardcover-slug": row_hardcover_slug(row, books_by_id.get(book_id)),
            "hardcover-edition": preferred_edition_id,
            "isbn13": preferred_edition.isbn_13 if preferred_edition else "",
            "asin": preferred_edition.asin if preferred_edition else "",
            "preferred_edition_language": str(
                (preferred_edition.language if preferred_edition else row.get("preferred_edition_language")) or ""
            ),
            "preferred_edition_format_normalized": str(
                row.get("preferred_edition_format_normalized") or ""
            ),
            "direct_edition_term": f"edition:{preferred_edition_id}" if preferred_edition_id else "",
            "direct_work_term": f"work:{display_book_id}" if display_book_id else "",
            "isbn_term": f"isbn:{preferred_edition.isbn_13}" if preferred_edition and preferred_edition.isbn_13 else "",
            "asin_term": f"asin:{preferred_edition.asin}" if preferred_edition and preferred_edition.asin else "",
            "title_author_term": build_title_author_lookup_term(
                str(row.get("display_title") or row.get("title") or ""),
                str(row.get("display_authors") or row.get("authors") or ""),
            ),
        }
        queue_rows.append(ordered_export_row(row_payload, BOOKSHELF_QUEUE_COLUMNS))
    return queue_rows


def run_bookshelf_integration(
    candidates: Sequence[Mapping[str, Any]],
    *,
    hardcover_client: Any,
    export_bookshelf: bool,
    push_bookshelf: bool,
    dry_run: bool,
    approval_mode: str,
    requested_mode: str,
    bookshelf_url: Optional[str] = None,
    bookshelf_api_key: Optional[str] = None,
    bookshelf_root_folder: Optional[str] = None,
    bookshelf_quality_profile_id: Optional[int] = None,
    bookshelf_metadata_profile_id: Optional[int] = None,
    bookshelf_trigger_search: bool = False,
    verbose: bool = False,
    bookshelf_client: Optional[Any] = None,
) -> BookshelfIntegrationResult:
    if not export_bookshelf and not push_bookshelf:
        return BookshelfIntegrationResult(queue_rows=[], push_log_rows=[], summary_lines=[])

    queue_rows = build_bookshelf_queue(
        candidates,
        hardcover_client=hardcover_client,
        approval_mode=approval_mode,
        requested_mode=requested_mode,
        verbose=verbose,
    )
    export_action = "dry-run exported" if dry_run else "exported"
    push_log_rows: list[dict[str, Any]] = []
    metadata_backend = "not_checked"
    metadata_source = ""
    for queue_row in queue_rows:
        push_log_rows.append(
            build_bookshelf_push_log_row(
                queue_row,
                log_index=len(push_log_rows) + 1,
                dry_run=dry_run,
                action=export_action,
                reason="queued for Bookshelf integration",
            )
        )

    if push_bookshelf and queue_rows:
        client = bookshelf_client or BookshelfClient(
            base_url=str(bookshelf_url or ""),
            api_key=str(bookshelf_api_key or ""),
        )
        try:
            development_config = client.get_development_config()
            metadata_source = str(development_config.get("metadataSource") or "")
            metadata_backend, hardcover_direct_ids_allowed, backend_reason = detect_bookshelf_metadata_backend(
                metadata_source
            )
        except Exception as exc:
            metadata_backend = "setup_failed"
            failure_reason = f"Bookshelf setup failed before lookup: {exc}"
            for queue_row in queue_rows:
                push_log_rows.append(
                    build_bookshelf_push_log_row(
                        queue_row,
                        log_index=len(push_log_rows) + 1,
                        dry_run=dry_run,
                        action="add_failed",
                        metadata_backend=metadata_backend,
                        metadata_source=metadata_source,
                        reason=failure_reason,
                    )
                )
            return BookshelfIntegrationResult(
                queue_rows=queue_rows,
                push_log_rows=push_log_rows,
                summary_lines=build_bookshelf_summary_lines(
                    queue_rows=queue_rows,
                    push_log_rows=push_log_rows,
                    export_bookshelf=export_bookshelf,
                    push_bookshelf=push_bookshelf,
                    dry_run=dry_run,
                    approval_mode=approval_mode,
                    requested_mode=requested_mode,
                    trigger_search=bookshelf_trigger_search,
                    metadata_backend=metadata_backend,
                    metadata_source=metadata_source,
                ),
                metadata_backend=metadata_backend,
                metadata_source=metadata_source,
            )

        for queue_row in queue_rows:
            push_log_rows.extend(
                process_bookshelf_queue_row(
                    queue_row,
                    client=client,
                    metadata_backend=metadata_backend,
                    metadata_source=metadata_source,
                    hardcover_direct_ids_allowed=hardcover_direct_ids_allowed,
                    dry_run=dry_run,
                    root_folder_path=str(bookshelf_root_folder or ""),
                    quality_profile_id=int(bookshelf_quality_profile_id or 0),
                    metadata_profile_id=int(bookshelf_metadata_profile_id or 0),
                    trigger_search=bookshelf_trigger_search,
                    initial_log_index=len(push_log_rows) + 1,
                )
            )

    summary_lines = build_bookshelf_summary_lines(
        queue_rows=queue_rows,
        push_log_rows=push_log_rows,
        export_bookshelf=export_bookshelf,
        push_bookshelf=push_bookshelf,
        dry_run=dry_run,
        approval_mode=approval_mode,
        requested_mode=requested_mode,
        trigger_search=bookshelf_trigger_search,
        metadata_backend=metadata_backend,
        metadata_source=metadata_source,
    )
    return BookshelfIntegrationResult(
        queue_rows=queue_rows,
        push_log_rows=push_log_rows,
        summary_lines=summary_lines,
        metadata_backend=metadata_backend,
        metadata_source=metadata_source,
    )


def row_is_bookshelf_eligible(
    row: Mapping[str, Any],
    *,
    approval_mode: str,
) -> tuple[bool, str]:
    return discovery_row_is_export_eligible(row, approval_mode=approval_mode)


def resolve_bookshelf_target_kind(row: Mapping[str, Any], *, requested_mode: str) -> str:
    if requested_mode in {"book", "author"}:
        return requested_mode
    has_specific_book = bool(str(row.get("preferred_edition_id") or row.get("hardcover-edition") or "").strip()) or bool(
        str(row.get("display_book_id") or row.get("hardcover-id") or "").strip()
    )
    return "book" if has_specific_book else "author"


def build_bookshelf_lookup_attempts(
    queue_row: Mapping[str, Any],
    *,
    metadata_backend: str,
    hardcover_direct_ids_allowed: bool,
) -> list[LookupAttempt]:
    attempts: list[LookupAttempt] = []
    if hardcover_direct_ids_allowed and str(queue_row.get("direct_edition_term") or "").strip():
        attempts.append(LookupAttempt("direct_edition_id", str(queue_row.get("direct_edition_term") or "").strip()))
    if hardcover_direct_ids_allowed and str(queue_row.get("direct_work_term") or "").strip():
        attempts.append(LookupAttempt("direct_work_id", str(queue_row.get("direct_work_term") or "").strip()))
    if str(queue_row.get("isbn_term") or "").strip():
        attempts.append(LookupAttempt("isbn", str(queue_row.get("isbn_term") or "").strip()))
    if str(queue_row.get("asin_term") or "").strip():
        attempts.append(LookupAttempt("asin", str(queue_row.get("asin_term") or "").strip()))
    title_author_term = str(queue_row.get("title_author_term") or "").strip()
    if title_author_term:
        attempts.append(LookupAttempt("title_author", title_author_term))
    return attempts


def process_bookshelf_queue_row(
    queue_row: Mapping[str, Any],
    *,
    client: Any,
    metadata_backend: str,
    metadata_source: str,
    hardcover_direct_ids_allowed: bool,
    dry_run: bool,
    root_folder_path: str,
    quality_profile_id: int,
    metadata_profile_id: int,
    trigger_search: bool,
    initial_log_index: int,
) -> list[dict[str, Any]]:
    log_rows: list[dict[str, Any]] = []
    lookup_attempts = build_bookshelf_lookup_attempts(
        queue_row,
        metadata_backend=metadata_backend,
        hardcover_direct_ids_allowed=hardcover_direct_ids_allowed,
    )
    target_kind = str(queue_row.get("bookshelf_target_kind") or "book")
    matched_payload: Optional[dict[str, Any]] = None
    matched_resource_type = ""
    matched_resource_id = ""
    matched_foreign_book_id = ""
    matched_foreign_edition_id = ""
    matched_foreign_author_id = ""

    for attempt in lookup_attempts:
        log_rows.append(
            build_bookshelf_push_log_row(
                queue_row,
                log_index=initial_log_index + len(log_rows),
                dry_run=dry_run,
                action="looked_up",
                metadata_backend=metadata_backend,
                metadata_source=metadata_source,
                lookup_strategy=attempt.strategy,
                lookup_term=attempt.term,
                reason="lookup request sent to Bookshelf search",
            )
        )
        try:
            search_results = client.search(attempt.term)
        except Exception as exc:
            log_rows.append(
                build_bookshelf_push_log_row(
                    queue_row,
                    log_index=initial_log_index + len(log_rows),
                    dry_run=dry_run,
                    action="add_failed",
                    metadata_backend=metadata_backend,
                    metadata_source=metadata_source,
                    lookup_strategy=attempt.strategy,
                    lookup_term=attempt.term,
                    reason=f"Bookshelf lookup failed: {exc}",
                )
            )
            return log_rows

        decision = select_bookshelf_match(
            queue_row,
            search_results=search_results,
            target_kind=target_kind,
            lookup_strategy=attempt.strategy,
        )

        if decision.status == "no_match":
            log_rows[-1]["reason"] = decision.reason
            continue

        if decision.status == "ambiguous":
            log_rows.append(
                build_bookshelf_push_log_row(
                    queue_row,
                    log_index=initial_log_index + len(log_rows),
                    dry_run=dry_run,
                    action="ambiguous_skipped",
                    metadata_backend=metadata_backend,
                    metadata_source=metadata_source,
                    lookup_strategy=attempt.strategy,
                    lookup_term=attempt.term,
                    match_confidence=decision.confidence,
                    ambiguity_status=decision.ambiguity_status,
                    reason=decision.reason,
                )
            )
            return log_rows

        matched_payload = dict(decision.payload or {})
        matched_resource_type = decision.resource_type
        matched_resource_id = str(matched_payload.get("id") or "")
        matched_foreign_book_id = str(matched_payload.get("foreignBookId") or "")
        matched_foreign_edition_id = str(matched_payload.get("foreignEditionId") or "")
        matched_foreign_author_id = str(matched_payload.get("foreignAuthorId") or "")

        if matched_resource_id and int(matched_resource_id or 0) > 0:
            log_rows.append(
                build_bookshelf_push_log_row(
                    queue_row,
                    log_index=initial_log_index + len(log_rows),
                    dry_run=dry_run,
                    action="duplicate_skipped",
                    metadata_backend=metadata_backend,
                    metadata_source=metadata_source,
                    lookup_strategy=attempt.strategy,
                    lookup_term=attempt.term,
                    match_confidence=decision.confidence,
                    ambiguity_status=decision.ambiguity_status,
                    resource_type=matched_resource_type,
                    resource_id=matched_resource_id,
                    foreign_book_id=matched_foreign_book_id,
                    foreign_edition_id=matched_foreign_edition_id,
                    foreign_author_id=matched_foreign_author_id,
                    reason="Bookshelf search result already has a local resource ID",
                )
            )
            return log_rows

        log_rows.append(
            build_bookshelf_push_log_row(
                queue_row,
                log_index=initial_log_index + len(log_rows),
                dry_run=dry_run,
                action="matched",
                metadata_backend=metadata_backend,
                metadata_source=metadata_source,
                lookup_strategy=attempt.strategy,
                lookup_term=attempt.term,
                match_confidence=decision.confidence,
                ambiguity_status=decision.ambiguity_status,
                resource_type=matched_resource_type,
                resource_id=matched_resource_id,
                foreign_book_id=matched_foreign_book_id,
                foreign_edition_id=matched_foreign_edition_id,
                foreign_author_id=matched_foreign_author_id,
                reason=decision.reason,
            )
        )
        break

    if not matched_payload:
        return log_rows

    if dry_run:
        log_rows.append(
            build_bookshelf_push_log_row(
                queue_row,
                log_index=initial_log_index + len(log_rows),
                dry_run=True,
                action="search_skipped",
                metadata_backend=metadata_backend,
                metadata_source=metadata_source,
                resource_type=matched_resource_type,
                resource_id=matched_resource_id,
                foreign_book_id=matched_foreign_book_id,
                foreign_edition_id=matched_foreign_edition_id,
                foreign_author_id=matched_foreign_author_id,
                reason="dry-run mode: add and post-add search were not sent",
            )
        )
        return log_rows

    try:
        if target_kind == "book":
            add_payload, payload_reason = build_bookshelf_book_add_payload(
                queue_row,
                matched_payload,
                root_folder_path=root_folder_path,
                quality_profile_id=quality_profile_id,
                metadata_profile_id=metadata_profile_id,
            )
            if not add_payload:
                log_rows.append(
                    build_bookshelf_push_log_row(
                        queue_row,
                        log_index=initial_log_index + len(log_rows),
                        dry_run=False,
                        action="add_failed",
                        metadata_backend=metadata_backend,
                        metadata_source=metadata_source,
                        resource_type=matched_resource_type,
                        resource_id=matched_resource_id,
                        foreign_book_id=matched_foreign_book_id,
                        foreign_edition_id=matched_foreign_edition_id,
                        foreign_author_id=matched_foreign_author_id,
                        reason=payload_reason,
                    )
                )
                return log_rows
            added = client.add_book(add_payload)
        else:
            add_payload = build_bookshelf_author_add_payload(
                matched_payload,
                root_folder_path=root_folder_path,
                quality_profile_id=quality_profile_id,
                metadata_profile_id=metadata_profile_id,
            )
            added = client.add_author(add_payload)
    except Exception as exc:
        log_rows.append(
            build_bookshelf_push_log_row(
                queue_row,
                log_index=initial_log_index + len(log_rows),
                dry_run=False,
                action="add_failed",
                metadata_backend=metadata_backend,
                metadata_source=metadata_source,
                resource_type=matched_resource_type,
                resource_id=matched_resource_id,
                foreign_book_id=matched_foreign_book_id,
                foreign_edition_id=matched_foreign_edition_id,
                foreign_author_id=matched_foreign_author_id,
                reason=f"Bookshelf add failed: {exc}",
            )
        )
        return log_rows

    added_resource_id = str(added.get("id") or matched_resource_id)
    added_foreign_book_id = str(added.get("foreignBookId") or matched_foreign_book_id)
    added_foreign_edition_id = str(added.get("foreignEditionId") or matched_foreign_edition_id)
    added_foreign_author_id = str(added.get("foreignAuthorId") or matched_foreign_author_id)
    log_rows.append(
        build_bookshelf_push_log_row(
            queue_row,
            log_index=initial_log_index + len(log_rows),
            dry_run=False,
            action="added_to_bookshelf",
            metadata_backend=metadata_backend,
            metadata_source=metadata_source,
            resource_type=target_kind,
            resource_id=added_resource_id,
            foreign_book_id=added_foreign_book_id,
            foreign_edition_id=added_foreign_edition_id,
            foreign_author_id=added_foreign_author_id,
            reason="Bookshelf add request completed successfully",
        )
    )

    if not trigger_search:
        log_rows.append(
            build_bookshelf_push_log_row(
                queue_row,
                log_index=initial_log_index + len(log_rows),
                dry_run=False,
                action="search_skipped",
                metadata_backend=metadata_backend,
                metadata_source=metadata_source,
                resource_type=target_kind,
                resource_id=added_resource_id,
                foreign_book_id=added_foreign_book_id,
                foreign_edition_id=added_foreign_edition_id,
                foreign_author_id=added_foreign_author_id,
                reason="post-add search was not requested",
            )
        )
        return log_rows

    try:
        if target_kind == "book":
            client.enqueue_command("BookSearch", {"bookIds": [int(added_resource_id)]})
        else:
            client.enqueue_command("AuthorSearch", {"authorId": int(added_resource_id)})
        log_rows.append(
            build_bookshelf_push_log_row(
                queue_row,
                log_index=initial_log_index + len(log_rows),
                dry_run=False,
                action="search_triggered",
                metadata_backend=metadata_backend,
                metadata_source=metadata_source,
                resource_type=target_kind,
                resource_id=added_resource_id,
                foreign_book_id=added_foreign_book_id,
                foreign_edition_id=added_foreign_edition_id,
                foreign_author_id=added_foreign_author_id,
                reason="explicit post-add search command accepted by Bookshelf",
            )
        )
    except Exception as exc:
        log_rows.append(
            build_bookshelf_push_log_row(
                queue_row,
                log_index=initial_log_index + len(log_rows),
                dry_run=False,
                action="add_failed",
                metadata_backend=metadata_backend,
                metadata_source=metadata_source,
                resource_type=target_kind,
                resource_id=added_resource_id,
                foreign_book_id=added_foreign_book_id,
                foreign_edition_id=added_foreign_edition_id,
                foreign_author_id=added_foreign_author_id,
                reason=f"post-add search command failed: {exc}",
            )
        )

    return log_rows


def select_bookshelf_match(
    queue_row: Mapping[str, Any],
    *,
    search_results: Sequence[Mapping[str, Any]],
    target_kind: str,
    lookup_strategy: str,
) -> MatchDecision:
    books = [dict(item.get("book") or {}) for item in search_results if isinstance(item, Mapping) and isinstance(item.get("book"), Mapping)]
    authors = [dict(item.get("author") or {}) for item in search_results if isinstance(item, Mapping) and isinstance(item.get("author"), Mapping)]
    if target_kind == "book":
        return select_bookshelf_book_match(queue_row, books=books, lookup_strategy=lookup_strategy)
    return select_bookshelf_author_match(
        queue_row,
        books=books,
        authors=authors,
        lookup_strategy=lookup_strategy,
    )


def select_bookshelf_book_match(
    queue_row: Mapping[str, Any],
    *,
    books: Sequence[Mapping[str, Any]],
    lookup_strategy: str,
) -> MatchDecision:
    if lookup_strategy == "direct_edition_id":
        exact = [dict(book) for book in books if _book_matches_edition_id(book, str(queue_row.get("hardcover-edition") or ""))]
        return _resolve_exact_match(
            exact,
            resource_type="book",
            success_reason="exact edition match from direct Bookshelf lookup",
            ambiguous_reason="multiple Bookshelf books matched the same edition ID",
            no_match_reason="no Bookshelf book matched the direct edition ID lookup",
        )

    if lookup_strategy == "direct_work_id":
        exact = [dict(book) for book in books if str(book.get("foreignBookId") or "") == str(queue_row.get("hardcover-id") or "")]
        return _resolve_exact_match(
            exact,
            resource_type="book",
            success_reason="exact work match from direct Bookshelf lookup",
            ambiguous_reason="multiple Bookshelf books matched the same work ID",
            no_match_reason="no Bookshelf book matched the direct work ID lookup",
        )

    if lookup_strategy == "isbn":
        exact = [dict(book) for book in books if _book_matches_isbn(book, str(queue_row.get("isbn13") or ""))]
        return _resolve_exact_match(
            exact,
            resource_type="book",
            success_reason="exact ISBN match from Bookshelf lookup",
            ambiguous_reason="multiple Bookshelf books matched the same ISBN",
            no_match_reason="no Bookshelf book matched the ISBN lookup",
        )

    if lookup_strategy == "asin":
        exact = [dict(book) for book in books if _book_matches_asin(book, str(queue_row.get("asin") or ""))]
        return _resolve_exact_match(
            exact,
            resource_type="book",
            success_reason="exact ASIN match from Bookshelf lookup",
            ambiguous_reason="multiple Bookshelf books matched the same ASIN",
            no_match_reason="no Bookshelf book matched the ASIN lookup",
        )

    strong = []
    for book in books:
        title_score = max(
            title_similarity(str(queue_row.get("display_title") or ""), str(book.get("title") or "")),
            bare_title_similarity(str(queue_row.get("display_title") or ""), str(book.get("title") or "")),
        )
        author_score = _book_author_similarity(book, str(queue_row.get("display_authors") or ""))
        if title_score >= 0.96 and author_score >= 0.5:
            strong.append((title_score, author_score, dict(book)))
    if not strong:
        return MatchDecision(
            status="no_match",
            confidence="none",
            ambiguity_status="no_match",
            reason="title+author lookup did not produce a single strong Bookshelf book match",
        )
    if len(strong) > 1:
        return MatchDecision(
            status="ambiguous",
            confidence="medium",
            ambiguity_status="ambiguous",
            reason="title+author lookup returned multiple strong Bookshelf book matches",
        )
    title_score, author_score, payload = strong[0]
    confidence = "high" if title_score >= 0.995 and author_score >= 0.99 else "medium"
    return MatchDecision(
        status="matched",
        confidence=confidence,
        ambiguity_status="exact",
        reason="title+author lookup produced a single strong Bookshelf book match",
        resource_type="book",
        payload=payload,
    )


def select_bookshelf_author_match(
    queue_row: Mapping[str, Any],
    *,
    books: Sequence[Mapping[str, Any]],
    authors: Sequence[Mapping[str, Any]],
    lookup_strategy: str,
) -> MatchDecision:
    if lookup_strategy in {"direct_edition_id", "direct_work_id", "isbn", "asin"}:
        book_decision = select_bookshelf_book_match(queue_row, books=books, lookup_strategy=lookup_strategy)
        if book_decision.status != "matched":
            return book_decision if book_decision.status != "no_match" else MatchDecision(
                status="no_match",
                confidence=book_decision.confidence,
                ambiguity_status=book_decision.ambiguity_status,
                reason=book_decision.reason,
            )
        if len(authors) == 1:
            return MatchDecision(
                status="matched",
                confidence=book_decision.confidence,
                ambiguity_status="exact",
                reason="specific-book lookup returned a single Bookshelf author result",
                resource_type="author",
                payload=dict(authors[0]),
            )
        if len(authors) > 1:
            return MatchDecision(
                status="ambiguous",
                confidence="medium",
                ambiguity_status="ambiguous",
                reason="specific-book lookup returned multiple Bookshelf author results",
            )
        return MatchDecision(
            status="no_match",
            confidence="none",
            ambiguity_status="no_match",
            reason="specific-book lookup did not return a Bookshelf author result",
        )

    target_authors = canonical_author_set(str(queue_row.get("display_authors") or ""))
    matched_authors = []
    for author in authors:
        author_name = str(author.get("authorName") or "")
        if not author_name:
            continue
        if canonical_author_set(author_name) == target_authors:
            matched_authors.append(dict(author))
            continue
        if primary_author_overlap(str(queue_row.get("display_authors") or ""), author_name):
            matched_authors.append(dict(author))
            continue
        if author_similarity(str(queue_row.get("display_authors") or ""), author_name) >= 0.99:
            matched_authors.append(dict(author))
    if not matched_authors:
        return MatchDecision(
            status="no_match",
            confidence="none",
            ambiguity_status="no_match",
            reason="title+author lookup did not produce a safe Bookshelf author match",
        )
    if len(matched_authors) > 1:
        return MatchDecision(
            status="ambiguous",
            confidence="medium",
            ambiguity_status="ambiguous",
            reason="title+author lookup returned multiple safe Bookshelf author matches",
        )
    return MatchDecision(
        status="matched",
        confidence="medium",
        ambiguity_status="exact",
        reason="title+author lookup produced a single safe Bookshelf author match",
        resource_type="author",
        payload=matched_authors[0],
    )


def build_bookshelf_book_add_payload(
    queue_row: Mapping[str, Any],
    matched_payload: Mapping[str, Any],
    *,
    root_folder_path: str,
    quality_profile_id: int,
    metadata_profile_id: int,
) -> tuple[Optional[dict[str, Any]], str]:
    payload = _json_copy(matched_payload)
    author = dict(payload.get("author") or {})
    editions = [dict(edition) for edition in (payload.get("editions") or []) if isinstance(edition, Mapping)]
    if not author:
        return None, "Bookshelf lookup result did not include an author payload"
    if not editions:
        return None, "Bookshelf lookup result did not include editions"

    preferred_edition_id = str(queue_row.get("hardcover-edition") or "")
    isbn13 = str(queue_row.get("isbn13") or "")
    asin = str(queue_row.get("asin") or "")
    monitored_edition = _choose_monitored_edition(
        editions,
        preferred_edition_id=preferred_edition_id,
        isbn13=isbn13,
        asin=asin,
    )
    if not monitored_edition:
        return None, "Bookshelf lookup result did not expose a safe monitored edition for add"

    for edition in editions:
        edition["monitored"] = str(edition.get("foreignEditionId") or "") == str(
            monitored_edition.get("foreignEditionId") or ""
        )

    author["qualityProfileId"] = quality_profile_id
    author["metadataProfileId"] = metadata_profile_id
    author["rootFolderPath"] = root_folder_path
    author["monitored"] = False
    author["monitorNewItems"] = "none"
    author["addOptions"] = {
        "monitor": "none",
        "monitored": False,
        "searchForMissingBooks": False,
    }

    payload["author"] = author
    payload["editions"] = editions
    payload["monitored"] = True
    payload["addOptions"] = {
        "addType": "manual",
        "searchForNewBook": False,
    }
    payload["foreignEditionId"] = str(monitored_edition.get("foreignEditionId") or "")
    return payload, ""


def build_bookshelf_author_add_payload(
    matched_payload: Mapping[str, Any],
    *,
    root_folder_path: str,
    quality_profile_id: int,
    metadata_profile_id: int,
) -> dict[str, Any]:
    payload = _json_copy(matched_payload)
    payload["qualityProfileId"] = quality_profile_id
    payload["metadataProfileId"] = metadata_profile_id
    payload["rootFolderPath"] = root_folder_path
    payload["monitored"] = False
    payload["monitorNewItems"] = "none"
    payload["addOptions"] = {
        "monitor": "none",
        "monitored": False,
        "searchForMissingBooks": False,
    }
    return payload


def build_bookshelf_push_log_row(
    queue_row: Mapping[str, Any],
    *,
    log_index: int,
    dry_run: bool,
    action: str,
    metadata_backend: str = "",
    metadata_source: str = "",
    lookup_strategy: str = "",
    lookup_term: str = "",
    match_confidence: str = "",
    ambiguity_status: str = "",
    resource_type: str = "",
    resource_id: str = "",
    foreign_book_id: str = "",
    foreign_edition_id: str = "",
    foreign_author_id: str = "",
    reason: str = "",
) -> dict[str, Any]:
    return ordered_export_row(
        {
            "log_index": str(log_index),
            "row_id": str(queue_row.get("row_id") or ""),
            "source_row_number": str(queue_row.get("source_row_number") or ""),
            "approval_mode": str(queue_row.get("approval_mode") or ""),
            "approval_bucket": str(queue_row.get("approval_bucket") or ""),
            "discovery_bucket": str(queue_row.get("discovery_bucket") or ""),
            "bookshelf_target_kind": str(queue_row.get("bookshelf_target_kind") or ""),
            "display_title": str(queue_row.get("display_title") or ""),
            "display_authors": str(queue_row.get("display_authors") or ""),
            "hardcover-id": str(queue_row.get("hardcover-id") or ""),
            "hardcover-slug": str(queue_row.get("hardcover-slug") or ""),
            "hardcover-edition": str(queue_row.get("hardcover-edition") or ""),
            "metadata_backend": metadata_backend,
            "metadata_source": metadata_source,
            "lookup_strategy": lookup_strategy,
            "lookup_term": lookup_term,
            "match_confidence": match_confidence,
            "ambiguity_status": ambiguity_status,
            "action": action,
            "bookshelf_resource_type": resource_type,
            "bookshelf_resource_id": resource_id,
            "bookshelf_foreign_book_id": foreign_book_id,
            "bookshelf_foreign_edition_id": foreign_edition_id,
            "bookshelf_foreign_author_id": foreign_author_id,
            "dry_run": dry_run,
            "reason": reason,
        },
        BOOKSHELF_PUSH_LOG_COLUMNS,
    )


def build_bookshelf_summary_lines(
    *,
    queue_rows: Sequence[Mapping[str, Any]],
    push_log_rows: Sequence[Mapping[str, Any]],
    export_bookshelf: bool,
    push_bookshelf: bool,
    dry_run: bool,
    approval_mode: str,
    requested_mode: str,
    trigger_search: bool,
    metadata_backend: str,
    metadata_source: str,
) -> list[str]:
    action_counts = Counter(str(row.get("action") or "unknown") for row in push_log_rows)
    summary_lines = [
        "# Bookshelf summary",
        "",
        f"- Queue rows written: **{len(queue_rows)}**",
        f"- Export requested: **{'yes' if export_bookshelf else 'no'}**",
        f"- Push requested: **{'yes' if push_bookshelf else 'no'}**",
        f"- Dry run: **{'yes' if dry_run else 'no'}**",
        f"- Approval mode: **{approval_mode}**",
        f"- Bookshelf mode: **{requested_mode}**",
        f"- Trigger post-add search: **{'yes' if trigger_search else 'no'}**",
        f"- Metadata backend: **{metadata_backend or 'not_checked'}**",
    ]
    if metadata_source:
        summary_lines.append(f"- metadataSource: `{metadata_source}`")
    summary_lines.extend(["", "## Action counts"])
    if action_counts:
        for action, count in sorted(action_counts.items(), key=lambda item: (-item[1], item[0])):
            summary_lines.append(f"- {action}: **{count}**")
    else:
        summary_lines.append("- No Bookshelf queue or push actions were recorded.")
    summary_lines.extend(
        [
            "",
            "## Files",
            "- bookshelf_queue.csv - filtered Bookshelf queue derived from discovery rows",
            "- bookshelf_queue.json - JSON form of the same Bookshelf queue",
            "- bookshelf_push_log.csv - step-by-step Bookshelf export and push log",
        ]
    )
    return summary_lines


def build_title_author_lookup_term(title: str, authors: str) -> str:
    normalized_title = normalize_search_query_title(title)
    author_name = primary_author(authors) or authors
    author_name = " ".join(part for part in author_name.split() if part)
    return " ".join(part for part in [normalized_title, author_name] if part).strip()


def _resolve_exact_match(
    matches: Sequence[Mapping[str, Any]],
    *,
    resource_type: str,
    success_reason: str,
    ambiguous_reason: str,
    no_match_reason: str,
) -> MatchDecision:
    if not matches:
        return MatchDecision(status="no_match", confidence="none", ambiguity_status="no_match", reason=no_match_reason)
    if len(matches) > 1:
        return MatchDecision(status="ambiguous", confidence="high", ambiguity_status="ambiguous", reason=ambiguous_reason)
    return MatchDecision(
        status="matched",
        confidence="high",
        ambiguity_status="exact",
        reason=success_reason,
        resource_type=resource_type,
        payload=dict(matches[0]),
    )


def _book_matches_edition_id(book: Mapping[str, Any], edition_id: str) -> bool:
    target = str(edition_id or "").strip()
    if not target:
        return False
    if str(book.get("foreignEditionId") or "") == target:
        return True
    for edition in book.get("editions") or []:
        if isinstance(edition, Mapping) and str(edition.get("foreignEditionId") or "") == target:
            return True
    return False


def _book_matches_isbn(book: Mapping[str, Any], isbn13: str) -> bool:
    target = str(isbn13 or "").strip()
    if not target:
        return False
    for edition in book.get("editions") or []:
        if isinstance(edition, Mapping) and str(edition.get("isbn13") or "") == target:
            return True
    return False


def _book_matches_asin(book: Mapping[str, Any], asin: str) -> bool:
    target = str(asin or "").strip()
    if not target:
        return False
    for edition in book.get("editions") or []:
        if isinstance(edition, Mapping) and str(edition.get("asin") or "") == target:
            return True
    return False


def _book_author_similarity(book: Mapping[str, Any], authors_text: str) -> float:
    author_payload = book.get("author") or {}
    author_name = ""
    if isinstance(author_payload, Mapping):
        author_name = str(author_payload.get("authorName") or "")
    if not author_name:
        author_name = str(book.get("authorTitle") or "")
    return author_similarity(authors_text, author_name)


def _choose_monitored_edition(
    editions: Sequence[Mapping[str, Any]],
    *,
    preferred_edition_id: str,
    isbn13: str,
    asin: str,
) -> Optional[dict[str, Any]]:
    target_edition_id = str(preferred_edition_id or "").strip()
    if target_edition_id:
        for edition in editions:
            if str(edition.get("foreignEditionId") or "") == target_edition_id:
                return dict(edition)
    if isbn13:
        for edition in editions:
            if str(edition.get("isbn13") or "") == isbn13:
                return dict(edition)
    if asin:
        for edition in editions:
            if str(edition.get("asin") or "") == asin:
                return dict(edition)
    monitored = [dict(edition) for edition in editions if bool(edition.get("monitored"))]
    if len(monitored) == 1:
        return monitored[0]
    return None


def _json_copy(payload: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload))


__all__ = [
    "BOOKSHELF_PUSH_LOG_COLUMNS",
    "BOOKSHELF_QUEUE_COLUMNS",
    "BookshelfIntegrationResult",
    "build_bookshelf_lookup_attempts",
    "build_bookshelf_push_log_row",
    "build_bookshelf_queue",
    "build_bookshelf_summary_lines",
    "build_title_author_lookup_term",
    "detect_bookshelf_metadata_backend",
    "process_bookshelf_queue_row",
    "resolve_bookshelf_target_kind",
    "row_is_bookshelf_eligible",
    "run_bookshelf_integration",
    "select_bookshelf_match",
]
