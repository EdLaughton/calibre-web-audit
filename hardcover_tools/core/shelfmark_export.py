from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

from .discovery_export_common import (
    discovery_row_is_export_eligible,
    extract_release_year,
    find_preferred_edition,
    ordered_export_row,
    row_hardcover_slug,
    to_bool,
)
from .models import HardcoverBook, HardcoverEdition
from .shelfmark_client import ShelfmarkApiError, ShelfmarkClient
from .text_normalization import normalize_search_query_title, norm, primary_author

SHELFMARK_QUEUE_COLUMNS = [
    "row_id",
    "source_row_number",
    "approval_mode",
    "approval_bucket",
    "discovery_bucket",
    "discovery_priority_bucket",
    "shortlist_reason",
    "eligible_for_shortlist_boolean",
    "display_title",
    "display_subtitle",
    "display_authors",
    "display_series",
    "series_position",
    "owned_author_names",
    "gap_kind",
    "hardcover-id",
    "hardcover-slug",
    "hardcover-edition",
    "release_year",
    "preferred_edition_language",
    "preferred_edition_format_normalized",
    "shelfmark_provider",
    "shelfmark_provider_id",
    "shelfmark_content_type",
    "shelfmark_request_level",
    "shelfmark_source",
    "shelfmark_source_url",
]

SHELFMARK_PUSH_LOG_COLUMNS = [
    "log_index",
    "row_id",
    "source_row_number",
    "approval_mode",
    "approval_bucket",
    "discovery_bucket",
    "display_title",
    "display_authors",
    "hardcover-id",
    "hardcover-slug",
    "hardcover-edition",
    "requests_enabled",
    "request_policy_mode",
    "request_policy_required_mode",
    "request_context_source",
    "request_context_level",
    "action",
    "http_status",
    "shelfmark_request_id",
    "dry_run",
    "reason",
]

SHELFMARK_RELEASE_CANDIDATE_COLUMNS = [
    "candidate_id",
    "row_id",
    "source_row_number",
    "approval_mode",
    "approval_bucket",
    "discovery_bucket",
    "discovery_priority_bucket",
    "display_title",
    "display_authors",
    "hardcover-id",
    "hardcover-slug",
    "hardcover-edition",
    "lookup_strategy",
    "lookup_term",
    "shelfmark_source_requested",
    "shelfmark_content_type_requested",
    "selection_rule",
    "format_keywords",
    "min_seeders",
    "release_index",
    "release_source",
    "release_source_id",
    "release_title",
    "release_author",
    "release_format",
    "release_content_type",
    "release_language",
    "release_size",
    "release_size_bytes",
    "release_seeders",
    "release_info_url",
    "release_download_url",
    "candidate_status",
    "rejection_reasons",
]

SHELFMARK_SELECTED_RELEASE_COLUMNS = [
    "row_id",
    "source_row_number",
    "approval_mode",
    "approval_bucket",
    "discovery_bucket",
    "discovery_priority_bucket",
    "display_title",
    "display_authors",
    "hardcover-id",
    "hardcover-slug",
    "hardcover-edition",
    "lookup_strategy",
    "lookup_attempts",
    "lookup_term",
    "shelfmark_source_requested",
    "shelfmark_content_type_requested",
    "selection_rule",
    "format_keywords",
    "min_seeders",
    "candidate_count",
    "accepted_candidate_count",
    "rejected_candidate_count",
    "selected_release_source",
    "selected_release_source_id",
    "selected_release_title",
    "selected_release_author",
    "selected_release_format",
    "selected_release_content_type",
    "selected_release_language",
    "selected_release_size",
    "selected_release_size_bytes",
    "selected_release_seeders",
    "selected_release_info_url",
    "selected_release_download_url",
    "queue_download_requested",
    "queue_download_executed",
    "dry_run",
    "reason",
]

SHELFMARK_DOWNLOAD_LOG_COLUMNS = [
    "log_index",
    "row_id",
    "source_row_number",
    "approval_mode",
    "approval_bucket",
    "discovery_bucket",
    "display_title",
    "display_authors",
    "hardcover-id",
    "hardcover-slug",
    "hardcover-edition",
    "lookup_strategy",
    "lookup_term",
    "shelfmark_source_requested",
    "shelfmark_content_type_requested",
    "selection_rule",
    "candidate_count",
    "accepted_candidate_count",
    "release_source",
    "release_source_id",
    "release_title",
    "release_format",
    "release_content_type",
    "release_seeders",
    "release_size_bytes",
    "action",
    "http_status",
    "dry_run",
    "reason",
]

DEFAULT_SHELFMARK_REQUEST_SOURCE = "*"
DEFAULT_REQUEST_CONTENT_TYPE = "ebook"
DEFAULT_REQUEST_LEVEL = "book"
DEFAULT_FORMAT_PRIORITY = {
    "ebook": ("epub", "kepub", "azw3", "mobi", "pdf", "cbz", "cbr"),
    "audiobook": ("m4b", "mp3", "m4a", "aac", "flac"),
}


@dataclass(frozen=True)
class ShelfmarkIntegrationResult:
    request_workflow_enabled: bool = False
    release_workflow_enabled: bool = False
    queue_rows: list[dict[str, Any]] = field(default_factory=list)
    push_log_rows: list[dict[str, Any]] = field(default_factory=list)
    release_candidate_rows: list[dict[str, Any]] = field(default_factory=list)
    selected_release_rows: list[dict[str, Any]] = field(default_factory=list)
    download_log_rows: list[dict[str, Any]] = field(default_factory=list)
    summary_lines: list[str] = field(default_factory=list)
    requests_enabled: bool = False
    request_policy_mode: str = ""


@dataclass(frozen=True)
class LookupAttempt:
    strategy: str
    term: str
    params: Mapping[str, Any]


def build_shelfmark_queue(
    candidates: Sequence[Mapping[str, Any]],
    *,
    hardcover_client: Any,
    approval_mode: str = "shortlist-only",
    verbose: bool = False,
) -> list[dict[str, Any]]:
    base_rows = _build_shelfmark_base_rows(
        candidates,
        hardcover_client=hardcover_client,
        approval_mode=approval_mode,
        verbose=verbose,
    )
    return [_build_request_queue_row(base_row) for base_row in base_rows]


def run_shelfmark_integration(
    candidates: Sequence[Mapping[str, Any]],
    *,
    hardcover_client: Any,
    export_shelfmark: bool,
    push_shelfmark: bool,
    export_shelfmark_releases: bool = False,
    push_shelfmark_download: bool = False,
    dry_run: bool,
    approval_mode: str,
    shelfmark_url: Optional[str] = None,
    shelfmark_username: Optional[str] = None,
    shelfmark_password: Optional[str] = None,
    shelfmark_note: Optional[str] = None,
    shelfmark_source: Optional[str] = None,
    shelfmark_content_type: str = "ebook",
    shelfmark_selection: str = "best",
    shelfmark_format_keywords: Sequence[str] = (),
    shelfmark_min_seeders: int = 0,
    verbose: bool = False,
    shelfmark_client: Optional[Any] = None,
) -> ShelfmarkIntegrationResult:
    if not (
        export_shelfmark
        or push_shelfmark
        or export_shelfmark_releases
        or push_shelfmark_download
    ):
        return ShelfmarkIntegrationResult()

    base_rows = _build_shelfmark_base_rows(
        candidates,
        hardcover_client=hardcover_client,
        approval_mode=approval_mode,
        verbose=verbose,
    )

    queue_rows: list[dict[str, Any]] = []
    push_log_rows: list[dict[str, Any]] = []
    release_candidate_rows: list[dict[str, Any]] = []
    selected_release_rows: list[dict[str, Any]] = []
    download_log_rows: list[dict[str, Any]] = []
    requests_enabled = False
    request_policy_mode = ""
    export_action = "dry-run exported" if dry_run else "exported"

    if export_shelfmark or push_shelfmark:
        queue_rows = [_build_request_queue_row(base_row) for base_row in base_rows]
        for queue_row in queue_rows:
            push_log_rows.append(
                _build_shelfmark_push_log_row(
                    queue_row,
                    log_index=len(push_log_rows) + 1,
                    dry_run=dry_run,
                    action=export_action,
                    reason="queued for Shelfmark request workflow",
                )
            )

    client: Any | None = None
    authenticated = False

    def get_client() -> Any:
        nonlocal client
        if client is None:
            client = shelfmark_client or ShelfmarkClient(base_url=str(shelfmark_url or ""))
        return client

    def login_if_requested() -> bool:
        nonlocal authenticated
        if authenticated:
            return True
        if not shelfmark_username or not shelfmark_password:
            return False
        get_client().login(username=str(shelfmark_username), password=str(shelfmark_password), remember_me=False)
        authenticated = True
        return True

    if push_shelfmark and queue_rows:
        try:
            login_if_requested()
            policy = get_client().get_request_policy()
            requests_enabled = to_bool(policy.get("requests_enabled"))
            defaults = policy.get("defaults") if isinstance(policy.get("defaults"), Mapping) else {}
            request_policy_mode = str(defaults.get(DEFAULT_REQUEST_CONTENT_TYPE) or policy.get(DEFAULT_REQUEST_CONTENT_TYPE) or "")
        except Exception as exc:
            for queue_row in queue_rows:
                push_log_rows.append(
                    _build_shelfmark_push_log_row(
                        queue_row,
                        log_index=len(push_log_rows) + 1,
                        dry_run=dry_run,
                        requests_enabled=requests_enabled,
                        request_policy_mode=request_policy_mode,
                        action="setup_failed",
                        reason=f"Shelfmark request setup failed: {exc}",
                    )
                )
        else:
            if not shelfmark_policy_allows_book_requests(
                requests_enabled=requests_enabled,
                request_policy_mode=request_policy_mode,
            ):
                for queue_row in queue_rows:
                    push_log_rows.append(
                        _build_shelfmark_push_log_row(
                            queue_row,
                            log_index=len(push_log_rows) + 1,
                            dry_run=dry_run,
                            requests_enabled=requests_enabled,
                            request_policy_mode=request_policy_mode,
                            request_policy_required_mode="request_book",
                            action="policy_incompatible",
                            reason="Shelfmark ebook request policy does not allow book-level requests",
                        )
                    )
            elif dry_run:
                for queue_row in queue_rows:
                    push_log_rows.append(
                        _build_shelfmark_push_log_row(
                            queue_row,
                            log_index=len(push_log_rows) + 1,
                            dry_run=dry_run,
                            requests_enabled=requests_enabled,
                            request_policy_mode=request_policy_mode,
                            request_policy_required_mode="request_book",
                            action="dry-run validated",
                            reason="Shelfmark request policy validated; live request skipped",
                        )
                    )
            else:
                for queue_row in queue_rows:
                    result_row = _submit_shelfmark_request(
                        queue_row,
                        client=get_client(),
                        shelfmark_note=shelfmark_note,
                        requests_enabled=requests_enabled,
                        request_policy_mode=request_policy_mode,
                        dry_run=dry_run,
                        log_index=len(push_log_rows) + 1,
                    )
                    push_log_rows.append(result_row)

    if export_shelfmark_releases or push_shelfmark_download:
        if shelfmark_username and shelfmark_password:
            try:
                login_if_requested()
            except Exception as exc:
                for base_row in base_rows:
                    selected_release_rows.append(
                        _build_selected_release_row(
                            base_row,
                            lookup_strategy="auth",
                            lookup_attempts="auth",
                            lookup_term="authenticated-session",
                            shelfmark_source=str(shelfmark_source or ""),
                            shelfmark_content_type=shelfmark_content_type,
                            selection_rule=shelfmark_selection,
                            format_keywords=shelfmark_format_keywords,
                            min_seeders=shelfmark_min_seeders,
                            candidate_count=0,
                            accepted_candidate_count=0,
                            selected_release=None,
                            queue_download_requested=push_shelfmark_download,
                            queue_download_executed=False,
                            dry_run=dry_run,
                            reason=f"Shelfmark login failed before release search: {exc}",
                        )
                    )
                    download_log_rows.append(
                        _build_download_log_row(
                            base_row,
                            log_index=len(download_log_rows) + 1,
                            lookup_strategy="auth",
                            lookup_term="authenticated-session",
                            shelfmark_source=str(shelfmark_source or ""),
                            shelfmark_content_type=shelfmark_content_type,
                            selection_rule=shelfmark_selection,
                            candidate_count=0,
                            accepted_candidate_count=0,
                            selected_release=None,
                            action="auth_failed",
                            dry_run=dry_run,
                            reason=f"Shelfmark login failed before release search: {exc}",
                        )
                    )
            else:
                pass
        if not selected_release_rows:
            release_candidate_rows, selected_release_rows, download_log_rows = _run_release_workflow(
                base_rows,
                client=get_client(),
                shelfmark_source=str(shelfmark_source or ""),
                shelfmark_content_type=str(shelfmark_content_type or "ebook"),
                selection_rule=str(shelfmark_selection or "best"),
                format_keywords=tuple(keyword for keyword in shelfmark_format_keywords if str(keyword).strip()),
                min_seeders=max(0, int(shelfmark_min_seeders or 0)),
                queue_download=push_shelfmark_download,
                dry_run=dry_run,
            )

    result = ShelfmarkIntegrationResult(
        request_workflow_enabled=bool(export_shelfmark or push_shelfmark),
        release_workflow_enabled=bool(export_shelfmark_releases or push_shelfmark_download),
        queue_rows=queue_rows,
        push_log_rows=push_log_rows,
        release_candidate_rows=release_candidate_rows,
        selected_release_rows=selected_release_rows,
        download_log_rows=download_log_rows,
        summary_lines=[],
        requests_enabled=requests_enabled,
        request_policy_mode=request_policy_mode,
    )
    return ShelfmarkIntegrationResult(
        **{**result.__dict__, "summary_lines": _build_shelfmark_summary_lines(result)}
    )


def shelfmark_policy_allows_book_requests(*, requests_enabled: bool, request_policy_mode: str) -> bool:
    return bool(requests_enabled) and str(request_policy_mode or "").strip().lower() == "request_book"


def _build_shelfmark_base_rows(
    candidates: Sequence[Mapping[str, Any]],
    *,
    hardcover_client: Any,
    approval_mode: str,
    verbose: bool,
) -> list[dict[str, Any]]:
    book_ids = sorted(
        {int(row.get("display_book_id") or 0) for row in candidates if int(row.get("display_book_id") or 0)}
    )
    books_by_id: dict[int, HardcoverBook] = {}
    editions_by_book_id: dict[int, list[HardcoverEdition]] = {}
    if book_ids:
        books_by_id, editions_by_book_id = hardcover_client.fetch_books_and_editions_for_books(
            book_ids,
            verbose=verbose,
            progress_label="shelfmark-enrich",
        )

    base_rows: list[dict[str, Any]] = []
    for index, row in enumerate(candidates, start=1):
        approval_allowed, approval_reason = discovery_row_is_export_eligible(row, approval_mode=approval_mode)
        if not approval_allowed:
            continue
        display_book_id = str(row.get("display_book_id") or "").strip()
        preferred_edition_id = str(row.get("preferred_edition_id") or "").strip()
        book_id = int(display_book_id or 0)
        book = books_by_id.get(book_id)
        preferred_edition = find_preferred_edition(
            editions_by_book_id.get(book_id) or [],
            preferred_edition_id=preferred_edition_id,
        )
        display_title = str(row.get("display_title") or row.get("title") or "").strip()
        display_authors = str(row.get("display_authors") or row.get("authors") or "").strip()
        display_subtitle = str(row.get("display_subtitle") or row.get("missing_subtitle") or "").strip()
        display_series = str(row.get("display_series") or row.get("series_name") or "").strip()
        base_rows.append(
            {
                "row_id": str(len(base_rows) + 1),
                "source_row_number": str(index),
                "approval_mode": approval_mode,
                "approval_bucket": str(row.get("discovery_priority_bucket") or "unknown"),
                "discovery_bucket": str(row.get("discovery_bucket") or ""),
                "discovery_priority_bucket": str(row.get("discovery_priority_bucket") or ""),
                "shortlist_reason": str(row.get("shortlist_reason") or approval_reason or ""),
                "eligible_for_shortlist_boolean": to_bool(row.get("eligible_for_shortlist_boolean")),
                "display_title": display_title,
                "display_subtitle": display_subtitle,
                "display_authors": display_authors,
                "display_series": display_series,
                "series_position": str(row.get("missing_position_display") or row.get("series_position") or ""),
                "owned_author_names": str(row.get("owned_author_names") or ""),
                "gap_kind": str(row.get("gap_kind") or row.get("reason") or ""),
                "hardcover-id": display_book_id,
                "hardcover-slug": row_hardcover_slug(row, book),
                "hardcover-edition": preferred_edition_id,
                "release_year": extract_release_year(
                    preferred_edition.release_date if preferred_edition else "",
                    book.release_date if book else "",
                ),
                "preferred_edition_language": str(
                    (preferred_edition.language if preferred_edition else row.get("preferred_edition_language")) or ""
                ),
                "preferred_edition_format_normalized": str(
                    row.get("preferred_edition_format_normalized")
                    or (preferred_edition.reading_format if preferred_edition else "")
                    or ""
                ),
                "hardcover_source_url": _build_hardcover_source_url(
                    row_hardcover_slug(row, book),
                    display_book_id,
                ),
                "search_title": normalize_search_query_title(display_title) or display_title,
                "search_author": primary_author(display_authors) or display_authors,
            }
        )
    return base_rows


def _build_request_queue_row(base_row: Mapping[str, Any]) -> dict[str, Any]:
    payload = {
        **dict(base_row),
        "shelfmark_provider": "hardcover",
        "shelfmark_provider_id": str(base_row.get("hardcover-id") or ""),
        "shelfmark_content_type": DEFAULT_REQUEST_CONTENT_TYPE,
        "shelfmark_request_level": DEFAULT_REQUEST_LEVEL,
        "shelfmark_source": DEFAULT_SHELFMARK_REQUEST_SOURCE,
        "shelfmark_source_url": str(base_row.get("hardcover_source_url") or ""),
    }
    return ordered_export_row(payload, SHELFMARK_QUEUE_COLUMNS)


def _build_hardcover_source_url(slug: str, book_id: str) -> str:
    if slug:
        return f"https://hardcover.app/books/{slug}"
    if book_id:
        return f"https://hardcover.app/books/{book_id}"
    return ""


def _build_shelfmark_push_log_row(
    queue_row: Mapping[str, Any],
    *,
    log_index: int,
    dry_run: bool,
    requests_enabled: bool = False,
    request_policy_mode: str = "",
    request_policy_required_mode: str = "",
    action: str,
    http_status: str = "",
    shelfmark_request_id: str = "",
    reason: str,
) -> dict[str, Any]:
    payload = {
        "log_index": str(log_index),
        "row_id": str(queue_row.get("row_id") or ""),
        "source_row_number": str(queue_row.get("source_row_number") or ""),
        "approval_mode": str(queue_row.get("approval_mode") or ""),
        "approval_bucket": str(queue_row.get("approval_bucket") or ""),
        "discovery_bucket": str(queue_row.get("discovery_bucket") or ""),
        "display_title": str(queue_row.get("display_title") or ""),
        "display_authors": str(queue_row.get("display_authors") or ""),
        "hardcover-id": str(queue_row.get("hardcover-id") or ""),
        "hardcover-slug": str(queue_row.get("hardcover-slug") or ""),
        "hardcover-edition": str(queue_row.get("hardcover-edition") or ""),
        "requests_enabled": bool(requests_enabled),
        "request_policy_mode": str(request_policy_mode or ""),
        "request_policy_required_mode": str(request_policy_required_mode or ""),
        "request_context_source": str(queue_row.get("shelfmark_source") or ""),
        "request_context_level": str(queue_row.get("shelfmark_request_level") or ""),
        "action": action,
        "http_status": str(http_status or ""),
        "shelfmark_request_id": str(shelfmark_request_id or ""),
        "dry_run": bool(dry_run),
        "reason": str(reason or ""),
    }
    return ordered_export_row(payload, SHELFMARK_PUSH_LOG_COLUMNS)


def _submit_shelfmark_request(
    queue_row: Mapping[str, Any],
    *,
    client: Any,
    shelfmark_note: Optional[str],
    requests_enabled: bool,
    request_policy_mode: str,
    dry_run: bool,
    log_index: int,
) -> dict[str, Any]:
    payload = build_shelfmark_request_payload(queue_row, shelfmark_note=shelfmark_note)
    try:
        response = client.create_request(payload)
    except ShelfmarkApiError as exc:
        error_code = str((exc.payload or {}).get("code") or "")
        if exc.status_code == 409 or error_code == "duplicate_pending_request":
            return _build_shelfmark_push_log_row(
                queue_row,
                log_index=log_index,
                dry_run=dry_run,
                requests_enabled=requests_enabled,
                request_policy_mode=request_policy_mode,
                request_policy_required_mode="request_book",
                action="duplicate_skipped",
                http_status=str(exc.status_code or ""),
                reason=str((exc.payload or {}).get("error") or exc.message or "Duplicate pending request"),
            )
        return _build_shelfmark_push_log_row(
            queue_row,
            log_index=log_index,
            dry_run=dry_run,
            requests_enabled=requests_enabled,
            request_policy_mode=request_policy_mode,
            request_policy_required_mode="request_book",
            action="request_failed",
            http_status=str(exc.status_code or ""),
            reason=str((exc.payload or {}).get("error") or exc.message or "Shelfmark request failed"),
        )
    return _build_shelfmark_push_log_row(
        queue_row,
        log_index=log_index,
        dry_run=dry_run,
        requests_enabled=requests_enabled,
        request_policy_mode=request_policy_mode,
        request_policy_required_mode="request_book",
        action="requested",
        shelfmark_request_id=str(response.get("id") or ""),
        reason="Submitted Shelfmark book request",
    )


def build_shelfmark_request_payload(
    queue_row: Mapping[str, Any],
    *,
    shelfmark_note: Optional[str],
) -> dict[str, Any]:
    payload = {
        "provider": str(queue_row.get("shelfmark_provider") or "hardcover"),
        "provider_id": str(queue_row.get("shelfmark_provider_id") or ""),
        "request_level": str(queue_row.get("shelfmark_request_level") or DEFAULT_REQUEST_LEVEL),
        "content_type": str(queue_row.get("shelfmark_content_type") or DEFAULT_REQUEST_CONTENT_TYPE),
        "source": str(queue_row.get("shelfmark_source") or DEFAULT_SHELFMARK_REQUEST_SOURCE),
        "title": str(queue_row.get("display_title") or ""),
        "authors": str(queue_row.get("display_authors") or ""),
    }
    if shelfmark_note:
        payload["note"] = str(shelfmark_note)
    return payload


def _run_release_workflow(
    base_rows: Sequence[Mapping[str, Any]],
    *,
    client: Any,
    shelfmark_source: str,
    shelfmark_content_type: str,
    selection_rule: str,
    format_keywords: Sequence[str],
    min_seeders: int,
    queue_download: bool,
    dry_run: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    release_candidate_rows: list[dict[str, Any]] = []
    selected_release_rows: list[dict[str, Any]] = []
    download_log_rows: list[dict[str, Any]] = []

    for base_row in base_rows:
        attempts = _build_release_lookup_attempts(
            base_row,
            shelfmark_source=shelfmark_source,
            shelfmark_content_type=shelfmark_content_type,
            format_keywords=format_keywords,
        )
        selected_strategy = ""
        selected_term = ""
        lookup_attempt_labels: list[str] = []
        raw_releases: list[dict[str, Any]] = []
        fatal_reason = ""

        for attempt in attempts:
            lookup_attempt_labels.append(attempt.strategy)
            try:
                response = client.search_releases(**dict(attempt.params))
            except ShelfmarkApiError as exc:
                fatal = int(exc.status_code or 0) in {401, 403}
                download_log_rows.append(
                    _build_download_log_row(
                        base_row,
                        log_index=len(download_log_rows) + 1,
                        lookup_strategy=attempt.strategy,
                        lookup_term=attempt.term,
                        shelfmark_source=shelfmark_source,
                        shelfmark_content_type=shelfmark_content_type,
                        selection_rule=selection_rule,
                        candidate_count=0,
                        accepted_candidate_count=0,
                        selected_release=None,
                        action="search_failed",
                        http_status=str(exc.status_code or ""),
                        dry_run=dry_run,
                        reason=str((exc.payload or {}).get("error") or exc.message or "Release search failed"),
                    )
                )
                if fatal:
                    fatal_reason = str((exc.payload or {}).get("error") or exc.message or "Shelfmark authentication required")
                    break
                continue

            candidate_payload = response.get("releases") if isinstance(response, Mapping) else []
            raw_releases = [dict(item) for item in (candidate_payload or []) if isinstance(item, Mapping)]
            selected_strategy = attempt.strategy
            selected_term = attempt.term
            download_log_rows.append(
                _build_download_log_row(
                    base_row,
                    log_index=len(download_log_rows) + 1,
                    lookup_strategy=attempt.strategy,
                    lookup_term=attempt.term,
                    shelfmark_source=shelfmark_source,
                    shelfmark_content_type=shelfmark_content_type,
                    selection_rule=selection_rule,
                    candidate_count=len(raw_releases),
                    accepted_candidate_count=0,
                    selected_release=None,
                    action="searched",
                    dry_run=dry_run,
                    reason=f"Release search returned {len(raw_releases)} candidate(s)",
                )
            )
            if raw_releases:
                break

        accepted_releases: list[dict[str, Any]] = []
        rejected_reason_counts: Counter[str] = Counter()
        for release_index, release in enumerate(raw_releases, start=1):
            rejection_reasons = _release_rejection_reasons(
                release,
                requested_source=shelfmark_source,
                requested_content_type=shelfmark_content_type,
                format_keywords=format_keywords,
                min_seeders=min_seeders,
            )
            if rejection_reasons:
                for reason in rejection_reasons:
                    rejected_reason_counts[reason] += 1
            else:
                accepted_releases.append(dict(release))
            release_candidate_rows.append(
                _build_release_candidate_row(
                    base_row,
                    release=release,
                    candidate_id=len(release_candidate_rows) + 1,
                    release_index=release_index,
                    lookup_strategy=selected_strategy or (attempts[-1].strategy if attempts else ""),
                    lookup_term=selected_term or (attempts[-1].term if attempts else ""),
                    shelfmark_source=shelfmark_source,
                    shelfmark_content_type=shelfmark_content_type,
                    selection_rule=selection_rule,
                    format_keywords=format_keywords,
                    min_seeders=min_seeders,
                    rejection_reasons=rejection_reasons,
                )
            )

        if fatal_reason:
            selected_release_rows.append(
                _build_selected_release_row(
                    base_row,
                    lookup_strategy=selected_strategy or "auth",
                    lookup_attempts=",".join(lookup_attempt_labels),
                    lookup_term=selected_term or "",
                    shelfmark_source=shelfmark_source,
                    shelfmark_content_type=shelfmark_content_type,
                    selection_rule=selection_rule,
                    format_keywords=format_keywords,
                    min_seeders=min_seeders,
                    candidate_count=0,
                    accepted_candidate_count=0,
                    selected_release=None,
                    queue_download_requested=queue_download,
                    queue_download_executed=False,
                    dry_run=dry_run,
                    reason=fatal_reason,
                )
            )
            continue

        if not raw_releases:
            selected_release_rows.append(
                _build_selected_release_row(
                    base_row,
                    lookup_strategy=selected_strategy or (attempts[-1].strategy if attempts else ""),
                    lookup_attempts=",".join(lookup_attempt_labels),
                    lookup_term=selected_term or (attempts[-1].term if attempts else ""),
                    shelfmark_source=shelfmark_source,
                    shelfmark_content_type=shelfmark_content_type,
                    selection_rule=selection_rule,
                    format_keywords=format_keywords,
                    min_seeders=min_seeders,
                    candidate_count=0,
                    accepted_candidate_count=0,
                    selected_release=None,
                    queue_download_requested=queue_download,
                    queue_download_executed=False,
                    dry_run=dry_run,
                    reason="No Shelfmark releases matched the discovery row",
                )
            )
            download_log_rows.append(
                _build_download_log_row(
                    base_row,
                    log_index=len(download_log_rows) + 1,
                    lookup_strategy=selected_strategy or (attempts[-1].strategy if attempts else ""),
                    lookup_term=selected_term or (attempts[-1].term if attempts else ""),
                    shelfmark_source=shelfmark_source,
                    shelfmark_content_type=shelfmark_content_type,
                    selection_rule=selection_rule,
                    candidate_count=0,
                    accepted_candidate_count=0,
                    selected_release=None,
                    action="no_candidates",
                    dry_run=dry_run,
                    reason="No Shelfmark releases matched the discovery row",
                )
            )
            continue

        if not accepted_releases:
            rejection_summary = _summarize_rejections(rejected_reason_counts)
            selected_release_rows.append(
                _build_selected_release_row(
                    base_row,
                    lookup_strategy=selected_strategy,
                    lookup_attempts=",".join(lookup_attempt_labels),
                    lookup_term=selected_term,
                    shelfmark_source=shelfmark_source,
                    shelfmark_content_type=shelfmark_content_type,
                    selection_rule=selection_rule,
                    format_keywords=format_keywords,
                    min_seeders=min_seeders,
                    candidate_count=len(raw_releases),
                    accepted_candidate_count=0,
                    selected_release=None,
                    queue_download_requested=queue_download,
                    queue_download_executed=False,
                    dry_run=dry_run,
                    reason=rejection_summary,
                )
            )
            download_log_rows.append(
                _build_download_log_row(
                    base_row,
                    log_index=len(download_log_rows) + 1,
                    lookup_strategy=selected_strategy,
                    lookup_term=selected_term,
                    shelfmark_source=shelfmark_source,
                    shelfmark_content_type=shelfmark_content_type,
                    selection_rule=selection_rule,
                    candidate_count=len(raw_releases),
                    accepted_candidate_count=0,
                    selected_release=None,
                    action="no_acceptable_release",
                    dry_run=dry_run,
                    reason=rejection_summary,
                )
            )
            continue

        selected_release, selection_reason = _select_release(
            accepted_releases,
            selection_rule=selection_rule,
            shelfmark_content_type=shelfmark_content_type,
            format_keywords=format_keywords,
        )
        queue_download_executed = False
        selected_reason = selection_reason
        download_action = "selected_release"
        http_status = ""
        if queue_download:
            if dry_run:
                download_action = "would_queue_download"
                selected_reason = f"{selection_reason}; dry-run skipped download queue"
            else:
                try:
                    queue_payload = _build_release_queue_payload(
                        base_row,
                        selected_release,
                        requested_content_type=shelfmark_content_type,
                    )
                    response = client.queue_release(queue_payload)
                    queue_download_executed = str(response.get("status") or "").strip().lower() == "queued"
                    download_action = "queued_download" if queue_download_executed else "queue_response"
                    selected_reason = f"{selection_reason}; Shelfmark returned {response.get('status') or 'unknown'}"
                except ShelfmarkApiError as exc:
                    download_action = "queue_failed"
                    http_status = str(exc.status_code or "")
                    selected_reason = str((exc.payload or {}).get("error") or exc.message or "Download queue failed")

        elif not dry_run:
            download_action = "selected_only"
            selected_reason = f"{selection_reason}; export-only mode did not queue download"
        else:
            download_action = "dry-run selected"
            selected_reason = f"{selection_reason}; dry-run export-only mode"

        selected_release_rows.append(
            _build_selected_release_row(
                base_row,
                lookup_strategy=selected_strategy,
                lookup_attempts=",".join(lookup_attempt_labels),
                lookup_term=selected_term,
                shelfmark_source=shelfmark_source,
                shelfmark_content_type=shelfmark_content_type,
                selection_rule=selection_rule,
                format_keywords=format_keywords,
                min_seeders=min_seeders,
                candidate_count=len(raw_releases),
                accepted_candidate_count=len(accepted_releases),
                selected_release=selected_release,
                queue_download_requested=queue_download,
                queue_download_executed=queue_download_executed,
                dry_run=dry_run,
                reason=selected_reason,
            )
        )
        download_log_rows.append(
            _build_download_log_row(
                base_row,
                log_index=len(download_log_rows) + 1,
                lookup_strategy=selected_strategy,
                lookup_term=selected_term,
                shelfmark_source=shelfmark_source,
                shelfmark_content_type=shelfmark_content_type,
                selection_rule=selection_rule,
                candidate_count=len(raw_releases),
                accepted_candidate_count=len(accepted_releases),
                selected_release=selected_release,
                action=download_action,
                http_status=http_status,
                dry_run=dry_run,
                reason=selected_reason,
            )
        )

    return release_candidate_rows, selected_release_rows, download_log_rows


def _build_release_lookup_attempts(
    base_row: Mapping[str, Any],
    *,
    shelfmark_source: str,
    shelfmark_content_type: str,
    format_keywords: Sequence[str],
) -> list[LookupAttempt]:
    attempts: list[LookupAttempt] = []
    hardcover_id = str(base_row.get("hardcover-id") or "").strip()
    if hardcover_id:
        attempts.append(
            LookupAttempt(
                strategy="hardcover_provider_book_id",
                term=(
                    f"provider=hardcover book_id={hardcover_id} "
                    f"source={shelfmark_source} content_type={shelfmark_content_type}"
                ),
                params={
                    "provider": "hardcover",
                    "book_id": hardcover_id,
                    "source": shelfmark_source,
                    "content_type": shelfmark_content_type,
                    "title": str(base_row.get("search_title") or ""),
                    "author": str(base_row.get("search_author") or ""),
                    "format_keywords": list(format_keywords),
                },
            )
        )
    attempts.append(
        LookupAttempt(
            strategy="manual_title_author",
            term=(
                f"title={base_row.get('search_title') or ''} "
                f"author={base_row.get('search_author') or ''} "
                f"source={shelfmark_source} content_type={shelfmark_content_type}"
            ).strip(),
            params={
                "source": shelfmark_source,
                "content_type": shelfmark_content_type,
                "query": str(base_row.get("search_title") or ""),
                "title": str(base_row.get("search_title") or ""),
                "author": str(base_row.get("search_author") or ""),
                "format_keywords": list(format_keywords),
            },
        )
    )
    return attempts


def _release_rejection_reasons(
    release: Mapping[str, Any],
    *,
    requested_source: str,
    requested_content_type: str,
    format_keywords: Sequence[str],
    min_seeders: int,
) -> list[str]:
    reasons: list[str] = []
    release_source = str(release.get("source") or "").strip()
    if requested_source and release_source != requested_source:
        reasons.append(f"source mismatch: expected {requested_source}, got {release_source or 'blank'}")
    if not release_source:
        reasons.append("missing release source")
    if not str(release.get("source_id") or "").strip():
        reasons.append("missing release source_id")
    release_content_type = str(release.get("content_type") or "").strip()
    if requested_content_type and release_content_type and release_content_type != requested_content_type:
        reasons.append(
            f"content type mismatch: expected {requested_content_type}, got {release_content_type}"
        )
    seeders = _to_int(release.get("seeders"))
    if seeders < min_seeders:
        reasons.append(f"seeders below minimum: {seeders} < {min_seeders}")
    if format_keywords and _format_keyword_rank(release, format_keywords) >= len(format_keywords):
        reasons.append(f"format does not match keywords: {', '.join(format_keywords)}")
    return reasons


def _select_release(
    releases: Sequence[Mapping[str, Any]],
    *,
    selection_rule: str,
    shelfmark_content_type: str,
    format_keywords: Sequence[str],
) -> tuple[dict[str, Any], str]:
    indexed = [(index, dict(release)) for index, release in enumerate(releases)]
    if selection_rule == "first":
        selected_index, selected_release = indexed[0]
        return selected_release, f"first selected release won by response order (index {selected_index})"

    if selection_rule == "most_seeders":
        selected_index, selected_release = min(
            indexed,
            key=lambda item: (
                -_to_int(item[1].get("seeders")),
                -_to_int(item[1].get("size_bytes")),
                item[0],
            ),
        )
        return (
            selected_release,
            "most_seeders selected highest seeder count "
            f"({_to_int(selected_release.get('seeders'))}) with deterministic size/order tie-breakers",
        )

    if selection_rule == "largest":
        selected_index, selected_release = min(
            indexed,
            key=lambda item: (
                -_to_int(item[1].get("size_bytes")),
                -_to_int(item[1].get("seeders")),
                item[0],
            ),
        )
        return (
            selected_release,
            "largest selected highest size_bytes "
            f"({_to_int(selected_release.get('size_bytes'))}) with seeders/order tie-breakers",
        )

    if selection_rule == "preferred-format":
        selected_index, selected_release = min(
            indexed,
            key=lambda item: (
                _format_keyword_rank(item[1], format_keywords),
                -_to_int(item[1].get("seeders")),
                -_to_int(item[1].get("size_bytes")),
                item[0],
            ),
        )
        return (
            selected_release,
            "preferred-format selected the earliest matching format keyword "
            f"({ _matching_format_keyword(selected_release, format_keywords) or 'unknown' }) "
            "with seeders/size/order tie-breakers",
        )

    selected_index, selected_release = min(
        indexed,
        key=lambda item: (
            _best_format_rank(item[1], shelfmark_content_type=shelfmark_content_type, format_keywords=format_keywords),
            -_to_int(item[1].get("seeders")),
            -_to_int(item[1].get("size_bytes")),
            item[0],
        ),
    )
    format_reason = _matching_format_keyword(selected_release, format_keywords)
    if not format_reason:
        format_reason = _release_primary_format(selected_release) or "unknown-format"
    return (
        selected_release,
        "best selected the most preferred format "
        f"({format_reason}) with seeders/size/order tie-breakers",
    )


def _best_format_rank(
    release: Mapping[str, Any],
    *,
    shelfmark_content_type: str,
    format_keywords: Sequence[str],
) -> int:
    if format_keywords:
        return _format_keyword_rank(release, format_keywords)
    priorities = DEFAULT_FORMAT_PRIORITY.get(str(shelfmark_content_type or "").strip().lower(), ())
    return _format_keyword_rank(release, priorities)


def _format_keyword_rank(release: Mapping[str, Any], keywords: Sequence[str]) -> int:
    normalized_text = _release_format_search_text(release)
    for index, keyword in enumerate(keywords):
        normalized_keyword = norm(str(keyword or ""))
        if normalized_keyword and normalized_keyword in normalized_text:
            return index
    return len(keywords)


def _matching_format_keyword(release: Mapping[str, Any], keywords: Sequence[str]) -> str:
    normalized_text = _release_format_search_text(release)
    for keyword in keywords:
        normalized_keyword = norm(str(keyword or ""))
        if normalized_keyword and normalized_keyword in normalized_text:
            return str(keyword)
    return ""


def _release_format_search_text(release: Mapping[str, Any]) -> str:
    extra = release.get("extra") if isinstance(release.get("extra"), Mapping) else {}
    parts = [
        str(release.get("format") or ""),
        str(release.get("title") or ""),
        str(extra.get("format") or ""),
        str(extra.get("formats") or ""),
        str(extra.get("formats_display") or ""),
    ]
    return " ".join(norm(part) for part in parts if str(part or "").strip()).strip()


def _release_primary_format(release: Mapping[str, Any]) -> str:
    return str(release.get("format") or "").strip().lower()


def _build_release_candidate_row(
    base_row: Mapping[str, Any],
    *,
    release: Mapping[str, Any],
    candidate_id: int,
    release_index: int,
    lookup_strategy: str,
    lookup_term: str,
    shelfmark_source: str,
    shelfmark_content_type: str,
    selection_rule: str,
    format_keywords: Sequence[str],
    min_seeders: int,
    rejection_reasons: Sequence[str],
) -> dict[str, Any]:
    payload = {
        "candidate_id": str(candidate_id),
        "row_id": str(base_row.get("row_id") or ""),
        "source_row_number": str(base_row.get("source_row_number") or ""),
        "approval_mode": str(base_row.get("approval_mode") or ""),
        "approval_bucket": str(base_row.get("approval_bucket") or ""),
        "discovery_bucket": str(base_row.get("discovery_bucket") or ""),
        "discovery_priority_bucket": str(base_row.get("discovery_priority_bucket") or ""),
        "display_title": str(base_row.get("display_title") or ""),
        "display_authors": str(base_row.get("display_authors") or ""),
        "hardcover-id": str(base_row.get("hardcover-id") or ""),
        "hardcover-slug": str(base_row.get("hardcover-slug") or ""),
        "hardcover-edition": str(base_row.get("hardcover-edition") or ""),
        "lookup_strategy": lookup_strategy,
        "lookup_term": lookup_term,
        "shelfmark_source_requested": shelfmark_source,
        "shelfmark_content_type_requested": shelfmark_content_type,
        "selection_rule": selection_rule,
        "format_keywords": ",".join(str(keyword) for keyword in format_keywords),
        "min_seeders": str(min_seeders),
        "release_index": str(release_index),
        "release_source": str(release.get("source") or ""),
        "release_source_id": str(release.get("source_id") or ""),
        "release_title": str(release.get("title") or ""),
        "release_author": str(release.get("author") or release.get("authors") or ""),
        "release_format": str(release.get("format") or ""),
        "release_content_type": str(release.get("content_type") or ""),
        "release_language": str(release.get("language") or ""),
        "release_size": str(release.get("size") or ""),
        "release_size_bytes": str(_to_int(release.get("size_bytes"))),
        "release_seeders": str(_to_int(release.get("seeders"))),
        "release_info_url": str(release.get("info_url") or ""),
        "release_download_url": str(release.get("download_url") or ""),
        "candidate_status": "rejected" if rejection_reasons else "accepted",
        "rejection_reasons": "; ".join(str(reason) for reason in rejection_reasons if str(reason).strip()),
    }
    return ordered_export_row(payload, SHELFMARK_RELEASE_CANDIDATE_COLUMNS)


def _build_selected_release_row(
    base_row: Mapping[str, Any],
    *,
    lookup_strategy: str,
    lookup_attempts: str,
    lookup_term: str,
    shelfmark_source: str,
    shelfmark_content_type: str,
    selection_rule: str,
    format_keywords: Sequence[str],
    min_seeders: int,
    candidate_count: int,
    accepted_candidate_count: int,
    selected_release: Optional[Mapping[str, Any]],
    queue_download_requested: bool,
    queue_download_executed: bool,
    dry_run: bool,
    reason: str,
) -> dict[str, Any]:
    selected_release = dict(selected_release or {})
    payload = {
        "row_id": str(base_row.get("row_id") or ""),
        "source_row_number": str(base_row.get("source_row_number") or ""),
        "approval_mode": str(base_row.get("approval_mode") or ""),
        "approval_bucket": str(base_row.get("approval_bucket") or ""),
        "discovery_bucket": str(base_row.get("discovery_bucket") or ""),
        "discovery_priority_bucket": str(base_row.get("discovery_priority_bucket") or ""),
        "display_title": str(base_row.get("display_title") or ""),
        "display_authors": str(base_row.get("display_authors") or ""),
        "hardcover-id": str(base_row.get("hardcover-id") or ""),
        "hardcover-slug": str(base_row.get("hardcover-slug") or ""),
        "hardcover-edition": str(base_row.get("hardcover-edition") or ""),
        "lookup_strategy": lookup_strategy,
        "lookup_attempts": lookup_attempts,
        "lookup_term": lookup_term,
        "shelfmark_source_requested": shelfmark_source,
        "shelfmark_content_type_requested": shelfmark_content_type,
        "selection_rule": selection_rule,
        "format_keywords": ",".join(str(keyword) for keyword in format_keywords),
        "min_seeders": str(min_seeders),
        "candidate_count": str(candidate_count),
        "accepted_candidate_count": str(accepted_candidate_count),
        "rejected_candidate_count": str(max(0, candidate_count - accepted_candidate_count)),
        "selected_release_source": str(selected_release.get("source") or ""),
        "selected_release_source_id": str(selected_release.get("source_id") or ""),
        "selected_release_title": str(selected_release.get("title") or ""),
        "selected_release_author": str(selected_release.get("author") or selected_release.get("authors") or ""),
        "selected_release_format": str(selected_release.get("format") or ""),
        "selected_release_content_type": str(selected_release.get("content_type") or ""),
        "selected_release_language": str(selected_release.get("language") or ""),
        "selected_release_size": str(selected_release.get("size") or ""),
        "selected_release_size_bytes": str(_to_int(selected_release.get("size_bytes"))),
        "selected_release_seeders": str(_to_int(selected_release.get("seeders"))),
        "selected_release_info_url": str(selected_release.get("info_url") or ""),
        "selected_release_download_url": str(selected_release.get("download_url") or ""),
        "queue_download_requested": bool(queue_download_requested),
        "queue_download_executed": bool(queue_download_executed),
        "dry_run": bool(dry_run),
        "reason": str(reason or ""),
    }
    return ordered_export_row(payload, SHELFMARK_SELECTED_RELEASE_COLUMNS)


def _build_download_log_row(
    base_row: Mapping[str, Any],
    *,
    log_index: int,
    lookup_strategy: str,
    lookup_term: str,
    shelfmark_source: str,
    shelfmark_content_type: str,
    selection_rule: str,
    candidate_count: int,
    accepted_candidate_count: int,
    selected_release: Optional[Mapping[str, Any]],
    action: str,
    dry_run: bool,
    reason: str,
    http_status: str = "",
) -> dict[str, Any]:
    selected_release = dict(selected_release or {})
    payload = {
        "log_index": str(log_index),
        "row_id": str(base_row.get("row_id") or ""),
        "source_row_number": str(base_row.get("source_row_number") or ""),
        "approval_mode": str(base_row.get("approval_mode") or ""),
        "approval_bucket": str(base_row.get("approval_bucket") or ""),
        "discovery_bucket": str(base_row.get("discovery_bucket") or ""),
        "display_title": str(base_row.get("display_title") or ""),
        "display_authors": str(base_row.get("display_authors") or ""),
        "hardcover-id": str(base_row.get("hardcover-id") or ""),
        "hardcover-slug": str(base_row.get("hardcover-slug") or ""),
        "hardcover-edition": str(base_row.get("hardcover-edition") or ""),
        "lookup_strategy": lookup_strategy,
        "lookup_term": lookup_term,
        "shelfmark_source_requested": shelfmark_source,
        "shelfmark_content_type_requested": shelfmark_content_type,
        "selection_rule": selection_rule,
        "candidate_count": str(candidate_count),
        "accepted_candidate_count": str(accepted_candidate_count),
        "release_source": str(selected_release.get("source") or ""),
        "release_source_id": str(selected_release.get("source_id") or ""),
        "release_title": str(selected_release.get("title") or ""),
        "release_format": str(selected_release.get("format") or ""),
        "release_content_type": str(selected_release.get("content_type") or ""),
        "release_seeders": str(_to_int(selected_release.get("seeders"))),
        "release_size_bytes": str(_to_int(selected_release.get("size_bytes"))),
        "action": action,
        "http_status": str(http_status or ""),
        "dry_run": bool(dry_run),
        "reason": str(reason or ""),
    }
    return ordered_export_row(payload, SHELFMARK_DOWNLOAD_LOG_COLUMNS)


def _build_release_queue_payload(
    base_row: Mapping[str, Any],
    release: Mapping[str, Any],
    *,
    requested_content_type: str,
) -> dict[str, Any]:
    payload = dict(release)
    if not payload.get("content_type") and requested_content_type:
        payload["content_type"] = requested_content_type
    payload.setdefault("author", str(base_row.get("display_authors") or ""))
    payload.setdefault("year", str(base_row.get("release_year") or ""))
    payload.setdefault("series_name", str(base_row.get("display_series") or ""))
    payload.setdefault("series_position", str(base_row.get("series_position") or ""))
    payload.setdefault("subtitle", str(base_row.get("display_subtitle") or ""))
    payload.setdefault("title", str(base_row.get("display_title") or payload.get("title") or ""))
    payload.setdefault("search_mode", "universal")
    return payload


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _summarize_rejections(rejection_counts: Counter[str]) -> str:
    if not rejection_counts:
        return "No acceptable release candidates remained after filtering"
    summary_bits = [
        f"{reason} ({count})"
        for reason, count in sorted(rejection_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    return "All release candidates were rejected: " + "; ".join(summary_bits)


def _build_shelfmark_summary_lines(result: ShelfmarkIntegrationResult) -> list[str]:
    summary_lines = [
        "# Shelfmark summary",
        "",
        "## Request workflow",
        f"- Queue rows written: **{len(result.queue_rows)}**",
        f"- Request log rows written: **{len(result.push_log_rows)}**",
        f"- Requests enabled: **{'yes' if result.requests_enabled else 'no'}**",
        f"- Ebook request policy: **{result.request_policy_mode or 'not_checked'}**",
        "",
        "## Release workflow",
        f"- Release candidate rows written: **{len(result.release_candidate_rows)}**",
        f"- Selected release rows written: **{len(result.selected_release_rows)}**",
        f"- Download log rows written: **{len(result.download_log_rows)}**",
    ]
    if result.selected_release_rows:
        queue_requested = sum(1 for row in result.selected_release_rows if to_bool(row.get("queue_download_requested")))
        queue_executed = sum(1 for row in result.selected_release_rows if to_bool(row.get("queue_download_executed")))
        summary_lines.extend(
            [
                f"- Rows requesting queue/download: **{queue_requested}**",
                f"- Rows queued successfully: **{queue_executed}**",
            ]
        )
    summary_lines.extend(
        [
            "",
            "## Files",
            "- shelfmark_queue.csv / shelfmark_queue.json — opt-in Shelfmark request workflow queue",
            "- shelfmark_push_log.csv — request workflow trace",
            "- shelfmark_release_candidates.csv / shelfmark_release_candidates.json — release-search candidates and rejection details",
            "- shelfmark_selected_releases.csv — per-row release selection decisions",
            "- shelfmark_download_log.csv — release search / selection / queue-download trace",
        ]
    )
    return summary_lines


__all__ = [
    "SHELFMARK_DOWNLOAD_LOG_COLUMNS",
    "SHELFMARK_PUSH_LOG_COLUMNS",
    "SHELFMARK_QUEUE_COLUMNS",
    "SHELFMARK_RELEASE_CANDIDATE_COLUMNS",
    "SHELFMARK_SELECTED_RELEASE_COLUMNS",
    "ShelfmarkIntegrationResult",
    "build_shelfmark_queue",
    "build_shelfmark_request_payload",
    "run_shelfmark_integration",
    "shelfmark_policy_allows_book_requests",
]
