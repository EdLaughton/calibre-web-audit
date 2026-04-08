from __future__ import annotations

import json
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Sequence

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
    "allowed_indexers",
    "blocked_indexers",
    "required_protocol",
    "retry_count",
    "release_index",
    "release_source",
    "release_source_id",
    "release_title",
    "release_author",
    "release_format",
    "release_content_type",
    "release_language",
    "release_protocol",
    "release_indexer",
    "release_size",
    "release_size_bytes",
    "release_seeders",
    "release_info_url",
    "release_download_url",
    "indexer_filter_decision",
    "protocol_filter_decision",
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
    "allowed_indexers",
    "blocked_indexers",
    "required_protocol",
    "candidate_count",
    "accepted_candidate_count",
    "rejected_candidate_count",
    "retry_count",
    "http_status",
    "error_kind",
    "error_message",
    "error_body",
    "selected_release_source",
    "selected_release_source_id",
    "selected_release_title",
    "selected_release_author",
    "selected_release_format",
    "selected_release_content_type",
    "selected_release_language",
    "selected_release_protocol",
    "selected_release_indexer",
    "selected_release_size",
    "selected_release_size_bytes",
    "selected_release_seeders",
    "selected_release_info_url",
    "selected_release_download_url",
    "queue_download_requested",
    "queue_download_executed",
    "dry_run",
    "final_action",
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
    "allowed_indexers",
    "blocked_indexers",
    "required_protocol",
    "candidate_count",
    "accepted_candidate_count",
    "retry_count",
    "http_status",
    "error_kind",
    "error_message",
    "error_body",
    "release_source",
    "release_source_id",
    "release_title",
    "release_format",
    "release_content_type",
    "release_protocol",
    "release_indexer",
    "release_seeders",
    "release_size_bytes",
    "action",
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
    release_summary_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class LookupAttempt:
    strategy: str
    term: str
    params: Mapping[str, Any]


@dataclass(frozen=True)
class ReleaseWorkflowSettings:
    source: str
    content_type: str
    selection_rule: str
    format_keywords: tuple[str, ...]
    min_seeders: int
    allowed_indexers: tuple[str, ...]
    blocked_indexers: tuple[str, ...]
    required_protocol: str
    timeout_seconds: int
    min_interval_ms: int
    max_retries: int
    retry_backoff_seconds: float
    queue_download: bool
    dry_run: bool


@dataclass(frozen=True)
class SearchAttemptResult:
    attempt: LookupAttempt
    releases: list[dict[str, Any]] = field(default_factory=list)
    retry_count: int = 0
    error: ShelfmarkApiError | None = None
    error_message: str = ""
    error_body: str = ""
    http_status: str = ""
    final_action: str = ""
    stop_row: bool = False


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
    shelfmark_allowed_indexers: Sequence[str] = (),
    shelfmark_blocked_indexers: Sequence[str] = (),
    shelfmark_require_protocol: str = "",
    shelfmark_timeout_seconds: int = 30,
    shelfmark_min_interval_ms: int = 1000,
    shelfmark_max_retries: int = 1,
    shelfmark_retry_backoff_seconds: float = 2.0,
    verbose: bool = False,
    shelfmark_client: Optional[Any] = None,
    sleep_fn: Optional[Callable[[float], None]] = None,
    monotonic_fn: Optional[Callable[[], float]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
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
    release_summary_counts: dict[str, int] = {}
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
            client = shelfmark_client or ShelfmarkClient(
                base_url=str(shelfmark_url or ""),
                timeout=max(1, int(shelfmark_timeout_seconds or 30)),
            )
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
            request_policy_mode = str(
                defaults.get(DEFAULT_REQUEST_CONTENT_TYPE) or policy.get(DEFAULT_REQUEST_CONTENT_TYPE) or ""
            )
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
                    push_log_rows.append(
                        _submit_shelfmark_request(
                            queue_row,
                            client=get_client(),
                            shelfmark_note=shelfmark_note,
                            requests_enabled=requests_enabled,
                            request_policy_mode=request_policy_mode,
                            dry_run=dry_run,
                            log_index=len(push_log_rows) + 1,
                        )
                    )

    if export_shelfmark_releases or push_shelfmark_download:
        if shelfmark_username and shelfmark_password:
            try:
                login_if_requested()
            except Exception as exc:
                reason = f"Shelfmark login failed before release search: {exc}"
                for base_row in base_rows:
                    selected_release_rows.append(
                        _build_selected_release_row(
                            base_row,
                            settings=ReleaseWorkflowSettings(
                                source=str(shelfmark_source or ""),
                                content_type=str(shelfmark_content_type or "ebook"),
                                selection_rule=str(shelfmark_selection or "best"),
                                format_keywords=tuple(str(keyword) for keyword in shelfmark_format_keywords if str(keyword).strip()),
                                min_seeders=max(0, int(shelfmark_min_seeders or 0)),
                                allowed_indexers=tuple(str(indexer) for indexer in shelfmark_allowed_indexers if str(indexer).strip()),
                                blocked_indexers=tuple(str(indexer) for indexer in shelfmark_blocked_indexers if str(indexer).strip()),
                                required_protocol=str(shelfmark_require_protocol or "").strip().lower(),
                                timeout_seconds=max(1, int(shelfmark_timeout_seconds or 30)),
                                min_interval_ms=max(0, int(shelfmark_min_interval_ms or 0)),
                                max_retries=max(0, int(shelfmark_max_retries or 0)),
                                retry_backoff_seconds=max(0.0, float(shelfmark_retry_backoff_seconds or 0.0)),
                                queue_download=bool(push_shelfmark_download),
                                dry_run=bool(dry_run),
                            ),
                            lookup_strategy="auth",
                            lookup_attempts="auth",
                            lookup_term="authenticated-session",
                            candidate_count=0,
                            accepted_candidate_count=0,
                            retry_count=0,
                            selected_release=None,
                            queue_download_requested=push_shelfmark_download,
                            queue_download_executed=False,
                            dry_run=dry_run,
                            final_action="failed",
                            reason=reason,
                            error_kind="auth_failed",
                            error_message=reason,
                        )
                    )
                    download_log_rows.append(
                        _build_download_log_row(
                            base_row,
                            settings=ReleaseWorkflowSettings(
                                source=str(shelfmark_source or ""),
                                content_type=str(shelfmark_content_type or "ebook"),
                                selection_rule=str(shelfmark_selection or "best"),
                                format_keywords=tuple(str(keyword) for keyword in shelfmark_format_keywords if str(keyword).strip()),
                                min_seeders=max(0, int(shelfmark_min_seeders or 0)),
                                allowed_indexers=tuple(str(indexer) for indexer in shelfmark_allowed_indexers if str(indexer).strip()),
                                blocked_indexers=tuple(str(indexer) for indexer in shelfmark_blocked_indexers if str(indexer).strip()),
                                required_protocol=str(shelfmark_require_protocol or "").strip().lower(),
                                timeout_seconds=max(1, int(shelfmark_timeout_seconds or 30)),
                                min_interval_ms=max(0, int(shelfmark_min_interval_ms or 0)),
                                max_retries=max(0, int(shelfmark_max_retries or 0)),
                                retry_backoff_seconds=max(0.0, float(shelfmark_retry_backoff_seconds or 0.0)),
                                queue_download=bool(push_shelfmark_download),
                                dry_run=bool(dry_run),
                            ),
                            log_index=len(download_log_rows) + 1,
                            lookup_strategy="auth",
                            lookup_term="authenticated-session",
                            candidate_count=0,
                            accepted_candidate_count=0,
                            retry_count=0,
                            selected_release=None,
                            action="failed",
                            dry_run=dry_run,
                            reason=reason,
                            error_kind="auth_failed",
                            error_message=reason,
                        )
                    )
                release_summary_counts = {
                    "rows_searched": len(base_rows),
                    "rows_with_candidates": 0,
                    "rows_selected": 0,
                    "rows_queued": 0,
                    "rows_skipped": 0,
                    "rows_filtered_out": 0,
                    "rows_failed": len(base_rows),
                    "timeouts": 0,
                    "http_errors": 0,
                    "retries_attempted": 0,
                    "retries_exhausted": 0,
                }
            else:
                pass
        if not selected_release_rows:
            settings = ReleaseWorkflowSettings(
                source=str(shelfmark_source or ""),
                content_type=str(shelfmark_content_type or "ebook"),
                selection_rule=str(shelfmark_selection or "best"),
                format_keywords=tuple(
                    str(keyword).strip()
                    for keyword in shelfmark_format_keywords
                    if str(keyword).strip()
                ),
                min_seeders=max(0, int(shelfmark_min_seeders or 0)),
                allowed_indexers=tuple(
                    str(indexer).strip()
                    for indexer in shelfmark_allowed_indexers
                    if str(indexer).strip()
                ),
                blocked_indexers=tuple(
                    str(indexer).strip()
                    for indexer in shelfmark_blocked_indexers
                    if str(indexer).strip()
                ),
                required_protocol=str(shelfmark_require_protocol or "").strip().lower(),
                timeout_seconds=max(1, int(shelfmark_timeout_seconds or 30)),
                min_interval_ms=max(0, int(shelfmark_min_interval_ms or 0)),
                max_retries=max(0, int(shelfmark_max_retries or 0)),
                retry_backoff_seconds=max(0.0, float(shelfmark_retry_backoff_seconds or 0.0)),
                queue_download=bool(push_shelfmark_download),
                dry_run=bool(dry_run),
            )
            runner = ShelfmarkReleaseRunner(
                client=get_client(),
                settings=settings,
                sleep_fn=sleep_fn or time.sleep,
                monotonic_fn=monotonic_fn or time.monotonic,
                log_fn=log_fn or print,
            )
            release_candidate_rows, selected_release_rows, download_log_rows, release_summary_counts = runner.run(base_rows)

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
        release_summary_counts=release_summary_counts,
    )
    return ShelfmarkIntegrationResult(
        **{**result.__dict__, "summary_lines": _build_shelfmark_summary_lines(result)}
    )


def shelfmark_policy_allows_book_requests(*, requests_enabled: bool, request_policy_mode: str) -> bool:
    return bool(requests_enabled) and str(request_policy_mode or "").strip().lower() == "request_book"


class ShelfmarkReleaseRunner:
    def __init__(
        self,
        *,
        client: Any,
        settings: ReleaseWorkflowSettings,
        sleep_fn: Callable[[float], None],
        monotonic_fn: Callable[[], float],
        log_fn: Callable[[str], None],
    ) -> None:
        self.client = client
        self.settings = settings
        self.sleep_fn = sleep_fn
        self.monotonic_fn = monotonic_fn
        self.log_fn = log_fn
        self.last_request_started_at: float | None = None
        self.release_candidate_rows: list[dict[str, Any]] = []
        self.selected_release_rows: list[dict[str, Any]] = []
        self.download_log_rows: list[dict[str, Any]] = []
        self.summary_counts: Counter[str] = Counter()

    def run(
        self,
        base_rows: Sequence[Mapping[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
        self.log_fn(
            "Shelfmark release phase: "
            f"rows={len(base_rows)} | source={self.settings.source} | "
            f"content_type={self.settings.content_type or 'any'} | selection={self.settings.selection_rule} | "
            f"allowed_indexers={','.join(self.settings.allowed_indexers) or '-'} | "
            f"blocked_indexers={','.join(self.settings.blocked_indexers) or '-'} | "
            f"protocol={self.settings.required_protocol or '-'} | "
            f"timeout={self.settings.timeout_seconds}s | "
            f"min_interval={self.settings.min_interval_ms}ms | "
            f"max_retries={self.settings.max_retries}"
        )
        self.log_fn("Shelfmark release phase: preparing eligible rows complete; starting release search")

        for position, base_row in enumerate(base_rows, start=1):
            self.summary_counts["rows_searched"] += 1
            self._process_row(base_row, position=position, total=len(base_rows))

        summary_counts = {
            "rows_searched": int(self.summary_counts.get("rows_searched", 0)),
            "rows_with_candidates": int(self.summary_counts.get("rows_with_candidates", 0)),
            "rows_selected": int(self.summary_counts.get("rows_selected", 0)),
            "rows_queued": int(self.summary_counts.get("rows_queued", 0)),
            "rows_skipped": int(self.summary_counts.get("rows_skipped", 0)),
            "rows_filtered_out": int(self.summary_counts.get("rows_filtered_out", 0)),
            "rows_failed": int(self.summary_counts.get("rows_failed", 0)),
            "timeouts": int(self.summary_counts.get("timeouts", 0)),
            "http_errors": int(self.summary_counts.get("http_errors", 0)),
            "retries_attempted": int(self.summary_counts.get("retries_attempted", 0)),
            "retries_exhausted": int(self.summary_counts.get("retries_exhausted", 0)),
        }
        self.log_fn("Shelfmark release phase: writing artifacts")
        return (
            self.release_candidate_rows,
            self.selected_release_rows,
            self.download_log_rows,
            summary_counts,
        )

    def _process_row(self, base_row: Mapping[str, Any], *, position: int, total: int) -> None:
        row_label = f"[{position}/{total}] calibre-row={base_row.get('source_row_number') or position}"
        display_title = str(base_row.get("display_title") or "")
        self.log_fn(f"Shelfmark release {row_label}: searching releases for {display_title or 'untitled row'}")

        attempts = _build_release_lookup_attempts(base_row, settings=self.settings)
        lookup_attempt_labels: list[str] = []
        successful_search: SearchAttemptResult | None = None
        final_failure: SearchAttemptResult | None = None
        total_retries = 0

        for attempt in attempts:
            lookup_attempt_labels.append(attempt.strategy)
            search_result = self._execute_search_attempt_with_retries(base_row, attempt)
            total_retries += search_result.retry_count
            if search_result.releases:
                successful_search = search_result
                self.summary_counts["rows_with_candidates"] += 1
                break
            if search_result.error is not None:
                final_failure = search_result
                if search_result.stop_row:
                    break
                self.log_fn(
                    f"Shelfmark release {row_label}: {search_result.final_action} on {attempt.strategy}; continuing"
                )
                continue
            final_failure = search_result

        if successful_search is None:
            self._finalize_without_release(
                base_row,
                lookup_attempt_labels=lookup_attempt_labels,
                failure=final_failure,
                retry_count=total_retries,
            )
            return

        self.log_fn(f"Shelfmark release {row_label}: filtering {len(successful_search.releases)} candidate(s)")
        accepted_releases: list[dict[str, Any]] = []
        rejected_reason_counts: Counter[str] = Counter()

        for release_index, release in enumerate(successful_search.releases, start=1):
            rejection = _release_filter_outcome(release, settings=self.settings)
            for reason in rejection.rejection_reasons:
                rejected_reason_counts[reason] += 1
            self.release_candidate_rows.append(
                _build_release_candidate_row(
                    base_row,
                    release=release,
                    candidate_id=len(self.release_candidate_rows) + 1,
                    release_index=release_index,
                    lookup_strategy=successful_search.attempt.strategy,
                    lookup_term=successful_search.attempt.term,
                    settings=self.settings,
                    retry_count=total_retries,
                    rejection=rejection,
                )
            )
            if not rejection.rejection_reasons:
                accepted_releases.append(dict(release))

        if not accepted_releases:
            reason = _summarize_rejections(rejected_reason_counts)
            self.summary_counts["rows_filtered_out"] += 1
            self.log_fn(f"Shelfmark release {row_label}: all candidates filtered out")
            self.selected_release_rows.append(
                _build_selected_release_row(
                    base_row,
                    settings=self.settings,
                    lookup_strategy=successful_search.attempt.strategy,
                    lookup_attempts=",".join(lookup_attempt_labels),
                    lookup_term=successful_search.attempt.term,
                    candidate_count=len(successful_search.releases),
                    accepted_candidate_count=0,
                    retry_count=total_retries,
                    selected_release=None,
                    queue_download_requested=self.settings.queue_download,
                    queue_download_executed=False,
                    dry_run=self.settings.dry_run,
                    final_action="filtered_out",
                    reason=reason,
                )
            )
            self.download_log_rows.append(
                _build_download_log_row(
                    base_row,
                    settings=self.settings,
                    log_index=len(self.download_log_rows) + 1,
                    lookup_strategy=successful_search.attempt.strategy,
                    lookup_term=successful_search.attempt.term,
                    candidate_count=len(successful_search.releases),
                    accepted_candidate_count=0,
                    retry_count=total_retries,
                    selected_release=None,
                    action="filtered_out",
                    dry_run=self.settings.dry_run,
                    reason=reason,
                )
            )
            return

        self.log_fn(f"Shelfmark release {row_label}: selecting winner from {len(accepted_releases)} filtered candidate(s)")
        selected_release, selection_reason = _select_release(
            accepted_releases,
            selection_rule=self.settings.selection_rule,
            shelfmark_content_type=self.settings.content_type,
            format_keywords=self.settings.format_keywords,
        )
        self.summary_counts["rows_selected"] += 1

        queue_download_executed = False
        final_action = "selected"
        final_reason = selection_reason
        http_status = ""
        error_kind = ""
        error_message = ""
        error_body = ""

        if self.settings.queue_download:
            self.log_fn(f"Shelfmark release {row_label}: queueing selected release")
            if self.settings.dry_run:
                final_action = "dry-run queued"
                final_reason = f"{selection_reason}; dry-run skipped queue/download"
            else:
                try:
                    queue_payload = _build_release_queue_payload(
                        base_row,
                        selected_release,
                        requested_content_type=self.settings.content_type,
                    )
                    response = self.client.queue_release(queue_payload)
                    queue_download_executed = str(response.get("status") or "").strip().lower() == "queued"
                    if queue_download_executed:
                        self.summary_counts["rows_queued"] += 1
                        final_action = "queued"
                        final_reason = f"{selection_reason}; Shelfmark queued the selected release"
                    else:
                        final_action = "failed"
                        final_reason = (
                            f"{selection_reason}; Shelfmark returned unexpected queue status "
                            f"{str(response.get('status') or 'unknown')}"
                        )
                        self.summary_counts["rows_failed"] += 1
                except ShelfmarkApiError as exc:
                    http_status, error_kind, error_message, error_body = _error_details(exc)
                    final_action = "failed"
                    final_reason = f"{selection_reason}; queue/download failed: {error_message}"
                    self.summary_counts["rows_failed"] += 1
        else:
            if self.settings.dry_run:
                final_reason = f"{selection_reason}; dry-run export-only mode"
            else:
                final_reason = f"{selection_reason}; export-only mode did not queue download"

        self.selected_release_rows.append(
            _build_selected_release_row(
                base_row,
                settings=self.settings,
                lookup_strategy=successful_search.attempt.strategy,
                lookup_attempts=",".join(lookup_attempt_labels),
                lookup_term=successful_search.attempt.term,
                candidate_count=len(successful_search.releases),
                accepted_candidate_count=len(accepted_releases),
                retry_count=total_retries,
                selected_release=selected_release,
                queue_download_requested=self.settings.queue_download,
                queue_download_executed=queue_download_executed,
                dry_run=self.settings.dry_run,
                final_action=final_action,
                reason=final_reason,
                http_status=http_status,
                error_kind=error_kind,
                error_message=error_message,
                error_body=error_body,
            )
        )
        self.download_log_rows.append(
            _build_download_log_row(
                base_row,
                settings=self.settings,
                log_index=len(self.download_log_rows) + 1,
                lookup_strategy=successful_search.attempt.strategy,
                lookup_term=successful_search.attempt.term,
                candidate_count=len(successful_search.releases),
                accepted_candidate_count=len(accepted_releases),
                retry_count=total_retries,
                selected_release=selected_release,
                action=final_action,
                dry_run=self.settings.dry_run,
                reason=final_reason,
                http_status=http_status,
                error_kind=error_kind,
                error_message=error_message,
                error_body=error_body,
            )
        )
        self.log_fn(f"Shelfmark release {row_label}: {final_action}")

    def _execute_search_attempt_with_retries(
        self,
        base_row: Mapping[str, Any],
        attempt: LookupAttempt,
    ) -> SearchAttemptResult:
        total_allowed_attempts = max(1, self.settings.max_retries + 1)
        retry_count = 0
        for attempt_index in range(1, total_allowed_attempts + 1):
            self._pace_before_search()
            self.log_fn(
                f"Shelfmark release search: strategy={attempt.strategy} | "
                f"source={self.settings.source} | row={base_row.get('row_id') or ''} | "
                f"attempt={attempt_index}/{total_allowed_attempts}"
            )
            try:
                response = self.client.search_releases(
                    **dict(attempt.params),
                    timeout_seconds=self.settings.timeout_seconds,
                )
            except ShelfmarkApiError as exc:
                http_status, error_kind, error_message, error_body = _error_details(exc)
                if error_kind == "timeout":
                    self.summary_counts["timeouts"] += 1
                elif error_kind == "http_error":
                    self.summary_counts["http_errors"] += 1
                should_retry = _is_retryable_release_error(exc) and attempt_index < total_allowed_attempts
                action = "retrying" if should_retry else error_kind
                self.download_log_rows.append(
                    _build_download_log_row(
                        base_row,
                        settings=self.settings,
                        log_index=len(self.download_log_rows) + 1,
                        lookup_strategy=attempt.strategy,
                        lookup_term=attempt.term,
                        candidate_count=0,
                        accepted_candidate_count=0,
                        retry_count=retry_count,
                        selected_release=None,
                        action=action,
                        dry_run=self.settings.dry_run,
                        reason=error_message,
                        http_status=http_status,
                        error_kind=error_kind,
                        error_message=error_message,
                        error_body=error_body,
                    )
                )
                if should_retry:
                    retry_count += 1
                    self.summary_counts["retries_attempted"] += 1
                    backoff_seconds = self.settings.retry_backoff_seconds * (2 ** (retry_count - 1))
                    self.log_fn(
                        f"Shelfmark release retry: strategy={attempt.strategy} | "
                        f"reason={error_kind} | status={http_status or '-'} | "
                        f"retry={retry_count}/{self.settings.max_retries} | backoff={backoff_seconds:.2f}s"
                    )
                    if backoff_seconds > 0:
                        self.sleep_fn(backoff_seconds)
                    continue
                if _is_retryable_release_error(exc) and attempt_index >= total_allowed_attempts:
                    self.summary_counts["retries_exhausted"] += 1
                return SearchAttemptResult(
                    attempt=attempt,
                    releases=[],
                    retry_count=retry_count,
                    error=exc,
                    error_message=error_message,
                    error_body=error_body,
                    http_status=http_status,
                    final_action=error_kind,
                    stop_row=int(exc.status_code or 0) in {401, 403},
                )

            releases = response.get("releases") if isinstance(response, Mapping) else []
            normalized_releases = [dict(item) for item in (releases or []) if isinstance(item, Mapping)]
            self.download_log_rows.append(
                _build_download_log_row(
                    base_row,
                    settings=self.settings,
                    log_index=len(self.download_log_rows) + 1,
                    lookup_strategy=attempt.strategy,
                    lookup_term=attempt.term,
                    candidate_count=len(normalized_releases),
                    accepted_candidate_count=0,
                    retry_count=retry_count,
                    selected_release=None,
                    action="searched",
                    dry_run=self.settings.dry_run,
                    reason=f"Release search returned {len(normalized_releases)} candidate(s)",
                )
            )
            return SearchAttemptResult(
                attempt=attempt,
                releases=normalized_releases,
                retry_count=retry_count,
                final_action="searched",
            )

        return SearchAttemptResult(attempt=attempt, releases=[], retry_count=retry_count, final_action="failed")

    def _finalize_without_release(
        self,
        base_row: Mapping[str, Any],
        *,
        lookup_attempt_labels: Sequence[str],
        failure: SearchAttemptResult | None,
        retry_count: int,
    ) -> None:
        lookup_strategy = failure.attempt.strategy if failure is not None else ""
        lookup_term = failure.attempt.term if failure is not None else ""
        error_kind = failure.final_action if failure is not None else ""
        error_message = failure.error_message if failure is not None else ""
        error_body = failure.error_body if failure is not None else ""
        http_status = failure.http_status if failure is not None else ""

        if failure is None:
            final_action = "skipped"
            reason = "No Shelfmark releases matched the discovery row"
            self.summary_counts["rows_skipped"] += 1
        elif failure.error is not None:
            if failure.final_action == "timeout":
                final_action = "timed_out"
            elif failure.final_action == "http_error":
                final_action = "http_error"
            else:
                final_action = "failed"
            reason = error_message or "Shelfmark release search failed"
            self.summary_counts["rows_failed"] += 1
        else:
            final_action = "skipped"
            reason = "No Shelfmark releases matched the discovery row"
            self.summary_counts["rows_skipped"] += 1

        self.selected_release_rows.append(
            _build_selected_release_row(
                base_row,
                settings=self.settings,
                lookup_strategy=lookup_strategy,
                lookup_attempts=",".join(lookup_attempt_labels),
                lookup_term=lookup_term,
                candidate_count=0,
                accepted_candidate_count=0,
                retry_count=retry_count,
                selected_release=None,
                queue_download_requested=self.settings.queue_download,
                queue_download_executed=False,
                dry_run=self.settings.dry_run,
                final_action=final_action,
                reason=reason,
                http_status=http_status,
                error_kind=error_kind,
                error_message=error_message,
                error_body=error_body,
            )
        )
        self.download_log_rows.append(
            _build_download_log_row(
                base_row,
                settings=self.settings,
                log_index=len(self.download_log_rows) + 1,
                lookup_strategy=lookup_strategy,
                lookup_term=lookup_term,
                candidate_count=0,
                accepted_candidate_count=0,
                retry_count=retry_count,
                selected_release=None,
                action=final_action,
                dry_run=self.settings.dry_run,
                reason=reason,
                http_status=http_status,
                error_kind=error_kind,
                error_message=error_message,
                error_body=error_body,
            )
        )
        self.log_fn(
            f"Shelfmark release [row {base_row.get('source_row_number') or '?'}]: {final_action}; continuing"
        )

    def _pace_before_search(self) -> None:
        if self.last_request_started_at is None:
            self.last_request_started_at = self.monotonic_fn()
            return
        min_interval_seconds = max(0.0, float(self.settings.min_interval_ms) / 1000.0)
        elapsed = self.monotonic_fn() - self.last_request_started_at
        if elapsed < min_interval_seconds:
            self.sleep_fn(min_interval_seconds - elapsed)
        self.last_request_started_at = self.monotonic_fn()


@dataclass(frozen=True)
class ReleaseFilterOutcome:
    rejection_reasons: tuple[str, ...]
    indexer_filter_decision: str
    protocol_filter_decision: str


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


def _build_release_lookup_attempts(
    base_row: Mapping[str, Any],
    *,
    settings: ReleaseWorkflowSettings,
) -> list[LookupAttempt]:
    attempts: list[LookupAttempt] = []
    hardcover_id = str(base_row.get("hardcover-id") or "").strip()
    common_params = {
        "source": settings.source,
        "content_type": settings.content_type,
        "format_keywords": list(settings.format_keywords),
    }
    if settings.source.lower() == "prowlarr" and settings.allowed_indexers:
        common_params["indexers"] = list(settings.allowed_indexers)
    if hardcover_id:
        provider_params = {
            **common_params,
            "provider": "hardcover",
            "book_id": hardcover_id,
            "title": str(base_row.get("search_title") or ""),
            "author": str(base_row.get("search_author") or ""),
        }
        attempts.append(
            LookupAttempt(
                strategy="hardcover_provider_book_id",
                term=_build_lookup_term(provider_params),
                params=provider_params,
            )
        )
    manual_params = {
        **common_params,
        "query": str(base_row.get("search_title") or ""),
        "title": str(base_row.get("search_title") or ""),
        "author": str(base_row.get("search_author") or ""),
    }
    attempts.append(
        LookupAttempt(
            strategy="manual_title_author",
            term=_build_lookup_term(manual_params),
            params=manual_params,
        )
    )
    return attempts


def _build_lookup_term(params: Mapping[str, Any]) -> str:
    bits: list[str] = []
    if str(params.get("provider") or "").strip():
        bits.append(f"provider={params.get('provider')}")
    if str(params.get("book_id") or "").strip():
        bits.append(f"book_id={params.get('book_id')}")
    if str(params.get("query") or "").strip():
        bits.append(f"query={params.get('query')}")
    if str(params.get("author") or "").strip():
        bits.append(f"author={params.get('author')}")
    if str(params.get("source") or "").strip():
        bits.append(f"source={params.get('source')}")
    if str(params.get("content_type") or "").strip():
        bits.append(f"content_type={params.get('content_type')}")
    indexers = params.get("indexers")
    if isinstance(indexers, Sequence) and not isinstance(indexers, (str, bytes)):
        normalized = [str(item).strip() for item in indexers if str(item).strip()]
        if normalized:
            bits.append(f"indexers={','.join(normalized)}")
    return " ".join(bits)


def _release_filter_outcome(
    release: Mapping[str, Any],
    *,
    settings: ReleaseWorkflowSettings,
) -> ReleaseFilterOutcome:
    reasons: list[str] = []
    release_source = str(release.get("source") or "").strip()
    release_source_id = str(release.get("source_id") or "").strip()
    release_title = str(release.get("title") or "").strip()
    release_content_type = str(release.get("content_type") or "").strip()
    release_seeders = _to_int(release.get("seeders"))
    release_indexer = _release_indexer(release)
    release_protocol = _release_protocol(release)

    if settings.source and release_source != settings.source:
        reasons.append(f"source mismatch: expected {settings.source}, got {release_source or 'blank'}")
    if not release_source:
        reasons.append("missing release source")
    if not release_source_id:
        reasons.append("missing release source_id")
    if not release_title:
        reasons.append("missing release title")
    if settings.content_type and release_content_type and release_content_type != settings.content_type:
        reasons.append(f"content type mismatch: expected {settings.content_type}, got {release_content_type}")
    if release_seeders < settings.min_seeders:
        reasons.append(f"seeders below minimum: {release_seeders} < {settings.min_seeders}")

    indexer_filter_decision = "not_configured"
    normalized_release_indexer = _normalize_filter_token(release_indexer)
    allowed_indexers = {_normalize_filter_token(value) for value in settings.allowed_indexers if _normalize_filter_token(value)}
    blocked_indexers = {_normalize_filter_token(value) for value in settings.blocked_indexers if _normalize_filter_token(value)}
    if allowed_indexers:
        if not normalized_release_indexer:
            indexer_filter_decision = "missing_indexer"
            reasons.append("indexer allowlist active but release indexer is blank")
        elif normalized_release_indexer not in allowed_indexers:
            indexer_filter_decision = "not_allowlisted"
            reasons.append(f"indexer not allowlisted: {release_indexer}")
        else:
            indexer_filter_decision = "allowlisted"
    if blocked_indexers and normalized_release_indexer in blocked_indexers:
        indexer_filter_decision = "blocklisted"
        reasons.append(f"indexer blocklisted: {release_indexer}")
    elif blocked_indexers and indexer_filter_decision == "not_configured":
        indexer_filter_decision = "not_blocklisted"

    protocol_filter_decision = "not_configured"
    normalized_release_protocol = _normalize_filter_token(release_protocol)
    if settings.required_protocol:
        if not normalized_release_protocol:
            protocol_filter_decision = "missing_protocol"
            reasons.append("required protocol filter active but release protocol is blank")
        elif normalized_release_protocol != _normalize_filter_token(settings.required_protocol):
            protocol_filter_decision = "protocol_mismatch"
            reasons.append(
                f"protocol mismatch: expected {settings.required_protocol}, got {release_protocol or 'blank'}"
            )
        else:
            protocol_filter_decision = "matched"

    if settings.format_keywords and _format_keyword_rank(release, settings.format_keywords) >= len(settings.format_keywords):
        reasons.append(f"format does not match keywords: {', '.join(settings.format_keywords)}")

    return ReleaseFilterOutcome(
        rejection_reasons=tuple(reasons),
        indexer_filter_decision=indexer_filter_decision,
        protocol_filter_decision=protocol_filter_decision,
    )


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
            f"({_matching_format_keyword(selected_release, format_keywords) or 'unknown'}) "
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


def _release_protocol(release: Mapping[str, Any]) -> str:
    extra = release.get("extra") if isinstance(release.get("extra"), Mapping) else {}
    return str(release.get("protocol") or extra.get("protocol") or "").strip()


def _release_indexer(release: Mapping[str, Any]) -> str:
    extra = release.get("extra") if isinstance(release.get("extra"), Mapping) else {}
    return str(release.get("indexer") or extra.get("indexer") or "").strip()


def _normalize_filter_token(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _build_release_candidate_row(
    base_row: Mapping[str, Any],
    *,
    release: Mapping[str, Any],
    candidate_id: int,
    release_index: int,
    lookup_strategy: str,
    lookup_term: str,
    settings: ReleaseWorkflowSettings,
    retry_count: int,
    rejection: ReleaseFilterOutcome,
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
        "shelfmark_source_requested": settings.source,
        "shelfmark_content_type_requested": settings.content_type,
        "selection_rule": settings.selection_rule,
        "format_keywords": ",".join(str(keyword) for keyword in settings.format_keywords),
        "min_seeders": str(settings.min_seeders),
        "allowed_indexers": ",".join(settings.allowed_indexers),
        "blocked_indexers": ",".join(settings.blocked_indexers),
        "required_protocol": settings.required_protocol,
        "retry_count": str(retry_count),
        "release_index": str(release_index),
        "release_source": str(release.get("source") or ""),
        "release_source_id": str(release.get("source_id") or ""),
        "release_title": str(release.get("title") or ""),
        "release_author": str(release.get("author") or release.get("authors") or ""),
        "release_format": str(release.get("format") or ""),
        "release_content_type": str(release.get("content_type") or ""),
        "release_language": str(release.get("language") or ""),
        "release_protocol": _release_protocol(release),
        "release_indexer": _release_indexer(release),
        "release_size": str(release.get("size") or ""),
        "release_size_bytes": str(_to_int(release.get("size_bytes"))),
        "release_seeders": str(_to_int(release.get("seeders"))),
        "release_info_url": str(release.get("info_url") or ""),
        "release_download_url": str(release.get("download_url") or ""),
        "indexer_filter_decision": rejection.indexer_filter_decision,
        "protocol_filter_decision": rejection.protocol_filter_decision,
        "candidate_status": "rejected" if rejection.rejection_reasons else "accepted",
        "rejection_reasons": "; ".join(rejection.rejection_reasons),
    }
    return ordered_export_row(payload, SHELFMARK_RELEASE_CANDIDATE_COLUMNS)


def _build_selected_release_row(
    base_row: Mapping[str, Any],
    *,
    settings: ReleaseWorkflowSettings,
    lookup_strategy: str,
    lookup_attempts: str,
    lookup_term: str,
    candidate_count: int,
    accepted_candidate_count: int,
    retry_count: int,
    selected_release: Optional[Mapping[str, Any]],
    queue_download_requested: bool,
    queue_download_executed: bool,
    dry_run: bool,
    final_action: str,
    reason: str,
    http_status: str = "",
    error_kind: str = "",
    error_message: str = "",
    error_body: str = "",
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
        "shelfmark_source_requested": settings.source,
        "shelfmark_content_type_requested": settings.content_type,
        "selection_rule": settings.selection_rule,
        "format_keywords": ",".join(str(keyword) for keyword in settings.format_keywords),
        "min_seeders": str(settings.min_seeders),
        "allowed_indexers": ",".join(settings.allowed_indexers),
        "blocked_indexers": ",".join(settings.blocked_indexers),
        "required_protocol": settings.required_protocol,
        "candidate_count": str(candidate_count),
        "accepted_candidate_count": str(accepted_candidate_count),
        "rejected_candidate_count": str(max(0, candidate_count - accepted_candidate_count)),
        "retry_count": str(retry_count),
        "http_status": str(http_status or ""),
        "error_kind": str(error_kind or ""),
        "error_message": str(error_message or ""),
        "error_body": str(error_body or ""),
        "selected_release_source": str(selected_release.get("source") or ""),
        "selected_release_source_id": str(selected_release.get("source_id") or ""),
        "selected_release_title": str(selected_release.get("title") or ""),
        "selected_release_author": str(selected_release.get("author") or selected_release.get("authors") or ""),
        "selected_release_format": str(selected_release.get("format") or ""),
        "selected_release_content_type": str(selected_release.get("content_type") or ""),
        "selected_release_language": str(selected_release.get("language") or ""),
        "selected_release_protocol": _release_protocol(selected_release),
        "selected_release_indexer": _release_indexer(selected_release),
        "selected_release_size": str(selected_release.get("size") or ""),
        "selected_release_size_bytes": str(_to_int(selected_release.get("size_bytes"))),
        "selected_release_seeders": str(_to_int(selected_release.get("seeders"))),
        "selected_release_info_url": str(selected_release.get("info_url") or ""),
        "selected_release_download_url": str(selected_release.get("download_url") or ""),
        "queue_download_requested": bool(queue_download_requested),
        "queue_download_executed": bool(queue_download_executed),
        "dry_run": bool(dry_run),
        "final_action": str(final_action or ""),
        "reason": str(reason or ""),
    }
    return ordered_export_row(payload, SHELFMARK_SELECTED_RELEASE_COLUMNS)


def _build_download_log_row(
    base_row: Mapping[str, Any],
    *,
    settings: ReleaseWorkflowSettings,
    log_index: int,
    lookup_strategy: str,
    lookup_term: str,
    candidate_count: int,
    accepted_candidate_count: int,
    retry_count: int,
    selected_release: Optional[Mapping[str, Any]],
    action: str,
    dry_run: bool,
    reason: str,
    http_status: str = "",
    error_kind: str = "",
    error_message: str = "",
    error_body: str = "",
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
        "shelfmark_source_requested": settings.source,
        "shelfmark_content_type_requested": settings.content_type,
        "selection_rule": settings.selection_rule,
        "allowed_indexers": ",".join(settings.allowed_indexers),
        "blocked_indexers": ",".join(settings.blocked_indexers),
        "required_protocol": settings.required_protocol,
        "candidate_count": str(candidate_count),
        "accepted_candidate_count": str(accepted_candidate_count),
        "retry_count": str(retry_count),
        "http_status": str(http_status or ""),
        "error_kind": str(error_kind or ""),
        "error_message": str(error_message or ""),
        "error_body": str(error_body or ""),
        "release_source": str(selected_release.get("source") or ""),
        "release_source_id": str(selected_release.get("source_id") or ""),
        "release_title": str(selected_release.get("title") or ""),
        "release_format": str(selected_release.get("format") or ""),
        "release_content_type": str(selected_release.get("content_type") or ""),
        "release_protocol": _release_protocol(selected_release),
        "release_indexer": _release_indexer(selected_release),
        "release_seeders": str(_to_int(selected_release.get("seeders"))),
        "release_size_bytes": str(_to_int(selected_release.get("size_bytes"))),
        "action": action,
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
        return "All release candidates were rejected"
    summary_bits = [
        f"{reason} ({count})"
        for reason, count in sorted(rejection_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    return "All release candidates were rejected: " + "; ".join(summary_bits)


def _is_retryable_release_error(exc: ShelfmarkApiError) -> bool:
    if exc.retryable:
        return True
    lowered = " ".join(
        filter(
            None,
            [
                str(exc.message or ""),
                str((exc.payload or {}).get("error") or ""),
                str(exc.response_body or ""),
            ],
        )
    ).lower()
    transient_markers = (
        "timed out",
        "timeout",
        "rate limit",
        "too many requests",
        "temporarily unavailable",
        "service unavailable",
        "upstream",
    )
    return any(marker in lowered for marker in transient_markers)


def _error_details(exc: ShelfmarkApiError) -> tuple[str, str, str, str]:
    payload_error = str((exc.payload or {}).get("error") or "").strip()
    message = payload_error or str(exc.message or "").strip() or "Shelfmark request failed"
    error_body = ""
    if exc.response_body:
        error_body = str(exc.response_body).strip()
    elif exc.payload:
        error_body = json.dumps(dict(exc.payload), ensure_ascii=False)
    return (
        str(exc.status_code or ""),
        str(exc.kind or "http_error"),
        message,
        error_body,
    )


def _build_shelfmark_summary_lines(result: ShelfmarkIntegrationResult) -> list[str]:
    counts = Counter(result.release_summary_counts or {})
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
        f"- Rows searched: **{counts.get('rows_searched', 0)}**",
        f"- Rows with candidates: **{counts.get('rows_with_candidates', 0)}**",
        f"- Rows selected: **{counts.get('rows_selected', 0)}**",
        f"- Rows queued: **{counts.get('rows_queued', 0)}**",
        f"- Rows skipped: **{counts.get('rows_skipped', 0)}**",
        f"- Rows filtered out: **{counts.get('rows_filtered_out', 0)}**",
        f"- Rows failed: **{counts.get('rows_failed', 0)}**",
        f"- Timeouts: **{counts.get('timeouts', 0)}**",
        f"- HTTP errors: **{counts.get('http_errors', 0)}**",
        f"- Retries attempted: **{counts.get('retries_attempted', 0)}**",
        f"- Retries exhausted: **{counts.get('retries_exhausted', 0)}**",
        "",
        "## Files",
        "- shelfmark_queue.csv / shelfmark_queue.json — opt-in Shelfmark request workflow queue",
        "- shelfmark_push_log.csv — request workflow trace",
        "- shelfmark_release_candidates.csv / shelfmark_release_candidates.json — release-search candidates and rejection details",
        "- shelfmark_selected_releases.csv — per-row release selection decisions",
        "- shelfmark_download_log.csv — release search / selection / queue-download trace",
    ]
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
