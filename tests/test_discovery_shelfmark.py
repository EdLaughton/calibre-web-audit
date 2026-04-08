from __future__ import annotations

import csv
from typing import Any

import pytest

from hardcover_tools.core.config import parse_discovery_args
from hardcover_tools.core.models import HardcoverBook, HardcoverEdition
from hardcover_tools.core.output import build_discovery_outputs
from hardcover_tools.core.shelfmark_client import ShelfmarkApiError
from hardcover_tools.core.shelfmark_export import (
    ShelfmarkIntegrationResult,
    build_shelfmark_queue,
    run_shelfmark_integration,
)


class FakeHardcoverClient:
    def __init__(self, books_by_id: dict[int, HardcoverBook], editions_by_book_id: dict[int, list[HardcoverEdition]]) -> None:
        self.books_by_id = books_by_id
        self.editions_by_book_id = editions_by_book_id
        self.calls: list[list[int]] = []

    def fetch_books_and_editions_for_books(self, ids, **kwargs):
        self.calls.append(list(ids))
        return self.books_by_id, self.editions_by_book_id


class FakeShelfmarkClient:
    def __init__(
        self,
        *,
        requests_enabled: bool = True,
        request_policy_mode: str = "request_book",
        create_response: dict | None = None,
        create_error: ShelfmarkApiError | None = None,
        release_search_results: dict[str, Any] | None = None,
        queue_response: dict | None = None,
        queue_error: ShelfmarkApiError | None = None,
    ) -> None:
        self.requests_enabled = requests_enabled
        self.request_policy_mode = request_policy_mode
        self.create_response = create_response or {"id": 701, "status": "pending"}
        self.create_error = create_error
        self.release_search_results = dict(release_search_results or {})
        self.queue_response = queue_response or {"status": "queued", "priority": 0}
        self.queue_error = queue_error
        self.login_calls: list[dict[str, object]] = []
        self.created_requests: list[dict[str, Any]] = []
        self.search_calls: list[dict[str, Any]] = []
        self.queue_calls: list[dict[str, Any]] = []

    def login(self, *, username: str, password: str, remember_me: bool = False) -> dict[str, Any]:
        self.login_calls.append({"username": username, "password": password, "remember_me": remember_me})
        return {"success": True}

    def get_request_policy(self) -> dict[str, Any]:
        return {
            "requests_enabled": self.requests_enabled,
            "defaults": {"ebook": self.request_policy_mode},
        }

    def create_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.create_error is not None:
            raise self.create_error
        self.created_requests.append(payload)
        return dict(self.create_response)

    def search_releases(self, **kwargs) -> dict[str, Any]:
        self.search_calls.append(dict(kwargs))
        key = self._release_key(kwargs)
        value = self.release_search_results.get(key, [])
        if isinstance(value, list) and value and (
            isinstance(value[0], Exception)
            or isinstance(value[0], list)
            or (isinstance(value[0], dict) and "releases" in value[0])
        ):
            next_value = value.pop(0)
            if isinstance(next_value, Exception):
                raise next_value
            if isinstance(next_value, dict) and "releases" in next_value:
                return dict(next_value)
            return {"releases": list(next_value or [])}
        if isinstance(value, Exception):
            raise value
        if isinstance(value, dict) and "releases" in value:
            return dict(value)
        return {"releases": list(value or [])}

    def queue_release(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.queue_error is not None:
            raise self.queue_error
        self.queue_calls.append(dict(payload))
        return dict(self.queue_response)

    @staticmethod
    def _release_key(params: dict[str, Any]) -> str:
        provider = str(params.get("provider") or "").strip()
        book_id = str(params.get("book_id") or "").strip()
        source = str(params.get("source") or "").strip()
        content_type = str(params.get("content_type") or "").strip()
        indexers = params.get("indexers") or []
        if isinstance(indexers, (list, tuple)):
            indexers_key = ",".join(str(item).strip() for item in indexers if str(item).strip())
        else:
            indexers_key = str(indexers or "").strip()
        if provider and book_id:
            suffix = f":{indexers_key}" if indexers_key else ""
            return f"provider:{provider}:{book_id}:{source}:{content_type}{suffix}"
        return (
            f"manual:{source}:{str(params.get('query') or '').strip()}:"
            f"{str(params.get('author') or '').strip()}:{content_type}"
            f"{f':{indexers_key}' if indexers_key else ''}"
        )


def _book(book_id: int = 101, *, slug: str = "the-last-kingdom", title: str = "The Last Kingdom") -> HardcoverBook:
    return HardcoverBook(
        id=book_id,
        title=title,
        subtitle="A Novel",
        authors="Bernard Cornwell",
        series="The Saxon Stories",
        release_date="2004-01-01",
        slug=slug,
    )


def _edition(
    edition_id: int = 501,
    *,
    book_id: int = 101,
    language: str = "English",
) -> HardcoverEdition:
    return HardcoverEdition(
        id=edition_id,
        book_id=book_id,
        title="The Last Kingdom",
        subtitle="A Novel",
        authors="Bernard Cornwell",
        edition_format="Ebook",
        reading_format="ebook",
        language=language,
        release_date="2004-01-01",
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
        "missing_position_display": "1",
    }


def _release(
    source_id: str,
    *,
    source: str = "libgen",
    title: str = "The Last Kingdom",
    author: str = "Bernard Cornwell",
    fmt: str = "epub",
    content_type: str = "ebook",
    seeders: int = 0,
    size_bytes: int = 0,
    size: str | None = None,
    protocol: str = "",
    indexer: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "source": source,
        "source_id": source_id,
        "title": title,
        "author": author,
        "format": fmt,
        "content_type": content_type,
        "language": "en",
        "seeders": seeders,
        "size_bytes": size_bytes,
        "size": size or f"{size_bytes}",
        "protocol": protocol,
        "indexer": indexer,
        "info_url": f"https://example.invalid/{source_id}",
        "download_url": f"https://example.invalid/download/{source_id}",
        "extra": extra or {},
    }


def _hardcover_client() -> FakeHardcoverClient:
    return FakeHardcoverClient({101: _book(), 102: _book(102, slug="other-book", title="Other Book")}, {101: [_edition()], 102: [_edition(edition_id=502, book_id=102)]})


def test_parse_discovery_args_supports_shelfmark_export_flags() -> None:
    config = parse_discovery_args(
        [
            "--library-root",
            "/tmp/library",
            "--export-shelfmark",
            "--shelfmark-approval",
            "safe-only",
            "--shelfmark-note",
            "Please queue this in Shelfmark",
            "--dry-run",
        ]
    )

    assert config.export_shelfmark is True
    assert config.push_shelfmark is False
    assert config.shelfmark_approval == "safe-only"
    assert config.shelfmark_note == "Please queue this in Shelfmark"
    assert config.dry_run is True


def test_parse_discovery_args_supports_shelfmark_release_flags() -> None:
    config = parse_discovery_args(
        [
            "--library-root",
            "/tmp/library",
            "--export-shelfmark-releases",
            "--shelfmark-url",
            "http://shelfmark.local:8084",
            "--shelfmark-source",
            "libgen",
            "--shelfmark-selection",
            "most_seeders",
            "--shelfmark-content-type",
            "ebook",
            "--shelfmark-min-seeders",
            "7",
            "--dry-run",
        ]
    )

    assert config.export_shelfmark_releases is True
    assert config.push_shelfmark_download is False
    assert config.shelfmark_source == "libgen"
    assert config.shelfmark_selection == "most_seeders"
    assert config.shelfmark_content_type == "ebook"
    assert config.shelfmark_min_seeders == 7


def test_parse_discovery_args_supports_shelfmark_release_hardening_flags() -> None:
    config = parse_discovery_args(
        [
            "--library-root",
            "/tmp/library",
            "--export-shelfmark-releases",
            "--shelfmark-url",
            "http://shelfmark.local:8084",
            "--shelfmark-source",
            "prowlarr",
            "--shelfmark-allowed-indexers",
            "MyAnonamouse, Anna's Archive ",
            "--shelfmark-blocked-indexers",
            " Bad Indexer ",
            "--shelfmark-require-protocol",
            "torrent",
            "--shelfmark-timeout-seconds",
            "45",
            "--shelfmark-min-interval-ms",
            "1500",
            "--shelfmark-max-retries",
            "3",
            "--shelfmark-retry-backoff-seconds",
            "1.5",
        ]
    )

    assert config.shelfmark_allowed_indexers == ("MyAnonamouse", "Anna's Archive")
    assert config.shelfmark_blocked_indexers == ("Bad Indexer",)
    assert config.shelfmark_require_protocol == "torrent"
    assert config.shelfmark_timeout_seconds == 45
    assert config.shelfmark_min_interval_ms == 1500
    assert config.shelfmark_max_retries == 3
    assert config.shelfmark_retry_backoff_seconds == 1.5


def test_parse_discovery_args_requires_connection_details_for_live_shelfmark_push() -> None:
    with pytest.raises(SystemExit):
        parse_discovery_args(["--library-root", "/tmp/library", "--push-shelfmark"])


def test_parse_discovery_args_requires_source_for_release_workflow() -> None:
    with pytest.raises(SystemExit):
        parse_discovery_args(
            [
                "--library-root",
                "/tmp/library",
                "--export-shelfmark-releases",
                "--shelfmark-url",
                "http://shelfmark.local:8084",
            ]
        )


def test_parse_discovery_args_requires_format_keywords_for_preferred_format() -> None:
    with pytest.raises(SystemExit):
        parse_discovery_args(
            [
                "--library-root",
                "/tmp/library",
                "--export-shelfmark-releases",
                "--shelfmark-url",
                "http://shelfmark.local:8084",
                "--shelfmark-source",
                "libgen",
                "--shelfmark-selection",
                "preferred-format",
            ]
        )


def test_build_shelfmark_queue_defaults_to_shortlist_only() -> None:
    hardcover_client = _hardcover_client()
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

    queue_rows = build_shelfmark_queue(
        candidates,
        hardcover_client=hardcover_client,
        approval_mode="shortlist-only",
    )

    assert [row["display_title"] for row in queue_rows] == ["The Last Kingdom", "Blank Language Pick"]
    assert [row["approval_bucket"] for row in queue_rows] == [
        "shortlist",
        "shortlist_blank_language_likely_english",
    ]


def test_build_shelfmark_queue_safe_only_excludes_blank_language_shortlist() -> None:
    hardcover_client = _hardcover_client()
    candidates = [
        _candidate(bucket="shortlist", eligible=True),
        _candidate(title="Blank Language Pick", bucket="shortlist_blank_language_likely_english", eligible=True),
    ]

    queue_rows = build_shelfmark_queue(
        candidates,
        hardcover_client=hardcover_client,
        approval_mode="safe-only",
    )

    assert [row["display_title"] for row in queue_rows] == ["The Last Kingdom"]


def test_shelfmark_push_dry_run_validates_policy_without_posting() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(requests_enabled=True, request_policy_mode="request_book")

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark=True,
        push_shelfmark=True,
        dry_run=True,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_username="alice",
        shelfmark_password="secret",
        shelfmark_client=shelfmark_client,
    )

    assert shelfmark_client.login_calls == [{"username": "alice", "password": "secret", "remember_me": False}]
    assert shelfmark_client.created_requests == []
    assert [row["action"] for row in result.push_log_rows] == ["dry-run exported", "dry-run validated"]


def test_shelfmark_push_skips_when_policy_is_not_request_book() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(requests_enabled=True, request_policy_mode="download")

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark=True,
        push_shelfmark=True,
        dry_run=False,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_username="alice",
        shelfmark_password="secret",
        shelfmark_client=shelfmark_client,
    )

    assert shelfmark_client.created_requests == []
    assert result.push_log_rows[-1]["action"] == "policy_incompatible"
    assert result.push_log_rows[-1]["request_policy_mode"] == "download"


def test_shelfmark_push_skips_duplicate_pending_requests() -> None:
    hardcover_client = _hardcover_client()
    duplicate_error = ShelfmarkApiError(
        message="duplicate",
        status_code=409,
        payload={"code": "duplicate_pending_request", "error": "Duplicate pending request"},
    )
    shelfmark_client = FakeShelfmarkClient(create_error=duplicate_error)

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark=True,
        push_shelfmark=True,
        dry_run=False,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_username="alice",
        shelfmark_password="secret",
        shelfmark_client=shelfmark_client,
    )

    assert result.push_log_rows[-1]["action"] == "duplicate_skipped"
    assert result.push_log_rows[-1]["http_status"] == "409"


def test_shelfmark_release_search_dry_run_records_candidates_without_queueing() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:libgen:ebook": [
                _release("libgen-1", seeders=8, size_bytes=4000, fmt="epub"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark=False,
        push_shelfmark=False,
        export_shelfmark_releases=True,
        push_shelfmark_download=False,
        dry_run=True,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="libgen",
        shelfmark_content_type="ebook",
        shelfmark_selection="best",
        shelfmark_client=shelfmark_client,
    )

    assert len(result.release_candidate_rows) == 1
    assert len(result.selected_release_rows) == 1
    assert shelfmark_client.queue_calls == []
    assert result.selected_release_rows[0]["selected_release_source_id"] == "libgen-1"
    assert result.download_log_rows[-1]["action"] == "selected"
    assert shelfmark_client.search_calls[0]["source"] == "libgen"
    assert shelfmark_client.search_calls[0]["content_type"] == "ebook"


def test_shelfmark_release_search_filters_by_content_type_and_min_seeders() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:libgen:ebook": [
                _release("low-seeders", seeders=1, size_bytes=5000, fmt="epub"),
                _release("audio", seeders=20, size_bytes=6000, fmt="m4b", content_type="audiobook"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark=False,
        push_shelfmark=False,
        export_shelfmark_releases=True,
        push_shelfmark_download=False,
        dry_run=False,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="libgen",
        shelfmark_content_type="ebook",
        shelfmark_selection="best",
        shelfmark_min_seeders=5,
        shelfmark_client=shelfmark_client,
    )

    assert result.selected_release_rows[0]["accepted_candidate_count"] == "0"
    assert "seeders below minimum" in result.selected_release_rows[0]["reason"]
    assert "content type mismatch" in result.selected_release_rows[0]["reason"]


def test_shelfmark_release_selection_first_is_deterministic() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:libgen:ebook": [
                _release("first", seeders=1, size_bytes=1000, fmt="pdf"),
                _release("second", seeders=50, size_bytes=9000, fmt="epub"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark=False,
        push_shelfmark=False,
        export_shelfmark_releases=True,
        dry_run=False,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="libgen",
        shelfmark_content_type="ebook",
        shelfmark_selection="first",
        shelfmark_client=shelfmark_client,
    )

    assert result.selected_release_rows[0]["selected_release_source_id"] == "first"


def test_shelfmark_release_selection_most_seeders_is_deterministic() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:libgen:ebook": [
                _release("a", seeders=5, size_bytes=5000, fmt="epub"),
                _release("b", seeders=15, size_bytes=4000, fmt="pdf"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark=False,
        push_shelfmark=False,
        export_shelfmark_releases=True,
        dry_run=False,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="libgen",
        shelfmark_content_type="ebook",
        shelfmark_selection="most_seeders",
        shelfmark_client=shelfmark_client,
    )

    assert result.selected_release_rows[0]["selected_release_source_id"] == "b"


def test_shelfmark_release_selection_largest_is_deterministic() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:libgen:ebook": [
                _release("small", seeders=50, size_bytes=2000, fmt="epub"),
                _release("large", seeders=5, size_bytes=9000, fmt="epub"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark=False,
        push_shelfmark=False,
        export_shelfmark_releases=True,
        dry_run=False,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="libgen",
        shelfmark_content_type="ebook",
        shelfmark_selection="largest",
        shelfmark_client=shelfmark_client,
    )

    assert result.selected_release_rows[0]["selected_release_source_id"] == "large"


def test_shelfmark_release_selection_preferred_format_is_deterministic() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:libgen:ebook": [
                _release("pdf", seeders=20, size_bytes=7000, fmt="pdf"),
                _release("epub", seeders=10, size_bytes=6000, fmt="epub"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark=False,
        push_shelfmark=False,
        export_shelfmark_releases=True,
        dry_run=False,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="libgen",
        shelfmark_content_type="ebook",
        shelfmark_selection="preferred-format",
        shelfmark_format_keywords=("epub", "pdf"),
        shelfmark_client=shelfmark_client,
    )

    assert result.selected_release_rows[0]["selected_release_source_id"] == "epub"


def test_shelfmark_release_selection_best_can_use_format_keywords() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:libgen:ebook": [
                _release("pdf", seeders=40, size_bytes=7000, fmt="pdf"),
                _release("epub", seeders=5, size_bytes=6000, fmt="epub"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark=False,
        push_shelfmark=False,
        export_shelfmark_releases=True,
        dry_run=False,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="libgen",
        shelfmark_content_type="ebook",
        shelfmark_selection="best",
        shelfmark_format_keywords=("epub", "pdf"),
        shelfmark_client=shelfmark_client,
    )

    assert result.selected_release_rows[0]["selected_release_source_id"] == "epub"


def test_shelfmark_push_download_dry_run_does_not_queue() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:libgen:ebook": [
                _release("queue-me", seeders=8, size_bytes=4000, fmt="epub"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark=False,
        push_shelfmark=False,
        export_shelfmark_releases=True,
        push_shelfmark_download=True,
        dry_run=True,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="libgen",
        shelfmark_content_type="ebook",
        shelfmark_selection="best",
        shelfmark_client=shelfmark_client,
    )

    assert result.selected_release_rows[0]["queue_download_requested"] is True
    assert result.selected_release_rows[0]["queue_download_executed"] is False
    assert shelfmark_client.queue_calls == []
    assert result.download_log_rows[-1]["action"] == "dry-run queued"


def test_shelfmark_push_download_queues_selected_release() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:libgen:ebook": [
                _release("queue-me", seeders=8, size_bytes=4000, fmt="epub"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark=False,
        push_shelfmark=False,
        export_shelfmark_releases=True,
        push_shelfmark_download=True,
        dry_run=False,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="libgen",
        shelfmark_content_type="ebook",
        shelfmark_selection="best",
        shelfmark_client=shelfmark_client,
    )

    assert len(shelfmark_client.queue_calls) == 1
    assert shelfmark_client.queue_calls[0]["source_id"] == "queue-me"
    assert result.selected_release_rows[0]["queue_download_executed"] is True
    assert result.download_log_rows[-1]["action"] == "queued"


def test_shelfmark_release_search_http_503_is_logged_and_skipped() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:prowlarr:ebook": ShelfmarkApiError(
                message="source temporarily unavailable",
                status_code=503,
                response_body='{"error":"prowlarr"}',
                payload={"error": "prowlarr"},
                kind="http_error",
                retryable=False,
            ),
            "manual:prowlarr:The Last Kingdom:Bernard Cornwell:ebook": [],
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark_releases=True,
        push_shelfmark=False,
        export_shelfmark=False,
        push_shelfmark_download=False,
        dry_run=True,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="prowlarr",
        shelfmark_content_type="ebook",
        shelfmark_selection="best",
        shelfmark_max_retries=0,
        shelfmark_client=shelfmark_client,
    )

    final_row = result.selected_release_rows[0]
    assert final_row["final_action"] == "skipped"
    assert result.download_log_rows[0]["action"] == "http_error"
    assert result.download_log_rows[0]["http_status"] == "503"
    assert result.download_log_rows[0]["error_message"] == "prowlarr"
    assert result.release_summary_counts["http_errors"] == 1
    assert result.release_summary_counts["rows_skipped"] == 1


def test_shelfmark_release_search_timeout_retries_then_recovers() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:libgen:ebook": [
                ShelfmarkApiError(
                    message="Shelfmark request timed out for GET /api/releases",
                    kind="timeout",
                    retryable=True,
                ),
                [_release("retry-win", seeders=12, size_bytes=5000, fmt="epub")],
            ]
        }
    )
    slept: list[float] = []
    monotonic_values = iter([0.0, 0.0, 0.0, 0.0, 5.0, 5.0])

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark_releases=True,
        push_shelfmark=False,
        export_shelfmark=False,
        push_shelfmark_download=False,
        dry_run=True,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="libgen",
        shelfmark_content_type="ebook",
        shelfmark_selection="best",
        shelfmark_max_retries=1,
        shelfmark_retry_backoff_seconds=2.0,
        shelfmark_client=shelfmark_client,
        sleep_fn=lambda seconds: slept.append(seconds),
        monotonic_fn=lambda: next(monotonic_values),
        log_fn=lambda _: None,
    )

    assert result.selected_release_rows[0]["final_action"] == "selected"
    assert result.selected_release_rows[0]["retry_count"] == "1"
    assert result.release_summary_counts["timeouts"] == 1
    assert result.release_summary_counts["retries_attempted"] == 1
    assert slept[0] == 2.0
    assert slept[1] == 1.0


def test_shelfmark_release_search_timeout_retries_exhausted() -> None:
    hardcover_client = _hardcover_client()
    timeout_error = ShelfmarkApiError(
        message="Shelfmark request timed out for GET /api/releases",
        kind="timeout",
        retryable=True,
    )
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:libgen:ebook": [timeout_error, timeout_error],
            "manual:libgen:The Last Kingdom:Bernard Cornwell:ebook": [],
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark_releases=True,
        push_shelfmark=False,
        export_shelfmark=False,
        push_shelfmark_download=False,
        dry_run=True,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="libgen",
        shelfmark_content_type="ebook",
        shelfmark_selection="best",
        shelfmark_max_retries=1,
        shelfmark_client=shelfmark_client,
        sleep_fn=lambda _: None,
        monotonic_fn=lambda: 0.0,
        log_fn=lambda _: None,
    )

    assert result.selected_release_rows[0]["final_action"] == "skipped"
    assert result.release_summary_counts["timeouts"] == 2
    assert result.release_summary_counts["retries_exhausted"] == 1


def test_shelfmark_release_search_continues_after_per_row_failure() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:libgen:ebook": ShelfmarkApiError(
                message="request failed",
                status_code=503,
                response_body='{"error":"libgen"}',
                payload={"error": "libgen"},
                kind="http_error",
                retryable=False,
            ),
            "manual:libgen:The Last Kingdom:Bernard Cornwell:ebook": [],
            "provider:hardcover:102:libgen:ebook": [
                _release("other-win", title="Other Book", author="Bernard Cornwell", seeders=9, size_bytes=4100),
            ],
        }
    )

    result = run_shelfmark_integration(
        [
            _candidate(display_book_id="101", preferred_edition_id="501"),
            _candidate(title="Other Book", display_book_id="102", preferred_edition_id="502"),
        ],
        hardcover_client=hardcover_client,
        export_shelfmark_releases=True,
        push_shelfmark=False,
        export_shelfmark=False,
        push_shelfmark_download=False,
        dry_run=True,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="libgen",
        shelfmark_content_type="ebook",
        shelfmark_selection="best",
        shelfmark_max_retries=0,
        shelfmark_client=shelfmark_client,
        log_fn=lambda _: None,
    )

    assert len(result.selected_release_rows) == 2
    assert result.selected_release_rows[0]["final_action"] == "skipped"
    assert result.selected_release_rows[1]["final_action"] == "selected"
    assert result.release_summary_counts["rows_selected"] == 1


def test_shelfmark_release_search_allowlisted_indexers_only() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:prowlarr:ebook:MyAnonamouse": [
                _release("allowed", source="prowlarr", fmt="epub", seeders=12, size_bytes=5000, indexer="MyAnonamouse", protocol="torrent"),
                _release("blocked", source="prowlarr", fmt="epub", seeders=20, size_bytes=9000, indexer="Other Indexer", protocol="torrent"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark_releases=True,
        push_shelfmark=False,
        export_shelfmark=False,
        push_shelfmark_download=False,
        dry_run=True,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="prowlarr",
        shelfmark_content_type="ebook",
        shelfmark_selection="most_seeders",
        shelfmark_allowed_indexers=("MyAnonamouse",),
        shelfmark_client=shelfmark_client,
        log_fn=lambda _: None,
    )

    assert shelfmark_client.search_calls[0]["indexers"] == ["MyAnonamouse"]
    assert result.selected_release_rows[0]["selected_release_source_id"] == "allowed"
    rejected = [row for row in result.release_candidate_rows if row["candidate_status"] == "rejected"]
    assert rejected[0]["indexer_filter_decision"] == "not_allowlisted"


def test_shelfmark_release_search_blocked_indexers_are_rejected() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:prowlarr:ebook": [
                _release("blocked", source="prowlarr", fmt="epub", seeders=30, size_bytes=8000, indexer="Bad Indexer", protocol="torrent"),
                _release("allowed", source="prowlarr", fmt="epub", seeders=5, size_bytes=3000, indexer="Good Indexer", protocol="torrent"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark_releases=True,
        push_shelfmark=False,
        export_shelfmark=False,
        push_shelfmark_download=False,
        dry_run=True,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="prowlarr",
        shelfmark_content_type="ebook",
        shelfmark_selection="best",
        shelfmark_blocked_indexers=("bad indexer",),
        shelfmark_client=shelfmark_client,
        log_fn=lambda _: None,
    )

    assert result.selected_release_rows[0]["selected_release_source_id"] == "allowed"
    blocked_row = next(row for row in result.release_candidate_rows if row["release_source_id"] == "blocked")
    assert blocked_row["indexer_filter_decision"] == "blocklisted"


def test_shelfmark_release_search_protocol_filter_applies_before_selection() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:prowlarr:ebook": [
                _release("http", source="prowlarr", fmt="epub", seeders=50, size_bytes=9000, indexer="MyAnonamouse", protocol="http"),
                _release("torrent", source="prowlarr", fmt="epub", seeders=5, size_bytes=5000, indexer="MyAnonamouse", protocol="torrent"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark_releases=True,
        push_shelfmark=False,
        export_shelfmark=False,
        push_shelfmark_download=False,
        dry_run=True,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="prowlarr",
        shelfmark_content_type="ebook",
        shelfmark_selection="most_seeders",
        shelfmark_require_protocol="torrent",
        shelfmark_client=shelfmark_client,
        log_fn=lambda _: None,
    )

    assert result.selected_release_rows[0]["selected_release_source_id"] == "torrent"
    rejected_row = next(row for row in result.release_candidate_rows if row["release_source_id"] == "http")
    assert rejected_row["protocol_filter_decision"] == "protocol_mismatch"


def test_shelfmark_release_search_skips_when_all_candidates_filtered_out() -> None:
    hardcover_client = _hardcover_client()
    shelfmark_client = FakeShelfmarkClient(
        release_search_results={
            "provider:hardcover:101:prowlarr:ebook:MyAnonamouse": [
                _release("one", source="prowlarr", fmt="pdf", seeders=1, size_bytes=1000, indexer="Bad One", protocol="http"),
                _release("two", source="prowlarr", fmt="m4b", seeders=0, size_bytes=2000, content_type="audiobook", indexer="Bad Two", protocol="http"),
            ]
        }
    )

    result = run_shelfmark_integration(
        [_candidate()],
        hardcover_client=hardcover_client,
        export_shelfmark_releases=True,
        push_shelfmark=False,
        export_shelfmark=False,
        push_shelfmark_download=False,
        dry_run=True,
        approval_mode="shortlist-only",
        shelfmark_url="http://shelfmark.local:8084",
        shelfmark_source="prowlarr",
        shelfmark_content_type="ebook",
        shelfmark_selection="best",
        shelfmark_format_keywords=("epub",),
        shelfmark_allowed_indexers=("MyAnonamouse",),
        shelfmark_require_protocol="torrent",
        shelfmark_min_seeders=5,
        shelfmark_client=shelfmark_client,
        log_fn=lambda _: None,
    )

    assert result.selected_release_rows[0]["final_action"] == "filtered_out"
    assert result.release_summary_counts["rows_filtered_out"] == 1
    assert result.download_log_rows[-1]["action"] == "filtered_out"


def test_discovery_outputs_write_request_and_release_shelfmark_artifacts_with_stable_headers(tmp_path) -> None:
    candidates = [_candidate()]
    shelfmark_result = ShelfmarkIntegrationResult(
        request_workflow_enabled=True,
        release_workflow_enabled=True,
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
                "display_title": "The Last Kingdom",
                "display_subtitle": "A Novel",
                "display_authors": "Bernard Cornwell",
                "display_series": "The Saxon Stories",
                "series_position": "1",
                "owned_author_names": "Bernard Cornwell",
                "gap_kind": "",
                "hardcover-id": "101",
                "hardcover-slug": "the-last-kingdom",
                "hardcover-edition": "501",
                "release_year": "2004",
                "preferred_edition_language": "English",
                "preferred_edition_format_normalized": "ebook",
                "shelfmark_provider": "hardcover",
                "shelfmark_provider_id": "101",
                "shelfmark_content_type": "ebook",
                "shelfmark_request_level": "book",
                "shelfmark_source": "*",
                "shelfmark_source_url": "https://hardcover.app/books/the-last-kingdom",
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
                "display_title": "The Last Kingdom",
                "display_authors": "Bernard Cornwell",
                "hardcover-id": "101",
                "hardcover-slug": "the-last-kingdom",
                "hardcover-edition": "501",
                "requests_enabled": True,
                "request_policy_mode": "request_book",
                "request_policy_required_mode": "",
                "request_context_source": "*",
                "request_context_level": "book",
                "action": "exported",
                "http_status": "",
                "shelfmark_request_id": "",
                "dry_run": False,
                "reason": "queued for Shelfmark integration",
            }
        ],
        release_candidate_rows=[
            {
                "candidate_id": "1",
                "row_id": "1",
                "source_row_number": "1",
                "approval_mode": "shortlist-only",
                "approval_bucket": "shortlist",
                "discovery_bucket": "missing_series",
                "discovery_priority_bucket": "shortlist",
                "display_title": "The Last Kingdom",
                "display_authors": "Bernard Cornwell",
                "hardcover-id": "101",
                "hardcover-slug": "the-last-kingdom",
                "hardcover-edition": "501",
                "lookup_strategy": "hardcover_provider_book_id",
                "lookup_term": "provider=hardcover book_id=101 source=libgen content_type=ebook",
                "shelfmark_source_requested": "libgen",
                "shelfmark_content_type_requested": "ebook",
                "selection_rule": "best",
                "format_keywords": "epub",
                "min_seeders": "0",
                "release_index": "1",
                "release_source": "libgen",
                "release_source_id": "libgen-1",
                "release_title": "The Last Kingdom",
                "release_author": "Bernard Cornwell",
                "release_format": "epub",
                "release_content_type": "ebook",
                "release_language": "en",
                "release_size": "4000",
                "release_size_bytes": "4000",
                "release_seeders": "8",
                "release_info_url": "https://example.invalid/libgen-1",
                "release_download_url": "https://example.invalid/download/libgen-1",
                "candidate_status": "accepted",
                "rejection_reasons": "",
            }
        ],
        selected_release_rows=[
            {
                "row_id": "1",
                "source_row_number": "1",
                "approval_mode": "shortlist-only",
                "approval_bucket": "shortlist",
                "discovery_bucket": "missing_series",
                "discovery_priority_bucket": "shortlist",
                "display_title": "The Last Kingdom",
                "display_authors": "Bernard Cornwell",
                "hardcover-id": "101",
                "hardcover-slug": "the-last-kingdom",
                "hardcover-edition": "501",
                "lookup_strategy": "hardcover_provider_book_id",
                "lookup_attempts": "hardcover_provider_book_id",
                "lookup_term": "provider=hardcover book_id=101 source=libgen content_type=ebook",
                "shelfmark_source_requested": "libgen",
                "shelfmark_content_type_requested": "ebook",
                "selection_rule": "best",
                "format_keywords": "epub",
                "min_seeders": "0",
                "candidate_count": "1",
                "accepted_candidate_count": "1",
                "rejected_candidate_count": "0",
                "selected_release_source": "libgen",
                "selected_release_source_id": "libgen-1",
                "selected_release_title": "The Last Kingdom",
                "selected_release_author": "Bernard Cornwell",
                "selected_release_format": "epub",
                "selected_release_content_type": "ebook",
                "selected_release_language": "en",
                "selected_release_size": "4000",
                "selected_release_size_bytes": "4000",
                "selected_release_seeders": "8",
                "selected_release_info_url": "https://example.invalid/libgen-1",
                "selected_release_download_url": "https://example.invalid/download/libgen-1",
                "queue_download_requested": False,
                "queue_download_executed": False,
                "dry_run": False,
                "reason": "best selected the most preferred format (epub)",
            }
        ],
        download_log_rows=[
            {
                "log_index": "1",
                "row_id": "1",
                "source_row_number": "1",
                "approval_mode": "shortlist-only",
                "approval_bucket": "shortlist",
                "discovery_bucket": "missing_series",
                "display_title": "The Last Kingdom",
                "display_authors": "Bernard Cornwell",
                "hardcover-id": "101",
                "hardcover-slug": "the-last-kingdom",
                "hardcover-edition": "501",
                "lookup_strategy": "hardcover_provider_book_id",
                "lookup_term": "provider=hardcover book_id=101 source=libgen content_type=ebook",
                "shelfmark_source_requested": "libgen",
                "shelfmark_content_type_requested": "ebook",
                "selection_rule": "best",
                "candidate_count": "1",
                "accepted_candidate_count": "1",
                "release_source": "libgen",
                "release_source_id": "libgen-1",
                "release_title": "The Last Kingdom",
                "release_format": "epub",
                "release_content_type": "ebook",
                "release_seeders": "8",
                "release_size_bytes": "4000",
                "action": "selected_only",
                "http_status": "",
                "dry_run": False,
                "reason": "export-only mode did not queue download",
            }
        ],
        summary_lines=["# Shelfmark summary", "", "- Queue rows written: **1**"],
        requests_enabled=True,
        request_policy_mode="request_book",
    )

    output_paths = build_discovery_outputs(candidates, tmp_path, shelfmark_result=shelfmark_result)

    assert output_paths.shelfmark_queue is not None
    assert output_paths.shelfmark_queue_json is not None
    assert output_paths.shelfmark_push_log is not None
    assert output_paths.shelfmark_release_candidates is not None
    assert output_paths.shelfmark_release_candidates_json is not None
    assert output_paths.shelfmark_selected_releases is not None
    assert output_paths.shelfmark_download_log is not None
    assert output_paths.shelfmark_summary is not None
    assert output_paths.shelfmark_queue.exists()
    assert output_paths.shelfmark_push_log.exists()
    assert output_paths.shelfmark_release_candidates.exists()
    assert output_paths.shelfmark_selected_releases.exists()
    assert output_paths.shelfmark_download_log.exists()

    summary_text = output_paths.summary.read_text(encoding="utf-8")
    assert "## Shelfmark" in summary_text
    assert "Shelfmark selected release rows" in summary_text

    with output_paths.shelfmark_queue.open(newline="", encoding="utf-8") as handle:
        queue_header = next(csv.reader(handle))
    with output_paths.shelfmark_push_log.open(newline="", encoding="utf-8") as handle:
        push_log_header = next(csv.reader(handle))
    with output_paths.shelfmark_release_candidates.open(newline="", encoding="utf-8") as handle:
        release_candidate_header = next(csv.reader(handle))
    with output_paths.shelfmark_selected_releases.open(newline="", encoding="utf-8") as handle:
        selected_header = next(csv.reader(handle))
    with output_paths.shelfmark_download_log.open(newline="", encoding="utf-8") as handle:
        download_log_header = next(csv.reader(handle))

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
    assert release_candidate_header[:6] == [
        "candidate_id",
        "row_id",
        "source_row_number",
        "approval_mode",
        "approval_bucket",
        "discovery_bucket",
    ]
    assert selected_header[:6] == [
        "row_id",
        "source_row_number",
        "approval_mode",
        "approval_bucket",
        "discovery_bucket",
        "discovery_priority_bucket",
    ]
    assert download_log_header[:6] == [
        "log_index",
        "row_id",
        "source_row_number",
        "approval_mode",
        "approval_bucket",
        "discovery_bucket",
    ]
