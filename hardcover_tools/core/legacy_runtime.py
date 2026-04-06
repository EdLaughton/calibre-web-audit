from __future__ import annotations

import hardcover_calibre_audit as _legacy

legacy = _legacy

HARDCOVER_DEFAULT_USER_AGENT = _legacy.HARDCOVER_DEFAULT_USER_AGENT
CACHE_FILENAME = _legacy.CACHE_FILENAME
LEGACY_CACHE_FILENAME = _legacy.LEGACY_CACHE_FILENAME
DEFAULT_CACHE_TTL_HOURS = _legacy.DEFAULT_CACHE_TTL_HOURS
DEFAULT_SEARCH_CACHE_TTL_HOURS = _legacy.DEFAULT_SEARCH_CACHE_TTL_HOURS
DEFAULT_EMPTY_CACHE_TTL_HOURS = _legacy.DEFAULT_EMPTY_CACHE_TTL_HOURS
DEFAULT_EDITION_CACHE_TTL_HOURS = _legacy.DEFAULT_EDITION_CACHE_TTL_HOURS
DEFAULT_PROGRESS_EVERY = _legacy.DEFAULT_PROGRESS_EVERY

build_compact_audit_actions = _legacy._build_compact_audit_actions
filter_compact_write_plan_rows = _legacy._filter_compact_write_plan_rows
build_write_plan = _legacy.build_write_plan
bucket_sort_key = _legacy.bucket_sort_key
classify_manual_review_bucket = _legacy.classify_manual_review_bucket

__all__ = [
    "legacy",
    "HARDCOVER_DEFAULT_USER_AGENT",
    "CACHE_FILENAME",
    "LEGACY_CACHE_FILENAME",
    "DEFAULT_CACHE_TTL_HOURS",
    "DEFAULT_SEARCH_CACHE_TTL_HOURS",
    "DEFAULT_EMPTY_CACHE_TTL_HOURS",
    "DEFAULT_EDITION_CACHE_TTL_HOURS",
    "DEFAULT_PROGRESS_EVERY",
    "build_compact_audit_actions",
    "filter_compact_write_plan_rows",
    "build_write_plan",
    "bucket_sort_key",
    "classify_manual_review_bucket",
]
