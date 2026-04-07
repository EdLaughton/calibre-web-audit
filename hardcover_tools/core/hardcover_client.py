from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib import error as urllib_error, request as urllib_request

from .audit_reporting import preview_names
from .identifiers import clean_isbn
from .models import HardcoverBook, HardcoverEdition
from .runtime_defaults import (
    DEFAULT_CACHE_TTL_HOURS,
    DEFAULT_EDITION_CACHE_TTL_HOURS,
    DEFAULT_EMPTY_CACHE_TTL_HOURS,
    DEFAULT_SEARCH_CACHE_TTL_HOURS,
    HARDCOVER_DEFAULT_USER_AGENT,
)
from .runtime_io import ensure_dir
from .text_normalization import (
    authors_from_contributions,
    is_primary_author_contribution,
    normalize_author_key,
    normalize_person_name,
    smart_title,
)

GRAPHQL_ENDPOINT = "https://api.hardcover.app/v1/graphql"
QUIET_HC_LABELS = {
    "book_single",
    "books",
    "book_editions",
    "book_editions_single",
    "series_books",
    "book_series_memberships",
    "editions_by_id",
    "author_books",
    "books_and_editions",
}


def vlog(verbose: bool, message: str) -> None:
    if verbose:
        print(message, flush=True)


def _log_label(text: Any, max_len: int = 60) -> str:
    value = " ".join(str(text or "-").split())
    return value or "-"


def chunked(sequence: List[Any], size: int) -> List[List[Any]]:
    if size <= 0:
        return [sequence[:]]
    return [sequence[index : index + size] for index in range(0, len(sequence), size)]


@dataclass
class HardcoverRequestMeta:
    label: str = ""
    transport: str = ""
    status_code: int = 0
    duration_s: float = 0.0
    attempt: int = 0
    from_cache: bool = False
    retry_after: str = ""
    rate_limit_limit: str = ""
    rate_limit_remaining: str = ""
    rate_limit_reset: str = ""
    error_summary: str = ""
    cache_key: str = ""
    cache_detail: str = ""


class HardcoverRequestFailure(RuntimeError):
    def __init__(self, message: str, meta: Optional[HardcoverRequestMeta] = None):
        super().__init__(message)
        self.meta = meta or HardcoverRequestMeta()


@dataclass
class CacheEntry:
    key: str
    label: str
    payload: Any
    created_at: float
    updated_at: float
    last_accessed_at: float
    is_empty: bool
    payload_size: int
    source_query: str = ""
    api_object_type: str = ""
    api_object_id: str = ""


class SQLiteCacheStore:
    def __init__(self, path: Path, verbose: bool = False, legacy_json_path: Optional[Path] = None):
        self.path = path
        self.verbose = bool(verbose)
        self.legacy_json_path = legacy_json_path
        self.imported_legacy_entries = 0
        ensure_dir(path.parent)
        self.conn = sqlite3.connect(str(path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()
        self._maybe_import_legacy_json()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache_entries (
                cache_key TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                is_empty INTEGER NOT NULL DEFAULT 0,
                payload_size INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_accessed_at REAL NOT NULL DEFAULT 0,
                source_query TEXT NOT NULL DEFAULT '',
                api_object_type TEXT NOT NULL DEFAULT '',
                api_object_id TEXT NOT NULL DEFAULT ''
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cache_entries_label ON cache_entries(label)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cache_entries_updated_at ON cache_entries(updated_at)")
        self._ensure_column("cache_entries", "last_accessed_at", "REAL NOT NULL DEFAULT 0")
        self._ensure_column("cache_entries", "source_query", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("cache_entries", "api_object_type", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("cache_entries", "api_object_id", "TEXT NOT NULL DEFAULT ''")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache_meta (
                meta_key TEXT PRIMARY KEY,
                meta_value TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def _ensure_column(self, table: str, column: str, ddl: str) -> None:
        columns = {str(row[1]) for row in self.conn.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
            self.conn.commit()

    def _get_meta(self, key: str) -> str:
        row = self.conn.execute("SELECT meta_value FROM cache_meta WHERE meta_key = ?", (key,)).fetchone()
        return str(row[0]) if row else ""

    def _set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            """
            INSERT INTO cache_meta(meta_key, meta_value)
            VALUES(?, ?)
            ON CONFLICT(meta_key) DO UPDATE SET meta_value=excluded.meta_value
            """,
            (key, value),
        )
        self.conn.commit()

    def _maybe_import_legacy_json(self) -> None:
        legacy_path = self.legacy_json_path
        if not legacy_path or not legacy_path.exists():
            return
        if self._get_meta("legacy_json_imported"):
            return
        existing_count = int(self.conn.execute("SELECT COUNT(*) FROM cache_entries").fetchone()[0] or 0)
        if existing_count > 0:
            self._set_meta("legacy_json_imported", "skipped_nonempty_db")
            return
        try:
            raw = json.loads(legacy_path.read_text(encoding="utf-8"))
        except Exception:
            self._set_meta("legacy_json_imported", "failed_to_parse")
            return
        if not isinstance(raw, dict) or not raw:
            self._set_meta("legacy_json_imported", "empty_or_invalid")
            return
        now = time.time()
        rows = []
        for key, payload in raw.items():
            try:
                payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                continue
            label = (str(key or "").split("::", 1)[0] or "graphql").strip()
            rows.append(
                (
                    str(key),
                    label,
                    payload_json,
                    1 if payload in (None, {}) else 0,
                    len(payload_json.encode("utf-8")),
                    now,
                    now,
                    now,
                    "",
                    "",
                    "",
                )
            )
        if rows:
            self.conn.executemany(
                """
                INSERT OR REPLACE INTO cache_entries(
                    cache_key,
                    label,
                    payload_json,
                    is_empty,
                    payload_size,
                    created_at,
                    updated_at,
                    last_accessed_at,
                    source_query,
                    api_object_type,
                    api_object_id
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self.conn.commit()
            self.imported_legacy_entries = len(rows)
            vlog(self.verbose, f"  HC CACHE IMPORT source={legacy_path} entries={len(rows)}")
            self._set_meta("legacy_json_imported", f"imported:{len(rows)}")
        else:
            self._set_meta("legacy_json_imported", "no_valid_rows")

    def get(self, cache_key: str) -> Optional[CacheEntry]:
        row = self.conn.execute(
            """
            SELECT cache_key, label, payload_json, is_empty, payload_size, created_at, updated_at,
                   last_accessed_at, source_query, api_object_type, api_object_id
            FROM cache_entries
            WHERE cache_key = ?
            """,
            (cache_key,),
        ).fetchone()
        if not row:
            return None
        try:
            payload = json.loads(row["payload_json"])
        except Exception:
            self.delete(cache_key)
            return None
        self.conn.execute(
            "UPDATE cache_entries SET last_accessed_at = ? WHERE cache_key = ?",
            (time.time(), cache_key),
        )
        self.conn.commit()
        return CacheEntry(
            key=str(row["cache_key"]),
            label=str(row["label"]),
            payload=payload,
            created_at=float(row["created_at"] or 0.0),
            updated_at=float(row["updated_at"] or 0.0),
            last_accessed_at=float(row["last_accessed_at"] or 0.0),
            is_empty=bool(row["is_empty"]),
            payload_size=int(row["payload_size"] or 0),
            source_query=str(row["source_query"] or ""),
            api_object_type=str(row["api_object_type"] or ""),
            api_object_id=str(row["api_object_id"] or ""),
        )

    def _metadata_from_cache_key(self, cache_key: str) -> Tuple[str, str, str]:
        text = str(cache_key or "")
        if "::" not in text:
            return ("", "", "")
        prefix, rest = text.split("::", 1)
        source_query = ""
        api_object_type = ""
        api_object_id = ""
        if prefix.startswith("search_book"):
            source_query = rest.split("::", 1)[0]
            api_object_type = "book_search"
        elif prefix.startswith("identifier_book_lookup"):
            source_query = rest
            api_object_type = "identifier_lookup"
        elif prefix.startswith("book_editions"):
            api_object_type = "edition"
            api_object_id = rest
        elif prefix.startswith("book_single") or prefix.startswith("books"):
            api_object_type = "book"
            api_object_id = rest
        elif prefix.startswith("series_books"):
            api_object_type = "series"
            api_object_id = rest
        return (source_query[:500], api_object_type[:100], api_object_id[:200])

    def set(self, cache_key: str, label: str, payload: Any, is_empty: bool = False) -> None:
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        now = time.time()
        source_query, api_object_type, api_object_id = self._metadata_from_cache_key(cache_key)
        self.conn.execute(
            """
            INSERT INTO cache_entries(
                cache_key, label, payload_json, is_empty, payload_size, created_at,
                updated_at, last_accessed_at, source_query, api_object_type, api_object_id
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                label=excluded.label,
                payload_json=excluded.payload_json,
                is_empty=excluded.is_empty,
                payload_size=excluded.payload_size,
                updated_at=excluded.updated_at,
                last_accessed_at=excluded.last_accessed_at,
                source_query=excluded.source_query,
                api_object_type=excluded.api_object_type,
                api_object_id=excluded.api_object_id
            """,
            (
                cache_key,
                label,
                payload_json,
                1 if is_empty else 0,
                len(payload_json.encode("utf-8")),
                now,
                now,
                now,
                source_query,
                api_object_type,
                api_object_id,
            ),
        )
        self.conn.commit()

    def delete(self, cache_key: str) -> None:
        self.conn.execute("DELETE FROM cache_entries WHERE cache_key = ?", (cache_key,))
        self.conn.commit()

    def checkpoint(self) -> None:
        try:
            self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except Exception:
            pass

    def close(self) -> None:
        try:
            self.checkpoint()
        finally:
            self.conn.close()


class HardcoverClient:
    def __init__(
        self,
        token: str,
        cache_path: Path,
        timeout: int = 15,
        retries: int = 2,
        user_agent: str = HARDCOVER_DEFAULT_USER_AGENT,
        min_interval: float = 1.0,
        verbose: bool = False,
        cache_ttl_hours: float = DEFAULT_CACHE_TTL_HOURS,
        search_cache_ttl_hours: float = DEFAULT_SEARCH_CACHE_TTL_HOURS,
        empty_cache_ttl_hours: float = DEFAULT_EMPTY_CACHE_TTL_HOURS,
        edition_cache_ttl_hours: float = DEFAULT_EDITION_CACHE_TTL_HOURS,
        legacy_cache_json_path: Optional[Path] = None,
        debug_hardcover: bool = False,
    ):
        self.token = token
        self.cache_path = cache_path
        self.timeout = max(3, int(timeout))
        self.retries = max(0, int(retries))
        self.user_agent = (user_agent or HARDCOVER_DEFAULT_USER_AGENT).strip()
        self.base_min_interval = max(0.0, float(min_interval))
        self.min_interval = self.base_min_interval
        self.verbose = bool(verbose)
        self.cache_ttl_hours = max(0.0, float(cache_ttl_hours))
        self.search_cache_ttl_hours = max(0.0, float(search_cache_ttl_hours))
        self.empty_cache_ttl_hours = max(0.0, float(empty_cache_ttl_hours))
        self.edition_cache_ttl_hours = max(0.0, float(edition_cache_ttl_hours))
        self.debug_hardcover = bool(debug_hardcover)
        self._quiet_hc_labels = set(QUIET_HC_LABELS)
        self._last_request_ts = 0.0
        self._cooldown_until_ts = 0.0
        self._rate_limit_streak = 0
        self._curl_path = shutil.which("curl")
        self.last_request_meta = HardcoverRequestMeta()
        self.stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "cache_stale": 0,
            "cache_writes": 0,
            "cache_deletes": 0,
            "network_requests": 0,
            "retries": 0,
            "rate_limit_hits": 0,
            "unauthorized_hits": 0,
            "graphql_errors": 0,
            "empty_responses": 0,
            "request_failures": 0,
            "http_status_counts": Counter(),
            "label_counts": Counter(),
            "cache_hit_labels": Counter(),
            "transport_counts": Counter(),
            "throttle_sleeps": 0,
            "throttle_sleep_seconds": 0.0,
            "suppressed_cache_hit_logs": 0,
            "suppressed_cache_store_logs": 0,
            "suppressed_http_logs": 0,
            "suppressed_throttle_logs": 0,
        }
        self.cache_store = SQLiteCacheStore(cache_path, verbose=self.verbose, legacy_json_path=legacy_cache_json_path)

    def save_cache(self) -> None:
        self.cache_store.checkpoint()

    def close(self) -> None:
        self.cache_store.close()

    def _should_log_cache_hit(self, label: str) -> bool:
        return self.debug_hardcover or label not in self._quiet_hc_labels

    def _should_log_cache_store(self, label: str) -> bool:
        return self.debug_hardcover or label not in self._quiet_hc_labels

    def _should_log_http(self, meta: HardcoverRequestMeta) -> bool:
        if self.debug_hardcover:
            return True
        if meta.status_code >= 400:
            return True
        label = meta.label or "graphql"
        return label not in self._quiet_hc_labels

    def stats_snapshot(self) -> Dict[str, float]:
        return {
            "network_requests": float(self.stats["network_requests"]),
            "cache_hits": float(self.stats["cache_hits"]),
            "throttle_sleeps": float(self.stats["throttle_sleeps"]),
            "throttle_sleep_seconds": float(self.stats["throttle_sleep_seconds"]),
        }

    def stats_delta_text(self, before: Dict[str, float], after: Optional[Dict[str, float]] = None) -> str:
        now = after or self.stats_snapshot()
        return (
            f"net={int(now['network_requests'] - before['network_requests'])} "
            f"cache={int(now['cache_hits'] - before['cache_hits'])} "
            f"throttle={int(now['throttle_sleeps'] - before['throttle_sleeps'])}/"
            f"{now['throttle_sleep_seconds'] - before['throttle_sleep_seconds']:.2f}s"
        )

    def _cache_ttl_seconds_for_label(self, label: str, is_empty: bool = False) -> float:
        if is_empty:
            return self.empty_cache_ttl_hours * 3600.0
        if label in {"search_book", "identifier_book_lookup", "book_series_memberships", "series_books"}:
            return self.search_cache_ttl_hours * 3600.0
        if label in {"book_editions", "book_editions_single", "editions_by_id"}:
            return self.edition_cache_ttl_hours * 3600.0
        return self.cache_ttl_hours * 3600.0

    def _cache_entry_is_stale(self, entry: CacheEntry) -> bool:
        ttl_seconds = self._cache_ttl_seconds_for_label(
            entry.label or self._derive_label(entry.key),
            is_empty=entry.is_empty,
        )
        if ttl_seconds <= 0:
            return False
        updated_at = float(entry.updated_at or entry.created_at or 0.0)
        if updated_at <= 0:
            return True
        return (time.time() - updated_at) > ttl_seconds

    def _cache_age_hours(self, entry: CacheEntry) -> float:
        updated_at = float(entry.updated_at or entry.created_at or 0.0)
        if updated_at <= 0:
            return 0.0
        return max(0.0, (time.time() - updated_at) / 3600.0)

    def _respect_rate_limit(self) -> None:
        now = time.monotonic()
        wait_for_interval = max(0.0, self.min_interval - (now - self._last_request_ts))
        wait_for_cooldown = max(0.0, self._cooldown_until_ts - now)
        wait = max(wait_for_interval, wait_for_cooldown)
        if wait > 0:
            reason = "cooldown" if wait_for_cooldown > wait_for_interval else "rpm_cap"
            self.stats["throttle_sleeps"] += 1
            self.stats["throttle_sleep_seconds"] += float(wait)
            should_log = self.debug_hardcover or wait_for_cooldown > 0 or wait >= 3.0
            if should_log:
                vlog(
                    self.verbose,
                    f"  HC THROTTLE sleep={wait:.2f}s reason={reason} min_interval={self.min_interval:.2f}s cooldown_active={'yes' if wait_for_cooldown > 0 else 'no'}",
                )
            else:
                self.stats["suppressed_throttle_logs"] += 1
            time.sleep(wait)

    def _adjust_interval_from_headers(self, meta: Optional[HardcoverRequestMeta]) -> None:
        if not meta:
            return
        remaining_text = (meta.rate_limit_remaining or "").strip()
        try:
            remaining = int(float(remaining_text)) if remaining_text else None
        except Exception:
            remaining = None
        if remaining is None:
            if self.min_interval > self.base_min_interval:
                self.min_interval = max(self.base_min_interval, round(self.min_interval * 0.9, 2))
            return
        if remaining <= 3:
            self.min_interval = max(self.base_min_interval, 5.0)
        elif remaining <= 10:
            self.min_interval = max(self.base_min_interval, 3.0)
        else:
            self.min_interval = self.base_min_interval

    def _apply_rate_limit_cooldown(self, meta: HardcoverRequestMeta) -> float:
        self._rate_limit_streak += 1
        header_seconds = self._parse_retry_after_seconds(meta.retry_after)
        fallback = 30.0 * (2 ** min(self._rate_limit_streak - 1, 2))
        cooldown_seconds = max(header_seconds if header_seconds is not None else 0.0, fallback)
        self._cooldown_until_ts = max(self._cooldown_until_ts, time.monotonic() + cooldown_seconds)
        self.min_interval = max(self.base_min_interval, 5.0)
        return cooldown_seconds

    def _note_success(self, meta: Optional[HardcoverRequestMeta]) -> None:
        self._last_request_ts = time.monotonic()
        self._rate_limit_streak = 0
        self._cooldown_until_ts = 0.0
        self._adjust_interval_from_headers(meta)

    def _derive_label(self, cache_key: str) -> str:
        base = (cache_key or "").split("::", 1)[0]
        base = re.sub(r"_v\d+$", "", base)
        return base or "graphql"

    @staticmethod
    def _header_lookup(headers: Dict[str, str], name: str) -> str:
        if not headers:
            return ""
        target = name.lower()
        for key, value in headers.items():
            if (key or "").lower() == target:
                return str(value or "").strip()
        return ""

    def _log_request_meta(self, meta: HardcoverRequestMeta) -> None:
        self.last_request_meta = meta
        label = meta.label or "graphql"
        if meta.from_cache:
            self.stats["cache_hits"] += 1
            self.stats["cache_hit_labels"][label] += 1
            if self._should_log_cache_hit(label):
                bits = [f"HC CACHE HIT label={label}"]
                if meta.cache_key:
                    bits.append(f"key={meta.cache_key}")
                if meta.cache_detail:
                    bits.append(f"detail={meta.cache_detail}")
                vlog(self.verbose, "  " + " ".join(bits))
            else:
                self.stats["suppressed_cache_hit_logs"] += 1
            return

        self.stats["cache_misses"] += 1
        self.stats["network_requests"] += 1
        self.stats["label_counts"][label] += 1
        if meta.transport:
            self.stats["transport_counts"][meta.transport] += 1
        if meta.status_code:
            self.stats["http_status_counts"][meta.status_code] += 1
        if meta.status_code == 429:
            self.stats["rate_limit_hits"] += 1
        if meta.status_code == 401:
            self.stats["unauthorized_hits"] += 1

        parts = [
            f"HC HTTP label={label}",
            f"attempt={meta.attempt}",
            f"transport={meta.transport or '-'}",
            f"status={meta.status_code or '-'}",
            f"dur={meta.duration_s:.2f}s",
        ]
        if meta.retry_after:
            parts.append(f"retry_after={meta.retry_after}")
        rate_limit_bits = []
        if meta.rate_limit_limit:
            rate_limit_bits.append(f"limit={meta.rate_limit_limit}")
        if meta.rate_limit_remaining:
            rate_limit_bits.append(f"remaining={meta.rate_limit_remaining}")
        if meta.rate_limit_reset:
            rate_limit_bits.append(f"reset={meta.rate_limit_reset}")
        if rate_limit_bits:
            parts.append("rate=" + ",".join(rate_limit_bits))
        if meta.error_summary:
            parts.append(f"error={meta.error_summary}")
        if self._should_log_http(meta):
            vlog(self.verbose, "  " + " ".join(parts))
        else:
            self.stats["suppressed_http_logs"] += 1

    def _parse_retry_after_seconds(self, value: str) -> Optional[float]:
        text = (value or "").strip()
        if not text:
            return None
        try:
            seconds = float(text)
            if seconds >= 0:
                return seconds
        except Exception:
            pass
        return None

    def _compute_backoff_seconds(
        self,
        attempt: int,
        retry_after: str = "",
        base: float = 2.0,
        cap: float = 30.0,
    ) -> float:
        header_seconds = self._parse_retry_after_seconds(retry_after)
        if header_seconds is not None:
            return max(0.0, min(cap, header_seconds))
        return max(0.0, min(cap, base * (2 ** attempt)))

    def _log_backoff(self, label: str, status_code: int, delay_s: float, source: str) -> None:
        vlog(self.verbose, f"  HC BACKOFF label={label} status={status_code} sleep={delay_s:.2f}s source={source}")

    def _build_stats_summary_lines(self) -> List[str]:
        status_bits = ", ".join(f"{code}:{count}" for code, count in sorted(self.stats["http_status_counts"].items())) or "-"
        label_bits = ", ".join(f"{label}:{count}" for label, count in self.stats["label_counts"].most_common(8)) or "-"
        hit_bits = ", ".join(f"{label}:{count}" for label, count in self.stats["cache_hit_labels"].most_common(8)) or "-"
        transport_bits = ", ".join(f"{name}:{count}" for name, count in self.stats["transport_counts"].items()) or "-"
        total_api_requests = int(self.stats["network_requests"])
        total_cache_hits = int(self.stats["cache_hits"])
        total_processed_request_events = total_api_requests + total_cache_hits
        work_search_requests = int(
            self.stats["label_counts"].get("search_book", 0)
            + self.stats["label_counts"].get("identifier_book_lookup", 0)
        )
        work_detail_requests = int(
            self.stats["label_counts"].get("book_single", 0) + self.stats["label_counts"].get("books", 0)
        )
        edition_requests = int(
            self.stats["label_counts"].get("book_editions", 0)
            + self.stats["label_counts"].get("book_editions_single", 0)
            + self.stats["label_counts"].get("editions_by_id", 0)
        )
        return [
            f"Hardcover request stats: network={self.stats['network_requests']} cache_hits={self.stats['cache_hits']} cache_misses={self.stats['cache_misses']} cache_stale={self.stats['cache_stale']} cache_writes={self.stats['cache_writes']} cache_deletes={self.stats['cache_deletes']} retries={self.stats['retries']} failures={self.stats['request_failures']} graphql_errors={self.stats['graphql_errors']} empty_responses={self.stats['empty_responses']} rate_limit_hits={self.stats['rate_limit_hits']} unauthorized_hits={self.stats['unauthorized_hits']}",
            f"Hardcover request breakdown: work_search={work_search_requests} work_detail={work_detail_requests} edition={edition_requests} throttle_sleeps={self.stats['throttle_sleeps']} throttle_sleep_seconds={self.stats['throttle_sleep_seconds']:.2f}",
            f"Hardcover total API requests sent: {total_api_requests}",
            f"Hardcover total request events handled (API + cache hits): {total_processed_request_events}",
            f"Hardcover suppressed verbose lines: cache_hits={self.stats['suppressed_cache_hit_logs']} cache_stores={self.stats['suppressed_cache_store_logs']} http={self.stats['suppressed_http_logs']} throttle={self.stats['suppressed_throttle_logs']}",
            f"Hardcover legacy JSON cache rows imported: {self.cache_store.imported_legacy_entries}",
            f"Hardcover HTTP status counts: {status_bits}",
            f"Hardcover request labels: {label_bits}",
            f"Hardcover cache-hit labels: {hit_bits}",
            f"Hardcover transports: {transport_bits}",
        ]

    def stats_summary_lines(self) -> List[str]:
        return list(self._build_stats_summary_lines())

    def print_stats_summary(self) -> None:
        for line in self.stats_summary_lines():
            print(line)

    def _post_graphql_via_curl(
        self,
        payload_text: str,
        headers: Dict[str, str],
    ) -> Tuple[Dict[str, Any], HardcoverRequestMeta]:
        if not self._curl_path:
            raise RuntimeError("curl is not available")
        started_at = time.monotonic()
        with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as body_file, tempfile.NamedTemporaryFile(
            mode="w+b",
            delete=False,
        ) as header_file:
            body_path = body_file.name
            header_path = header_file.name
        try:
            command = [
                self._curl_path,
                "-sS",
                "-X",
                "POST",
                GRAPHQL_ENDPOINT,
                "--connect-timeout",
                str(min(10, self.timeout)),
                "--max-time",
                str(self.timeout),
                "--header",
                f"content-type: {headers['content-type']}",
                "--header",
                f"authorization: {headers['authorization']}",
                "--header",
                f"user-agent: {headers['user-agent']}",
                "--dump-header",
                header_path,
                "--output",
                body_path,
                "--write-out",
                "%{http_code}",
                "--data-binary",
                payload_text,
            ]
            process = subprocess.run(command, capture_output=True, text=True)
            duration = time.monotonic() - started_at
            status_text = (process.stdout or "").strip()
            try:
                status_code = int(status_text) if status_text else 0
            except Exception:
                status_code = 0
            header_text = Path(header_path).read_text(encoding="utf-8", errors="replace") if Path(header_path).exists() else ""
            body = Path(body_path).read_text(encoding="utf-8", errors="replace").strip() if Path(body_path).exists() else ""
            header_map: Dict[str, str] = {}
            for line in header_text.splitlines():
                if ":" not in line:
                    continue
                key, value = line.split(":", 1)
                header_map[key.strip()] = value.strip()
            meta = HardcoverRequestMeta(
                transport="curl",
                status_code=status_code,
                duration_s=duration,
                retry_after=self._header_lookup(header_map, "Retry-After"),
                rate_limit_limit=self._header_lookup(header_map, "X-RateLimit-Limit"),
                rate_limit_remaining=self._header_lookup(header_map, "X-RateLimit-Remaining"),
                rate_limit_reset=self._header_lookup(header_map, "X-RateLimit-Reset"),
            )
            self.last_request_meta = meta
            if process.returncode != 0:
                error_text = (process.stderr or "").strip() or body[:500]
                meta.error_summary = f"curl_exit_{process.returncode}"
                raise HardcoverRequestFailure(f"curl exited {process.returncode}: {error_text[:500]}", meta)
            if status_code >= 400:
                if status_code == 401:
                    meta.error_summary = "unauthorized"
                elif status_code == 429:
                    meta.error_summary = "rate_limited"
                else:
                    meta.error_summary = f"http_{status_code}"
                raise HardcoverRequestFailure(f"Hardcover HTTP {status_code}: {body[:500]}", meta)
            if not body:
                self.stats["empty_responses"] += 1
                meta.error_summary = "empty_response"
                raise HardcoverRequestFailure("Hardcover API returned empty response", meta)
            data = json.loads(body)
            if data.get("errors"):
                self.stats["graphql_errors"] += 1
                meta.error_summary = "graphql_error"
                raise HardcoverRequestFailure(f"Hardcover API GraphQL error: {data['errors']}", meta)
            return data.get("data", {}), meta
        finally:
            for file_path in (body_path, header_path):
                try:
                    os.unlink(file_path)
                except Exception:
                    pass

    def _post_graphql_via_urllib(
        self,
        payload: bytes,
        headers: Dict[str, str],
    ) -> Tuple[Dict[str, Any], HardcoverRequestMeta]:
        request = urllib_request.Request(GRAPHQL_ENDPOINT, data=payload, headers=headers, method="POST")
        started_at = time.monotonic()
        with urllib_request.urlopen(request, timeout=self.timeout) as response:
            status_code = int(getattr(response, "status", 0) or 0)
            header_map = {key: value for key, value in dict(response.headers).items()}
            body = response.read().decode("utf-8", errors="replace").strip()
        duration = time.monotonic() - started_at
        meta = HardcoverRequestMeta(
            transport="urllib",
            status_code=status_code,
            duration_s=duration,
            retry_after=self._header_lookup(header_map, "Retry-After"),
            rate_limit_limit=self._header_lookup(header_map, "X-RateLimit-Limit"),
            rate_limit_remaining=self._header_lookup(header_map, "X-RateLimit-Remaining"),
            rate_limit_reset=self._header_lookup(header_map, "X-RateLimit-Reset"),
        )
        self.last_request_meta = meta
        if not body:
            self.stats["empty_responses"] += 1
            meta.error_summary = "empty_response"
            raise HardcoverRequestFailure("Hardcover API returned empty response", meta)
        data = json.loads(body)
        if data.get("errors"):
            self.stats["graphql_errors"] += 1
            meta.error_summary = "graphql_error"
            raise HardcoverRequestFailure(f"Hardcover API GraphQL error: {data['errors']}", meta)
        return data.get("data", {}), meta

    def _post_graphql(self, query: str, variables: Dict[str, Any], label: str = "graphql") -> Dict[str, Any]:
        payload_text = json.dumps({"query": query, "variables": variables}, ensure_ascii=False)
        payload = payload_text.encode("utf-8")
        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {self.token}",
            "user-agent": self.user_agent,
        }
        last_error: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            try:
                self._respect_rate_limit()
                if self._curl_path:
                    data, meta = self._post_graphql_via_curl(payload_text, headers)
                else:
                    data, meta = self._post_graphql_via_urllib(payload, headers)
                meta.label = label
                meta.attempt = attempt + 1
                self._note_success(meta)
                self._log_request_meta(meta)
                return data
            except urllib_error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace").strip() if hasattr(exc, "read") else ""
                meta = HardcoverRequestMeta(
                    label=label,
                    transport="urllib",
                    status_code=int(getattr(exc, "code", 0) or 0),
                    attempt=attempt + 1,
                    error_summary="http_error",
                )
                meta.retry_after = self._header_lookup(dict(getattr(exc, "headers", {}) or {}), "Retry-After")
                meta.rate_limit_limit = self._header_lookup(
                    dict(getattr(exc, "headers", {}) or {}),
                    "X-RateLimit-Limit",
                )
                meta.rate_limit_remaining = self._header_lookup(
                    dict(getattr(exc, "headers", {}) or {}),
                    "X-RateLimit-Remaining",
                )
                meta.rate_limit_reset = self._header_lookup(
                    dict(getattr(exc, "headers", {}) or {}),
                    "X-RateLimit-Reset",
                )
                self._log_request_meta(meta)
                last_error = RuntimeError(f"Hardcover HTTP {exc.code}: {detail[:500]}")
                if exc.code == 429 and attempt < self.retries:
                    self.stats["retries"] += 1
                    cooldown_seconds = self._apply_rate_limit_cooldown(meta)
                    delay_seconds = max(
                        cooldown_seconds,
                        self._compute_backoff_seconds(attempt, meta.retry_after, base=30.0, cap=120.0),
                    )
                    self._log_backoff(
                        label,
                        exc.code,
                        delay_seconds,
                        "retry_after"
                        if self._parse_retry_after_seconds(meta.retry_after) is not None
                        else "rate_limit_cooldown",
                    )
                    time.sleep(delay_seconds)
                    continue
                if exc.code == 401:
                    vlog(self.verbose, f"  HC AUTH WARNING label={label} status=401 unauthorized; not retrying")
                    break
                if 500 <= exc.code < 600 and attempt < self.retries:
                    self.stats["retries"] += 1
                    delay_seconds = self._compute_backoff_seconds(attempt, "", base=1.0, cap=10.0)
                    self._log_backoff(label, exc.code, delay_seconds, "exponential")
                    time.sleep(delay_seconds)
                    continue
                break
            except (urllib_error.URLError, TimeoutError, json.JSONDecodeError, HardcoverRequestFailure, RuntimeError) as exc:
                meta = (
                    exc.meta
                    if isinstance(exc, HardcoverRequestFailure)
                    else (self.last_request_meta if self.last_request_meta else HardcoverRequestMeta())
                )
                if not meta.label:
                    meta = HardcoverRequestMeta(label=label, attempt=attempt + 1, error_summary=type(exc).__name__)
                meta.label = label
                meta.attempt = attempt + 1
                if not meta.error_summary:
                    meta.error_summary = type(exc).__name__
                self._log_request_meta(meta)
                last_error = exc
                if meta.status_code == 429 and attempt < self.retries:
                    self.stats["retries"] += 1
                    cooldown_seconds = self._apply_rate_limit_cooldown(meta)
                    delay_seconds = max(
                        cooldown_seconds,
                        self._compute_backoff_seconds(attempt, meta.retry_after, base=30.0, cap=120.0),
                    )
                    self._log_backoff(
                        label,
                        meta.status_code,
                        delay_seconds,
                        "retry_after"
                        if self._parse_retry_after_seconds(meta.retry_after) is not None
                        else "rate_limit_cooldown",
                    )
                    time.sleep(delay_seconds)
                    continue
                if meta.status_code == 401:
                    vlog(self.verbose, f"  HC AUTH WARNING label={label} status=401 unauthorized; not retrying")
                    break
                if meta.status_code and 500 <= meta.status_code < 600 and attempt < self.retries:
                    self.stats["retries"] += 1
                    delay_seconds = self._compute_backoff_seconds(attempt, "", base=2.0, cap=20.0)
                    self._log_backoff(label, meta.status_code, delay_seconds, "exponential")
                    time.sleep(delay_seconds)
                    continue
                if attempt < self.retries and not meta.status_code:
                    self.stats["retries"] += 1
                    delay_seconds = self._compute_backoff_seconds(attempt, "", base=1.5, cap=10.0)
                    self._log_backoff(label, 0, delay_seconds, "exponential")
                    time.sleep(delay_seconds)
                    continue
                break
        self.stats["request_failures"] += 1
        raise RuntimeError(f"Hardcover request failed: {last_error}")

    def cached_query(
        self,
        cache_key: str,
        query: str,
        variables: Dict[str, Any],
        force_refresh: bool = False,
        cache_empty: bool = False,
    ) -> Dict[str, Any]:
        label = self._derive_label(cache_key)
        if not force_refresh:
            entry = self.cache_store.get(cache_key)
            if entry is not None:
                if self._cache_entry_is_stale(entry):
                    self.stats["cache_stale"] += 1
                    age_h = self._cache_age_hours(entry)
                    vlog(self.verbose, f"  HC CACHE STALE label={label} key={cache_key} age_h={age_h:.1f}")
                else:
                    if not entry.is_empty or cache_empty:
                        cache_detail = f"bytes={entry.payload_size} age_h={self._cache_age_hours(entry):.1f}"
                        self._log_request_meta(
                            HardcoverRequestMeta(
                                label=label,
                                from_cache=True,
                                cache_key=cache_key,
                                cache_detail=cache_detail,
                            )
                        )
                        return entry.payload or {}
        data = self._post_graphql(query, variables, label=label)
        if data not in (None, {}):
            self.cache_store.set(cache_key, label, data, is_empty=False)
            self.stats["cache_writes"] += 1
            if self._should_log_cache_store(label):
                vlog(self.verbose, f"  HC CACHE STORE label={label} key={cache_key}")
            else:
                self.stats["suppressed_cache_store_logs"] += 1
        elif cache_empty:
            self.cache_store.set(cache_key, label, data or {}, is_empty=True)
            self.stats["cache_writes"] += 1
        else:
            existing = self.cache_store.get(cache_key)
            if existing is not None and existing.is_empty:
                self.cache_store.delete(cache_key)
                self.stats["cache_deletes"] += 1
        return data or {}

    def search_book_ids(self, query_text: str, per_page: int = 5, page: int = 1) -> List[int]:
        cache_key = f"search_book_v2::{query_text}::{per_page}::{page}"
        query = """
        query SearchBooks($query: String!, $perPage: Int!, $page: Int!) {
          search(query: $query, query_type: "Book", per_page: $perPage, page: $page) {
            ids
            results
          }
        }
        """
        data = self.cached_query(cache_key, query, {"query": query_text, "perPage": per_page, "page": page})
        ids = (((data or {}).get("search") or {}).get("ids") or [])
        output: List[int] = []
        for value in ids:
            try:
                output.append(int(value))
            except Exception:
                pass
        return output

    def find_book_ids_by_identifier(self, token: str) -> List[int]:
        token = clean_isbn(token)
        if not token:
            return []
        cache_key = f"identifier_book_lookup::{token}"
        query = """
        query FindBookByIdentifier($token: String!) {
          editions(where: {_or: [{isbn_13: {_eq: $token}}, {isbn_10: {_eq: $token}}, {asin: {_eq: $token}}]}) {
            id
            book_id
          }
        }
        """
        try:
            data = self.cached_query(cache_key, query, {"token": token}, cache_empty=False)
        except Exception:
            return []
        output: List[int] = []
        for edition in data.get("editions", []) or []:
            try:
                book_id = int(edition.get("book_id") or 0)
            except Exception:
                book_id = 0
            if book_id and book_id not in output:
                output.append(book_id)
        return output

    def _book_from_node(self, book_node: Dict[str, Any]) -> HardcoverBook:
        authors = authors_from_contributions(book_node.get("contributions") or [])
        series_parts = []
        for series_membership in book_node.get("book_series") or []:
            series_name = ((series_membership.get("series") or {}).get("name") or "").strip()
            position = series_membership.get("position")
            if series_name:
                series_parts.append(f"{series_name} [{position}]" if position is not None else series_name)
        return HardcoverBook(
            id=int(book_node["id"]),
            title=smart_title(book_node.get("title") or ""),
            subtitle=smart_title(book_node.get("subtitle") or ""),
            authors=authors,
            series=" | ".join(series_parts),
            release_date=book_node.get("release_date") or "",
            slug=(book_node.get("slug") or "").strip(),
            users_count=int(book_node.get("users_count") or 0),
            users_read_count=int(book_node.get("users_read_count") or 0),
            rating=float(book_node.get("rating") or 0.0),
            lists_count=int(book_node.get("lists_count") or 0),
            default_ebook_edition_id=int(book_node.get("default_ebook_edition_id") or 0),
            default_physical_edition_id=int(book_node.get("default_physical_edition_id") or 0),
            default_audio_edition_id=int(book_node.get("default_audio_edition_id") or 0),
            default_cover_edition_id=int(book_node.get("default_cover_edition_id") or 0),
        )

    def _edition_from_node(self, edition_node: Dict[str, Any], book_id: int) -> HardcoverEdition:
        authors = authors_from_contributions(edition_node.get("contributions") or [])
        return HardcoverEdition(
            id=int(edition_node.get("id")),
            book_id=book_id,
            title=smart_title(edition_node.get("title") or ""),
            subtitle=smart_title(edition_node.get("subtitle") or ""),
            authors=authors,
            score=int(edition_node.get("score") or 0),
            rating=float(edition_node.get("rating") or 0.0),
            users_count=int(edition_node.get("users_count") or 0),
            users_read_count=int(edition_node.get("users_read_count") or 0),
            lists_count=int(edition_node.get("lists_count") or 0),
            release_date=edition_node.get("release_date") or "",
            isbn_10=clean_isbn(edition_node.get("isbn_10") or ""),
            isbn_13=clean_isbn(edition_node.get("isbn_13") or ""),
            asin=clean_isbn(edition_node.get("asin") or ""),
            audio_seconds=(
                int(edition_node.get("audio_seconds"))
                if edition_node.get("audio_seconds") not in (None, "")
                else None
            ),
            physical_format=(edition_node.get("physical_format") or ""),
            edition_format=(edition_node.get("edition_format") or ""),
            reading_format=((edition_node.get("reading_format") or {}).get("format") or ""),
            language=((edition_node.get("language") or {}).get("language") or ""),
        )

    def fetch_book_by_id(self, book_id: int, force_refresh: bool = False) -> Optional[HardcoverBook]:
        cache_key = f"book_single_v5::{book_id}"
        query = """
        query FetchBookSingle($id: Int!) {
          books_by_pk(id: $id) {
            id
            title
            subtitle
            release_date
            slug
            users_count
            users_read_count
            rating
            lists_count
            default_ebook_edition_id
            default_physical_edition_id
            default_audio_edition_id
            default_cover_edition_id
            contributions { contribution author { name } }
            book_series { position series { name } }
          }
        }
        """
        data = self.cached_query(cache_key, query, {"id": int(book_id)}, force_refresh=force_refresh, cache_empty=False)
        book = data.get("books_by_pk")
        if not book:
            return None
        return self._book_from_node(book)

    def fetch_books(self, ids: List[int], force_refresh: bool = False) -> Dict[int, HardcoverBook]:
        ids = sorted(set(int(value) for value in ids if value))
        if not ids:
            return {}
        cache_key = "books_v5::" + ",".join(map(str, ids))
        query = """
        query FetchBooks($ids: [Int!]) {
          books(where: {id: {_in: $ids}}) {
            id
            title
            subtitle
            release_date
            slug
            users_count
            users_read_count
            rating
            lists_count
            default_ebook_edition_id
            default_physical_edition_id
            default_audio_edition_id
            default_cover_edition_id
            contributions { contribution author { name } }
            book_series { position series { name } }
          }
        }
        """
        data = self.cached_query(cache_key, query, {"ids": ids}, force_refresh=force_refresh, cache_empty=False)
        output: Dict[int, HardcoverBook] = {}
        for book in data.get("books", []) or []:
            output[int(book["id"])] = self._book_from_node(book)
        missing_ids = [book_id for book_id in ids if book_id not in output]
        for missing_id in missing_ids:
            single = self.fetch_book_by_id(missing_id, force_refresh=True)
            if single:
                output[missing_id] = single
        return output

    def fetch_books_and_editions_for_books(
        self,
        ids: List[int],
        force_refresh: bool = False,
        verbose: bool = False,
        progress_label: str = "candidate-catalogs",
        display_labels: Optional[Dict[int, str]] = None,
    ) -> Tuple[Dict[int, HardcoverBook], Dict[int, List[HardcoverEdition]]]:
        ids = sorted(set(int(value) for value in ids if value))
        if not ids:
            return {}, {}
        query = """
        query FetchBooksAndEditions($ids: [Int!]) {
          books(where: {id: {_in: $ids}}) {
            id
            title
            subtitle
            release_date
            slug
            users_count
            users_read_count
            rating
            lists_count
            default_ebook_edition_id
            default_physical_edition_id
            default_audio_edition_id
            default_cover_edition_id
            contributions { contribution author { name } }
            book_series { position series { name } }
            editions {
              id
              title
              subtitle
              score
              rating
              users_count
              users_read_count
              lists_count
              release_date
              isbn_10
              isbn_13
              asin
              audio_seconds
              physical_format
              edition_format
              reading_format { format }
              language { language }
              contributions { contribution author { name } }
            }
          }
        }
        """
        books_out: Dict[int, HardcoverBook] = {}
        editions_out: Dict[int, List[HardcoverEdition]] = {book_id: [] for book_id in ids}
        display_labels = display_labels or {}
        id_chunks = list(chunked(ids, 25))
        total_batches = len(id_chunks)
        if verbose and total_batches:
            vlog(verbose, f"  {progress_label}: fetching {len(ids)} books in {total_batches} batches")
        for batch_idx, id_chunk in enumerate(id_chunks, start=1):
            before = self.stats_snapshot()
            cache_key = "books_and_editions_v1::" + ",".join(map(str, id_chunk))
            data = self.cached_query(cache_key, query, {"ids": id_chunk}, force_refresh=force_refresh, cache_empty=False)
            seen: set[int] = set()
            edition_count = 0
            for book in data.get("books", []) or []:
                book_id = int(book.get("id") or 0)
                if not book_id:
                    continue
                books_out[book_id] = self._book_from_node(book)
                editions = [self._edition_from_node(edition, book_id) for edition in (book.get("editions") or [])]
                editions_out[book_id] = editions
                edition_count += len(editions)
                seen.add(book_id)
            missing_ids = [book_id for book_id in id_chunk if book_id not in seen]
            for missing_id in missing_ids:
                single = self.fetch_book_by_id(missing_id, force_refresh=True)
                if single:
                    books_out[missing_id] = single
                single_editions = self.fetch_editions_for_books([missing_id], force_refresh=True).get(missing_id, [])
                editions_out[missing_id] = single_editions
                edition_count += len(single_editions)
                if single or single_editions:
                    seen.add(missing_id)
            if verbose:
                after = self.stats_snapshot()
                net_delta = int(after["network_requests"] - before["network_requests"])
                cache_delta = int(after["cache_hits"] - before["cache_hits"])
                if net_delta and cache_delta:
                    batch_cache_status = "mixed"
                elif net_delta:
                    batch_cache_status = "miss"
                elif cache_delta:
                    batch_cache_status = "hit"
                else:
                    batch_cache_status = "none"
                book_display_items = [display_labels.get(book_id) or str(book_id) for book_id in id_chunk]
                book_sample = preview_names(book_display_items, limit=min(4, len(id_chunk)), max_len=42)
                vlog(
                    verbose,
                    f"    [{batch_idx}/{total_batches}] books={book_sample} matched_books={len(seen)}/{len(id_chunk)} editions={edition_count} batch_cache={batch_cache_status} {self.stats_delta_text(before, after)}",
                )
        return books_out, editions_out

    def fetch_editions_for_books(
        self,
        ids: List[int],
        force_refresh: bool = False,
        verbose: bool = False,
        progress_label: str = "edition-catalogs",
        display_labels: Optional[Dict[int, str]] = None,
    ) -> Dict[int, List[HardcoverEdition]]:
        ids = sorted(set(int(value) for value in ids if value))
        if not ids:
            return {}
        query = """
        query FetchBookEditions($ids: [Int!]) {
          books(where: {id: {_in: $ids}}) {
            id
            editions {
              id
              title
              subtitle
              score
              rating
              users_count
              users_read_count
              lists_count
              release_date
              isbn_10
              isbn_13
              asin
              audio_seconds
              physical_format
              edition_format
              reading_format { format }
              language { language }
              contributions { contribution author { name } }
            }
          }
        }
        """
        output: Dict[int, List[HardcoverEdition]] = {book_id: [] for book_id in ids}
        display_labels = display_labels or {}
        id_chunks = list(chunked(ids, 25))
        total_batches = len(id_chunks)
        if verbose and total_batches:
            vlog(verbose, f"  {progress_label}: fetching {len(ids)} books in {total_batches} batches")
        for batch_idx, id_chunk in enumerate(id_chunks, start=1):
            cache_key = "book_editions_v5::" + ",".join(map(str, id_chunk))
            before = self.stats_snapshot()
            data = self.cached_query(cache_key, query, {"ids": id_chunk}, force_refresh=force_refresh, cache_empty=False)
            seen: set[int] = set()
            edition_count = 0
            for book in data.get("books", []) or []:
                book_id = int(book.get("id"))
                editions = [self._edition_from_node(edition, book_id) for edition in (book.get("editions") or [])]
                edition_count += len(editions)
                output[book_id] = editions
                seen.add(book_id)
            missing_ids = [book_id for book_id in id_chunk if book_id not in seen]
            for missing_id in missing_ids:
                single_key = f"book_editions_single_v4::{missing_id}"
                single_query = """
                query FetchBookEditionsSingle($id: Int!) {
                  books(where: {id: {_eq: $id}}) {
                    id
                    editions {
                      id
                      title
                      subtitle
                      score
                      rating
                      users_count
                      users_read_count
                      lists_count
                      release_date
                      isbn_10
                      isbn_13
                      asin
                      audio_seconds
                      physical_format
                      edition_format
                      reading_format { format }
                      language { language }
                      contributions { contribution author { name } }
                    }
                  }
                }
                """
                single_data = self.cached_query(
                    single_key,
                    single_query,
                    {"id": missing_id},
                    force_refresh=True,
                    cache_empty=False,
                )
                books = single_data.get("books", []) or []
                if not books:
                    continue
                editions = [self._edition_from_node(edition, missing_id) for edition in (books[0].get("editions") or [])]
                output[missing_id] = editions
                edition_count += len(editions)
                seen.add(missing_id)
            if verbose:
                after = self.stats_snapshot()
                net_delta = int(after["network_requests"] - before["network_requests"])
                cache_delta = int(after["cache_hits"] - before["cache_hits"])
                if net_delta and cache_delta:
                    batch_cache_status = "mixed"
                elif net_delta:
                    batch_cache_status = "miss"
                elif cache_delta:
                    batch_cache_status = "hit"
                else:
                    batch_cache_status = "none"
                book_display_items = [display_labels.get(book_id) or str(book_id) for book_id in id_chunk]
                book_sample = preview_names(book_display_items, limit=min(4, len(id_chunk)), max_len=42)
                vlog(
                    verbose,
                    f"    [{batch_idx}/{total_batches}] books={book_sample} matched_books={len(seen)}/{len(id_chunk)} editions={edition_count} batch_cache={batch_cache_status} {self.stats_delta_text(before, after)}",
                )
        return output

    def fetch_book_series_memberships(
        self,
        book_ids: List[int],
        force_refresh: bool = False,
        verbose: bool = False,
        progress_label: str = "memberships",
        display_labels: Optional[Dict[int, str]] = None,
    ) -> Dict[int, List[Dict[str, Any]]]:
        book_ids = sorted(set(int(value) for value in book_ids if value))
        if not book_ids:
            return {}
        output: Dict[int, List[Dict[str, Any]]] = {book_id: [] for book_id in book_ids}
        query = """
        query FetchBookSeriesMemberships($ids: [Int!]) {
          book_series(
            where: {book_id: {_in: $ids}, compilation: {_eq: false}}
            order_by: [{series_id: asc}, {position: asc}, {id: asc}]
          ) {
            book_id
            position
            series_id
            compilation
            book {
              id
              title
              canonical {
                id
                title
              }
            }
            series {
              id
              name
              slug
              canonical_id
              is_completed
              books_count
              primary_books_count
              canonical {
                id
                name
                slug
                is_completed
                books_count
                primary_books_count
              }
            }
          }
        }
        """
        display_labels = display_labels or {}
        id_chunks = list(chunked(book_ids, 50))
        total_batches = len(id_chunks)
        if verbose and total_batches:
            vlog(verbose, f"  {progress_label}: fetching {len(book_ids)} books in {total_batches} batches")
        for batch_idx, id_chunk in enumerate(id_chunks, start=1):
            cache_key = "book_series_memberships::" + ",".join(map(str, id_chunk))
            before = self.stats_snapshot()
            data = self.cached_query(cache_key, query, {"ids": id_chunk}, force_refresh=force_refresh, cache_empty=False)
            matched_book_ids: Set[int] = set()
            matched_series_ids: Set[int] = set()
            matched_book_labels: Dict[int, str] = {}
            for node in data.get("book_series", []) or []:
                series = node.get("series") or {}
                canonical = series.get("canonical") or {}
                chosen_series = canonical or series
                raw_series_id = series.get("id")
                chosen_series_id = chosen_series.get("id") or raw_series_id
                if not chosen_series_id:
                    continue
                book_id = int(node.get("book_id"))
                book_node = node.get("book") or {}
                canonical_book = book_node.get("canonical") or {}
                if book_id and book_id not in matched_book_labels:
                    title = smart_title(canonical_book.get("title") or book_node.get("title") or "")
                    matched_book_labels[book_id] = f"{title} [{book_id}]" if title else str(book_id)
                matched_book_ids.add(book_id)
                matched_series_ids.add(int(chosen_series_id))
                output.setdefault(book_id, []).append(
                    {
                        "book_id": book_id,
                        "position": node.get("position"),
                        "series_id": int(chosen_series_id),
                        "series_name": smart_title(chosen_series.get("name") or series.get("name") or ""),
                        "series_slug": (chosen_series.get("slug") or series.get("slug") or "").strip(),
                        "raw_series_id": int(raw_series_id) if raw_series_id else int(chosen_series_id),
                        "raw_series_name": smart_title(series.get("name") or ""),
                        "compilation": bool(node.get("compilation")),
                        "is_completed": chosen_series.get("is_completed"),
                        "books_count": chosen_series.get("books_count"),
                        "primary_books_count": chosen_series.get("primary_books_count"),
                    }
                )
            if verbose:
                after = self.stats_snapshot()
                net_delta = int(after["network_requests"] - before["network_requests"])
                cache_delta = int(after["cache_hits"] - before["cache_hits"])
                if net_delta and cache_delta:
                    batch_cache_status = "mixed"
                elif net_delta:
                    batch_cache_status = "miss"
                elif cache_delta:
                    batch_cache_status = "hit"
                else:
                    batch_cache_status = "none"
                book_display_items = [
                    display_labels.get(book_id) or matched_book_labels.get(book_id) or str(book_id) for book_id in id_chunk
                ]
                book_sample = preview_names(book_display_items, limit=min(6, len(id_chunk)), max_len=40)
                vlog(
                    verbose,
                    f"    [{batch_idx}/{total_batches}] books={book_sample} books_with_series={len(matched_book_ids)}/{len(id_chunk)} unique_series={len(matched_series_ids)} batch_cache={batch_cache_status} {self.stats_delta_text(before, after)}",
                )
        return output

    def fetch_series_books(
        self,
        series_ids: List[int],
        force_refresh: bool = False,
        verbose: bool = False,
        progress_label: str = "series-catalogs",
    ) -> Dict[int, Dict[str, Any]]:
        series_ids = sorted(set(int(value) for value in series_ids if value))
        if not series_ids:
            return {}
        output: Dict[int, Dict[str, Any]] = {}
        query = """
        query FetchSeriesBooks($ids: [Int!]) {
          series(where: {id: {_in: $ids}}) {
            id
            name
            slug
            is_completed
            books_count
            primary_books_count
            author {
              id
              name
            }
            book_series(
              where: {compilation: {_eq: false}}
              order_by: [{position: asc}, {id: asc}]
            ) {
              position
              details
              featured
              book {
                id
                title
                subtitle
                release_date
                slug
                users_count
                users_read_count
                rating
                lists_count
                canonical_id
                state
                canonical {
                  id
                  title
                  slug
                }
                contributions {
                  contribution
                  author { name }
                }
                default_ebook_edition {
                  edition_format
                  reading_format { format }
                  language { language }
                }
                default_physical_edition {
                  edition_format
                  reading_format { format }
                  language { language }
                }
                default_cover_edition {
                  edition_format
                  reading_format { format }
                  language { language }
                }
                default_audio_edition {
                  edition_format
                  reading_format { format }
                  language { language }
                }
              }
            }
          }
        }
        """

        def edition_language(node: Dict[str, Any], key: str) -> str:
            edition = node.get(key) or {}
            language = (edition.get("language") or {}).get("language") or ""
            return str(language or "")

        id_chunks = list(chunked(series_ids, 25))
        total_batches = len(id_chunks)
        if verbose and total_batches:
            vlog(verbose, f"  {progress_label}: fetching {len(series_ids)} series in {total_batches} batches")
        for batch_idx, id_chunk in enumerate(id_chunks, start=1):
            cache_key = "series_books_v3::" + ",".join(map(str, id_chunk))
            before = self.stats_snapshot()
            data = self.cached_query(cache_key, query, {"ids": id_chunk}, force_refresh=force_refresh, cache_empty=False)
            batch_books = 0
            matched_series_ids: Set[int] = set()
            matched_series_labels: List[str] = []
            for series in data.get("series", []) or []:
                series_id = int(series.get("id"))
                books: List[Dict[str, Any]] = []
                seen_book_ids: set[int] = set()
                series_name = smart_title(series.get("name") or "")
                matched_series_ids.add(series_id)
                matched_series_labels.append(f"{series_name} [{series_id}]" if series_name else str(series_id))
                for book_series in series.get("book_series") or []:
                    book = book_series.get("book") or {}
                    if not book.get("id"):
                        continue
                    book_id = int(book.get("id"))
                    if book_id in seen_book_ids:
                        continue
                    seen_book_ids.add(book_id)
                    batch_books += 1
                    authors = authors_from_contributions(book.get("contributions") or [])
                    canonical = book.get("canonical") or {}
                    books.append(
                        {
                            "book_id": book_id,
                            "position": book_series.get("position"),
                            "details": str(book_series.get("details") or ""),
                            "featured": bool(book_series.get("featured")),
                            "title": smart_title(book.get("title") or ""),
                            "subtitle": smart_title(book.get("subtitle") or ""),
                            "authors": authors,
                            "release_date": book.get("release_date") or "",
                            "slug": (book.get("slug") or "").strip(),
                            "canonical_id": int(book.get("canonical_id") or canonical.get("id") or 0),
                            "canonical_title": smart_title(canonical.get("title") or ""),
                            "canonical_slug": (canonical.get("slug") or "").strip(),
                            "state": str(book.get("state") or ""),
                            "users_count": int(book.get("users_count") or 0),
                            "users_read_count": int(book.get("users_read_count") or 0),
                            "rating": float(book.get("rating") or 0.0),
                            "lists_count": int(book.get("lists_count") or 0),
                            "default_ebook_language": edition_language(book, "default_ebook_edition"),
                            "default_physical_language": edition_language(book, "default_physical_edition"),
                            "default_cover_language": edition_language(book, "default_cover_edition"),
                            "default_audio_language": edition_language(book, "default_audio_edition"),
                        }
                    )
                series_author = series.get("author") or {}
                total_users = sum(int(book.get("users_count") or 0) for book in books)
                total_users_read = sum(int(book.get("users_read_count") or 0) for book in books)
                total_lists = sum(int(book.get("lists_count") or 0) for book in books)
                top_book_users_read = max((int(book.get("users_read_count") or 0) for book in books), default=0)
                output[series_id] = {
                    "series_id": series_id,
                    "series_name": series_name,
                    "series_slug": (series.get("slug") or "").strip(),
                    "series_author_id": int(series_author.get("id") or 0),
                    "series_author_name": normalize_person_name((series_author.get("name") or "").strip()),
                    "is_completed": series.get("is_completed"),
                    "books_count": int(series.get("books_count") or 0),
                    "primary_books_count": int(series.get("primary_books_count") or 0),
                    "series_users_count_total": total_users,
                    "series_users_read_count_total": total_users_read,
                    "series_lists_count_total": total_lists,
                    "series_top_book_users_read_count": top_book_users_read,
                    "books": books,
                }
            if verbose:
                after = self.stats_snapshot()
                net_delta = int(after["network_requests"] - before["network_requests"])
                cache_delta = int(after["cache_hits"] - before["cache_hits"])
                if net_delta and cache_delta:
                    batch_cache_status = "mixed"
                elif net_delta:
                    batch_cache_status = "miss"
                elif cache_delta:
                    batch_cache_status = "hit"
                else:
                    batch_cache_status = "none"
                series_display_items = matched_series_labels + [str(x) for x in id_chunk if int(x) not in matched_series_ids]
                series_sample = preview_names(
                    series_display_items or [str(value) for value in id_chunk],
                    limit=max(3, len(id_chunk)),
                    max_len=30,
                )
                vlog(
                    verbose,
                    f"    [{batch_idx}/{total_batches}] series={series_sample} matched_series={len(matched_series_ids)}/{len(id_chunk)} books_returned={batch_books} batch_cache={batch_cache_status} {self.stats_delta_text(before, after)}",
                )
        return output

    def fetch_books_for_authors(
        self,
        author_names: List[str],
        force_refresh: bool = False,
        verbose: bool = False,
        progress_label: str = "author-catalogs",
    ) -> Dict[str, Dict[str, Any]]:
        author_names = [smart_title(name) for name in author_names if smart_title(name)]
        if not author_names:
            return {}
        output: Dict[str, Dict[str, Any]] = {}
        query = """
        query FetchBooksForAuthors($names: [String!]) {
          authors(where: {name: {_in: $names}}) {
            id
            name
            canonical { id name }
            contributions {
              contribution
              book {
                id
                title
                subtitle
                release_date
                slug
                users_count
                users_read_count
                rating
                lists_count
                book_series {
                  position
                  series {
                    id
                    name
                    slug
                    is_completed
                    books_count
                    primary_books_count
                    canonical {
                      id
                      name
                      slug
                      is_completed
                      books_count
                      primary_books_count
                    }
                  }
                }
              }
            }
          }
        }
        """
        name_chunks = list(chunked(author_names, 5))
        total_batches = len(name_chunks)
        if verbose and total_batches:
            vlog(verbose, f"  {progress_label}: fetching {len(author_names)} authors in {total_batches} batches")
        for batch_idx, name_chunk in enumerate(name_chunks, start=1):
            cache_key = "author_books_v1::" + "|".join(name_chunk)
            before = self.stats_snapshot()
            data = self.cached_query(cache_key, query, {"names": name_chunk}, force_refresh=force_refresh, cache_empty=False)
            batch_books = 0
            matched_author_keys: Set[str] = set()
            matched_author_labels: List[str] = []
            for author in data.get("authors", []) or []:
                canonical = author.get("canonical") or {}
                display_name = normalize_person_name((canonical.get("name") or author.get("name") or "").strip())
                author_id = int(canonical.get("id") or author.get("id") or 0)
                key = normalize_author_key(display_name)
                if not key:
                    continue
                matched_author_keys.add(key)
                matched_author_labels.append(f"{display_name} [{author_id}]" if author_id else display_name)
                entry = output.setdefault(
                    key,
                    {
                        "author_key": key,
                        "author_id": author_id,
                        "author_name": display_name,
                        "matched_names": Counter(),
                        "books": [],
                    },
                )
                entry["matched_names"][normalize_person_name((author.get("name") or "").strip()) or display_name] += 1
                seen_book_ids: Set[int] = {
                    int(book.get("book_id") or 0) for book in entry["books"] if int(book.get("book_id") or 0)
                }
                for contribution in author.get("contributions") or []:
                    if not is_primary_author_contribution((contribution.get("contribution") or "")):
                        continue
                    book = contribution.get("book") or {}
                    book_id = int(book.get("id") or 0)
                    if not book_id or book_id in seen_book_ids:
                        continue
                    seen_book_ids.add(book_id)
                    batch_books += 1
                    memberships: List[Dict[str, Any]] = []
                    seen_series_ids: Set[int] = set()
                    for book_series in book.get("book_series") or []:
                        series = book_series.get("series") or {}
                        canonical_series = series.get("canonical") or {}
                        chosen_series = canonical_series or series
                        series_id = int(chosen_series.get("id") or series.get("id") or 0)
                        if not series_id or series_id in seen_series_ids:
                            continue
                        seen_series_ids.add(series_id)
                        memberships.append(
                            {
                                "series_id": series_id,
                                "series_name": smart_title(chosen_series.get("name") or series.get("name") or ""),
                                "series_slug": (chosen_series.get("slug") or series.get("slug") or "").strip(),
                                "is_completed": chosen_series.get("is_completed"),
                                "books_count": int(chosen_series.get("books_count") or 0),
                                "primary_books_count": int(chosen_series.get("primary_books_count") or 0),
                                "position": book_series.get("position"),
                            }
                        )
                    entry["books"].append(
                        {
                            "book_id": book_id,
                            "title": smart_title(book.get("title") or ""),
                            "subtitle": smart_title(book.get("subtitle") or ""),
                            "release_date": book.get("release_date") or "",
                            "slug": (book.get("slug") or "").strip(),
                            "users_count": int(book.get("users_count") or 0),
                            "users_read_count": int(book.get("users_read_count") or 0),
                            "rating": float(book.get("rating") or 0.0),
                            "lists_count": int(book.get("lists_count") or 0),
                            "series_memberships": memberships,
                        }
                    )
            if verbose:
                after = self.stats_snapshot()
                net_delta = int(after["network_requests"] - before["network_requests"])
                cache_delta = int(after["cache_hits"] - before["cache_hits"])
                if net_delta and cache_delta:
                    batch_cache_status = "mixed"
                elif net_delta:
                    batch_cache_status = "miss"
                elif cache_delta:
                    batch_cache_status = "hit"
                else:
                    batch_cache_status = "none"
                unmatched_author_names = [name for name in name_chunk if normalize_author_key(name) not in matched_author_keys]
                author_display_items = matched_author_labels + unmatched_author_names
                author_sample = preview_names(author_display_items or name_chunk, limit=len(name_chunk), max_len=36)
                vlog(
                    verbose,
                    f"    [{batch_idx}/{total_batches}] authors={author_sample} matched={len(matched_author_keys)}/{len(name_chunk)} books={batch_books} batch_cache={batch_cache_status} {self.stats_delta_text(before, after)}",
                )
        return output
