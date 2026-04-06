from __future__ import annotations

import csv
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .config import ApplyCliConfig
from .file_metadata_writer import (
    FileMutationSession,
    apply_selected_file_metadata,
    build_file_metadata_payload,
    resolve_book_file_targets,
    select_file_write_target,
)
from .identifiers import (
    HARDCOVER_EDITION,
    HARDCOVER_ID,
    HARDCOVER_SLUG,
    LEGACY_IDENTIFIER_ALIASES,
    canonicalize_identifier_name,
    extract_numeric_id,
)
from .output import build_apply_outputs
from .runtime_io import TeeStream
from .runtime_support import resolve_runtime_paths
from .text_normalization import split_author_like_string

SUPPORTED_APPLY_ACTIONS = {
    "keep_hardcover_id",
    "replace_hardcover_id",
    "safe_auto_fix",
    "set_hardcover_id",
    "update_calibre_metadata",
}

MANUAL_REVIEW_ACTIONS = {
    "likely_non_english",
    "manual_review",
    "manual_review_title_match_author_unconfirmed",
    "suspected_author_mismatch",
    "suspected_file_mismatch",
}

ATTEMPTED_STATUSES = {"applied", "would_apply", "no_changes", "rolled_back", "error", "skipped_file_write"}

IDENTIFIER_ALIASES_BY_CANONICAL = {
    canonical: tuple(
        alias for alias, target in LEGACY_IDENTIFIER_ALIASES.items() if target == canonical and alias != canonical
    )
    for canonical in (HARDCOVER_ID, HARDCOVER_SLUG, HARDCOVER_EDITION)
}


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _normalize_identifier_value(identifier_name: str, raw_value: str) -> str:
    canonical_name = canonicalize_identifier_name(identifier_name)
    value = str(raw_value or "").strip()
    if not value:
        return ""
    if canonical_name in {HARDCOVER_ID, HARDCOVER_EDITION}:
        return extract_numeric_id(value) or value
    return value


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(sep=" ")


def _title_sort(value: Any) -> str:
    title = " ".join(str(value or "").strip().split())
    if not title:
        return ""
    for article in ("A", "An", "The"):
        prefix = f"{article} "
        if title.startswith(prefix):
            remainder = title[len(prefix) :].strip()
            if remainder:
                return f"{remainder}, {article}"
    return title


def _author_sort_fallback(name: str) -> str:
    current = " ".join(str(name or "").strip().split())
    if not current:
        return ""
    if "," in current:
        return current
    parts = current.split(" ")
    if len(parts) == 1:
        return current
    return f"{parts[-1]}, {' '.join(parts[:-1])}"


def _resolve_write_plan_path(library_root: Path, explicit_path: Optional[Path]) -> Path:
    candidates = [explicit_path.resolve()] if explicit_path else []
    if not explicit_path:
        candidates.extend(
            [
                (library_root / "audit" / "write_plan.csv").resolve(),
                (library_root / "write_plan.csv").resolve(),
            ]
        )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    candidate_list = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"write plan not found; looked at: {candidate_list}")


@dataclass(frozen=True)
class ApplyPlanRow:
    source_index: int
    action_type: str
    calibre_book_id: int
    confidence: float
    current_calibre_author: str
    current_calibre_title: str
    current_hardcover_author: str
    current_hardcover_edition_id: str
    current_hardcover_id: str
    current_hardcover_slug: str
    current_hardcover_title: str
    new_calibre_author: str
    new_calibre_title: str
    new_hardcover_edition_id: str
    new_hardcover_id: str
    new_hardcover_slug: str
    reason: str
    relink_confidence: str
    relink_reason: str
    safe_to_apply: bool
    safe_to_apply_reason: str
    title: str
    raw: Mapping[str, Any]

    @classmethod
    def from_csv_row(cls, index: int, row: Mapping[str, Any]) -> "ApplyPlanRow":
        return cls(
            source_index=index,
            action_type=str(row.get("action_type") or "").strip(),
            calibre_book_id=_to_int(row.get("calibre_book_id")),
            confidence=_to_float(row.get("confidence")),
            current_calibre_author=str(row.get("current_calibre_author") or "").strip(),
            current_calibre_title=str(row.get("current_calibre_title") or "").strip(),
            current_hardcover_author=str(row.get("current_hardcover_author") or "").strip(),
            current_hardcover_edition_id=_normalize_identifier_value(
                HARDCOVER_EDITION,
                str(row.get("current_hardcover_edition_id") or ""),
            ),
            current_hardcover_id=_normalize_identifier_value(
                HARDCOVER_ID,
                str(row.get("current_hardcover_id") or ""),
            ),
            current_hardcover_slug=str(row.get("current_hardcover_slug") or "").strip(),
            current_hardcover_title=str(row.get("current_hardcover_title") or "").strip(),
            new_calibre_author=str(row.get("new_calibre_author") or "").strip(),
            new_calibre_title=str(row.get("new_calibre_title") or "").strip(),
            new_hardcover_edition_id=_normalize_identifier_value(
                HARDCOVER_EDITION,
                str(row.get("new_hardcover_edition_id") or ""),
            ),
            new_hardcover_id=_normalize_identifier_value(
                HARDCOVER_ID,
                str(row.get("new_hardcover_id") or ""),
            ),
            new_hardcover_slug=str(
                row.get("new_hardcover_slug")
                or row.get("suggested_hardcover_slug")
                or ""
            ).strip(),
            reason=str(row.get("reason") or "").strip(),
            relink_confidence=str(row.get("relink_confidence") or "").strip(),
            relink_reason=str(row.get("relink_reason") or "").strip(),
            safe_to_apply=_to_bool(row.get("safe_to_apply_boolean")),
            safe_to_apply_reason=str(row.get("safe_to_apply_reason") or "").strip(),
            title=str(row.get("title") or row.get("current_calibre_title") or "").strip(),
            raw=dict(row),
        )

    @property
    def is_manual_review(self) -> bool:
        return self.action_type.startswith("manual_review") or self.action_type in MANUAL_REVIEW_ACTIONS

    @property
    def requested_title(self) -> str:
        return self.new_calibre_title or self.current_calibre_title or self.title

    @property
    def requested_authors(self) -> str:
        return self.new_calibre_author or self.current_calibre_author

    @property
    def requested_hardcover_id(self) -> str:
        return self.new_hardcover_id or self.current_hardcover_id

    @property
    def requested_hardcover_edition_id(self) -> str:
        return self.new_hardcover_edition_id or self.current_hardcover_edition_id


@dataclass
class BookState:
    title: str
    author_sort: str
    authors: list[str]
    identifiers: dict[str, str]


@dataclass(frozen=True)
class DesiredMetadata:
    title: str
    authors: tuple[str, ...]
    hardcover_id: str
    hardcover_slug: str
    hardcover_edition_id: str


def load_apply_plan(path: Path) -> list[ApplyPlanRow]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [ApplyPlanRow.from_csv_row(index + 1, row) for index, row in enumerate(csv.DictReader(handle))]


def _open_metadata_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.create_function("title_sort", 1, _title_sort)
    return connection


def _fetch_book_state(connection: sqlite3.Connection, calibre_book_id: int) -> BookState:
    book_row = connection.execute(
        "SELECT title, author_sort FROM books WHERE id = ?",
        (calibre_book_id,),
    ).fetchone()
    if book_row is None:
        raise LookupError(f"calibre book id {calibre_book_id} was not found in metadata.db")
    author_rows = connection.execute(
        """
        SELECT a.name
        FROM books_authors_link bal
        JOIN authors a ON a.id = bal.author
        WHERE bal.book = ?
        ORDER BY bal.id
        """,
        (calibre_book_id,),
    ).fetchall()
    identifier_rows = connection.execute(
        "SELECT type, val FROM identifiers WHERE book = ? ORDER BY id",
        (calibre_book_id,),
    ).fetchall()
    identifier_map: dict[str, str] = {}
    for row in identifier_rows:
        canonical = canonicalize_identifier_name(str(row["type"] or ""))
        if canonical in {HARDCOVER_ID, HARDCOVER_SLUG, HARDCOVER_EDITION}:
            identifier_map[canonical] = str(row["val"] or "").strip()
    return BookState(
        title=str(book_row["title"] or "").strip(),
        author_sort=str(book_row["author_sort"] or "").strip(),
        authors=[str(row["name"] or "").strip() for row in author_rows],
        identifiers=identifier_map,
    )


def _delete_identifier_types(connection: sqlite3.Connection, calibre_book_id: int, types: Iterable[str]) -> int:
    deleted = 0
    for identifier_type in types:
        cursor = connection.execute(
            "DELETE FROM identifiers WHERE book = ? AND type = ?",
            (calibre_book_id, identifier_type),
        )
        deleted += int(cursor.rowcount or 0)
    return deleted


def _upsert_identifier(
    connection: sqlite3.Connection,
    calibre_book_id: int,
    identifier_name: str,
    value: str,
) -> bool:
    canonical_name = canonicalize_identifier_name(identifier_name)
    alias_names = IDENTIFIER_ALIASES_BY_CANONICAL.get(canonical_name, ())
    current_row = connection.execute(
        "SELECT val FROM identifiers WHERE book = ? AND type = ?",
        (calibre_book_id, canonical_name),
    ).fetchone()
    current_value = str(current_row["val"] or "").strip() if current_row is not None else ""
    aliases_deleted = _delete_identifier_types(connection, calibre_book_id, alias_names)
    if not value:
        canonical_deleted = _delete_identifier_types(connection, calibre_book_id, (canonical_name,))
        return bool(aliases_deleted or canonical_deleted)
    connection.execute(
        """
        INSERT INTO identifiers (book, type, val)
        VALUES (?, ?, ?)
        ON CONFLICT(book, type) DO UPDATE SET val = excluded.val
        """,
        (calibre_book_id, canonical_name, value),
    )
    return bool(aliases_deleted or current_value != value)


def _ensure_author(connection: sqlite3.Connection, author_name: str) -> tuple[int, str]:
    row = connection.execute(
        "SELECT id, sort FROM authors WHERE name = ? COLLATE NOCASE",
        (author_name,),
    ).fetchone()
    if row is not None:
        return int(row["id"]), str(row["sort"] or "").strip() or _author_sort_fallback(author_name)
    author_sort = _author_sort_fallback(author_name)
    cursor = connection.execute(
        "INSERT INTO authors (name, sort, link) VALUES (?, ?, '')",
        (author_name, author_sort),
    )
    return int(cursor.lastrowid), author_sort


def _update_authors_for_book(
    connection: sqlite3.Connection,
    calibre_book_id: int,
    author_names: Sequence[str],
) -> str:
    existing_author_rows = connection.execute(
        "SELECT author FROM books_authors_link WHERE book = ? ORDER BY id",
        (calibre_book_id,),
    ).fetchall()
    existing_author_ids = [int(row["author"]) for row in existing_author_rows]
    connection.execute("DELETE FROM books_authors_link WHERE book = ?", (calibre_book_id,))
    author_sort_values: list[str] = []
    for author_name in author_names:
        author_id, author_sort = _ensure_author(connection, author_name)
        connection.execute(
            "INSERT INTO books_authors_link (book, author) VALUES (?, ?)",
            (calibre_book_id, author_id),
        )
        author_sort_values.append(author_sort)
    for author_id in existing_author_ids:
        still_referenced = connection.execute(
            "SELECT 1 FROM books_authors_link WHERE author = ? LIMIT 1",
            (author_id,),
        ).fetchone()
        if still_referenced is None:
            connection.execute("DELETE FROM authors WHERE id = ?", (author_id,))
    return " & ".join(value for value in author_sort_values if value)


def _book_last_modified(connection: sqlite3.Connection, calibre_book_id: int, timestamp: str) -> None:
    connection.execute(
        "UPDATE books SET last_modified = ? WHERE id = ?",
        (timestamp, calibre_book_id),
    )


def _base_log_row(plan_row: ApplyPlanRow) -> dict[str, Any]:
    return {
        "source_index": plan_row.source_index,
        "calibre_book_id": plan_row.calibre_book_id,
        "action_type": plan_row.action_type,
        "confidence": plan_row.confidence,
        "safe_to_apply_boolean": plan_row.safe_to_apply,
        "safe_to_apply_reason": plan_row.safe_to_apply_reason,
        "current_calibre_title": plan_row.current_calibre_title,
        "new_calibre_title": plan_row.new_calibre_title,
        "current_calibre_author": plan_row.current_calibre_author,
        "new_calibre_author": plan_row.new_calibre_author,
        "current_hardcover_id": plan_row.current_hardcover_id,
        "new_hardcover_id": plan_row.new_hardcover_id,
        "current_hardcover_slug": plan_row.current_hardcover_slug,
        "new_hardcover_slug": plan_row.new_hardcover_slug,
        "current_hardcover_edition_id": plan_row.current_hardcover_edition_id,
        "new_hardcover_edition_id": plan_row.new_hardcover_edition_id,
        "reason": plan_row.reason,
        "relink_reason": plan_row.relink_reason,
        "db_write_requested": False,
        "db_write_status": "not_requested",
        "db_write_reason": "metadata.db writes not enabled",
        "db_changed_fields": "",
        "file_write_requested": False,
        "file_write_target": "",
        "file_write_path": "",
        "file_write_status": "not_requested",
        "file_write_reason": "file metadata writes not enabled",
        "file_changed_fields": "",
    }


def _resolve_desired_metadata(
    plan_row: ApplyPlanRow,
    state: BookState,
    *,
    include_calibre_title_author: bool,
) -> DesiredMetadata:
    desired_hardcover_id = _normalize_identifier_value(
        HARDCOVER_ID,
        plan_row.requested_hardcover_id or state.identifiers.get(HARDCOVER_ID, ""),
    )
    desired_hardcover_edition_id = _normalize_identifier_value(
        HARDCOVER_EDITION,
        plan_row.requested_hardcover_edition_id or state.identifiers.get(HARDCOVER_EDITION, ""),
    )
    current_db_hardcover_id = _normalize_identifier_value(HARDCOVER_ID, state.identifiers.get(HARDCOVER_ID, ""))
    explicit_new_slug = _normalize_identifier_value(HARDCOVER_SLUG, plan_row.new_hardcover_slug)
    if explicit_new_slug:
        desired_hardcover_slug = explicit_new_slug
    elif desired_hardcover_id and desired_hardcover_id == current_db_hardcover_id:
        desired_hardcover_slug = state.identifiers.get(HARDCOVER_SLUG, "")
    else:
        desired_hardcover_slug = ""

    requested_title = (plan_row.requested_title.strip() or state.title) if include_calibre_title_author else state.title
    requested_author_names = (
        tuple(split_author_like_string(plan_row.requested_authors)) or tuple(state.authors)
    ) if include_calibre_title_author else tuple(state.authors)
    return DesiredMetadata(
        title=requested_title,
        authors=requested_author_names,
        hardcover_id=desired_hardcover_id,
        hardcover_slug=desired_hardcover_slug,
        hardcover_edition_id=desired_hardcover_edition_id,
    )


def _apply_database_changes(
    connection: sqlite3.Connection,
    plan_row: ApplyPlanRow,
    desired: DesiredMetadata,
    *,
    include_calibre_title_author: bool,
) -> list[str]:
    state = _fetch_book_state(connection, plan_row.calibre_book_id)
    timestamp = _utc_timestamp()
    changed_fields: list[str] = []

    if desired.hardcover_id and _upsert_identifier(
        connection,
        plan_row.calibre_book_id,
        HARDCOVER_ID,
        desired.hardcover_id,
    ):
        changed_fields.append(HARDCOVER_ID)
    if desired.hardcover_edition_id and _upsert_identifier(
        connection,
        plan_row.calibre_book_id,
        HARDCOVER_EDITION,
        desired.hardcover_edition_id,
    ):
        changed_fields.append(HARDCOVER_EDITION)
    if _upsert_identifier(
        connection,
        plan_row.calibre_book_id,
        HARDCOVER_SLUG,
        desired.hardcover_slug,
    ):
        changed_fields.append(HARDCOVER_SLUG)

    if include_calibre_title_author:
        if desired.title and desired.title != state.title:
            connection.execute(
                "UPDATE books SET title = ? WHERE id = ?",
                (desired.title, plan_row.calibre_book_id),
            )
            changed_fields.append("calibre_title")
        requested_author_names = list(desired.authors)
        if requested_author_names and requested_author_names != state.authors:
            book_author_sort = _update_authors_for_book(
                connection,
                plan_row.calibre_book_id,
                requested_author_names,
            )
            connection.execute(
                "UPDATE books SET author_sort = ? WHERE id = ?",
                (book_author_sort, plan_row.calibre_book_id),
            )
            changed_fields.append("calibre_authors")

    if changed_fields:
        _book_last_modified(connection, plan_row.calibre_book_id, timestamp)
    return changed_fields


def _refresh_overall_status(log_row: dict[str, Any]) -> None:
    db_status = str(log_row.get("db_write_status") or "")
    file_status = str(log_row.get("file_write_status") or "")
    db_requested = bool(log_row.get("db_write_requested"))
    file_requested = bool(log_row.get("file_write_requested"))

    if "error" in {db_status, file_status}:
        log_row["status"] = "error"
        log_row["status_reason"] = log_row.get("file_write_reason") or log_row.get("db_write_reason") or "error"
        return
    if "rolled_back" in {db_status, file_status}:
        log_row["status"] = "rolled_back"
        log_row["status_reason"] = "transaction rolled back after a later row failed"
        return
    if "would_apply" in {db_status, file_status}:
        log_row["status"] = "would_apply"
        reasons = [reason for reason in (log_row.get("db_write_reason"), log_row.get("file_write_reason")) if reason]
        log_row["status_reason"] = "; ".join(reasons) or "dry-run would apply changes"
        return
    if "applied" in {db_status, file_status}:
        log_row["status"] = "applied"
        reasons = [reason for reason in (log_row.get("db_write_reason"), log_row.get("file_write_reason")) if reason]
        log_row["status_reason"] = "; ".join(reasons) or "changes applied"
        return
    if file_requested and file_status in {"skipped_no_target", "skipped_unsupported_format"} and not db_requested:
        log_row["status"] = "skipped_file_write"
        log_row["status_reason"] = log_row.get("file_write_reason") or "requested file metadata target was unavailable"
        return
    if file_requested and file_status in {"skipped_no_target", "skipped_unsupported_format"} and db_status in {
        "no_changes",
        "skipped_mode",
        "not_requested",
    }:
        log_row["status"] = "skipped_file_write"
        log_row["status_reason"] = log_row.get("file_write_reason") or "requested file metadata target was unavailable"
        return
    log_row["status"] = "no_changes"
    reasons = []
    if db_requested and db_status == "no_changes":
        reasons.append("database already matched the selected apply scope")
    if file_requested and file_status == "no_changes":
        reasons.append("file metadata already matched the selected apply scope")
    log_row["status_reason"] = "; ".join(reasons) or "no changes were needed"


def _attempt_row_apply(
    connection: sqlite3.Connection,
    library_root: Path,
    plan_row: ApplyPlanRow,
    *,
    config: ApplyCliConfig,
    file_session: FileMutationSession,
) -> dict[str, Any]:
    state = _fetch_book_state(connection, plan_row.calibre_book_id)
    desired = _resolve_desired_metadata(
        plan_row,
        state,
        include_calibre_title_author=config.include_calibre_title_author,
    )
    log_row = _base_log_row(plan_row)
    log_row["resolved_new_hardcover_slug"] = desired.hardcover_slug

    if config.write_db:
        db_changed_fields = _apply_database_changes(
            connection,
            plan_row,
            desired,
            include_calibre_title_author=config.include_calibre_title_author,
        )
        log_row["db_write_requested"] = True
        log_row["db_changed_fields"] = ",".join(db_changed_fields)
        if db_changed_fields:
            log_row["db_write_status"] = "applied"
            log_row["db_write_reason"] = "metadata.db updated"
        else:
            log_row["db_write_status"] = "no_changes"
            log_row["db_write_reason"] = "metadata.db already matched the selected apply scope"
    else:
        log_row["db_write_requested"] = False
        log_row["db_write_status"] = "skipped_mode"
        log_row["db_write_reason"] = "files-only mode skipped metadata.db writes"

    if config.write_sidecar_opf or config.write_epub_opf:
        log_row["file_write_requested"] = True
        targets = resolve_book_file_targets(connection, library_root, plan_row.calibre_book_id)
        selection = select_file_write_target(
            targets,
            allow_sidecar_opf=config.write_sidecar_opf,
            allow_epub_opf=config.write_epub_opf,
            prefer_sidecar_opf=config.prefer_sidecar_opf,
        )
        payload = build_file_metadata_payload(
            title=desired.title,
            authors=desired.authors,
            include_title_author=config.include_calibre_title_author,
            hardcover_id=desired.hardcover_id,
            hardcover_slug=desired.hardcover_slug,
            hardcover_edition_id=desired.hardcover_edition_id,
        )
        file_result = apply_selected_file_metadata(
            selection,
            payload,
            dry_run=config.dry_run,
            session=file_session,
        )
        log_row["file_write_target"] = file_result.target_kind
        log_row["file_write_path"] = str(file_result.target_path) if file_result.target_path else ""
        log_row["file_write_status"] = file_result.status
        log_row["file_write_reason"] = file_result.reason
        log_row["file_changed_fields"] = ",".join(file_result.changed_fields)
    else:
        log_row["file_write_requested"] = False
        log_row["file_write_status"] = "not_requested"
        log_row["file_write_reason"] = "file metadata writes not enabled"

    _refresh_overall_status(log_row)
    return log_row


def run_apply(config: ApplyCliConfig) -> int:
    runtime_paths = resolve_runtime_paths(
        library_root=config.library_root,
        metadata_db=config.metadata_db,
        output_dir=config.output_dir,
        cache_path=None,
    )

    try:
        write_plan_path = _resolve_write_plan_path(runtime_paths.library_root, config.write_plan)
        plan_rows = load_apply_plan(write_plan_path)
    except (FileNotFoundError, OSError, csv.Error) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    with runtime_paths.log_path.open("w", encoding="utf-8") as log_handle:
        try:
            sys.stdout = TeeStream(original_stdout, log_handle)
            sys.stderr = TeeStream(original_stderr, log_handle)

            print(f"Using library root: {runtime_paths.library_root}")
            print(f"Using metadata DB: {runtime_paths.metadata_db}")
            print(f"Using write plan: {write_plan_path}")
            print(f"Writing outputs to: {runtime_paths.output_dir}")
            print(f"Writing log to: {runtime_paths.log_path}")
            print(
                "Apply mode: "
                f"safe_only={'yes' if config.apply_safe_only else 'no'} | "
                f"identifiers_only={'yes' if config.include_identifiers_only else 'no'} | "
                f"include_calibre_title_author={'yes' if config.include_calibre_title_author else 'no'} | "
                f"write_db={'yes' if config.write_db else 'no'} | "
                f"write_sidecar_opf={'yes' if config.write_sidecar_opf else 'no'} | "
                f"write_epub_opf={'yes' if config.write_epub_opf else 'no'} | "
                f"prefer_sidecar_opf={'yes' if config.prefer_sidecar_opf else 'no'} | "
                f"dry_run={'yes' if config.dry_run else 'no'}"
            )
            if config.apply_actions:
                print(f"Action filter: {', '.join(config.apply_actions)}")
            if config.limit is not None:
                print(f"Apply row limit: {config.limit}")
            print(f"Loaded {len(plan_rows)} write-plan rows")

            selected_rows: list[ApplyPlanRow] = []
            apply_log_rows: list[dict[str, Any]] = []
            requested_actions = set(config.apply_actions)

            for plan_row in plan_rows:
                if plan_row.is_manual_review:
                    log_row = _base_log_row(plan_row)
                    log_row["status"] = "skipped_manual_review"
                    log_row["status_reason"] = "manual-review style actions are never applied by default"
                    apply_log_rows.append(log_row)
                    continue
                if plan_row.action_type not in SUPPORTED_APPLY_ACTIONS:
                    log_row = _base_log_row(plan_row)
                    log_row["status"] = "skipped_unsupported_action"
                    log_row["status_reason"] = "action is not supported by the Stage 3 apply engine"
                    apply_log_rows.append(log_row)
                    continue
                if requested_actions and plan_row.action_type not in requested_actions:
                    log_row = _base_log_row(plan_row)
                    log_row["status"] = "skipped_action_filter"
                    log_row["status_reason"] = "action did not match --apply-actions"
                    apply_log_rows.append(log_row)
                    continue
                if config.apply_safe_only and not plan_row.safe_to_apply:
                    log_row = _base_log_row(plan_row)
                    log_row["status"] = "skipped_not_safe"
                    log_row["status_reason"] = plan_row.safe_to_apply_reason or "row was not marked safe_to_apply"
                    apply_log_rows.append(log_row)
                    continue
                selected_rows.append(plan_row)

            limited_rows = selected_rows[: config.limit] if config.limit is not None else selected_rows
            skipped_due_to_limit = selected_rows[len(limited_rows) :]
            for plan_row in skipped_due_to_limit:
                log_row = _base_log_row(plan_row)
                log_row["status"] = "skipped_limit"
                log_row["status_reason"] = "row was outside the requested --limit window"
                apply_log_rows.append(log_row)

            transaction_status = "not_started"
            exit_code = 0
            attempted_count = 0
            with _open_metadata_connection(runtime_paths.metadata_db) as connection:
                attempted_logs: list[dict[str, Any]] = []
                file_session = FileMutationSession()
                try:
                    if config.write_db:
                        connection.execute("BEGIN IMMEDIATE")
                        transaction_status = "in_progress"
                    else:
                        transaction_status = "files_only_in_progress"
                    for plan_row in limited_rows:
                        attempted_count += 1
                        attempted_log = _attempt_row_apply(
                            connection,
                            runtime_paths.library_root,
                            plan_row,
                            config=config,
                            file_session=file_session,
                        )
                        attempted_logs.append(attempted_log)
                    if config.dry_run:
                        if config.write_db:
                            connection.rollback()
                        file_session.restore_all()
                        transaction_status = "rolled_back_dry_run"
                        for log_row in attempted_logs:
                            if log_row["db_write_status"] == "applied":
                                log_row["db_write_status"] = "would_apply"
                                log_row["db_write_reason"] = "dry-run would update metadata.db"
                            if log_row["file_write_status"] == "applied":
                                log_row["file_write_status"] = "would_apply"
                                log_row["file_write_reason"] = "dry-run would update file metadata"
                            elif log_row["file_write_status"] == "no_changes":
                                log_row["file_write_reason"] = "dry-run verified file metadata already matched"
                            _refresh_overall_status(log_row)
                        apply_log_rows.extend(attempted_logs)
                    else:
                        if config.write_db:
                            connection.commit()
                            transaction_status = "committed"
                        else:
                            transaction_status = "completed_files_only"
                        apply_log_rows.extend(attempted_logs)
                except Exception as exc:
                    if config.write_db:
                        connection.rollback()
                    try:
                        file_session.restore_all()
                    except Exception as restore_exc:
                        exc = RuntimeError(f"{exc}; file rollback failed: {restore_exc}")
                    transaction_status = "rolled_back_error"
                    exit_code = 1
                    for log_row in attempted_logs:
                        if log_row["db_write_status"] == "applied":
                            log_row["db_write_status"] = "rolled_back"
                            log_row["db_write_reason"] = "metadata.db transaction rolled back after a later row failed"
                        if log_row["file_write_status"] == "applied":
                            log_row["file_write_status"] = "rolled_back"
                            log_row["file_write_reason"] = "file metadata restored after a later row failed"
                        _refresh_overall_status(log_row)
                    apply_log_rows.extend(attempted_logs)
                    failing_log = _base_log_row(
                        limited_rows[min(attempted_count - 1, len(limited_rows) - 1)]
                    ) if limited_rows else None
                    if failing_log is not None:
                        failing_log["status"] = "error"
                        failing_log["status_reason"] = str(exc)
                        failing_log["db_write_requested"] = config.write_db
                        failing_log["db_write_status"] = "error" if config.write_db else "skipped_mode"
                        failing_log["db_write_reason"] = str(exc) if config.write_db else "files-only mode skipped metadata.db writes"
                        failing_log["file_write_requested"] = bool(config.write_sidecar_opf or config.write_epub_opf)
                        failing_log["file_write_status"] = "error" if failing_log["file_write_requested"] else "not_requested"
                        failing_log["file_write_reason"] = str(exc) if failing_log["file_write_requested"] else "file metadata writes not enabled"
                        apply_log_rows.append(failing_log)
                    print(f"ERROR: apply transaction failed: {exc}", file=sys.stderr)
                finally:
                    file_session.cleanup()

            output_paths = build_apply_outputs(
                apply_log_rows,
                runtime_paths.output_dir,
                summary={
                    "input_rows": len(plan_rows),
                    "selected_rows": len(limited_rows),
                    "attempted_rows": sum(
                        1 for row in apply_log_rows if str(row.get("status") or "") in ATTEMPTED_STATUSES
                    ),
                    "transaction_status": transaction_status,
                    "dry_run": config.dry_run,
                    "metadata_db_path": str(runtime_paths.metadata_db),
                    "write_plan_path": str(write_plan_path),
                    "apply_safe_only": config.apply_safe_only,
                    "include_identifiers_only": config.include_identifiers_only,
                    "include_calibre_title_author": config.include_calibre_title_author,
                    "write_db": config.write_db,
                    "write_sidecar_opf": config.write_sidecar_opf,
                    "write_epub_opf": config.write_epub_opf,
                    "prefer_sidecar_opf": config.prefer_sidecar_opf,
                    "apply_actions_display": ", ".join(config.apply_actions) if config.apply_actions else "",
                    "limit_display": str(config.limit) if config.limit is not None else "",
                },
            )

            print(f"Selected rows for apply: {len(limited_rows)}")
            print(f"Attempted rows: {attempted_count}")
            print(f"Transaction outcome: {transaction_status}")
            print(f"Apply summary: {output_paths['summary']}")
            print(f"Apply log: {output_paths['apply_log']}")
            return exit_code
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr
