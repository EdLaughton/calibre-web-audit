from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Sequence

from .audit_insights import (
    build_metadata_probe_rollup,
    build_reason_family_rollups,
    metadata_probe_diagnostic,
)
from .audit_reporting import (
    bucket_sort_key,
    build_compact_audit_actions,
    build_write_plan,
    classify_manual_review_bucket,
    filter_compact_write_plan_rows,
)
from .bookshelf_export import (
    BOOKSHELF_PUSH_LOG_COLUMNS,
    BOOKSHELF_QUEUE_COLUMNS,
    BookshelfIntegrationResult,
)
from .runtime_io import ensure_dir, write_csv, write_json

AUDIT_OPERATOR_COLUMNS = [
    "review_source",
    "action_bucket",
    "recommended_action",
    "safe_to_apply_boolean",
    "safe_to_apply_reason",
    "confidence_score",
    "confidence_tier",
    "calibre_book_id",
    "calibre_title",
    "calibre_authors",
    "calibre_series",
    "current_hardcover_id",
    "current_hardcover_slug",
    "current_hardcover_title",
    "current_hardcover_authors",
    "current_hardcover_match_ok",
    "suggested_hardcover_id",
    "suggested_hardcover_slug",
    "suggested_hardcover_title",
    "suggested_hardcover_authors",
    "suggested_hardcover_edition_id",
    "suggested_hardcover_edition_title",
    "suggested_hardcover_edition_format_normalized",
    "suggested_hardcover_edition_language",
    "suggested_calibre_title",
    "suggested_calibre_authors",
    "reason",
    "issue_category",
    "matched_by",
    "fix_basis",
    "metadata_probe_warning",
    "metadata_probe_details",
    "duplicate_group_id",
    "duplicate_group_size",
    "suggested_display_name",
    "note",
    "file_work_title",
    "file_work_authors",
    "file_work_title_basis",
    "file_work_authors_basis",
    "ebook_meta_tool_used",
    "file_path",
]


@dataclass(frozen=True)
class CommandOutputPaths:
    root: Path
    summary: Path
    readme: Path


@dataclass(frozen=True)
class AuditOutputPaths(CommandOutputPaths):
    actions_operator: Path
    actions: Path
    write_plan: Path


@dataclass(frozen=True)
class DiscoveryOutputPaths(CommandOutputPaths):
    candidates: Path
    bookshelf_queue: Path | None = None
    bookshelf_queue_json: Path | None = None
    bookshelf_push_log: Path | None = None
    bookshelf_summary: Path | None = None


@dataclass(frozen=True)
class ApplyOutputPaths(CommandOutputPaths):
    apply_log: Path


def _write_summary(path: Path, lines: Iterable[str]) -> None:
    ensure_dir(path.parent)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _write_root_readme(output_dir: Path, section_name: str, lines: Sequence[str]) -> Path:
    readme_lines = ["# Output overview", "", f"## {section_name}", *lines, "", "`run.log` remains in the root output directory for the full execution trace."]
    path = output_dir / "README.md"
    _write_summary(path, readme_lines)
    return path


def _enrich_write_plan_rows(rows: Sequence[Any], write_plan: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    audit_rows_by_book_id = {
        int(getattr(row, "calibre_book_id", 0) or 0): row
        for row in rows
        if int(getattr(row, "calibre_book_id", 0) or 0)
    }
    enriched_rows: list[dict[str, Any]] = []
    for plan_row in write_plan:
        row = dict(plan_row)
        audit_row = audit_rows_by_book_id.get(int(row.get("calibre_book_id") or 0))
        current_hardcover_slug = str(getattr(audit_row, "calibre_hardcover_slug", "") or "")
        suggested_hardcover_slug = str(getattr(audit_row, "suggested_hardcover_slug", "") or "")
        current_hardcover_id = str(row.get("current_hardcover_id") or "")
        new_hardcover_id = str(row.get("new_hardcover_id") or "")
        new_hardcover_slug = suggested_hardcover_slug or (
            current_hardcover_slug if new_hardcover_id == current_hardcover_id else ""
        )
        row["current_hardcover_slug"] = current_hardcover_slug
        row["new_hardcover_slug"] = new_hardcover_slug
        enriched_rows.append(row)
    return enriched_rows


def _build_actions_operator_rows(audit_actions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in audit_actions:
        metadata_probe_warning, metadata_probe_details = metadata_probe_diagnostic(row)
        operator_row = {
            "review_source": row.get("review_source", ""),
            "action_bucket": row.get("action_bucket", ""),
            "recommended_action": row.get("recommended_action", ""),
            "safe_to_apply_boolean": row.get("safe_to_apply_boolean", ""),
            "safe_to_apply_reason": row.get("safe_to_apply_reason", ""),
            "confidence_score": row.get("confidence_score", ""),
            "confidence_tier": row.get("confidence_tier", ""),
            "calibre_book_id": row.get("calibre_book_id", ""),
            "calibre_title": row.get("calibre_title", ""),
            "calibre_authors": row.get("calibre_authors", ""),
            "calibre_series": row.get("calibre_series", ""),
            "current_hardcover_id": row.get("calibre_hardcover_id", ""),
            "current_hardcover_slug": row.get("calibre_hardcover_slug", ""),
            "current_hardcover_title": row.get("current_hardcover_title", ""),
            "current_hardcover_authors": row.get("current_hardcover_authors", ""),
            "current_hardcover_match_ok": row.get("current_hardcover_match_ok", ""),
            "suggested_hardcover_id": row.get("suggested_hardcover_id", ""),
            "suggested_hardcover_slug": row.get("suggested_hardcover_slug", ""),
            "suggested_hardcover_title": row.get("suggested_hardcover_title", ""),
            "suggested_hardcover_authors": row.get("suggested_hardcover_authors", ""),
            "suggested_hardcover_edition_id": row.get("suggested_hardcover_edition_id", ""),
            "suggested_hardcover_edition_title": row.get("suggested_hardcover_edition_title", ""),
            "suggested_hardcover_edition_format_normalized": row.get(
                "suggested_hardcover_edition_format_normalized",
                "",
            ),
            "suggested_hardcover_edition_language": row.get("suggested_hardcover_edition_language", ""),
            "suggested_calibre_title": row.get("suggested_calibre_title", ""),
            "suggested_calibre_authors": row.get("suggested_calibre_authors", ""),
            "reason": row.get("reason", ""),
            "issue_category": row.get("issue_category", ""),
            "matched_by": row.get("matched_by", ""),
            "fix_basis": row.get("fix_basis", ""),
            "metadata_probe_warning": metadata_probe_warning,
            "metadata_probe_details": metadata_probe_details,
            "duplicate_group_id": row.get("duplicate_group_id", ""),
            "duplicate_group_size": row.get("duplicate_group_size", ""),
            "suggested_display_name": row.get("suggested_display_name", ""),
            "note": row.get("note", ""),
            "file_work_title": row.get("file_work_title", ""),
            "file_work_authors": row.get("file_work_authors", ""),
            "file_work_title_basis": row.get("file_work_title_basis", ""),
            "file_work_authors_basis": row.get("file_work_authors_basis", ""),
            "ebook_meta_tool_used": row.get("ebook_meta_tool_used", ""),
            "file_path": row.get("file_path", ""),
        }
        output.append(operator_row)
    return output


def build_audit_outputs(rows: Sequence[Any], output_dir: Path) -> AuditOutputPaths:
    ensure_dir(output_dir)
    audit_dir = output_dir / "audit"
    ensure_dir(audit_dir)

    audit_actions = build_compact_audit_actions(list(rows))
    audit_actions_operator = _build_actions_operator_rows(audit_actions)
    write_plan = _enrich_write_plan_rows(
        rows,
        filter_compact_write_plan_rows(build_write_plan(list(rows))),
    )

    write_csv(audit_dir / "actions.csv", audit_actions)
    write_csv(audit_dir / "actions_operator.csv", audit_actions_operator)
    write_csv(audit_dir / "write_plan.csv", write_plan)

    action_counts = Counter(getattr(row, "recommended_action", "") for row in rows)
    tier_counts = Counter(getattr(row, "confidence_tier", "") or "unknown" for row in rows)
    action_bucket_counts = Counter(str(row.get("action_bucket") or "unknown") for row in audit_actions)
    review_source_counts = Counter(str(row.get("review_source") or "unknown") for row in audit_actions)
    safe_actions = sum(1 for row in audit_actions if bool(row.get("safe_to_apply_boolean")))
    safe_write_plan_rows = sum(1 for row in write_plan if bool(row.get("safe_to_apply_boolean")))
    reason_family_rollups = build_reason_family_rollups(rows, audit_actions)
    metadata_probe_counts, metadata_probe_samples = build_metadata_probe_rollup(rows)
    top_fix = sorted(
        (row for row in rows if getattr(row, "recommended_action", "") != "keep_hardcover_id"),
        key=bucket_sort_key,
    )[:100]

    summary_lines = [
        "# Audit summary",
        "",
        f"- Total books audited: **{len(rows)}**",
        f"- Action rows written: **{len(audit_actions)}**",
        f"- Write-plan rows written (full-library apply sheet): **{len(write_plan)}**",
        f"- Safe-to-apply audit rows: **{safe_actions}**",
        f"- Safe-to-apply write-plan rows: **{safe_write_plan_rows}**",
        "",
        "## Recommended actions",
    ]
    for action, count in sorted(action_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {action}: **{count}**")
    summary_lines.extend(["", "## Action buckets in actions.csv"])
    for label, count in sorted(action_bucket_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {label}: **{count}**")
    summary_lines.extend(["", "## Review sources"])
    for label, count in sorted(review_source_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {label}: **{count}**")
    summary_lines.extend(["", "## Confidence tiers"])
    for tier, count in sorted(tier_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {tier}: **{count}**")
    summary_lines.extend(["", "## Operator rollups"])
    for label, count in reason_family_rollups:
        summary_lines.append(f"- {label}: **{count}**")
    summary_lines.extend(["", "## Metadata probe diagnostics"])
    if metadata_probe_counts:
        for label, count in sorted(metadata_probe_counts.items(), key=lambda item: (-item[1], item[0])):
            summary_lines.append(f"- {label}: **{count}**")
            for sample in metadata_probe_samples.get(label, [])[:3]:
                summary_lines.append(f"sample: {sample}")
    else:
        summary_lines.append("- No obvious metadata-probe oddities detected in audit rows.")
    summary_lines.extend(["", "## Highest-priority rows", ""])
    for row in top_fix:
        review_bucket = classify_manual_review_bucket(row) if row.recommended_action == "manual_review" else ""
        bucket_hint = f" | review_bucket={review_bucket}" if review_bucket else ""
        summary_lines.append(
            f"- [{row.recommended_action}] calibre_id={row.calibre_book_id} | {row.calibre_title} | "
            f"hc={row.suggested_hardcover_id or row.calibre_hardcover_id} | "
            f"score={row.confidence_score} ({row.confidence_tier}){bucket_hint} | {row.reason}"
        )
    summary_lines.extend(
        [
            "",
            "## Operator hints",
            "- Review `actions_operator.csv` first for human triage. It keeps the same action-row set as `actions.csv` but uses a compact review-first column layout.",
            "- `actions.csv` remains the full forensic audit artifact for non-keep rows and review rows.",
            "- `write_plan.csv` is the full-library apply sheet. Filter it by `safe_to_apply_boolean`, `safe_to_apply_reason`, and `action_type` before running `apply`.",
            "- `safe_to_apply_boolean=True` means the row passed the current apply guardrails. It does not override the need to review changes or to use `apply --dry-run` first.",
            "",
            "## Files",
            "- actions_operator.csv — compact operator review sheet derived from the existing audit action rows",
            "- actions.csv — prioritized non-keep audit rows plus duplicate/author review rows",
            "- write_plan.csv — full-library apply sheet, including keep_hardcover_id confirmations and unsafe rows for filtering",
        ]
    )
    _write_summary(audit_dir / "summary.md", summary_lines)

    readme_path = _write_root_readme(
        output_dir,
        "Audit",
        [
            f"- Summary: `{(audit_dir / 'summary.md').name}`",
            f"- Operator review sheet: `{(audit_dir / 'actions_operator.csv').name}` — compact triage-first layout derived from the action rows",
            f"- Actions: `{(audit_dir / 'actions.csv').name}` — prioritized non-keep audit rows and review rows",
            f"- Write plan: `{(audit_dir / 'write_plan.csv').name}` — full-library apply sheet; review `safe_to_apply_boolean` and `action_type` before apply",
        ],
    )

    return AuditOutputPaths(
        root=output_dir,
        summary=audit_dir / "summary.md",
        readme=readme_path,
        actions_operator=audit_dir / "actions_operator.csv",
        actions=audit_dir / "actions.csv",
        write_plan=audit_dir / "write_plan.csv",
    )


def build_discovery_outputs(
    candidates: Sequence[Mapping[str, Any]],
    output_dir: Path,
    *,
    bookshelf_result: BookshelfIntegrationResult | None = None,
) -> DiscoveryOutputPaths:
    ensure_dir(output_dir)
    discovery_dir = output_dir / "discovery"
    ensure_dir(discovery_dir)

    write_csv(discovery_dir / "candidates.csv", list(candidates))

    discovery_bucket_counts = Counter(str(row.get("discovery_bucket") or "unknown") for row in candidates)
    shortlist_count = sum(1 for row in candidates if bool(row.get("eligible_for_shortlist_boolean")))
    shortlist_reason_counts = Counter(str(row.get("shortlist_reason") or "unknown") for row in candidates)

    summary_lines = [
        "# Discovery summary",
        "",
        f"- Discovery rows written: **{len(candidates)}**",
        f"- Shortlist-eligible rows: **{shortlist_count}**",
        f"- Manual-review / suppressed rows: **{len(candidates) - shortlist_count}**",
        "",
        "## Discovery buckets",
    ]
    for label, count in sorted(discovery_bucket_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {label}: **{count}**")
    summary_lines.extend(["", "## Shortlist reasons"])
    for label, count in sorted(shortlist_reason_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {label}: **{count}**")
    summary_lines.extend(
        [
            "",
            "## Files",
            "- candidates.csv — unified discovery sheet for missing-series and owned-author candidates",
        ]
    )
    bookshelf_queue_path: Path | None = None
    bookshelf_queue_json_path: Path | None = None
    bookshelf_push_log_path: Path | None = None
    bookshelf_summary_path: Path | None = None

    if bookshelf_result is not None:
        bookshelf_queue_path = discovery_dir / "bookshelf_queue.csv"
        bookshelf_queue_json_path = discovery_dir / "bookshelf_queue.json"
        bookshelf_push_log_path = discovery_dir / "bookshelf_push_log.csv"
        bookshelf_summary_path = discovery_dir / "bookshelf_summary.md"
        write_csv(
            bookshelf_queue_path,
            list(bookshelf_result.queue_rows),
            fieldnames=BOOKSHELF_QUEUE_COLUMNS,
        )
        write_json(bookshelf_queue_json_path, list(bookshelf_result.queue_rows))
        write_csv(
            bookshelf_push_log_path,
            list(bookshelf_result.push_log_rows),
            fieldnames=BOOKSHELF_PUSH_LOG_COLUMNS,
        )
        _write_summary(bookshelf_summary_path, bookshelf_result.summary_lines)
        summary_lines.extend(
            [
                "",
                "## Bookshelf",
                f"- Bookshelf queue rows: **{len(bookshelf_result.queue_rows)}**",
                f"- Bookshelf log rows: **{len(bookshelf_result.push_log_rows)}**",
                f"- Metadata backend: **{bookshelf_result.metadata_backend or 'not_checked'}**",
                "- See `bookshelf_summary.md` and `bookshelf_push_log.csv` for the export/push trace.",
                "",
                "## Bookshelf Files",
                "- bookshelf_queue.csv — opt-in Bookshelf queue derived from eligible discovery rows",
                "- bookshelf_queue.json — JSON form of the Bookshelf queue",
                "- bookshelf_push_log.csv — step-by-step Bookshelf export and push log",
                "- bookshelf_summary.md — Bookshelf export/push summary",
            ]
        )
    _write_summary(discovery_dir / "summary.md", summary_lines)

    readme_lines = [
        f"- Summary: `{(discovery_dir / 'summary.md').name}`",
        f"- Candidates: `{(discovery_dir / 'candidates.csv').name}`",
    ]
    if bookshelf_result is not None:
        readme_lines.extend(
            [
                f"- Bookshelf queue: `{(discovery_dir / 'bookshelf_queue.csv').name}`",
                f"- Bookshelf queue JSON: `{(discovery_dir / 'bookshelf_queue.json').name}`",
                f"- Bookshelf push log: `{(discovery_dir / 'bookshelf_push_log.csv').name}`",
                f"- Bookshelf summary: `{(discovery_dir / 'bookshelf_summary.md').name}`",
            ]
        )
    readme_path = _write_root_readme(output_dir, "Discovery", readme_lines)

    return DiscoveryOutputPaths(
        root=output_dir,
        summary=discovery_dir / "summary.md",
        readme=readme_path,
        candidates=discovery_dir / "candidates.csv",
        bookshelf_queue=bookshelf_queue_path,
        bookshelf_queue_json=bookshelf_queue_json_path,
        bookshelf_push_log=bookshelf_push_log_path,
        bookshelf_summary=bookshelf_summary_path,
    )


def build_apply_outputs(
    apply_log_rows: Sequence[Mapping[str, Any]],
    output_dir: Path,
    *,
    summary: Mapping[str, Any],
) -> ApplyOutputPaths:
    ensure_dir(output_dir)
    apply_dir = output_dir / "apply"
    ensure_dir(apply_dir)

    write_csv(apply_dir / "apply_log.csv", list(apply_log_rows))

    status_counts = Counter(str(row.get("status") or "unknown") for row in apply_log_rows)
    db_status_counts = Counter(str(row.get("db_write_status") or "unknown") for row in apply_log_rows)
    file_status_counts = Counter(str(row.get("file_write_status") or "unknown") for row in apply_log_rows)
    file_target_counts = Counter(str(row.get("file_write_target") or "none") for row in apply_log_rows)
    attempted_action_counts = Counter(
        str(row.get("action_type") or "unknown")
        for row in apply_log_rows
        if str(row.get("status") or "") in {"applied", "would_apply", "no_changes", "rolled_back", "error", "skipped_file_write"}
    )
    summary_lines = [
        "# Apply summary",
        "",
        f"- Input rows loaded: **{int(summary.get('input_rows') or 0)}**",
        f"- Selected rows after filters: **{int(summary.get('selected_rows') or 0)}**",
        f"- Attempted rows: **{int(summary.get('attempted_rows') or 0)}**",
        f"- Transaction outcome: **{summary.get('transaction_status') or 'unknown'}**",
        f"- Dry-run: **{'yes' if bool(summary.get('dry_run')) else 'no'}**",
        f"- Metadata DB: `{summary.get('metadata_db_path') or ''}`",
        f"- Write plan: `{summary.get('write_plan_path') or ''}`",
        "",
        "## Apply mode",
        f"- Safe-only filter: **{'yes' if bool(summary.get('apply_safe_only')) else 'no'}**",
        f"- Identifiers-only: **{'yes' if bool(summary.get('include_identifiers_only')) else 'no'}**",
        f"- Include calibre title/author: **{'yes' if bool(summary.get('include_calibre_title_author')) else 'no'}**",
        f"- metadata.db writes requested: **{'yes' if bool(summary.get('write_db')) else 'no'}**",
        f"- Sidecar OPF writes requested: **{'yes' if bool(summary.get('write_sidecar_opf')) else 'no'}**",
        f"- Internal EPUB OPF writes requested: **{'yes' if bool(summary.get('write_epub_opf')) else 'no'}**",
        f"- File target preference: **{'sidecar_opf' if bool(summary.get('prefer_sidecar_opf', True)) else 'epub_opf'}**",
        f"- Action filter: `{summary.get('apply_actions_display') or 'all supported safe actions'}`",
        f"- Limit: `{summary.get('limit_display') or 'none'}`",
        "",
        "## Safety notes",
        "- `metadata.db` remains the primary source of truth. File writes are opt-in and do not change the default DB-only apply behavior.",
        "- File writes use best-effort backup/restore around each modified target. They are not a single atomic cross-file transaction with `metadata.db`.",
        "- Use `--dry-run` before any live file-write mode. Dry-run resolves file targets and logs would-apply results without persisting DB or file changes.",
        "",
        "## Row statuses",
    ]
    for label, count in sorted(status_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {label}: **{count}**")
    summary_lines.extend(["", "## metadata.db write statuses"])
    for label, count in sorted(db_status_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {label}: **{count}**")
    summary_lines.extend(["", "## File write statuses"])
    for label, count in sorted(file_status_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {label}: **{count}**")
    summary_lines.extend(["", "## File write targets"])
    for label, count in sorted(file_target_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {label}: **{count}**")
    summary_lines.extend(["", "## Attempted actions"])
    for label, count in sorted(attempted_action_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {label}: **{count}**")
    summary_lines.extend(
        [
            "",
            "## Files",
            "- apply_log.csv — per-row apply decisions plus separate metadata.db and file-write outcomes",
        ]
    )
    _write_summary(apply_dir / "summary.md", summary_lines)

    readme_path = _write_root_readme(
        output_dir,
        "Apply",
        [
            f"- Summary: `{(apply_dir / 'summary.md').name}`",
            f"- Apply log: `{(apply_dir / 'apply_log.csv').name}` — includes db_write_status, file_write_target, and file_write_status columns",
        ],
    )

    return ApplyOutputPaths(
        root=output_dir,
        summary=apply_dir / "summary.md",
        readme=readme_path,
        apply_log=apply_dir / "apply_log.csv",
    )
