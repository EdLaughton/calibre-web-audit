from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

from .audit_reporting import (
    bucket_sort_key,
    build_compact_audit_actions,
    build_write_plan,
    classify_manual_review_bucket,
    filter_compact_write_plan_rows,
)
from .runtime_io import ensure_dir, write_csv
from .text_normalization import norm, smart_title

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


def _write_summary(path: Path, lines: Iterable[str]) -> None:
    ensure_dir(path.parent)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _row_value(row: Any, field_name: str, default: Any = "") -> Any:
    if isinstance(row, Mapping):
        return row.get(field_name, default)
    return getattr(row, field_name, default)


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


def _normalized_title_text(value: Any) -> str:
    return norm(smart_title(str(value or "")))


def _looks_like_truncated_prefix(candidate: str, reference: str) -> bool:
    if not candidate or not reference or candidate == reference:
        return False
    if len(candidate) < 5 or len(reference) < 6:
        return False
    if len(reference) - len(candidate) not in {1, 2}:
        return False
    return reference.startswith(candidate)


def _metadata_probe_diagnostic(row: Any) -> Tuple[str, str]:
    file_work_title = smart_title(str(_row_value(row, "file_work_title", "") or ""))
    if not file_work_title:
        return ("", "")
    normalized_file_work = _normalized_title_text(file_work_title)
    if not normalized_file_work:
        return ("", "")

    reference_fields = [
        "calibre_title",
        "suggested_calibre_title",
        "current_hardcover_title",
        "suggested_hardcover_title",
        "hardcover_title",
    ]
    for field_name in reference_fields:
        reference_title = smart_title(str(_row_value(row, field_name, "") or ""))
        normalized_reference = _normalized_title_text(reference_title)
        if not normalized_reference or normalized_reference == normalized_file_work:
            continue
        if _looks_like_truncated_prefix(normalized_file_work, normalized_reference):
            title_basis = str(_row_value(row, "file_work_title_basis", "") or "-")
            tool_used = str(_row_value(row, "ebook_meta_tool_used", "") or "-")
            return (
                "possible_file_work_title_truncation",
                f'file_work_title="{file_work_title}" vs {field_name}="{reference_title}" source={title_basis} tool={tool_used}',
            )
    return ("", "")


def _build_actions_operator_rows(audit_actions: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for row in audit_actions:
        metadata_probe_warning, metadata_probe_details = _metadata_probe_diagnostic(row)
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


def _build_reason_family_rollups(
    rows: Sequence[Any],
    audit_actions: Sequence[Mapping[str, Any]],
) -> List[Tuple[str, int]]:
    duplicate_review_rows = sum(1 for row in audit_actions if str(row.get("review_source") or "") == "duplicate_review")
    author_normalisation_review_rows = sum(
        1 for row in audit_actions if str(row.get("review_source") or "") == "author_normalisation_review"
    )
    blank_language_guardrail_rows = sum(
        1 for row in rows if "preferred_edition_blank_language" in str(_row_value(row, "reason", "") or "")
    )
    default_ebook_gap_guardrail_rows = sum(
        1
        for row in rows
        if "preferred_edition_differs_from_hardcover_default_ebook_with_narrow_gap"
        in str(_row_value(row, "reason", "") or "")
    )
    relink_block_rows = sum(
        1 for row in rows if str(_row_value(row, "reason", "") or "").startswith("relink:block_")
    )
    title_metadata_cleanup_rows = sum(
        1
        for row in rows
        if str(_row_value(row, "recommended_action", "") or "") in {"safe_auto_fix", "update_calibre_metadata"}
        and str(_row_value(row, "suggested_calibre_title", "") or "")
        and str(_row_value(row, "suggested_calibre_title", "") or "")
        != str(_row_value(row, "calibre_title", "") or "")
    )
    author_metadata_cleanup_rows = sum(
        1
        for row in rows
        if str(_row_value(row, "recommended_action", "") or "") == "update_calibre_metadata"
        and str(_row_value(row, "suggested_calibre_authors", "") or "")
        and str(_row_value(row, "suggested_calibre_authors", "") or "")
        != str(_row_value(row, "calibre_authors", "") or "")
    )
    return [
        ("Blank-language edition guardrails", blank_language_guardrail_rows),
        ("Default-ebook gap guardrails", default_ebook_gap_guardrail_rows),
        ("Relink-block rows", relink_block_rows),
        ("Duplicate-review rows", duplicate_review_rows),
        ("Author-normalisation review rows", author_normalisation_review_rows),
        ("Title metadata cleanup rows", title_metadata_cleanup_rows),
        ("Author metadata cleanup rows", author_metadata_cleanup_rows),
    ]


def _build_metadata_probe_rollup(
    rows: Sequence[Any],
) -> Tuple[Counter[str], Dict[str, List[str]]]:
    counts: Counter[str] = Counter()
    samples: Dict[str, List[str]] = {}
    for row in rows:
        warning, details = _metadata_probe_diagnostic(row)
        if not warning:
            continue
        counts[warning] += 1
        samples.setdefault(warning, [])
        if len(samples[warning]) >= 5:
            continue
        samples[warning].append(
            f'calibre_id={_row_value(row, "calibre_book_id", "")} | {_row_value(row, "calibre_title", "")} | {details}'
        )
    return counts, samples


def build_audit_outputs(rows: Sequence[Any], output_dir: Path) -> Dict[str, Path]:
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
    reason_family_rollups = _build_reason_family_rollups(rows, audit_actions)
    metadata_probe_counts, metadata_probe_samples = _build_metadata_probe_rollup(rows)
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

    readme_lines = [
        "# Output overview",
        "",
        "## Audit",
        f"- Summary: `{(audit_dir / 'summary.md').name}`",
        f"- Operator review sheet: `{(audit_dir / 'actions_operator.csv').name}` — compact triage-first layout derived from the action rows",
        f"- Actions: `{(audit_dir / 'actions.csv').name}` — prioritized non-keep audit rows and review rows",
        f"- Write plan: `{(audit_dir / 'write_plan.csv').name}` — full-library apply sheet; review `safe_to_apply_boolean` and `action_type` before apply",
        "",
        "`run.log` remains in the root output directory for the full execution trace.",
    ]
    _write_summary(output_dir / "README.md", readme_lines)

    return {
        "root": output_dir,
        "summary": audit_dir / "summary.md",
        "actions_operator": audit_dir / "actions_operator.csv",
        "actions": audit_dir / "actions.csv",
        "write_plan": audit_dir / "write_plan.csv",
        "readme": output_dir / "README.md",
    }


def build_discovery_outputs(candidates: Sequence[Mapping[str, Any]], output_dir: Path) -> Dict[str, Path]:
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
    _write_summary(discovery_dir / "summary.md", summary_lines)

    readme_lines = [
        "# Output overview",
        "",
        "## Discovery",
        f"- Summary: `{(discovery_dir / 'summary.md').name}`",
        f"- Candidates: `{(discovery_dir / 'candidates.csv').name}`",
        "",
        "`run.log` remains in the root output directory for the full execution trace.",
    ]
    _write_summary(output_dir / "README.md", readme_lines)

    return {
        "root": output_dir,
        "summary": discovery_dir / "summary.md",
        "candidates": discovery_dir / "candidates.csv",
        "readme": output_dir / "README.md",
    }


def build_apply_outputs(
    apply_log_rows: Sequence[Mapping[str, Any]],
    output_dir: Path,
    *,
    summary: Mapping[str, Any],
) -> Dict[str, Path]:
    ensure_dir(output_dir)
    apply_dir = output_dir / "apply"
    ensure_dir(apply_dir)

    write_csv(apply_dir / "apply_log.csv", list(apply_log_rows))

    status_counts = Counter(str(row.get("status") or "unknown") for row in apply_log_rows)
    attempted_action_counts = Counter(
        str(row.get("action_type") or "unknown")
        for row in apply_log_rows
        if str(row.get("status") or "") in {"applied", "would_apply", "no_changes", "rolled_back", "error"}
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
        f"- Action filter: `{summary.get('apply_actions_display') or 'all supported safe actions'}`",
        f"- Limit: `{summary.get('limit_display') or 'none'}`",
        "",
        "## Row statuses",
    ]
    for label, count in sorted(status_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {label}: **{count}**")
    summary_lines.extend(["", "## Attempted actions"])
    for label, count in sorted(attempted_action_counts.items(), key=lambda item: (-item[1], item[0])):
        summary_lines.append(f"- {label}: **{count}**")
    summary_lines.extend(
        [
            "",
            "## Files",
            "- apply_log.csv — per-row apply decisions and mutation results",
        ]
    )
    _write_summary(apply_dir / "summary.md", summary_lines)

    readme_lines = [
        "# Output overview",
        "",
        "## Apply",
        f"- Summary: `{(apply_dir / 'summary.md').name}`",
        f"- Apply log: `{(apply_dir / 'apply_log.csv').name}`",
        "",
        "`run.log` remains in the root output directory for the full execution trace.",
    ]
    _write_summary(output_dir / "README.md", readme_lines)

    return {
        "root": output_dir,
        "summary": apply_dir / "summary.md",
        "apply_log": apply_dir / "apply_log.csv",
        "readme": output_dir / "README.md",
    }
