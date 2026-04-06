from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence

from .legacy_runtime import (
    bucket_sort_key,
    build_compact_audit_actions,
    build_write_plan,
    classify_manual_review_bucket,
    filter_compact_write_plan_rows,
    legacy,
)


def _write_summary(path: Path, lines: Iterable[str]) -> None:
    legacy.ensure_dir(path.parent)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


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


def build_audit_outputs(rows: Sequence[Any], output_dir: Path) -> Dict[str, Path]:
    legacy.ensure_dir(output_dir)
    audit_dir = output_dir / "audit"
    legacy.ensure_dir(audit_dir)

    audit_actions = build_compact_audit_actions(list(rows))
    write_plan = _enrich_write_plan_rows(
        rows,
        filter_compact_write_plan_rows(build_write_plan(list(rows))),
    )

    legacy.write_csv(audit_dir / "actions.csv", audit_actions)
    legacy.write_csv(audit_dir / "write_plan.csv", write_plan)

    action_counts = Counter(getattr(row, "recommended_action", "") for row in rows)
    tier_counts = Counter(getattr(row, "confidence_tier", "") or "unknown" for row in rows)
    action_bucket_counts = Counter(str(row.get("action_bucket") or "unknown") for row in audit_actions)
    review_source_counts = Counter(str(row.get("review_source") or "unknown") for row in audit_actions)
    safe_actions = sum(1 for row in audit_actions if bool(row.get("safe_to_apply_boolean")))
    top_fix = sorted(
        (row for row in rows if getattr(row, "recommended_action", "") != "keep_hardcover_id"),
        key=bucket_sort_key,
    )[:100]

    summary_lines = [
        "# Audit summary",
        "",
        f"- Total books audited: **{len(rows)}**",
        f"- Action rows written: **{len(audit_actions)}**",
        f"- Write-plan rows written: **{len(write_plan)}**",
        f"- Safe-to-apply audit rows: **{safe_actions}**",
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
            "## Files",
            "- actions.csv — master actionable audit sheet",
            "- write_plan.csv — dry-run metadata / identifier write plan",
        ]
    )
    _write_summary(audit_dir / "summary.md", summary_lines)

    readme_lines = [
        "# Output overview",
        "",
        "## Audit",
        f"- Summary: `{(audit_dir / 'summary.md').name}`",
        f"- Actions: `{(audit_dir / 'actions.csv').name}`",
        f"- Write plan: `{(audit_dir / 'write_plan.csv').name}`",
        "",
        "`run.log` remains in the root output directory for the full execution trace.",
    ]
    _write_summary(output_dir / "README.md", readme_lines)

    return {
        "root": output_dir,
        "summary": audit_dir / "summary.md",
        "actions": audit_dir / "actions.csv",
        "write_plan": audit_dir / "write_plan.csv",
        "readme": output_dir / "README.md",
    }


def build_discovery_outputs(candidates: Sequence[Mapping[str, Any]], output_dir: Path) -> Dict[str, Path]:
    legacy.ensure_dir(output_dir)
    discovery_dir = output_dir / "discovery"
    legacy.ensure_dir(discovery_dir)

    legacy.write_csv(discovery_dir / "candidates.csv", list(candidates))

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
    legacy.ensure_dir(output_dir)
    apply_dir = output_dir / "apply"
    legacy.ensure_dir(apply_dir)

    legacy.write_csv(apply_dir / "apply_log.csv", list(apply_log_rows))

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
