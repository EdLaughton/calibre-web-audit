from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .text_normalization import norm, smart_title

MANUAL_REVIEW_ACTIONS = {
    "likely_non_english",
    "suspected_author_mismatch",
    "suspected_file_mismatch",
}

METADATA_CLEANUP_ACTIONS = {
    "safe_auto_fix",
    "update_calibre_metadata",
}

IDENTIFIER_CHANGE_ACTIONS = {
    "replace_hardcover_id",
    "set_hardcover_id",
}


def row_value(row: Any, field_name: str, default: Any = "") -> Any:
    if isinstance(row, Mapping):
        return row.get(field_name, default)
    return getattr(row, field_name, default)


def is_manual_review_action(action: Any) -> bool:
    action_text = str(action or "").strip()
    return action_text.startswith("manual_review") or action_text in MANUAL_REVIEW_ACTIONS


def build_action_family_counts(rows: Sequence[Any]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        action = str(row_value(row, "recommended_action", "") or "")
        if not action:
            counts["other"] += 1
        elif action == "keep_hardcover_id":
            counts["keep"] += 1
        elif action in METADATA_CLEANUP_ACTIONS:
            counts["metadata"] += 1
        elif action in IDENTIFIER_CHANGE_ACTIONS:
            counts["identifier"] += 1
        elif is_manual_review_action(action):
            counts["review"] += 1
        else:
            counts["other"] += 1
    return counts


def format_action_family_counts(rows: Sequence[Any]) -> str:
    counts = build_action_family_counts(rows)
    ordered = [
        ("keep", counts.get("keep", 0)),
        ("meta", counts.get("metadata", 0)),
        ("id", counts.get("identifier", 0)),
        ("review", counts.get("review", 0)),
        ("other", counts.get("other", 0)),
    ]
    return ", ".join(f"{label}:{count}" for label, count in ordered if count > 0) or "-"


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


def metadata_probe_diagnostic(row: Any) -> Tuple[str, str]:
    file_work_title = smart_title(str(row_value(row, "file_work_title", "") or ""))
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
        reference_title = smart_title(str(row_value(row, field_name, "") or ""))
        normalized_reference = _normalized_title_text(reference_title)
        if not normalized_reference or normalized_reference == normalized_file_work:
            continue
        if _looks_like_truncated_prefix(normalized_file_work, normalized_reference):
            title_basis = str(row_value(row, "file_work_title_basis", "") or "-")
            tool_used = str(row_value(row, "ebook_meta_tool_used", "") or "-")
            return (
                "possible_file_work_title_truncation",
                f'file_work_title="{file_work_title}" vs {field_name}="{reference_title}" source={title_basis} tool={tool_used}',
            )
    return ("", "")


def build_metadata_probe_rollup(
    rows: Sequence[Any],
    *,
    sample_limit: int = 5,
) -> Tuple[Counter[str], Dict[str, List[str]]]:
    counts: Counter[str] = Counter()
    samples: Dict[str, List[str]] = {}
    for row in rows:
        warning, details = metadata_probe_diagnostic(row)
        if not warning:
            continue
        counts[warning] += 1
        samples.setdefault(warning, [])
        if len(samples[warning]) >= sample_limit:
            continue
        samples[warning].append(
            f'calibre_id={row_value(row, "calibre_book_id", "")} | {row_value(row, "calibre_title", "")} | {details}'
        )
    return counts, samples


def build_reason_family_rollups(
    rows: Sequence[Any],
    audit_actions: Optional[Sequence[Mapping[str, Any]]] = None,
) -> List[Tuple[str, int]]:
    audit_actions = list(audit_actions or [])
    duplicate_review_rows = sum(1 for row in audit_actions if str(row.get("review_source") or "") == "duplicate_review")
    author_normalisation_review_rows = sum(
        1 for row in audit_actions if str(row.get("review_source") or "") == "author_normalisation_review"
    )
    blank_language_guardrail_rows = sum(
        1 for row in rows if "preferred_edition_blank_language" in str(row_value(row, "reason", "") or "")
    )
    default_ebook_gap_guardrail_rows = sum(
        1
        for row in rows
        if "preferred_edition_differs_from_hardcover_default_ebook_with_narrow_gap"
        in str(row_value(row, "reason", "") or "")
    )
    relink_block_rows = sum(
        1 for row in rows if str(row_value(row, "reason", "") or "").startswith("relink:block_")
    )
    title_metadata_cleanup_rows = sum(
        1
        for row in rows
        if str(row_value(row, "recommended_action", "") or "") in METADATA_CLEANUP_ACTIONS
        and str(row_value(row, "suggested_calibre_title", "") or "")
        and str(row_value(row, "suggested_calibre_title", "") or "")
        != str(row_value(row, "calibre_title", "") or "")
    )
    author_metadata_cleanup_rows = sum(
        1
        for row in rows
        if str(row_value(row, "recommended_action", "") or "") == "update_calibre_metadata"
        and str(row_value(row, "suggested_calibre_authors", "") or "")
        and str(row_value(row, "suggested_calibre_authors", "") or "")
        != str(row_value(row, "calibre_authors", "") or "")
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


def format_live_alert_counts(rows: Sequence[Any]) -> str:
    reason_rollups = build_reason_family_rollups(rows)
    metadata_probe_counts, _samples = build_metadata_probe_rollup(rows, sample_limit=0)
    counts_by_key = {
        "blank_lang": dict(reason_rollups).get("Blank-language edition guardrails", 0),
        "default_gap": dict(reason_rollups).get("Default-ebook gap guardrails", 0),
        "relink_block": dict(reason_rollups).get("Relink-block rows", 0),
        "probe": sum(metadata_probe_counts.values()),
    }
    return ", ".join(f"{label}:{count}" for label, count in counts_by_key.items() if count > 0) or "-"


def build_progress_line(
    rows: Sequence[Any],
    *,
    current: int,
    total: int,
    elapsed_s: float,
    hardcover_delta_text: str,
) -> str:
    rate = (current / elapsed_s) if elapsed_s > 0 else 0.0
    eta = ((total - current) / rate) if rate > 0 else 0.0
    percent = ((current / total) * 100.0) if total > 0 else 100.0
    parts = [
        f"[PROGRESS] books={current}/{total}",
        f"pct={percent:.0f}%",
        f"elapsed={elapsed_s:.1f}s",
        f"eta={eta:.1f}s",
        f"rate={rate:.2f}/s",
        f"actions={format_action_family_counts(rows)}",
    ]
    alerts = format_live_alert_counts(rows)
    if alerts != "-":
        parts.append(f"alerts={alerts}")
    parts.append(f"hc={hardcover_delta_text}")
    return " ".join(parts)


__all__ = [
    "IDENTIFIER_CHANGE_ACTIONS",
    "MANUAL_REVIEW_ACTIONS",
    "METADATA_CLEANUP_ACTIONS",
    "build_action_family_counts",
    "build_metadata_probe_rollup",
    "build_progress_line",
    "build_reason_family_rollups",
    "format_action_family_counts",
    "format_live_alert_counts",
    "is_manual_review_action",
    "metadata_probe_diagnostic",
    "row_value",
]
