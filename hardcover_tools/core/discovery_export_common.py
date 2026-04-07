from __future__ import annotations

import re
from typing import Any, Mapping, Optional, Sequence

from .models import HardcoverBook, HardcoverEdition


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def discovery_row_is_export_eligible(
    row: Mapping[str, Any],
    *,
    approval_mode: str,
) -> tuple[bool, str]:
    priority_bucket = str(row.get("discovery_priority_bucket") or "")
    if priority_bucket.startswith("suppressed_"):
        return False, "suppressed discovery rows are never exported"
    if approval_mode == "safe-only":
        return priority_bucket == "shortlist", "safe-only keeps only plain shortlist rows"
    if approval_mode == "shortlist-only":
        return to_bool(row.get("eligible_for_shortlist_boolean")), "shortlist-only keeps shortlist-eligible rows"
    return True, "all-approved keeps all non-suppressed discovery rows"


def ordered_export_row(payload: Mapping[str, Any], columns: Sequence[str]) -> dict[str, Any]:
    row = {column: payload.get(column, "") for column in columns}
    for key, value in payload.items():
        if key not in row:
            row[key] = value
    return row


def row_hardcover_slug(row: Mapping[str, Any], book: Optional[HardcoverBook]) -> str:
    for key in ("missing_slug", "slug"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return str(book.slug if book else "")


def find_preferred_edition(
    editions: Sequence[HardcoverEdition],
    *,
    preferred_edition_id: str,
) -> Optional[HardcoverEdition]:
    target = str(preferred_edition_id or "").strip()
    if not target:
        return None
    for edition in editions:
        if str(edition.id) == target:
            return edition
    return None


def extract_release_year(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        match = re.search(r"\b(1[5-9]\d{2}|20\d{2}|21\d{2})\b", text)
        if match:
            return match.group(1)
    return ""


__all__ = [
    "discovery_row_is_export_eligible",
    "extract_release_year",
    "find_preferred_edition",
    "ordered_export_row",
    "row_hardcover_slug",
    "to_bool",
]
