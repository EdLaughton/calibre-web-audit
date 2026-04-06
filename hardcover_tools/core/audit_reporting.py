from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Set, Tuple

from .edition_selection import edition_gap_tier, is_edition_write_blocked_row, normalize_edition_format
from .identifiers import extract_numeric_id
from .models import AuditRow, Decision, EditionChoiceInfo, HardcoverBook, HardcoverEdition
from .text_normalization import (
    canonical_author_set,
    norm,
    normalize_author_key,
    normalize_author_string,
    smart_title,
    split_author_like_string,
    strip_series_suffix,
)


def _log_label(text: Any, max_len: int = 60) -> str:
    value = " ".join(str(text or "-").split())
    return value or "-"


def _preview_names(items: List[str], limit: int = 3, max_len: int = 40) -> str:
    cleaned: List[str] = []
    seen: set[str] = set()
    for item in items:
        label = _log_label(item, max_len=max_len)
        key = norm(label)
        if not label or label == "-" or key in seen:
            continue
        seen.add(key)
        cleaned.append(label)
        if len(cleaned) >= limit:
            break
    if not cleaned:
        return "-"
    return "; ".join(cleaned)


def log_label(text: Any, max_len: int = 60) -> str:
    return _log_label(text, max_len=max_len)


def preview_names(items: List[str], limit: int = 3, max_len: int = 40) -> str:
    return _preview_names(items, limit=limit, max_len=max_len)


def _fmt_position_value(value: Any) -> str:
    if value in (None, ""):
        return "?"
    try:
        value_f = float(value)
    except Exception:
        return str(value)
    if value_f.is_integer():
        return str(int(value_f))
    return f"{value_f:g}"


def _series_position_bracket(position: Any, primary_books_count: Any) -> str:
    pos_text = _fmt_position_value(position)
    try:
        primary_count = int(primary_books_count or 0)
    except Exception:
        primary_count = 0
    if primary_count > 0:
        return f"[{pos_text}/{primary_count}]"
    return f"[{pos_text}]"


def compact_edition_marker(
    edition: Optional[HardcoverEdition],
    pick_score: Optional[float] = None,
) -> str:
    if not edition:
        return "-"
    edition_format = normalize_edition_format(edition.edition_format, edition.reading_format) or (
        edition.edition_format or edition.reading_format or "-"
    )
    language = edition.language or "-"
    title = _log_label(edition.title or "-", max_len=48)
    parts = [f'edition="{title}" [{edition.id}]', f"fmt={edition_format}", f"lang={language}", f"users={edition.users_count or 0}"]
    if pick_score is not None:
        parts.append(f"pick={float(pick_score):.1f}")
    return " ".join(parts)


def compact_ranked_editions(
    ranked: List[Tuple[Tuple[Any, ...], float, str, HardcoverEdition]],
    limit: int = 3,
) -> str:
    items: List[str] = []
    for _rank, score, _reason, edition in ranked[:limit]:
        edition_format = normalize_edition_format(edition.edition_format, edition.reading_format) or (
            edition.edition_format or edition.reading_format or "-"
        )
        items.append(
            f'{_log_label(edition.title or "-", max_len=32)} [{edition.id}; {edition_format}; {edition.language or "-"}; {float(score):.1f}]'
        )
    return " | ".join(items) if items else "-"


def compact_ranked_editions_from_choice(
    ranked: List[Tuple[Tuple[Any, ...], float, str, HardcoverEdition]],
    skip: int = 0,
    limit: int = 3,
) -> str:
    if skip < 0:
        skip = 0
    return compact_ranked_editions(ranked[skip:], limit=limit) if ranked[skip:] else "-"


def compact_book_marker(book: Optional[HardcoverBook]) -> str:
    if not book:
        return "-"
    title = _log_label(smart_title(book.title or "-"), max_len=48)
    authors = _log_label(smart_title(book.authors or "-"), max_len=40)
    return f'"{title}" [{book.id}] by {authors}'


def compact_suggest_fields(
    decision: Decision,
    suggested_book: Optional[HardcoverBook],
    suggested_edition: Optional[HardcoverEdition],
) -> str:
    parts: List[str] = []
    if decision.suggested_calibre_title:
        parts.append(f'calibre_title="{_log_label(decision.suggested_calibre_title, max_len=56)}"')
    if decision.suggested_calibre_authors:
        parts.append(f'calibre_authors="{_log_label(decision.suggested_calibre_authors, max_len=48)}"')
    book = suggested_book
    if book and (decision.suggested_hardcover_id or decision.suggested_hardcover_edition_id):
        parts.append(f"hc={compact_book_marker(book)}")
    elif decision.suggested_hardcover_id:
        parts.append(f"hc_id={decision.suggested_hardcover_id}")
    edition = suggested_edition
    if edition and (decision.suggested_hardcover_edition_id or decision.suggested_hardcover_id):
        parts.append(f'edition="{_log_label(edition.title or "-", max_len=48)}" [{edition.id}]')
    elif decision.suggested_hardcover_edition_id:
        parts.append(f"edition_id={decision.suggested_hardcover_edition_id}")
    return " ".join(parts)


def fmt_bool(flag: Optional[bool]) -> str:
    if flag is True:
        return "yes"
    if flag is False:
        return "no"
    return "-"


def compact_missing_series_marker(
    missing: Dict[str, Any],
    primary_books_count: int = 0,
    include_meta: bool = True,
) -> str:
    slot = _series_position_bracket(missing.get("position"), primary_books_count)
    title = _log_label(missing.get("title") or "-")
    book_id = int(missing.get("book_id") or 0)
    parts = [f"{slot} {title} [{book_id}]"] if book_id else [f"{slot} {title}"]
    canonical_id = int(missing.get("canonical_id") or 0)
    canonical_title = _log_label(missing.get("canonical_title") or "-")
    if canonical_id or (canonical_title and canonical_title != "-"):
        parts.append(f'canon="{canonical_title}" [{canonical_id}]' if canonical_id else f'canon="{canonical_title}"')
    if include_meta:
        state = _log_label(missing.get("state") or "-")
        parts.append(f"state={state}")
        parts.append(f"featured={'yes' if bool(missing.get('featured')) else 'no'}")
        details = _log_label(missing.get("details") or "-")
        parts.append(f'details="{details}"')
    return " ".join(parts)


def edition_choice_summary(
    choice: EditionChoiceInfo,
    ranked: List[Tuple[Tuple[Any, ...], float, str, HardcoverEdition]],
) -> Tuple[str, str]:
    preferred_summary = compact_edition_marker(choice.chosen, choice.chosen_score) if choice.chosen else "-"
    alternatives = compact_ranked_editions_from_choice(ranked, skip=1, limit=2)
    return preferred_summary, alternatives


def bucket_sort_key(row: AuditRow) -> Tuple[int, float, int]:
    action_priority = {
        "replace_hardcover_id": 0,
        "set_hardcover_id": 1,
        "update_calibre_metadata": 2,
        "safe_auto_fix": 3,
        "manual_review": 4,
        "manual_review_title_match_author_unconfirmed": 5,
        "suspected_author_mismatch": 6,
        "suspected_file_mismatch": 7,
        "likely_non_english": 8,
        "keep_hardcover_id": 9,
    }
    return (
        action_priority.get(row.recommended_action, 99),
        -float(row.confidence_score or 0.0),
        int(row.calibre_book_id or 0),
    )


def classify_manual_review_bucket(row: AuditRow) -> str:
    if row.recommended_action == "manual_review_title_match_author_unconfirmed":
        return "manual_review_title_match_author_unconfirmed"
    if row.calibre_hardcover_id and row.current_hardcover_match_ok == "":
        return "manual_review_unresolved_current_id"
    if float(row.confidence_score or 0.0) >= 75:
        return "manual_review_strong_candidate"
    if float(row.confidence_score or 0.0) >= 60:
        return "manual_review_plausible_candidate"
    return "manual_review_no_candidate"


def build_hardcover_edition_write_candidates(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    trusted_actions = {
        "keep_hardcover_id",
        "safe_auto_fix",
        "update_calibre_metadata",
        "set_hardcover_id",
        "replace_hardcover_id",
    }
    output: List[Dict[str, Any]] = []

    def _write_guardrail_reason(
        row: AuditRow,
        same_as_current_hardcover: bool,
        current_edition_id: str,
        proposed_edition_id: str,
    ) -> str:
        proposed_format = row.suggested_hardcover_edition_format_normalized or row.preferred_edition_format_normalized
        proposed_language = row.suggested_hardcover_edition_language or row.preferred_edition_language
        if not same_as_current_hardcover:
            return "suggested hardcover-id differs from current hardcover-id"
        if row.recommended_action in {
            "manual_review",
            "manual_review_title_match_author_unconfirmed",
            "suspected_author_mismatch",
            "suspected_file_mismatch",
            "likely_non_english",
        }:
            return f"recommended_action={row.recommended_action}"
        if float(row.confidence_score or 0.0) < 75.0:
            return "confidence below 75"
        if not proposed_edition_id:
            return "no proposed hardcover-edition"
        if norm(str(proposed_format or "")) == "audiobook":
            return "proposed hardcover-edition is audiobook; blocked by default"
        if not str(proposed_language or "").strip():
            return "proposed hardcover-edition has blank language; manual review required"
        if current_edition_id and current_edition_id == proposed_edition_id:
            return "current hardcover-edition already matches suggestion"
        return "ok"

    for row in rows:
        proposed_hardcover_id = extract_numeric_id(row.suggested_hardcover_id) or extract_numeric_id(
            row.calibre_hardcover_id
        )
        current_hardcover_id = extract_numeric_id(row.calibre_hardcover_id)
        current_edition_id = extract_numeric_id(row.current_hardcover_edition_id)
        proposed_edition_id = extract_numeric_id(row.suggested_hardcover_edition_id or row.preferred_edition_id)
        if not proposed_hardcover_id or not proposed_edition_id:
            continue
        same_as_current_hardcover = bool(current_hardcover_id and proposed_hardcover_id == current_hardcover_id)
        same_as_current_edition = bool(current_edition_id and proposed_edition_id == current_edition_id)
        write_guardrail_reason = _write_guardrail_reason(
            row,
            same_as_current_hardcover,
            current_edition_id,
            proposed_edition_id,
        )
        proposed_format = row.suggested_hardcover_edition_format_normalized or row.preferred_edition_format_normalized
        proposed_language = row.suggested_hardcover_edition_language or row.preferred_edition_language
        blocked_edition_write = is_edition_write_blocked_row(
            format_normalized=proposed_format,
            language=proposed_language,
        )
        safe_for_current_id_write_pass = bool(
            same_as_current_hardcover
            and row.recommended_action in trusted_actions
            and float(row.confidence_score or 0.0) >= 75.0
            and (row.current_hardcover_match_ok in {"yes", "", "no"})
            and row.recommended_action
            not in {
                "manual_review",
                "manual_review_title_match_author_unconfirmed",
                "suspected_author_mismatch",
                "suspected_file_mismatch",
                "likely_non_english",
            }
            and not same_as_current_edition
            and not blocked_edition_write
        )
        output.append(
            {
                "calibre_book_id": row.calibre_book_id,
                "calibre_title": row.calibre_title,
                "calibre_authors": row.calibre_authors,
                "file_path": row.file_path,
                "file_format": row.file_format,
                "current_hardcover_id": row.calibre_hardcover_id,
                "current_hardcover_slug": row.calibre_hardcover_slug,
                "current_hardcover_edition_id": row.current_hardcover_edition_id,
                "suggested_hardcover_id": proposed_hardcover_id,
                "suggested_hardcover_slug": row.suggested_hardcover_slug
                or row.hardcover_slug
                or row.calibre_hardcover_slug,
                "suggested_hardcover_edition_id": proposed_edition_id,
                "suggested_hardcover_edition_title": row.suggested_hardcover_edition_title
                or row.preferred_edition_title,
                "suggested_hardcover_reading_format": row.suggested_hardcover_reading_format
                or row.preferred_edition_reading_format,
                "suggested_hardcover_edition_format_raw": row.suggested_hardcover_edition_format_raw
                or row.preferred_edition_edition_format,
                "suggested_hardcover_edition_format_normalized": row.suggested_hardcover_edition_format_normalized
                or row.preferred_edition_format_normalized,
                "suggested_hardcover_edition_is_ebookish": row.suggested_hardcover_edition_is_ebookish
                if row.suggested_hardcover_edition_id
                else row.preferred_edition_is_ebookish,
                "default_ebook_edition_id": row.default_ebook_edition_id,
                "preferred_matches_default_ebook": row.preferred_matches_default_ebook,
                "suggested_hardcover_edition_format": row.suggested_hardcover_edition_format
                or row.preferred_edition_edition_format
                or row.preferred_edition_reading_format,
                "suggested_hardcover_edition_language": row.suggested_hardcover_edition_language
                or row.preferred_edition_language,
                "edition_choice_score": row.edition_choice_score,
                "edition_runner_up_id": row.runner_up_edition_id,
                "edition_runner_up_title": row.runner_up_edition_title,
                "edition_runner_up_reading_format": row.runner_up_edition_reading_format,
                "edition_runner_up_format_raw": row.runner_up_edition_edition_format,
                "edition_runner_up_format_normalized": row.runner_up_edition_format_normalized,
                "edition_runner_up_is_ebookish": row.runner_up_edition_is_ebookish,
                "edition_runner_up_format": row.runner_up_edition_reading_format or row.runner_up_edition_edition_format,
                "edition_runner_up_language": row.runner_up_edition_language,
                "edition_runner_up_score": row.edition_runner_up_score,
                "edition_choice_score_gap": row.edition_choice_score_gap,
                "edition_gap_tier": edition_gap_tier(
                    float(row.edition_choice_score_gap or 0.0),
                    bool(row.runner_up_edition_id),
                ),
                "edition_choice_reason": row.preferred_edition_reason,
                "default_ebook_edition_score": row.default_ebook_edition_score,
                "preferred_vs_default_ebook_score_gap": row.preferred_vs_default_ebook_score_gap,
                "edition_runner_up_reason": row.runner_up_edition_reason,
                "edition_candidates_considered": row.edition_candidates_considered,
                "recommended_action": row.recommended_action,
                "confidence_score": row.confidence_score,
                "confidence_tier": row.confidence_tier,
                "current_hardcover_match_ok": row.current_hardcover_match_ok,
                "edition_matches_current_hardcover_id": same_as_current_hardcover,
                "edition_matches_current_hardcover_edition": same_as_current_edition,
                "current_has_hardcover_edition": bool(current_edition_id),
                "needs_hardcover_edition_write": bool(not same_as_current_edition),
                "safe_for_current_id_write_pass": safe_for_current_id_write_pass,
                "write_guardrail_reason": write_guardrail_reason,
                "reason": row.reason,
                "fix_basis": row.fix_basis,
                "current_hardcover_title": row.current_hardcover_title,
                "current_hardcover_author": row.current_hardcover_authors,
                "suggested_hardcover_title": row.suggested_hardcover_title or row.hardcover_title,
                "suggested_hardcover_author": row.suggested_hardcover_authors or row.hardcover_authors,
                "relink_confidence": row.confidence_tier,
                "relink_reason": row.reason,
            }
        )
    output.sort(
        key=lambda row: (
            not bool(row.get("safe_for_current_id_write_pass")),
            not bool(row.get("edition_matches_current_hardcover_id")),
            -float(row.get("confidence_score") or 0.0),
            int(row.get("calibre_book_id") or 0),
        )
    )
    return output


def build_same_id_edition_write_candidates(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    output = [row for row in build_hardcover_edition_write_candidates(rows) if row.get("edition_matches_current_hardcover_id")]
    output.sort(
        key=lambda row: (
            not bool(row.get("safe_for_current_id_write_pass")),
            bool(row.get("edition_matches_current_hardcover_edition")),
            -float(row.get("edition_choice_score_gap") or 0.0),
            -float(row.get("confidence_score") or 0.0),
            int(row.get("calibre_book_id") or 0),
        )
    )
    return output


def build_edition_manual_review_queue(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    output = [
        row
        for row in build_hardcover_edition_write_candidates(rows)
        if not row.get("safe_for_current_id_write_pass")
        or str(row.get("edition_gap_tier") or "") in {"narrow", "tie_or_negative"}
        or not row.get("edition_matches_current_hardcover_id")
    ]
    output.sort(
        key=lambda row: (
            bool(row.get("safe_for_current_id_write_pass")),
            float(row.get("edition_choice_score_gap") or 0.0),
            -float(row.get("confidence_score") or 0.0),
            int(row.get("calibre_book_id") or 0),
        )
    )
    return output


def build_write_plan(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    trusted_actions = {
        "safe_auto_fix",
        "update_calibre_metadata",
        "set_hardcover_id",
        "replace_hardcover_id",
        "keep_hardcover_id",
    }
    output: List[Dict[str, Any]] = []
    for row in rows:
        proposed_format = row.suggested_hardcover_edition_format_normalized or row.preferred_edition_format_normalized
        proposed_language = row.suggested_hardcover_edition_language or row.preferred_edition_language
        blocked_edition_write = is_edition_write_blocked_row(
            format_normalized=proposed_format,
            language=proposed_language,
        )
        if row.recommended_action in {
            "manual_review",
            "manual_review_title_match_author_unconfirmed",
            "suspected_author_mismatch",
            "suspected_file_mismatch",
            "likely_non_english",
        }:
            safe_reason = f"recommended_action={row.recommended_action}"
        elif float(row.confidence_score or 0.0) < 75.0:
            safe_reason = "confidence below 75"
        elif norm(str(proposed_format or "")) == "audiobook":
            safe_reason = "proposed hardcover-edition is audiobook; blocked by default"
        elif not str(proposed_language or "").strip():
            safe_reason = "proposed hardcover-edition has blank language; manual review required"
        else:
            safe_reason = "ok"
        safe = bool(
            row.recommended_action in trusted_actions
            and float(row.confidence_score or 0.0) >= 75.0
            and row.recommended_action
            not in {
                "manual_review",
                "manual_review_title_match_author_unconfirmed",
                "suspected_author_mismatch",
                "suspected_file_mismatch",
                "likely_non_english",
            }
            and not blocked_edition_write
        )
        output.append(
            {
                "calibre_book_id": row.calibre_book_id,
                "title": row.calibre_title,
                "current_calibre_title": row.calibre_title,
                "new_calibre_title": row.suggested_calibre_title or row.calibre_title,
                "current_calibre_author": row.calibre_authors,
                "new_calibre_author": row.suggested_calibre_authors or row.calibre_authors,
                "current_hardcover_id": row.calibre_hardcover_id,
                "new_hardcover_id": row.suggested_hardcover_id or row.calibre_hardcover_id,
                "current_hardcover_title": row.current_hardcover_title,
                "current_hardcover_author": row.current_hardcover_authors,
                "suggested_hardcover_title": row.suggested_hardcover_title or row.hardcover_title,
                "suggested_hardcover_author": row.suggested_hardcover_authors or row.hardcover_authors,
                "relink_confidence": row.confidence_tier,
                "relink_reason": row.reason,
                "current_hardcover_edition_id": row.current_hardcover_edition_id,
                "new_hardcover_edition_id": row.suggested_hardcover_edition_id or row.preferred_edition_id,
                "suggested_hardcover_reading_format": row.suggested_hardcover_reading_format
                or row.preferred_edition_reading_format,
                "suggested_hardcover_edition_format_raw": row.suggested_hardcover_edition_format_raw
                or row.preferred_edition_edition_format,
                "suggested_hardcover_edition_format_normalized": row.suggested_hardcover_edition_format_normalized
                or row.preferred_edition_format_normalized,
                "suggested_hardcover_edition_is_ebookish": row.suggested_hardcover_edition_is_ebookish
                if row.suggested_hardcover_edition_id
                else row.preferred_edition_is_ebookish,
                "action_type": row.recommended_action,
                "confidence": row.confidence_score,
                "reason": row.reason,
                "safe_to_apply_boolean": safe,
                "safe_to_apply_reason": safe_reason,
            }
        )
    output.sort(
        key=lambda row: (
            not bool(row.get("safe_to_apply_boolean")),
            -float(row.get("confidence") or 0.0),
            int(row.get("calibre_book_id") or 0),
        )
    )
    return output


def _counter_preview(counter: Counter[str], limit: int = 8) -> str:
    parts: List[str] = []
    for value, count in counter.most_common(limit):
        label = _log_label(value, max_len=64)
        if not label or label == "-":
            continue
        parts.append(f"{label} [{count}]")
    return " || ".join(parts) if parts else "-"


def _int_preview(values: Set[int], limit: int = 12) -> str:
    ordered = sorted(int(value) for value in values if value)
    if not ordered:
        return "-"
    return ", ".join(str(value) for value in ordered)


def choose_preferred_display(counter: Counter[str]) -> str:
    if not counter:
        return ""
    return sorted(counter.items(), key=lambda item: (-item[1], len(item[0]), norm(item[0]), item[0]))[0][0]


def build_author_normalisation_review(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    name_clusters: Dict[str, Dict[str, Any]] = {}
    string_clusters: Dict[Tuple[str, ...], Dict[str, Any]] = {}

    def ensure_name_cluster(key: str) -> Dict[str, Any]:
        if key not in name_clusters:
            name_clusters[key] = {
                "calibre_variants": Counter(),
                "reference_variants": Counter(),
                "book_ids": set(),
                "titles": [],
                "full_author_strings": Counter(),
            }
        return name_clusters[key]

    def ensure_string_cluster(key: Tuple[str, ...]) -> Dict[str, Any]:
        if key not in string_clusters:
            string_clusters[key] = {
                "calibre_variants": Counter(),
                "reference_variants": Counter(),
                "book_ids": set(),
                "titles": [],
            }
        return string_clusters[key]

    for row in rows:
        title = smart_title(row.suggested_calibre_title or row.calibre_title or "")
        calibre_full = normalize_author_string(row.calibre_authors)
        reference_full = normalize_author_string(
            row.suggested_calibre_authors
            or row.suggested_hardcover_authors
            or row.current_hardcover_authors
            or row.hardcover_authors
            or ""
        )

        if calibre_full:
            full_key = canonical_author_set(calibre_full)
            if full_key:
                cluster = ensure_string_cluster(full_key)
                cluster["calibre_variants"][calibre_full] += 1
                cluster["book_ids"].add(int(row.calibre_book_id))
                if title:
                    cluster["titles"].append(title)
                if reference_full:
                    cluster["reference_variants"][reference_full] += 1

        for name in split_author_like_string(calibre_full):
            key = normalize_author_key(name)
            if not key:
                continue
            cluster = ensure_name_cluster(key)
            cluster["calibre_variants"][name] += 1
            cluster["book_ids"].add(int(row.calibre_book_id))
            cluster["full_author_strings"][calibre_full] += 1
            if title:
                cluster["titles"].append(title)

        for name in split_author_like_string(reference_full):
            key = normalize_author_key(name)
            if not key:
                continue
            cluster = ensure_name_cluster(key)
            cluster["reference_variants"][name] += 1
            cluster["book_ids"].add(int(row.calibre_book_id))
            if title:
                cluster["titles"].append(title)

    output: List[Dict[str, Any]] = []

    for key, cluster in sorted(name_clusters.items(), key=lambda item: (-len(item[1]["book_ids"]), item[0])):
        calibre_variants: Counter[str] = cluster["calibre_variants"]
        reference_variants: Counter[str] = cluster["reference_variants"]
        if not calibre_variants:
            continue
        suggested_display = choose_preferred_display(reference_variants) or choose_preferred_display(calibre_variants)
        dominant_calibre = choose_preferred_display(calibre_variants)
        include = False
        note_parts: List[str] = []
        if len(calibre_variants) > 1:
            include = True
            note_parts.append("multiple calibre display variants")
        if suggested_display and dominant_calibre and suggested_display != dominant_calibre:
            include = True
            note_parts.append("Hardcover suggests a different display form")
        if not include:
            continue
        output.append(
            {
                "review_type": "individual_author_name",
                "canonical_author_key": key,
                "suggested_display_name": suggested_display or dominant_calibre or "",
                "dominant_calibre_variant": dominant_calibre or "",
                "calibre_variant_count": len(calibre_variants),
                "affected_book_count": len(cluster["book_ids"]),
                "calibre_variants": _counter_preview(calibre_variants),
                "hardcover_reference_variants": _counter_preview(reference_variants),
                "calibre_author_strings": _counter_preview(cluster["full_author_strings"], limit=6),
                "sample_titles": _preview_names(cluster["titles"], limit=5, max_len=48),
                "sample_calibre_book_ids": _int_preview(cluster["book_ids"]),
                "note": "; ".join(note_parts),
            }
        )

    for key, cluster in sorted(
        string_clusters.items(),
        key=lambda item: (-len(item[1]["book_ids"]), "|".join(item[0])),
    ):
        calibre_variants: Counter[str] = cluster["calibre_variants"]
        if len(calibre_variants) <= 1:
            continue
        reference_variants: Counter[str] = cluster["reference_variants"]
        suggested_string = choose_preferred_display(reference_variants) or choose_preferred_display(calibre_variants)
        output.append(
            {
                "review_type": "author_string",
                "canonical_author_set_key": " | ".join(key),
                "suggested_display_name": suggested_string or "",
                "dominant_calibre_variant": choose_preferred_display(calibre_variants) or "",
                "calibre_variant_count": len(calibre_variants),
                "affected_book_count": len(cluster["book_ids"]),
                "calibre_variants": _counter_preview(calibre_variants),
                "hardcover_reference_variants": _counter_preview(reference_variants),
                "sample_titles": _preview_names(cluster["titles"], limit=5, max_len=48),
                "sample_calibre_book_ids": _int_preview(cluster["book_ids"]),
                "note": "same canonical author set appears with multiple calibre display strings",
            }
        )

    output.sort(
        key=lambda row: (
            row.get("review_type") or "",
            -(int(row.get("affected_book_count") or 0)),
            norm(str(row.get("suggested_display_name") or row.get("canonical_author_key") or "")),
        )
    )
    return output


def _duplicate_candidate_work_id(row: AuditRow) -> str:
    current_id = extract_numeric_id(row.calibre_hardcover_id)
    suggested_id = extract_numeric_id(row.suggested_hardcover_id)
    if row.current_hardcover_match_ok == "yes" and current_id:
        return current_id
    trusted_actions = {
        "keep_hardcover_id",
        "safe_auto_fix",
        "set_hardcover_id",
        "replace_hardcover_id",
        "update_calibre_metadata",
    }
    if row.recommended_action in trusted_actions and float(row.confidence_score or 0.0) >= 75 and suggested_id:
        return suggested_id
    if current_id and suggested_id and current_id == suggested_id:
        return current_id
    return ""


def _duplicate_candidate_edition_id(row: AuditRow) -> str:
    current_id = extract_numeric_id(row.current_hardcover_edition_id)
    preferred_id = extract_numeric_id(row.preferred_edition_id)
    suggested_id = extract_numeric_id(row.suggested_hardcover_edition_id)
    if current_id and current_id == preferred_id:
        return current_id
    if suggested_id:
        return suggested_id
    if preferred_id:
        return preferred_id
    return current_id


def _duplicate_title_author_key(row: AuditRow) -> Tuple[str, Tuple[str, ...]]:
    title = (
        row.suggested_calibre_title
        or row.file_work_title
        or row.calibre_title
        or row.hardcover_title
        or row.current_hardcover_title
    )
    authors = (
        row.suggested_calibre_authors
        or row.file_work_authors
        or row.calibre_authors
        or row.suggested_hardcover_authors
        or row.current_hardcover_authors
    )
    return norm(strip_series_suffix(title or "")), canonical_author_set(authors or "")


def build_duplicate_review(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    if not rows:
        return []
    parent = list(range(len(rows)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        root_left, root_right = find(left), find(right)
        if root_left != root_right:
            parent[root_right] = root_left

    work_groups: Dict[str, List[int]] = defaultdict(list)
    edition_groups: Dict[str, List[int]] = defaultdict(list)
    title_author_groups: Dict[Tuple[str, Tuple[str, ...]], List[int]] = defaultdict(list)

    for index, row in enumerate(rows):
        work_id = _duplicate_candidate_work_id(row)
        if work_id:
            work_groups[work_id].append(index)
        edition_id = _duplicate_candidate_edition_id(row)
        if edition_id:
            edition_groups[edition_id].append(index)
        title_key = _duplicate_title_author_key(row)
        if title_key[0] and title_key[1]:
            title_author_groups[title_key].append(index)

    for groups in (work_groups, edition_groups, title_author_groups):
        for members in groups.values():
            if len(members) <= 1:
                continue
            base = members[0]
            for other in members[1:]:
                union(base, other)

    grouped: Dict[int, List[int]] = defaultdict(list)
    for index in range(len(rows)):
        grouped[find(index)].append(index)

    def _confidence_rank(bases: Set[str]) -> Tuple[int, str]:
        if "shared_trusted_hardcover_work" in bases or "shared_hardcover_edition" in bases:
            return 3, "high"
        if "same_normalized_title_author" in bases:
            return 2, "medium"
        return 1, "low"

    output: List[Dict[str, Any]] = []
    group_number = 0
    for _root, members in sorted(
        grouped.items(),
        key=lambda item: (-len(item[1]), min(rows[index].calibre_book_id for index in item[1])),
    ):
        if len(members) <= 1:
            continue
        bases: Set[str] = set()
        shared_work_ids = {
            _duplicate_candidate_work_id(rows[index])
            for index in members
            if _duplicate_candidate_work_id(rows[index])
        }
        shared_edition_ids = {
            _duplicate_candidate_edition_id(rows[index])
            for index in members
            if _duplicate_candidate_edition_id(rows[index])
        }
        title_keys = {
            _duplicate_title_author_key(rows[index])
            for index in members
            if _duplicate_title_author_key(rows[index])[0] and _duplicate_title_author_key(rows[index])[1]
        }
        if len(shared_work_ids) == 1 and len(members) == len(
            [index for index in members if _duplicate_candidate_work_id(rows[index])]
        ):
            bases.add("shared_trusted_hardcover_work")
        elif shared_work_ids:
            bases.add("overlapping_hardcover_work")
        if len(shared_edition_ids) == 1 and len(members) == len(
            [index for index in members if _duplicate_candidate_edition_id(rows[index])]
        ):
            bases.add("shared_hardcover_edition")
        if len(title_keys) == 1:
            bases.add("same_normalized_title_author")

        confidence_rank, confidence_label = _confidence_rank(bases)
        group_number += 1
        group_id = f"D{group_number:04d}"
        group_titles = _preview_names(
            [
                rows[index].suggested_calibre_title
                or rows[index].file_work_title
                or rows[index].calibre_title
                or rows[index].hardcover_title
                for index in members
            ],
            limit=6,
            max_len=48,
        )
        group_calibre_ids = ", ".join(
            str(rows[index].calibre_book_id)
            for index in sorted(members, key=lambda item: rows[item].calibre_book_id)
        )
        group_hardcover_ids = ", ".join(sorted(shared_work_ids)) if shared_work_ids else ""
        group_edition_ids = ", ".join(sorted(shared_edition_ids)) if shared_edition_ids else ""

        canonical_author_set_key = ""
        normalized_title_key = ""
        if len(title_keys) == 1:
            only_key = next(iter(title_keys))
            normalized_title_key = only_key[0]
            canonical_author_set_key = " | ".join(only_key[1])

        for index in sorted(members, key=lambda item: (rows[item].calibre_book_id, norm(rows[item].calibre_title))):
            row = rows[index]
            output.append(
                {
                    "duplicate_group_id": group_id,
                    "duplicate_group_size": len(members),
                    "duplicate_confidence": confidence_label,
                    "duplicate_confidence_rank": confidence_rank,
                    "duplicate_basis": " | ".join(sorted(bases)) if bases else "",
                    "shared_hardcover_work_ids": group_hardcover_ids,
                    "shared_hardcover_edition_ids": group_edition_ids,
                    "normalized_title_key": normalized_title_key,
                    "canonical_author_set_key": canonical_author_set_key,
                    "group_titles": group_titles,
                    "group_calibre_book_ids": group_calibre_ids,
                    "calibre_book_id": row.calibre_book_id,
                    "calibre_title": row.calibre_title,
                    "suggested_calibre_title": row.suggested_calibre_title,
                    "calibre_authors": row.calibre_authors,
                    "suggested_calibre_authors": row.suggested_calibre_authors,
                    "file_work_title": row.file_work_title,
                    "file_work_authors": row.file_work_authors,
                    "calibre_hardcover_id": row.calibre_hardcover_id,
                    "suggested_hardcover_id": row.suggested_hardcover_id,
                    "current_hardcover_edition_id": row.current_hardcover_edition_id,
                    "preferred_edition_id": row.preferred_edition_id,
                    "confidence_score": row.confidence_score,
                    "confidence_tier": row.confidence_tier,
                    "recommended_action": row.recommended_action,
                    "reason": row.reason,
                    "file_path": row.file_path,
                }
            )

    output.sort(
        key=lambda row: (
            -int(row.get("duplicate_confidence_rank") or 0),
            -int(row.get("duplicate_group_size") or 0),
            norm(str(row.get("normalized_title_key") or row.get("calibre_title") or "")),
            int(row.get("calibre_book_id") or 0),
        )
    )
    return output


def _audit_action_bucket(row: AuditRow) -> str:
    if row.recommended_action == "manual_review":
        return classify_manual_review_bucket(row)
    mapping = {
        "safe_auto_fix": "safe_auto_fix",
        "set_hardcover_id": "set_hardcover_id",
        "replace_hardcover_id": "replace_hardcover_id",
        "update_calibre_metadata": "update_calibre_metadata",
        "manual_review_title_match_author_unconfirmed": "manual_review_title_match_author_unconfirmed",
        "suspected_author_mismatch": "suspected_author_mismatch",
        "suspected_file_mismatch": "suspected_file_mismatch",
        "likely_non_english": "likely_non_english",
        "keep_hardcover_id": "verified_current_hardcover_link",
    }
    return mapping.get(row.recommended_action, row.recommended_action or "unknown")


def build_compact_audit_actions(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    write_plan_map = {int(row.get("calibre_book_id") or 0): row for row in build_write_plan(rows)}
    duplicate_review = build_duplicate_review(rows)
    author_normalisation_review = build_author_normalisation_review(rows)
    action_rows: List[Dict[str, Any]] = []

    for row in sorted(rows, key=bucket_sort_key):
        if row.recommended_action == "keep_hardcover_id":
            continue
        payload = asdict(row)
        plan_row = write_plan_map.get(int(row.calibre_book_id or 0), {})
        payload.update(
            {
                "phase": "audit",
                "review_source": "book_audit",
                "action_bucket": _audit_action_bucket(row),
                "safe_to_apply_boolean": plan_row.get("safe_to_apply_boolean", ""),
                "safe_to_apply_reason": plan_row.get("safe_to_apply_reason", ""),
            }
        )
        action_rows.append(payload)

    for row in duplicate_review:
        payload = dict(row)
        payload.update(
            {
                "phase": "audit",
                "review_source": "duplicate_review",
                "action_bucket": "duplicate_review",
                "safe_to_apply_boolean": False,
                "safe_to_apply_reason": "duplicate or near-duplicate review required",
            }
        )
        action_rows.append(payload)

    for row in author_normalisation_review:
        payload = dict(row)
        payload.update(
            {
                "phase": "audit",
                "review_source": "author_normalisation_review",
                "action_bucket": "author_normalisation_review",
                "safe_to_apply_boolean": False,
                "safe_to_apply_reason": "author normalisation review required",
            }
        )
        action_rows.append(payload)

    action_rows.sort(
        key=lambda row: (
            0 if row.get("review_source") == "book_audit" else 1,
            0 if bool(row.get("safe_to_apply_boolean")) else 1,
            -float(row.get("confidence_score") or row.get("confidence") or 0.0),
            int(row.get("calibre_book_id") or 0),
            norm(
                str(
                    row.get("suggested_calibre_title")
                    or row.get("calibre_title")
                    or row.get("title")
                    or row.get("suggested_display_name")
                    or ""
                )
            ),
        )
    )
    return action_rows


def filter_compact_write_plan_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for row in rows:
        if row.get("action_type") != "keep_hardcover_id":
            output.append(row)
            continue
        if row.get("safe_to_apply_boolean"):
            output.append(row)
            continue
        if any(
            [
                str(row.get("current_calibre_title") or "") != str(row.get("new_calibre_title") or ""),
                str(row.get("current_calibre_author") or "") != str(row.get("new_calibre_author") or ""),
                str(row.get("current_hardcover_id") or "") != str(row.get("new_hardcover_id") or ""),
                str(row.get("current_hardcover_edition_id") or "")
                != str(row.get("new_hardcover_edition_id") or ""),
                str(row.get("safe_to_apply_reason") or "") != "ok",
            ]
        ):
            output.append(row)
    return output


__all__ = [
    "bucket_sort_key",
    "build_author_normalisation_review",
    "build_compact_audit_actions",
    "build_duplicate_review",
    "build_hardcover_edition_write_candidates",
    "build_same_id_edition_write_candidates",
    "build_edition_manual_review_queue",
    "build_write_plan",
    "choose_preferred_display",
    "classify_manual_review_bucket",
    "filter_compact_write_plan_rows",
]
