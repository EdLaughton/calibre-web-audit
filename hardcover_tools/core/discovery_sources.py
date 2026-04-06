from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from .audit_reporting import (
    choose_preferred_display,
    compact_book_marker,
    compact_edition_marker,
    compact_missing_series_marker,
    compact_ranked_editions_from_choice,
    log_label,
    preview_names,
)
from .edition_selection import (
    choose_preferred_edition_info,
    edition_gap_tier,
    is_ebookish_edition,
    is_english_language_name,
    normalize_edition_format,
    rank_candidate_editions,
)
from .hardcover_client import HardcoverClient
from .language import DE_STOPWORDS, EN_STOPWORDS, ES_STOPWORDS, FR_STOPWORDS
from .matching import title_marketing_penalty
from .models import AuditRow, BookRecord, EditionChoiceInfo, EmbeddedMeta, FileWork, HardcoverBook, HardcoverEdition
from .text_normalization import clean_title_for_matching, norm, normalize_author_key, primary_author, smart_title, split_author_like_string


def vlog(verbose: bool, message: str) -> None:
    if verbose:
        print(message, flush=True)


def series_scan_trusted_book_id(row: AuditRow) -> Optional[int]:
    trusted_actions = {
        "keep_hardcover_id",
        "safe_auto_fix",
        "set_hardcover_id",
        "replace_hardcover_id",
        "update_calibre_metadata",
    }
    if row.recommended_action not in trusted_actions and row.current_hardcover_match_ok != "yes":
        return None
    candidate = (
        row.suggested_hardcover_id or row.hardcover_candidate_id or row.calibre_hardcover_id
    )
    candidate_id = re.search(r"\b(\d{3,})\b", str(candidate or ""))
    if not candidate_id:
        return None
    if float(row.confidence_score or 0.0) < 75 and row.current_hardcover_match_ok != "yes":
        return None
    return int(candidate_id.group(1))


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


def _fmt_positions(values: List[float]) -> str:
    return ", ".join(_fmt_position_value(value) for value in values)


def _position_to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _position_is_integer(value: Any) -> bool:
    value_f = _position_to_float(value)
    return bool(value_f is not None and float(value_f).is_integer())


def _position_is_fractional(value: Any) -> bool:
    value_f = _position_to_float(value)
    return bool(value_f is not None and not float(value_f).is_integer())


def _series_position_bracket(position: Any, primary_books_count: Any) -> str:
    pos_text = _fmt_position_value(position)
    try:
        primary_count = int(primary_books_count or 0)
    except Exception:
        primary_count = 0
    if primary_count > 0:
        return f"[{pos_text}/{primary_count}]"
    return f"[{pos_text}]"


def _slot_bucket(position: Any, primary_books_count: Any) -> str:
    value_f = _position_to_float(position)
    if value_f is None:
        return "unpositioned"
    if not float(value_f).is_integer():
        return "fractional"
    try:
        primary_count = int(primary_books_count or 0)
    except Exception:
        primary_count = 0
    if primary_count > 0 and 0.0 <= value_f <= float(primary_count):
        return "integer_within_declared_primary_range"
    return "integer_outside_declared_primary_range"


def _position_sort_value(value: Any) -> Tuple[int, float]:
    if value in (None, ""):
        return (1, float("inf"))
    try:
        return (0, float(value))
    except Exception:
        return (1, float("inf"))


def _series_group_key(book: Dict[str, Any]) -> str:
    position = book.get("position")
    if position not in (None, ""):
        try:
            return f"pos:{float(position):g}"
        except Exception:
            return f"pos:{position}"
    clean_title = clean_title_for_matching(book.get("title") or "")
    return f"title:{norm(clean_title)}|author:{normalize_author_key(primary_author(book.get('authors') or ''))}"


def _foreign_stopword_hits(text: str, stopwords: Set[str]) -> int:
    tokens = {token for token in norm(text).split() if token}
    return sum(1 for token in tokens if token in stopwords)


def _text_looks_non_english(*samples: Any) -> bool:
    sample_text = " ".join(str(part or "") for part in samples if str(part or "").strip()).strip()
    if not sample_text:
        return False
    lowered = norm(sample_text)
    if not lowered:
        return False
    accent_hint = bool(re.search(r"[áàâäãåæçéèêëíìîïñóòôöõøœúùûüýÿß]", sample_text, re.I))
    english_hits = _foreign_stopword_hits(lowered, EN_STOPWORDS)
    foreign_hits = max(
        _foreign_stopword_hits(lowered, DE_STOPWORDS),
        _foreign_stopword_hits(lowered, FR_STOPWORDS),
        _foreign_stopword_hits(lowered, ES_STOPWORDS),
    )
    if foreign_hits >= 2 and english_hits == 0:
        return True
    if accent_hint and foreign_hits >= 1 and english_hits == 0:
        return True
    explicit_prefixes = (
        "el ",
        "la ",
        "las ",
        "los ",
        "un ",
        "una ",
        "le ",
        "les ",
        "des ",
        "der ",
        "die ",
        "das ",
    )
    if lowered.startswith(explicit_prefixes) and english_hits == 0:
        return True
    return False


def _series_book_title_looks_non_english(book: Dict[str, Any]) -> bool:
    return _text_looks_non_english(book.get("title") or "", book.get("subtitle") or "", book.get("details") or "")


def _series_book_language_rank(book: Dict[str, Any]) -> int:
    language_candidates = [
        book.get("default_ebook_language") or "",
        book.get("default_physical_language") or "",
        book.get("default_cover_language") or "",
        book.get("default_audio_language") or "",
    ]
    for language in language_candidates:
        if not language:
            continue
        return 2 if is_english_language_name(language) else 0
    if _series_book_title_looks_non_english(book):
        return 0
    return 1


def _series_book_language_bucket(book: Dict[str, Any]) -> str:
    rank = _series_book_language_rank(book)
    if rank >= 2:
        return "english"
    if rank == 1:
        return "unknown"
    return "non_english"


def _series_book_rank(book: Dict[str, Any]) -> Tuple[Any, ...]:
    title = book.get("title") or ""
    cleanish = 1 if title_marketing_penalty(title) == 0 else 0
    language_rank = _series_book_language_rank(book)
    return (
        language_rank,
        cleanish,
        int(book.get("users_read_count") or 0),
        int(book.get("users_count") or 0),
        int(round(float(book.get("rating") or 0.0) * 100)),
        int(book.get("lists_count") or 0),
        str(book.get("release_date") or ""),
        -int(book.get("book_id") or 0),
    )


def _choose_series_group_rep(group: List[Dict[str, Any]], allow_non_english: bool) -> Optional[Dict[str, Any]]:
    if not group:
        return None
    english = [book for book in group if _series_book_language_bucket(book) == "english"]
    if english:
        return max(english, key=_series_book_rank)
    unknown = [book for book in group if _series_book_language_bucket(book) == "unknown"]
    if unknown:
        return max(unknown, key=_series_book_rank)
    non_english = [book for book in group if _series_book_language_bucket(book) == "non_english"]
    if allow_non_english and non_english:
        return max(non_english, key=_series_book_rank)
    return None


def _series_catalog_display_counts(catalog_books: List[Dict[str, Any]], primary_books_count: Any) -> Dict[str, int]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for book in catalog_books:
        grouped[_series_group_key(book)].append(book)
    filtered_reps: List[Dict[str, Any]] = []
    dropped_non_english_only = 0
    for group in grouped.values():
        rep = _choose_series_group_rep(group, allow_non_english=False)
        if rep is None:
            dropped_non_english_only += 1
            continue
        filtered_reps.append(rep)
    fractional_count = sum(1 for rep in filtered_reps if _position_is_fractional(rep.get("position")))
    secondary_count = max(0, len(filtered_reps) - int(primary_books_count or 0) - fractional_count)
    return {
        "raw_books": len(catalog_books),
        "grouped_slots": len(grouped),
        "display_books": len(filtered_reps),
        "primary_books": int(primary_books_count or 0),
        "fractional_books": fractional_count,
        "secondary_books": secondary_count,
        "dropped_non_english_only": dropped_non_english_only,
    }


def _collapse_series_catalog_books(
    catalog_books: List[Dict[str, Any]],
    owned_ids: set[int],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for book in catalog_books:
        grouped[_series_group_key(book)].append(book)
    owned_reps: List[Dict[str, Any]] = []
    missing_reps: List[Dict[str, Any]] = []
    for group in grouped.values():
        owned_group = [book for book in group if int(book.get("book_id") or 0) in owned_ids]
        if owned_group:
            owned_rep = _choose_series_group_rep(owned_group, allow_non_english=True)
            if owned_rep is not None:
                owned_reps.append(owned_rep)
            continue
        missing_rep = _choose_series_group_rep(group, allow_non_english=False)
        if missing_rep is not None:
            missing_reps.append(missing_rep)
    return owned_reps, missing_reps


def _fetch_books_with_progress(
    ids: List[int],
    hardcover_client: HardcoverClient,
    verbose: bool = False,
    label: str = "resolve",
    display_labels: Optional[Dict[int, str]] = None,
) -> Dict[int, HardcoverBook]:
    ids = sorted(set(int(value) for value in ids if value))
    if not ids:
        return {}
    total = len(ids)
    output: Dict[int, HardcoverBook] = {}
    display_labels = display_labels or {}
    id_chunks = [ids[index : index + 25] for index in range(0, len(ids), 25)]
    total_batches = len(id_chunks)
    if verbose and total_batches:
        vlog(verbose, f"  {label}: fetching {total} books in {total_batches} batches")
    for batch_idx, id_chunk in enumerate(id_chunks, start=1):
        before = hardcover_client.stats_snapshot()
        chunk_books = hardcover_client.fetch_books(id_chunk)
        output.update(chunk_books)
        after = hardcover_client.stats_snapshot()
        net_delta = int(after["network_requests"] - before["network_requests"])
        cache_delta = int(after["cache_hits"] - before["cache_hits"])
        if net_delta and cache_delta:
            chunk_cache_status = "mixed"
        elif net_delta:
            chunk_cache_status = "miss"
        elif cache_delta:
            chunk_cache_status = "hit"
        else:
            chunk_cache_status = "none"
        display_items = [
            display_labels.get(book_id)
            or f"{log_label((chunk_books.get(book_id).title if chunk_books.get(book_id) else '-'))} [{book_id}]"
            for book_id in id_chunk
        ]
        sample = preview_names(display_items, limit=min(4, len(id_chunk)), max_len=42)
        vlog(
            verbose,
            f"    [{batch_idx}/{total_batches}] books={sample} matched={len(chunk_books)}/{len(id_chunk)} batch_cache={chunk_cache_status} {hardcover_client.stats_delta_text(before, after)}",
        )
    return output


def build_missing_series_books(
    rows: List[AuditRow],
    hardcover_client: HardcoverClient,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    trusted_by_book_id: Dict[int, List[AuditRow]] = defaultdict(list)
    for row in rows:
        trusted_book_id = series_scan_trusted_book_id(row)
        if trusted_book_id:
            trusted_by_book_id[trusted_book_id].append(row)
    if not trusted_by_book_id:
        return []

    vlog(verbose, f"[MISSING-SERIES] trusted_books={len(trusted_by_book_id)}")
    trusted_book_labels = {
        book_id: f"{(rows_for_book[0].hardcover_title or rows_for_book[0].calibre_title or '-')} [{book_id}]"
        for book_id, rows_for_book in trusted_by_book_id.items()
        if rows_for_book
    }
    phase0 = hardcover_client.stats_snapshot()
    memberships = hardcover_client.fetch_book_series_memberships(
        list(trusted_by_book_id.keys()),
        verbose=verbose,
        progress_label="memberships",
        display_labels=trusted_book_labels,
    )
    series_to_owned: Dict[int, Dict[int, Dict[str, Any]]] = defaultdict(dict)
    for book_id, membership_rows in memberships.items():
        for membership in membership_rows:
            series_id = int(membership.get("series_id") or 0)
            if not series_id:
                continue
            series_to_owned[series_id][book_id] = membership
    if not series_to_owned:
        return []

    vlog(verbose, f"  memberships: books={len(trusted_by_book_id)} series={len(series_to_owned)} {hardcover_client.stats_delta_text(phase0)}")

    phase1 = hardcover_client.stats_snapshot()
    all_missing_book_ids: List[int] = []
    missing_book_display_labels: Dict[int, str] = {}
    series_items = sorted(
        series_to_owned.items(),
        key=lambda item: (norm(next(iter(item[1].values())).get("series_name") or ""), item[0]) if item[1] else ("", item[0]),
    )
    series_catalogs: Dict[int, Dict[str, Any]] = hardcover_client.fetch_series_books(
        [series_id for series_id, _owned_map in series_items],
        verbose=verbose,
        progress_label="series-catalogs",
    ) if series_items else {}
    total_series = len(series_items)
    for processed_series, (series_id, owned_map) in enumerate(series_items, start=1):
        series_name = next(iter(owned_map.values())).get("series_name") if owned_map else ""
        catalog = series_catalogs.get(series_id)
        catalog_books = (catalog or {}).get("books") or []
        owned_catalog_books, missing_books = _collapse_series_catalog_books(catalog_books, set(owned_map.keys())) if catalog_books else ([], [])
        display_counts = _series_catalog_display_counts(catalog_books, (catalog or {}).get("primary_books_count") or 0)
        owned_count = len(owned_catalog_books)
        display_name = log_label(series_name or (catalog or {}).get("series_name") or "-", max_len=48)
        line = (
            f"  [inspect {processed_series}/{total_series}] {display_name} [{series_id}] "
            f"primary={display_counts['primary_books']} fractional={display_counts['fractional_books']} "
            f"secondary={display_counts['secondary_books']} owned={owned_count} "
            f"readers={int((catalog or {}).get('series_users_read_count_total') or 0)}"
        )
        if hardcover_client.debug_hardcover:
            line += (
                f" raw={display_counts['raw_books']} grouped={display_counts['grouped_slots']} "
                f"shown={display_counts['display_books']} dropped_non_english={display_counts['dropped_non_english_only']}"
            )
        vlog(verbose, line)
        if missing_books:
            for missing in missing_books:
                missing_book_id = int(missing.get("book_id") or 0)
                if not missing_book_id:
                    continue
                marker = compact_missing_series_marker(missing, (catalog or {}).get("primary_books_count") or 0, include_meta=True)
                missing_book_display_labels.setdefault(missing_book_id, marker)
                vlog(verbose, f"           missing {marker}")
        for missing in missing_books:
            missing_book_id = int(missing.get("book_id") or 0)
            if missing_book_id:
                all_missing_book_ids.append(missing_book_id)

    missing_book_ids = sorted(set(all_missing_book_ids))
    vlog(verbose, f"  catalogs: resolving {len(missing_book_ids)} missing books")
    missing_book_details = _fetch_books_with_progress(
        missing_book_ids,
        hardcover_client=hardcover_client,
        verbose=verbose,
        label="resolve",
        display_labels=missing_book_display_labels,
    ) if missing_book_ids else {}
    missing_preview = preview_names(
        [
            f"{(missing_book_details.get(book_id).title if missing_book_details.get(book_id) else '-') or '-'} [{book_id}]"
            for book_id in missing_book_ids
        ],
        limit=4,
        max_len=52,
    )
    vlog(verbose, f"  catalogs: fetching edition candidates for {len(missing_book_ids)} missing books")
    missing_book_editions = hardcover_client.fetch_editions_for_books(
        missing_book_ids,
        verbose=verbose,
        progress_label="edition-catalogs",
        display_labels=missing_book_display_labels,
    ) if missing_book_ids else {}
    vlog(
        verbose,
        f"  catalogs: series={len(series_to_owned)} missing_books={len(missing_book_ids)} sample_missing={missing_preview} {hardcover_client.stats_delta_text(phase1)}",
    )

    output: List[Dict[str, Any]] = []
    vlog(verbose, f"  catalogs: building output rows for {len(series_to_owned)} series")

    build_series_items = sorted(
        series_to_owned.items(),
        key=lambda item: (norm(next(iter(item[1].values())).get("series_name") or ""), item[0]) if item[1] else ("", item[0]),
    )
    for build_index, (series_id, owned_map) in enumerate(build_series_items, start=1):
        catalog = series_catalogs.get(series_id)
        if not catalog:
            continue
        catalog_books = catalog.get("books") or []
        if not catalog_books:
            continue
        owned_ids = set(owned_map.keys())
        owned_catalog_books, missing_books = _collapse_series_catalog_books(catalog_books, owned_ids)
        display_name = log_label(
            catalog.get("series_name") or next(iter(owned_map.values())).get("series_name") or "-",
            max_len=48,
        )
        if verbose:
            vlog(True, f"  [build {build_index}/{len(build_series_items)}] {display_name} [{series_id}] owned={len(owned_catalog_books)} missing={len(missing_books)}")
        if not owned_catalog_books or not missing_books:
            continue
        owned_positions = sorted(float(book.get("position")) for book in owned_catalog_books if book.get("position") not in (None, ""))
        first_owned = min(owned_positions) if owned_positions else None
        last_owned = max(owned_positions) if owned_positions else None
        owned_titles = [book.get("title") or "" for book in owned_catalog_books]

        for missing in missing_books:
            missing_book_id = int(missing.get("book_id") or 0)
            missing_position = missing.get("position")
            before_owned = [
                book
                for book in owned_catalog_books
                if book.get("position") not in (None, "")
                and missing_position not in (None, "")
                and float(book.get("position")) < float(missing_position)
            ]
            after_owned = [
                book
                for book in owned_catalog_books
                if book.get("position") not in (None, "")
                and missing_position not in (None, "")
                and float(book.get("position")) > float(missing_position)
            ]
            nearest_before = max(before_owned, key=lambda book: float(book.get("position")), default=None)
            nearest_after = min(after_owned, key=lambda book: float(book.get("position")), default=None)

            if missing_position in (None, ""):
                gap_kind = "unpositioned_missing"
                between_owned = False
            elif first_owned is None or last_owned is None:
                gap_kind = "series_missing_positioned"
                between_owned = False
            elif float(missing_position) < first_owned:
                gap_kind = "before_owned_range"
                between_owned = False
            elif float(missing_position) > last_owned:
                gap_kind = "after_owned_range"
                between_owned = False
            else:
                gap_kind = "internal_gap"
                between_owned = True

            missing_book = missing_book_details.get(missing_book_id)
            if missing_book is None and missing_book_id:
                missing_book = HardcoverBook(
                    id=missing_book_id,
                    title=smart_title(missing.get("title") or ""),
                    subtitle=smart_title(missing.get("subtitle") or ""),
                    authors=missing.get("authors") or "",
                    series=(
                        f"{catalog.get('series_name') or ''} [{missing_position}]"
                        if missing_position not in (None, "")
                        else (catalog.get("series_name") or "")
                    ),
                    release_date=missing.get("release_date") or "",
                    slug=missing.get("slug") or "",
                )
            editions = missing_book_editions.get(missing_book_id, [])
            synthetic_record = BookRecord(
                calibre_book_id=0,
                calibre_title=(missing_book.title if missing_book else smart_title(missing.get("title") or "")),
                calibre_authors=(missing_book.authors if missing_book else (missing.get("authors") or "")),
                calibre_series=catalog.get("series_name") or "",
                calibre_series_index=(float(missing_position) if missing_position not in (None, "") else None),
                calibre_language="eng",
                calibre_hardcover_id=str(missing_book_id) if missing_book_id else "",
                calibre_hardcover_slug=(missing_book.slug if missing_book else (missing.get("slug") or "")),
                file_format="EPUB",
            )
            synthetic_file_work = FileWork(
                title=synthetic_record.calibre_title,
                authors=synthetic_record.calibre_authors,
                language="English",
                title_basis="series_catalog",
                authors_basis="series_catalog",
            )
            preferred_choice = choose_preferred_edition_info(
                synthetic_record,
                synthetic_file_work,
                EmbeddedMeta(),
                missing_book,
                editions,
            ) if missing_book else EditionChoiceInfo()
            preferred_edition = preferred_choice.chosen
            runner_up_edition = preferred_choice.runner_up
            preferred_authors = (
                preferred_edition.authors if preferred_edition and preferred_edition.authors else (missing_book.authors if missing_book else "")
            )
            ranked_missing_editions = rank_candidate_editions(
                synthetic_record,
                synthetic_file_work,
                EmbeddedMeta(),
                missing_book,
                editions,
            ) if missing_book else []

            if verbose and missing_book:
                meta_marker = compact_missing_series_marker(missing, catalog.get("primary_books_count") or 0, include_meta=True)
                vlog(True, f"    missing {meta_marker} hc={compact_book_marker(missing_book)} editions={len(editions)} gap_kind={gap_kind}")
                if preferred_edition:
                    vlog(True, f"      preferred={compact_edition_marker(preferred_edition, preferred_choice.chosen_score)} gap={preferred_choice.score_gap:.1f} {edition_gap_tier(preferred_choice.score_gap, bool(preferred_choice.runner_up))}")
                alt_editions = compact_ranked_editions_from_choice(ranked_missing_editions, skip=1, limit=2)
                if alt_editions != "-":
                    vlog(True, f"      alternatives={alt_editions}")

            missing_canonical_id = int(missing.get("canonical_id") or 0)
            slot_bucket = _slot_bucket(missing_position, catalog.get("primary_books_count") or 0)
            output.append(
                {
                    "series_id": series_id,
                    "series_name": catalog.get("series_name") or "",
                    "series_slug": catalog.get("series_slug") or "",
                    "series_is_completed": catalog.get("is_completed"),
                    "series_books_count": int(catalog.get("books_count") or 0),
                    "series_primary_books_count": int(catalog.get("primary_books_count") or 0),
                    "series_users_count_total": int(catalog.get("series_users_count_total") or 0),
                    "series_users_read_count_total": int(catalog.get("series_users_read_count_total") or 0),
                    "series_lists_count_total": int(catalog.get("series_lists_count_total") or 0),
                    "series_top_book_users_read_count": int(catalog.get("series_top_book_users_read_count") or 0),
                    "owned_count_in_series": len(owned_catalog_books),
                    "missing_count_in_series": len(missing_books),
                    "owned_positions": _fmt_positions(owned_positions),
                    "owned_titles": " | ".join(owned_titles),
                    "owned_hardcover_book_ids": ", ".join(
                        str(int(book.get("book_id")))
                        for book in sorted(owned_catalog_books, key=lambda value: _position_sort_value(value.get("position")))
                    ),
                    "owned_calibre_book_ids": ", ".join(
                        str(row.calibre_book_id)
                        for book_id in sorted(owned_ids)
                        for row in trusted_by_book_id.get(book_id, [])
                    ),
                    "missing_hardcover_book_id": missing_book_id,
                    "missing_position": missing_position,
                    "missing_position_display": _fmt_position_value(missing_position),
                    "missing_position_is_fractional": _position_is_fractional(missing_position),
                    "missing_position_is_integer": _position_is_integer(missing_position),
                    "missing_position_within_declared_primary_range": slot_bucket == "integer_within_declared_primary_range",
                    "missing_slot_bucket": slot_bucket,
                    "missing_slot_label": _series_position_bracket(missing_position, catalog.get("primary_books_count") or 0),
                    "missing_details": str(missing.get("details") or ""),
                    "missing_featured": bool(missing.get("featured")),
                    "missing_canonical_id": missing_canonical_id,
                    "missing_has_canonical_parent": bool(missing_canonical_id),
                    "missing_canonical_title": str(missing.get("canonical_title") or ""),
                    "missing_canonical_slug": str(missing.get("canonical_slug") or ""),
                    "missing_state": str(missing.get("state") or ""),
                    "missing_title": (missing_book.title if missing_book else (missing.get("title") or "")),
                    "missing_authors": (missing_book.authors if missing_book else (missing.get("authors") or "")),
                    "missing_release_date": (missing_book.release_date if missing_book else (missing.get("release_date") or "")),
                    "missing_slug": (missing_book.slug if missing_book else (missing.get("slug") or "")),
                    "missing_preferred_edition_id": str(preferred_edition.id) if preferred_edition else "",
                    "missing_preferred_edition_title": preferred_edition.title if preferred_edition else "",
                    "missing_preferred_edition_authors": preferred_authors,
                    "missing_preferred_edition_reading_format": preferred_edition.reading_format if preferred_edition else "",
                    "missing_preferred_edition_format": (preferred_edition.edition_format or preferred_edition.reading_format) if preferred_edition else "",
                    "missing_preferred_edition_format_normalized": normalize_edition_format(preferred_edition.edition_format, preferred_edition.reading_format) if preferred_edition else "",
                    "missing_preferred_edition_is_ebookish": bool(is_ebookish_edition(preferred_edition)) if preferred_edition else False,
                    "missing_preferred_edition_language": preferred_edition.language if preferred_edition else "",
                    "missing_preferred_edition_reason": preferred_choice.chosen_reason if preferred_edition else "",
                    "missing_preferred_edition_score": round(preferred_choice.chosen_score, 3) if preferred_edition else 0.0,
                    "missing_preferred_edition_score_gap": round(preferred_choice.score_gap, 3) if preferred_edition else 0.0,
                    "missing_editions_count": len(editions),
                    "missing_preferred_edition_candidates_considered": preferred_choice.count_considered,
                    "missing_runner_up_edition_id": str(runner_up_edition.id) if runner_up_edition else "",
                    "missing_runner_up_edition_title": runner_up_edition.title if runner_up_edition else "",
                    "missing_runner_up_edition_language": runner_up_edition.language if runner_up_edition else "",
                    "missing_runner_up_edition_reason": preferred_choice.runner_up_reason if runner_up_edition else "",
                    "gap_kind": gap_kind,
                    "between_owned_range": between_owned,
                    "first_owned_position": first_owned,
                    "last_owned_position": last_owned,
                    "nearest_owned_before_position": nearest_before.get("position") if nearest_before else "",
                    "nearest_owned_before_title": nearest_before.get("title") if nearest_before else "",
                    "nearest_owned_after_position": nearest_after.get("position") if nearest_after else "",
                    "nearest_owned_after_title": nearest_after.get("title") if nearest_after else "",
                }
            )

    output.sort(key=lambda row: (norm(str(row.get("series_name") or "")), _position_sort_value(row.get("missing_position")), norm(str(row.get("missing_title") or ""))))
    hardcover_client.save_cache()
    if verbose:
        phase_total = hardcover_client.stats_snapshot()
        series_count = len({int(row.get("series_id") or 0) for row in output if row.get("series_id")})
        mainline_count = sum(1 for row in output if not _position_is_fractional(row.get("missing_position")))
        fractional_count = len(output) - mainline_count
        preview = preview_names(
            [
                f"{row.get('series_name') or '-'} -> {row.get('missing_title') or '-'} [{int(row.get('missing_hardcover_book_id') or 0)}]"
                for row in output
            ],
            limit=4,
            max_len=60,
        )
        vlog(
            True,
            f"  result: rows={len(output)} series={series_count} mainline={mainline_count} fractional={fractional_count} sample={preview} {hardcover_client.stats_delta_text(phase0, phase_total)}",
        )
    return output


def _discovery_series_entry_sort_key(book: Dict[str, Any]) -> Tuple[Any, ...]:
    pos = book.get("position")
    if pos in (None, ""):
        position_rank = 2
    else:
        try:
            position_rank = 0 if float(pos).is_integer() else 1
        except Exception:
            position_rank = 2
    return (
        position_rank,
        _position_sort_value(pos),
        -int(book.get("users_read_count") or 0),
        -int(book.get("users_count") or 0),
        norm(str(book.get("title") or "")),
    )


def _choose_discovery_preferred_edition_info(book: HardcoverBook, editions: List[HardcoverEdition]) -> EditionChoiceInfo:
    synthetic_record = BookRecord(
        calibre_book_id=0,
        calibre_title=book.title or "",
        calibre_authors=book.authors or "",
        calibre_series="",
        calibre_series_index=None,
        calibre_language="English",
        calibre_hardcover_id=str(book.id),
        calibre_hardcover_slug=book.slug or "",
        file_format="EPUB",
    )
    synthetic_file_work = FileWork(
        title=synthetic_record.calibre_title,
        authors=synthetic_record.calibre_authors,
        language="English",
        title_basis="discovery_catalog",
        authors_basis="discovery_catalog",
    )
    return choose_preferred_edition_info(synthetic_record, synthetic_file_work, EmbeddedMeta(), book, editions)


def build_owned_author_discovery(
    rows: List[AuditRow],
    hardcover_client: HardcoverClient,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    trusted_by_book_id: Dict[int, List[AuditRow]] = defaultdict(list)
    owned_author_clusters: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        trusted_book_id = series_scan_trusted_book_id(row)
        if not trusted_book_id:
            continue
        trusted_by_book_id[trusted_book_id].append(row)
        author_text = row.suggested_hardcover_authors or row.current_hardcover_authors or row.hardcover_authors or row.calibre_authors
        for author_name in split_author_like_string(author_text):
            key = normalize_author_key(author_name)
            if not key:
                continue
            cluster = owned_author_clusters.setdefault(
                key,
                {
                    "display_names": Counter(),
                    "owned_hardcover_book_ids": set(),
                    "owned_calibre_book_ids": set(),
                    "owned_titles": [],
                },
            )
            cluster["display_names"][author_name] += 1
            cluster["owned_hardcover_book_ids"].add(trusted_book_id)
            cluster["owned_calibre_book_ids"].add(int(row.calibre_book_id))
            if row.hardcover_title or row.calibre_title:
                cluster["owned_titles"].append(row.hardcover_title or row.calibre_title)
    if not trusted_by_book_id or not owned_author_clusters:
        return []

    owned_book_ids = set(trusted_by_book_id.keys())
    author_names = [
        choose_preferred_display(cluster["display_names"]) or next(iter(cluster["display_names"].keys()))
        for _key, cluster in sorted(owned_author_clusters.items())
        if cluster["display_names"]
    ]
    author_preview = preview_names(author_names, limit=4, max_len=28)
    vlog(verbose, f"[OWNED-AUTHOR-DISCOVERY] trusted_books={len(trusted_by_book_id)} authors={len(author_names)} sample_authors={author_preview}")

    owned_book_labels = {
        book_id: f"{(rows_for_book[0].hardcover_title or rows_for_book[0].calibre_title or '-')} [{book_id}]"
        for book_id, rows_for_book in trusted_by_book_id.items()
        if rows_for_book
    }
    phase0 = hardcover_client.stats_snapshot()
    memberships = hardcover_client.fetch_book_series_memberships(
        list(owned_book_ids),
        verbose=verbose,
        progress_label="owned-series",
        display_labels=owned_book_labels,
    )
    owned_series_ids: Set[int] = set()
    for membership_rows in memberships.values():
        for membership in membership_rows:
            series_id = int(membership.get("series_id") or 0)
            if series_id:
                owned_series_ids.add(series_id)
    vlog(verbose, f"  owned-series books={len(owned_book_ids)} series={len(owned_series_ids)} {hardcover_client.stats_delta_text(phase0)}")

    phase1 = hardcover_client.stats_snapshot()
    author_catalogs = hardcover_client.fetch_books_for_authors(author_names, verbose=verbose, progress_label="author-catalogs")
    author_catalog_book_count = sum(len((catalog or {}).get("books") or []) for catalog in author_catalogs.values())
    vlog(verbose, f"  author-catalogs authors={len(author_names)} matched={len(author_catalogs)} books={author_catalog_book_count} {hardcover_client.stats_delta_text(phase1)}")

    series_to_author_keys: Dict[int, Set[str]] = defaultdict(set)
    standalone_to_author_keys: Dict[int, Set[str]] = defaultdict(set)
    standalone_book_labels: Dict[int, str] = {}
    for author_key, catalog in author_catalogs.items():
        if author_key not in owned_author_clusters:
            continue
        for book in catalog.get("books") or []:
            book_id = int(book.get("book_id") or 0)
            if not book_id or book_id in owned_book_ids:
                continue
            memberships = book.get("series_memberships") or []
            if memberships:
                for membership in memberships:
                    series_id = int(membership.get("series_id") or 0)
                    if series_id and series_id not in owned_series_ids:
                        series_to_author_keys[series_id].add(author_key)
            else:
                standalone_to_author_keys[book_id].add(author_key)
                title = log_label(book.get("title") or "-", max_len=48)
                standalone_book_labels.setdefault(book_id, f"{title} [{book_id}]" if title and title != "-" else str(book_id))

    phase2 = hardcover_client.stats_snapshot()
    series_catalogs = hardcover_client.fetch_series_books(
        list(series_to_author_keys.keys()),
        verbose=verbose,
        progress_label="series-discovery",
    ) if series_to_author_keys else {}
    candidate_series_rows: List[Dict[str, Any]] = []
    candidate_book_ids: Set[int] = set(standalone_to_author_keys.keys())
    for series_id, author_keys in sorted(series_to_author_keys.items(), key=lambda item: item[0]):
        catalog = series_catalogs.get(series_id)
        if not catalog:
            continue
        catalog_books = [book for book in (catalog.get("books") or []) if int(book.get("book_id") or 0) not in owned_book_ids]
        if not catalog_books:
            continue
        starter = sorted(catalog_books, key=_discovery_series_entry_sort_key)[0]
        starter_id = int(starter.get("book_id") or 0)
        if not starter_id:
            continue
        candidate_book_ids.add(starter_id)
        candidate_series_rows.append(
            {
                "series_id": series_id,
                "series_name": catalog.get("series_name") or "",
                "series_slug": catalog.get("series_slug") or "",
                "series_is_completed": catalog.get("is_completed"),
                "series_books_count": int(catalog.get("books_count") or 0),
                "series_primary_books_count": int(catalog.get("primary_books_count") or 0),
                "starter_book_id": starter_id,
                "starter_position": starter.get("position"),
                "starter_title": starter.get("title") or "",
                "starter_release_date": starter.get("release_date") or "",
                "starter_slug": starter.get("slug") or "",
                "starter_users_count": int(starter.get("users_count") or 0),
                "starter_users_read_count": int(starter.get("users_read_count") or 0),
                "starter_rating": float(starter.get("rating") or 0.0),
                "starter_lists_count": int(starter.get("lists_count") or 0),
                "owned_author_keys": sorted(author_keys),
            }
        )
    vlog(verbose, f"  series-discovery series={len(candidate_series_rows)} {hardcover_client.stats_delta_text(phase2)}")

    phase3 = hardcover_client.stats_snapshot()
    candidate_book_labels: Dict[int, str] = {}
    for row in candidate_series_rows:
        starter_id = int(row.get("starter_book_id") or 0)
        if starter_id:
            candidate_book_labels[starter_id] = f"{log_label(row.get('starter_title') or '-', max_len=48)} [{starter_id}]"
    for book_id in standalone_to_author_keys.keys():
        candidate_book_labels.setdefault(book_id, standalone_book_labels.get(book_id) or str(book_id))
    if candidate_book_ids:
        candidate_books, candidate_editions = hardcover_client.fetch_books_and_editions_for_books(
            sorted(candidate_book_ids),
            verbose=verbose,
            progress_label="candidate-catalogs",
            display_labels=candidate_book_labels,
        )
    else:
        candidate_books, candidate_editions = {}, {}
    vlog(
        verbose,
        f"  candidate-catalogs books={len(candidate_books)} editions={sum(len(value) for value in candidate_editions.values())} {hardcover_client.stats_delta_text(phase3)}",
    )

    output: List[Dict[str, Any]] = []
    used_series_book_ids: Set[int] = set()
    for row in candidate_series_rows:
        starter_id = int(row.get("starter_book_id") or 0)
        book = candidate_books.get(starter_id)
        preferred_choice = (
            _choose_discovery_preferred_edition_info(book, candidate_editions.get(starter_id) or [])
            if book
            else EditionChoiceInfo()
        )
        preferred_edition = preferred_choice.chosen
        if not book or not preferred_edition:
            continue
        used_series_book_ids.add(starter_id)
        author_keys = row.get("owned_author_keys") or []
        author_names_text = " | ".join(
            choose_preferred_display(owned_author_clusters[key]["display_names"])
            for key in author_keys
            if key in owned_author_clusters and owned_author_clusters[key]["display_names"]
        )
        owned_titles: List[str] = []
        owned_calibre_ids: List[str] = []
        for key in author_keys:
            cluster = owned_author_clusters.get(key)
            if not cluster:
                continue
            owned_titles.extend(cluster["owned_titles"])
            owned_calibre_ids.extend(str(value) for value in sorted(cluster["owned_calibre_book_ids"]))
        output.append(
            {
                "discovery_type": "unowned_series",
                "owned_author_keys": " | ".join(author_keys),
                "owned_author_names": author_names_text,
                "owned_calibre_book_ids": ", ".join(sorted(set(owned_calibre_ids), key=lambda value: int(value))),
                "owned_title_samples": preview_names(owned_titles, limit=5, max_len=40),
                "hardcover_book_id": starter_id,
                "title": book.title or row.get("starter_title") or "",
                "authors": book.authors or "",
                "slug": book.slug or row.get("starter_slug") or "",
                "release_date": book.release_date or row.get("starter_release_date") or "",
                "users_count": int(book.users_count or row.get("starter_users_count") or 0),
                "users_read_count": int(book.users_read_count or row.get("starter_users_read_count") or 0),
                "rating": float(book.rating or row.get("starter_rating") or 0.0),
                "lists_count": int(book.lists_count or row.get("starter_lists_count") or 0),
                "series_id": row.get("series_id") or 0,
                "series_name": row.get("series_name") or "",
                "series_slug": row.get("series_slug") or "",
                "series_is_completed": row.get("series_is_completed"),
                "series_books_count": int(row.get("series_books_count") or 0),
                "series_primary_books_count": int(row.get("series_primary_books_count") or 0),
                "series_start_position": row.get("starter_position"),
                "preferred_edition_id": str(preferred_edition.id),
                "preferred_edition_title": preferred_edition.title or "",
                "preferred_edition_format_normalized": normalize_edition_format(preferred_edition.edition_format, preferred_edition.reading_format),
                "preferred_edition_language": preferred_edition.language or "",
                "preferred_edition_users_count": int(preferred_edition.users_count or 0),
                "preferred_edition_users_read_count": int(preferred_edition.users_read_count or 0),
                "preferred_edition_candidates_considered": preferred_choice.count_considered,
                "reason": "Series by an owned author that is not yet started in Calibre",
            }
        )

    for book_id, author_keys in sorted(standalone_to_author_keys.items(), key=lambda item: item[0]):
        if book_id in used_series_book_ids:
            continue
        book = candidate_books.get(book_id)
        preferred_choice = (
            _choose_discovery_preferred_edition_info(book, candidate_editions.get(book_id) or [])
            if book
            else EditionChoiceInfo()
        )
        preferred_edition = preferred_choice.chosen
        if not book or not preferred_edition:
            continue
        owned_titles: List[str] = []
        owned_calibre_ids: List[str] = []
        author_names: List[str] = []
        for key in sorted(author_keys):
            cluster = owned_author_clusters.get(key)
            if not cluster:
                continue
            display_name = choose_preferred_display(cluster["display_names"])
            if display_name:
                author_names.append(display_name)
            owned_titles.extend(cluster["owned_titles"])
            owned_calibre_ids.extend(str(value) for value in sorted(cluster["owned_calibre_book_ids"]))
        output.append(
            {
                "discovery_type": "unowned_standalone",
                "owned_author_keys": " | ".join(sorted(author_keys)),
                "owned_author_names": " | ".join(author_names),
                "owned_calibre_book_ids": ", ".join(sorted(set(owned_calibre_ids), key=lambda value: int(value))),
                "owned_title_samples": preview_names(owned_titles, limit=5, max_len=40),
                "hardcover_book_id": book_id,
                "title": book.title or "",
                "authors": book.authors or "",
                "slug": book.slug or "",
                "release_date": book.release_date or "",
                "users_count": int(book.users_count or 0),
                "users_read_count": int(book.users_read_count or 0),
                "rating": float(book.rating or 0.0),
                "lists_count": int(book.lists_count or 0),
                "series_id": 0,
                "series_name": "",
                "series_slug": "",
                "series_is_completed": "",
                "series_books_count": 0,
                "series_primary_books_count": 0,
                "series_start_position": "",
                "preferred_edition_id": str(preferred_edition.id),
                "preferred_edition_title": preferred_edition.title or "",
                "preferred_edition_format_normalized": normalize_edition_format(preferred_edition.edition_format, preferred_edition.reading_format),
                "preferred_edition_language": preferred_edition.language or "",
                "preferred_edition_users_count": int(preferred_edition.users_count or 0),
                "preferred_edition_users_read_count": int(preferred_edition.users_read_count or 0),
                "preferred_edition_candidates_considered": preferred_choice.count_considered,
                "reason": "Standalone book by an owned author that is not yet in Calibre",
            }
        )

    output.sort(
        key=lambda row: (
            0 if row.get("discovery_type") == "unowned_series" else 1,
            norm(str(row.get("owned_author_names") or "")),
            -int(row.get("users_read_count") or 0),
            norm(str(row.get("series_name") or row.get("title") or "")),
            norm(str(row.get("title") or "")),
        )
    )
    if verbose:
        series_count = sum(1 for row in output if row.get("discovery_type") == "unowned_series")
        standalone_count = sum(1 for row in output if row.get("discovery_type") == "unowned_standalone")
        preview = preview_names(
            [f"{row.get('owned_author_names') or '-'} -> {row.get('series_name') or row.get('title') or '-'}" for row in output],
            limit=4,
            max_len=56,
        )
        vlog(True, f"  result rows={len(output)} series={series_count} standalones={standalone_count} sample={preview} {hardcover_client.stats_delta_text(phase0)}")
    hardcover_client.save_cache()
    return output
