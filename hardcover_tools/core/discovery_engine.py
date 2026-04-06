from __future__ import annotations

import os
import re
import sys
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

from .audit_pipeline import audit_books
from .bookshelf_export import run_bookshelf_integration
from .calibre_db import load_calibre_books
from .config import DiscoveryCliConfig
from .discovery_sources import build_missing_series_books, build_owned_author_discovery
from .ebook_meta import EbookMetaRunner
from .edition_selection import is_english_language_name
from .hardcover_client import HardcoverClient
from .language import DE_STOPWORDS, EN_STOPWORDS, ES_STOPWORDS, FR_STOPWORDS, looks_englishish_text
from .output import build_discovery_outputs
from .runtime_io import TeeStream
from .runtime_support import resolve_runtime_paths, validate_hardcover_token
from . import text_normalization
from .text_normalization import canonical_author_set, load_author_alias_map, norm

DISCOVERY_SIDE_MATERIAL_HINTS = (
    " companion ",
    " companions ",
    " guide ",
    " handbook ",
    " journal ",
    " atlas ",
    " encyclopedia ",
    " lexicon ",
    " treasury ",
    " omnibus ",
    " collection ",
    " collections ",
    " box set ",
    " short story ",
    " short stories ",
    " novella ",
    " novellas ",
    " tales ",
    " stories ",
    " files ",
    " world of ",
    " history of ",
    " untold history ",
)

DISCOVERY_METADATA_JUNK_PATTERNS = (
    r"\barticles?\s+(?:about|on)\b",
    r"\bbooks?\s+by\b",
    r"\bnovels?\s+by\b",
    r"\bplays?\s+by\b",
    r"\bessays?\s+by\b",
    r"\bshort stories by\b",
    r"\bworks?\s+by\b",
    r"\bworks inspired by\b",
    r"\bbook guide\b",
    r"\bstudy guide\b",
    r"\blist of\b",
    r"\bcharacters?\b",
    r"\bincluding\b",
)

DISCOVERY_COLLECTIONISH_PATTERNS = (
    r"\banthology\b",
    r"\bomnibus\b",
    r"\bcollection\b",
    r"\bbox(?:ed)?\s+set\b",
    r"\bcomplete\s+collection\b",
    r"\b\d+\s*book\s+collection\b",
)

DISCOVERY_GRAPHICISH_PATTERNS = (
    r"\bgraphic novel\b",
    r"\bmanga\b",
)

DISCOVERY_METADATA_JUNK_AUTHOR_HINTS = (
    "books llc",
    "hephaestus books",
    "source wikipedia",
)

DISCOVERY_FILENAME_EXTENSIONS = (
    "azw",
    "azw3",
    "cbz",
    "cbr",
    "docx",
    "epub",
    "kepub",
    "m4b",
    "mobi",
    "mp3",
    "pdf",
    "rar",
    "txt",
    "zip",
)

DISCOVERY_PRIORITY_RANKS = {
    "shortlist": 0,
    "shortlist_blank_language_likely_english": 1,
    "manual_review": 2,
    "low_priority_blank_language_series_core": 3,
    "low_priority_blank_language_review": 4,
    "low_priority_blank_language_cold_singleton": 5,
    "low_priority_blank_language_side_material": 6,
    "low_priority_unpositioned": 7,
    "low_priority_side_material": 8,
    "suppressed_translated_sibling": 9,
    "suppressed_blank_language_metadata_junk": 10,
    "suppressed_blank_language_weak_signal": 11,
    "suppressed_zero_editions": 12,
    "suppressed_non_english": 13,
    "suppressed_collectionish": 14,
    "suppressed_graphicish": 15,
    "suppressed_filename_like": 16,
    "suppressed_untitled": 17,
    "suppressed_audio": 18,
}


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "yes", "y"}


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _preferred_language(row: Mapping[str, Any]) -> str:
    return str(row.get("preferred_edition_language") or "").strip()


def _row_display_title(row: Mapping[str, Any]) -> str:
    return str(row.get("display_title") or row.get("missing_title") or row.get("title") or "").strip()


def _row_display_subtitle(row: Mapping[str, Any]) -> str:
    return str(row.get("display_subtitle") or row.get("missing_subtitle") or row.get("subtitle") or "").strip()


def _row_series_name(row: Mapping[str, Any]) -> str:
    return str(row.get("display_series") or row.get("series_name") or "").strip()


def _row_details(row: Mapping[str, Any]) -> str:
    return str(row.get("missing_details") or row.get("details") or "").strip()


def _foreign_stopword_hits(text: str, stopwords: Iterable[str]) -> int:
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


def discovery_title_language_bucket(row: Mapping[str, Any]) -> str:
    if _text_looks_non_english(_row_display_title(row), _row_display_subtitle(row), _row_details(row)):
        return "non_english"
    return "unknown"


def discovery_row_has_clear_english_signal(row: Mapping[str, Any]) -> bool:
    language = _preferred_language(row)
    if is_english_language_name(language):
        return True
    if language and not is_english_language_name(language):
        return False
    if _to_int(row.get("edition_candidates_considered")) <= 0:
        return False
    return str(row.get("title_language_bucket") or "") != "non_english"


def discovery_row_has_english_series_sibling(
    row: Mapping[str, Any],
    series_groups: Mapping[int, Sequence[Mapping[str, Any]]],
) -> bool:
    series_id = _to_int(row.get("series_id"))
    if not series_id:
        return False
    group = list(series_groups.get(series_id) or [])
    if len(group) <= 1:
        return False
    current_authors = canonical_author_set(str(row.get("display_authors") or row.get("authors") or ""))
    for other in group:
        if other is row:
            continue
        other_authors = canonical_author_set(str(other.get("display_authors") or other.get("authors") or ""))
        if current_authors and other_authors and not set(current_authors).intersection(other_authors):
            continue
        if discovery_row_has_clear_english_signal(other):
            return True
    return False


def discovery_row_looks_like_side_material(row: Mapping[str, Any]) -> bool:
    gap_kind = str(row.get("gap_kind") or row.get("reason") or "")
    if gap_kind == "unpositioned_missing":
        return True
    text = f" {norm(_row_display_title(row))} {norm(_row_details(row))} "
    return any(token in text for token in DISCOVERY_SIDE_MATERIAL_HINTS)


def discovery_row_is_untitled(row: Mapping[str, Any]) -> bool:
    return norm(_row_display_title(row)) == "untitled"


def discovery_row_looks_filename_like(row: Mapping[str, Any]) -> bool:
    title = _row_display_title(row)
    if not title:
        return False
    extension_pattern = "|".join(re.escape(ext) for ext in DISCOVERY_FILENAME_EXTENSIONS)
    return bool(re.search(rf"\.(?:{extension_pattern})\b", title, re.I))


def discovery_row_looks_collectionish(row: Mapping[str, Any]) -> bool:
    text = " ".join(part for part in [_row_display_title(row), _row_display_subtitle(row)] if part)
    if not text:
        return False
    return any(re.search(pattern, text, re.I) for pattern in DISCOVERY_COLLECTIONISH_PATTERNS)


def discovery_row_looks_graphicish(row: Mapping[str, Any]) -> bool:
    text = " ".join(part for part in [_row_display_title(row), _row_display_subtitle(row)] if part)
    if not text:
        return False
    return any(re.search(pattern, text, re.I) for pattern in DISCOVERY_GRAPHICISH_PATTERNS)


def discovery_row_has_metadata_junk_signals(row: Mapping[str, Any]) -> bool:
    title = _row_display_title(row)
    details = _row_details(row)
    combined = " ".join(part for part in [title, details] if part).strip()
    if not combined:
        return False
    normalized = norm(combined)
    author_text = " ".join(
        part
        for part in [
            str(row.get("authors") or "").strip(),
            str(row.get("display_authors") or "").strip(),
            str(row.get("owned_author_names") or "").strip(),
        ]
        if part
    )
    normalized_authors = norm(author_text)
    if "|" in combined:
        return True
    if ":" in combined and combined.count(",") >= 4:
        return True
    if any(hint in normalized_authors for hint in DISCOVERY_METADATA_JUNK_AUTHOR_HINTS):
        return True
    return any(re.search(pattern, normalized, re.I) for pattern in DISCOVERY_METADATA_JUNK_PATTERNS)


def discovery_row_has_weak_english_signal(row: Mapping[str, Any]) -> bool:
    if str(row.get("title_language_bucket") or "") == "non_english":
        return True
    title_samples = [
        _row_display_title(row),
        str(row.get("preferred_edition_title") or "").strip(),
        _row_display_subtitle(row),
    ]
    sample = " ".join(part for part in title_samples if part).strip()
    if not sample:
        return False
    alpha_chars = [char for char in sample if char.isalpha()]
    if alpha_chars:
        ascii_ratio = sum(1 for char in alpha_chars if char.isascii()) / len(alpha_chars)
        if ascii_ratio < 0.95:
            return True
    if re.search(r"[áàâäãåæçéèêëíìîïñóòôöõøœúùûüýÿßčďěłńřśšťůžăâîșț]", sample, re.I):
        return True
    return not looks_englishish_text(sample)


def classify_discovery_candidate(
    row: Mapping[str, Any],
    series_groups: Mapping[int, Sequence[Mapping[str, Any]]],
) -> Tuple[bool, str, str]:
    format_normalized = norm(str(row.get("preferred_edition_format_normalized") or ""))
    language = _preferred_language(row)
    title_language_bucket = str(row.get("title_language_bucket") or "unknown")
    editions_count = _to_int(row.get("edition_candidates_considered"))
    gap_kind = str(row.get("gap_kind") or row.get("reason") or "")
    users_count = _to_int(row.get("users_count"))
    users_read_count = _to_int(row.get("users_read_count"))
    discovery_bucket = str(row.get("discovery_bucket") or "")
    looks_like_side_material = _to_bool(row.get("looks_like_side_material"))
    has_english_series_sibling = _to_bool(row.get("has_english_series_sibling"))
    is_untitled = _to_bool(row.get("is_untitled"))
    looks_filename_like = _to_bool(row.get("looks_filename_like"))
    looks_collectionish = _to_bool(row.get("looks_collectionish"))
    looks_graphicish = _to_bool(row.get("looks_graphicish"))
    has_metadata_junk_signals = _to_bool(row.get("has_metadata_junk_signals"))
    has_weak_english_signal = _to_bool(row.get("has_weak_english_signal"))

    if is_untitled:
        return False, "title is Untitled; suppressed by default", "suppressed_untitled"
    if looks_filename_like:
        return False, "title looks like a filename/import artefact", "suppressed_filename_like"
    if format_normalized == "audiobook":
        return False, "preferred edition is audiobook; blocked by default", "suppressed_audio"
    if has_english_series_sibling and (
        title_language_bucket == "non_english"
        or not language
        or editions_count <= 0
        or (language and not is_english_language_name(language))
    ):
        return False, "suppressed as translated duplicate of English series sibling", "suppressed_translated_sibling"
    if looks_collectionish:
        return False, "anthology/omnibus/collection discovery suppressed by default", "suppressed_collectionish"
    if looks_graphicish:
        return False, "graphic/comics/manga discovery suppressed by default", "suppressed_graphicish"
    if editions_count <= 0:
        return False, "no usable editions on Hardcover", "suppressed_zero_editions"
    if not language:
        if has_metadata_junk_signals:
            return False, "preferred edition has blank language and metadata-junk signals; suppressed", "suppressed_blank_language_metadata_junk"
        if looks_like_side_material:
            return False, "preferred edition has blank language and looks like side material; low priority", "low_priority_blank_language_side_material"
        if discovery_bucket != "unowned_standalone":
            return False, "preferred edition has blank language but looks like a core series entry; review", "low_priority_blank_language_series_core"
        if format_normalized == "read" and editions_count == 1 and users_count <= 2 and users_read_count <= 2:
            return False, "preferred edition has blank language and is a cold single-edition read stub; low priority", "low_priority_blank_language_cold_singleton"
        if has_weak_english_signal:
            return False, "preferred edition has blank language and weak English signal; suppressed", "suppressed_blank_language_weak_signal"
        if format_normalized == "ebook" or users_count >= 10 or users_read_count >= 5:
            return True, "preferred edition has blank language but looks likely English; promoted shortlist", "shortlist_blank_language_likely_english"
        return False, "preferred edition has blank language but looks plausibly English; low-priority review", "low_priority_blank_language_review"
    if not is_english_language_name(language):
        return False, f"preferred edition is non-English ({language})", "suppressed_non_english"
    if gap_kind == "unpositioned_missing":
        return False, "unpositioned series entry; low priority", "low_priority_unpositioned"
    if looks_like_side_material:
        return False, "translation-like or companion side material; low priority", "low_priority_side_material"
    return True, "ok", "shortlist"


def annotate_discovery_candidates(candidates: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    annotated: List[Dict[str, Any]] = [dict(row) for row in candidates]
    for row in annotated:
        row["title_language_bucket"] = discovery_title_language_bucket(row)
        row["is_untitled"] = discovery_row_is_untitled(row)
        row["looks_filename_like"] = discovery_row_looks_filename_like(row)
        row["looks_collectionish"] = discovery_row_looks_collectionish(row)
        row["looks_graphicish"] = discovery_row_looks_graphicish(row)
        row["looks_like_side_material"] = discovery_row_looks_like_side_material(row)
        row["has_metadata_junk_signals"] = discovery_row_has_metadata_junk_signals(row)
        row["has_weak_english_signal"] = discovery_row_has_weak_english_signal(row)

    series_groups: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in annotated:
        series_id = _to_int(row.get("series_id"))
        if series_id:
            series_groups[series_id].append(row)

    for row in annotated:
        row["has_english_series_sibling"] = discovery_row_has_english_series_sibling(row, series_groups)
        eligible, shortlist_reason, priority_bucket = classify_discovery_candidate(row, series_groups)
        row["eligible_for_shortlist_boolean"] = eligible
        row["shortlist_reason"] = shortlist_reason
        row["discovery_priority_bucket"] = priority_bucket

    annotated.sort(
        key=lambda row: (
            0 if _to_bool(row.get("eligible_for_shortlist_boolean")) else 1,
            DISCOVERY_PRIORITY_RANKS.get(str(row.get("discovery_priority_bucket") or "manual_review"), 99),
            0 if row.get("discovery_bucket") == "missing_series" else 1,
            -_to_int(row.get("users_read_count")),
            -_to_int(row.get("users_count")),
            norm(str(row.get("display_series") or row.get("series_name") or "")),
            norm(str(row.get("display_title") or row.get("title") or "")),
        )
    )
    return annotated


def build_discovery_candidates(
    missing_series_books: Sequence[Mapping[str, Any]],
    owned_author_discovery: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    for row in missing_series_books:
        payload = dict(row)
        payload.update(
            {
                "phase": "discovery",
                "discovery_bucket": "missing_series",
                "display_title": row.get("missing_title") or "",
                "display_authors": row.get("missing_authors") or "",
                "display_series": row.get("series_name") or "",
                "display_book_id": row.get("missing_hardcover_book_id") or "",
                "preferred_edition_id": row.get("missing_preferred_edition_id") or "",
                "preferred_edition_title": row.get("missing_preferred_edition_title") or "",
                "preferred_edition_format_normalized": row.get("missing_preferred_edition_format_normalized") or "",
                "preferred_edition_language": row.get("missing_preferred_edition_language") or "",
                "users_read_count": row.get("missing_preferred_edition_users_read_count") or row.get("users_read_count") or 0,
                "users_count": row.get("missing_preferred_edition_users_count") or row.get("users_count") or 0,
                "reason": row.get("gap_kind") or row.get("reason") or "",
                "gap_kind": row.get("gap_kind") or "",
                "edition_candidates_considered": row.get("missing_editions_count")
                or row.get("missing_preferred_edition_candidates_considered")
                or 0,
            }
        )
        candidates.append(payload)

    for row in owned_author_discovery:
        payload = dict(row)
        payload.update(
            {
                "phase": "discovery",
                "discovery_bucket": str(row.get("discovery_type") or "owned_author_discovery"),
                "display_title": row.get("title") or "",
                "display_authors": row.get("authors") or "",
                "display_series": row.get("series_name") or "",
                "display_book_id": row.get("hardcover_book_id") or "",
                "gap_kind": row.get("gap_kind") or "",
                "edition_candidates_considered": row.get("preferred_edition_candidates_considered")
                or (1 if row.get("preferred_edition_id") else 0),
            }
        )
        candidates.append(payload)

    return annotate_discovery_candidates(candidates)


def run_discovery(config: DiscoveryCliConfig) -> int:
    try:
        token = validate_hardcover_token(os.environ.get("HARDCOVER_TOKEN", ""))
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    text_normalization.AUTHOR_ALIAS_MAP = load_author_alias_map(config.author_aliases_json)
    runtime_paths = resolve_runtime_paths(
        library_root=config.library_root,
        metadata_db=config.metadata_db,
        output_dir=config.output_dir,
        cache_path=config.cache_path,
    )

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    with runtime_paths.log_path.open("w", encoding="utf-8") as log_handle:
        sys.stdout = TeeStream(original_stdout, log_handle)
        sys.stderr = TeeStream(original_stderr, log_handle)
        try:
            print(f"Using library root: {runtime_paths.library_root}")
            print(f"Using metadata DB: {runtime_paths.metadata_db}")
            print(f"Writing outputs to: {runtime_paths.output_dir}")
            print(f"Using cache DB: {runtime_paths.cache_path}")
            if runtime_paths.legacy_cache_json_path.exists():
                print(f"Legacy JSON cache detected: {runtime_paths.legacy_cache_json_path}")
            print(f"Writing log to: {runtime_paths.log_path}")

            records = load_calibre_books(runtime_paths.metadata_db, runtime_paths.library_root)
            print(f"Loaded {len(records)} calibre records")
            if config.verbose:
                state = "on" if config.debug_hardcover else "off"
                print(
                    "Verbose discovery logging enabled "
                    f"(progress every {config.progress_every} books; low-level Hardcover debug={state})"
                )

            hc = HardcoverClient(
                token=token,
                cache_path=runtime_paths.cache_path,
                timeout=config.hardcover_timeout,
                retries=config.hardcover_retries,
                user_agent=config.hardcover_user_agent,
                min_interval=config.hardcover_min_interval,
                verbose=config.verbose,
                cache_ttl_hours=config.cache_ttl_hours,
                search_cache_ttl_hours=config.search_cache_ttl_hours,
                empty_cache_ttl_hours=config.empty_cache_ttl_hours,
                edition_cache_ttl_hours=config.edition_cache_ttl_hours,
                legacy_cache_json_path=runtime_paths.legacy_cache_json_path,
                debug_hardcover=config.debug_hardcover,
            )
            ebook_meta_runner = EbookMetaRunner(
                library_root=runtime_paths.library_root,
                ebook_meta_command=config.ebook_meta_command,
                docker_container_name=config.docker_ebook_meta_container,
                container_library_root=config.container_library_root,
                host_timeout=config.ebook_meta_host_timeout,
                docker_timeout=config.ebook_meta_docker_timeout,
            )

            print("Starting audit prerequisite pass...")
            rows = audit_books(
                records,
                hardcover_client=hc,
                ebook_meta_runner=ebook_meta_runner,
                limit=config.limit,
                verbose=config.verbose,
                progress_every=config.progress_every,
            )
            print("Starting missing-series pass...")
            missing_series_books = build_missing_series_books(rows, hardcover_client=hc, verbose=config.verbose)
            print("Starting owned-author discovery pass...")
            owned_author_discovery = build_owned_author_discovery(rows, hardcover_client=hc, verbose=config.verbose)
            discovery_candidates = build_discovery_candidates(missing_series_books, owned_author_discovery)
            bookshelf_result = None
            if config.export_bookshelf or config.push_bookshelf:
                bookshelf_result = run_bookshelf_integration(
                    discovery_candidates,
                    hardcover_client=hc,
                    export_bookshelf=config.export_bookshelf,
                    push_bookshelf=config.push_bookshelf,
                    dry_run=config.dry_run,
                    approval_mode=config.bookshelf_approval,
                    requested_mode=config.bookshelf_mode,
                    bookshelf_url=config.bookshelf_url,
                    bookshelf_api_key=config.bookshelf_api_key,
                    bookshelf_root_folder=config.bookshelf_root_folder,
                    bookshelf_quality_profile_id=config.bookshelf_quality_profile_id,
                    bookshelf_metadata_profile_id=config.bookshelf_metadata_profile_id,
                    bookshelf_trigger_search=config.bookshelf_trigger_search,
                    verbose=config.verbose,
                )
            output_paths = build_discovery_outputs(
                discovery_candidates,
                runtime_paths.output_dir,
                bookshelf_result=bookshelf_result,
            )

            shortlist_count = sum(1 for row in discovery_candidates if _to_bool(row.get("eligible_for_shortlist_boolean")))
            print(f"Discovery rows written: {len(discovery_candidates)}")
            print(f"Shortlist-eligible discovery rows: {shortlist_count}")
            print(f"Manual-review / suppressed discovery rows: {len(discovery_candidates) - shortlist_count}")
            if bookshelf_result is not None:
                print(f"Bookshelf queue rows written: {len(bookshelf_result.queue_rows)}")
                if config.push_bookshelf:
                    print(
                        "Bookshelf metadata backend: "
                        f"{bookshelf_result.metadata_backend or 'not_checked'}"
                    )
            hc.print_stats_summary()
            print("Done.")
            print(f"Discovery summary: {output_paths['summary']}")
            print(f"Discovery candidates: {output_paths['candidates']}")
            if bookshelf_result is not None:
                print(f"Bookshelf queue: {output_paths['bookshelf_queue']}")
                print(f"Bookshelf push log: {output_paths['bookshelf_push_log']}")
            return 0
        finally:
            try:
                if "hc" in locals() and hc is not None:
                    hc.close()
            except Exception:
                pass
            sys.stdout = original_stdout
            sys.stderr = original_stderr
