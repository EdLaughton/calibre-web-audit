from __future__ import annotations

import html
import re
import time
from pathlib import Path
from typing import List, Optional, Tuple

from .audit_insights import build_progress_line, metadata_probe_diagnostic
from .audit_reporting import (
    compact_book_marker,
    compact_edition_marker,
    compact_ranked_editions_from_choice,
    compact_suggest_fields,
    fmt_bool,
    log_label,
)
from .calibre_db import PREFERRED_FORMATS
from .ebook_meta import (
    EbookMetaRunner,
    derive_file_work,
    extract_content_signals,
    parse_epub_opf_metadata,
)
from .edition_selection import (
    EditionChoiceInfo,
    book_selection_adjusted_score,
    choose_preferred_edition_info,
    edition_decision_payload,
    edition_gap_tier,
    edition_language_ok_rank,
    effective_candidate_authors,
    is_audio_edition,
    is_blank_language_edition,
    is_collectionish_edition,
    is_edition_write_blocked_blank_language,
    is_ebookish_edition,
    normalize_edition_format,
    rank_candidate_editions,
)
from .hardcover_client import HardcoverClient
from .work_classification import title_normalization_candidate
from .identifiers import clean_isbn, extract_numeric_id
from .language import normalize_language_signal
from .matching import (
    author_coverage,
    author_similarity,
    bare_title_similarity,
    canonically_distinct_authors,
    confidence_tier,
    contributor_count,
    explain_author_mismatch,
    normalize_author_csv,
    normalize_primary_author_value,
    summarize_embedded_mismatch,
    textually_distinct_authors,
    textually_distinct_titles,
    title_marketing_penalty,
)
from .models import (
    AuditRow,
    BookRecord,
    ContentSignals,
    Decision,
    EmbeddedMeta,
    FileWork,
    HardcoverBook,
    HardcoverEdition,
    MatchScores,
)
from .text_normalization import (
    canonical_author_set,
    clean_title_for_matching,
    norm,
    normalize_author_key,
    primary_author,
    smart_title,
    split_author_like_string,
    strip_series_suffix,
    title_query_variants,
    normalize_search_query_title,
)


def vlog(verbose: bool, message: str) -> None:
    if verbose:
        print(message, flush=True)


def _title_normalization_candidate(calibre_title: str, canonical_title: str) -> bool:
    return title_normalization_candidate(calibre_title, canonical_title)


def _canonical_title_variants(title: str) -> List[str]:
    raw = smart_title(title)
    if not raw:
        return []
    candidates: List[str] = []
    seeds = [
        raw,
        clean_title_for_matching(raw),
        normalize_search_query_title(raw),
    ]
    subtitle_patterns = [
        r":\s*(?:book|volume|vol\.?|part)\b.*$",
        r":\s*[^:]{0,120}\b(?:novel|mystery|thriller|novella|chronicle|saga|series|tale|story|collection)\b.*$",
        r"\s*\([^)]{0,120}\)\s*$",
        r"\s*[-–—]\s*[^-–—]{0,120}\b(?:series|book)\b.*$",
    ]
    for seed in list(seeds):
        for pattern in subtitle_patterns:
            seeds.append(re.sub(pattern, '', seed, flags=re.I).strip(' -:;,.[]'))
    for seed in seeds:
        for candidate in title_query_variants(seed or ''):
            candidate = smart_title(candidate)
            candidate = html.unescape(candidate)
            candidate = re.sub(r"\s+", " ", candidate).strip(' -:;,.[]')
            if candidate and candidate not in candidates:
                candidates.append(candidate)
    return candidates


def _best_title_similarity(source_title: str, candidate_title: str) -> float:
    best = 0.0
    for variant in _canonical_title_variants(source_title):
        best = max(best, bare_title_similarity(variant, candidate_title))
    return best


def _is_interesting_verbose_row(
    decision: Decision,
    *,
    current_ok: Optional[bool],
    search_beyond_current: bool,
    metadata_probe_warning: str,
) -> bool:
    reason = str(decision.reason or "")
    if decision.action != "keep_hardcover_id":
        return True
    if decision.confidence_tier != "high":
        return True
    if current_ok is not True:
        return True
    if search_beyond_current:
        return True
    if metadata_probe_warning:
        return True
    if reason.startswith("relink:block_"):
        return True
    if "preferred_edition_blank_language" in reason:
        return True
    if "preferred_edition_differs_from_hardcover_default_ebook_with_narrow_gap" in reason:
        return True
    return False


def _guardrail_note(reason: str) -> str:
    reason_text = str(reason or "")
    if "preferred_edition_blank_language" in reason_text:
        return "blank-language preferred edition blocked automatic edition write"
    if "preferred_edition_differs_from_hardcover_default_ebook_with_narrow_gap" in reason_text:
        return "preferred edition differs from Hardcover default ebook with a narrow score gap"
    if reason_text.startswith("relink:block_"):
        return reason_text
    return ""


def _compact_book_log_line(
    idx: int,
    total: int,
    row: AuditRow,
    *,
    current_ok: Optional[bool],
) -> str:
    parts = [
        f"[{idx}/{total}]",
        f"action={row.recommended_action}",
        f"tier={row.confidence_tier}",
        f"score={row.confidence_score:.2f}",
        f"calibre_id={row.calibre_book_id}",
        f"format={row.file_format or '-'}",
        f'calibre="{log_label(row.calibre_title, max_len=56)}"',
        f'file="{log_label(row.file_work_title or "-", max_len=48)}"',
    ]
    if row.suggested_hardcover_id or row.calibre_hardcover_id:
        parts.append(f"hc={row.suggested_hardcover_id or row.calibre_hardcover_id}")
    if row.suggested_hardcover_edition_id:
        edition_bits = [row.suggested_hardcover_edition_id]
        if row.suggested_hardcover_edition_format_normalized:
            edition_bits.append(row.suggested_hardcover_edition_format_normalized)
        if row.suggested_hardcover_edition_language:
            edition_bits.append(row.suggested_hardcover_edition_language)
        parts.append(f"edition={'/'.join(edition_bits)}")
    parts.append(f"verified={fmt_bool(current_ok)}")
    if row.matched_by:
        parts.append(f"why={row.matched_by}")
    return " ".join(parts)


def _build_verbose_detail_lines(
    row: AuditRow,
    *,
    file_work: FileWork,
    current_book: Optional[HardcoverBook],
    current_score: float,
    current_ok: Optional[bool],
    current_why: str,
    best_choice: EditionChoiceInfo,
    best_book: Optional[HardcoverBook],
    best_edition: Optional[HardcoverEdition],
    search_beyond_current: bool,
    search_reason: str,
    metadata_probe_warning: str,
    metadata_probe_details: str,
    suggest_text: str,
) -> List[str]:
    lines = [
        f'  file work="{log_label(file_work.title or "-", max_len=64)}" authors={file_work.authors or "-"} source={file_work.title_basis}/{file_work.authors_basis}'
    ]
    if current_book:
        lines.append(
            f"  current={compact_book_marker(current_book)} match={current_score:.2f} verified={fmt_bool(current_ok)} why={current_why or '-'}"
        )
    elif row.calibre_hardcover_id:
        lines.append(f"  current_hardcover_id={row.calibre_hardcover_id} verified={fmt_bool(current_ok)}")

    if best_edition:
        gap_tier = (
            edition_gap_tier(best_choice.score_gap, bool(best_choice.runner_up))
            if best_choice.score_gap is not None
            else "-"
        )
        lines.append(
            f"  preferred={compact_edition_marker(best_edition, best_choice.chosen_score)} gap={best_choice.score_gap:.1f} {gap_tier}"
        )
        if best_choice.runner_up:
            lines.append(
                f"  runner_up={compact_edition_marker(best_choice.runner_up, best_choice.runner_up_score)}"
            )
    elif best_book:
        lines.append(f"  best={compact_book_marker(best_book)}")

    if search_beyond_current and search_reason:
        lines.append(f"  search_reason={search_reason}")

    guardrail_note = _guardrail_note(row.reason)
    if guardrail_note:
        lines.append(f"  note={guardrail_note}")
    if metadata_probe_warning:
        lines.append(f"  warning={metadata_probe_warning} | {metadata_probe_details}")

    lines.append(f"  result reason={row.reason}")
    if suggest_text:
        lines.append(f"  suggest {suggest_text}")
    return lines


def is_searchworthy_token(query: str, current_hardcover_id: str = "") -> bool:
    query = (query or "").strip()
    if not query:
        return False
    if current_hardcover_id and query == current_hardcover_id:
        return False
    if re.fullmatch(r"\d{1,9}", query):
        return False
    if re.fullmatch(r"[A-Z0-9_]{6,}", query):
        if re.fullmatch(r"(97[89]\d{10}|\d{10}|\d{13}|B0[A-Z0-9]{8,}|[A-Z0-9]{10})", query):
            return True
        return False
    return True


def _is_ignorable_title_query_candidate(candidate: str) -> bool:
    candidate = norm(candidate or "")
    if not candidate:
        return True
    generic_patterns = [
        r"^about the author$",
        r"^about the authors$",
        r"^about the writers?$",
        r"^about the editor$",
        r"^acknowledg(?:e)?ments?$",
        r"^copyright$",
        r"^title page$",
        r"^table of contents$",
        r"^contents$",
        r"^also by .+$",
    ]
    return any(re.fullmatch(pattern, candidate) for pattern in generic_patterns)


def _content_title_candidate_is_plausible(raw_candidate: str, *, trusted_titles: List[str]) -> bool:
    candidate = smart_title(raw_candidate or "")
    if not candidate or _is_ignorable_title_query_candidate(candidate):
        return False
    trusted = [smart_title(value or "") for value in trusted_titles if value]
    if not trusted:
        return True
    return max((bare_title_similarity(candidate, value) for value in trusted), default=0.0) >= 0.72


def build_search_queries(
    record: BookRecord,
    file_work: FileWork,
    embedded: EmbeddedMeta,
    content: ContentSignals,
) -> List[str]:
    queries: List[str] = []
    current_hardcover_id = extract_numeric_id(record.calibre_hardcover_id)

    exact_tokens: List[str] = []
    for value in record.isbn_candidates + record.asin_candidates:
        if value:
            exact_tokens.append(clean_isbn(value))
    for value in embedded.embedded_identifiers.values():
        cleaned = clean_isbn(value)
        if cleaned:
            exact_tokens.append(cleaned)

    trusted_titles = [file_work.title, embedded.embedded_title, record.calibre_title]
    raw_title_inputs: List[str] = [raw for raw in trusted_titles if raw]
    if content.inferred_title_from_content and _content_title_candidate_is_plausible(
        content.inferred_title_from_content,
        trusted_titles=trusted_titles,
    ):
        raw_title_inputs.append(content.inferred_title_from_content)

    title_candidates: List[str] = []
    for raw in raw_title_inputs:
        for candidate in _canonical_title_variants(raw or ''):
            if not candidate or _is_ignorable_title_query_candidate(candidate):
                continue
            if candidate not in title_candidates:
                title_candidates.append(candidate)

    author_parts = split_author_like_string(file_work.authors or record.calibre_authors)
    primary_author_name = author_parts[0] if author_parts else ''

    for token in exact_tokens:
        if token and is_searchworthy_token(token, current_hardcover_id):
            queries.append(token)

    for candidate in title_candidates[:6]:
        if candidate and primary_author_name:
            queries.append(f"{candidate} {primary_author_name}")
        if candidate:
            queries.append(candidate)

    output: List[str] = []
    seen = set()
    for query in queries:
        query = re.sub(r"\s+", " ", html.unescape(query).strip())
        if query and query not in seen and is_searchworthy_token(query, current_hardcover_id):
            output.append(query)
            seen.add(query)
    return output


def _hardcover_candidate_authors(
    book: Optional[HardcoverBook],
    edition: Optional[HardcoverEdition],
) -> str:
    if edition and getattr(edition, "authors", None) and not is_audio_edition(edition):
        return edition.authors
    if book and getattr(book, "authors", None):
        return book.authors
    return ""


def _preferred_metadata_authors(
    primary_book: Optional[HardcoverBook],
    primary_edition: Optional[HardcoverEdition],
    *,
    secondary_book: Optional[HardcoverBook] = None,
    secondary_edition: Optional[HardcoverEdition] = None,
    file_authors: str = "",
    fallback_authors: str = "",
) -> str:
    return (
        _hardcover_candidate_authors(primary_book, primary_edition)
        or _hardcover_candidate_authors(secondary_book, secondary_edition)
        or file_authors
        or fallback_authors
        or ""
    )


def score_candidate_against_file(
    file_work: FileWork,
    record: BookRecord,
    hardcover_book: HardcoverBook,
    preferred_edition: Optional[HardcoverEdition] = None,
) -> Tuple[float, MatchScores, str]:
    title_score = _best_title_similarity(file_work.title, hardcover_book.title) if file_work.title else 0.0
    if title_score == 0 and record.calibre_title:
        title_score = _best_title_similarity(record.calibre_title, hardcover_book.title) * 0.7
    if (file_work.title and _title_normalization_candidate(file_work.title, hardcover_book.title)) or _title_normalization_candidate(record.calibre_title, hardcover_book.title):
        title_score = max(title_score, 0.98)

    candidate_authors = effective_candidate_authors(hardcover_book, preferred_edition)
    effective_file_authors = file_work.authors or record.calibre_authors
    author_score = 0.0
    if file_work.authors:
        full_author = author_coverage(file_work.authors, candidate_authors)
        primary = author_coverage(primary_author(file_work.authors), candidate_authors)
        if file_work.authors_basis == "calibre_fallback":
            author_score = max(full_author, primary * 0.95)
        else:
            author_score = max(full_author, primary * 0.85)
    if author_score == 0 and record.calibre_authors:
        author_score = author_coverage(record.calibre_authors, candidate_authors) * 0.6

    series_score = 10.0 if (
        record.calibre_series and hardcover_book.series and norm(record.calibre_series) in norm(hardcover_book.series)
    ) else 0.0
    total = title_score * 70 + author_score * 20 + series_score - title_marketing_penalty(hardcover_book.title)

    candidate_contributors = contributor_count(candidate_authors)
    file_contributors = contributor_count(effective_file_authors)
    no_author_overlap = bool(effective_file_authors) and author_coverage(effective_file_authors, candidate_authors) == 0.0

    if no_author_overlap:
        total -= 25.0
        if candidate_contributors >= max(3, file_contributors + 2):
            total -= min(12.0, 2.5 * (candidate_contributors - max(1, file_contributors)))
    elif effective_file_authors and candidate_contributors >= 3 and file_contributors <= 2 and author_score < 0.55:
        total -= 8.0

    if preferred_edition and is_audio_edition(preferred_edition) and author_score < 0.95:
        total -= 12.0
    if preferred_edition and is_blank_language_edition(preferred_edition):
        total -= 2.5
    if preferred_edition and hardcover_book.default_ebook_edition_id and int(preferred_edition.id) == int(
        hardcover_book.default_ebook_edition_id
    ):
        total += 1.0

    total = round(total, 2)
    reasons = []
    if title_score >= 0.98:
        reasons.append("exact-title")
    elif title_score >= 0.90:
        reasons.append("close-title")
    elif title_score >= 0.75:
        reasons.append("partial-title")
    if author_score >= 0.99:
        reasons.append("author")
    elif author_score >= 0.50:
        reasons.append("partial-author")
    elif no_author_overlap and effective_file_authors:
        reasons.append("author-mismatch")
    if series_score:
        reasons.append("series")
    if preferred_edition and is_audio_edition(preferred_edition):
        reasons.append("audio-edition")
    if no_author_overlap and candidate_contributors >= max(3, file_contributors + 2):
        reasons.append("multi-contributor-mismatch")
    if title_marketing_penalty(hardcover_book.title):
        reasons.append("marketing-title")
    return total, MatchScores(round(title_score, 3), round(author_score, 3), round(series_score, 3), total), ",".join(reasons)


def fetch_current_book_resilient(hardcover_client: HardcoverClient, book_id: int) -> Optional[HardcoverBook]:
    book = hardcover_client.fetch_books([int(book_id)], force_refresh=False).get(int(book_id))
    if book:
        return book
    for _ in range(2):
        try:
            book = hardcover_client.fetch_book_by_id(int(book_id), force_refresh=True)
        except Exception:
            book = None
        if book:
            return book
        time.sleep(0.25)
    return None


def validate_current_hardcover_link(
    record: BookRecord,
    file_work: FileWork,
    embedded: EmbeddedMeta,
    hardcover_client: HardcoverClient,
    verbose: bool = False,
) -> Tuple[Optional[HardcoverBook], EditionChoiceInfo, float, MatchScores, Optional[bool], str]:
    current_hardcover_id = extract_numeric_id(record.calibre_hardcover_id)
    if not current_hardcover_id:
        vlog(verbose, "  direct current-id validation failed: no current hardcover-id")
        return None, EditionChoiceInfo(), 0.0, MatchScores(), None, "no-current-hc-id"
    try:
        current_book = fetch_current_book_resilient(hardcover_client, int(current_hardcover_id))
        if not current_book:
            vlog(verbose, f"  direct current-id lookup returned empty for current id {current_hardcover_id}")
            return None, EditionChoiceInfo(), 0.0, MatchScores(), None, "current-hc-id-lookup-empty"
        current_editions = hardcover_client.fetch_editions_for_books([current_book.id]).get(current_book.id, [])
        if not current_editions:
            current_editions = hardcover_client.fetch_editions_for_books([current_book.id], force_refresh=True).get(
                current_book.id,
                [],
            )
        current_ranked_editions = rank_candidate_editions(record, file_work, embedded, current_book, current_editions)
        current_choice = choose_preferred_edition_info(record, file_work, embedded, current_book, current_editions)
        current_edition = current_choice.chosen
        current_score, current_breakdown, why = score_candidate_against_file(file_work, record, current_book, current_edition)
        current_score = book_selection_adjusted_score(current_score, file_work, current_book, current_edition)
        current_ok = current_score >= 75
        if current_book and title_marketing_penalty(current_book.title) and current_breakdown.title_score < 0.90:
            current_ok = False
        preferred_summary = compact_edition_marker(current_edition, current_choice.chosen_score) if current_edition else "-"
        vlog(
            verbose,
            f"  current hc={compact_book_marker(current_book)} match={current_score:.2f} verified={fmt_bool(current_ok)} why={why or '-'} editions={len(current_editions)}",
        )
        if current_edition:
            vlog(
                verbose,
                f"  current preferred={preferred_summary} gap={current_choice.score_gap:.1f} {edition_gap_tier(current_choice.score_gap, bool(current_choice.runner_up))}",
            )
        alt_editions = compact_ranked_editions_from_choice(current_ranked_editions, skip=1, limit=2)
        if alt_editions != "-":
            vlog(verbose, f"  current alternatives={alt_editions}")
        return current_book, current_choice, current_score, current_breakdown, current_ok, why or "current_hardcover_id"
    except Exception as exc:
        vlog(
            verbose,
            f"  HARDCOVER FETCH ERROR calibre_id={record.calibre_book_id} hc_id={current_hardcover_id}: {exc}",
        )
        vlog(verbose, "  direct current-id validation failed")
        return None, EditionChoiceInfo(), 0.0, MatchScores(), None, f"current-hc-fetch-error:{exc}"


def choose_best_candidate(
    record: BookRecord,
    file_work: FileWork,
    embedded: EmbeddedMeta,
    content: ContentSignals,
    hardcover_client: HardcoverClient,
    verbose: bool = False,
) -> Tuple[Optional[HardcoverBook], EditionChoiceInfo, float, MatchScores, str]:
    seen_ids: List[int] = []
    current_hardcover_id = extract_numeric_id(record.calibre_hardcover_id)
    if current_hardcover_id:
        try:
            seen_ids.append(int(current_hardcover_id))
        except Exception:
            pass

    exact_tokens: List[str] = []
    for value in record.isbn_candidates + record.asin_candidates + list(embedded.embedded_identifiers.values()):
        token = clean_isbn(str(value or ""))
        if token and token not in exact_tokens:
            exact_tokens.append(token)

    for token in exact_tokens[:3]:
        ids = hardcover_client.find_book_ids_by_identifier(token)
        if ids:
            vlog(verbose, f"  identifier {token!r} -> ids={ids[:8]}")
        for book_id in ids:
            if book_id not in seen_ids:
                seen_ids.append(book_id)

    search_queries = build_search_queries(record, file_work, embedded, content)
    vlog(verbose, f"  search queries={search_queries[:4]}")
    for query in search_queries[:4]:
        try:
            ids = hardcover_client.search_book_ids(query, per_page=5, page=1)
            vlog(verbose, f"  search {query!r} -> ids={ids[:8]}")
        except Exception as exc:
            vlog(verbose, f"  HARDCOVER SEARCH ERROR query={query!r}: {exc}")
            continue
        for book_id in ids:
            if book_id not in seen_ids:
                seen_ids.append(book_id)
        if len(seen_ids) >= 12:
            break
    if not seen_ids:
        return None, EditionChoiceInfo(), 0.0, MatchScores(), "no-api-candidate"
    try:
        books = hardcover_client.fetch_books(seen_ids)
        editions_by_book = hardcover_client.fetch_editions_for_books(list(books.keys()))
        vlog(verbose, f"  candidate fetch books={len(books)} editions={sum(len(value) for value in editions_by_book.values())}")
    except Exception as exc:
        vlog(verbose, f"  HARDCOVER FETCH ERROR ids={seen_ids}: {exc}")
        return None, EditionChoiceInfo(), 0.0, MatchScores(), f"fetch-error:{exc}"
    scored: List[Tuple[float, MatchScores, str, HardcoverBook, EditionChoiceInfo]] = []
    for book in books.values():
        edition_choice = choose_preferred_edition_info(record, file_work, embedded, book, editions_by_book.get(book.id, []))
        preferred = edition_choice.chosen
        score, breakdown, why = score_candidate_against_file(file_work, record, book, preferred)
        adjusted = book_selection_adjusted_score(score, file_work, book, preferred)
        scored.append((adjusted, breakdown, why, book, edition_choice))
    scored.sort(key=lambda item: item[0], reverse=True)
    if verbose and scored:
        preview = []
        for score, _breakdown, _why, book, edition_choice in scored[:4]:
            edition = edition_choice.chosen
            suffix = (
                f" | preferred={compact_edition_marker(edition, edition_choice.chosen_score)} gap={edition_choice.score_gap:.1f}"
                if edition
                else ""
            )
            preview.append(f"{compact_book_marker(book)} score={score:.2f}{suffix}")
        vlog(verbose, f"  search candidates={preview}")
    if not scored:
        return None, EditionChoiceInfo(), 0.0, MatchScores(), "no-book-details"
    best_score, best_breakdown, why, best_book, best_edition_choice = scored[0]
    if verbose and best_book:
        best_ranked_editions = rank_candidate_editions(
            record,
            file_work,
            embedded,
            best_book,
            editions_by_book.get(best_book.id, []),
        )
        if best_edition_choice.chosen:
            vlog(
                verbose,
                f"  search best preferred={compact_edition_marker(best_edition_choice.chosen, best_edition_choice.chosen_score)} gap={best_edition_choice.score_gap:.1f} {edition_gap_tier(best_edition_choice.score_gap, bool(best_edition_choice.runner_up))}",
            )
        best_alt_editions = compact_ranked_editions_from_choice(best_ranked_editions, skip=1, limit=2)
        if best_alt_editions != "-":
            vlog(verbose, f"  search best alternatives={best_alt_editions}")
    return best_book, best_edition_choice, best_score, best_breakdown, why or "search"


def should_search_after_current_validation(
    record: BookRecord,
    current_book: Optional[HardcoverBook],
    current_score: float,
) -> Tuple[bool, str]:
    if not current_book:
        return True, "current id lookup returned empty"
    if current_score < 75:
        return True, "current work did not match confidently"
    raw_title = smart_title(current_book.title)
    cleaned_title = clean_title_for_matching(raw_title)
    looks_marketing = title_marketing_penalty(raw_title) > 0
    looks_collection = bool(re.search(r"\b(collection|boxed set|books? set|series by|must-read)\b", raw_title, re.I))
    if looks_marketing or looks_collection or cleaned_title != raw_title:
        return True, "current link is plausible but checking for cleaner alternative"
    return False, "direct current-id validation succeeded"


def decide_action(
    record: BookRecord,
    file_work: FileWork,
    embedded: EmbeddedMeta,
    content: ContentSignals,
    current_book: Optional[HardcoverBook],
    current_edition: Optional[HardcoverEdition],
    current_score: float,
    best_book: Optional[HardcoverBook],
    best_edition: Optional[HardcoverEdition],
    best_score: float,
) -> Decision:
    file_vs_calibre_title = bare_title_similarity(file_work.title, record.calibre_title) if file_work.title else 0.0
    file_vs_calibre_auth = author_similarity(file_work.authors, record.calibre_authors) if file_work.authors else 0.0
    file_vs_current_title = bare_title_similarity(file_work.title, current_book.title) if (file_work.title and current_book) else 0.0
    current_candidate_authors = _hardcover_candidate_authors(current_book, current_edition)
    file_vs_current_auth = author_coverage(file_work.authors, current_candidate_authors) if (file_work.authors and current_candidate_authors) else 0.0

    current_match = current_book is not None and current_score >= 75
    best_match = best_book is not None and best_score >= 80

    cleaned_calibre = clean_title_for_matching(record.calibre_title)
    title_needs_cleanup = cleaned_calibre != smart_title(record.calibre_title)

    file_conflicts_with_calibre = (
        (file_work.title and file_vs_calibre_title < 0.55)
        or (file_work.authors and file_vs_calibre_auth < 0.40)
    )
    file_conflicts_with_current = current_book is not None and current_score < 55
    best_candidate_authors = _hardcover_candidate_authors(best_book, best_edition)
    best_file_title_score = _best_title_similarity(file_work.title, best_book.title) if (file_work.title and best_book) else 0.0
    best_file_author_score = author_coverage(file_work.authors, best_candidate_authors) if (
        file_work.authors and best_candidate_authors
    ) else 0.0

    current_clean = clean_title_for_matching(current_book.title) if current_book else ""
    best_clean = clean_title_for_matching(best_book.title) if best_book else ""
    best_cleaner_same_work = bool(
        best_book
        and current_book
        and best_book.id != current_book.id
        and best_clean == current_clean
        and title_marketing_penalty(best_book.title) < title_marketing_penalty(current_book.title)
        and best_score >= current_score - 4
    )

    current_hardcover_id = extract_numeric_id(record.calibre_hardcover_id)
    best_hardcover_id = str(best_book.id) if best_book else ""
    same_current_and_best_id = bool(current_hardcover_id and best_hardcover_id and current_hardcover_id == best_hardcover_id)

    if best_book and best_match and same_current_and_best_id and record.calibre_hardcover_id and not current_match:
        same_id_edition = current_edition or best_edition
        if title_needs_cleanup and bare_title_similarity(cleaned_calibre, best_book.title) >= 0.92:
            return Decision(
                action="safe_auto_fix",
                confidence_score=max(best_score, 90.0),
                confidence_tier="high",
                reason="Current Hardcover work is already correct; normalize the calibre title only",
                issue_category="formatting_cleanup",
                suggested_calibre_title=clean_title_for_matching(best_book.title),
                suggested_calibre_authors=_preferred_metadata_authors(
                    current_book,
                    current_edition,
                    secondary_book=best_book,
                    secondary_edition=best_edition,
                    file_authors=file_work.authors,
                    fallback_authors=record.calibre_authors,
                ),
                suggested_hardcover_id=record.calibre_hardcover_id,
                suggested_hardcover_slug=best_book.slug,
                **edition_decision_payload(same_id_edition),
                fix_basis="same_hc_id_title_cleanup",
            )
        if file_work.title and bare_title_similarity(file_work.title, best_book.title) >= 0.90 and file_vs_calibre_title < 0.70:
            return Decision(
                action="update_calibre_metadata",
                confidence_score=max(best_score, 85.0),
                confidence_tier="high",
                reason="Current Hardcover work is already correct; the calibre title needs updating",
                issue_category="real_mismatch",
                suggested_calibre_title=clean_title_for_matching(best_book.title),
                suggested_calibre_authors=_preferred_metadata_authors(
                    current_book,
                    current_edition,
                    secondary_book=best_book,
                    secondary_edition=best_edition,
                    file_authors=file_work.authors,
                    fallback_authors=record.calibre_authors,
                ),
                suggested_hardcover_id=record.calibre_hardcover_id,
                suggested_hardcover_slug=best_book.slug,
                **edition_decision_payload(same_id_edition),
                fix_basis="same_hc_id_title_update",
            )
        if file_work.authors and author_coverage(file_work.authors, best_book.authors) >= 0.95 and file_vs_calibre_auth < 0.70:
            return Decision(
                action="update_calibre_metadata",
                confidence_score=max(best_score, 85.0),
                confidence_tier="high",
                reason="Current Hardcover work is already correct; the calibre author needs updating",
                issue_category="real_mismatch",
                suggested_calibre_title=record.calibre_title,
                suggested_calibre_authors=_preferred_metadata_authors(
                    current_book,
                    current_edition,
                    secondary_book=best_book,
                    secondary_edition=best_edition,
                    file_authors=file_work.authors,
                    fallback_authors=record.calibre_authors,
                ),
                suggested_hardcover_id=record.calibre_hardcover_id,
                suggested_hardcover_slug=best_book.slug,
                **edition_decision_payload(same_id_edition),
                fix_basis="same_hc_id_author_update",
            )
        return Decision(
            action="keep_hardcover_id",
            confidence_score=max(best_score, 80.0),
            confidence_tier=confidence_tier(max(best_score, 80.0)),
            reason="Current Hardcover ID was re-confirmed by search against the actual ebook file",
            issue_category="verified",
            suggested_hardcover_id=record.calibre_hardcover_id,
            suggested_hardcover_slug=best_book.slug,
            **edition_decision_payload(same_id_edition),
            fix_basis="current_hc_verified_by_search",
        )

    if best_book and best_match and ((not current_book) or best_book.id != current_book.id) and (
        best_score >= current_score + 5 or best_cleaner_same_work
    ):
        relink_block_reason = ""
        if record.calibre_hardcover_id:
            if best_file_title_score < 0.90 and not (_title_normalization_candidate(file_work.title, best_book.title) or _title_normalization_candidate(record.calibre_title, best_book.title)):
                relink_block_reason = "relink:block_title_not_close_enough"
            elif file_work.authors and best_file_author_score < 0.95:
                relink_block_reason = "relink:block_primary_author_not_strong_enough"
            elif record.calibre_series and best_book.series and norm(record.calibre_series) not in norm(best_book.series):
                relink_block_reason = "relink:block_series_conflict"
            elif best_edition and is_collectionish_edition(best_edition):
                relink_block_reason = "relink:block_collectionish_candidate"
        if relink_block_reason:
            return Decision(
                action="manual_review",
                confidence_score=best_score,
                confidence_tier=confidence_tier(best_score),
                reason=relink_block_reason,
                issue_category="manual_review",
                suggested_calibre_title=clean_title_for_matching(best_book.title),
                suggested_calibre_authors=_preferred_metadata_authors(
                    best_book,
                    best_edition,
                    file_authors=file_work.authors,
                    fallback_authors=record.calibre_authors,
                ),
                suggested_hardcover_id=str(best_book.id),
                suggested_hardcover_slug=best_book.slug,
                **edition_decision_payload(best_edition),
                fix_basis="relink_needs_review",
            )
        return Decision(
            action="replace_hardcover_id" if record.calibre_hardcover_id else "set_hardcover_id",
            confidence_score=best_score,
            confidence_tier=confidence_tier(best_score),
            reason=f"relink:ok; file_best_match; hardcover_id={best_book.id}",
            issue_category="hardcover_link",
            suggested_calibre_title=clean_title_for_matching(best_book.title),
            suggested_calibre_authors=file_work.authors or best_book.authors,
            suggested_hardcover_id=str(best_book.id),
            suggested_hardcover_slug=best_book.slug,
            **edition_decision_payload(best_edition),
            fix_basis="file_first_best_match",
        )

    if current_book and current_match:
        if title_needs_cleanup and bare_title_similarity(cleaned_calibre, current_book.title) >= 0.92:
            return Decision(
                action="safe_auto_fix",
                confidence_score=max(current_score, 90.0),
                confidence_tier="high",
                reason="Calibre title contains removable series/marketing suffix; normalize to bare work title",
                issue_category="formatting_cleanup",
                suggested_calibre_title=clean_title_for_matching(current_book.title),
                suggested_calibre_authors=_preferred_metadata_authors(
                    current_book,
                    current_edition,
                    secondary_book=best_book,
                    secondary_edition=best_edition,
                    file_authors=file_work.authors,
                    fallback_authors=record.calibre_authors,
                ),
                suggested_hardcover_id=record.calibre_hardcover_id,
                suggested_hardcover_slug=current_book.slug,
                **edition_decision_payload(current_edition),
                fix_basis="bare_title_policy",
            )
        if file_work.title and file_vs_current_title >= 0.90 and file_vs_calibre_title < 0.70:
            return Decision(
                action="update_calibre_metadata",
                confidence_score=max(current_score, 85.0),
                confidence_tier="high",
                reason="Actual ebook file title differs materially from the calibre title",
                issue_category="real_mismatch",
                suggested_calibre_title=clean_title_for_matching(current_book.title),
                suggested_calibre_authors=_preferred_metadata_authors(
                    current_book,
                    current_edition,
                    secondary_book=best_book,
                    secondary_edition=best_edition,
                    file_authors=file_work.authors,
                    fallback_authors=record.calibre_authors,
                ),
                suggested_hardcover_id=record.calibre_hardcover_id,
                suggested_hardcover_slug=current_book.slug,
                **edition_decision_payload(current_edition),
                fix_basis="file_and_current_hc_agree",
            )
        if file_work.authors and file_vs_current_auth >= 0.95 and file_vs_calibre_auth < 0.70:
            return Decision(
                action="update_calibre_metadata",
                confidence_score=max(current_score, 85.0),
                confidence_tier="high",
                reason="Actual ebook file author differs materially from the calibre author",
                issue_category="real_mismatch",
                suggested_calibre_title=record.calibre_title,
                suggested_calibre_authors=_preferred_metadata_authors(
                    current_book,
                    current_edition,
                    secondary_book=best_book,
                    secondary_edition=best_edition,
                    file_authors=file_work.authors,
                    fallback_authors=record.calibre_authors,
                ),
                suggested_hardcover_id=record.calibre_hardcover_id,
                suggested_hardcover_slug=current_book.slug,
                **edition_decision_payload(current_edition),
                fix_basis="file_author_overrides_calibre",
            )
        if file_conflicts_with_calibre and not file_conflicts_with_current:
            return Decision(
                action="update_calibre_metadata",
                confidence_score=max(current_score, 80.0),
                confidence_tier="medium",
                reason="Actual ebook file fits the current Hardcover work but not the current calibre metadata",
                issue_category="real_mismatch",
                suggested_calibre_title=clean_title_for_matching(current_book.title),
                suggested_calibre_authors=_preferred_metadata_authors(
                    current_book,
                    current_edition,
                    secondary_book=best_book,
                    secondary_edition=best_edition,
                    file_authors=file_work.authors,
                    fallback_authors=record.calibre_authors,
                ),
                suggested_hardcover_id=record.calibre_hardcover_id,
                suggested_hardcover_slug=current_book.slug,
                **edition_decision_payload(current_edition),
                fix_basis="file_first_current_hc_ok",
            )
        return Decision(
            action="keep_hardcover_id",
            confidence_score=max(current_score, 80.0),
            confidence_tier=confidence_tier(max(current_score, 80.0)),
            reason="Current Hardcover link verified against the actual ebook file",
            issue_category="verified",
            suggested_hardcover_id=record.calibre_hardcover_id,
            suggested_hardcover_slug=current_book.slug,
            **edition_decision_payload(current_edition),
            fix_basis="current_hc_verified_by_file",
        )

    if best_book and best_match:
        action = "replace_hardcover_id" if record.calibre_hardcover_id else "set_hardcover_id"
        if same_current_and_best_id and action == "replace_hardcover_id":
            if _title_normalization_candidate(record.calibre_title, best_book.title):
                return Decision(
                    action="safe_auto_fix",
                    confidence_score=max(best_score, 82.0),
                    confidence_tier=confidence_tier(max(best_score, 82.0)),
                    reason="Current Hardcover work is already correct; normalize the calibre title only",
                    issue_category="cleanup_only",
                    suggested_calibre_title=clean_title_for_matching(best_book.title),
                    suggested_calibre_authors=_preferred_metadata_authors(
                        current_book,
                        current_edition,
                        secondary_book=best_book,
                        secondary_edition=best_edition,
                        file_authors=file_work.authors,
                        fallback_authors=record.calibre_authors,
                    ),
                    suggested_hardcover_id=record.calibre_hardcover_id,
                    suggested_hardcover_slug=best_book.slug,
                    **edition_decision_payload(best_edition),
                    fix_basis="same_hc_id_title_cleanup",
                )
            action = "manual_review"
        if action == "replace_hardcover_id" and (
            best_file_title_score < 0.90 or (file_work.authors and best_file_author_score < 0.95)
        ):
            action = "manual_review"
        return Decision(
            action=action,
            confidence_score=best_score,
            confidence_tier=confidence_tier(best_score),
            reason=(
                f"relink:ok; file_best_match; hardcover_id={best_book.id}"
                if action == "replace_hardcover_id"
                else (
                    f"Actual ebook file matches Hardcover {best_book.id}"
                    if action == "set_hardcover_id"
                    else "Current Hardcover link could not be confirmed confidently"
                )
            ),
            issue_category=(
                "hardcover_link"
                if action in {"replace_hardcover_id", "set_hardcover_id"}
                else "manual_review"
            ),
            suggested_calibre_title=clean_title_for_matching(best_book.title),
            suggested_calibre_authors=file_work.authors or best_book.authors,
            suggested_hardcover_id=(record.calibre_hardcover_id if action != "replace_hardcover_id" else str(best_book.id)),
            suggested_hardcover_slug=best_book.slug,
            **edition_decision_payload(best_edition),
            fix_basis=(
                "file_best_match" if action in {"replace_hardcover_id", "set_hardcover_id"} else "same_hc_id_needs_review"
            ),
        )

    content_lang = normalize_language_signal(content.inferred_language_from_content)
    embedded_lang = normalize_language_signal(embedded.embedded_language)
    if (
        content_lang in {"deu", "fra", "spa"}
        and content.language_confidence >= 0.02
        and not current_match
        and not best_match
    ) or (embedded_lang in {"deu", "fra", "spa"} and not current_match and not best_match):
        language_label = content_lang or embedded_lang
        return Decision(
            action="likely_non_english",
            confidence_score=max(best_score, current_score, 78.0),
            confidence_tier="medium",
            reason=f"Actual ebook file appears to be non-English ({language_label}); English-title matching may be unreliable",
            issue_category="language",
            fix_basis="content_language_probe",
        )

    if file_work.title and not current_match and best_file_title_score >= 0.95 and best_file_author_score < 0.40:
        return Decision(
            action="manual_review_title_match_author_unconfirmed",
            confidence_score=max(best_score, 70.0),
            confidence_tier=confidence_tier(max(best_score, 70.0)),
            reason=f"manual_review:title_exact_author_unconfirmed; author_reason={explain_author_mismatch(file_work.authors, best_candidate_authors)}",
            issue_category="manual_review",
            suggested_calibre_title=clean_title_for_matching(best_book.title) if best_book else record.calibre_title,
            suggested_calibre_authors=_preferred_metadata_authors(
                best_book,
                best_edition,
                file_authors=file_work.authors,
                fallback_authors=record.calibre_authors,
            ),
            suggested_hardcover_id=str(best_book.id) if best_book else "",
            suggested_hardcover_slug=best_book.slug if best_book else "",
            **edition_decision_payload(best_edition),
            fix_basis="title_exact_author_unconfirmed",
        )

    if file_work.title and file_vs_calibre_title >= 0.95 and file_work.authors and file_vs_calibre_auth == 0.0 and not current_match and not best_match:
        return Decision(
            action="suspected_author_mismatch",
            confidence_score=72.0,
            confidence_tier="low",
            reason=f"suspected_author_mismatch; author_reason={explain_author_mismatch(file_work.authors, record.calibre_authors)}",
            issue_category="author_mismatch",
            fix_basis="file_title_matches_calibre_author_does_not",
        )

    if file_conflicts_with_calibre and not current_match:
        return Decision(
            action="suspected_file_mismatch",
            confidence_score=80.0,
            confidence_tier="medium",
            reason="suspected_file_mismatch; file_not_calibre; no_strong_hardcover_confirmation",
            issue_category="wrong_file",
            fix_basis="file_not_calibre",
        )

    return Decision(
        action="manual_review",
        confidence_score=max(current_score, best_score),
        confidence_tier=confidence_tier(max(current_score, best_score)),
        reason="Current Hardcover link could not be confirmed confidently",
        issue_category="manual_review",
        **edition_decision_payload(best_edition),
        fix_basis="needs_review",
    )


def apply_preferred_edition_guardrails(
    record: BookRecord,
    file_work: FileWork,
    decision: Decision,
    current_book: Optional[HardcoverBook],
    current_choice: EditionChoiceInfo,
    current_score: float,
    best_book: Optional[HardcoverBook],
    best_choice: EditionChoiceInfo,
    best_score: float,
) -> Decision:
    trusted_actions = {
        "keep_hardcover_id",
        "safe_auto_fix",
        "update_calibre_metadata",
        "set_hardcover_id",
        "replace_hardcover_id",
    }
    if decision.action not in trusted_actions:
        return decision

    target_hardcover_id = extract_numeric_id(decision.suggested_hardcover_id) or extract_numeric_id(record.calibre_hardcover_id)
    target_book: Optional[HardcoverBook] = None
    target_choice = EditionChoiceInfo()
    target_edition: Optional[HardcoverEdition] = None
    target_score = max(current_score, best_score, float(decision.confidence_score or 0.0))

    if target_hardcover_id and current_book and str(current_book.id) == target_hardcover_id:
        target_book = current_book
        target_choice = current_choice
        target_edition = current_choice.chosen
        target_score = max(current_score, float(decision.confidence_score or 0.0))
    elif target_hardcover_id and best_book and str(best_book.id) == target_hardcover_id:
        target_book = best_book
        target_choice = best_choice
        target_edition = best_choice.chosen
        target_score = max(best_score, float(decision.confidence_score or 0.0))
    elif decision.action in {"set_hardcover_id", "replace_hardcover_id"} and best_book:
        target_book = best_book
        target_choice = best_choice
        target_edition = best_choice.chosen
        target_score = max(best_score, float(decision.confidence_score or 0.0))
    elif current_book:
        target_book = current_book
        target_choice = current_choice
        target_edition = current_choice.chosen
        target_score = max(current_score, float(decision.confidence_score or 0.0))
    elif best_book:
        target_book = best_book
        target_choice = best_choice
        target_edition = best_choice.chosen
        target_score = max(best_score, float(decision.confidence_score or 0.0))

    if not target_book or not target_edition:
        return decision
    if is_edition_write_blocked_blank_language(target_edition):
        return Decision(
            action="manual_review",
            confidence_score=max(target_score, 75.0),
            confidence_tier=confidence_tier(max(target_score, 75.0)),
            reason="preferred_edition_blank_language; manual review required before any edition write",
            issue_category="manual_review",
            suggested_calibre_title=(decision.suggested_calibre_title or clean_title_for_matching(target_book.title) or record.calibre_title),
            suggested_calibre_authors=(decision.suggested_calibre_authors or file_work.authors or target_book.authors or record.calibre_authors),
            suggested_hardcover_id=(decision.suggested_hardcover_id or str(target_book.id)),
            suggested_hardcover_slug=(decision.suggested_hardcover_slug or target_book.slug),
            **edition_decision_payload(target_edition),
            fix_basis="preferred_edition_blank_language",
        )

    prefers_ebook = (record.file_format or "").upper() in set(PREFERRED_FORMATS)
    default_ebook = target_choice.default_ebook
    default_ebook_gap = abs(float(target_choice.chosen_vs_default_ebook_score_gap or 0.0))
    if (
        prefers_ebook
        and default_ebook
        and target_edition
        and int(target_edition.id) != int(default_ebook.id)
        and not is_audio_edition(default_ebook)
        and edition_language_ok_rank(default_ebook)
        and default_ebook_gap <= 75.0
    ):
        return Decision(
            action="manual_review",
            confidence_score=max(target_score, 75.0),
            confidence_tier=confidence_tier(max(target_score, 75.0)),
            reason=f"preferred_edition_differs_from_hardcover_default_ebook_with_narrow_gap; default_ebook={default_ebook.id}; gap={default_ebook_gap:.1f}",
            issue_category="manual_review",
            suggested_calibre_title=(decision.suggested_calibre_title or clean_title_for_matching(target_book.title) or record.calibre_title),
            suggested_calibre_authors=(decision.suggested_calibre_authors or file_work.authors or target_book.authors or record.calibre_authors),
            suggested_hardcover_id=(decision.suggested_hardcover_id or str(target_book.id)),
            suggested_hardcover_slug=(decision.suggested_hardcover_slug or target_book.slug),
            **edition_decision_payload(target_edition),
            fix_basis="preferred_edition_vs_default_ebook_needs_review",
        )

    return decision


def row_from_result(
    record: BookRecord,
    file_work: FileWork,
    embedded: EmbeddedMeta,
    content: ContentSignals,
    current_book: Optional[HardcoverBook],
    best_book: Optional[HardcoverBook],
    edition_choice: EditionChoiceInfo,
    matched_by: str,
    current_ok: Optional[bool],
    decision: Decision,
    best_breakdown: MatchScores,
) -> AuditRow:
    preferred_edition = edition_choice.chosen
    suggested_title = decision.suggested_calibre_title or record.calibre_title
    suggested_authors = decision.suggested_calibre_authors or record.calibre_authors
    same_hardcover_id_as_suggestion = bool(
        extract_numeric_id(record.calibre_hardcover_id)
        and extract_numeric_id(decision.suggested_hardcover_id)
        and extract_numeric_id(record.calibre_hardcover_id) == extract_numeric_id(decision.suggested_hardcover_id)
    )
    embedded_mismatch_summary = summarize_embedded_mismatch(
        embedded,
        record.calibre_title,
        record.calibre_authors,
        suggested_title,
        suggested_authors,
    )
    current_title = best_book.title if best_book else ""
    current_authors = best_book.authors if best_book else ""
    current_hardcover_title = current_book.title if current_book else ""
    current_hardcover_authors = current_book.authors if current_book else ""
    preferred_candidate_authors = effective_candidate_authors(best_book, preferred_edition) if best_book else ""
    return AuditRow(
        calibre_book_id=record.calibre_book_id,
        calibre_title=record.calibre_title,
        calibre_authors=record.calibre_authors,
        calibre_series=record.calibre_series,
        calibre_series_index=record.calibre_series_index,
        calibre_language=record.calibre_language,
        calibre_hardcover_id=record.calibre_hardcover_id,
        calibre_hardcover_slug=record.calibre_hardcover_slug,
        current_hardcover_edition_id=record.calibre_hardcover_edition_id,
        file_path=record.file_path,
        file_format=record.file_format,
        file_work_title=file_work.title,
        file_work_authors=file_work.authors,
        file_work_title_basis=file_work.title_basis,
        file_work_authors_basis=file_work.authors_basis,
        embedded_title=embedded.embedded_title,
        embedded_authors=embedded.embedded_authors,
        embedded_language=embedded.embedded_language,
        inferred_title_from_content=content.inferred_title_from_content,
        inferred_author_from_content=content.inferred_author_from_content,
        inferred_language_from_content=content.inferred_language_from_content,
        hardcover_candidate_id=str(best_book.id) if best_book else "",
        hardcover_title=current_title,
        hardcover_authors=current_authors,
        hardcover_series=best_book.series if best_book else "",
        hardcover_slug=best_book.slug if best_book else "",
        current_hardcover_title=current_hardcover_title,
        current_hardcover_authors=current_hardcover_authors,
        suggested_hardcover_title=best_book.title if best_book else decision.suggested_calibre_title,
        suggested_hardcover_authors=preferred_candidate_authors or decision.suggested_calibre_authors or (best_book.authors if best_book else ""),
        preferred_edition_id=str(preferred_edition.id) if preferred_edition else "",
        preferred_edition_title=preferred_edition.title if preferred_edition else "",
        preferred_edition_reading_format=preferred_edition.reading_format if preferred_edition else "",
        preferred_edition_edition_format=preferred_edition.edition_format if preferred_edition else "",
        preferred_edition_format_normalized=normalize_edition_format(
            preferred_edition.edition_format,
            preferred_edition.reading_format,
        ) if preferred_edition else "",
        preferred_edition_is_ebookish=bool(is_ebookish_edition(preferred_edition)) if preferred_edition else False,
        preferred_edition_language=preferred_edition.language if preferred_edition else "",
        preferred_edition_reason=edition_choice.chosen_reason,
        preferred_edition_score=int(preferred_edition.score or 0) if preferred_edition else 0,
        preferred_edition_users_count=int(preferred_edition.users_count or 0) if preferred_edition else 0,
        preferred_edition_users_read_count=int(preferred_edition.users_read_count or 0) if preferred_edition else 0,
        preferred_edition_rating=float(preferred_edition.rating or 0.0) if preferred_edition else 0.0,
        preferred_edition_lists_count=int(preferred_edition.lists_count or 0) if preferred_edition else 0,
        preferred_edition_release_date=preferred_edition.release_date if preferred_edition else "",
        runner_up_edition_id=str(edition_choice.runner_up.id) if edition_choice.runner_up else "",
        runner_up_edition_title=edition_choice.runner_up.title if edition_choice.runner_up else "",
        runner_up_edition_reading_format=edition_choice.runner_up.reading_format if edition_choice.runner_up else "",
        runner_up_edition_edition_format=edition_choice.runner_up.edition_format if edition_choice.runner_up else "",
        runner_up_edition_format_normalized=normalize_edition_format(
            edition_choice.runner_up.edition_format,
            edition_choice.runner_up.reading_format,
        ) if edition_choice.runner_up else "",
        runner_up_edition_is_ebookish=bool(is_ebookish_edition(edition_choice.runner_up)) if edition_choice.runner_up else False,
        runner_up_edition_language=edition_choice.runner_up.language if edition_choice.runner_up else "",
        runner_up_edition_reason=edition_choice.runner_up_reason,
        runner_up_edition_score=int(edition_choice.runner_up.score or 0) if edition_choice.runner_up else 0,
        runner_up_edition_users_count=int(edition_choice.runner_up.users_count or 0) if edition_choice.runner_up else 0,
        runner_up_edition_users_read_count=int(edition_choice.runner_up.users_read_count or 0) if edition_choice.runner_up else 0,
        runner_up_edition_rating=float(edition_choice.runner_up.rating or 0.0) if edition_choice.runner_up else 0.0,
        runner_up_edition_lists_count=int(edition_choice.runner_up.lists_count or 0) if edition_choice.runner_up else 0,
        runner_up_edition_release_date=edition_choice.runner_up.release_date if edition_choice.runner_up else "",
        default_ebook_edition_id=str(edition_choice.default_ebook.id) if edition_choice.default_ebook else "",
        default_ebook_edition_title=edition_choice.default_ebook.title if edition_choice.default_ebook else "",
        default_ebook_edition_reading_format=edition_choice.default_ebook.reading_format if edition_choice.default_ebook else "",
        default_ebook_edition_edition_format=edition_choice.default_ebook.edition_format if edition_choice.default_ebook else "",
        default_ebook_edition_format_normalized=normalize_edition_format(
            edition_choice.default_ebook.edition_format,
            edition_choice.default_ebook.reading_format,
        ) if edition_choice.default_ebook else "",
        default_ebook_edition_language=edition_choice.default_ebook.language if edition_choice.default_ebook else "",
        default_ebook_edition_reason=edition_choice.default_ebook_reason,
        default_ebook_edition_score=round(float(edition_choice.default_ebook_score or 0.0), 3),
        preferred_matches_default_ebook=bool(edition_choice.chosen_matches_default_ebook),
        preferred_vs_default_ebook_score_gap=round(float(edition_choice.chosen_vs_default_ebook_score_gap or 0.0), 3),
        edition_choice_score=round(float(edition_choice.chosen_score or 0.0), 3),
        edition_runner_up_score=round(float(edition_choice.runner_up_score or 0.0), 3),
        edition_choice_score_gap=round(float(edition_choice.score_gap or 0.0), 3),
        edition_candidates_considered=int(edition_choice.count_considered or 0),
        confidence_score=round(decision.confidence_score, 2),
        confidence_tier=decision.confidence_tier,
        recommended_action=decision.action,
        reason=decision.reason,
        issue_category=decision.issue_category,
        matched_by=matched_by,
        current_hardcover_match_ok="" if current_ok is None else ("yes" if current_ok else "no"),
        first_heading_excerpt=content.first_heading_excerpt,
        ebook_meta_tool_used=embedded.tool_used,
        file_vs_calibre_title_score=round(
            bare_title_similarity(file_work.title, record.calibre_title) if file_work.title else 0.0,
            3,
        ),
        file_vs_calibre_authors_score=round(
            author_similarity(file_work.authors, record.calibre_authors) if file_work.authors else 0.0,
            3,
        ),
        file_vs_current_title_score=round(
            bare_title_similarity(file_work.title, current_hardcover_title) if file_work.title and current_hardcover_title else 0.0,
            3,
        ),
        file_vs_current_authors_score=round(
            author_coverage(file_work.authors, current_hardcover_authors) if file_work.authors and current_hardcover_authors else 0.0,
            3,
        ),
        best_title_score=best_breakdown.title_score,
        best_author_score=best_breakdown.author_score,
        best_series_score=best_breakdown.series_score,
        best_total_score=best_breakdown.total_score,
        suggested_calibre_title=decision.suggested_calibre_title,
        suggested_calibre_authors=decision.suggested_calibre_authors,
        suggested_hardcover_id=decision.suggested_hardcover_id,
        suggested_hardcover_slug=decision.suggested_hardcover_slug,
        suggested_hardcover_edition_id=decision.suggested_hardcover_edition_id,
        suggested_hardcover_edition_title=decision.suggested_hardcover_edition_title,
        suggested_hardcover_edition_format=decision.suggested_hardcover_edition_format,
        suggested_hardcover_reading_format=decision.suggested_hardcover_reading_format,
        suggested_hardcover_edition_format_raw=decision.suggested_hardcover_edition_format_raw,
        suggested_hardcover_edition_format_normalized=decision.suggested_hardcover_edition_format_normalized,
        suggested_hardcover_edition_is_ebookish=bool(decision.suggested_hardcover_edition_is_ebookish),
        suggested_hardcover_edition_language=decision.suggested_hardcover_edition_language,
        calibre_author_normalized=normalize_author_csv(record.calibre_authors),
        file_author_normalized=normalize_author_csv(file_work.authors),
        hardcover_primary_author_normalized=normalize_primary_author_value(preferred_candidate_authors or current_authors),
        author_mismatch_reason=explain_author_mismatch(file_work.authors, preferred_candidate_authors or current_authors),
        same_hardcover_id_as_suggestion=same_hardcover_id_as_suggestion,
        embedded_title_mismatch_to_calibre=bool(
            embedded.embedded_title and record.calibre_title and textually_distinct_titles(embedded.embedded_title, record.calibre_title)
        ),
        embedded_authors_mismatch_to_calibre_text=bool(
            embedded.embedded_authors and record.calibre_authors and textually_distinct_authors(embedded.embedded_authors, record.calibre_authors)
        ),
        embedded_authors_mismatch_to_calibre_canonical=bool(
            embedded.embedded_authors
            and record.calibre_authors
            and canonically_distinct_authors(embedded.embedded_authors, record.calibre_authors)
        ),
        embedded_title_mismatch_to_suggested=bool(
            embedded.embedded_title and suggested_title and textually_distinct_titles(embedded.embedded_title, suggested_title)
        ),
        embedded_authors_mismatch_to_suggested_text=bool(
            embedded.embedded_authors and suggested_authors and textually_distinct_authors(embedded.embedded_authors, suggested_authors)
        ),
        embedded_authors_mismatch_to_suggested_canonical=bool(
            embedded.embedded_authors
            and suggested_authors
            and canonically_distinct_authors(embedded.embedded_authors, suggested_authors)
        ),
        embedded_calibre_mismatch_summary=embedded_mismatch_summary,
        fix_basis=decision.fix_basis,
    )


def audit_books(
    records: List[BookRecord],
    hardcover_client: HardcoverClient,
    ebook_meta_runner: EbookMetaRunner,
    limit: Optional[int],
    verbose: bool,
    progress_every: int = 100,
    show_progress_summary: bool = False,
) -> List[AuditRow]:
    rows: List[AuditRow] = []
    subset = records[:limit] if limit else records
    total = len(subset)
    audit_started_at = time.monotonic()
    audit_hardcover_start = hardcover_client.stats_snapshot()
    for idx, record in enumerate(subset, start=1):
        host_path = Path(record.file_path)
        embedded = ebook_meta_runner.run(host_path)
        if not embedded.embedded_title and host_path.suffix.lower() in {".epub", ".kepub", ".oebzip"}:
            opf = parse_epub_opf_metadata(host_path)
            if opf.embedded_title:
                embedded = opf
        content = extract_content_signals(record.file_path, record.calibre_title, record.calibre_authors)
        file_work = derive_file_work(record, embedded, content)

        current_book = None
        current_choice = EditionChoiceInfo()
        current_ok = None
        current_score = 0.0
        current_breakdown = MatchScores()
        current_why = ""

        current_book, current_choice, current_score, current_breakdown, current_ok, current_why = validate_current_hardcover_link(
            record,
            file_work,
            embedded,
            hardcover_client,
            verbose=verbose,
        )

        current_edition = current_choice.chosen
        best_book = current_book
        best_choice = current_choice
        best_edition = best_choice.chosen
        best_score = current_score
        best_breakdown = current_breakdown
        matched_by = "current_hardcover_id" if current_book else ""

        search_beyond_current, search_reason = should_search_after_current_validation(record, current_book, current_score)
        if current_ok:
            matched_by = current_why or "current_hardcover_id"

        if search_beyond_current:
            vlog(verbose, f"  search reason={search_reason}")
            candidate_book, candidate_choice, candidate_score, candidate_breakdown, candidate_why = choose_best_candidate(
                record,
                file_work,
                embedded,
                content,
                hardcover_client,
                verbose=verbose,
            )
            candidate_edition = candidate_choice.chosen
            if candidate_book and (best_book is None or candidate_score >= best_score):
                best_book = candidate_book
                best_choice = candidate_choice
                best_edition = candidate_edition
                best_score = candidate_score
                best_breakdown = candidate_breakdown
                matched_by = candidate_why or "search"
                extra = (
                    f" | preferred={compact_edition_marker(candidate_edition, candidate_choice.chosen_score)} gap={candidate_choice.score_gap:.1f}"
                    if candidate_edition
                    else ""
                )
                vlog(
                    verbose,
                    f"  search best candidate={compact_book_marker(candidate_book)} score={candidate_score:.2f} matched_by={matched_by}{extra}",
                )

        decision = decide_action(
            record,
            file_work,
            embedded,
            content,
            current_book,
            current_edition,
            current_score,
            best_book,
            best_edition,
            best_score,
        )
        decision = apply_preferred_edition_guardrails(
            record,
            file_work,
            decision,
            current_book,
            current_choice,
            current_score,
            best_book,
            best_choice,
            best_score,
        )
        audit_row = row_from_result(
            record,
            file_work,
            embedded,
            content,
            current_book,
            best_book,
            best_choice,
            matched_by,
            current_ok,
            decision,
            best_breakdown,
        )
        metadata_probe_warning, metadata_probe_details = metadata_probe_diagnostic(audit_row)
        suggest_text = compact_suggest_fields(decision, best_book, best_edition)

        if verbose:
            vlog(
                True,
                _compact_book_log_line(
                    idx,
                    total,
                    audit_row,
                    current_ok=current_ok,
                ),
            )
            if _is_interesting_verbose_row(
                decision,
                current_ok=current_ok,
                search_beyond_current=search_beyond_current,
                metadata_probe_warning=metadata_probe_warning,
            ):
                for line in _build_verbose_detail_lines(
                    audit_row,
                    file_work=file_work,
                    current_book=current_book,
                    current_score=current_score,
                    current_ok=current_ok,
                    current_why=current_why,
                    best_choice=best_choice,
                    best_book=best_book,
                    best_edition=best_edition,
                    search_beyond_current=search_beyond_current,
                    search_reason=search_reason,
                    metadata_probe_warning=metadata_probe_warning,
                    metadata_probe_details=metadata_probe_details,
                    suggest_text=suggest_text,
                ):
                    vlog(True, line)

        rows.append(audit_row)

        if idx % 20 == 0:
            hardcover_client.save_cache()
        if (show_progress_summary or verbose) and (idx % max(1, int(progress_every)) == 0 or idx == total):
            elapsed = time.monotonic() - audit_started_at
            vlog(
                True,
                build_progress_line(
                    rows,
                    current=idx,
                    total=total,
                    elapsed_s=elapsed,
                    hardcover_delta_text=hardcover_client.stats_delta_text(audit_hardcover_start),
                ),
            )

    hardcover_client.save_cache()
    return rows
