from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from .calibre_db import PREFERRED_FORMATS
from .identifiers import clean_isbn
from .matching import explain_author_mismatch, title_marketing_penalty
from .models import BookRecord, EditionChoiceInfo, EmbeddedMeta, FileWork, HardcoverBook, HardcoverEdition
from .text_normalization import (
    author_match_set,
    canonical_author_set,
    clean_title_for_matching,
    norm,
    smart_title,
)
from .work_classification import classify_hardcover_edition, classify_local_file_kind, work_kind_penalty
from .work_matching import candidate_author_string

EBOOKISH_EDITION_FORMAT_TOKENS = (
    "ebook",
    "e-book",
    "kindle",
    "epub",
    "kepub",
    "kobo",
    "azw",
    "azw3",
    "mobi",
    "digital",
    "electronic",
)


def edition_gap_tier(gap: float, has_runner_up: bool) -> str:
    if not has_runner_up:
        return "single_candidate"
    if gap >= 100:
        return "dominant"
    if gap >= 40:
        return "clear"
    if gap >= 15:
        return "moderate"
    if gap > 0:
        return "narrow"
    return "tie_or_negative"


def is_audio_edition(edition: HardcoverEdition) -> bool:
    reading = norm(edition.reading_format)
    physical = norm(edition.physical_format)
    edition_format = norm(edition.edition_format)
    return (
        reading == "listened"
        or (edition.audio_seconds or 0) > 0
        or "audio" in reading
        or "audio" in physical
        or "audio" in edition_format
    )


def is_blank_language_edition(edition: Optional[HardcoverEdition]) -> bool:
    return bool(edition and not str(edition.language or "").strip())


def is_edition_write_blocked_audio(edition: Optional[HardcoverEdition]) -> bool:
    return bool(edition and is_audio_edition(edition))


def is_edition_write_blocked_blank_language(edition: Optional[HardcoverEdition]) -> bool:
    return is_blank_language_edition(edition)


def is_edition_write_blocked_row(*, format_normalized: Any = "", language: Any = "") -> bool:
    fmt = norm(str(format_normalized or ""))
    return fmt == "audiobook" or not str(language or "").strip()


def is_english_language_name(name: str) -> bool:
    return bool(str(name or "").strip()) and norm(name).startswith("english")


def is_unknown_language_name(name: str) -> bool:
    return not str(name or "").strip()


def edition_language_ok_rank(edition: HardcoverEdition) -> int:
    return 1 if is_english_language_name(edition.language) else 0


def edition_unknown_language_rank(edition: HardcoverEdition) -> int:
    return 1 if is_unknown_language_name(edition.language) else 0


def edition_explicit_english_rank(edition: HardcoverEdition) -> int:
    return 1 if norm(edition.language).startswith("english") else 0


def is_ebookish_edition(edition: HardcoverEdition) -> bool:
    if is_audio_edition(edition):
        return False
    reading = norm(edition.reading_format)
    edition_format = norm(edition.edition_format)
    if reading == "ebook":
        return True
    return any(token in edition_format for token in EBOOKISH_EDITION_FORMAT_TOKENS)


def normalize_edition_format(value: str, reading_format: str = "") -> str:
    reading = norm(reading_format)
    edition_format = norm(value)
    if reading == "ebook" or any(token in edition_format for token in EBOOKISH_EDITION_FORMAT_TOKENS):
        return "ebook"
    if reading == "listened" or "audio" in reading or "audio" in edition_format or "audible" in edition_format:
        return "audiobook"
    if "hardcover" in edition_format or "hardback" in edition_format:
        return "hardcover"
    if "paperback" in edition_format or "mass market" in edition_format or "softcover" in edition_format:
        return "paperback"
    if "digital" in edition_format:
        return "digital"
    if reading == "read":
        return "read"
    if edition_format:
        return edition_format
    if reading:
        return reading
    return "unknown"


def is_collectionish_edition(edition: HardcoverEdition) -> bool:
    raw = " ".join(
        value for value in [smart_title(edition.title or ""), smart_title(edition.subtitle or "")] if value
    ).strip()
    if not raw:
        return False
    patterns = [
        r"\bomnibus\b",
        r"\bbox(?:ed)?\s+set\b",
        r"\bcollection\b",
        r"\bcollected\b",
        r"\bbundle\b",
        r"\bcomplete\s+(?:works|novels|series|trilogy|saga|stories)\b",
        r"\b3[- ]in[- ]1\b",
        r"\b2[- ]in[- ]1\b",
        r"\bcontains\s+books?\b",
    ]
    return any(re.search(pattern, raw, re.I) for pattern in patterns)


def edition_text_blob(edition: HardcoverEdition, book: Optional[HardcoverBook] = None) -> str:
    parts = [
        smart_title(edition.title or ""),
        smart_title(edition.subtitle or ""),
        smart_title(book.title or "") if book else "",
    ]
    return " ".join(part for part in parts if part).strip()


def file_title_signals_non_prose(file_work: FileWork, book: Optional[HardcoverBook] = None) -> bool:
    local_kind = classify_local_file_kind(file_work, None, "")
    return local_kind not in {"prose_novel", "unknown"}


def is_non_prose_or_adaptation_edition(edition: HardcoverEdition, book: Optional[HardcoverBook] = None) -> bool:
    return classify_hardcover_edition(edition, book) not in {"prose_novel", "unknown"}


def edition_side_material_penalty(edition: HardcoverEdition, file_work: FileWork, book: Optional[HardcoverBook] = None) -> float:
    local_kind = classify_local_file_kind(file_work, None, "")
    candidate_kind = classify_hardcover_edition(edition, book)
    penalty = 0.0
    if is_collectionish_edition(edition):
        penalty += 40.0
    penalty += work_kind_penalty(local_kind, candidate_kind) * 2.5
    if not str(edition.language or "").strip():
        penalty += 30.0
    return penalty


def identifier_candidates(record: BookRecord, embedded: EmbeddedMeta) -> set[str]:
    values: set[str] = set()
    for candidate in (
        record.isbn_candidates
        + record.asin_candidates
        + list(record.all_identifiers.values())
        + list(embedded.embedded_identifiers.values())
    ):
        cleaned = clean_isbn(str(candidate or ""))
        if cleaned:
            values.add(cleaned)
    return values


def edition_decision_payload(edition: Optional[HardcoverEdition]) -> Dict[str, Any]:
    if not edition:
        return {
            "suggested_hardcover_edition_id": "",
            "suggested_hardcover_edition_title": "",
            "suggested_hardcover_edition_format": "",
            "suggested_hardcover_reading_format": "",
            "suggested_hardcover_edition_format_raw": "",
            "suggested_hardcover_edition_format_normalized": "",
            "suggested_hardcover_edition_is_ebookish": False,
            "suggested_hardcover_edition_language": "",
        }
    return {
        "suggested_hardcover_edition_id": str(edition.id),
        "suggested_hardcover_edition_title": edition.title or "",
        "suggested_hardcover_edition_format": edition.edition_format or edition.reading_format or "",
        "suggested_hardcover_reading_format": edition.reading_format or "",
        "suggested_hardcover_edition_format_raw": edition.edition_format or "",
        "suggested_hardcover_edition_format_normalized": normalize_edition_format(
            edition.edition_format,
            edition.reading_format,
        ),
        "suggested_hardcover_edition_is_ebookish": bool(is_ebookish_edition(edition)),
        "suggested_hardcover_edition_language": edition.language or "",
    }


def edition_reason_parts(
    *,
    record: BookRecord,
    file_work: FileWork,
    embedded: EmbeddedMeta,
    book: HardcoverBook,
    edition: HardcoverEdition,
    id_match: int,
    non_audio: int,
    language_ok: int,
    unknown_language: int,
    default_rank: int,
    ebook_pref: int,
    type_rank: int,
    explicit_english: int,
    author_match: int,
    clean_title_match: int,
    no_collection: int,
    no_marketing: int,
    clean_title_bonus: int,
) -> List[str]:
    parts: List[str] = []
    if id_match:
        parts.append("identifier_match")
    if clean_title_match:
        parts.append("title_exact")
    elif clean_title_for_matching(edition.title or book.title) == clean_title_for_matching(record.calibre_title):
        parts.append("title_matches_calibre")
    if author_match >= 2:
        parts.append("primary_author_exact")
    elif author_match == 1:
        parts.append("primary_author_partial")
    else:
        parts.append(explain_author_mismatch(file_work.authors or record.calibre_authors, edition.authors or book.authors))
    if ebook_pref:
        parts.append("edition_preferred_ebookish")
    elif type_rank == 1:
        parts.append("edition_reading_format_read")
    if non_audio:
        parts.append("not_audiobook")
    if language_ok:
        parts.append("language_ok")
    elif unknown_language:
        parts.append("language_unknown")
    else:
        parts.append("language_non_english_or_mismatch")
    if explicit_english:
        parts.append("language_english")
    if default_rank:
        if book.default_ebook_edition_id and int(edition.id) == int(book.default_ebook_edition_id):
            parts.append("matches_hardcover_default_ebook")
        else:
            parts.append("default_edition_boost")
    elif book.default_ebook_edition_id and (record.file_format or "").upper() in set(PREFERRED_FORMATS):
        parts.append("differs_from_hardcover_default_ebook")
    if no_collection:
        parts.append("not_collectionish")
    else:
        parts.append("collectionish_deprioritized")
    if no_marketing:
        parts.append("clean_title")
    else:
        parts.append("marketing_title_penalty")
    if clean_title_bonus:
        parts.append("edition_title_clean")
    penalty = edition_side_material_penalty(edition, file_work, book)
    if penalty >= 150:
        parts.append("non_prose_or_adaptation_penalty")
    elif penalty >= 60:
        parts.append("collectionish_penalty")
    elif penalty > 0:
        parts.append("metadata_quality_penalty")
    return parts


def edition_reason_text(**kwargs: Any) -> str:
    return "; ".join(edition_reason_parts(**kwargs))


def edition_type_rank(edition: HardcoverEdition) -> int:
    reading = norm(edition.reading_format)
    if is_ebookish_edition(edition):
        return 2
    if reading == "read":
        return 1
    return 0


def preferred_default_edition_id(record: BookRecord, book: HardcoverBook) -> int:
    prefers_ebook = (record.file_format or "").upper() in set(PREFERRED_FORMATS)
    if prefers_ebook and book.default_ebook_edition_id:
        return int(book.default_ebook_edition_id)
    if (not prefers_ebook) and book.default_physical_edition_id:
        return int(book.default_physical_edition_id)
    if book.default_cover_edition_id:
        return int(book.default_cover_edition_id)
    if book.default_ebook_edition_id:
        return int(book.default_ebook_edition_id)
    if book.default_physical_edition_id:
        return int(book.default_physical_edition_id)
    return 0


def edition_default_rank(record: BookRecord, book: HardcoverBook, edition: HardcoverEdition) -> int:
    preferred_default = preferred_default_edition_id(record, book)
    if preferred_default and int(edition.id) == int(preferred_default):
        return 2
    if book.default_cover_edition_id and int(edition.id) == int(book.default_cover_edition_id):
        return 1
    return 0


def edition_author_match_rank(
    edition: HardcoverEdition,
    file_work: FileWork,
    book: HardcoverBook,
) -> int:
    edition_authors = author_match_set(candidate_author_string(book, edition))
    target_authors = author_match_set(file_work.authors or candidate_author_string(book, None))
    if not edition_authors or not target_authors:
        return 0
    canonical_edition = set(canonical_author_set(candidate_author_string(book, edition)))
    canonical_target = set(canonical_author_set(file_work.authors or candidate_author_string(book, None)))
    if canonical_edition and canonical_target and canonical_edition == canonical_target:
        return 2
    if edition_authors & target_authors:
        return 1
    return 0


def edition_review_score(
    *,
    id_match: int,
    non_audio: int,
    language_ok: int,
    unknown_language: int,
    default_rank: int,
    ebook_pref: int,
    type_rank: int,
    explicit_english: int,
    author_match: int,
    clean_title_match: int,
    no_collection: int,
    no_marketing: int,
    clean_title_bonus: int,
    edition: HardcoverEdition,
    file_work: FileWork,
    book: HardcoverBook,
) -> float:
    score = 0.0
    score += id_match * 1200.0
    score += non_audio * 260.0
    score += language_ok * 180.0
    score += explicit_english * 80.0
    score += ebook_pref * 100.0
    score += type_rank * 40.0
    score += default_rank * 70.0
    score += author_match * 130.0
    score += clean_title_match * 220.0
    score += no_collection * 15.0
    score += no_marketing * 20.0
    score -= unknown_language * 25.0
    score += min(6.0, float(edition.score or 0) / 250.0)
    score += min(3.0, float(edition.users_read_count or 0) / 50000.0)
    score += min(2.0, float(edition.users_count or 0) / 50000.0)
    score += min(2.0, float(edition.rating or 0.0) / 2.5)
    score += min(1.0, float(edition.lists_count or 0) / 5000.0)
    score += clean_title_bonus * 4.0
    score -= edition_side_material_penalty(edition, file_work, book)
    return round(score, 3)


def rank_candidate_editions(
    record: BookRecord,
    file_work: FileWork,
    embedded: EmbeddedMeta,
    book: HardcoverBook,
    editions: List[HardcoverEdition],
) -> List[Tuple[Tuple[Any, ...], float, str, HardcoverEdition]]:
    if not editions:
        return []
    identifiers = identifier_candidates(record, embedded)
    file_clean = clean_title_for_matching(file_work.title or book.title)
    prefers_ebook = (record.file_format or "").upper() in set(PREFERRED_FORMATS)
    ranked: List[Tuple[Tuple[Any, ...], float, str, HardcoverEdition]] = []
    fallback: List[Tuple[Tuple[Any, ...], float, str, HardcoverEdition]] = []
    for edition in editions:
        type_rank = edition_type_rank(edition)
        language_ok = edition_language_ok_rank(edition)
        unknown_language = edition_unknown_language_rank(edition)
        explicit_english = edition_explicit_english_rank(edition)
        clean_title_match = 1 if clean_title_for_matching(edition.title or book.title) == file_clean else 0
        clean_title_bonus = 1 if smart_title(edition.title or "") == clean_title_for_matching(edition.title or "") else 0
        local_kind = classify_local_file_kind(file_work, record, record.file_format)
        candidate_kind = classify_hardcover_edition(edition, book)
        kind_ok = 1 if work_kind_penalty(local_kind, candidate_kind) == 0 else 0
        id_match = 1 if any(
            identifier
            and identifier in {clean_isbn(edition.isbn_10), clean_isbn(edition.isbn_13), clean_isbn(edition.asin)}
            for identifier in identifiers
        ) else 0
        non_audio = 1 if not is_audio_edition(edition) else 0
        ebook_pref = 1 if prefers_ebook and is_ebookish_edition(edition) else 0
        default_rank = edition_default_rank(record, book, edition)
        author_match = edition_author_match_rank(edition, file_work, book)
        no_collection = 1 if not is_collectionish_edition(edition) else 0
        no_marketing = 1 if title_marketing_penalty(" ".join([edition.title or "", edition.subtitle or ""]).strip()) == 0 else 0
        rank = (
            id_match,
            kind_ok,
            clean_title_match,
            author_match,
            ebook_pref,
            non_audio,
            language_ok,
            explicit_english,
            default_rank,
            unknown_language,
            no_collection,
            no_marketing,
            int(edition.score or 0),
            int(edition.users_read_count or 0),
            int(edition.users_count or 0),
            int(round((edition.rating or 0.0) * 100)),
            int(edition.lists_count or 0),
            clean_title_bonus,
            edition.release_date or "",
        )
        review_score = edition_review_score(
            id_match=id_match,
            non_audio=non_audio,
            language_ok=language_ok,
            unknown_language=unknown_language,
            default_rank=default_rank,
            ebook_pref=ebook_pref,
            type_rank=type_rank,
            explicit_english=explicit_english,
            author_match=author_match,
            clean_title_match=clean_title_match,
            no_collection=no_collection,
            no_marketing=no_marketing,
            clean_title_bonus=clean_title_bonus,
            edition=edition,
            file_work=file_work,
            book=book,
        )
        reason = edition_reason_text(
            record=record,
            file_work=file_work,
            embedded=embedded,
            book=book,
            edition=edition,
            id_match=id_match,
            non_audio=non_audio,
            language_ok=language_ok,
            unknown_language=unknown_language,
            default_rank=default_rank,
            ebook_pref=ebook_pref,
            type_rank=type_rank,
            explicit_english=explicit_english,
            author_match=author_match,
            clean_title_match=clean_title_match,
            no_collection=no_collection,
            no_marketing=no_marketing,
            clean_title_bonus=clean_title_bonus,
        )
        item = (rank, review_score, reason, edition)
        if kind_ok and non_audio and type_rank > 0 and language_ok:
            ranked.append(item)
        else:
            fallback.append(item)
    pool = ranked or fallback
    pool.sort(key=lambda item: (item[1], item[0]), reverse=True)
    return pool


def choose_preferred_edition_info(
    record: BookRecord,
    file_work: FileWork,
    embedded: EmbeddedMeta,
    book: HardcoverBook,
    editions: List[HardcoverEdition],
) -> EditionChoiceInfo:
    ranked = rank_candidate_editions(record, file_work, embedded, book, editions)
    if not ranked:
        return EditionChoiceInfo()
    _chosen_rank, chosen_score, chosen_reason, chosen = ranked[0]
    runner_up = ranked[1][3] if len(ranked) > 1 else None
    runner_up_score = float(ranked[1][1]) if len(ranked) > 1 else 0.0
    runner_up_reason = str(ranked[1][2]) if len(ranked) > 1 else ""
    default_ebook = None
    default_ebook_score = 0.0
    default_ebook_reason = ""
    default_ebook_gap = 0.0
    if book.default_ebook_edition_id:
        for _rank, score, reason, edition in ranked:
            if int(edition.id) == int(book.default_ebook_edition_id):
                default_ebook = edition
                default_ebook_score = float(score)
                default_ebook_reason = str(reason)
                default_ebook_gap = round(float(chosen_score) - float(score), 3)
                break
    return EditionChoiceInfo(
        chosen=chosen,
        runner_up=runner_up,
        default_ebook=default_ebook,
        chosen_score=float(chosen_score),
        runner_up_score=runner_up_score,
        default_ebook_score=default_ebook_score,
        score_gap=round(float(chosen_score) - runner_up_score, 3),
        chosen_vs_default_ebook_score_gap=default_ebook_gap,
        count_considered=len(ranked),
        chosen_reason=chosen_reason,
        runner_up_reason=runner_up_reason,
        default_ebook_reason=default_ebook_reason,
        chosen_matches_default_ebook=bool(default_ebook and chosen and int(default_ebook.id) == int(chosen.id)),
    )


def choose_preferred_edition(
    record: BookRecord,
    file_work: FileWork,
    embedded: EmbeddedMeta,
    book: HardcoverBook,
    editions: List[HardcoverEdition],
) -> Optional[HardcoverEdition]:
    return choose_preferred_edition_info(record, file_work, embedded, book, editions).chosen


def effective_candidate_authors(
    book: Optional[HardcoverBook],
    preferred_edition: Optional[HardcoverEdition],
) -> str:
    if preferred_edition and getattr(preferred_edition, "authors", "") and not is_audio_edition(preferred_edition):
        return preferred_edition.authors
    if book and getattr(book, "authors", ""):
        return book.authors
    return ""


def book_selection_adjusted_score(
    raw_score: float,
    file_work: FileWork,
    book: HardcoverBook,
    preferred_edition: Optional[HardcoverEdition],
) -> float:
    score = float(raw_score)
    score -= title_marketing_penalty(book.title)
    if preferred_edition:
        reading = norm(preferred_edition.reading_format)
        score += 2.0 if is_ebookish_edition(preferred_edition) else 1.0 if reading == "read" else 0.0
        if edition_language_ok_rank(preferred_edition):
            score += 1.0
        elif edition_unknown_language_rank(preferred_edition):
            score -= 1.0
        else:
            score -= 3.0
        score += 0.5 if edition_explicit_english_rank(preferred_edition) else 0.0
        if book.default_ebook_edition_id and int(preferred_edition.id) == int(book.default_ebook_edition_id):
            score += 0.75
        score += min(2.5, (preferred_edition.score or 0) / 1000.0)
        score += min(1.5, (preferred_edition.users_count or 0) / 100.0)
        if clean_title_for_matching(preferred_edition.title or book.title) == clean_title_for_matching(
            file_work.title or book.title
        ):
            score += 1.5
        if smart_title(preferred_edition.title or "") == clean_title_for_matching(preferred_edition.title or ""):
            score += 0.75
        if is_collectionish_edition(preferred_edition):
            score -= 2.0
        if is_audio_edition(preferred_edition):
            score -= 4.0
    score += min(1.0, (book.users_read_count or 0) / 1000.0)
    return round(score, 2)
