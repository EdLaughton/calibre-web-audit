from __future__ import annotations

from typing import Optional, Tuple

from .matching import author_coverage, bare_title_similarity, contributor_count, title_marketing_penalty
from .models import BookRecord, FileWork, HardcoverBook, HardcoverEdition, MatchScores
from .text_normalization import clean_title_for_matching, norm, primary_author, smart_title, split_author_like_string, strip_series_suffix
from .work_classification import classify_hardcover_book, classify_local_file_kind, title_normalization_candidate, work_kind_penalty


def _edition_is_audio(edition: Optional[HardcoverEdition]) -> bool:
    if not edition:
        return False
    return bool((edition.audio_seconds or 0) > 0 or 'audio' in norm(edition.reading_format) or 'audio' in norm(edition.edition_format))


def candidate_primary_author_list(book: Optional[HardcoverBook], edition: Optional[HardcoverEdition] = None) -> list[str]:
    if edition and getattr(edition, 'primary_authors', None) and not _edition_is_audio(edition):
        return [name for name in (edition.primary_authors or []) if name]
    if book and getattr(book, 'primary_authors', None):
        return [name for name in (book.primary_authors or []) if name]
    authors = ''
    if edition and getattr(edition, 'authors', '') and not _edition_is_audio(edition):
        authors = edition.authors
    elif book and getattr(book, 'authors', ''):
        authors = book.authors
    return split_author_like_string(authors)


def candidate_author_string(book: Optional[HardcoverBook], edition: Optional[HardcoverEdition] = None) -> str:
    names = candidate_primary_author_list(book, edition)
    if names:
        return ' & '.join(names)
    if edition and getattr(edition, 'authors', '') and not _edition_is_audio(edition):
        return edition.authors
    if book and getattr(book, 'authors', ''):
        return book.authors
    return ''


def score_work_candidate(
    file_work: FileWork,
    record: BookRecord,
    hardcover_book: HardcoverBook,
    preferred_edition: Optional[HardcoverEdition] = None,
) -> Tuple[float, MatchScores, str]:
    title_score = 0.0
    if file_work.title:
        title_score = max(
            bare_title_similarity(file_work.title, hardcover_book.title),
            bare_title_similarity(strip_series_suffix(file_work.title), hardcover_book.title),
            bare_title_similarity(clean_title_for_matching(file_work.title), hardcover_book.title),
        )
    if title_score == 0 and record.calibre_title:
        title_score = max(
            bare_title_similarity(record.calibre_title, hardcover_book.title) * 0.7,
            bare_title_similarity(clean_title_for_matching(record.calibre_title), hardcover_book.title) * 0.8,
        )

    candidate_authors = candidate_author_string(hardcover_book, preferred_edition)
    effective_file_authors = file_work.authors or record.calibre_authors
    author_score = 0.0
    if file_work.authors:
        full_author = author_coverage(file_work.authors, candidate_authors)
        primary = author_coverage(primary_author(file_work.authors), candidate_authors)
        author_score = max(full_author, primary * (0.95 if file_work.authors_basis == 'calibre_fallback' else 0.9))
    if author_score == 0 and record.calibre_authors:
        author_score = author_coverage(record.calibre_authors, candidate_authors) * 0.6

    series_score = 10.0 if (
        record.calibre_series and hardcover_book.series and norm(record.calibre_series) in norm(hardcover_book.series)
    ) else 0.0

    total = title_score * 72 + author_score * 24 + series_score - title_marketing_penalty(hardcover_book.title)

    candidate_contributors = contributor_count(candidate_authors)
    file_contributors = contributor_count(effective_file_authors)
    no_author_overlap = bool(effective_file_authors) and author_coverage(effective_file_authors, candidate_authors) == 0.0

    if no_author_overlap:
        total -= 30.0
        if candidate_contributors >= max(3, file_contributors + 2):
            total -= min(15.0, 3.0 * (candidate_contributors - max(1, file_contributors)))
    elif effective_file_authors and candidate_contributors >= 3 and file_contributors <= 2 and author_score < 0.55:
        total -= 7.5

    if preferred_edition and _edition_is_audio(preferred_edition) and author_score < 0.98:
        total -= 15.0
    if preferred_edition and not str(preferred_edition.language or '').strip():
        total -= 6.0
    if preferred_edition and hardcover_book.default_ebook_edition_id and int(preferred_edition.id) == int(hardcover_book.default_ebook_edition_id):
        total += 1.0

    local_kind = classify_local_file_kind(file_work, record, record.file_format)
    candidate_kind = classify_hardcover_book(hardcover_book, preferred_edition)
    kind_penalty = work_kind_penalty(local_kind, candidate_kind)
    total -= kind_penalty
    if title_normalization_candidate(record.calibre_title, hardcover_book.title) and author_score >= 0.90:
        total += 4.0

    total = round(total, 2)
    reasons = []
    if title_score >= 0.98:
        reasons.append('exact-title')
    elif title_score >= 0.90:
        reasons.append('close-title')
    elif title_score >= 0.75:
        reasons.append('partial-title')
    if author_score >= 0.99:
        reasons.append('author')
    elif author_score >= 0.50:
        reasons.append('partial-author')
    elif no_author_overlap and effective_file_authors:
        reasons.append('author-mismatch')
    if series_score:
        reasons.append('series')
    if preferred_edition and _edition_is_audio(preferred_edition):
        reasons.append('audio-edition')
    if title_normalization_candidate(record.calibre_title, hardcover_book.title):
        reasons.append('decorated-title')
    if kind_penalty >= 50:
        reasons.append(f'kind-conflict:{candidate_kind}')
    elif candidate_kind and candidate_kind != 'unknown':
        reasons.append(f'kind:{candidate_kind}')
    return total, MatchScores(round(title_score, 3), round(author_score, 3), round(series_score, 3), total), ','.join(reasons)


def is_strong_work_match(
    *,
    score: float,
    breakdown: MatchScores,
    why: str,
    book: Optional[HardcoverBook],
    edition: Optional[HardcoverEdition],
    file_work: FileWork,
    record: BookRecord,
) -> bool:
    if not book:
        return False
    candidate_kind = classify_hardcover_book(book, edition)
    local_kind = classify_local_file_kind(file_work, record, record.file_format)
    if work_kind_penalty(local_kind, candidate_kind) >= 50.0:
        return False
    if score < 85.0:
        return False
    if breakdown.author_score < 0.90:
        return False
    if breakdown.title_score >= 0.95:
        return True
    if breakdown.title_score >= 0.90 and 'close-title' in why:
        return True
    return False


def is_tolerable_work_match(
    *,
    score: float,
    breakdown: MatchScores,
    why: str,
    book: Optional[HardcoverBook],
    edition: Optional[HardcoverEdition],
    file_work: FileWork,
    record: BookRecord,
) -> bool:
    if not book:
        return False
    candidate_kind = classify_hardcover_book(book, edition)
    local_kind = classify_local_file_kind(file_work, record, record.file_format)
    if work_kind_penalty(local_kind, candidate_kind) >= 50.0:
        return False
    if score < 78.0:
        return False
    if breakdown.title_score >= 0.85 and breakdown.author_score >= 0.85:
        return True
    if breakdown.title_score >= 0.70 and breakdown.author_score >= 0.95:
        return True
    if title_normalization_candidate(record.calibre_title, book.title) and breakdown.author_score >= 0.95:
        return True
    return False
