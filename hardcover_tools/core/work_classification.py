from __future__ import annotations

import re
from typing import Optional

from .models import BookRecord, FileWork, HardcoverBook, HardcoverContribution, HardcoverEdition
from .text_normalization import norm, smart_title

GRAPHIC_PATTERNS = re.compile(r"\b(graphic\s+novel|graphic|comic(?:s)?|manga|adaptation|adapted|illustrated)\b", re.I)
COLLECTION_PATTERNS = re.compile(r"\b(omnibus|boxed?\s+set|collection|collected|anthology|bundle|complete\s+(?:works|novels|series|trilogy|saga|stories)|contains\s+books?)\b", re.I)
COMPANION_PATTERNS = re.compile(r"\b(companion|guide|encyclopedia|annotated|screenplay|script|handbook|atlas|art\s+book)\b", re.I)
PARTIAL_PATTERNS = re.compile(r"\b(excerpt|sampler|preview|part\s+one|part\s+1|episode|serial)\b", re.I)
PRIMARY_ROLE_PATTERNS = ("author", "writer", "written by", "story by", "creator")
ADAPTATION_ROLE_PATTERNS = ("adapt", "screenplay", "script")
VISUAL_ROLE_PATTERNS = ("illustrat", "inker", "penc", "artist", "cover")
EDITORIAL_ROLE_PATTERNS = ("edit", "foreword", "afterword", "introduction", "annotat", "comment")
AUDIO_ROLE_PATTERNS = ("narrat", "reader", "read by", "perform")
TRANSLATION_ROLE_PATTERNS = ("translat",)


def _text_blob(*values: str) -> str:
    return " ".join(smart_title(value or "") for value in values if smart_title(value or "")).strip()


def _role_matches(contribution: HardcoverContribution, patterns: tuple[str, ...]) -> bool:
    role = contribution.normalized_role or norm(contribution.contribution or "")
    return any(token in role for token in patterns)


def _has_role(contributions: list[HardcoverContribution], patterns: tuple[str, ...]) -> bool:
    return any(_role_matches(contribution, patterns) for contribution in contributions or [])


def primary_contributor_names(contributions: list[HardcoverContribution]) -> list[str]:
    names = [c.author_name for c in contributions or [] if c.is_primary and c.author_name]
    if names:
        return names
    return [c.author_name for c in contributions or [] if c.author_name]


def local_file_title(record: Optional[BookRecord], file_work: FileWork) -> str:
    return smart_title(file_work.title or (record.calibre_title if record else ""))


def classify_local_file_kind(file_work: FileWork, record: Optional[BookRecord] = None, file_format: str = "") -> str:
    title = local_file_title(record, file_work)
    text = _text_blob(title, file_work.authors)
    format_norm = norm(file_format)
    if "audio" in format_norm:
        return "audiobook"
    if COLLECTION_PATTERNS.search(text):
        return "anthology_collection"
    if GRAPHIC_PATTERNS.search(text):
        return "graphic_adaptation"
    if COMPANION_PATTERNS.search(text):
        return "companion_reference"
    if PARTIAL_PATTERNS.search(text):
        return "partial_work"
    return "prose_novel"


def classify_hardcover_book(book: Optional[HardcoverBook], preferred_edition: Optional[HardcoverEdition] = None) -> str:
    if not book and not preferred_edition:
        return "unknown"
    contributions = list(getattr(preferred_edition, 'contributions', []) or []) + list(getattr(book, 'contributions', []) or [])
    text = _text_blob(
        getattr(preferred_edition, 'title', ''),
        getattr(preferred_edition, 'subtitle', ''),
        getattr(book, 'title', ''),
        getattr(book, 'subtitle', ''),
    )
    if preferred_edition and ((preferred_edition.audio_seconds or 0) > 0 or 'audio' in norm(preferred_edition.reading_format) or 'audio' in norm(preferred_edition.edition_format)):
        return 'audiobook'
    if getattr(book, 'compilation', None) is True or COLLECTION_PATTERNS.search(text):
        return 'anthology_collection'
    if getattr(book, 'is_partial_book', None) is True or getattr(book, 'parent_book_id', 0):
        return 'partial_work'
    if PARTIAL_PATTERNS.search(text):
        return 'partial_work'
    if _has_role(contributions, AUDIO_ROLE_PATTERNS):
        return 'audiobook'
    if _has_role(contributions, ADAPTATION_ROLE_PATTERNS) or _has_role(contributions, VISUAL_ROLE_PATTERNS) or GRAPHIC_PATTERNS.search(text):
        return 'graphic_adaptation'
    if COMPANION_PATTERNS.search(text) or _has_role(contributions, EDITORIAL_ROLE_PATTERNS):
        return 'companion_reference'
    return 'prose_novel'


def classify_hardcover_edition(edition: Optional[HardcoverEdition], book: Optional[HardcoverBook] = None) -> str:
    if not edition:
        return classify_hardcover_book(book, None)
    kind = classify_hardcover_book(book, edition)
    return kind


def work_kinds_compatible(local_kind: str, candidate_kind: str) -> bool:
    local = norm(local_kind)
    candidate = norm(candidate_kind)
    if not candidate or candidate == 'unknown':
        return True
    if local == candidate:
        return True
    compatibility = {
        'prose novel': {'novella', 'short fiction', 'unknown'},
        'novella': {'prose novel', 'short fiction', 'unknown'},
        'anthology collection': {'omnibus', 'unknown'},
        'graphic adaptation': {'comic collection', 'unknown'},
        'companion reference': {'unknown'},
        'partial work': {'unknown'},
        'audiobook': {'unknown'},
    }
    return candidate in compatibility.get(local, {'unknown'})


def work_kind_penalty(local_kind: str, candidate_kind: str) -> float:
    local = norm(local_kind)
    candidate = norm(candidate_kind)
    if work_kinds_compatible(local, candidate):
        return 0.0
    hard_conflicts = {
        ('prose novel', 'graphic adaptation'),
        ('prose novel', 'anthology collection'),
        ('prose novel', 'companion reference'),
        ('prose novel', 'partial work'),
        ('prose novel', 'audiobook'),
        ('graphic adaptation', 'prose novel'),
        ('anthology collection', 'prose novel'),
    }
    if (local, candidate) in hard_conflicts:
        return 60.0
    return 25.0


def title_normalization_candidate(calibre_title: str, canonical_title: str) -> bool:
    raw_calibre = smart_title(calibre_title)
    raw_canonical = smart_title(canonical_title)
    from .text_normalization import clean_title_for_matching
    cleaned_calibre = clean_title_for_matching(raw_calibre)
    cleaned_canonical = clean_title_for_matching(raw_canonical)
    if not raw_calibre or not raw_canonical or not cleaned_calibre or not cleaned_canonical:
        return False
    return cleaned_calibre == cleaned_canonical and raw_calibre != raw_canonical
