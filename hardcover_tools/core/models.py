from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class BookRecord:
    calibre_book_id: int
    calibre_title: str
    calibre_authors: str
    calibre_series: str
    calibre_series_index: Optional[float]
    calibre_language: str
    calibre_hardcover_id: str
    calibre_hardcover_slug: str
    calibre_hardcover_edition_id: str = ""
    file_path: str = ""
    file_format: str = ""
    all_identifiers: Dict[str, str] = field(default_factory=dict)
    isbn_candidates: List[str] = field(default_factory=list)
    asin_candidates: List[str] = field(default_factory=list)


@dataclass
class EmbeddedMeta:
    embedded_title: str = ""
    embedded_authors: str = ""
    embedded_language: str = ""
    embedded_identifiers: Dict[str, str] = field(default_factory=dict)
    raw: str = ""
    tool_used: str = ""
    parse_error: str = ""


@dataclass
class ContentSignals:
    inferred_title_from_content: str = ""
    inferred_author_from_content: str = ""
    inferred_language_from_content: str = ""
    content_title_match_strength: float = 0.0
    content_author_match_strength: float = 0.0
    first_heading_excerpt: str = ""
    extracted_sample_len: int = 0
    extractor: str = ""
    language_confidence: float = 0.0


@dataclass
class FileWork:
    title: str = ""
    authors: str = ""
    language: str = ""
    title_basis: str = ""
    authors_basis: str = ""


@dataclass
class HardcoverContribution:
    author_name: str
    contribution: str = ""
    normalized_role: str = ""
    is_primary: bool = False


@dataclass
class HardcoverSeriesMembership:
    series_id: int = 0
    series_name: str = ""
    series_slug: str = ""
    position: Optional[float] = None
    details: str = ""
    featured: bool = False
    compilation: bool = False
    raw_series_id: int = 0
    raw_series_name: str = ""
    is_completed: Optional[bool] = None
    books_count: Optional[int] = None
    primary_books_count: Optional[int] = None


@dataclass
class HardcoverBook:
    id: int
    title: str
    subtitle: str
    authors: str
    series: str
    release_date: str
    slug: str
    users_count: int = 0
    users_read_count: int = 0
    rating: float = 0.0
    lists_count: int = 0
    default_ebook_edition_id: int = 0
    default_physical_edition_id: int = 0
    default_audio_edition_id: int = 0
    default_cover_edition_id: int = 0
    contributions: List[HardcoverContribution] = field(default_factory=list)
    primary_authors: List[str] = field(default_factory=list)
    secondary_contributors: List[str] = field(default_factory=list)
    narrators: List[str] = field(default_factory=list)
    adapters: List[str] = field(default_factory=list)
    illustrators: List[str] = field(default_factory=list)
    editors: List[str] = field(default_factory=list)
    translators: List[str] = field(default_factory=list)
    series_memberships: List[HardcoverSeriesMembership] = field(default_factory=list)
    canonical_id: int = 0
    compilation: Optional[bool] = None
    is_partial_book: Optional[bool] = None
    parent_book_id: int = 0
    literary_type_id: int = 0
    work_kind: str = "unknown"


@dataclass
class HardcoverEdition:
    id: int
    book_id: int
    title: str
    subtitle: str
    authors: str = ""
    score: int = 0
    rating: float = 0.0
    users_count: int = 0
    users_read_count: int = 0
    lists_count: int = 0
    release_date: str = ""
    isbn_10: str = ""
    isbn_13: str = ""
    asin: str = ""
    audio_seconds: Optional[int] = None
    physical_format: str = ""
    edition_format: str = ""
    reading_format: str = ""
    language: str = ""
    contributions: List[HardcoverContribution] = field(default_factory=list)
    primary_authors: List[str] = field(default_factory=list)
    secondary_contributors: List[str] = field(default_factory=list)
    narrators: List[str] = field(default_factory=list)
    adapters: List[str] = field(default_factory=list)
    illustrators: List[str] = field(default_factory=list)
    editors: List[str] = field(default_factory=list)
    translators: List[str] = field(default_factory=list)
    reading_format_id: int = 0
    language_id: int = 0
    work_kind: str = "unknown"


@dataclass
class MatchScores:
    title_score: float = 0.0
    author_score: float = 0.0
    series_score: float = 0.0
    total_score: float = 0.0


@dataclass
class EditionChoiceInfo:
    chosen: Optional[HardcoverEdition] = None
    runner_up: Optional[HardcoverEdition] = None
    default_ebook: Optional[HardcoverEdition] = None
    chosen_score: float = 0.0
    runner_up_score: float = 0.0
    default_ebook_score: float = 0.0
    score_gap: float = 0.0
    chosen_vs_default_ebook_score_gap: float = 0.0
    count_considered: int = 0
    chosen_reason: str = ""
    runner_up_reason: str = ""
    default_ebook_reason: str = ""
    chosen_matches_default_ebook: bool = False


@dataclass
class Decision:
    action: str
    confidence_score: float
    confidence_tier: str
    reason: str
    issue_category: str
    suggested_calibre_title: str = ""
    suggested_calibre_authors: str = ""
    suggested_hardcover_id: str = ""
    suggested_hardcover_slug: str = ""
    suggested_hardcover_edition_id: str = ""
    suggested_hardcover_edition_title: str = ""
    suggested_hardcover_edition_format: str = ""
    suggested_hardcover_reading_format: str = ""
    suggested_hardcover_edition_format_raw: str = ""
    suggested_hardcover_edition_format_normalized: str = ""
    suggested_hardcover_edition_is_ebookish: bool = False
    suggested_hardcover_edition_language: str = ""
    fix_basis: str = ""


@dataclass
class AuditRow:
    calibre_book_id: int
    calibre_title: str
    calibre_authors: str
    calibre_series: str
    calibre_series_index: Optional[float]
    calibre_language: str
    calibre_hardcover_id: str
    calibre_hardcover_slug: str
    current_hardcover_edition_id: str = ""
    file_path: str = ""
    file_format: str = ""
    file_work_title: str = ""
    file_work_authors: str = ""
    file_work_title_basis: str = ""
    file_work_authors_basis: str = ""
    embedded_title: str = ""
    embedded_authors: str = ""
    embedded_language: str = ""
    inferred_title_from_content: str = ""
    inferred_author_from_content: str = ""
    inferred_language_from_content: str = ""
    hardcover_candidate_id: str = ""
    hardcover_title: str = ""
    hardcover_authors: str = ""
    hardcover_series: str = ""
    hardcover_slug: str = ""
    current_hardcover_title: str = ""
    current_hardcover_authors: str = ""
    suggested_hardcover_title: str = ""
    suggested_hardcover_authors: str = ""
    preferred_edition_id: str = ""
    preferred_edition_title: str = ""
    preferred_edition_reading_format: str = ""
    preferred_edition_edition_format: str = ""
    preferred_edition_format_normalized: str = ""
    preferred_edition_is_ebookish: bool = False
    preferred_edition_language: str = ""
    preferred_edition_reason: str = ""
    preferred_edition_score: int = 0
    preferred_edition_users_count: int = 0
    preferred_edition_users_read_count: int = 0
    preferred_edition_rating: float = 0.0
    preferred_edition_lists_count: int = 0
    preferred_edition_release_date: str = ""
    runner_up_edition_id: str = ""
    runner_up_edition_title: str = ""
    runner_up_edition_reading_format: str = ""
    runner_up_edition_edition_format: str = ""
    runner_up_edition_format_normalized: str = ""
    runner_up_edition_is_ebookish: bool = False
    runner_up_edition_language: str = ""
    runner_up_edition_reason: str = ""
    runner_up_edition_score: int = 0
    runner_up_edition_users_count: int = 0
    runner_up_edition_users_read_count: int = 0
    runner_up_edition_rating: float = 0.0
    runner_up_edition_lists_count: int = 0
    runner_up_edition_release_date: str = ""
    default_ebook_edition_id: str = ""
    default_ebook_edition_title: str = ""
    default_ebook_edition_reading_format: str = ""
    default_ebook_edition_edition_format: str = ""
    default_ebook_edition_format_normalized: str = ""
    default_ebook_edition_language: str = ""
    default_ebook_edition_reason: str = ""
    default_ebook_edition_score: float = 0.0
    preferred_matches_default_ebook: bool = False
    preferred_vs_default_ebook_score_gap: float = 0.0
    edition_choice_score: float = 0.0
    edition_runner_up_score: float = 0.0
    edition_choice_score_gap: float = 0.0
    edition_candidates_considered: int = 0
    confidence_score: float = 0.0
    confidence_tier: str = ""
    recommended_action: str = ""
    reason: str = ""
    issue_category: str = ""
    matched_by: str = ""
    current_hardcover_match_ok: str = ""
    first_heading_excerpt: str = ""
    ebook_meta_tool_used: str = ""
    file_vs_calibre_title_score: float = 0.0
    file_vs_calibre_authors_score: float = 0.0
    file_vs_current_title_score: float = 0.0
    file_vs_current_authors_score: float = 0.0
    best_title_score: float = 0.0
    best_author_score: float = 0.0
    best_series_score: float = 0.0
    best_total_score: float = 0.0
    suggested_calibre_title: str = ""
    suggested_calibre_authors: str = ""
    suggested_hardcover_id: str = ""
    suggested_hardcover_slug: str = ""
    suggested_hardcover_edition_id: str = ""
    suggested_hardcover_edition_title: str = ""
    suggested_hardcover_edition_format: str = ""
    suggested_hardcover_reading_format: str = ""
    suggested_hardcover_edition_format_raw: str = ""
    suggested_hardcover_edition_format_normalized: str = ""
    suggested_hardcover_edition_is_ebookish: bool = False
    suggested_hardcover_edition_language: str = ""
    calibre_author_normalized: str = ""
    file_author_normalized: str = ""
    hardcover_primary_author_normalized: str = ""
    author_mismatch_reason: str = ""
    same_hardcover_id_as_suggestion: bool = False
    embedded_title_mismatch_to_calibre: bool = False
    embedded_authors_mismatch_to_calibre_text: bool = False
    embedded_authors_mismatch_to_calibre_canonical: bool = False
    embedded_title_mismatch_to_suggested: bool = False
    embedded_authors_mismatch_to_suggested_text: bool = False
    embedded_authors_mismatch_to_suggested_canonical: bool = False
    embedded_calibre_mismatch_summary: str = ""
    fix_basis: str = ""


@dataclass
class HardcoverRequestMeta:
    label: str = ""
    transport: str = ""
    status_code: int = 0
    duration_s: float = 0.0
    attempt: int = 0
    from_cache: bool = False
    retry_after: str = ""
    rate_limit_limit: str = ""
    rate_limit_remaining: str = ""
    rate_limit_reset: str = ""
    error_summary: str = ""
    cache_key: str = ""
    cache_detail: str = ""
