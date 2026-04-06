
# -*- coding: utf-8 -*-
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import shlex
import shutil
from collections import Counter
import tempfile
import sqlite3
import subprocess
import sys
import time
import unicodedata
import zipfile
from datetime import datetime
from urllib import error as urllib_error, request as urllib_request
from collections import defaultdict
from dataclasses import dataclass, asdict, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set
from xml.etree import ElementTree as ET

from .runtime_defaults import (
    CACHE_FILENAME,
    DEFAULT_CACHE_TTL_HOURS,
    DEFAULT_EDITION_CACHE_TTL_HOURS,
    DEFAULT_EMPTY_CACHE_TTL_HOURS,
    DEFAULT_PROGRESS_EVERY,
    DEFAULT_SEARCH_CACHE_TTL_HOURS,
    HARDCOVER_DEFAULT_USER_AGENT,
    LEGACY_CACHE_FILENAME,
)
from .runtime_io import (
    TeeStream,
    default_output_dir,
    default_output_dir_name,
    ensure_dir,
    find_metadata_db,
    write_csv,
    write_jsonl,
)

GRAPHQL_ENDPOINT = "https://api.hardcover.app/v1/graphql"
QUIET_HC_LABELS = {"book_single", "books", "book_editions", "book_editions_single", "series_books", "book_series_memberships", "editions_by_id", "author_books", "books_and_editions"}
PREFERRED_FORMATS = ["EPUB", "KEPUB", "AZW3", "MOBI", "PDF", "TXT", "DOCX", "HTML", "HTM"]
CONTAINER_NS = {"container": "urn:oasis:names:tc:opendocument:xmlns:container"}

EN_STOPWORDS = {
    "the","and","of","to","a","in","is","it","that","for","you","with","on","as","this","be","are","was",
    "by","or","an","at","from","his","her","their","but","not","have","has","had","he","she","they","we","i",
    "my","me","our","your","who","what","when","where","why","how","said","chapter","prologue","epilogue",
}
DE_STOPWORDS = {"der","die","das","und","ist","nicht","ein","eine","mit","zu","von","den","im","auf","für","sie","ich"}
FR_STOPWORDS = {"le","la","les","et","est","une","un","des","dans","pour","pas","que","qui","sur","avec"}
ES_STOPWORDS = {"el","la","los","las","y","es","una","un","de","que","en","para","con","por","no","como"}

IGNORE_CONTENT_TITLES = {
    "cover", "title page", "copyright", "contents", "toc", "table of contents",
    "full page image", "unknown", "brand page", "dedication", "other titles",
    "front endpapers",
    "part0001", "cover the", "also by", "cover,",
}

SERIES_SUFFIX_PATTERNS = [
    r"\s*\((?:[^)]*book\s+\d+[^)]*)\)\s*$",
    r"\s*\((?:[^)]*series[^)]*)\)\s*$",
    r"\s*\((?:[^)]*trilogy[^)]*)\)\s*$",
    r"\s*\((?:[^)]*chronicles[^)]*)\)\s*$",
    r"\s*\((?:[^)]*files[^)]*)\)\s*$",
    r"\s*\((?:[^)]*saga[^)]*)\)\s*$",
    r"\s*\((?:[^)]*cycle[^)]*)\)\s*$",
    r"\s*\((?:[^)]*world[^)]*)\)\s*$",
    r"\s*\((?:[^)]*verse[^)]*)\)\s*$",
    r"\s*\((?:[^)]*novel[^)]*)\)\s*$",
    r"\s*-\s*[^-]*series[^-]*\s*$",
    r"\s*-\s*[^-]*book\s+\d+[^-]*\s*$",
]
ISBN_SUFFIX_PATTERN = r"\s*\((?:97[89]\d{10}|\d{10})\)\s*$"

MARKETING_SUFFIX_PATTERNS = [
    r"\s*:\s*A Novel\s*$",
    r"\s*\(With Bonus Chapter\)\s*$",
    r"\s*:\s*The World of [^:]+$",
    r"\s*The Brand New Must-Read [^-]+$",
    r"\s*Series By [^-]+ collection Set\s*$",
]

TRAILING_METADATA_KEYWORDS = {
    "book", "series", "trilogy", "chronicles", "files", "saga", "cycle", "world", "verse",
    "bonus chapter", "collection", "collection set", "box set", "omnibus", "edition",
    "must read",
}
YEAR_SUFFIX_PATTERN = r"\s*\((?:19|20)\d{2}\)\s*$"

# Optional, user-supplied alias map. Keep the built-in default empty so the script
# stays generic across different libraries. Expected shape in JSON:
# {
#   "v e schwab": "victoria e schwab",
#   "s a chakraborty": "shannon chakraborty"
# }
AUTHOR_ALIAS_MAP: Dict[str, str] = {}


def load_author_alias_map(path: Optional[Path]) -> Dict[str, str]:
    if not path:
        return {}
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("author aliases JSON must be an object mapping alias -> canonical author")
    out: Dict[str, str] = {}
    for k, v in raw.items():
        nk = norm(str(k or ""))
        nv = norm(str(v or ""))
        if nk and nv:
            out[nk] = nv
    return out


def normalize_author_key(name: str) -> str:
    key = norm(name)
    return AUTHOR_ALIAS_MAP.get(key, key)

def extract_numeric_id(value: str) -> str:
    m = re.search(r"\b(\d{3,})\b", str(value or ""))
    return m.group(1) if m else ""

def clean_title_for_matching(title: str) -> str:
    raw = smart_title(title)
    if not raw:
        return ""
    title = raw
    if title.startswith("[") and title.endswith("]"):
        title = title[1:-1].strip()

    def _tail_has_metadata_keywords(val: str) -> bool:
        v = norm(val)
        if not v:
            return False
        words = set(v.split())
        padded = f" {v} "
        for keyword in TRAILING_METADATA_KEYWORDS:
            k = norm(keyword)
            if not k:
                continue
            if " " in k:
                if f" {k} " in padded:
                    return True
            elif k in words:
                return True
        return (
            bool(re.search(r"\b(?:book|series)\s+\d+\b", v))
            or bool(re.search(r"\b(?:collection\s+set|box\s+set|omnibus|edition)\b", v))
            or bool(re.search(r"\ba\s+novel\b", v))
        )

    changed = True
    while changed and title:
        changed = False
        new_title = re.sub(ISBN_SUFFIX_PATTERN, "", title, flags=re.I).strip()
        if new_title != title:
            title = new_title
            changed = True
            continue
        new_title = re.sub(YEAR_SUFFIX_PATTERN, "", title, flags=re.I).strip()
        if new_title != title:
            title = new_title
            changed = True
            continue
        for pat in MARKETING_SUFFIX_PATTERNS:
            new_title = re.sub(pat, "", title, flags=re.I).strip()
            if new_title != title:
                title = new_title
                changed = True
                break
        if changed:
            continue
        m = re.search(r"\s*\(([^()]*)\)\s*$", title)
        if m:
            inner = m.group(1).strip()
            inner_norm = re.sub(r"\s+", "", norm(inner))
            if re.fullmatch(r"(?:97[89]\d{10}|\d{10}|(?:19|20)\d{2})", inner_norm) or _tail_has_metadata_keywords(inner):
                title = title[:m.start()].strip()
                changed = True
                continue
        m = re.search(r"\s*-\s*([^-]+)\s*$", title)
        if m and _tail_has_metadata_keywords(m.group(1).strip()):
            title = title[:m.start()].strip()
            changed = True
            continue

    title = re.sub(r"\s+", " ", title)
    title = re.sub(r"\s*[:;,.]+\s*$", "", title)
    return title.strip(" -:;,.[]")

def title_query_variants(title: str) -> List[str]:
    vals: List[str] = []
    raw = smart_title(title)

    for candidate in [
        raw,
        clean_title_for_matching(title),
        re.sub(r"\s*\([^)]*\)\s*$", "", raw).strip(" -:;,.[ ]"),
        re.sub(r":\s*(A Novel|With Bonus Chapter|The World of .+)$", "", raw, flags=re.I).strip(" -:;,.[ ]"),
        re.sub(r"\s*-\s*[^-]*(?:series|book)\s+\d+[^-]*$", "", raw, flags=re.I).strip(" -:;,.[ ]"),
    ]:
        candidate = smart_title(candidate)
        candidate = re.sub(r"\s+", " ", candidate).strip(" -:;,.[]")
        if candidate and candidate not in vals:
            vals.append(candidate)
    return vals

def normalize_search_query_title(title: str) -> str:
    title = smart_title(title)
    title = html.unescape(title)
    title = clean_title_for_matching(title)
    title = re.sub(r"\s*\([^)]{0,80}\)\s*$", "", title).strip(" -:;,.[]")
    title = re.sub(r"\s*[:–—-]\s*(?:a novel|with bonus chapter|the world of .+)$", "", title, flags=re.I).strip(" -:;,.[]")
    title = re.sub(r"\s+", " ", title)
    return title.strip()

def is_searchworthy_token(q: str, current_hc_id: str = "") -> bool:
    q = (q or "").strip()
    if not q:
        return False
    if current_hc_id and q == current_hc_id:
        return False
    if re.fullmatch(r"\d{1,9}", q):
        return False
    if re.fullmatch(r"[A-Z0-9_]{6,}", q):
        if re.fullmatch(r"(97[89]\d{10}|\d{10}|\d{13}|B0[A-Z0-9]{8,}|[A-Z0-9]{10})", q):
            return True
        return False
    return True


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


def vlog(verbose: bool, message: str) -> None:
    if verbose:
        print(message, file=sys.stderr, flush=True)


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


def describe_edition(ed: Optional[HardcoverEdition], pick_score: Optional[float] = None) -> str:
    if not ed:
        return "-"
    parts = [
        f"id={ed.id}",
        f"title={smart_title(ed.title or '-') }",
        f"reading={ed.reading_format or '-'}",
        f"edition={ed.edition_format or '-'}",
        f"lang={ed.language or '-'}",
        f"hc_score={ed.score or 0}",
        f"users={ed.users_count or 0}",
    ]
    if pick_score is not None:
        parts.append(f"pick_score={float(pick_score):.3f}")
    return " | ".join(parts)


def summarize_ranked_editions(ranked: List[Tuple[Tuple[Any, ...], float, str, HardcoverEdition]], limit: int = 3) -> List[str]:
    return [f"{describe_edition(ed, pick_score=score)} | why={reason}" for _rank, score, reason, ed in ranked[:limit]]


def _log_label(text: Optional[str], max_len: int = 60) -> str:
    value = " ".join(str(text or "-").split())
    return value or "-"


def preview_names(items: List[str], limit: int = 3, max_len: int = 40) -> str:
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


def compact_edition_marker(ed: Optional[HardcoverEdition], pick_score: Optional[float] = None) -> str:
    if not ed:
        return "-"
    fmt = normalize_edition_format(ed.edition_format, ed.reading_format) or (ed.edition_format or ed.reading_format or "-")
    lang = ed.language or "-"
    title = _log_label(ed.title or "-", max_len=48)
    parts = [f'edition="{title}" [{ed.id}]', f"fmt={fmt}", f"lang={lang}", f"users={ed.users_count or 0}"]
    if pick_score is not None:
        parts.append(f"pick={float(pick_score):.1f}")
    return " ".join(parts)


def compact_ranked_editions(ranked: List[Tuple[Tuple[Any, ...], float, str, HardcoverEdition]], limit: int = 3) -> str:
    items: List[str] = []
    for _rank, score, _reason, ed in ranked[:limit]:
        fmt = normalize_edition_format(ed.edition_format, ed.reading_format) or (ed.edition_format or ed.reading_format or "-")
        items.append(f'{_log_label(ed.title or "-", max_len=32)} [{ed.id}; {fmt}; {ed.language or "-"}; {float(score):.1f}]')
    return " | ".join(items) if items else "-"


def compact_ranked_editions_from_choice(ranked: List[Tuple[Tuple[Any, ...], float, str, HardcoverEdition]], skip: int = 0, limit: int = 3) -> str:
    if skip < 0:
        skip = 0
    return compact_ranked_editions(ranked[skip:], limit=limit) if ranked[skip:] else "-"


def compact_suggest_fields(decision: Decision, suggested_book: Optional[HardcoverBook], suggested_edition: Optional[HardcoverEdition]) -> str:
    parts: List[str] = []
    if decision.suggested_calibre_title:
        parts.append(f'calibre_title="{_log_label(decision.suggested_calibre_title, max_len=56)}"')
    if decision.suggested_calibre_authors:
        parts.append(f'calibre_authors="{_log_label(decision.suggested_calibre_authors, max_len=48)}"')
    book = suggested_book
    if book and (decision.suggested_hardcover_id or decision.suggested_hardcover_edition_id):
        parts.append(f'hc={compact_book_marker(book)}')
    elif decision.suggested_hardcover_id:
        parts.append(f'hc_id={decision.suggested_hardcover_id}')
    ed = suggested_edition
    if ed and (decision.suggested_hardcover_edition_id or decision.suggested_hardcover_id):
        parts.append(f'edition="{_log_label(ed.title or "-", max_len=48)}" [{ed.id}]')
    elif decision.suggested_hardcover_edition_id:
        parts.append(f'edition_id={decision.suggested_hardcover_edition_id}')
    return " ".join(parts)


def compact_book_marker(book: Optional[HardcoverBook]) -> str:
    if not book:
        return "-"
    title = _log_label(smart_title(book.title or "-"), max_len=48)
    authors = _log_label(smart_title(book.authors or "-"), max_len=40)
    return f'"{title}" [{book.id}] by {authors}'


def compact_missing_series_marker(missing: Dict[str, Any], primary_books_count: int = 0, include_meta: bool = True) -> str:
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


def fmt_bool(flag: Optional[bool]) -> str:
    if flag is True:
        return "yes"
    if flag is False:
        return "no"
    return "-"


def norm(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().replace("&", " and ")
    s = re.sub(r"[’'`]", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def clean_isbn(value: str) -> str:
    return (value or "").strip().upper().replace("-", "").replace(" ", "")

def smart_title(title: str) -> str:
    title = html.unescape((title or "").replace(" ", " "))
    return re.sub(r"\s+", " ", title.strip()).strip("[] ").strip()

def strip_series_suffix(title: str) -> str:
    return clean_title_for_matching(title)

def normalize_person_name(name: str) -> str:
    name = (name or "").strip().strip(";").strip(",")
    name = re.sub(r"\s*\[[^\]]+\]\s*$", "", name).strip()
    if "," in name:
        parts = [p.strip() for p in name.split(",") if p.strip()]
        if len(parts) >= 2:
            name = " ".join(parts[1:] + [parts[0]])
    name = re.sub(r"\s+", " ", name).strip()
    return name

def split_author_like_string(val: str) -> List[str]:
    val = (val or "").strip()
    val = re.sub(r"\s*\[[^\]]+\]\s*$", "", val).strip()
    raw_parts = re.split(r"\s*;\s*|\s*&\s*", val) if (";" in val or " & " in val) else [val]
    out: List[str] = []
    for part in raw_parts:
        part = normalize_person_name(part)
        if part and part not in out:
            out.append(part)
    return out

def normalize_author_string(val: str) -> str:
    return " & ".join(split_author_like_string(val))

PRIMARY_AUTHOR_ROLE_EXCLUDE_SUBSTRINGS = (
    "adapt",
    "afterword",
    "annotat",
    "comment",
    "cover",
    "edit",
    "foreword",
    "illustrat",
    "inker",
    "introduction",
    "letter",
    "narrat",
    "perform",
    "photograph",
    "penc",
    "read by",
    "reader",
    "translat",
)
PRIMARY_AUTHOR_ROLE_INCLUDE_EXACT = {
    "author",
    "authors",
    "co author",
    "co authors",
    "coauthor",
    "coauthors",
    "created by",
    "creator",
    "joint author",
    "joint authors",
    "primary author",
    "primary authors",
    "story by",
    "written by",
    "writer",
    "writers",
}


def normalize_contribution_role(role: str) -> str:
    return norm(role)


def is_primary_author_contribution(role: str) -> bool:
    role_norm = normalize_contribution_role(role)
    if not role_norm:
        return True
    if any(token in role_norm for token in PRIMARY_AUTHOR_ROLE_EXCLUDE_SUBSTRINGS):
        return False
    if role_norm in PRIMARY_AUTHOR_ROLE_INCLUDE_EXACT:
        return True
    if "author" in role_norm:
        return True
    if "writer" in role_norm:
        return True
    return False


def authors_from_contributions(contributions: List[Dict[str, Any]]) -> str:
    primary_names: List[str] = []
    fallback_names: List[str] = []
    seen_primary: Set[str] = set()
    seen_fallback: Set[str] = set()
    for contribution in contributions or []:
        author = (contribution.get("author") or {}) if isinstance(contribution, dict) else {}
        name = normalize_person_name((author.get("name") or "").strip())
        if not name:
            continue
        name_key = normalize_author_key(name)
        if name_key not in seen_fallback:
            fallback_names.append(name)
            seen_fallback.add(name_key)
        role = (contribution.get("contribution") or "") if isinstance(contribution, dict) else ""
        if is_primary_author_contribution(role) and name_key not in seen_primary:
            primary_names.append(name)
            seen_primary.add(name_key)
    chosen = primary_names or fallback_names
    return normalize_author_string(" & ".join(chosen))


def canonical_author_set(val: str) -> Tuple[str, ...]:
    return tuple(sorted({normalize_author_key(p) for p in split_author_like_string(val) if normalize_author_key(p)}))


def _author_initial_surname_key(name: str) -> str:
    parts = [p for p in norm(name).split() if p]
    if len(parts) < 2:
        return parts[0] if parts else ""
    surname = parts[-1]
    initials = "".join(p[0] for p in parts[:-1] if p)
    return f"{initials} {surname}".strip()


def author_match_set(val: str) -> Set[str]:
    out: Set[str] = set()
    for part in split_author_like_string(val):
        canon = normalize_author_key(part)
        if canon:
            out.add(canon)
            alt = _author_initial_surname_key(canon)
            if alt:
                out.add(alt)
    return out

def title_marketing_penalty(title: str) -> float:
    raw = smart_title(title)
    if not raw:
        return 0.0
    cleaned = clean_title_for_matching(raw)
    penalty = 0.0
    if raw != cleaned:
        penalty += 8.0
    if re.search(r":\s*A Novel\s*$", raw, re.I):
        penalty += 4.0
    if re.search(r"\(With Bonus Chapter\)\s*$", raw, re.I):
        penalty += 3.0
    if re.search(r"The Brand New Must-Read", raw, re.I):
        penalty += 4.0
    if re.search(r"collection set", raw, re.I):
        penalty += 6.0
    return penalty

def primary_author(authors: str) -> str:
    parts = split_author_like_string(authors)
    return parts[0] if parts else ""

def textually_distinct_titles(a: str, b: str) -> bool:
    aa = smart_title(a)
    bb = smart_title(b)
    return bool(aa and bb and norm(aa) != norm(bb))


def textually_distinct_authors(a: str, b: str) -> bool:
    aa = normalize_author_string(a)
    bb = normalize_author_string(b)
    return bool(aa and bb and aa != bb)


def canonically_distinct_authors(a: str, b: str) -> bool:
    aa = canonical_author_set(a)
    bb = canonical_author_set(b)
    return bool(aa and bb and aa != bb)


def summarize_embedded_mismatch(embedded: EmbeddedMeta, calibre_title: str, calibre_authors: str, suggested_title: str, suggested_authors: str) -> str:
    tags: List[str] = []
    if embedded.embedded_title and calibre_title and textually_distinct_titles(embedded.embedded_title, calibre_title):
        tags.append("embedded_title_vs_calibre")
    if embedded.embedded_authors and calibre_authors and textually_distinct_authors(embedded.embedded_authors, calibre_authors):
        tags.append("embedded_authors_text_vs_calibre")
    if embedded.embedded_authors and calibre_authors and canonically_distinct_authors(embedded.embedded_authors, calibre_authors):
        tags.append("embedded_authors_canonical_vs_calibre")
    if embedded.embedded_title and suggested_title and textually_distinct_titles(embedded.embedded_title, suggested_title):
        tags.append("embedded_title_vs_suggested")
    if embedded.embedded_authors and suggested_authors and textually_distinct_authors(embedded.embedded_authors, suggested_authors):
        tags.append("embedded_authors_text_vs_suggested")
    if embedded.embedded_authors and suggested_authors and canonically_distinct_authors(embedded.embedded_authors, suggested_authors):
        tags.append("embedded_authors_canonical_vs_suggested")
    return ";".join(tags)

def identifier_candidates(record: BookRecord, embedded: EmbeddedMeta) -> set[str]:
    vals: set[str] = set()
    for x in record.isbn_candidates + record.asin_candidates + list(record.all_identifiers.values()) + list(embedded.embedded_identifiers.values()):
        c = clean_isbn(str(x or ""))
        if c:
            vals.add(c)
    return vals

def is_audio_edition(ed: HardcoverEdition) -> bool:
    rf = norm(ed.reading_format)
    pf = norm(ed.physical_format)
    ef = norm(ed.edition_format)
    return (
        rf == "listened"
        or (ed.audio_seconds or 0) > 0
        or "audio" in rf
        or "audio" in pf
        or "audio" in ef
    )


def is_blank_language_edition(ed: Optional[HardcoverEdition]) -> bool:
    return bool(ed and not str(ed.language or "").strip())


def is_edition_write_blocked_audio(ed: Optional[HardcoverEdition]) -> bool:
    return bool(ed and is_audio_edition(ed))


def is_edition_write_blocked_blank_language(ed: Optional[HardcoverEdition]) -> bool:
    return is_blank_language_edition(ed)


def is_edition_write_blocked_row(*, format_normalized: Any = "", language: Any = "") -> bool:
    fmt = norm(str(format_normalized or ""))
    lang = str(language or "").strip()
    return fmt == "audiobook" or not lang


def is_english_language_name(name: str) -> bool:
    return bool(str(name or "").strip()) and norm(name).startswith("english")


def is_unknown_language_name(name: str) -> bool:
    return not str(name or "").strip()


def edition_language_ok_rank(ed: HardcoverEdition) -> int:
    return 1 if is_english_language_name(ed.language) else 0


def edition_unknown_language_rank(ed: HardcoverEdition) -> int:
    return 1 if is_unknown_language_name(ed.language) else 0


def edition_explicit_english_rank(ed: HardcoverEdition) -> int:
    return 1 if norm(ed.language).startswith("english") else 0


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


def is_ebookish_edition(ed: HardcoverEdition) -> bool:
    if is_audio_edition(ed):
        return False
    rf = norm(ed.reading_format)
    ef = norm(ed.edition_format)
    if rf == "ebook":
        return True
    return any(token in ef for token in EBOOKISH_EDITION_FORMAT_TOKENS)


def normalize_author_csv(val: str) -> str:
    return " | ".join(canonical_author_set(val))


def normalize_primary_author_value(val: str) -> str:
    primary = primary_author(val)
    return normalize_author_key(primary) if primary else ""


def explain_author_mismatch(file_authors: str, other_authors: str) -> str:
    file_keys = set(canonical_author_set(file_authors))
    other_keys = set(canonical_author_set(other_authors))
    if not file_keys and not other_keys:
        return "no_author_data"
    if not file_keys:
        return "file_authors_missing"
    if not other_keys:
        return "candidate_authors_missing"
    overlap = file_keys & other_keys
    if file_keys == other_keys:
        return "primary_author_exact"
    if overlap:
        if other_keys > file_keys:
            return "candidate_has_extra_primary_authors"
        if file_keys > other_keys:
            return "file_has_extra_primary_authors"
        return "primary_author_partial_overlap"
    if len(file_keys) == 1 and len(other_keys) > 1:
        return "no_overlap_candidate_multi_contributor"
    if len(file_keys) > 1 and len(other_keys) == 1:
        return "no_overlap_file_multi_contributor"
    return "no_primary_author_overlap"


def normalize_edition_format(value: str, reading_format: str = "") -> str:
    rf = norm(reading_format)
    ef = norm(value)
    if rf == "ebook" or any(token in ef for token in EBOOKISH_EDITION_FORMAT_TOKENS):
        return "ebook"
    if rf == "listened" or "audio" in rf or "audio" in ef or "audible" in ef:
        return "audiobook"
    if "hardcover" in ef or "hardback" in ef:
        return "hardcover"
    if "paperback" in ef or "mass market" in ef or "softcover" in ef:
        return "paperback"
    if "digital" in ef:
        return "digital"
    if rf == "read":
        return "read"
    if ef:
        return ef
    if rf:
        return rf
    return "unknown"


def edition_decision_payload(ed: Optional[HardcoverEdition]) -> Dict[str, Any]:
    if not ed:
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
        "suggested_hardcover_edition_id": str(ed.id),
        "suggested_hardcover_edition_title": ed.title or "",
        "suggested_hardcover_edition_format": ed.edition_format or ed.reading_format or "",
        "suggested_hardcover_reading_format": ed.reading_format or "",
        "suggested_hardcover_edition_format_raw": ed.edition_format or "",
        "suggested_hardcover_edition_format_normalized": normalize_edition_format(ed.edition_format, ed.reading_format),
        "suggested_hardcover_edition_is_ebookish": bool(is_ebookish_edition(ed)),
        "suggested_hardcover_edition_language": ed.language or "",
    }


def edition_reason_parts(*, record: BookRecord, file_work: FileWork, embedded: EmbeddedMeta, book: HardcoverBook, ed: HardcoverEdition, id_match: int, non_audio: int, language_ok: int, unknown_language: int, default_rank: int, ebook_pref: int, type_rank: int, explicit_english: int, author_match: int, clean_title_match: int, no_collection: int, no_marketing: int, clean_title_bonus: int) -> List[str]:
    parts: List[str] = []
    if id_match:
        parts.append("identifier_match")
    if clean_title_match:
        parts.append("title_exact")
    elif clean_title_for_matching(ed.title or book.title) == clean_title_for_matching(record.calibre_title):
        parts.append("title_matches_calibre")
    if author_match >= 2:
        parts.append("primary_author_exact")
    elif author_match == 1:
        parts.append("primary_author_partial")
    else:
        parts.append(explain_author_mismatch(file_work.authors or record.calibre_authors, ed.authors or book.authors))
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
        if book.default_ebook_edition_id and int(ed.id) == int(book.default_ebook_edition_id):
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
    return parts


def edition_reason_text(**kwargs: Any) -> str:
    return "; ".join(edition_reason_parts(**kwargs))


def edition_type_rank(ed: HardcoverEdition) -> int:
    rf = norm(ed.reading_format)
    if is_ebookish_edition(ed):
        return 2
    if rf == "read":
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

def edition_default_rank(record: BookRecord, book: HardcoverBook, ed: HardcoverEdition) -> int:
    pref = preferred_default_edition_id(record, book)
    if pref and int(ed.id) == int(pref):
        return 2
    if book.default_cover_edition_id and int(ed.id) == int(book.default_cover_edition_id):
        return 1
    return 0

def edition_author_match_rank(ed: HardcoverEdition, file_work: FileWork, book: HardcoverBook) -> int:
    ed_authors = author_match_set(ed.authors)
    target_authors = author_match_set(file_work.authors or book.authors)
    if not ed_authors or not target_authors:
        return 0
    canon_ed = set(canonical_author_set(ed.authors))
    canon_target = set(canonical_author_set(file_work.authors or book.authors))
    if canon_ed and canon_target and canon_ed == canon_target:
        return 2
    if ed_authors & target_authors:
        return 1
    return 0


def is_collectionish_edition(ed: HardcoverEdition) -> bool:
    raw = " ".join(x for x in [smart_title(ed.title or ""), smart_title(ed.subtitle or "")] if x).strip()
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
    return any(re.search(p, raw, re.I) for p in patterns)


def edition_review_score(*, id_match: int, non_audio: int, language_ok: int, unknown_language: int, default_rank: int, ebook_pref: int, type_rank: int, explicit_english: int, author_match: int, clean_title_match: int, no_collection: int, no_marketing: int, clean_title_bonus: int, ed: HardcoverEdition) -> float:
    score = 0.0
    score += id_match * 1000.0
    score += non_audio * 320.0
    score += language_ok * 220.0
    score += unknown_language * 20.0
    score += default_rank * 190.0
    score += ebook_pref * 120.0
    score += type_rank * 80.0
    score += explicit_english * 50.0
    score += author_match * 40.0
    score += clean_title_match * 30.0
    score += no_collection * 20.0
    score += no_marketing * 10.0
    score += min(9.9, float(ed.score or 0) / 100.0)
    score += min(4.9, float(ed.users_read_count or 0) / 10000.0)
    score += min(3.9, float(ed.users_count or 0) / 10000.0)
    score += min(5.0, float(ed.rating or 0.0))
    score += min(2.9, float(ed.lists_count or 0) / 1000.0)
    score += clean_title_bonus * 1.0
    return round(score, 3)


def rank_candidate_editions(record: BookRecord, file_work: FileWork, embedded: EmbeddedMeta, book: HardcoverBook, editions: List[HardcoverEdition]) -> List[Tuple[Tuple[Any, ...], float, str, HardcoverEdition]]:
    if not editions:
        return []
    ids = identifier_candidates(record, embedded)
    file_clean = clean_title_for_matching(file_work.title or book.title)
    prefers_ebook = (record.file_format or "").upper() in set(PREFERRED_FORMATS)
    ranked: List[Tuple[Tuple[Any, ...], float, str, HardcoverEdition]] = []
    fallback: List[Tuple[Tuple[Any, ...], float, str, HardcoverEdition]] = []
    for ed in editions:
        type_rank = edition_type_rank(ed)
        language_ok = edition_language_ok_rank(ed)
        unknown_language = edition_unknown_language_rank(ed)
        explicit_english = edition_explicit_english_rank(ed)
        clean_title_match = 1 if clean_title_for_matching(ed.title or book.title) == file_clean else 0
        clean_title_bonus = 1 if smart_title(ed.title or "") == clean_title_for_matching(ed.title or "") else 0
        id_match = 1 if any(x and x in {clean_isbn(ed.isbn_10), clean_isbn(ed.isbn_13), clean_isbn(ed.asin)} for x in ids) else 0
        non_audio = 1 if not is_audio_edition(ed) else 0
        ebook_pref = 1 if prefers_ebook and is_ebookish_edition(ed) else 0
        default_rank = edition_default_rank(record, book, ed)
        author_match = edition_author_match_rank(ed, file_work, book)
        no_collection = 1 if not is_collectionish_edition(ed) else 0
        no_marketing = 1 if title_marketing_penalty(" ".join([ed.title or "", ed.subtitle or ""]).strip()) == 0 else 0
        rank = (id_match, clean_title_match, author_match, ebook_pref, non_audio, language_ok, explicit_english, default_rank, unknown_language, no_collection, no_marketing, int(ed.score or 0), int(ed.users_read_count or 0), int(ed.users_count or 0), int(round((ed.rating or 0.0) * 100)), int(ed.lists_count or 0), clean_title_bonus, ed.release_date or "")
        review_score = edition_review_score(id_match=id_match, non_audio=non_audio, language_ok=language_ok, unknown_language=unknown_language, default_rank=default_rank, ebook_pref=ebook_pref, type_rank=type_rank, explicit_english=explicit_english, author_match=author_match, clean_title_match=clean_title_match, no_collection=no_collection, no_marketing=no_marketing, clean_title_bonus=clean_title_bonus, ed=ed)
        reason = edition_reason_text(record=record, file_work=file_work, embedded=embedded, book=book, ed=ed, id_match=id_match, non_audio=non_audio, language_ok=language_ok, unknown_language=unknown_language, default_rank=default_rank, ebook_pref=ebook_pref, type_rank=type_rank, explicit_english=explicit_english, author_match=author_match, clean_title_match=clean_title_match, no_collection=no_collection, no_marketing=no_marketing, clean_title_bonus=clean_title_bonus)
        item = (rank, review_score, reason, ed)
        if non_audio and type_rank > 0 and language_ok:
            ranked.append(item)
        else:
            fallback.append(item)
    pool = ranked or fallback
    pool.sort(key=lambda x: (x[1], x[0]), reverse=True)
    return pool


def choose_preferred_edition_info(record: BookRecord, file_work: FileWork, embedded: EmbeddedMeta, book: HardcoverBook, editions: List[HardcoverEdition]) -> EditionChoiceInfo:
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
        for _rank, score, reason, ed in ranked:
            if int(ed.id) == int(book.default_ebook_edition_id):
                default_ebook = ed
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


def choose_preferred_edition(record: BookRecord, file_work: FileWork, embedded: EmbeddedMeta, book: HardcoverBook, editions: List[HardcoverEdition]) -> Optional[HardcoverEdition]:
    return choose_preferred_edition_info(record, file_work, embedded, book, editions).chosen


def book_selection_adjusted_score(raw_score: float, file_work: FileWork, book: HardcoverBook, preferred_edition: Optional[HardcoverEdition]) -> float:
    score = float(raw_score)
    score -= title_marketing_penalty(book.title)
    if preferred_edition:
        rf = norm(preferred_edition.reading_format)
        score += 2.0 if is_ebookish_edition(preferred_edition) else 1.0 if rf == "read" else 0.0
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
        if clean_title_for_matching(preferred_edition.title or book.title) == clean_title_for_matching(file_work.title or book.title):
            score += 1.5
        if smart_title(preferred_edition.title or "") == clean_title_for_matching(preferred_edition.title or ""):
            score += 0.75
        if is_collectionish_edition(preferred_edition):
            score -= 2.0
        if is_audio_edition(preferred_edition):
            score -= 4.0
    score += min(1.0, (book.users_read_count or 0) / 1000.0)
    return round(score, 2)


def title_similarity(a: str, b: str) -> float:
    na, nb = norm(a), norm(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    return SequenceMatcher(None, na, nb).ratio()

def bare_title_similarity(a: str, b: str) -> float:
    return title_similarity(strip_series_suffix(a), strip_series_suffix(b))

def author_similarity(a: str, b: str) -> float:
    aa = set(canonical_author_set(a))
    bb = set(canonical_author_set(b))
    if not aa or not bb:
        return 0.0
    inter = len(aa & bb)
    union = len(aa | bb)
    return inter / union if union else 0.0

def author_coverage(file_authors: str, other_authors: str) -> float:
    aa = set(canonical_author_set(file_authors))
    bb = set(canonical_author_set(other_authors))
    if not aa or not bb:
        return 0.0
    inter = len(aa & bb)
    return inter / len(aa)


def effective_candidate_authors(book: HardcoverBook, preferred_edition: Optional[HardcoverEdition]) -> str:
    if preferred_edition and preferred_edition.authors and not is_audio_edition(preferred_edition):
        return preferred_edition.authors
    return book.authors


def contributor_count(authors: str) -> int:
    return len(canonical_author_set(authors))

def confidence_tier(score: float) -> str:
    if score >= 90:
        return "high"
    if score >= 75:
        return "medium"
    return "low"

def looks_english(text: str) -> Tuple[str, float]:
    text = (text or "")[:20000].lower()
    words = re.findall(r"[a-zA-ZÀ-ÿ']+", text)
    if len(words) < 50:
        return ("uncertain", 0.0)
    total = len(words)
    def hit_rate(vocab: set[str]) -> float:
        return sum(1 for w in words if w in vocab) / total
    en = hit_rate(EN_STOPWORDS); de = hit_rate(DE_STOPWORDS); fr = hit_rate(FR_STOPWORDS); es = hit_rate(ES_STOPWORDS)
    rates = {"eng": en, "deu": de, "fra": fr, "spa": es}
    lang, score = max(rates.items(), key=lambda kv: kv[1])
    if score < 0.015:
        return ("uncertain", score)
    if lang == "eng" and en >= max(de, fr, es) * 1.5:
        return ("eng", en)
    if lang != "eng" and score >= en * 1.2:
        return (lang, score)
    return ("uncertain", score)


def normalize_language_signal(value: str) -> str:
    v = norm(value)
    if not v:
        return ""
    mapping = {
        "eng": "eng",
        "english": "eng",
        "deu": "deu",
        "de": "deu",
        "german": "deu",
        "deutsch": "deu",
        "fra": "fra",
        "fre": "fra",
        "fr": "fra",
        "french": "fra",
        "francais": "fra",
        "francais france": "fra",
        "spa": "spa",
        "es": "spa",
        "spanish": "spa",
        "espanol": "spa",
        "uncertain": "uncertain",
    }
    return mapping.get(v, v)


def is_non_english_signal(value: str) -> bool:
    return normalize_language_signal(value) in {"deu", "fra", "spa"}


def build_text_probe(text: str, slice_chars: int = 4000) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    if len(text) <= slice_chars * 2:
        return text[:slice_chars * 2]
    offsets = [0, max(0, len(text) // 2 - slice_chars // 2), max(0, len(text) - slice_chars)]
    parts: List[str] = []
    seen: set[int] = set()
    for start in offsets:
        start = max(0, min(start, max(0, len(text) - slice_chars)))
        if start in seen:
            continue
        seen.add(start)
        part = text[start:start + slice_chars].strip()
        if part:
            parts.append(part)
    return " ".join(parts)


def extract_epub_language_probe(path: Path, slice_chars: int = 4000) -> str:
    try:
        with zipfile.ZipFile(path) as zf:
            names = zf.namelist()
            opf_path = None
            if "META-INF/container.xml" in names:
                root = ET.fromstring(zf.read("META-INF/container.xml"))
                el = root.find(".//container:rootfile", CONTAINER_NS)
                if el is not None:
                    opf_path = el.attrib.get("full-path")
            if not opf_path:
                opf_candidates = [n for n in names if n.lower().endswith(".opf")]
                opf_path = opf_candidates[0] if opf_candidates else None
            spine_hrefs: List[str] = []
            if opf_path and opf_path in names:
                opf_root = ET.fromstring(zf.read(opf_path))
                manifest: Dict[str, str] = {}
                for item in opf_root.findall('.//{http://www.idpf.org/2007/opf}manifest/{http://www.idpf.org/2007/opf}item'):
                    manifest[item.attrib.get('id')] = item.attrib.get('href')
                base = str(Path(opf_path).parent)
                for itemref in opf_root.findall('.//{http://www.idpf.org/2007/opf}spine/{http://www.idpf.org/2007/opf}itemref'):
                    href = manifest.get(itemref.attrib.get('idref'))
                    if href:
                        spine_hrefs.append(str((Path(base) / href).as_posix()).lstrip('./'))
            candidates = spine_hrefs or sorted([n for n in names if re.search(r'\.(xhtml|html|htm|xml)$', n, re.I)])
            if not candidates:
                return ""
            probe_indices = sorted(set([0, len(candidates) // 2, len(candidates) - 1]))
            parts: List[str] = []
            for idx in probe_indices:
                name = candidates[idx]
                if name not in names:
                    matches = [n for n in names if n.endswith(name)]
                    if matches:
                        name = matches[0]
                    else:
                        continue
                raw = zf.read(name).decode('utf-8', errors='ignore')
                txt = _strip_tags(raw)
                if txt:
                    parts.append(txt[:slice_chars])
            return ' '.join(parts)
    except Exception:
        return ""


def looks_englishish_text(text: str) -> bool:
    sample = (text or "").strip()
    if not sample:
        return True
    ascii_letters = sum(1 for ch in sample if ch.isascii() and ch.isalpha())
    alpha = sum(1 for ch in sample if ch.isalpha())
    if alpha and (ascii_letters / alpha) < 0.75:
        return False
    lang, score = looks_english(sample)
    if lang == "eng":
        return True
    if lang in {"deu", "fra", "spa"} and score >= 0.02:
        return False
    return True


def preferred_format_key(fmt: str) -> Tuple[int, str]:
    fmt = (fmt or "").upper()
    return (PREFERRED_FORMATS.index(fmt), fmt) if fmt in PREFERRED_FORMATS else (999, fmt)

def choose_primary_file(paths_by_format: Dict[str, str]) -> Tuple[str, str]:
    if not paths_by_format:
        return ("", "")
    fmt = sorted(paths_by_format, key=lambda f: preferred_format_key(f))[0]
    return paths_by_format[fmt], fmt

def chunked(seq: List[int], size: int) -> List[List[int]]:
    if size <= 0:
        return [seq[:]]
    return [seq[i:i + size] for i in range(0, len(seq), size)]

def load_calibre_books(metadata_db: Path, library_root: Path) -> List[BookRecord]:
    conn = sqlite3.connect(str(metadata_db))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    books: Dict[int, Dict[str, Any]] = {}
    for row in cur.execute("SELECT id, title, series_index, path FROM books ORDER BY id"):
        books[row["id"]] = {"id": row["id"], "title": row["title"] or "", "series_index": row["series_index"], "path": row["path"] or "", "authors": [], "series": "", "languages": [], "identifiers": {}, "files": {}}
    try:
        for row in cur.execute("""SELECT bal.book AS book_id, a.name AS author_name FROM books_authors_link bal JOIN authors a ON a.id = bal.author ORDER BY bal.book, bal.id"""):
            if row["book_id"] in books and row["author_name"]:
                books[row["book_id"]]["authors"].append(row["author_name"])
    except sqlite3.Error:
        pass
    try:
        for row in cur.execute("""SELECT bsl.book AS book_id, s.name AS series_name FROM books_series_link bsl JOIN series s ON s.id = bsl.series ORDER BY bsl.book"""):
            if row["book_id"] in books and row["series_name"] and not books[row["book_id"]]["series"]:
                books[row["book_id"]]["series"] = row["series_name"]
    except sqlite3.Error:
        pass
    try:
        for row in cur.execute("""SELECT bll.book AS book_id, l.lang_code AS lang_code FROM books_languages_link bll JOIN languages l ON l.id = bll.lang_code ORDER BY bll.book"""):
            if row["book_id"] in books and row["lang_code"]:
                books[row["book_id"]]["languages"].append(row["lang_code"])
    except sqlite3.Error:
        pass
    try:
        for row in cur.execute("SELECT book, type, val FROM identifiers ORDER BY book"):
            if row["book"] in books and row["type"] and row["val"]:
                books[row["book"]]["identifiers"][str(row["type"]).strip().lower()] = str(row["val"]).strip()
    except sqlite3.Error:
        pass
    try:
        for row in cur.execute("SELECT book, format, name FROM data ORDER BY book"):
            if row["book"] not in books or not row["format"] or not row["name"]:
                continue
            rel = Path(books[row["book"]]["path"]) / f"{row['name']}.{str(row['format']).lower()}"
            books[row["book"]]["files"][str(row["format"]).upper()] = str(library_root / rel)
    except sqlite3.Error:
        pass
    out: List[BookRecord] = []
    for b in books.values():
        file_path, file_format = choose_primary_file(b["files"])
        authors = normalize_author_string(" & ".join([a for a in b["authors"] if a]).strip())
        languages = ",".join(sorted(set([x for x in b["languages"] if x])))
        idents = b["identifiers"]
        hc_id = idents.get("hardcover-id") or idents.get("hardcover_id") or idents.get("hardcover") or ""
        hc_slug = idents.get("hardcover-slug") or idents.get("hardcover_slug") or ""
        hc_edition = idents.get("hardcover-edition") or idents.get("hardcover_edition") or ""
        isbn_candidates, asin_candidates = [], []
        for k, v in idents.items():
            cv = clean_isbn(v)
            if "isbn" in k and cv:
                isbn_candidates.append(cv)
            if ("asin" in k or "amazon" in k) and cv:
                asin_candidates.append(cv)
        out.append(BookRecord(
            calibre_book_id=int(b["id"]),
            calibre_title=b["title"],
            calibre_authors=authors,
            calibre_series=b["series"],
            calibre_series_index=float(b["series_index"]) if b["series_index"] is not None else None,
            calibre_language=languages,
            calibre_hardcover_id=hc_id,
            calibre_hardcover_slug=hc_slug,
            calibre_hardcover_edition_id=hc_edition,
            file_path=file_path,
            file_format=file_format,
            all_identifiers=idents,
            isbn_candidates=sorted(set(isbn_candidates)),
            asin_candidates=sorted(set(asin_candidates)),
        ))
    conn.close()
    return out

class EbookMetaRunner:
    def __init__(self, library_root: Path, ebook_meta_command: Optional[str] = None, docker_container_name: Optional[str] = None, container_library_root: Optional[str] = None, host_timeout: int = 15, docker_timeout: int = 20):
        self.library_root = library_root.resolve()
        self.ebook_meta_command = ebook_meta_command
        self.docker_container_name = docker_container_name
        self.container_library_root = container_library_root or "/calibre-library"
        self.host_timeout = max(1, int(host_timeout or 15))
        self.docker_timeout = max(1, int(docker_timeout or 20))
    def _host_command(self) -> Optional[List[str]]:
        if self.ebook_meta_command:
            return shlex.split(self.ebook_meta_command)
        host = shutil.which("ebook-meta")
        return [host] if host else None
    def _docker_command(self, host_file_path: Path) -> Optional[List[str]]:
        if not self.docker_container_name:
            return None
        try:
            rel = host_file_path.resolve().relative_to(self.library_root)
        except Exception:
            return None
        container_path = str(Path(self.container_library_root) / rel)
        cmd = f"ebook-meta {shlex.quote(container_path)}"
        return ["docker", "exec", "-i", self.docker_container_name, "sh", "-lc", cmd]
    def run(self, host_file_path: Path) -> EmbeddedMeta:
        if not host_file_path.exists():
            return EmbeddedMeta(tool_used="missing_file", parse_error="file does not exist")

        ext = host_file_path.suffix.lower()
        if ext in {".epub", ".kepub", ".oebzip"}:
            opf = parse_epub_opf_metadata(host_file_path)
            if opf.embedded_title or opf.embedded_authors or opf.embedded_identifiers:
                opf.tool_used = "epub-opf-fastpath"
                return opf

        host_cmd = self._host_command()
        if host_cmd:
            try:
                proc = subprocess.run(host_cmd + [str(host_file_path)], capture_output=True, text=True, timeout=self.host_timeout, check=False)
                if proc.returncode == 0:
                    return parse_ebook_meta_output((proc.stdout or "") + "\n" + (proc.stderr or ""), "host-ebook-meta")
            except Exception:
                pass
        docker_cmd = self._docker_command(host_file_path)
        if docker_cmd:
            try:
                proc = subprocess.run(docker_cmd, capture_output=True, text=True, timeout=self.docker_timeout, check=False)
                if proc.returncode == 0:
                    return parse_ebook_meta_output((proc.stdout or "") + "\n" + (proc.stderr or ""), f"docker:{self.docker_container_name}")
                return EmbeddedMeta(tool_used=f"docker:{self.docker_container_name}", parse_error=(proc.stderr or "")[:500])
            except Exception as exc:
                return EmbeddedMeta(tool_used=f"docker:{self.docker_container_name}", parse_error=str(exc))
        return EmbeddedMeta(tool_used="none", parse_error="ebook-meta unavailable")

def parse_ebook_meta_output(text: str, tool_used: str) -> EmbeddedMeta:
    md = EmbeddedMeta(raw=text, tool_used=tool_used)
    idents: Dict[str, str] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, val = line.split(":", 1)
        key = key.strip().lower()
        val = val.strip()
        if not val:
            continue
        if key.startswith("title") and not md.embedded_title:
            md.embedded_title = smart_title(val)
        elif key.startswith("author(s)") or key == "authors":
            md.embedded_authors = normalize_author_string(val)
        elif key.startswith("languages"):
            md.embedded_language = val
        elif key.startswith("identifiers"):
            for part in re.split(r",\s*", val):
                if ":" in part:
                    k, v = part.split(":", 1)
                    idents[k.strip().lower()] = v.strip()
        elif key.startswith("isbn"):
            idents["isbn"] = clean_isbn(val)
    md.embedded_identifiers = idents
    return md

def parse_epub_opf_metadata(file_path: Path) -> EmbeddedMeta:
    out = EmbeddedMeta(tool_used="epub-opf-fallback")
    try:
        with zipfile.ZipFile(file_path) as zf:
            names = zf.namelist()
            if "META-INF/container.xml" not in names:
                out.parse_error = "container.xml not found"
                return out
            root = ET.fromstring(zf.read("META-INF/container.xml"))
            el = root.find(".//container:rootfile", CONTAINER_NS)
            opf_path = el.attrib.get("full-path") if el is not None else None
            if not opf_path or opf_path not in names:
                out.parse_error = "OPF path not found"
                return out
            opf = zf.read(opf_path).decode("utf-8", errors="ignore")
            def first(pattern: str) -> str:
                m = re.search(pattern, opf, re.I | re.S)
                return smart_title(re.sub(r"\s+", " ", m.group(1)).strip()) if m else ""
            out.embedded_title = first(r"<dc:title[^>]*>(.*?)</dc:title>")
            creators = [re.sub(r"\s+", " ", m.group(1)).strip() for m in re.finditer(r"<dc:creator[^>]*>(.*?)</dc:creator>", opf, re.I | re.S)]
            out.embedded_authors = normalize_author_string(" & ".join([c for c in creators if c]))
            out.embedded_language = first(r"<dc:language[^>]*>(.*?)</dc:language>")
            ids = {}
            for m in re.finditer(r"<dc:identifier[^>]*>(.*?)</dc:identifier>", opf, re.I | re.S):
                val = re.sub(r"\s+", " ", m.group(1)).strip()
                cv = clean_isbn(val)
                if len(cv) in (10, 13):
                    ids["isbn"] = cv
            out.embedded_identifiers = ids
            return out
    except Exception as exc:
        out.parse_error = str(exc)
        return out

def _read_text_file(path: Path, limit: int = 20000) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")[:limit]
    except Exception:
        try:
            return path.read_text(encoding="latin-1", errors="ignore")[:limit]
        except Exception:
            return ""

def _strip_tags(text: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()

def clean_content_title_hint(title: str) -> str:
    title = smart_title(title)
    if not title:
        return ""
    n = norm(title)
    if n in IGNORE_CONTENT_TITLES:
        return ""
    if re.fullmatch(r"part\d{4,}", n):
        return ""
    if re.fullmatch(r"97[89]\d{10}.*", n):
        return ""
    return title

def extract_epub_text(path: Path, limit: int = 30000) -> Tuple[str, str]:
    sample_parts: List[str] = []
    heading = ""
    try:
        with zipfile.ZipFile(path) as zf:
            names = zf.namelist()
            opf_path = None
            if "META-INF/container.xml" in names:
                root = ET.fromstring(zf.read("META-INF/container.xml"))
                el = root.find(".//container:rootfile", CONTAINER_NS)
                if el is not None:
                    opf_path = el.attrib.get("full-path")
            if not opf_path:
                opf_candidates = [n for n in names if n.lower().endswith(".opf")]
                opf_path = opf_candidates[0] if opf_candidates else None
            spine_hrefs: List[str] = []
            if opf_path and opf_path in names:
                opf_root = ET.fromstring(zf.read(opf_path))
                manifest: Dict[str, str] = {}
                for item in opf_root.findall(".//{http://www.idpf.org/2007/opf}manifest/{http://www.idpf.org/2007/opf}item"):
                    manifest[item.attrib.get("id")] = item.attrib.get("href")
                base = str(Path(opf_path).parent)
                for itemref in opf_root.findall(".//{http://www.idpf.org/2007/opf}spine/{http://www.idpf.org/2007/opf}itemref"):
                    href = manifest.get(itemref.attrib.get("idref"))
                    if href:
                        spine_hrefs.append(str((Path(base) / href).as_posix()).lstrip("./"))
            candidates = spine_hrefs or sorted([n for n in names if re.search(r"\.(xhtml|html|htm|xml)$", n, re.I)])[:5]
            for name in candidates[:5]:
                if name not in names:
                    matches = [n for n in names if n.endswith(name)]
                    if matches:
                        name = matches[0]
                    else:
                        continue
                raw = zf.read(name).decode("utf-8", errors="ignore")
                if not heading:
                    m = re.search(r"(?is)<title>(.*?)</title>", raw) or re.search(r"(?is)<h1[^>]*>(.*?)</h1>", raw)
                    if m:
                        heading = clean_content_title_hint(_strip_tags(m.group(1)))
                txt = _strip_tags(raw)
                if txt:
                    sample_parts.append(txt)
                if sum(len(x) for x in sample_parts) >= limit:
                    break
    except Exception:
        return ("", "")
    return (" ".join(sample_parts)[:limit], heading[:300])

def extract_docx_text(path: Path, limit: int = 30000) -> Tuple[str, str]:
    try:
        with zipfile.ZipFile(path) as zf:
            raw = zf.read("word/document.xml").decode("utf-8", errors="ignore")
            txt = _strip_tags(raw)
            lines = [x.strip() for x in re.split(r"[\r\n]+", txt) if x.strip()]
            heading = clean_content_title_hint(lines[0][:300] if lines else "")
            return (txt[:limit], heading)
    except Exception:
        return ("", "")

def extract_content_signals(file_path: str, calibre_title: str, calibre_authors: str) -> ContentSignals:
    p = Path(file_path)
    if not p.exists():
        return ContentSignals(extractor="missing_file")
    ext = p.suffix.lower()
    sample = ""
    heading = ""
    extractor = ""
    lang_probe = ""
    try:
        if ext in {".epub", ".kepub", ".oebzip"}:
            sample, heading = extract_epub_text(p)
            lang_probe = build_text_probe(sample)
            extractor = "epub"
        elif ext in {".txt", ".text"}:
            raw = _read_text_file(p, 120000)
            sample = raw[:30000]
            lines = [x.strip() for x in raw.splitlines() if x.strip()]
            heading = clean_content_title_hint(lines[0][:300] if lines else "")
            lang_probe = build_text_probe(raw)
            extractor = "txt"
        elif ext in {".html", ".htm", ".xhtml", ".xml"}:
            raw = _read_text_file(p, 120000)
            stripped = _strip_tags(raw)
            sample = stripped[:30000]
            m = re.search(r"(?is)<title>(.*?)</title>", raw)
            heading = clean_content_title_hint(_strip_tags(m.group(1)) if m else "")
            lang_probe = build_text_probe(stripped)
            extractor = "html"
        elif ext == ".docx":
            full_text, heading = extract_docx_text(p, limit=120000)
            sample = full_text[:30000]
            lang_probe = build_text_probe(full_text)
            extractor = "docx"
        else:
            return ContentSignals(extractor=f"unsupported:{ext.lstrip('.')}")
    except Exception as exc:
        return ContentSignals(extractor=f"extract-error:{exc}")
    lang, lang_score = looks_english(lang_probe or sample)
    sample_norm = norm(sample[:10000])
    content_title_probe = heading or sample[:250]
    title_strength = bare_title_similarity(calibre_title, content_title_probe) if calibre_title and content_title_probe else 0.0
    author_strength = max((1.0 if norm(a) and norm(a) in sample_norm else 0.0 for a in [calibre_authors] if a), default=0.0)
    inferred_author = ""
    m = re.search(r"\bby\s+([A-Z][A-Za-z.\- ]{2,80})", sample[:3000], re.I)
    if m:
        inferred_author = normalize_person_name(m.group(1).strip())
    return ContentSignals(
        inferred_title_from_content=(heading or "")[:300],
        inferred_author_from_content=inferred_author[:300],
        inferred_language_from_content=lang,
        content_title_match_strength=round(title_strength, 3),
        content_author_match_strength=round(author_strength, 3),
        first_heading_excerpt=(heading or sample[:250])[:300],
        extracted_sample_len=len(sample),
        extractor=extractor,
        language_confidence=round(lang_score, 4),
    )

def derive_file_work(record: BookRecord, embedded: EmbeddedMeta, content: ContentSignals) -> FileWork:
    title = ""
    title_basis = ""
    for candidate, basis in [
        (embedded.embedded_title, "embedded"),
        (content.inferred_title_from_content, "content"),
        (Path(record.file_path).stem.replace("_", " "), "filename"),
        (record.calibre_title, "calibre_fallback"),
    ]:
        candidate = clean_content_title_hint(candidate) if basis == "content" else smart_title(candidate)
        if candidate:
            title = candidate
            title_basis = basis
            break

    def _looks_reasonable_content_author(author_text: str) -> bool:
        parts = split_author_like_string(author_text)
        if not parts:
            return False
        if len(parts) > 4:
            return False
        if any(len(p) > 60 for p in parts):
            return False
        return True

    authors = ""
    authors_basis = ""
    embedded_auths = normalize_author_string(embedded.embedded_authors)
    content_auth = normalize_author_string(content.inferred_author_from_content)
    calibre_auth = normalize_author_string(record.calibre_authors)

    if embedded_auths:
        authors = embedded_auths
        authors_basis = "embedded"
    elif content_auth and _looks_reasonable_content_author(content_auth):
        authors = content_auth
        authors_basis = "content"
    elif calibre_auth:
        authors = calibre_auth
        authors_basis = "calibre_fallback"
    elif content_auth:
        authors = content_auth
        authors_basis = "content"

    language = embedded.embedded_language or content.inferred_language_from_content or record.calibre_language
    return FileWork(title=title, authors=authors, language=language, title_basis=title_basis, authors_basis=authors_basis)

class HardcoverRequestFailure(RuntimeError):
    def __init__(self, message: str, meta: Optional[HardcoverRequestMeta] = None):
        super().__init__(message)
        self.meta = meta or HardcoverRequestMeta()


@dataclass
class CacheEntry:
    key: str
    label: str
    payload: Any
    created_at: float
    updated_at: float
    last_accessed_at: float
    is_empty: bool
    payload_size: int
    source_query: str = ""
    api_object_type: str = ""
    api_object_id: str = ""


class SQLiteCacheStore:
    def __init__(self, path: Path, verbose: bool = False, legacy_json_path: Optional[Path] = None):
        self.path = path
        self.verbose = bool(verbose)
        self.legacy_json_path = legacy_json_path
        self.imported_legacy_entries = 0
        ensure_dir(path.parent)
        self.conn = sqlite3.connect(str(path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()
        self._maybe_import_legacy_json()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache_entries (
                cache_key TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                is_empty INTEGER NOT NULL DEFAULT 0,
                payload_size INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_accessed_at REAL NOT NULL DEFAULT 0,
                source_query TEXT NOT NULL DEFAULT '',
                api_object_type TEXT NOT NULL DEFAULT '',
                api_object_id TEXT NOT NULL DEFAULT ''
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cache_entries_label ON cache_entries(label)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cache_entries_updated_at ON cache_entries(updated_at)")
        self._ensure_column("cache_entries", "last_accessed_at", "REAL NOT NULL DEFAULT 0")
        self._ensure_column("cache_entries", "source_query", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("cache_entries", "api_object_type", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("cache_entries", "api_object_id", "TEXT NOT NULL DEFAULT ''")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache_meta (
                meta_key TEXT PRIMARY KEY,
                meta_value TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def _ensure_column(self, table: str, column: str, ddl: str) -> None:
        cols = {str(row[1]) for row in self.conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
            self.conn.commit()

    def _get_meta(self, key: str) -> str:
        row = self.conn.execute("SELECT meta_value FROM cache_meta WHERE meta_key = ?", (key,)).fetchone()
        return str(row[0]) if row else ""

    def _set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO cache_meta(meta_key, meta_value) VALUES(?, ?) ON CONFLICT(meta_key) DO UPDATE SET meta_value=excluded.meta_value",
            (key, value),
        )
        self.conn.commit()

    def _maybe_import_legacy_json(self) -> None:
        legacy_path = self.legacy_json_path
        if not legacy_path or not legacy_path.exists():
            return
        if self._get_meta("legacy_json_imported"):
            return
        existing_count = int(self.conn.execute("SELECT COUNT(*) FROM cache_entries").fetchone()[0] or 0)
        if existing_count > 0:
            self._set_meta("legacy_json_imported", "skipped_nonempty_db")
            return
        try:
            raw = json.loads(legacy_path.read_text(encoding="utf-8"))
        except Exception:
            self._set_meta("legacy_json_imported", "failed_to_parse")
            return
        if not isinstance(raw, dict) or not raw:
            self._set_meta("legacy_json_imported", "empty_or_invalid")
            return
        now = time.time()
        rows = []
        for key, payload in raw.items():
            try:
                payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                continue
            label = (str(key or "").split("::", 1)[0] or "graphql").strip()
            rows.append((str(key), label, payload_json, 1 if payload in (None, {}) else 0, len(payload_json.encode("utf-8")), now, now, now, "", "", ""))
        if rows:
            self.conn.executemany(
                "INSERT OR REPLACE INTO cache_entries(cache_key, label, payload_json, is_empty, payload_size, created_at, updated_at, last_accessed_at, source_query, api_object_type, api_object_id) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            self.conn.commit()
            self.imported_legacy_entries = len(rows)
            vlog(self.verbose, f"  HC CACHE IMPORT source={legacy_path} entries={len(rows)}")
            self._set_meta("legacy_json_imported", f"imported:{len(rows)}")
        else:
            self._set_meta("legacy_json_imported", "no_valid_rows")

    def get(self, cache_key: str) -> Optional[CacheEntry]:
        row = self.conn.execute(
            "SELECT cache_key, label, payload_json, is_empty, payload_size, created_at, updated_at, last_accessed_at, source_query, api_object_type, api_object_id FROM cache_entries WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if not row:
            return None
        try:
            payload = json.loads(row["payload_json"])
        except Exception:
            self.delete(cache_key)
            return None
        self.conn.execute("UPDATE cache_entries SET last_accessed_at = ? WHERE cache_key = ?", (time.time(), cache_key))
        self.conn.commit()
        return CacheEntry(
            key=str(row["cache_key"]),
            label=str(row["label"]),
            payload=payload,
            created_at=float(row["created_at"] or 0.0),
            updated_at=float(row["updated_at"] or 0.0),
            last_accessed_at=float(row["last_accessed_at"] or 0.0),
            is_empty=bool(row["is_empty"]),
            payload_size=int(row["payload_size"] or 0),
            source_query=str(row["source_query"] or ""),
            api_object_type=str(row["api_object_type"] or ""),
            api_object_id=str(row["api_object_id"] or ""),
        )

    def _metadata_from_cache_key(self, cache_key: str) -> Tuple[str, str, str]:
        text = str(cache_key or "")
        if "::" not in text:
            return ("", "", "")
        prefix, rest = text.split("::", 1)
        source_query = ""
        api_object_type = ""
        api_object_id = ""
        if prefix.startswith("search_book"):
            source_query = rest.split("::", 1)[0]
            api_object_type = "book_search"
        elif prefix.startswith("identifier_book_lookup"):
            source_query = rest
            api_object_type = "identifier_lookup"
        elif prefix.startswith("book_editions"):
            api_object_type = "edition"
            api_object_id = rest
        elif prefix.startswith("book_single") or prefix.startswith("books"):
            api_object_type = "book"
            api_object_id = rest
        elif prefix.startswith("series_books"):
            api_object_type = "series"
            api_object_id = rest
        return (source_query[:500], api_object_type[:100], api_object_id[:200])

    def set(self, cache_key: str, label: str, payload: Any, is_empty: bool = False) -> None:
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        now = time.time()
        source_query, api_object_type, api_object_id = self._metadata_from_cache_key(cache_key)
        self.conn.execute(
            """
            INSERT INTO cache_entries(cache_key, label, payload_json, is_empty, payload_size, created_at, updated_at, last_accessed_at, source_query, api_object_type, api_object_id)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                label=excluded.label,
                payload_json=excluded.payload_json,
                is_empty=excluded.is_empty,
                payload_size=excluded.payload_size,
                updated_at=excluded.updated_at,
                last_accessed_at=excluded.last_accessed_at,
                source_query=excluded.source_query,
                api_object_type=excluded.api_object_type,
                api_object_id=excluded.api_object_id
            """,
            (cache_key, label, payload_json, 1 if is_empty else 0, len(payload_json.encode("utf-8")), now, now, now, source_query, api_object_type, api_object_id),
        )
        self.conn.commit()

    def delete(self, cache_key: str) -> None:
        self.conn.execute("DELETE FROM cache_entries WHERE cache_key = ?", (cache_key,))
        self.conn.commit()

    def checkpoint(self) -> None:
        try:
            self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except Exception:
            pass

    def close(self) -> None:
        try:
            self.checkpoint()
        finally:
            self.conn.close()


class HardcoverClient:
    def __init__(self, token: str, cache_path: Path, timeout: int = 15, retries: int = 2, user_agent: str = HARDCOVER_DEFAULT_USER_AGENT, min_interval: float = 1.0, verbose: bool = False, cache_ttl_hours: float = DEFAULT_CACHE_TTL_HOURS, search_cache_ttl_hours: float = DEFAULT_SEARCH_CACHE_TTL_HOURS, empty_cache_ttl_hours: float = DEFAULT_EMPTY_CACHE_TTL_HOURS, edition_cache_ttl_hours: float = DEFAULT_EDITION_CACHE_TTL_HOURS, legacy_cache_json_path: Optional[Path] = None, debug_hardcover: bool = False):
        self.token = token
        self.cache_path = cache_path
        self.timeout = max(3, int(timeout))
        self.retries = max(0, int(retries))
        self.user_agent = (user_agent or HARDCOVER_DEFAULT_USER_AGENT).strip()
        self.base_min_interval = max(0.0, float(min_interval))
        self.min_interval = self.base_min_interval
        self.verbose = bool(verbose)
        self.cache_ttl_hours = max(0.0, float(cache_ttl_hours))
        self.search_cache_ttl_hours = max(0.0, float(search_cache_ttl_hours))
        self.empty_cache_ttl_hours = max(0.0, float(empty_cache_ttl_hours))
        self.edition_cache_ttl_hours = max(0.0, float(edition_cache_ttl_hours))
        self.debug_hardcover = bool(debug_hardcover)
        self._quiet_hc_labels = set(QUIET_HC_LABELS)
        self._last_request_ts = 0.0
        self._cooldown_until_ts = 0.0
        self._rate_limit_streak = 0
        self._curl_path = shutil.which("curl")
        self.last_request_meta = HardcoverRequestMeta()
        self.stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "cache_stale": 0,
            "cache_writes": 0,
            "cache_deletes": 0,
            "network_requests": 0,
            "retries": 0,
            "rate_limit_hits": 0,
            "unauthorized_hits": 0,
            "graphql_errors": 0,
            "empty_responses": 0,
            "request_failures": 0,
            "http_status_counts": Counter(),
            "label_counts": Counter(),
            "cache_hit_labels": Counter(),
            "transport_counts": Counter(),
            "throttle_sleeps": 0,
            "throttle_sleep_seconds": 0.0,
            "suppressed_cache_hit_logs": 0,
            "suppressed_cache_store_logs": 0,
            "suppressed_http_logs": 0,
            "suppressed_throttle_logs": 0,
        }
        self._label_log_counts: Counter = Counter()
        self.cache_store = SQLiteCacheStore(cache_path, verbose=self.verbose, legacy_json_path=legacy_cache_json_path)

    def save_cache(self) -> None:
        self.cache_store.checkpoint()

    def close(self) -> None:
        self.cache_store.close()

    def _should_log_cache_hit(self, label: str) -> bool:
        return self.debug_hardcover or label not in self._quiet_hc_labels

    def _should_log_cache_store(self, label: str) -> bool:
        return self.debug_hardcover or label not in self._quiet_hc_labels

    def _should_log_http(self, meta: HardcoverRequestMeta) -> bool:
        if self.debug_hardcover:
            return True
        if meta.status_code >= 400:
            return True
        label = meta.label or "graphql"
        return label not in self._quiet_hc_labels

    def stats_snapshot(self) -> Dict[str, float]:
        return {
            "network_requests": float(self.stats["network_requests"]),
            "cache_hits": float(self.stats["cache_hits"]),
            "throttle_sleeps": float(self.stats["throttle_sleeps"]),
            "throttle_sleep_seconds": float(self.stats["throttle_sleep_seconds"]),
        }

    def stats_delta_text(self, before: Dict[str, float], after: Optional[Dict[str, float]] = None) -> str:
        now = after or self.stats_snapshot()
        return (
            f"net={int(now['network_requests'] - before['network_requests'])} "
            f"cache={int(now['cache_hits'] - before['cache_hits'])} "
            f"throttle={int(now['throttle_sleeps'] - before['throttle_sleeps'])}/"
            f"{now['throttle_sleep_seconds'] - before['throttle_sleep_seconds']:.2f}s"
        )

    def _cache_ttl_seconds_for_label(self, label: str, is_empty: bool = False) -> float:
        if is_empty:
            return self.empty_cache_ttl_hours * 3600.0
        if label in {"search_book", "identifier_book_lookup", "book_series_memberships", "series_books"}:
            return self.search_cache_ttl_hours * 3600.0
        if label in {"book_editions", "book_editions_single", "editions_by_id"}:
            return self.edition_cache_ttl_hours * 3600.0
        return self.cache_ttl_hours * 3600.0

    def _cache_entry_is_stale(self, entry: CacheEntry) -> bool:
        ttl_seconds = self._cache_ttl_seconds_for_label(entry.label or self._derive_label(entry.key), is_empty=entry.is_empty)
        if ttl_seconds <= 0:
            return False
        updated_at = float(entry.updated_at or entry.created_at or 0.0)
        if updated_at <= 0:
            return True
        return (time.time() - updated_at) > ttl_seconds

    def _cache_age_hours(self, entry: CacheEntry) -> float:
        updated_at = float(entry.updated_at or entry.created_at or 0.0)
        if updated_at <= 0:
            return 0.0
        return max(0.0, (time.time() - updated_at) / 3600.0)

    def _respect_rate_limit(self) -> None:
        now = time.monotonic()
        wait_for_interval = max(0.0, self.min_interval - (now - self._last_request_ts))
        wait_for_cooldown = max(0.0, self._cooldown_until_ts - now)
        wait = max(wait_for_interval, wait_for_cooldown)
        if wait > 0:
            reason = "cooldown" if wait_for_cooldown > wait_for_interval else "rpm_cap"
            self.stats["throttle_sleeps"] += 1
            self.stats["throttle_sleep_seconds"] += float(wait)
            should_log = self.debug_hardcover or wait_for_cooldown > 0 or wait >= 3.0
            if should_log:
                vlog(self.verbose, f"  HC THROTTLE sleep={wait:.2f}s reason={reason} min_interval={self.min_interval:.2f}s cooldown_active={'yes' if wait_for_cooldown > 0 else 'no'}")
            else:
                self.stats["suppressed_throttle_logs"] += 1
            time.sleep(wait)

    def _adjust_interval_from_headers(self, meta: Optional[HardcoverRequestMeta]) -> None:
        if not meta:
            return
        remaining_text = (meta.rate_limit_remaining or "").strip()
        try:
            remaining = int(float(remaining_text)) if remaining_text else None
        except Exception:
            remaining = None
        if remaining is None:
            if self.min_interval > self.base_min_interval:
                self.min_interval = max(self.base_min_interval, round(self.min_interval * 0.9, 2))
            return
        if remaining <= 3:
            self.min_interval = max(self.base_min_interval, 5.0)
        elif remaining <= 10:
            self.min_interval = max(self.base_min_interval, 3.0)
        else:
            self.min_interval = self.base_min_interval

    def _apply_rate_limit_cooldown(self, meta: HardcoverRequestMeta) -> float:
        self._rate_limit_streak += 1
        header_seconds = self._parse_retry_after_seconds(meta.retry_after)
        fallback = 30.0 * (2 ** min(self._rate_limit_streak - 1, 2))
        cooldown_s = max(header_seconds if header_seconds is not None else 0.0, fallback)
        self._cooldown_until_ts = max(self._cooldown_until_ts, time.monotonic() + cooldown_s)
        self.min_interval = max(self.base_min_interval, 5.0)
        return cooldown_s

    def _note_success(self, meta: Optional[HardcoverRequestMeta]) -> None:
        self._last_request_ts = time.monotonic()
        self._rate_limit_streak = 0
        self._cooldown_until_ts = 0.0
        self._adjust_interval_from_headers(meta)

    def _derive_label(self, cache_key: str) -> str:
        base = (cache_key or "").split("::", 1)[0]
        base = re.sub(r"_v\d+$", "", base)
        return base or "graphql"

    def _summarize_cache_key(self, cache_key: str) -> str:
        if not cache_key:
            return "-"
        return cache_key

    @staticmethod
    def _header_lookup(headers: Dict[str, str], name: str) -> str:
        if not headers:
            return ""
        target = name.lower()
        for k, v in headers.items():
            if (k or "").lower() == target:
                return str(v or "").strip()
        return ""

    def _log_request_meta(self, meta: HardcoverRequestMeta) -> None:
        self.last_request_meta = meta
        label = meta.label or "graphql"
        if meta.from_cache:
            self.stats["cache_hits"] += 1
            self.stats["cache_hit_labels"][label] += 1
            if self._should_log_cache_hit(label):
                bits = [f"HC CACHE HIT label={label}"]
                if meta.cache_key:
                    bits.append(f"key={meta.cache_key}")
                if meta.cache_detail:
                    bits.append(f"detail={meta.cache_detail}")
                vlog(self.verbose, "  " + " ".join(bits))
            else:
                self.stats["suppressed_cache_hit_logs"] += 1
            return
        self.stats["cache_misses"] += 1
        self.stats["network_requests"] += 1
        self.stats["label_counts"][label] += 1
        if meta.transport:
            self.stats["transport_counts"][meta.transport] += 1
        if meta.status_code:
            self.stats["http_status_counts"][meta.status_code] += 1
        if meta.status_code == 429:
            self.stats["rate_limit_hits"] += 1
        if meta.status_code == 401:
            self.stats["unauthorized_hits"] += 1
        parts = [f"HC HTTP label={label}", f"attempt={meta.attempt}", f"transport={meta.transport or '-'}", f"status={meta.status_code or '-'}", f"dur={meta.duration_s:.2f}s"]
        if meta.retry_after:
            parts.append(f"retry_after={meta.retry_after}")
        rl = []
        if meta.rate_limit_limit:
            rl.append(f"limit={meta.rate_limit_limit}")
        if meta.rate_limit_remaining:
            rl.append(f"remaining={meta.rate_limit_remaining}")
        if meta.rate_limit_reset:
            rl.append(f"reset={meta.rate_limit_reset}")
        if rl:
            parts.append("rate=" + ",".join(rl))
        if meta.error_summary:
            parts.append(f"error={meta.error_summary}")
        if self._should_log_http(meta):
            vlog(self.verbose, "  " + " ".join(parts))
        else:
            self.stats["suppressed_http_logs"] += 1

    def _parse_retry_after_seconds(self, value: str) -> Optional[float]:
        text = (value or "").strip()
        if not text:
            return None
        try:
            seconds = float(text)
            if seconds >= 0:
                return seconds
        except Exception:
            pass
        return None

    def _compute_backoff_seconds(self, attempt: int, retry_after: str = "", base: float = 2.0, cap: float = 30.0) -> float:
        header_seconds = self._parse_retry_after_seconds(retry_after)
        if header_seconds is not None:
            return max(0.0, min(cap, header_seconds))
        return max(0.0, min(cap, base * (2 ** attempt)))

    def _log_backoff(self, label: str, status_code: int, delay_s: float, source: str) -> None:
        vlog(self.verbose, f"  HC BACKOFF label={label} status={status_code} sleep={delay_s:.2f}s source={source}")

    def _build_stats_summary_lines(self) -> List[str]:
        status_bits = ", ".join(f"{code}:{count}" for code, count in sorted(self.stats["http_status_counts"].items())) or "-"
        label_bits = ", ".join(f"{label}:{count}" for label, count in self.stats["label_counts"].most_common(8)) or "-"
        hit_bits = ", ".join(f"{label}:{count}" for label, count in self.stats["cache_hit_labels"].most_common(8)) or "-"
        transport_bits = ", ".join(f"{name}:{count}" for name, count in self.stats["transport_counts"].items()) or "-"
        total_api_requests = int(self.stats["network_requests"])
        total_cache_hits = int(self.stats["cache_hits"])
        total_processed_request_events = total_api_requests + total_cache_hits
        work_search_requests = int(self.stats["label_counts"].get("search_book", 0) + self.stats["label_counts"].get("identifier_book_lookup", 0))
        work_detail_requests = int(self.stats["label_counts"].get("book_single", 0) + self.stats["label_counts"].get("books", 0))
        edition_requests = int(self.stats["label_counts"].get("book_editions", 0) + self.stats["label_counts"].get("book_editions_single", 0) + self.stats["label_counts"].get("editions_by_id", 0))
        return [
            f"Hardcover request stats: network={self.stats['network_requests']} cache_hits={self.stats['cache_hits']} cache_misses={self.stats['cache_misses']} cache_stale={self.stats['cache_stale']} cache_writes={self.stats['cache_writes']} cache_deletes={self.stats['cache_deletes']} retries={self.stats['retries']} failures={self.stats['request_failures']} graphql_errors={self.stats['graphql_errors']} empty_responses={self.stats['empty_responses']} rate_limit_hits={self.stats['rate_limit_hits']} unauthorized_hits={self.stats['unauthorized_hits']}",
            f"Hardcover request breakdown: work_search={work_search_requests} work_detail={work_detail_requests} edition={edition_requests} throttle_sleeps={self.stats['throttle_sleeps']} throttle_sleep_seconds={self.stats['throttle_sleep_seconds']:.2f}",
            f"Hardcover total API requests sent: {total_api_requests}",
            f"Hardcover total request events handled (API + cache hits): {total_processed_request_events}",
            f"Hardcover suppressed verbose lines: cache_hits={self.stats['suppressed_cache_hit_logs']} cache_stores={self.stats['suppressed_cache_store_logs']} http={self.stats['suppressed_http_logs']} throttle={self.stats['suppressed_throttle_logs']}",
            f"Hardcover legacy JSON cache rows imported: {self.cache_store.imported_legacy_entries}",
            f"Hardcover HTTP status counts: {status_bits}",
            f"Hardcover request labels: {label_bits}",
            f"Hardcover cache-hit labels: {hit_bits}",
            f"Hardcover transports: {transport_bits}",
        ]

    def print_stats_summary(self) -> None:
        for line in self._build_stats_summary_lines():
            print(line)

    def _post_graphql_via_curl(self, payload_text: str, headers: Dict[str, str]) -> Tuple[Dict[str, Any], HardcoverRequestMeta]:
        if not self._curl_path:
            raise RuntimeError("curl is not available")
        t0 = time.monotonic()
        with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as body_f, tempfile.NamedTemporaryFile(mode="w+b", delete=False) as head_f:
            body_path = body_f.name
            head_path = head_f.name
        try:
            cmd = [
                self._curl_path,
                "-sS",
                "-X", "POST",
                GRAPHQL_ENDPOINT,
                "--connect-timeout", str(min(10, self.timeout)),
                "--max-time", str(self.timeout),
                "--header", f"content-type: {headers['content-type']}",
                "--header", f"authorization: {headers['authorization']}",
                "--header", f"user-agent: {headers['user-agent']}",
                "--dump-header", head_path,
                "--output", body_path,
                "--write-out", "%{http_code}",
                "--data-binary", payload_text,
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            duration = time.monotonic() - t0
            status_text = (proc.stdout or "").strip()
            try:
                status_code = int(status_text) if status_text else 0
            except Exception:
                status_code = 0
            header_text = Path(head_path).read_text(encoding="utf-8", errors="replace") if Path(head_path).exists() else ""
            body = Path(body_path).read_text(encoding="utf-8", errors="replace").strip() if Path(body_path).exists() else ""
            header_map: Dict[str, str] = {}
            for line in header_text.splitlines():
                if ":" not in line:
                    continue
                k, v = line.split(":", 1)
                header_map[k.strip()] = v.strip()
            meta = HardcoverRequestMeta(
                transport="curl",
                status_code=status_code,
                duration_s=duration,
                retry_after=self._header_lookup(header_map, "Retry-After"),
                rate_limit_limit=self._header_lookup(header_map, "X-RateLimit-Limit"),
                rate_limit_remaining=self._header_lookup(header_map, "X-RateLimit-Remaining"),
                rate_limit_reset=self._header_lookup(header_map, "X-RateLimit-Reset"),
            )
            self.last_request_meta = meta
            if proc.returncode != 0:
                err = (proc.stderr or "").strip() or body[:500]
                meta.error_summary = f"curl_exit_{proc.returncode}"
                raise HardcoverRequestFailure(f"curl exited {proc.returncode}: {err[:500]}", meta)
            body_preview = body[:500]
            if status_code >= 400:
                if status_code == 401:
                    meta.error_summary = "unauthorized"
                elif status_code == 429:
                    meta.error_summary = "rate_limited"
                else:
                    meta.error_summary = f"http_{status_code}"
                raise HardcoverRequestFailure(f"Hardcover HTTP {status_code}: {body_preview}", meta)
            if not body:
                self.stats["empty_responses"] += 1
                meta.error_summary = "empty_response"
                raise HardcoverRequestFailure("Hardcover API returned empty response", meta)
            data = json.loads(body)
            if data.get("errors"):
                self.stats["graphql_errors"] += 1
                meta.error_summary = "graphql_error"
                raise HardcoverRequestFailure(f"Hardcover API GraphQL error: {data['errors']}", meta)
            return data.get("data", {}), meta
        finally:
            for fp in (body_path, head_path):
                try:
                    os.unlink(fp)
                except Exception:
                    pass

    def _post_graphql_via_urllib(self, payload: bytes, headers: Dict[str, str]) -> Tuple[Dict[str, Any], HardcoverRequestMeta]:
        req = urllib_request.Request(GRAPHQL_ENDPOINT, data=payload, headers=headers, method="POST")
        t0 = time.monotonic()
        with urllib_request.urlopen(req, timeout=self.timeout) as resp:
            status_code = int(getattr(resp, "status", 0) or 0)
            header_map = {k: v for k, v in dict(resp.headers).items()}
            body = resp.read().decode("utf-8", errors="replace").strip()
        duration = time.monotonic() - t0
        meta = HardcoverRequestMeta(
            transport="urllib",
            status_code=status_code,
            duration_s=duration,
            retry_after=self._header_lookup(header_map, "Retry-After"),
            rate_limit_limit=self._header_lookup(header_map, "X-RateLimit-Limit"),
            rate_limit_remaining=self._header_lookup(header_map, "X-RateLimit-Remaining"),
            rate_limit_reset=self._header_lookup(header_map, "X-RateLimit-Reset"),
        )
        self.last_request_meta = meta
        if not body:
            self.stats["empty_responses"] += 1
            meta.error_summary = "empty_response"
            raise HardcoverRequestFailure("Hardcover API returned empty response", meta)
        data = json.loads(body)
        if data.get("errors"):
            self.stats["graphql_errors"] += 1
            meta.error_summary = "graphql_error"
            raise HardcoverRequestFailure(f"Hardcover API GraphQL error: {data['errors']}", meta)
        return data.get("data", {}), meta

    def _post_graphql(self, query: str, variables: Dict[str, Any], label: str = "graphql") -> Dict[str, Any]:
        payload_text = json.dumps({"query": query, "variables": variables}, ensure_ascii=False)
        payload = payload_text.encode("utf-8")
        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {self.token}",
            "user-agent": self.user_agent,
        }
        last_error: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            try:
                self._respect_rate_limit()
                if self._curl_path:
                    data, meta = self._post_graphql_via_curl(payload_text, headers)
                else:
                    data, meta = self._post_graphql_via_urllib(payload, headers)
                meta.label = label
                meta.attempt = attempt + 1
                self._note_success(meta)
                self._log_request_meta(meta)
                return data
            except urllib_error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace").strip() if hasattr(exc, 'read') else ""
                meta = HardcoverRequestMeta(label=label, transport="urllib", status_code=int(getattr(exc, 'code', 0) or 0), attempt=attempt + 1, error_summary="http_error")
                meta.retry_after = self._header_lookup(dict(getattr(exc, 'headers', {}) or {}), "Retry-After")
                meta.rate_limit_limit = self._header_lookup(dict(getattr(exc, 'headers', {}) or {}), "X-RateLimit-Limit")
                meta.rate_limit_remaining = self._header_lookup(dict(getattr(exc, 'headers', {}) or {}), "X-RateLimit-Remaining")
                meta.rate_limit_reset = self._header_lookup(dict(getattr(exc, 'headers', {}) or {}), "X-RateLimit-Reset")
                self._log_request_meta(meta)
                last_error = RuntimeError(f"Hardcover HTTP {exc.code}: {detail[:500]}")
                if exc.code == 429 and attempt < self.retries:
                    self.stats["retries"] += 1
                    cooldown_s = self._apply_rate_limit_cooldown(meta)
                    delay_s = max(cooldown_s, self._compute_backoff_seconds(attempt, meta.retry_after, base=30.0, cap=120.0))
                    self._log_backoff(label, exc.code, delay_s, "retry_after" if self._parse_retry_after_seconds(meta.retry_after) is not None else "rate_limit_cooldown")
                    time.sleep(delay_s)
                    continue
                if exc.code == 401:
                    vlog(self.verbose, f"  HC AUTH WARNING label={label} status=401 unauthorized; not retrying")
                    break
                if 500 <= exc.code < 600 and attempt < self.retries:
                    self.stats["retries"] += 1
                    delay_s = self._compute_backoff_seconds(attempt, "", base=1.0, cap=10.0)
                    self._log_backoff(label, exc.code, delay_s, "exponential")
                    time.sleep(delay_s)
                    continue
                break
            except (urllib_error.URLError, TimeoutError, json.JSONDecodeError, HardcoverRequestFailure, RuntimeError) as exc:
                meta = exc.meta if isinstance(exc, HardcoverRequestFailure) else (self.last_request_meta if self.last_request_meta else HardcoverRequestMeta())
                if not meta.label:
                    meta = HardcoverRequestMeta(label=label, attempt=attempt + 1, error_summary=type(exc).__name__)
                meta.label = label
                meta.attempt = attempt + 1
                if not meta.error_summary:
                    meta.error_summary = type(exc).__name__
                self._log_request_meta(meta)
                last_error = exc
                if meta.status_code == 429 and attempt < self.retries:
                    self.stats["retries"] += 1
                    cooldown_s = self._apply_rate_limit_cooldown(meta)
                    delay_s = max(cooldown_s, self._compute_backoff_seconds(attempt, meta.retry_after, base=30.0, cap=120.0))
                    self._log_backoff(label, meta.status_code, delay_s, "retry_after" if self._parse_retry_after_seconds(meta.retry_after) is not None else "rate_limit_cooldown")
                    time.sleep(delay_s)
                    continue
                if meta.status_code == 401:
                    vlog(self.verbose, f"  HC AUTH WARNING label={label} status=401 unauthorized; not retrying")
                    break
                if meta.status_code and 500 <= meta.status_code < 600 and attempt < self.retries:
                    self.stats["retries"] += 1
                    delay_s = self._compute_backoff_seconds(attempt, "", base=2.0, cap=20.0)
                    self._log_backoff(label, meta.status_code, delay_s, "exponential")
                    time.sleep(delay_s)
                    continue
                if attempt < self.retries and not meta.status_code:
                    self.stats["retries"] += 1
                    delay_s = self._compute_backoff_seconds(attempt, "", base=1.5, cap=10.0)
                    self._log_backoff(label, 0, delay_s, "exponential")
                    time.sleep(delay_s)
                    continue
                break
        self.stats["request_failures"] += 1
        raise RuntimeError(f"Hardcover request failed: {last_error}")

    def cached_query(self, cache_key: str, query: str, variables: Dict[str, Any], force_refresh: bool = False, cache_empty: bool = False) -> Dict[str, Any]:
        label = self._derive_label(cache_key)
        if not force_refresh:
            entry = self.cache_store.get(cache_key)
            if entry is not None:
                if self._cache_entry_is_stale(entry):
                    self.stats["cache_stale"] += 1
                    age_h = self._cache_age_hours(entry)
                    vlog(self.verbose, f"  HC CACHE STALE label={label} key={self._summarize_cache_key(cache_key)} age_h={age_h:.1f}")
                else:
                    if not entry.is_empty or cache_empty:
                        cache_detail = f"bytes={entry.payload_size} age_h={self._cache_age_hours(entry):.1f}"
                        self._log_request_meta(HardcoverRequestMeta(label=label, from_cache=True, cache_key=self._summarize_cache_key(cache_key), cache_detail=cache_detail))
                        return entry.payload or {}
        data = self._post_graphql(query, variables, label=label)
        if data not in (None, {}):
            self.cache_store.set(cache_key, label, data, is_empty=False)
            self.stats["cache_writes"] += 1
            if self._should_log_cache_store(label):
                vlog(self.verbose, f"  HC CACHE STORE label={label} key={self._summarize_cache_key(cache_key)}")
            else:
                self.stats["suppressed_cache_store_logs"] += 1
        elif cache_empty:
            self.cache_store.set(cache_key, label, data or {}, is_empty=True)
            self.stats["cache_writes"] += 1
        else:
            existing = self.cache_store.get(cache_key)
            if existing is not None and existing.is_empty:
                self.cache_store.delete(cache_key)
                self.stats["cache_deletes"] += 1
        return data or {}

    def search_book_ids(self, query_text: str, per_page: int = 5, page: int = 1) -> List[int]:
        cache_key = f"search_book_v2::{query_text}::{per_page}::{page}"
        query = """
        query SearchBooks($query: String!, $perPage: Int!, $page: Int!) {
          search(query: $query, query_type: "Book", per_page: $perPage, page: $page) {
            ids
            results
          }
        }
        """
        data = self.cached_query(cache_key, query, {"query": query_text, "perPage": per_page, "page": page})
        ids = (((data or {}).get("search") or {}).get("ids") or [])
        out: List[int] = []
        for x in ids:
            try:
                out.append(int(x))
            except Exception:
                pass
        return out
    def find_book_ids_by_identifier(self, token: str) -> List[int]:
        token = clean_isbn(token)
        if not token:
            return []
        cache_key = f"identifier_book_lookup::{token}"
        query = """
        query FindBookByIdentifier($token: String!) {
          editions(where: {_or: [{isbn_13: {_eq: $token}}, {isbn_10: {_eq: $token}}, {asin: {_eq: $token}}]}) {
            id
            book_id
          }
        }
        """
        try:
            data = self.cached_query(cache_key, query, {"token": token}, cache_empty=False)
        except Exception:
            return []
        out: List[int] = []
        for ed in data.get("editions", []) or []:
            try:
                bid = int(ed.get("book_id") or 0)
            except Exception:
                bid = 0
            if bid and bid not in out:
                out.append(bid)
        return out

    def _book_from_node(self, b: Dict[str, Any]) -> HardcoverBook:
        authors = authors_from_contributions(b.get("contributions") or [])
        series_parts = []
        for bs in b.get("book_series") or []:
            sname = ((bs.get("series") or {}).get("name") or "").strip()
            pos = bs.get("position")
            if sname:
                series_parts.append(f"{sname} [{pos}]" if pos is not None else sname)
        return HardcoverBook(
            id=int(b["id"]),
            title=smart_title(b.get("title") or ""),
            subtitle=smart_title(b.get("subtitle") or ""),
            authors=authors,
            series=" | ".join(series_parts),
            release_date=b.get("release_date") or "",
            slug=(b.get("slug") or "").strip(),
            users_count=int(b.get("users_count") or 0),
            users_read_count=int(b.get("users_read_count") or 0),
            rating=float(b.get("rating") or 0.0),
            lists_count=int(b.get("lists_count") or 0),
            default_ebook_edition_id=int(b.get("default_ebook_edition_id") or 0),
            default_physical_edition_id=int(b.get("default_physical_edition_id") or 0),
            default_audio_edition_id=int(b.get("default_audio_edition_id") or 0),
            default_cover_edition_id=int(b.get("default_cover_edition_id") or 0),
        )

    def _edition_from_node(self, ed: Dict[str, Any], book_id: int) -> HardcoverEdition:
        authors = authors_from_contributions(ed.get("contributions") or [])
        return HardcoverEdition(
            id=int(ed.get("id")),
            book_id=book_id,
            title=smart_title(ed.get("title") or ""),
            subtitle=smart_title(ed.get("subtitle") or ""),
            authors=authors,
            score=int(ed.get("score") or 0),
            rating=float(ed.get("rating") or 0.0),
            users_count=int(ed.get("users_count") or 0),
            users_read_count=int(ed.get("users_read_count") or 0),
            lists_count=int(ed.get("lists_count") or 0),
            release_date=ed.get("release_date") or "",
            isbn_10=clean_isbn(ed.get("isbn_10") or ""),
            isbn_13=clean_isbn(ed.get("isbn_13") or ""),
            asin=clean_isbn(ed.get("asin") or ""),
            audio_seconds=(int(ed.get("audio_seconds")) if ed.get("audio_seconds") not in (None, "") else None),
            physical_format=(ed.get("physical_format") or ""),
            edition_format=(ed.get("edition_format") or ""),
            reading_format=((ed.get("reading_format") or {}).get("format") or ""),
            language=((ed.get("language") or {}).get("language") or ""),
        )

    def fetch_book_by_id(self, book_id: int, force_refresh: bool = False) -> Optional[HardcoverBook]:
        cache_key = f"book_single_v5::{book_id}"
        query = """
        query FetchBookSingle($id: Int!) {
          books_by_pk(id: $id) {
            id
            title
            subtitle
            release_date
            slug
            users_count
            users_read_count
            rating
            lists_count
            default_ebook_edition_id
            default_physical_edition_id
            default_audio_edition_id
            default_cover_edition_id
            contributions { contribution author { name } }
            book_series { position series { name } }
          }
        }
        """
        data = self.cached_query(cache_key, query, {"id": int(book_id)}, force_refresh=force_refresh, cache_empty=False)
        book = data.get("books_by_pk")
        if not book:
            return None
        return self._book_from_node(book)

    def fetch_books(self, ids: List[int], force_refresh: bool = False) -> Dict[int, HardcoverBook]:
        ids = sorted(set(int(x) for x in ids if x))
        if not ids:
            return {}
        cache_key = "books_v5::" + ",".join(map(str, ids))
        query = """
        query FetchBooks($ids: [Int!]) {
          books(where: {id: {_in: $ids}}) {
            id
            title
            subtitle
            release_date
            slug
            users_count
            users_read_count
            rating
            lists_count
            default_ebook_edition_id
            default_physical_edition_id
            default_audio_edition_id
            default_cover_edition_id
            contributions { contribution author { name } }
            book_series { position series { name } }
          }
        }
        """
        data = self.cached_query(cache_key, query, {"ids": ids}, force_refresh=force_refresh, cache_empty=False)
        out: Dict[int, HardcoverBook] = {}
        for b in data.get("books", []) or []:
            out[int(b["id"])] = self._book_from_node(b)

        missing_ids = [book_id for book_id in ids if book_id not in out]
        for missing_id in missing_ids:
            single = self.fetch_book_by_id(missing_id, force_refresh=True)
            if single:
                out[missing_id] = single
        return out


    def fetch_books_and_editions_for_books(
        self,
        ids: List[int],
        force_refresh: bool = False,
        verbose: bool = False,
        progress_label: str = "candidate-catalogs",
        display_labels: Optional[Dict[int, str]] = None,
    ) -> Tuple[Dict[int, HardcoverBook], Dict[int, List[HardcoverEdition]]]:
        ids = sorted(set(int(x) for x in ids if x))
        if not ids:
            return {}, {}
        query = """
        query FetchBooksAndEditions($ids: [Int!]) {
          books(where: {id: {_in: $ids}}) {
            id
            title
            subtitle
            release_date
            slug
            users_count
            users_read_count
            rating
            lists_count
            default_ebook_edition_id
            default_physical_edition_id
            default_audio_edition_id
            default_cover_edition_id
            contributions { contribution author { name } }
            book_series { position series { name } }
            editions {
              id
              title
              subtitle
              score
              rating
              users_count
              users_read_count
              lists_count
              release_date
              isbn_10
              isbn_13
              asin
              audio_seconds
              physical_format
              edition_format
              reading_format { format }
              language { language }
              contributions { contribution author { name } }
            }
          }
        }
        """
        books_out: Dict[int, HardcoverBook] = {}
        editions_out: Dict[int, List[HardcoverEdition]] = {i: [] for i in ids}
        display_labels = display_labels or {}
        id_chunks = list(chunked(ids, 25))
        total_batches = len(id_chunks)
        if verbose and total_batches:
            vlog(verbose, f"  {progress_label}: fetching {len(ids)} books in {total_batches} batches")
        for batch_idx, id_chunk in enumerate(id_chunks, start=1):
            cache_key = "books_and_editions_v1::" + ",".join(map(str, id_chunk))
            before = self.stats_snapshot()
            data = self.cached_query(cache_key, query, {"ids": id_chunk}, force_refresh=force_refresh, cache_empty=False)
            seen: set[int] = set()
            edition_count = 0
            for b in data.get("books", []) or []:
                book_id = int(b.get("id") or 0)
                if not book_id:
                    continue
                books_out[book_id] = self._book_from_node(b)
                editions: List[HardcoverEdition] = []
                for ed in b.get("editions") or []:
                    editions.append(self._edition_from_node(ed, book_id))
                editions_out[book_id] = editions
                edition_count += len(editions)
                seen.add(book_id)
            missing_ids = [book_id for book_id in id_chunk if book_id not in seen]
            for missing_id in missing_ids:
                single = self.fetch_book_by_id(missing_id, force_refresh=True)
                if single:
                    books_out[missing_id] = single
                single_editions = self.fetch_editions_for_books([missing_id], force_refresh=True).get(missing_id, [])
                editions_out[missing_id] = single_editions
                edition_count += len(single_editions)
                if single or single_editions:
                    seen.add(missing_id)
            if verbose:
                after = self.stats_snapshot()
                net_delta = int(after["network_requests"] - before["network_requests"])
                cache_delta = int(after["cache_hits"] - before["cache_hits"])
                if net_delta and cache_delta:
                    batch_cache_status = "mixed"
                elif net_delta:
                    batch_cache_status = "miss"
                elif cache_delta:
                    batch_cache_status = "hit"
                else:
                    batch_cache_status = "none"
                book_display_items = [display_labels.get(book_id) or str(book_id) for book_id in id_chunk]
                book_sample = preview_names(book_display_items, limit=min(4, len(id_chunk)), max_len=42)
                vlog(
                    verbose,
                    f"    [{batch_idx}/{total_batches}] books={book_sample} matched_books={len(seen)}/{len(id_chunk)} editions={edition_count} batch_cache={batch_cache_status} {self.stats_delta_text(before, after)}",
                )
        return books_out, editions_out

    def fetch_editions_for_books(
        self,
        ids: List[int],
        force_refresh: bool = False,
        verbose: bool = False,
        progress_label: str = "edition-catalogs",
        display_labels: Optional[Dict[int, str]] = None,
    ) -> Dict[int, List[HardcoverEdition]]:
        ids = sorted(set(int(x) for x in ids if x))
        if not ids:
            return {}
        query = """
        query FetchBookEditions($ids: [Int!]) {
          books(where: {id: {_in: $ids}}) {
            id
            editions {
              id
              title
              subtitle
              score
              rating
              users_count
              users_read_count
              lists_count
              release_date
              isbn_10
              isbn_13
              asin
              audio_seconds
              physical_format
              edition_format
              reading_format { format }
              language { language }
              contributions { contribution author { name } }
            }
          }
        }
        """
        out: Dict[int, List[HardcoverEdition]] = {i: [] for i in ids}
        display_labels = display_labels or {}
        id_chunks = list(chunked(ids, 25))
        total_batches = len(id_chunks)
        if verbose and total_batches:
            vlog(verbose, f"  {progress_label}: fetching {len(ids)} books in {total_batches} batches")
        for batch_idx, id_chunk in enumerate(id_chunks, start=1):
            cache_key = "book_editions_v5::" + ",".join(map(str, id_chunk))
            before = self.stats_snapshot()
            data = self.cached_query(cache_key, query, {"ids": id_chunk}, force_refresh=force_refresh, cache_empty=False)
            seen: set[int] = set()
            edition_count = 0
            for b in data.get("books", []) or []:
                book_id = int(b.get("id"))
                editions: List[HardcoverEdition] = []
                for ed in b.get("editions") or []:
                    editions.append(self._edition_from_node(ed, book_id))
                edition_count += len(editions)
                out[book_id] = editions
                seen.add(book_id)

            missing_ids = [book_id for book_id in id_chunk if book_id not in seen]
            for missing_id in missing_ids:
                single_key = f"book_editions_single_v4::{missing_id}"
                single_query = """
                query FetchBookEditionsSingle($id: Int!) {
                  books(where: {id: {_eq: $id}}) {
                    id
                    editions {
                      id
                      title
                      subtitle
                      score
                      rating
                      users_count
                      users_read_count
                      lists_count
                      release_date
                      isbn_10
                      isbn_13
                      asin
                      audio_seconds
                      physical_format
                      edition_format
                      reading_format { format }
                      language { language }
                      contributions { contribution author { name } }
                    }
                  }
                }
                """
                single_data = self.cached_query(single_key, single_query, {"id": missing_id}, force_refresh=True, cache_empty=False)
                books = single_data.get("books", []) or []
                if not books:
                    continue
                editions = [self._edition_from_node(ed, missing_id) for ed in (books[0].get("editions") or [])]
                out[missing_id] = editions
                edition_count += len(editions)
                seen.add(missing_id)
            if verbose:
                after = self.stats_snapshot()
                net_delta = int(after["network_requests"] - before["network_requests"])
                cache_delta = int(after["cache_hits"] - before["cache_hits"])
                if net_delta and cache_delta:
                    batch_cache_status = "mixed"
                elif net_delta:
                    batch_cache_status = "miss"
                elif cache_delta:
                    batch_cache_status = "hit"
                else:
                    batch_cache_status = "none"
                book_display_items = [display_labels.get(book_id) or str(book_id) for book_id in id_chunk]
                book_sample = preview_names(book_display_items, limit=min(4, len(id_chunk)), max_len=42)
                vlog(
                    verbose,
                    f"    [{batch_idx}/{total_batches}] books={book_sample} matched_books={len(seen)}/{len(id_chunk)} editions={edition_count} batch_cache={batch_cache_status} {self.stats_delta_text(before, after)}",
                )
        return out


    def fetch_editions_by_ids(self, edition_ids: List[int], force_refresh: bool = False) -> Dict[int, HardcoverEdition]:
        edition_ids = sorted(set(int(x) for x in edition_ids if x))
        if not edition_ids:
            return {}
        cache_key = "editions_by_id_v2::" + ",".join(map(str, edition_ids))
        query = """
        query FetchEditionsByIds($ids: [Int!]) {
          editions(where: {id: {_in: $ids}}) {
            id
            book_id
            title
            subtitle
            score
            rating
            users_count
            users_read_count
            lists_count
            release_date
            isbn_10
            isbn_13
            asin
            audio_seconds
            physical_format
            edition_format
            reading_format { format }
            language { language }
            contributions { contribution author { name } }
          }
        }
        """
        data = self.cached_query(cache_key, query, {"ids": edition_ids}, force_refresh=force_refresh, cache_empty=False)
        out: Dict[int, HardcoverEdition] = {}
        for ed in data.get("editions", []) or []:
            try:
                edition = self._edition_from_node(ed, int(ed.get("book_id") or 0))
                out[int(edition.id)] = edition
            except Exception:
                continue
        return out

    def fetch_book_series_memberships(
        self,
        book_ids: List[int],
        force_refresh: bool = False,
        verbose: bool = False,
        progress_label: str = "memberships",
        display_labels: Optional[Dict[int, str]] = None,
    ) -> Dict[int, List[Dict[str, Any]]]:
        book_ids = sorted(set(int(x) for x in book_ids if x))
        if not book_ids:
            return {}
        out: Dict[int, List[Dict[str, Any]]] = {book_id: [] for book_id in book_ids}
        query = """
        query FetchBookSeriesMemberships($ids: [Int!]) {
          book_series(
            where: {book_id: {_in: $ids}, compilation: {_eq: false}}
            order_by: [{series_id: asc}, {position: asc}, {id: asc}]
          ) {
            book_id
            position
            series_id
            compilation
            book {
              id
              title
              canonical {
                id
                title
              }
            }
            series {
              id
              name
              slug
              canonical_id
              is_completed
              books_count
              primary_books_count
              canonical {
                id
                name
                slug
                is_completed
                books_count
                primary_books_count
              }
            }
          }
        }
        """
        display_labels = display_labels or {}
        id_chunks = list(chunked(book_ids, 50))
        total_batches = len(id_chunks)
        if verbose and total_batches:
            vlog(verbose, f"  {progress_label}: fetching {len(book_ids)} books in {total_batches} batches")
        for batch_idx, id_chunk in enumerate(id_chunks, start=1):
            cache_key = "book_series_memberships::" + ",".join(map(str, id_chunk))
            before = self.stats_snapshot()
            data = self.cached_query(cache_key, query, {"ids": id_chunk}, force_refresh=force_refresh, cache_empty=False)
            matched_book_ids: Set[int] = set()
            matched_series_ids: Set[int] = set()
            matched_book_labels: Dict[int, str] = {}
            for node in data.get("book_series", []) or []:
                series = node.get("series") or {}
                canonical = series.get("canonical") or {}
                chosen_series = canonical or series
                raw_series_id = series.get("id")
                chosen_series_id = chosen_series.get("id") or raw_series_id
                if not chosen_series_id:
                    continue
                book_id = int(node.get("book_id"))
                book_node = node.get("book") or {}
                canonical_book = book_node.get("canonical") or {}
                if book_id and book_id not in matched_book_labels:
                    title = smart_title(canonical_book.get("title") or book_node.get("title") or "")
                    matched_book_labels[book_id] = f"{title} [{book_id}]" if title else str(book_id)
                matched_book_ids.add(book_id)
                matched_series_ids.add(int(chosen_series_id))
                out.setdefault(book_id, []).append({
                    "book_id": book_id,
                    "position": node.get("position"),
                    "series_id": int(chosen_series_id),
                    "series_name": smart_title(chosen_series.get("name") or series.get("name") or ""),
                    "series_slug": (chosen_series.get("slug") or series.get("slug") or "").strip(),
                    "raw_series_id": int(raw_series_id) if raw_series_id else int(chosen_series_id),
                    "raw_series_name": smart_title(series.get("name") or ""),
                    "compilation": bool(node.get("compilation")),
                    "is_completed": chosen_series.get("is_completed"),
                    "books_count": chosen_series.get("books_count"),
                    "primary_books_count": chosen_series.get("primary_books_count"),
                })
            if verbose:
                after = self.stats_snapshot()
                net_delta = int(after["network_requests"] - before["network_requests"])
                cache_delta = int(after["cache_hits"] - before["cache_hits"])
                if net_delta and cache_delta:
                    batch_cache_status = "mixed"
                elif net_delta:
                    batch_cache_status = "miss"
                elif cache_delta:
                    batch_cache_status = "hit"
                else:
                    batch_cache_status = "none"
                book_display_items = [display_labels.get(book_id) or matched_book_labels.get(book_id) or str(book_id) for book_id in id_chunk]
                book_sample = preview_names(book_display_items, limit=min(6, len(id_chunk)), max_len=40)
                vlog(
                    verbose,
                    f"    [{batch_idx}/{total_batches}] books={book_sample} books_with_series={len(matched_book_ids)}/{len(id_chunk)} unique_series={len(matched_series_ids)} batch_cache={batch_cache_status} {self.stats_delta_text(before, after)}",
                )
        return out

    def fetch_series_books(self, series_ids: List[int], force_refresh: bool = False, verbose: bool = False, progress_label: str = "series-catalogs") -> Dict[int, Dict[str, Any]]:
        series_ids = sorted(set(int(x) for x in series_ids if x))
        if not series_ids:
            return {}
        out: Dict[int, Dict[str, Any]] = {}
        query = """
        query FetchSeriesBooks($ids: [Int!]) {
          series(where: {id: {_in: $ids}}) {
            id
            name
            slug
            is_completed
            books_count
            primary_books_count
            author {
              id
              name
            }
            book_series(
              where: {compilation: {_eq: false}}
              order_by: [{position: asc}, {id: asc}]
            ) {
              position
              details
              featured
              book {
                id
                title
                subtitle
                release_date
                slug
                users_count
                users_read_count
                rating
                lists_count
                canonical_id
                state
                canonical {
                  id
                  title
                  slug
                }
                contributions {
                  contribution
                  author { name }
                }
                default_ebook_edition {
                  edition_format
                  reading_format { format }
                  language { language }
                }
                default_physical_edition {
                  edition_format
                  reading_format { format }
                  language { language }
                }
                default_cover_edition {
                  edition_format
                  reading_format { format }
                  language { language }
                }
                default_audio_edition {
                  edition_format
                  reading_format { format }
                  language { language }
                }
              }
            }
          }
        }
        """
        def _edition_language(node: Dict[str, Any], key: str) -> str:
            edition = node.get(key) or {}
            language = (edition.get("language") or {}).get("language") or ""
            return str(language or "")
        id_chunks = list(chunked(series_ids, 25))
        total_batches = len(id_chunks)
        if verbose and total_batches:
            vlog(verbose, f"  {progress_label}: fetching {len(series_ids)} series in {total_batches} batches")
        for batch_idx, id_chunk in enumerate(id_chunks, start=1):
            cache_key = "series_books_v3::" + ",".join(map(str, id_chunk))
            before = self.stats_snapshot()
            data = self.cached_query(cache_key, query, {"ids": id_chunk}, force_refresh=force_refresh, cache_empty=False)
            batch_books = 0
            matched_series_ids: Set[int] = set()
            matched_series_labels: List[str] = []
            for series in data.get("series", []) or []:
                sid = int(series.get("id"))
                books: List[Dict[str, Any]] = []
                seen_book_ids: set[int] = set()
                series_name = smart_title(series.get("name") or "")
                matched_series_ids.add(sid)
                if series_name:
                    matched_series_labels.append(f"{series_name} [{sid}]")
                else:
                    matched_series_labels.append(str(sid))
                for bs in series.get("book_series") or []:
                    book = bs.get("book") or {}
                    if not book.get("id"):
                        continue
                    book_id = int(book.get("id"))
                    if book_id in seen_book_ids:
                        continue
                    seen_book_ids.add(book_id)
                    batch_books += 1
                    authors = authors_from_contributions(book.get("contributions") or [])
                    canonical = book.get("canonical") or {}
                    books.append({
                        "book_id": book_id,
                        "position": bs.get("position"),
                        "details": str(bs.get("details") or ""),
                        "featured": bool(bs.get("featured")),
                        "title": smart_title(book.get("title") or ""),
                        "subtitle": smart_title(book.get("subtitle") or ""),
                        "authors": authors,
                        "release_date": book.get("release_date") or "",
                        "slug": (book.get("slug") or "").strip(),
                        "canonical_id": int(book.get("canonical_id") or canonical.get("id") or 0),
                        "canonical_title": smart_title(canonical.get("title") or ""),
                        "canonical_slug": (canonical.get("slug") or "").strip(),
                        "state": str(book.get("state") or ""),
                        "users_count": int(book.get("users_count") or 0),
                        "users_read_count": int(book.get("users_read_count") or 0),
                        "rating": float(book.get("rating") or 0.0),
                        "lists_count": int(book.get("lists_count") or 0),
                        "default_ebook_language": _edition_language(book, "default_ebook_edition"),
                        "default_physical_language": _edition_language(book, "default_physical_edition"),
                        "default_cover_language": _edition_language(book, "default_cover_edition"),
                        "default_audio_language": _edition_language(book, "default_audio_edition"),
                    })
                series_author = series.get("author") or {}
                total_users = sum(int(book.get("users_count") or 0) for book in books)
                total_users_read = sum(int(book.get("users_read_count") or 0) for book in books)
                total_lists = sum(int(book.get("lists_count") or 0) for book in books)
                top_book_users_read = max((int(book.get("users_read_count") or 0) for book in books), default=0)
                out[sid] = {
                    "series_id": sid,
                    "series_name": series_name,
                    "series_slug": (series.get("slug") or "").strip(),
                    "series_author_id": int(series_author.get("id") or 0),
                    "series_author_name": normalize_person_name((series_author.get("name") or "").strip()),
                    "is_completed": series.get("is_completed"),
                    "books_count": int(series.get("books_count") or 0),
                    "primary_books_count": int(series.get("primary_books_count") or 0),
                    "series_users_count_total": total_users,
                    "series_users_read_count_total": total_users_read,
                    "series_lists_count_total": total_lists,
                    "series_top_book_users_read_count": top_book_users_read,
                    "books": books,
                }
            if verbose:
                after = self.stats_snapshot()
                net_delta = int(after["network_requests"] - before["network_requests"])
                cache_delta = int(after["cache_hits"] - before["cache_hits"])
                if net_delta and cache_delta:
                    batch_cache_status = "mixed"
                elif net_delta:
                    batch_cache_status = "miss"
                elif cache_delta:
                    batch_cache_status = "hit"
                else:
                    batch_cache_status = "none"
                series_display_items = matched_series_labels + [str(x) for x in id_chunk if int(x) not in matched_series_ids]
                series_sample = preview_names(series_display_items or [str(x) for x in id_chunk], limit=max(3, len(id_chunk)), max_len=30)
                vlog(
                    verbose,
                    f"    [{batch_idx}/{total_batches}] series={series_sample} matched_series={len(matched_series_ids)}/{len(id_chunk)} books_returned={batch_books} batch_cache={batch_cache_status} {self.stats_delta_text(before, after)}",
                )
        return out

    def fetch_books_for_authors(self, author_names: List[str], force_refresh: bool = False, verbose: bool = False, progress_label: str = "author-catalogs") -> Dict[str, Dict[str, Any]]:
        author_names = [smart_title(name) for name in author_names if smart_title(name)]
        if not author_names:
            return {}
        out: Dict[str, Dict[str, Any]] = {}
        query = """
        query FetchBooksForAuthors($names: [String!]) {
          authors(where: {name: {_in: $names}}) {
            id
            name
            canonical { id name }
            contributions {
              contribution
              book {
                id
                title
                subtitle
                release_date
                slug
                users_count
                users_read_count
                rating
                lists_count
                book_series {
                  position
                  series {
                    id
                    name
                    slug
                    is_completed
                    books_count
                    primary_books_count
                    canonical {
                      id
                      name
                      slug
                      is_completed
                      books_count
                      primary_books_count
                    }
                  }
                }
              }
            }
          }
        }
        """
        name_chunks = list(chunked(author_names, 5))
        total_batches = len(name_chunks)
        if verbose and total_batches:
            vlog(verbose, f"  {progress_label}: fetching {len(author_names)} authors in {total_batches} batches")
        for batch_idx, name_chunk in enumerate(name_chunks, start=1):
            cache_key = "author_books_v1::" + "|".join(name_chunk)
            before = self.stats_snapshot()
            data = self.cached_query(cache_key, query, {"names": name_chunk}, force_refresh=force_refresh, cache_empty=False)
            batch_books = 0
            matched_author_keys: Set[str] = set()
            matched_author_labels: List[str] = []
            for author in data.get("authors", []) or []:
                canonical = author.get("canonical") or {}
                display_name = normalize_person_name((canonical.get("name") or author.get("name") or "").strip())
                author_id = int(canonical.get("id") or author.get("id") or 0)
                key = normalize_author_key(display_name)
                if not key:
                    continue
                matched_author_keys.add(key)
                if author_id:
                    matched_author_labels.append(f"{display_name} [{author_id}]")
                elif display_name:
                    matched_author_labels.append(display_name)
                entry = out.setdefault(key, {
                    "author_key": key,
                    "author_id": author_id,
                    "author_name": display_name,
                    "matched_names": Counter(),
                    "books": [],
                })
                entry["matched_names"][normalize_person_name((author.get("name") or "").strip()) or display_name] += 1
                seen_book_ids: Set[int] = {int(book.get("book_id") or 0) for book in entry["books"] if int(book.get("book_id") or 0)}
                for contribution in author.get("contributions") or []:
                    if not is_primary_author_contribution((contribution.get("contribution") or "")):
                        continue
                    book = contribution.get("book") or {}
                    book_id = int(book.get("id") or 0)
                    if not book_id or book_id in seen_book_ids:
                        continue
                    seen_book_ids.add(book_id)
                    batch_books += 1
                    memberships: List[Dict[str, Any]] = []
                    seen_series_ids: Set[int] = set()
                    for bs in book.get("book_series") or []:
                        series = bs.get("series") or {}
                        canonical_series = series.get("canonical") or {}
                        chosen_series = canonical_series or series
                        series_id = int(chosen_series.get("id") or series.get("id") or 0)
                        if not series_id or series_id in seen_series_ids:
                            continue
                        seen_series_ids.add(series_id)
                        memberships.append({
                            "series_id": series_id,
                            "series_name": smart_title(chosen_series.get("name") or series.get("name") or ""),
                            "series_slug": (chosen_series.get("slug") or series.get("slug") or "").strip(),
                            "is_completed": chosen_series.get("is_completed"),
                            "books_count": int(chosen_series.get("books_count") or 0),
                            "primary_books_count": int(chosen_series.get("primary_books_count") or 0),
                            "position": bs.get("position"),
                        })
                    entry["books"].append({
                        "book_id": book_id,
                        "title": smart_title(book.get("title") or ""),
                        "subtitle": smart_title(book.get("subtitle") or ""),
                        "release_date": book.get("release_date") or "",
                        "slug": (book.get("slug") or "").strip(),
                        "users_count": int(book.get("users_count") or 0),
                        "users_read_count": int(book.get("users_read_count") or 0),
                        "rating": float(book.get("rating") or 0.0),
                        "lists_count": int(book.get("lists_count") or 0),
                        "series_memberships": memberships,
                    })
            if verbose:
                after = self.stats_snapshot()
                net_delta = int(after["network_requests"] - before["network_requests"])
                cache_delta = int(after["cache_hits"] - before["cache_hits"])
                if net_delta and cache_delta:
                    batch_cache_status = "mixed"
                elif net_delta:
                    batch_cache_status = "miss"
                elif cache_delta:
                    batch_cache_status = "hit"
                else:
                    batch_cache_status = "none"
                unmatched_author_names = [name for name in name_chunk if normalize_author_key(name) not in matched_author_keys]
                author_display_items = matched_author_labels + unmatched_author_names
                author_sample = preview_names(author_display_items or name_chunk, limit=len(name_chunk), max_len=36)
                vlog(
                    verbose,
                    f"    [{batch_idx}/{total_batches}] authors={author_sample} matched={len(matched_author_keys)}/{len(name_chunk)} books={batch_books} batch_cache={batch_cache_status} {self.stats_delta_text(before, after)}",
                )
        return out

def build_search_queries(record: BookRecord, file_work: FileWork, embedded: EmbeddedMeta, content: ContentSignals) -> List[str]:
    queries: List[str] = []
    current_hc_id = extract_numeric_id(record.calibre_hardcover_id)

    exact_tokens: List[str] = []
    for x in record.isbn_candidates + record.asin_candidates:
        if x:
            exact_tokens.append(clean_isbn(x))
    for v in embedded.embedded_identifiers.values():
        cv = clean_isbn(v)
        if cv:
            exact_tokens.append(cv)

    title_candidates: List[str] = []
    for raw in [file_work.title, embedded.embedded_title, record.calibre_title, content.inferred_title_from_content]:
        q = normalize_search_query_title(raw or "")
        if q and q not in title_candidates:
            title_candidates.append(q)

    bare_title = title_candidates[0] if title_candidates else ""
    author_parts = split_author_like_string(file_work.authors or record.calibre_authors)
    primary_author_name = author_parts[0] if author_parts else ""

    for token in exact_tokens:
        if token and is_searchworthy_token(token, current_hc_id):
            queries.append(token)

    if bare_title and primary_author_name:
        queries.append(f"{bare_title} {primary_author_name}")
    if bare_title:
        queries.append(bare_title)

    out: List[str] = []
    seen = set()
    for q in queries:
        q = re.sub(r"\s+", " ", html.unescape(q).strip())
        if q and q not in seen and is_searchworthy_token(q, current_hc_id):
            out.append(q)
            seen.add(q)
    return out

def score_candidate_against_file(file_work: FileWork, record: BookRecord, hc_book: HardcoverBook, preferred_edition: Optional[HardcoverEdition] = None) -> Tuple[float, MatchScores, str]:
    title_score = max(
        bare_title_similarity(file_work.title, hc_book.title),
        bare_title_similarity(strip_series_suffix(file_work.title), hc_book.title),
    ) if file_work.title else 0.0
    if title_score == 0 and record.calibre_title:
        title_score = bare_title_similarity(record.calibre_title, hc_book.title) * 0.7

    candidate_authors = effective_candidate_authors(hc_book, preferred_edition)
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

    series_score = 10.0 if (record.calibre_series and hc_book.series and norm(record.calibre_series) in norm(hc_book.series)) else 0.0
    total = title_score * 70 + author_score * 20 + series_score - title_marketing_penalty(hc_book.title)

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
    if preferred_edition and hc_book.default_ebook_edition_id and int(preferred_edition.id) == int(hc_book.default_ebook_edition_id):
        total += 1.0

    total = round(total, 2)
    reasons = []
    if title_score >= 0.98: reasons.append("exact-title")
    elif title_score >= 0.90: reasons.append("close-title")
    elif title_score >= 0.75: reasons.append("partial-title")
    if author_score >= 0.99: reasons.append("author")
    elif author_score >= 0.50: reasons.append("partial-author")
    elif no_author_overlap and effective_file_authors: reasons.append("author-mismatch")
    if series_score: reasons.append("series")
    if preferred_edition and is_audio_edition(preferred_edition): reasons.append("audio-edition")
    if no_author_overlap and candidate_contributors >= max(3, file_contributors + 2): reasons.append("multi-contributor-mismatch")
    if title_marketing_penalty(hc_book.title): reasons.append("marketing-title")
    return total, MatchScores(round(title_score,3), round(author_score,3), round(series_score,3), total), ",".join(reasons)

def fetch_current_book_resilient(hc: HardcoverClient, book_id: int) -> Optional[HardcoverBook]:
    book = hc.fetch_books([int(book_id)], force_refresh=False).get(int(book_id))
    if book:
        return book
    for _ in range(2):
        try:
            book = hc.fetch_book_by_id(int(book_id), force_refresh=True)
        except Exception:
            book = None
        if book:
            return book
        time.sleep(0.25)
    return None

def validate_current_hardcover_link(record: BookRecord, file_work: FileWork, embedded: EmbeddedMeta, hc: HardcoverClient, verbose: bool = False) -> Tuple[Optional[HardcoverBook], EditionChoiceInfo, float, MatchScores, Optional[bool], str]:
    current_hc_id = extract_numeric_id(record.calibre_hardcover_id)
    if not current_hc_id:
        vlog(verbose, "  direct current-id validation failed: no current hardcover-id")
        return None, EditionChoiceInfo(), 0.0, MatchScores(), None, "no-current-hc-id"
    try:
        current_book = fetch_current_book_resilient(hc, int(current_hc_id))
        if not current_book:
            vlog(verbose, f"  direct current-id lookup returned empty for current id {current_hc_id}")
            return None, EditionChoiceInfo(), 0.0, MatchScores(), None, "current-hc-id-lookup-empty"
        current_editions = hc.fetch_editions_for_books([current_book.id]).get(current_book.id, [])
        if not current_editions:
            current_editions = hc.fetch_editions_for_books([current_book.id], force_refresh=True).get(current_book.id, [])
        current_ranked_editions = rank_candidate_editions(record, file_work, embedded, current_book, current_editions)
        current_edition_choice = choose_preferred_edition_info(record, file_work, embedded, current_book, current_editions)
        current_edition = current_edition_choice.chosen
        current_score, current_breakdown, why = score_candidate_against_file(file_work, record, current_book, current_edition)
        current_score = book_selection_adjusted_score(current_score, file_work, current_book, current_edition)
        current_ok = current_score >= 75
        gap_tier = edition_gap_tier(current_edition_choice.score_gap, bool(current_edition_choice.runner_up))
        preferred_summary = compact_edition_marker(current_edition, current_edition_choice.chosen_score) if current_edition else "-"
        vlog(verbose, f"  current hc={compact_book_marker(current_book)} match={current_score:.2f} verified={fmt_bool(current_ok)} why={why or '-'} editions={len(current_editions)}")
        if current_edition:
            vlog(verbose, f"  current preferred={preferred_summary} gap={current_edition_choice.score_gap:.1f} {gap_tier}")
        alt_editions = compact_ranked_editions_from_choice(current_ranked_editions, skip=1, limit=2)
        if alt_editions != "-":
            vlog(verbose, f"  current alternatives={alt_editions}")
        return current_book, current_edition_choice, current_score, current_breakdown, current_ok, why or "current_hardcover_id"
    except Exception as exc:
        vlog(verbose, f"  HARDCOVER FETCH ERROR calibre_id={record.calibre_book_id} hc_id={current_hc_id}: {exc}")
        vlog(verbose, "  direct current-id validation failed")
        return None, EditionChoiceInfo(), 0.0, MatchScores(), None, f"current-hc-fetch-error:{exc}"



def choose_best_candidate(record: BookRecord, file_work: FileWork, embedded: EmbeddedMeta, content: ContentSignals, hc: HardcoverClient, verbose: bool = False) -> Tuple[Optional[HardcoverBook], EditionChoiceInfo, float, MatchScores, str]:
    seen_ids: List[int] = []
    current_hc_id = extract_numeric_id(record.calibre_hardcover_id)
    if current_hc_id:
        try:
            seen_ids.append(int(current_hc_id))
        except Exception:
            pass

    exact_tokens: List[str] = []
    for x in record.isbn_candidates + record.asin_candidates + list(embedded.embedded_identifiers.values()):
        token = clean_isbn(str(x or ""))
        if token and token not in exact_tokens:
            exact_tokens.append(token)

    for token in exact_tokens[:3]:
        ids = hc.find_book_ids_by_identifier(token)
        if ids:
            vlog(verbose, f"  identifier {token!r} -> ids={ids[:8]}")
        for bid in ids:
            if bid not in seen_ids:
                seen_ids.append(bid)

    search_queries = build_search_queries(record, file_work, embedded, content)
    vlog(verbose, f"  search queries={search_queries[:4]}")
    for q in search_queries[:4]:
        try:
            ids = hc.search_book_ids(q, per_page=5, page=1)
            vlog(verbose, f"  search {q!r} -> ids={ids[:8]}")
        except Exception as exc:
            vlog(verbose, f"  HARDCOVER SEARCH ERROR query={q!r}: {exc}")
            continue
        for i in ids:
            if i not in seen_ids:
                seen_ids.append(i)
        if len(seen_ids) >= 12:
            break
    if not seen_ids:
        return None, EditionChoiceInfo(), 0.0, MatchScores(), "no-api-candidate"
    try:
        books = hc.fetch_books(seen_ids)
        editions_by_book = hc.fetch_editions_for_books(list(books.keys()))
        vlog(verbose, f"  candidate fetch books={len(books)} editions={sum(len(v) for v in editions_by_book.values())}")
    except Exception as exc:
        vlog(verbose, f"  HARDCOVER FETCH ERROR ids={seen_ids}: {exc}")
        return None, EditionChoiceInfo(), 0.0, MatchScores(), f"fetch-error:{exc}"
    scored: List[Tuple[float, MatchScores, str, HardcoverBook, EditionChoiceInfo]] = []
    for _, book in books.items():
        edition_choice = choose_preferred_edition_info(record, file_work, embedded, book, editions_by_book.get(book.id, []))
        preferred = edition_choice.chosen
        s, breakdown, why = score_candidate_against_file(file_work, record, book, preferred)
        adjusted = book_selection_adjusted_score(s, file_work, book, preferred)
        scored.append((adjusted, breakdown, why, book, edition_choice))
    scored.sort(key=lambda x: x[0], reverse=True)
    if verbose and scored:
        preview = []
        for s, _bd, _w, b, ed_choice in scored[:4]:
            ed = ed_choice.chosen
            suffix = f" | preferred={compact_edition_marker(ed, ed_choice.chosen_score)} gap={ed_choice.score_gap:.1f}" if ed else ""
            preview.append(f"{compact_book_marker(b)} score={s:.2f}{suffix}")
        vlog(verbose, f"  search candidates={preview}")
    if not scored:
        return None, EditionChoiceInfo(), 0.0, MatchScores(), "no-book-details"
    best_score, best_breakdown, why, best_book, best_edition_choice = scored[0]
    if verbose and best_book:
        best_ranked_editions = rank_candidate_editions(record, file_work, embedded, best_book, editions_by_book.get(best_book.id, []))
        if best_edition_choice.chosen:
            vlog(verbose, f"  search best preferred={compact_edition_marker(best_edition_choice.chosen, best_edition_choice.chosen_score)} gap={best_edition_choice.score_gap:.1f} {edition_gap_tier(best_edition_choice.score_gap, bool(best_edition_choice.runner_up))}")
        best_alt_editions = compact_ranked_editions_from_choice(best_ranked_editions, skip=1, limit=2)
        if best_alt_editions != "-":
            vlog(verbose, f"  search best alternatives={best_alt_editions}")
    return best_book, best_edition_choice, best_score, best_breakdown, why or "search"


def should_search_after_current_validation(record: BookRecord, current_book: Optional[HardcoverBook], current_score: float) -> Tuple[bool, str]:
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

def decide_action(record: BookRecord, file_work: FileWork, embedded: EmbeddedMeta, content: ContentSignals, current_book: Optional[HardcoverBook], current_edition: Optional[HardcoverEdition], current_score: float, best_book: Optional[HardcoverBook], best_edition: Optional[HardcoverEdition], best_score: float) -> Decision:
    file_vs_calibre_title = bare_title_similarity(file_work.title, record.calibre_title) if file_work.title else 0.0
    file_vs_calibre_auth = author_similarity(file_work.authors, record.calibre_authors) if file_work.authors else 0.0
    file_vs_current_title = bare_title_similarity(file_work.title, current_book.title) if (file_work.title and current_book) else 0.0
    file_vs_current_auth = author_coverage(file_work.authors, current_book.authors) if (file_work.authors and current_book) else 0.0

    current_match = current_book is not None and current_score >= 75
    best_match = best_book is not None and best_score >= 80

    cleaned_calibre = clean_title_for_matching(record.calibre_title)
    title_needs_cleanup = cleaned_calibre != smart_title(record.calibre_title)

    file_conflicts_with_calibre = ((file_work.title and file_vs_calibre_title < 0.55) or (file_work.authors and file_vs_calibre_auth < 0.40))
    file_conflicts_with_current = current_book is not None and current_score < 55
    best_candidate_authors = effective_candidate_authors(best_book, best_edition) if best_book else ""
    best_file_title_score = bare_title_similarity(file_work.title, best_book.title) if (file_work.title and best_book) else 0.0
    best_file_author_score = author_coverage(file_work.authors, best_candidate_authors) if (file_work.authors and best_candidate_authors) else 0.0

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

    current_hc_id = extract_numeric_id(record.calibre_hardcover_id)
    best_hc_id = str(best_book.id) if best_book else ""
    same_current_and_best_id = bool(current_hc_id and best_hc_id and current_hc_id == best_hc_id)

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
                suggested_calibre_authors=file_work.authors or record.calibre_authors,
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
                suggested_calibre_authors=file_work.authors or record.calibre_authors,
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
                suggested_calibre_authors=file_work.authors,
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

    if best_book and best_match and ((not current_book) or best_book.id != current_book.id) and (best_score >= current_score + 5 or best_cleaner_same_work):
        relink_block_reason = ""
        if record.calibre_hardcover_id:
            if best_file_title_score < 0.90:
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
                suggested_calibre_authors=file_work.authors or best_book.authors,
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
                suggested_calibre_authors=file_work.authors or record.calibre_authors,
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
                suggested_calibre_authors=file_work.authors or record.calibre_authors,
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
                suggested_calibre_authors=file_work.authors,
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
                suggested_calibre_authors=file_work.authors or record.calibre_authors,
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
            action = "keep_hardcover_id"
        if action == "replace_hardcover_id" and (best_file_title_score < 0.90 or (file_work.authors and best_file_author_score < 0.95)):
            action = "manual_review"
        return Decision(
            action=action,
            confidence_score=best_score,
            confidence_tier=confidence_tier(best_score),
            reason=(f"relink:ok; file_best_match; hardcover_id={best_book.id}" if action == "replace_hardcover_id" else (f"Actual ebook file matches Hardcover {best_book.id}" if action == "set_hardcover_id" else "Current Hardcover ID was re-confirmed by search against the actual ebook file")),
            issue_category=("hardcover_link" if action in {"replace_hardcover_id", "set_hardcover_id"} else "verified" if action == "keep_hardcover_id" else "manual_review"),
            suggested_calibre_title=clean_title_for_matching(best_book.title),
            suggested_calibre_authors=file_work.authors or best_book.authors,
            suggested_hardcover_id=(record.calibre_hardcover_id if action == "keep_hardcover_id" else str(best_book.id)),
            suggested_hardcover_slug=best_book.slug,
            **edition_decision_payload(best_edition),
            fix_basis=("current_hc_verified_by_search" if action == "keep_hardcover_id" else "file_best_match" if action in {"replace_hardcover_id", "set_hardcover_id"} else "relink_needs_review"),
        )

    content_lang = normalize_language_signal(content.inferred_language_from_content)
    embedded_lang = normalize_language_signal(embedded.embedded_language)
    if (content_lang in {"deu", "fra", "spa"} and content.language_confidence >= 0.02 and not current_match and not best_match) or (embedded_lang in {"deu", "fra", "spa"} and not current_match and not best_match):
        lang_label = content_lang or embedded_lang
        return Decision(
            action="likely_non_english",
            confidence_score=max(best_score, current_score, 78.0),
            confidence_tier="medium",
            reason=f"Actual ebook file appears to be non-English ({lang_label}); English-title matching may be unreliable",
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
            suggested_calibre_authors=file_work.authors or record.calibre_authors,
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


def apply_preferred_edition_guardrails(record: BookRecord, file_work: FileWork, decision: Decision, current_book: Optional[HardcoverBook], current_choice: EditionChoiceInfo, current_score: float, best_book: Optional[HardcoverBook], best_choice: EditionChoiceInfo, best_score: float) -> Decision:
    trusted_actions = {"keep_hardcover_id", "safe_auto_fix", "update_calibre_metadata", "set_hardcover_id", "replace_hardcover_id"}
    if decision.action not in trusted_actions:
        return decision

    target_hc_id = extract_numeric_id(decision.suggested_hardcover_id) or extract_numeric_id(record.calibre_hardcover_id)
    target_book: Optional[HardcoverBook] = None
    target_choice = EditionChoiceInfo()
    target_edition: Optional[HardcoverEdition] = None
    target_score = max(current_score, best_score, float(decision.confidence_score or 0.0))

    if target_hc_id and current_book and str(current_book.id) == target_hc_id:
        target_book = current_book
        target_choice = current_choice
        target_edition = current_choice.chosen
        target_score = max(current_score, float(decision.confidence_score or 0.0))
    elif target_hc_id and best_book and str(best_book.id) == target_hc_id:
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


def row_from_result(record: BookRecord, file_work: FileWork, embedded: EmbeddedMeta, content: ContentSignals, current_book: Optional[HardcoverBook], best_book: Optional[HardcoverBook], edition_choice: EditionChoiceInfo, matched_by: str, current_ok: Optional[bool], decision: Decision, best_breakdown: MatchScores) -> AuditRow:
    preferred_edition = edition_choice.chosen
    suggested_title = decision.suggested_calibre_title or record.calibre_title
    suggested_authors = decision.suggested_calibre_authors or record.calibre_authors
    same_hc_id_as_suggestion = bool(extract_numeric_id(record.calibre_hardcover_id) and extract_numeric_id(decision.suggested_hardcover_id) and extract_numeric_id(record.calibre_hardcover_id) == extract_numeric_id(decision.suggested_hardcover_id))
    embedded_mismatch_summary = summarize_embedded_mismatch(embedded, record.calibre_title, record.calibre_authors, suggested_title, suggested_authors)
    current_title = best_book.title if best_book else ""
    current_auth = best_book.authors if best_book else ""
    current_hc_title = current_book.title if current_book else ""
    current_hc_auth = current_book.authors if current_book else ""
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
        hardcover_authors=current_auth,
        hardcover_series=best_book.series if best_book else "",
        hardcover_slug=best_book.slug if best_book else "",
        current_hardcover_title=current_hc_title,
        current_hardcover_authors=current_hc_auth,
        suggested_hardcover_title=best_book.title if best_book else decision.suggested_calibre_title,
        suggested_hardcover_authors=preferred_candidate_authors or decision.suggested_calibre_authors or (best_book.authors if best_book else ""),
        preferred_edition_id=str(preferred_edition.id) if preferred_edition else "",
        preferred_edition_title=preferred_edition.title if preferred_edition else "",
        preferred_edition_reading_format=preferred_edition.reading_format if preferred_edition else "",
        preferred_edition_edition_format=preferred_edition.edition_format if preferred_edition else "",
        preferred_edition_format_normalized=normalize_edition_format(preferred_edition.edition_format, preferred_edition.reading_format) if preferred_edition else "",
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
        runner_up_edition_format_normalized=normalize_edition_format(edition_choice.runner_up.edition_format, edition_choice.runner_up.reading_format) if edition_choice.runner_up else "",
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
        default_ebook_edition_format_normalized=normalize_edition_format(edition_choice.default_ebook.edition_format, edition_choice.default_ebook.reading_format) if edition_choice.default_ebook else "",
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
        file_vs_calibre_title_score=round(bare_title_similarity(file_work.title, record.calibre_title) if file_work.title else 0.0, 3),
        file_vs_calibre_authors_score=round(author_similarity(file_work.authors, record.calibre_authors) if file_work.authors else 0.0, 3),
        file_vs_current_title_score=round(bare_title_similarity(file_work.title, current_hc_title) if file_work.title and current_hc_title else 0.0, 3),
        file_vs_current_authors_score=round(author_coverage(file_work.authors, current_hc_auth) if file_work.authors and current_hc_auth else 0.0, 3),
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
        hardcover_primary_author_normalized=normalize_primary_author_value(preferred_candidate_authors or current_auth),
        author_mismatch_reason=explain_author_mismatch(file_work.authors, preferred_candidate_authors or current_auth),
        same_hardcover_id_as_suggestion=same_hc_id_as_suggestion,
        embedded_title_mismatch_to_calibre=bool(embedded.embedded_title and record.calibre_title and textually_distinct_titles(embedded.embedded_title, record.calibre_title)),
        embedded_authors_mismatch_to_calibre_text=bool(embedded.embedded_authors and record.calibre_authors and textually_distinct_authors(embedded.embedded_authors, record.calibre_authors)),
        embedded_authors_mismatch_to_calibre_canonical=bool(embedded.embedded_authors and record.calibre_authors and canonically_distinct_authors(embedded.embedded_authors, record.calibre_authors)),
        embedded_title_mismatch_to_suggested=bool(embedded.embedded_title and suggested_title and textually_distinct_titles(embedded.embedded_title, suggested_title)),
        embedded_authors_mismatch_to_suggested_text=bool(embedded.embedded_authors and suggested_authors and textually_distinct_authors(embedded.embedded_authors, suggested_authors)),
        embedded_authors_mismatch_to_suggested_canonical=bool(embedded.embedded_authors and suggested_authors and canonically_distinct_authors(embedded.embedded_authors, suggested_authors)),
        embedded_calibre_mismatch_summary=embedded_mismatch_summary,
        fix_basis=decision.fix_basis,
    )



def audit_books(records: List[BookRecord], hc: HardcoverClient, ebook_meta_runner: EbookMetaRunner, limit: Optional[int], verbose: bool, progress_every: int = DEFAULT_PROGRESS_EVERY) -> List[AuditRow]:
    rows: List[AuditRow] = []
    subset = records[:limit] if limit else records
    total = len(subset)
    audit_started_at = time.monotonic()
    audit_hc_start = hc.stats_snapshot()
    for idx, record in enumerate(subset, start=1):
        row_t0 = time.monotonic()
        vlog(verbose, f"[{idx}/{total}] calibre_id={record.calibre_book_id} format={record.file_format or '-'} calibre=\"{_log_label(record.calibre_title, max_len=64)}\"")
        host_path = Path(record.file_path)
        t0 = time.monotonic()
        embedded = ebook_meta_runner.run(host_path)
        t1 = time.monotonic()
        if not embedded.embedded_title and host_path.suffix.lower() in {".epub", ".kepub", ".oebzip"}:
            opf = parse_epub_opf_metadata(host_path)
            if opf.embedded_title:
                embedded = opf
        t2 = time.monotonic()
        content = extract_content_signals(record.file_path, record.calibre_title, record.calibre_authors)
        t3 = time.monotonic()
        file_work = derive_file_work(record, embedded, content)
        vlog(verbose, f"  file work=\"{_log_label(file_work.title or '-', max_len=64)}\" authors={file_work.authors or '-'} source={file_work.title_basis}/{file_work.authors_basis}")

        current_book = None
        current_choice = EditionChoiceInfo()
        current_ok = None
        current_score = 0.0
        current_breakdown = MatchScores()
        current_why = ""

        hc_t0 = time.monotonic()
        current_book, current_choice, current_score, current_breakdown, current_ok, current_why = validate_current_hardcover_link(record, file_work, embedded, hc, verbose=verbose)
        hc_t1 = time.monotonic()

        current_edition = current_choice.chosen
        best_book = current_book
        best_choice = current_choice
        best_edition = best_choice.chosen
        best_score = current_score
        best_breakdown = current_breakdown
        matched_by = "current_hardcover_id" if current_book else ""

        search_t0 = hc_t1
        search_t1 = hc_t1
        search_beyond_current, search_reason = should_search_after_current_validation(record, current_book, current_score)
        if current_ok:
            matched_by = current_why or "current_hardcover_id"

        if search_beyond_current:
            vlog(verbose, f"  search reason={search_reason}")
            search_t0 = time.monotonic()
            cand_book, cand_choice, cand_score, cand_breakdown, cand_why = choose_best_candidate(record, file_work, embedded, content, hc, verbose=verbose)
            search_t1 = time.monotonic()
            cand_edition = cand_choice.chosen
            if cand_book and (best_book is None or cand_score >= best_score):
                best_book = cand_book
                best_choice = cand_choice
                best_edition = cand_edition
                best_score = cand_score
                best_breakdown = cand_breakdown
                matched_by = cand_why or "search"
                extra = f" | preferred={compact_edition_marker(cand_edition, cand_choice.chosen_score)} gap={cand_choice.score_gap:.1f}" if cand_edition else ""
                vlog(verbose, f"  search best candidate={compact_book_marker(cand_book)} score={cand_score:.2f} matched_by={matched_by}{extra}")

        decision = decide_action(record, file_work, embedded, content, current_book, current_edition, current_score, best_book, best_edition, best_score)
        decision = apply_preferred_edition_guardrails(record, file_work, decision, current_book, current_choice, current_score, best_book, best_choice, best_score)
        vlog(verbose, f"  RESULT action={decision.action} confidence={decision.confidence_score:.2f} tier={decision.confidence_tier} reason={decision.reason}")
        suggest_text = compact_suggest_fields(decision, best_book, best_edition)
        if suggest_text:
            vlog(verbose, f"  SUGGEST {suggest_text}")

        rows.append(row_from_result(record, file_work, embedded, content, current_book, best_book, best_choice, matched_by, current_ok, decision, best_breakdown))

        if idx % 20 == 0:
            hc.save_cache()
        if verbose and (idx % max(1, int(progress_every)) == 0 or idx == total):
            elapsed = time.monotonic() - audit_started_at
            rate = (idx / elapsed) if elapsed > 0 else 0.0
            eta = ((total - idx) / rate) if rate > 0 else 0.0
            action_counts = Counter(r.recommended_action for r in rows)
            top_actions = ", ".join(f"{k}:{v}" for k, v in action_counts.most_common(4)) or "-"
            vlog(True, f"[PROGRESS] books={idx}/{total} elapsed={elapsed:.1f}s rate={rate:.2f}/s eta={eta:.1f}s actions={top_actions} hc={hc.stats_delta_text(audit_hc_start)}")

    hc.save_cache()
    return rows



def bucket_sort_key(r: AuditRow) -> Tuple[int, float, int]:
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
    return (action_priority.get(r.recommended_action, 99), -float(r.confidence_score or 0.0), int(r.calibre_book_id or 0))


def classify_manual_review_bucket(r: AuditRow) -> str:
    if r.recommended_action == "manual_review_title_match_author_unconfirmed":
        return "manual_review_title_match_author_unconfirmed"
    if r.calibre_hardcover_id and r.current_hardcover_match_ok == "":
        return "manual_review_unresolved_current_id"
    if float(r.confidence_score or 0.0) >= 75:
        return "manual_review_strong_candidate"
    if float(r.confidence_score or 0.0) >= 60:
        return "manual_review_plausible_candidate"
    return "manual_review_no_candidate"


def build_bucket_definitions(rows: List[AuditRow], duplicates: List[Dict[str, Any]], series_issues: List[Dict[str, Any]]) -> List[Tuple[str, str, List[Dict[str, Any]]]]:
    bucket_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in sorted(rows, key=bucket_sort_key):
        row = asdict(r)
        if r.recommended_action == "keep_hardcover_id":
            bucket_rows["00_verified_current_hardcover_links.csv"].append(row)
        elif r.recommended_action == "safe_auto_fix":
            bucket_rows["01_safe_automatic_fixes.csv"].append(row)
        elif r.recommended_action == "set_hardcover_id":
            bucket_rows["02_missing_hardcover_id_high_confidence.csv"].append(row)
        elif r.recommended_action == "replace_hardcover_id":
            bucket_rows["03_hardcover_id_relink_candidates.csv"].append(row)
        elif r.recommended_action == "update_calibre_metadata":
            bucket_rows["04_calibre_metadata_updates.csv"].append(row)
        elif r.recommended_action == "manual_review":
            review_bucket = classify_manual_review_bucket(r)
            if review_bucket == "manual_review_unresolved_current_id":
                bucket_rows["05_manual_review_unresolved_current_id.csv"].append(row)
            elif review_bucket == "manual_review_strong_candidate":
                bucket_rows["06_manual_review_strong_candidates.csv"].append(row)
            elif review_bucket == "manual_review_plausible_candidate":
                bucket_rows["07_manual_review_plausible_candidates.csv"].append(row)
            elif review_bucket == "manual_review_title_match_author_unconfirmed":
                bucket_rows["08_manual_review_title_match_author_unconfirmed.csv"].append(row)
            else:
                bucket_rows["09_manual_review_no_candidate.csv"].append(row)
        elif r.recommended_action == "suspected_author_mismatch":
            bucket_rows["10_suspected_author_mismatch.csv"].append(row)
        elif r.recommended_action == "suspected_file_mismatch":
            bucket_rows["11_suspected_file_mismatch.csv"].append(row)
        elif r.recommended_action == "likely_non_english":
            bucket_rows["12_non_english_or_uncertain_language.csv"].append(row)

    bucket_rows["13_series_and_number_issues.csv"] = sorted(series_issues, key=lambda row: (norm(str(row.get("hardcover_series") or row.get("calibre_series") or "")), int(row.get("calibre_book_id") or 0)))
    bucket_rows["14_duplicate_or_near_duplicate_books.csv"] = sorted(duplicates, key=lambda row: (norm(str(row.get("suggested_calibre_title") or row.get("calibre_title") or "")), int(row.get("calibre_book_id") or 0)))

    labels = {
        "00_verified_current_hardcover_links.csv": "Verified current Hardcover links",
        "01_safe_automatic_fixes.csv": "Safe automatic title cleanups",
        "02_missing_hardcover_id_high_confidence.csv": "Missing Hardcover IDs with strong candidate",
        "03_hardcover_id_relink_candidates.csv": "Hardcover relink candidates",
        "04_calibre_metadata_updates.csv": "Calibre metadata updates",
        "05_manual_review_unresolved_current_id.csv": "Manual review: current Hardcover ID unresolved",
        "06_manual_review_strong_candidates.csv": "Manual review: strong candidate present",
        "07_manual_review_plausible_candidates.csv": "Manual review: plausible candidate present",
        "08_manual_review_title_match_author_unconfirmed.csv": "Manual review: title match but author unconfirmed",
        "09_manual_review_no_candidate.csv": "Manual review: no good candidate",
        "10_suspected_author_mismatch.csv": "Suspected author mismatch",
        "11_suspected_file_mismatch.csv": "Suspected file mismatch",
        "12_non_english_or_uncertain_language.csv": "Non-English or uncertain language",
        "13_series_and_number_issues.csv": "Series and numbering issues",
        "14_duplicate_or_near_duplicate_books.csv": "Duplicate or near-duplicate books",
    }
    ordered = [
        "00_verified_current_hardcover_links.csv",
        "01_safe_automatic_fixes.csv",
        "02_missing_hardcover_id_high_confidence.csv",
        "03_hardcover_id_relink_candidates.csv",
        "04_calibre_metadata_updates.csv",
        "05_manual_review_unresolved_current_id.csv",
        "06_manual_review_strong_candidates.csv",
        "07_manual_review_plausible_candidates.csv",
        "08_manual_review_title_match_author_unconfirmed.csv",
        "09_manual_review_no_candidate.csv",
        "10_suspected_author_mismatch.csv",
        "11_suspected_file_mismatch.csv",
        "12_non_english_or_uncertain_language.csv",
        "13_series_and_number_issues.csv",
        "14_duplicate_or_near_duplicate_books.csv",
    ]
    return [(name, labels[name], bucket_rows.get(name, [])) for name in ordered]


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
    candidate = extract_numeric_id(row.suggested_hardcover_id) or extract_numeric_id(row.hardcover_candidate_id) or extract_numeric_id(row.calibre_hardcover_id)
    if not candidate:
        return None
    if float(row.confidence_score or 0.0) < 75 and row.current_hardcover_match_ok != "yes":
        return None
    return candidate


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
    pos = book.get("position")
    if pos not in (None, ""):
        try:
            return f"pos:{float(pos):g}"
        except Exception:
            return f"pos:{pos}"
    clean_title = clean_title_for_matching(book.get("title") or "")
    return f"title:{norm(clean_title)}|author:{normalize_author_key(primary_author(book.get('authors') or ''))}"


def _foreign_stopword_hits(text: str, stopwords: Set[str]) -> int:
    tokens = {tok for tok in norm(text).split() if tok}
    return sum(1 for tok in tokens if tok in stopwords)


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
        "el ", "la ", "las ", "los ", "un ", "una ",
        "le ", "les ", "des ", "der ", "die ", "das ",
    )
    if lowered.startswith(explicit_prefixes) and english_hits == 0:
        return True
    return False


def _series_book_title_looks_non_english(book: Dict[str, Any]) -> bool:
    return _text_looks_non_english(
        book.get("title") or "",
        book.get("subtitle") or "",
        book.get("details") or "",
    )


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


def _collapse_series_catalog_books(catalog_books: List[Dict[str, Any]], owned_ids: set[int]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for book in catalog_books:
        grouped[_series_group_key(book)].append(book)
    owned_reps: List[Dict[str, Any]] = []
    missing_reps: List[Dict[str, Any]] = []
    for _key, group in grouped.items():
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


def _fetch_books_with_progress(ids: List[int], hc: HardcoverClient, verbose: bool = False, label: str = "resolve", display_labels: Optional[Dict[int, str]] = None) -> Dict[int, HardcoverBook]:
    ids = sorted(set(int(x) for x in ids if x))
    if not ids:
        return {}
    total = len(ids)
    out: Dict[int, HardcoverBook] = {}
    display_labels = display_labels or {}
    id_chunks = list(chunked(ids, 25))
    total_batches = len(id_chunks)
    if verbose and total_batches:
        vlog(verbose, f"  {label}: fetching {total} books in {total_batches} batches")
    for batch_idx, id_chunk in enumerate(id_chunks, start=1):
        chunk_before = hc.stats_snapshot()
        chunk_books = hc.fetch_books(id_chunk)
        out.update(chunk_books)
        chunk_after = hc.stats_snapshot()
        net_delta = int(chunk_after["network_requests"] - chunk_before["network_requests"])
        cache_delta = int(chunk_after["cache_hits"] - chunk_before["cache_hits"])
        if net_delta and cache_delta:
            chunk_cache_status = "mixed"
        elif net_delta:
            chunk_cache_status = "miss"
        elif cache_delta:
            chunk_cache_status = "hit"
        else:
            chunk_cache_status = "none"
        display_items = [display_labels.get(book_id) or f"{_log_label((chunk_books.get(book_id).title if chunk_books.get(book_id) else '-'))} [{book_id}]" for book_id in id_chunk]
        sample = preview_names(display_items, limit=min(4, len(id_chunk)), max_len=42)
        vlog(verbose, f"    [{batch_idx}/{total_batches}] books={sample} matched={len(chunk_books)}/{len(id_chunk)} batch_cache={chunk_cache_status} {hc.stats_delta_text(chunk_before, chunk_after)}")
    return out


def build_missing_series_books(rows: List[AuditRow], hc: HardcoverClient, verbose: bool = False) -> List[Dict[str, Any]]:
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
    phase0 = hc.stats_snapshot()
    memberships = hc.fetch_book_series_memberships(
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

    vlog(verbose, f"  memberships: books={len(trusted_by_book_id)} series={len(series_to_owned)} {hc.stats_delta_text(phase0)}")

    phase1 = hc.stats_snapshot()
    all_missing_book_ids: List[int] = []
    missing_book_display_labels: Dict[int, str] = {}
    series_items = sorted(
        series_to_owned.items(),
        key=lambda kv: (norm(next(iter(kv[1].values())).get("series_name") or ""), kv[0]) if kv[1] else ("", kv[0]),
    )
    series_catalogs: Dict[int, Dict[str, Any]] = hc.fetch_series_books(
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
        display_name = _log_label(series_name or (catalog or {}).get("series_name") or "-", max_len=48)
        line = (
            f"  [inspect {processed_series}/{total_series}] {display_name} [{series_id}] "
            f"primary={display_counts['primary_books']} fractional={display_counts['fractional_books']} "
            f"secondary={display_counts['secondary_books']} owned={owned_count} "
            f"readers={int((catalog or {}).get('series_users_read_count_total') or 0)}"
        )
        if hc.debug_hardcover:
            line += f" raw={display_counts['raw_books']} grouped={display_counts['grouped_slots']} shown={display_counts['display_books']} dropped_non_english={display_counts['dropped_non_english_only']}"
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
        hc=hc,
        verbose=verbose,
        label="resolve",
        display_labels=missing_book_display_labels,
    ) if missing_book_ids else {}
    missing_preview = preview_names([
        f"{(missing_book_details.get(book_id).title if missing_book_details.get(book_id) else '-') or '-'} [{book_id}]"
        for book_id in missing_book_ids
    ], limit=4, max_len=52)
    vlog(verbose, f"  catalogs: fetching edition candidates for {len(missing_book_ids)} missing books")
    missing_book_editions = hc.fetch_editions_for_books(
        missing_book_ids,
        verbose=verbose,
        progress_label="edition-catalogs",
        display_labels=missing_book_display_labels,
    ) if missing_book_ids else {}
    vlog(verbose, f"  catalogs: series={len(series_to_owned)} missing_books={len(missing_book_ids)} sample_missing={missing_preview} {hc.stats_delta_text(phase1)}")

    output: List[Dict[str, Any]] = []
    vlog(verbose, f"  catalogs: building output rows for {len(series_to_owned)} series")

    build_series_items = sorted(
        series_to_owned.items(),
        key=lambda kv: (norm(next(iter(kv[1].values())).get("series_name") or ""), kv[0]) if kv[1] else ("", kv[0]),
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
        display_name = _log_label(catalog.get("series_name") or next(iter(owned_map.values())).get("series_name") or "-", max_len=48)
        if verbose:
            vlog(True, f"  [build {build_index}/{len(build_series_items)}] {display_name} [{series_id}] owned={len(owned_catalog_books)} missing={len(missing_books)}")
        if not owned_catalog_books:
            continue
        if not missing_books:
            continue
        owned_positions = sorted(float(book.get("position")) for book in owned_catalog_books if book.get("position") not in (None, ""))
        first_owned = min(owned_positions) if owned_positions else None
        last_owned = max(owned_positions) if owned_positions else None
        owned_titles = [book.get("title") or "" for book in owned_catalog_books]

        for missing in missing_books:
            missing_book_id = int(missing.get("book_id") or 0)
            missing_position = missing.get("position")
            before_owned = [book for book in owned_catalog_books if book.get("position") not in (None, "") and missing_position not in (None, "") and float(book.get("position")) < float(missing_position)]
            after_owned = [book for book in owned_catalog_books if book.get("position") not in (None, "") and missing_position not in (None, "") and float(book.get("position")) > float(missing_position)]
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
                    series=f"{catalog.get('series_name') or ''} [{missing_position}]" if missing_position not in (None, "") else (catalog.get('series_name') or ""),
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
            preferred_choice = choose_preferred_edition_info(synthetic_record, synthetic_file_work, EmbeddedMeta(), missing_book, editions) if missing_book else EditionChoiceInfo()
            preferred_edition = preferred_choice.chosen
            runner_up_edition = preferred_choice.runner_up
            preferred_authors = effective_candidate_authors(missing_book, preferred_edition) if missing_book else ""
            ranked_missing_editions = rank_candidate_editions(synthetic_record, synthetic_file_work, EmbeddedMeta(), missing_book, editions) if missing_book else []

            if verbose and missing_book:
                slot = _series_position_bracket(missing_position, catalog.get("primary_books_count") or 0)
                meta_marker = compact_missing_series_marker(missing, catalog.get("primary_books_count") or 0, include_meta=True)
                vlog(True, f"    missing {meta_marker} hc={compact_book_marker(missing_book)} editions={len(editions)} gap_kind={gap_kind}")
                if preferred_edition:
                    vlog(True, f"      preferred={compact_edition_marker(preferred_edition, preferred_choice.chosen_score)} gap={preferred_choice.score_gap:.1f} {edition_gap_tier(preferred_choice.score_gap, bool(preferred_choice.runner_up))}")
                alt_editions = compact_ranked_editions_from_choice(ranked_missing_editions, skip=1, limit=2)
                if alt_editions != "-":
                    vlog(True, f"      alternatives={alt_editions}")

            missing_canonical_id = int(missing.get("canonical_id") or 0)
            slot_bucket = _slot_bucket(missing_position, catalog.get("primary_books_count") or 0)
            output.append({
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
                "owned_hardcover_book_ids": ", ".join(str(int(book.get("book_id"))) for book in sorted(owned_catalog_books, key=lambda b: _position_sort_value(b.get("position")))),
                "owned_calibre_book_ids": ", ".join(str(r.calibre_book_id) for book_id in sorted(owned_ids) for r in trusted_by_book_id.get(book_id, [])),
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
            })

    output.sort(key=lambda row: (norm(str(row.get("series_name") or "")), _position_sort_value(row.get("missing_position")), norm(str(row.get("missing_title") or ""))))
    hc.save_cache()
    if verbose:
        phase_total = hc.stats_snapshot()
        series_count = len({int(row.get("series_id") or 0) for row in output if row.get("series_id")})
        mainline_count = sum(1 for row in output if not _position_is_fractional(row.get("missing_position")))
        fractional_count = len(output) - mainline_count
        preview = preview_names([
            f"{row.get('series_name') or '-'} -> {row.get('missing_title') or '-'} [{int(row.get('missing_hardcover_book_id') or 0)}]"
            for row in output
        ], limit=4, max_len=60)
        vlog(True, f"  result: rows={len(output)} series={series_count} mainline={mainline_count} fractional={fractional_count} sample={preview} {hc.stats_delta_text(phase0, phase_total)}")
    return output

def build_hardcover_edition_write_candidates(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    trusted_actions = {
        "keep_hardcover_id",
        "safe_auto_fix",
        "update_calibre_metadata",
        "set_hardcover_id",
        "replace_hardcover_id",
    }
    out: List[Dict[str, Any]] = []

    def _write_guardrail_reason(r: AuditRow, same_as_current_hc: bool, current_edition_id: str, proposed_edition_id: str) -> str:
        proposed_fmt = r.suggested_hardcover_edition_format_normalized or r.preferred_edition_format_normalized
        proposed_lang = r.suggested_hardcover_edition_language or r.preferred_edition_language
        if not same_as_current_hc:
            return "suggested hardcover-id differs from current hardcover-id"
        if r.recommended_action in {"manual_review", "manual_review_title_match_author_unconfirmed", "suspected_author_mismatch", "suspected_file_mismatch", "likely_non_english"}:
            return f"recommended_action={r.recommended_action}"
        if float(r.confidence_score or 0.0) < 75.0:
            return "confidence below 75"
        if not proposed_edition_id:
            return "no proposed hardcover-edition"
        if norm(str(proposed_fmt or "")) == "audiobook":
            return "proposed hardcover-edition is audiobook; blocked by default"
        if not str(proposed_lang or "").strip():
            return "proposed hardcover-edition has blank language; manual review required"
        if current_edition_id and current_edition_id == proposed_edition_id:
            return "current hardcover-edition already matches suggestion"
        return "ok"

    for r in rows:
        proposed_hc_id = extract_numeric_id(r.suggested_hardcover_id) or extract_numeric_id(r.calibre_hardcover_id)
        current_hc_id = extract_numeric_id(r.calibre_hardcover_id)
        current_edition_id = extract_numeric_id(r.current_hardcover_edition_id)
        proposed_edition_id = extract_numeric_id(r.suggested_hardcover_edition_id or r.preferred_edition_id)
        if not proposed_hc_id or not proposed_edition_id:
            continue
        same_as_current_hc = bool(current_hc_id and proposed_hc_id == current_hc_id)
        same_as_current_edition = bool(current_edition_id and proposed_edition_id == current_edition_id)
        write_guardrail_reason = _write_guardrail_reason(r, same_as_current_hc, current_edition_id, proposed_edition_id)
        proposed_fmt = r.suggested_hardcover_edition_format_normalized or r.preferred_edition_format_normalized
        proposed_lang = r.suggested_hardcover_edition_language or r.preferred_edition_language
        blocked_edition_write = is_edition_write_blocked_row(format_normalized=proposed_fmt, language=proposed_lang)
        safe_for_current_id_write_pass = bool(
            same_as_current_hc and r.recommended_action in trusted_actions and float(r.confidence_score or 0.0) >= 75.0 and (r.current_hardcover_match_ok in {"yes", "", "no"}) and r.recommended_action not in {"manual_review", "manual_review_title_match_author_unconfirmed", "suspected_author_mismatch", "suspected_file_mismatch", "likely_non_english"} and not same_as_current_edition and not blocked_edition_write
        )
        out.append({
            "calibre_book_id": r.calibre_book_id,
            "calibre_title": r.calibre_title,
            "calibre_authors": r.calibre_authors,
            "file_path": r.file_path,
            "file_format": r.file_format,
            "current_hardcover_id": r.calibre_hardcover_id,
            "current_hardcover_slug": r.calibre_hardcover_slug,
            "current_hardcover_edition_id": r.current_hardcover_edition_id,
            "suggested_hardcover_id": proposed_hc_id,
            "suggested_hardcover_slug": r.suggested_hardcover_slug or r.hardcover_slug or r.calibre_hardcover_slug,
            "suggested_hardcover_edition_id": proposed_edition_id,
            "suggested_hardcover_edition_title": r.suggested_hardcover_edition_title or r.preferred_edition_title,
            "suggested_hardcover_reading_format": r.suggested_hardcover_reading_format or r.preferred_edition_reading_format,
            "suggested_hardcover_edition_format_raw": r.suggested_hardcover_edition_format_raw or r.preferred_edition_edition_format,
            "suggested_hardcover_edition_format_normalized": r.suggested_hardcover_edition_format_normalized or r.preferred_edition_format_normalized,
            "suggested_hardcover_edition_is_ebookish": r.suggested_hardcover_edition_is_ebookish if r.suggested_hardcover_edition_id else r.preferred_edition_is_ebookish,
            "default_ebook_edition_id": r.default_ebook_edition_id,
            "preferred_matches_default_ebook": r.preferred_matches_default_ebook,
            "suggested_hardcover_edition_format": r.suggested_hardcover_edition_format or r.preferred_edition_edition_format or r.preferred_edition_reading_format,
            "suggested_hardcover_edition_language": r.suggested_hardcover_edition_language or r.preferred_edition_language,
            "edition_choice_score": r.edition_choice_score,
            "edition_runner_up_id": r.runner_up_edition_id,
            "edition_runner_up_title": r.runner_up_edition_title,
            "edition_runner_up_reading_format": r.runner_up_edition_reading_format,
            "edition_runner_up_format_raw": r.runner_up_edition_edition_format,
            "edition_runner_up_format_normalized": r.runner_up_edition_format_normalized,
            "edition_runner_up_is_ebookish": r.runner_up_edition_is_ebookish,
            "edition_runner_up_format": r.runner_up_edition_reading_format or r.runner_up_edition_edition_format,
            "edition_runner_up_language": r.runner_up_edition_language,
            "edition_runner_up_score": r.edition_runner_up_score,
            "edition_choice_score_gap": r.edition_choice_score_gap,
            "edition_gap_tier": edition_gap_tier(float(r.edition_choice_score_gap or 0.0), bool(r.runner_up_edition_id)),
            "edition_choice_reason": r.preferred_edition_reason,
            "default_ebook_edition_id": r.default_ebook_edition_id,
            "default_ebook_edition_score": r.default_ebook_edition_score,
            "preferred_matches_default_ebook": r.preferred_matches_default_ebook,
            "preferred_vs_default_ebook_score_gap": r.preferred_vs_default_ebook_score_gap,
            "edition_runner_up_reason": r.runner_up_edition_reason,
            "edition_candidates_considered": r.edition_candidates_considered,
            "recommended_action": r.recommended_action,
            "confidence_score": r.confidence_score,
            "confidence_tier": r.confidence_tier,
            "current_hardcover_match_ok": r.current_hardcover_match_ok,
            "edition_matches_current_hardcover_id": same_as_current_hc,
            "edition_matches_current_hardcover_edition": same_as_current_edition,
            "current_has_hardcover_edition": bool(current_edition_id),
            "needs_hardcover_edition_write": bool(not same_as_current_edition),
            "safe_for_current_id_write_pass": safe_for_current_id_write_pass,
            "write_guardrail_reason": write_guardrail_reason,
            "reason": r.reason,
            "fix_basis": r.fix_basis,
            "current_hardcover_title": r.current_hardcover_title,
            "current_hardcover_author": r.current_hardcover_authors,
            "suggested_hardcover_title": r.suggested_hardcover_title or r.hardcover_title,
            "suggested_hardcover_author": r.suggested_hardcover_authors or r.hardcover_authors,
            "relink_confidence": r.confidence_tier,
            "relink_reason": r.reason,
        })
    out.sort(key=lambda row: (not bool(row.get("safe_for_current_id_write_pass")), not bool(row.get("edition_matches_current_hardcover_id")), -float(row.get("confidence_score") or 0.0), int(row.get("calibre_book_id") or 0)))
    return out


def build_same_id_edition_write_candidates(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    out = [row for row in build_hardcover_edition_write_candidates(rows) if row.get("edition_matches_current_hardcover_id")]
    out.sort(key=lambda row: (not bool(row.get("safe_for_current_id_write_pass")), bool(row.get("edition_matches_current_hardcover_edition")), -float(row.get("edition_choice_score_gap") or 0.0), -float(row.get("confidence_score") or 0.0), int(row.get("calibre_book_id") or 0)))
    return out


def build_edition_manual_review_queue(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    out = [row for row in build_hardcover_edition_write_candidates(rows) if not row.get("safe_for_current_id_write_pass") or str(row.get("edition_gap_tier") or "") in {"narrow", "tie_or_negative"} or not row.get("edition_matches_current_hardcover_id")]
    out.sort(key=lambda row: (bool(row.get("safe_for_current_id_write_pass")), float(row.get("edition_choice_score_gap") or 0.0), -float(row.get("confidence_score") or 0.0), int(row.get("calibre_book_id") or 0)))
    return out


def _position_is_fractional(value: Any) -> bool:
    if value in (None, ""):
        return False
    try:
        return not float(value).is_integer()
    except Exception:
        return False


def build_write_plan(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    trusted_actions = {"safe_auto_fix", "update_calibre_metadata", "set_hardcover_id", "replace_hardcover_id", "keep_hardcover_id"}
    out: List[Dict[str, Any]] = []
    for r in rows:
        proposed_fmt = r.suggested_hardcover_edition_format_normalized or r.preferred_edition_format_normalized
        proposed_lang = r.suggested_hardcover_edition_language or r.preferred_edition_language
        blocked_edition_write = is_edition_write_blocked_row(format_normalized=proposed_fmt, language=proposed_lang)
        if r.recommended_action in {"manual_review", "manual_review_title_match_author_unconfirmed", "suspected_author_mismatch", "suspected_file_mismatch", "likely_non_english"}:
            safe_reason = f"recommended_action={r.recommended_action}"
        elif float(r.confidence_score or 0.0) < 75.0:
            safe_reason = "confidence below 75"
        elif norm(str(proposed_fmt or "")) == "audiobook":
            safe_reason = "proposed hardcover-edition is audiobook; blocked by default"
        elif not str(proposed_lang or "").strip():
            safe_reason = "proposed hardcover-edition has blank language; manual review required"
        else:
            safe_reason = "ok"
        safe = bool(r.recommended_action in trusted_actions and float(r.confidence_score or 0.0) >= 75.0 and r.recommended_action not in {"manual_review", "manual_review_title_match_author_unconfirmed", "suspected_author_mismatch", "suspected_file_mismatch", "likely_non_english"} and not blocked_edition_write)
        out.append({
            "calibre_book_id": r.calibre_book_id,
            "title": r.calibre_title,
            "current_calibre_title": r.calibre_title,
            "new_calibre_title": r.suggested_calibre_title or r.calibre_title,
            "current_calibre_author": r.calibre_authors,
            "new_calibre_author": r.suggested_calibre_authors or r.calibre_authors,
            "current_hardcover_id": r.calibre_hardcover_id,
            "new_hardcover_id": r.suggested_hardcover_id or r.calibre_hardcover_id,
            "current_hardcover_title": r.current_hardcover_title,
            "current_hardcover_author": r.current_hardcover_authors,
            "suggested_hardcover_title": r.suggested_hardcover_title or r.hardcover_title,
            "suggested_hardcover_author": r.suggested_hardcover_authors or r.hardcover_authors,
            "relink_confidence": r.confidence_tier,
            "relink_reason": r.reason,
            "current_hardcover_edition_id": r.current_hardcover_edition_id,
            "new_hardcover_edition_id": r.suggested_hardcover_edition_id or r.preferred_edition_id,
            "suggested_hardcover_reading_format": r.suggested_hardcover_reading_format or r.preferred_edition_reading_format,
            "suggested_hardcover_edition_format_raw": r.suggested_hardcover_edition_format_raw or r.preferred_edition_edition_format,
            "suggested_hardcover_edition_format_normalized": r.suggested_hardcover_edition_format_normalized or r.preferred_edition_format_normalized,
            "suggested_hardcover_edition_is_ebookish": r.suggested_hardcover_edition_is_ebookish if r.suggested_hardcover_edition_id else r.preferred_edition_is_ebookish,
            "action_type": r.recommended_action,
            "confidence": r.confidence_score,
            "reason": r.reason,
            "safe_to_apply_boolean": safe,
            "safe_to_apply_reason": safe_reason,
        })
    out.sort(key=lambda row: (not bool(row.get("safe_to_apply_boolean")), -float(row.get("confidence") or 0.0), int(row.get("calibre_book_id") or 0)))
    return out



def _counter_preview(counter: Counter[str], limit: int = 8) -> str:
    parts: List[str] = []
    for value, count in counter.most_common(limit):
        label = _log_label(value, max_len=64)
        if not label or label == "-":
            continue
        parts.append(f"{label} [{count}]")
    return " || ".join(parts) if parts else "-"


def _int_preview(values: Set[int], limit: int = 12) -> str:
    ordered = sorted(int(v) for v in values if v)
    if not ordered:
        return "-"
    return ", ".join(str(v) for v in ordered)


def _choose_preferred_display(counter: Counter[str]) -> str:
    if not counter:
        return ""
    return sorted(counter.items(), key=lambda kv: (-kv[1], len(kv[0]), norm(kv[0]), kv[0]))[0][0]


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

    for r in rows:
        title = smart_title(r.suggested_calibre_title or r.calibre_title or "")
        calibre_full = normalize_author_string(r.calibre_authors)
        reference_full = normalize_author_string(
            r.suggested_calibre_authors
            or r.suggested_hardcover_authors
            or r.current_hardcover_authors
            or r.hardcover_authors
            or ""
        )

        if calibre_full:
            full_key = canonical_author_set(calibre_full)
            if full_key:
                cluster = ensure_string_cluster(full_key)
                cluster["calibre_variants"][calibre_full] += 1
                cluster["book_ids"].add(int(r.calibre_book_id))
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
            cluster["book_ids"].add(int(r.calibre_book_id))
            cluster["full_author_strings"][calibre_full] += 1
            if title:
                cluster["titles"].append(title)

        for name in split_author_like_string(reference_full):
            key = normalize_author_key(name)
            if not key:
                continue
            cluster = ensure_name_cluster(key)
            cluster["reference_variants"][name] += 1
            cluster["book_ids"].add(int(r.calibre_book_id))
            if title:
                cluster["titles"].append(title)

    output: List[Dict[str, Any]] = []

    for key, cluster in sorted(name_clusters.items(), key=lambda kv: (-len(kv[1]["book_ids"]), kv[0])):
        calibre_variants: Counter[str] = cluster["calibre_variants"]
        reference_variants: Counter[str] = cluster["reference_variants"]
        if not calibre_variants:
            continue
        suggested_display = _choose_preferred_display(reference_variants) or _choose_preferred_display(calibre_variants)
        dominant_calibre = _choose_preferred_display(calibre_variants)
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
        output.append({
            "review_type": "individual_author_name",
            "canonical_author_key": key,
            "suggested_display_name": suggested_display or dominant_calibre or "",
            "dominant_calibre_variant": dominant_calibre or "",
            "calibre_variant_count": len(calibre_variants),
            "affected_book_count": len(cluster["book_ids"]),
            "calibre_variants": _counter_preview(calibre_variants),
            "hardcover_reference_variants": _counter_preview(reference_variants),
            "calibre_author_strings": _counter_preview(cluster["full_author_strings"], limit=6),
            "sample_titles": preview_names(cluster["titles"], limit=5, max_len=48),
            "sample_calibre_book_ids": _int_preview(cluster["book_ids"]),
            "note": "; ".join(note_parts),
        })

    for key, cluster in sorted(string_clusters.items(), key=lambda kv: (-len(kv[1]["book_ids"]), "|".join(kv[0]))):
        calibre_variants: Counter[str] = cluster["calibre_variants"]
        if len(calibre_variants) <= 1:
            continue
        reference_variants: Counter[str] = cluster["reference_variants"]
        suggested_string = _choose_preferred_display(reference_variants) or _choose_preferred_display(calibre_variants)
        output.append({
            "review_type": "author_string",
            "canonical_author_set_key": " | ".join(key),
            "suggested_display_name": suggested_string or "",
            "dominant_calibre_variant": _choose_preferred_display(calibre_variants) or "",
            "calibre_variant_count": len(calibre_variants),
            "affected_book_count": len(cluster["book_ids"]),
            "calibre_variants": _counter_preview(calibre_variants),
            "hardcover_reference_variants": _counter_preview(reference_variants),
            "sample_titles": preview_names(cluster["titles"], limit=5, max_len=48),
            "sample_calibre_book_ids": _int_preview(cluster["book_ids"]),
            "note": "same canonical author set appears with multiple calibre display strings",
        })

    output.sort(key=lambda row: (row.get("review_type") or "", -(int(row.get("affected_book_count") or 0)), norm(str(row.get("suggested_display_name") or row.get("canonical_author_key") or ""))))
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
    title = row.suggested_calibre_title or row.file_work_title or row.calibre_title or row.hardcover_title or row.current_hardcover_title
    authors = row.suggested_calibre_authors or row.file_work_authors or row.calibre_authors or row.suggested_hardcover_authors or row.current_hardcover_authors
    return norm(strip_series_suffix(title or "")), canonical_author_set(authors or "")


def build_duplicate_review(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    if not rows:
        return []
    parent = list(range(len(rows)))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    work_groups: Dict[str, List[int]] = defaultdict(list)
    edition_groups: Dict[str, List[int]] = defaultdict(list)
    title_author_groups: Dict[Tuple[str, Tuple[str, ...]], List[int]] = defaultdict(list)

    for idx, row in enumerate(rows):
        work_id = _duplicate_candidate_work_id(row)
        if work_id:
            work_groups[work_id].append(idx)
        edition_id = _duplicate_candidate_edition_id(row)
        if edition_id:
            edition_groups[edition_id].append(idx)
        title_key = _duplicate_title_author_key(row)
        if title_key[0] and title_key[1]:
            title_author_groups[title_key].append(idx)

    for groups in (work_groups, edition_groups, title_author_groups):
        for members in groups.values():
            if len(members) > 1:
                base = members[0]
                for other in members[1:]:
                    union(base, other)

    grouped: Dict[int, List[int]] = defaultdict(list)
    for idx in range(len(rows)):
        grouped[find(idx)].append(idx)

    def _confidence_rank(bases: Set[str]) -> Tuple[int, str]:
        if "shared_trusted_hardcover_work" in bases or "shared_hardcover_edition" in bases:
            return 3, "high"
        if "same_normalized_title_author" in bases:
            return 2, "medium"
        return 1, "low"

    output: List[Dict[str, Any]] = []
    group_num = 0
    for _root, members in sorted(grouped.items(), key=lambda kv: (-len(kv[1]), min(rows[i].calibre_book_id for i in kv[1]))):
        if len(members) <= 1:
            continue
        bases: Set[str] = set()
        shared_work_ids = {_duplicate_candidate_work_id(rows[i]) for i in members if _duplicate_candidate_work_id(rows[i])}
        shared_edition_ids = {_duplicate_candidate_edition_id(rows[i]) for i in members if _duplicate_candidate_edition_id(rows[i])}
        title_keys = {_duplicate_title_author_key(rows[i]) for i in members if _duplicate_title_author_key(rows[i])[0] and _duplicate_title_author_key(rows[i])[1]}
        if len(shared_work_ids) == 1 and len(members) == len([i for i in members if _duplicate_candidate_work_id(rows[i])]):
            bases.add("shared_trusted_hardcover_work")
        elif shared_work_ids:
            bases.add("overlapping_hardcover_work")
        if len(shared_edition_ids) == 1 and len(members) == len([i for i in members if _duplicate_candidate_edition_id(rows[i])]):
            bases.add("shared_hardcover_edition")
        if len(title_keys) == 1:
            bases.add("same_normalized_title_author")

        conf_rank, conf_label = _confidence_rank(bases)
        group_num += 1
        group_id = f"D{group_num:04d}"
        group_titles = preview_names([
            rows[i].suggested_calibre_title or rows[i].file_work_title or rows[i].calibre_title or rows[i].hardcover_title
            for i in members
        ], limit=6, max_len=48)
        group_calibre_ids = ", ".join(str(rows[i].calibre_book_id) for i in sorted(members, key=lambda idx: rows[idx].calibre_book_id))
        group_hardcover_ids = ", ".join(sorted(shared_work_ids)) if shared_work_ids else ""
        group_edition_ids = ", ".join(sorted(shared_edition_ids)) if shared_edition_ids else ""

        canonical_author_set_key = ""
        normalized_title_key = ""
        if len(title_keys) == 1:
            only_key = next(iter(title_keys))
            normalized_title_key = only_key[0]
            canonical_author_set_key = " | ".join(only_key[1])

        for idx in sorted(members, key=lambda i: (rows[i].calibre_book_id, norm(rows[i].calibre_title))):
            row = rows[idx]
            output.append({
                "duplicate_group_id": group_id,
                "duplicate_group_size": len(members),
                "duplicate_confidence": conf_label,
                "duplicate_confidence_rank": conf_rank,
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
            })

    output.sort(key=lambda row: (-int(row.get("duplicate_confidence_rank") or 0), -int(row.get("duplicate_group_size") or 0), norm(str(row.get("normalized_title_key") or row.get("calibre_title") or "")), int(row.get("calibre_book_id") or 0)))
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
    return (position_rank, _position_sort_value(pos), -(int(book.get("users_read_count") or 0)), -(int(book.get("users_count") or 0)), norm(str(book.get("title") or "")))


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


def build_owned_author_discovery(rows: List[AuditRow], hc: HardcoverClient, verbose: bool = False) -> List[Dict[str, Any]]:
    trusted_by_book_id: Dict[int, List[AuditRow]] = defaultdict(list)
    owned_author_clusters: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        trusted_book_id = series_scan_trusted_book_id(row)
        if not trusted_book_id:
            continue
        trusted_book_id = int(trusted_book_id)
        trusted_by_book_id[trusted_book_id].append(row)
        author_text = row.suggested_hardcover_authors or row.current_hardcover_authors or row.hardcover_authors or row.calibre_authors
        for author_name in split_author_like_string(author_text):
            key = normalize_author_key(author_name)
            if not key:
                continue
            cluster = owned_author_clusters.setdefault(key, {"display_names": Counter(), "owned_hardcover_book_ids": set(), "owned_calibre_book_ids": set(), "owned_titles": []})
            cluster["display_names"][author_name] += 1
            cluster["owned_hardcover_book_ids"].add(trusted_book_id)
            cluster["owned_calibre_book_ids"].add(int(row.calibre_book_id))
            if row.hardcover_title or row.calibre_title:
                cluster["owned_titles"].append(row.hardcover_title or row.calibre_title)
    if not trusted_by_book_id or not owned_author_clusters:
        return []

    owned_book_ids = set(trusted_by_book_id.keys())
    author_names = [_choose_preferred_display(cluster["display_names"]) or next(iter(cluster["display_names"].keys())) for _key, cluster in sorted(owned_author_clusters.items()) if cluster["display_names"]]
    author_preview = preview_names(author_names, limit=4, max_len=28)
    vlog(verbose, f"[OWNED-AUTHOR-DISCOVERY] trusted_books={len(trusted_by_book_id)} authors={len(author_names)} sample_authors={author_preview}")

    owned_book_labels = {
        book_id: f"{(rows_for_book[0].hardcover_title or rows_for_book[0].calibre_title or '-')} [{book_id}]"
        for book_id, rows_for_book in trusted_by_book_id.items()
        if rows_for_book
    }
    phase0 = hc.stats_snapshot()
    memberships = hc.fetch_book_series_memberships(
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
    vlog(verbose, f"  owned-series books={len(owned_book_ids)} series={len(owned_series_ids)} {hc.stats_delta_text(phase0)}")

    phase1 = hc.stats_snapshot()
    author_catalogs = hc.fetch_books_for_authors(author_names, verbose=verbose, progress_label="author-catalogs")
    author_catalog_book_count = sum(len((catalog or {}).get("books") or []) for catalog in author_catalogs.values())
    vlog(verbose, f"  author-catalogs authors={len(author_names)} matched={len(author_catalogs)} books={author_catalog_book_count} {hc.stats_delta_text(phase1)}")

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
                title = _log_label(book.get("title") or "-", max_len=48)
                standalone_book_labels.setdefault(book_id, f"{title} [{book_id}]" if title and title != "-" else str(book_id))

    phase2 = hc.stats_snapshot()
    series_catalogs = hc.fetch_series_books(list(series_to_author_keys.keys()), verbose=verbose, progress_label="series-discovery") if series_to_author_keys else {}
    candidate_series_rows: List[Dict[str, Any]] = []
    candidate_book_ids: Set[int] = set(standalone_to_author_keys.keys())
    for series_id, author_keys in sorted(series_to_author_keys.items(), key=lambda kv: kv[0]):
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
        candidate_series_rows.append({
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
        })
    vlog(verbose, f"  series-discovery series={len(candidate_series_rows)} {hc.stats_delta_text(phase2)}")

    phase3 = hc.stats_snapshot()
    candidate_book_labels: Dict[int, str] = {}
    for row in candidate_series_rows:
        starter_id = int(row.get("starter_book_id") or 0)
        if starter_id:
            candidate_book_labels[starter_id] = f"{_log_label(row.get('starter_title') or '-', max_len=48)} [{starter_id}]"
    for book_id in standalone_to_author_keys.keys():
        candidate_book_labels.setdefault(book_id, standalone_book_labels.get(book_id) or str(book_id))
    if candidate_book_ids:
        candidate_books, candidate_editions = hc.fetch_books_and_editions_for_books(
            sorted(candidate_book_ids),
            verbose=verbose,
            progress_label="candidate-catalogs",
            display_labels=candidate_book_labels,
        )
    else:
        candidate_books, candidate_editions = {}, {}
    vlog(
        verbose,
        f"  candidate-catalogs books={len(candidate_books)} editions={sum(len(v) for v in candidate_editions.values())} {hc.stats_delta_text(phase3)}",
    )

    output: List[Dict[str, Any]] = []
    used_series_book_ids: Set[int] = set()
    for row in candidate_series_rows:
        starter_id = int(row.get("starter_book_id") or 0)
        book = candidate_books.get(starter_id)
        preferred_choice = _choose_discovery_preferred_edition_info(book, candidate_editions.get(starter_id) or []) if book else EditionChoiceInfo()
        preferred_edition = preferred_choice.chosen
        if not book or not preferred_edition:
            continue
        used_series_book_ids.add(starter_id)
        author_keys = row.get("owned_author_keys") or []
        author_names_text = " | ".join(_choose_preferred_display(owned_author_clusters[key]["display_names"]) for key in author_keys if key in owned_author_clusters and owned_author_clusters[key]["display_names"])
        owned_titles: List[str] = []
        owned_calibre_ids: List[str] = []
        for key in author_keys:
            cluster = owned_author_clusters.get(key)
            if not cluster:
                continue
            owned_titles.extend(cluster["owned_titles"])
            owned_calibre_ids.extend(str(v) for v in sorted(cluster["owned_calibre_book_ids"]))
        output.append({
            "discovery_type": "unowned_series",
            "owned_author_keys": " | ".join(author_keys),
            "owned_author_names": author_names_text,
            "owned_calibre_book_ids": ", ".join(sorted(set(owned_calibre_ids), key=lambda v: int(v))),
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
        })

    for book_id, author_keys in sorted(standalone_to_author_keys.items(), key=lambda kv: kv[0]):
        if book_id in used_series_book_ids:
            continue
        book = candidate_books.get(book_id)
        preferred_choice = _choose_discovery_preferred_edition_info(book, candidate_editions.get(book_id) or []) if book else EditionChoiceInfo()
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
            display_name = _choose_preferred_display(cluster["display_names"])
            if display_name:
                author_names.append(display_name)
            owned_titles.extend(cluster["owned_titles"])
            owned_calibre_ids.extend(str(v) for v in sorted(cluster["owned_calibre_book_ids"]))
        output.append({
            "discovery_type": "unowned_standalone",
            "owned_author_keys": " | ".join(sorted(author_keys)),
            "owned_author_names": " | ".join(author_names),
            "owned_calibre_book_ids": ", ".join(sorted(set(owned_calibre_ids), key=lambda v: int(v))),
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
        })

    output.sort(key=lambda row: (0 if row.get("discovery_type") == "unowned_series" else 1, norm(str(row.get("owned_author_names") or "")), -int(row.get("users_read_count") or 0), norm(str(row.get("series_name") or row.get("title") or "")), norm(str(row.get("title") or ""))))
    if verbose:
        series_count = sum(1 for row in output if row.get("discovery_type") == "unowned_series")
        standalone_count = sum(1 for row in output if row.get("discovery_type") == "unowned_standalone")
        preview = preview_names([f"{row.get('owned_author_names') or '-'} -> {row.get('series_name') or row.get('title') or '-'}" for row in output], limit=4, max_len=56)
        vlog(True, f"  result rows={len(output)} series={series_count} standalones={standalone_count} sample={preview} {hc.stats_delta_text(phase0)}")
    hc.save_cache()
    return output


def _build_outputs_full(
    rows: List[AuditRow],
    output_dir: Path,
    missing_series_books: Optional[List[Dict[str, Any]]] = None,
    owned_author_discovery: Optional[List[Dict[str, Any]]] = None,
) -> None:
    ensure_dir(output_dir)
    row_dicts = [asdict(r) for r in rows]
    write_jsonl(output_dir / "15_all_rows.jsonl", row_dicts)

    missing_series_books = missing_series_books or []
    missing_series_mainline = [
        row for row in missing_series_books
        if str(row.get("missing_slot_bucket") or "") in {"integer_within_declared_primary_range", "unpositioned"}
    ]
    missing_series_fractional = [row for row in missing_series_books if str(row.get("missing_slot_bucket") or "") == "fractional"]
    owned_author_discovery = owned_author_discovery or []
    owned_author_series = [row for row in owned_author_discovery if row.get("discovery_type") == "unowned_series"]
    owned_author_standalones = [row for row in owned_author_discovery if row.get("discovery_type") == "unowned_standalone"]

    hardcover_edition_write_candidates = build_hardcover_edition_write_candidates(rows)
    same_id_edition_write_candidates = build_same_id_edition_write_candidates(rows)
    edition_manual_review_queue = build_edition_manual_review_queue(rows)
    write_plan = build_write_plan(rows)
    duplicate_review = build_duplicate_review(rows)
    author_normalisation_review = build_author_normalisation_review(rows)

    series_issues = [asdict(r) for r in rows if "series" in (r.reason or "").lower()]
    embedded_mismatches = [
        asdict(r) for r in rows
        if r.embedded_title_mismatch_to_calibre
        or r.embedded_authors_mismatch_to_calibre_text
        or r.embedded_authors_mismatch_to_calibre_canonical
    ]

    bucket_defs = build_bucket_definitions(rows, duplicate_review, series_issues)
    bucket_overview_rows: List[Dict[str, Any]] = []
    for filename, label, bucket_rows in bucket_defs:
        write_csv(output_dir / filename, bucket_rows)
        bucket_overview_rows.append({"file": filename, "bucket": label, "count": len(bucket_rows)})

    write_csv(output_dir / "13_embedded_vs_calibre_mismatches.csv", embedded_mismatches)
    bucket_overview_rows.append({"file": "13_embedded_vs_calibre_mismatches.csv", "bucket": "Embedded metadata differs from calibre metadata", "count": len(embedded_mismatches)})

    write_csv(output_dir / "17_missing_series_books.csv", missing_series_books)
    bucket_overview_rows.append({"file": "17_missing_series_books.csv", "bucket": "Series books present on Hardcover but missing from Calibre", "count": len(missing_series_books)})
    write_csv(output_dir / "17a_missing_series_mainline_books.csv", missing_series_mainline)
    bucket_overview_rows.append({"file": "17a_missing_series_mainline_books.csv", "bucket": "Mainline or unpositioned missing series books", "count": len(missing_series_mainline)})
    write_csv(output_dir / "17b_missing_series_fractional_books.csv", missing_series_fractional)
    bucket_overview_rows.append({"file": "17b_missing_series_fractional_books.csv", "bucket": "Fractional-position missing series books", "count": len(missing_series_fractional)})

    write_csv(output_dir / "18_hardcover_edition_write_candidates.csv", hardcover_edition_write_candidates)
    bucket_overview_rows.append({"file": "18_hardcover_edition_write_candidates.csv", "bucket": "Suggested hardcover-edition identifiers for a later write pass", "count": len(hardcover_edition_write_candidates)})
    write_csv(output_dir / "19_current_id_edition_write_candidates.csv", same_id_edition_write_candidates)
    bucket_overview_rows.append({"file": "19_current_id_edition_write_candidates.csv", "bucket": "Suggested hardcover-edition writes where the current Hardcover work id already matches", "count": len(same_id_edition_write_candidates)})
    write_csv(output_dir / "20_edition_manual_review_queue.csv", edition_manual_review_queue)
    bucket_overview_rows.append({"file": "20_edition_manual_review_queue.csv", "bucket": "Edition suggestions that need manual review before any write pass", "count": len(edition_manual_review_queue)})
    write_csv(output_dir / "21_write_plan.csv", write_plan)
    bucket_overview_rows.append({"file": "21_write_plan.csv", "bucket": "Dry-run write plan across calibre and Hardcover identifiers", "count": len(write_plan)})
    write_csv(output_dir / "23_author_normalisation_review.csv", author_normalisation_review)
    bucket_overview_rows.append({"file": "23_author_normalisation_review.csv", "bucket": "Author display-form consistency and normalisation review", "count": len(author_normalisation_review)})

    write_csv(output_dir / "24_owned_authors_discovery.csv", owned_author_discovery)
    bucket_overview_rows.append({"file": "24_owned_authors_discovery.csv", "bucket": "Books and series by owned authors that are not yet in Calibre", "count": len(owned_author_discovery)})
    write_csv(output_dir / "24a_owned_authors_unowned_series.csv", owned_author_series)
    bucket_overview_rows.append({"file": "24a_owned_authors_unowned_series.csv", "bucket": "Series by owned authors that are not yet started in Calibre", "count": len(owned_author_series)})
    write_csv(output_dir / "24b_owned_authors_unowned_standalones.csv", owned_author_standalones)
    bucket_overview_rows.append({"file": "24b_owned_authors_unowned_standalones.csv", "bucket": "Standalone books by owned authors that are not yet in Calibre", "count": len(owned_author_standalones)})

    write_csv(output_dir / "14_bucket_overview.csv", bucket_overview_rows)

    action_counts: Dict[str, int] = defaultdict(int)
    tier_counts: Dict[str, int] = defaultdict(int)
    current_validation_counts: Dict[str, int] = defaultdict(int)
    fix_basis_counts: Dict[str, int] = defaultdict(int)
    matched_by_counts: Dict[str, int] = defaultdict(int)
    for r in rows:
        action_counts[r.recommended_action] += 1
        tier_counts[r.confidence_tier or "unknown"] += 1
        current_validation_counts[(r.current_hardcover_match_ok or "unresolved")] += 1
        fix_basis_counts[r.fix_basis or "unknown"] += 1
        matched_by_counts[r.matched_by or "unknown"] += 1

    top_fix = sorted(rows, key=bucket_sort_key)[:100]
    lines = [
        "# Calibre + Hardcover audit summary",
        "",
        f"- Total books audited: **{len(rows)}**",
        "",
        "## Action counts",
    ]
    for action, count in sorted(action_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"- {action}: **{count}**")

    lines.extend(["", "## Bucket counts"])
    for row in bucket_overview_rows:
        lines.append(f"- {row['file']}: **{row['count']}** — {row['bucket']}")

    lines.extend(["", "## Confidence tiers"])
    for tier, count in sorted(tier_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"- {tier}: **{count}**")

    lines.extend(["", "## Current-link validation"])
    for label, count in [("yes", current_validation_counts.get("yes", 0)), ("no", current_validation_counts.get("no", 0)), ("unresolved", current_validation_counts.get("unresolved", 0))]:
        lines.append(f"- {label}: **{count}**")

    lines.extend(["", "## Top fix bases"])
    for fix_basis, count in sorted(fix_basis_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:15]:
        lines.append(f"- {fix_basis}: **{count}**")

    lines.extend(["", "## Top matched-by reasons"])
    for matched_by, count in sorted(matched_by_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:15]:
        lines.append(f"- {matched_by}: **{count}**")

    lines.extend(["", "## Highest-priority first 100", ""])
    for r in top_fix:
        review_bucket = classify_manual_review_bucket(r) if r.recommended_action == "manual_review" else ""
        bucket_hint = f" | review_bucket={review_bucket}" if review_bucket else ""
        lines.append(
            f"- [{r.recommended_action}] calibre_id={r.calibre_book_id} | {r.calibre_title} | hc={r.suggested_hardcover_id or r.calibre_hardcover_id} | score={r.confidence_score} ({r.confidence_tier}){bucket_hint} | {r.reason}"
        )

    lines.extend(["", "## Output files"])
    for name, _label, _rows in bucket_defs:
        lines.append(f"- {name}")
    lines.extend([
        "- 13_embedded_vs_calibre_mismatches.csv",
        "- 14_bucket_overview.csv",
        "- 15_all_rows.jsonl",
        "- 16_summary.md",
        "- 17_missing_series_books.csv",
        "- 17a_missing_series_mainline_books.csv",
        "- 17b_missing_series_fractional_books.csv",
        "- 18_hardcover_edition_write_candidates.csv",
        "- 19_current_id_edition_write_candidates.csv",
        "- 20_edition_manual_review_queue.csv",
        "- 21_write_plan.csv",
        "- 23_author_normalisation_review.csv",
        "- 24_owned_authors_discovery.csv",
        "- 24a_owned_authors_unowned_series.csv",
        "- 24b_owned_authors_unowned_standalones.csv",
    ])
    if missing_series_books:
        unique_series = len({int(row.get('series_id') or 0) for row in missing_series_books if row.get('series_id')})
        lines.extend(["", "## Missing series books", f"- Missing series-book rows: **{len(missing_series_books)}**", f"- Unique affected series: **{unique_series}**"])
    if author_normalisation_review:
        by_type = Counter(str(row.get("review_type") or "unknown") for row in author_normalisation_review)
        lines.extend(["", "## Author normalisation review", f"- Review rows: **{len(author_normalisation_review)}**"])
        for review_type, count in sorted(by_type.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"- {review_type}: **{count}**")
    if duplicate_review:
        dup_group_count = len({row.get("duplicate_group_id") for row in duplicate_review if row.get("duplicate_group_id")})
        dup_conf_counts = Counter(str(row.get("duplicate_confidence") or "unknown") for row in duplicate_review)
        lines.extend(["", "## Duplicate / near-duplicate review", f"- Duplicate rows: **{len(duplicate_review)}**", f"- Duplicate groups: **{dup_group_count}**"])
        for label, count in sorted(dup_conf_counts.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"- {label}: **{count}**")
    if owned_author_discovery:
        discovery_counts = Counter(str(row.get("discovery_type") or "unknown") for row in owned_author_discovery)
        lines.extend(["", "## Owned-author discovery", f"- Discovery rows: **{len(owned_author_discovery)}**"])
        for label, count in sorted(discovery_counts.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"- {label}: **{count}**")
    (output_dir / "16_summary.md").write_text("\n".join(lines), encoding="utf-8")

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


def _build_compact_audit_actions(rows: List[AuditRow]) -> List[Dict[str, Any]]:
    write_plan_map = {int(row.get("calibre_book_id") or 0): row for row in build_write_plan(rows)}
    duplicate_review = build_duplicate_review(rows)
    author_normalisation_review = build_author_normalisation_review(rows)
    action_rows: List[Dict[str, Any]] = []

    for row in sorted(rows, key=bucket_sort_key):
        if row.recommended_action == "keep_hardcover_id":
            continue
        payload = asdict(row)
        plan_row = write_plan_map.get(int(row.calibre_book_id or 0), {})
        payload.update({
            "phase": "audit",
            "review_source": "book_audit",
            "action_bucket": _audit_action_bucket(row),
            "safe_to_apply_boolean": plan_row.get("safe_to_apply_boolean", ""),
            "safe_to_apply_reason": plan_row.get("safe_to_apply_reason", ""),
        })
        action_rows.append(payload)

    for row in duplicate_review:
        payload = dict(row)
        payload.update({
            "phase": "audit",
            "review_source": "duplicate_review",
            "action_bucket": "duplicate_review",
            "safe_to_apply_boolean": False,
            "safe_to_apply_reason": "duplicate or near-duplicate review required",
        })
        action_rows.append(payload)

    for row in author_normalisation_review:
        payload = dict(row)
        payload.update({
            "phase": "audit",
            "review_source": "author_normalisation_review",
            "action_bucket": "author_normalisation_review",
            "safe_to_apply_boolean": False,
            "safe_to_apply_reason": "author normalisation review required",
        })
        action_rows.append(payload)

    action_rows.sort(key=lambda row: (
        0 if row.get("review_source") == "book_audit" else 1,
        0 if bool(row.get("safe_to_apply_boolean")) else 1,
        -float(row.get("confidence_score") or row.get("confidence") or 0.0),
        int(row.get("calibre_book_id") or 0),
        norm(str(row.get("suggested_calibre_title") or row.get("calibre_title") or row.get("title") or row.get("suggested_display_name") or "")),
    ))
    return action_rows


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


DISCOVERY_PRIORITY_RANKS = {
    "shortlist": 0,
    "manual_review": 1,
    "low_priority_unpositioned": 2,
    "low_priority_side_material": 3,
    "suppressed_translated_sibling": 4,
    "suppressed_zero_editions": 5,
    "suppressed_non_english": 6,
    "suppressed_audio": 7,
}


def _discovery_title_language_bucket(row: Dict[str, Any]) -> str:
    if _text_looks_non_english(
        row.get("display_title") or row.get("title") or "",
        row.get("display_subtitle") or row.get("subtitle") or "",
        row.get("missing_details") or row.get("details") or "",
    ):
        return "non_english"
    return "unknown"


def _discovery_row_has_clear_english_signal(row: Dict[str, Any]) -> bool:
    lang = str(row.get("preferred_edition_language") or "").strip()
    if is_english_language_name(lang):
        return True
    if lang and not is_english_language_name(lang):
        return False
    if int(row.get("edition_candidates_considered") or 0) <= 0:
        return False
    return str(row.get("title_language_bucket") or "") != "non_english"


def _discovery_row_has_english_series_sibling(row: Dict[str, Any], series_groups: Dict[int, List[Dict[str, Any]]]) -> bool:
    series_id = int(row.get("series_id") or 0)
    if not series_id:
        return False
    group = series_groups.get(series_id) or []
    if len(group) <= 1:
        return False
    current_authors = canonical_author_set(str(row.get("display_authors") or row.get("authors") or ""))
    for other in group:
        if other is row:
            continue
        other_authors = canonical_author_set(str(other.get("display_authors") or other.get("authors") or ""))
        if current_authors and other_authors and not set(current_authors).intersection(other_authors):
            continue
        if _discovery_row_has_clear_english_signal(other):
            return True
    return False


def _discovery_row_looks_like_side_material(row: Dict[str, Any]) -> bool:
    gap_kind = str(row.get("gap_kind") or row.get("reason") or "")
    if gap_kind == "unpositioned_missing":
        return True
    text = f" {norm(str(row.get('display_title') or row.get('title') or ''))} {norm(str(row.get('missing_details') or row.get('details') or ''))} "
    return any(token in text for token in DISCOVERY_SIDE_MATERIAL_HINTS)


def _classify_discovery_row(row: Dict[str, Any], series_groups: Dict[int, List[Dict[str, Any]]]) -> Tuple[bool, str, str]:
    fmt_norm = norm(str(row.get("preferred_edition_format_normalized") or ""))
    lang = str(row.get("preferred_edition_language") or "").strip()
    title_language_bucket = str(row.get("title_language_bucket") or "unknown")
    editions_count = int(row.get("edition_candidates_considered") or 0)
    gap_kind = str(row.get("gap_kind") or row.get("reason") or "")
    side_material = bool(row.get("looks_like_side_material"))
    english_sibling = bool(row.get("has_english_series_sibling"))

    if fmt_norm == "audiobook":
        return False, "preferred edition is audiobook; blocked by default", "suppressed_audio"
    if english_sibling and (title_language_bucket == "non_english" or not lang or editions_count <= 0 or (lang and not is_english_language_name(lang))):
        return False, "suppressed as translated duplicate of English series sibling", "suppressed_translated_sibling"
    if editions_count <= 0:
        if title_language_bucket == "non_english":
            return False, "no usable editions on Hardcover and title looks non-English", "suppressed_zero_editions"
        return False, "no usable editions on Hardcover", "suppressed_zero_editions"
    if not lang:
        if title_language_bucket == "non_english":
            return False, "preferred edition has blank language and title looks non-English", "suppressed_non_english"
        return False, "preferred edition has blank language; manual review required", "manual_review"
    if not is_english_language_name(lang):
        return False, f"preferred edition is non-English ({lang})", "suppressed_non_english"
    if gap_kind == "unpositioned_missing":
        return False, "unpositioned series entry; low priority", "low_priority_unpositioned"
    if side_material:
        return False, "translation-like or companion side material; low priority", "low_priority_side_material"
    return True, "ok", "shortlist"


def _build_compact_discovery_candidates(missing_series_books: List[Dict[str, Any]], owned_author_discovery: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    for row in missing_series_books:
        payload = dict(row)
        payload.update({
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
            "edition_candidates_considered": row.get("missing_editions_count") or row.get("missing_preferred_edition_candidates_considered") or 0,
            "title_language_bucket": _discovery_title_language_bucket({
                "display_title": row.get("missing_title") or "",
                "display_subtitle": row.get("missing_subtitle") or "",
                "missing_details": row.get("missing_details") or "",
            }),
        })
        candidates.append(payload)

    for row in owned_author_discovery:
        payload = dict(row)
        payload.update({
            "phase": "discovery",
            "discovery_bucket": str(row.get("discovery_type") or "owned_author_discovery"),
            "display_title": row.get("title") or "",
            "display_authors": row.get("authors") or "",
            "display_series": row.get("series_name") or "",
            "display_book_id": row.get("hardcover_book_id") or "",
            "gap_kind": row.get("gap_kind") or "",
            "edition_candidates_considered": row.get("preferred_edition_candidates_considered") or (1 if row.get("preferred_edition_id") else 0),
            "title_language_bucket": _discovery_title_language_bucket({
                "display_title": row.get("title") or "",
                "display_subtitle": row.get("subtitle") or "",
                "missing_details": row.get("details") or "",
            }),
        })
        candidates.append(payload)

    series_groups: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        series_id = int(row.get("series_id") or 0)
        if series_id:
            series_groups[series_id].append(row)

    for row in candidates:
        row["has_english_series_sibling"] = _discovery_row_has_english_series_sibling(row, series_groups)
        row["looks_like_side_material"] = _discovery_row_looks_like_side_material(row)
        eligible, reason, priority_bucket = _classify_discovery_row(row, series_groups)
        row["eligible_for_shortlist_boolean"] = eligible
        row["shortlist_reason"] = reason
        row["discovery_priority_bucket"] = priority_bucket

    candidates.sort(key=lambda row: (
        0 if bool(row.get("eligible_for_shortlist_boolean")) else 1,
        DISCOVERY_PRIORITY_RANKS.get(str(row.get("discovery_priority_bucket") or "manual_review"), 99),
        0 if row.get("discovery_bucket") == "missing_series" else 1,
        -int(row.get("users_read_count") or 0),
        -int(row.get("users_count") or 0),
        norm(str(row.get("display_series") or row.get("series_name") or "")),
        norm(str(row.get("display_title") or row.get("title") or "")),
    ))
    return candidates


def _filter_compact_write_plan_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        if row.get("action_type") != "keep_hardcover_id":
            out.append(row)
            continue
        if row.get("safe_to_apply_boolean"):
            out.append(row)
            continue
        if any([
            str(row.get("current_calibre_title") or "") != str(row.get("new_calibre_title") or ""),
            str(row.get("current_calibre_author") or "") != str(row.get("new_calibre_author") or ""),
            str(row.get("current_hardcover_id") or "") != str(row.get("new_hardcover_id") or ""),
            str(row.get("current_hardcover_edition_id") or "") != str(row.get("new_hardcover_edition_id") or ""),
            str(row.get("safe_to_apply_reason") or "") != "ok",
        ]):
            out.append(row)
    return out


def _write_compact_summary(path: Path, title: str, lines: List[str]) -> None:
    ensure_dir(path.parent)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def build_outputs(
    rows: List[AuditRow],
    output_dir: Path,
    missing_series_books: Optional[List[Dict[str, Any]]] = None,
    owned_author_discovery: Optional[List[Dict[str, Any]]] = None,
    output_mode: str = "compact",
) -> Dict[str, Path]:
    if output_mode == "full":
        _build_outputs_full(rows, output_dir, missing_series_books=missing_series_books, owned_author_discovery=owned_author_discovery)
        return {
            "root": output_dir,
            "audit_summary": output_dir / "16_summary.md",
            "discovery_summary": output_dir / "16_summary.md",
            "audit_actions": output_dir / "14_bucket_overview.csv",
            "discovery_candidates": output_dir / "24_owned_authors_discovery.csv",
            "audit_write_plan": output_dir / "21_write_plan.csv",
        }

    ensure_dir(output_dir)
    audit_dir = output_dir / "audit"
    discovery_dir = output_dir / "discovery"
    ensure_dir(audit_dir)
    ensure_dir(discovery_dir)

    missing_series_books = missing_series_books or []
    owned_author_discovery = owned_author_discovery or []

    audit_actions = _build_compact_audit_actions(rows)
    write_plan = _filter_compact_write_plan_rows(build_write_plan(rows))
    discovery_candidates = _build_compact_discovery_candidates(missing_series_books, owned_author_discovery)

    write_csv(audit_dir / "actions.csv", audit_actions)
    write_csv(audit_dir / "write_plan.csv", write_plan)
    write_csv(discovery_dir / "candidates.csv", discovery_candidates)

    action_counts = Counter(r.recommended_action for r in rows)
    tier_counts = Counter(r.confidence_tier or "unknown" for r in rows)
    action_bucket_counts = Counter(str(row.get("action_bucket") or "unknown") for row in audit_actions)
    review_source_counts = Counter(str(row.get("review_source") or "unknown") for row in audit_actions)
    safe_actions = sum(1 for row in audit_actions if bool(row.get("safe_to_apply_boolean")))
    top_fix = sorted((r for r in rows if r.recommended_action != "keep_hardcover_id"), key=bucket_sort_key)[:100]

    audit_summary_lines = [
        "# Audit summary",
        "",
        f"- Total books audited: **{len(rows)}**",
        f"- Action rows written: **{len(audit_actions)}**",
        f"- Write-plan rows written: **{len(write_plan)}**",
        f"- Safe-to-apply audit rows: **{safe_actions}**",
        "",
        "## Recommended actions",
    ]
    for action, count in sorted(action_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        audit_summary_lines.append(f"- {action}: **{count}**")
    audit_summary_lines.extend(["", "## Action buckets in actions.csv"])
    for label, count in sorted(action_bucket_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        audit_summary_lines.append(f"- {label}: **{count}**")
    audit_summary_lines.extend(["", "## Review sources"])
    for label, count in sorted(review_source_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        audit_summary_lines.append(f"- {label}: **{count}**")
    audit_summary_lines.extend(["", "## Confidence tiers"])
    for tier, count in sorted(tier_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        audit_summary_lines.append(f"- {tier}: **{count}**")
    audit_summary_lines.extend(["", "## Highest-priority rows", ""])
    for r in top_fix:
        review_bucket = classify_manual_review_bucket(r) if r.recommended_action == "manual_review" else ""
        bucket_hint = f" | review_bucket={review_bucket}" if review_bucket else ""
        audit_summary_lines.append(
            f"- [{r.recommended_action}] calibre_id={r.calibre_book_id} | {r.calibre_title} | hc={r.suggested_hardcover_id or r.calibre_hardcover_id} | score={r.confidence_score} ({r.confidence_tier}){bucket_hint} | {r.reason}"
        )
    audit_summary_lines.extend([
        "",
        "## Files",
        "- actions.csv — master actionable audit sheet",
        "- write_plan.csv — dry-run metadata / identifier write plan",
    ])
    _write_compact_summary(audit_dir / "summary.md", "Audit summary", audit_summary_lines)

    discovery_bucket_counts = Counter(str(row.get("discovery_bucket") or "unknown") for row in discovery_candidates)
    shortlist_count = sum(1 for row in discovery_candidates if bool(row.get("eligible_for_shortlist_boolean")))
    shortlist_reason_counts = Counter(str(row.get("shortlist_reason") or "unknown") for row in discovery_candidates)
    discovery_summary_lines = [
        "# Discovery summary",
        "",
        f"- Discovery rows written: **{len(discovery_candidates)}**",
        f"- Shortlist-eligible rows: **{shortlist_count}**",
        f"- Manual-review / suppressed rows: **{len(discovery_candidates) - shortlist_count}**",
        "",
        "## Discovery buckets",
    ]
    for label, count in sorted(discovery_bucket_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        discovery_summary_lines.append(f"- {label}: **{count}**")
    discovery_summary_lines.extend(["", "## Shortlist reasons"])
    for label, count in sorted(shortlist_reason_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        discovery_summary_lines.append(f"- {label}: **{count}**")
    discovery_summary_lines.extend([
        "",
        "## Files",
        "- candidates.csv — unified discovery sheet for missing-series and owned-author candidates",
    ])
    _write_compact_summary(discovery_dir / "summary.md", "Discovery summary", discovery_summary_lines)

    readme_lines = [
        "# Output overview",
        "",
        "## Audit",
        f"- Summary: `{(audit_dir / 'summary.md').name}`",
        f"- Actions: `{(audit_dir / 'actions.csv').name}`",
        f"- Write plan: `{(audit_dir / 'write_plan.csv').name}`",
        "",
        "## Discovery",
        f"- Summary: `{(discovery_dir / 'summary.md').name}`",
        f"- Candidates: `{(discovery_dir / 'candidates.csv').name}`",
        "",
        "`run.log` remains in the root output directory for the full execution trace.",
    ]
    _write_compact_summary(output_dir / "README.md", "Output overview", readme_lines)

    return {
        "root": output_dir,
        "audit_summary": audit_dir / "summary.md",
        "audit_actions": audit_dir / "actions.csv",
        "audit_write_plan": audit_dir / "write_plan.csv",
        "discovery_summary": discovery_dir / "summary.md",
        "discovery_candidates": discovery_dir / "candidates.csv",
        "readme": output_dir / "README.md",
    }


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit a local Calibre library against Hardcover and ebook files")
    p.add_argument("--library-root", type=Path, default=Path("."), help="Calibre library root containing metadata.db")
    p.add_argument("--metadata-db", type=Path, default=None)
    p.add_argument("--output-dir", type=Path, default=None, help="Output directory. Defaults to a timestamped folder under --library-root")
    p.add_argument("--cache-path", type=Path, default=None, help="Path to the persistent Hardcover cache SQLite database. Defaults to <library-root>/hardcover_cache.sqlite")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--ebook-meta-command", type=str, default=None)
    p.add_argument("--docker-ebook-meta-container", type=str, default=None)
    p.add_argument("--container-library-root", type=str, default="/calibre-library")
    p.add_argument("--author-aliases-json", type=Path, default=None, help="Optional JSON file mapping author aliases to canonical names")
    p.add_argument("--ebook-meta-host-timeout", type=int, default=15, help="Timeout in seconds for host ebook-meta calls")
    p.add_argument("--ebook-meta-docker-timeout", type=int, default=20, help="Timeout in seconds for docker ebook-meta calls")
    p.add_argument("--hardcover-timeout", type=int, default=15, help="Timeout in seconds for each Hardcover API request")
    p.add_argument("--hardcover-retries", type=int, default=2, help="Retries for transient Hardcover API failures")
    p.add_argument("--hardcover-user-agent", type=str, default=HARDCOVER_DEFAULT_USER_AGENT, help="User-Agent header to send to Hardcover")
    p.add_argument("--hardcover-min-interval", type=float, default=1.0, help="Minimum interval in seconds between Hardcover requests (default: 1.0, about 60 requests/minute before any cooldowns)")
    p.add_argument("--cache-ttl-hours", type=float, default=DEFAULT_CACHE_TTL_HOURS, help="TTL in hours for most Hardcover cache entries (default: 168)")
    p.add_argument("--search-cache-ttl-hours", type=float, default=DEFAULT_SEARCH_CACHE_TTL_HOURS, help="TTL in hours for Hardcover search / identifier cache entries (default: 72)")
    p.add_argument("--empty-cache-ttl-hours", type=float, default=DEFAULT_EMPTY_CACHE_TTL_HOURS, help="TTL in hours for cached empty Hardcover responses (default: 168)")
    p.add_argument("--edition-cache-ttl-hours", type=float, default=DEFAULT_EDITION_CACHE_TTL_HOURS, help="TTL in hours for cached Hardcover edition payloads (default: 720)")
    p.add_argument("--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY, help="Emit an audit progress checkpoint every N books when --verbose is enabled")
    p.add_argument("--debug-hardcover", action="store_true", help="Emit low-level Hardcover cache and HTTP logs as well as the compact verbose audit log")
    p.add_argument("--output-mode", choices=["compact", "full"], default="compact", help="Write either compact action/discovery outputs (default) or the legacy full output pack")
    return p.parse_args(argv)

def main(argv: Optional[List[str]] = None) -> int:
    global AUTHOR_ALIAS_MAP
    args = parse_args(argv)
    AUTHOR_ALIAS_MAP = load_author_alias_map(args.author_aliases_json)
    token = os.environ.get("HARDCOVER_TOKEN", "").strip()
    if not token:
        print("ERROR: HARDCOVER_TOKEN environment variable is not set", file=sys.stderr)
        return 2
    if token.startswith("Bearer "):
        print("ERROR: HARDCOVER_TOKEN should be the raw token only, without the 'Bearer ' prefix", file=sys.stderr)
        return 2

    library_root = args.library_root.resolve()
    metadata_db = find_metadata_db(library_root, args.metadata_db.resolve() if args.metadata_db else None)
    output_dir = args.output_dir.resolve() if args.output_dir else default_output_dir(library_root)
    ensure_dir(output_dir)
    cache_path = args.cache_path.resolve() if args.cache_path else (library_root / CACHE_FILENAME)
    legacy_cache_json_path = library_root / LEGACY_CACHE_FILENAME
    ensure_dir(cache_path.parent)
    log_path = output_dir / "run.log"

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    with log_path.open("w", encoding="utf-8") as log_handle:
        sys.stdout = TeeStream(original_stdout, log_handle)
        sys.stderr = TeeStream(original_stderr, log_handle)
        try:
            print(f"Using library root: {library_root}")
            print(f"Using metadata DB: {metadata_db}")
            print(f"Writing outputs to: {output_dir}")
            print(f"Using cache DB: {cache_path}")
            if legacy_cache_json_path.exists():
                print(f"Legacy JSON cache detected: {legacy_cache_json_path}")
            print(f"Writing log to: {log_path}")
            records = load_calibre_books(metadata_db, library_root)
            print(f"Loaded {len(records)} calibre records")
            if args.verbose:
                print(f"Verbose audit logging enabled (progress every {args.progress_every} books; low-level Hardcover debug={'on' if args.debug_hardcover else 'off'})")
            hc = HardcoverClient(token=token, cache_path=cache_path, timeout=args.hardcover_timeout, retries=args.hardcover_retries, user_agent=args.hardcover_user_agent, min_interval=args.hardcover_min_interval, verbose=args.verbose, cache_ttl_hours=args.cache_ttl_hours, search_cache_ttl_hours=args.search_cache_ttl_hours, empty_cache_ttl_hours=args.empty_cache_ttl_hours, edition_cache_ttl_hours=args.edition_cache_ttl_hours, legacy_cache_json_path=legacy_cache_json_path, debug_hardcover=args.debug_hardcover)
            ebook_meta_runner = EbookMetaRunner(
                library_root=library_root,
                ebook_meta_command=args.ebook_meta_command,
                docker_container_name=args.docker_ebook_meta_container,
                container_library_root=args.container_library_root,
                host_timeout=args.ebook_meta_host_timeout,
                docker_timeout=args.ebook_meta_docker_timeout,
            )
            print("Starting main audit pass...")
            rows = audit_books(records, hc=hc, ebook_meta_runner=ebook_meta_runner, limit=args.limit, verbose=args.verbose, progress_every=args.progress_every)
            print("Starting missing-series pass...")
            missing_series_books = build_missing_series_books(rows, hc=hc, verbose=args.verbose)
            print("Starting owned-author discovery pass...")
            owned_author_discovery = build_owned_author_discovery(rows, hc=hc, verbose=args.verbose)
            output_paths = build_outputs(rows, output_dir, missing_series_books=missing_series_books, owned_author_discovery=owned_author_discovery, output_mode=args.output_mode)
            existing_edition_count = sum(1 for r in rows if extract_numeric_id(r.current_hardcover_edition_id))
            same_id_write_count = sum(1 for row in build_same_id_edition_write_candidates(rows) if row.get("safe_for_current_id_write_pass"))
            review_queue_count = len(build_edition_manual_review_queue(rows))
            print(f"Rows with an existing stored hardcover-edition: {existing_edition_count}")
            print(f"Safe same-current-id edition write candidates: {same_id_write_count}")
            print(f"Edition manual review queue size: {review_queue_count}")
            hc.print_stats_summary()
            print("Done.")
            if args.output_mode == 'full':
                print(f"Summary: {output_paths['audit_summary']}")
            else:
                print(f"Audit summary: {output_paths['audit_summary']}")
                print(f"Discovery summary: {output_paths['discovery_summary']}")
                print(f"Audit actions: {output_paths['audit_actions']}")
                print(f"Discovery candidates: {output_paths['discovery_candidates']}")
            return 0
        finally:
            try:
                if "hc" in locals() and hc is not None:
                    hc.close()
            except Exception:
                pass
            sys.stdout = original_stdout
            sys.stderr = original_stderr

if __name__ == "__main__":
    raise SystemExit(main())
