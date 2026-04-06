from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Mapping

from .models import EmbeddedMeta
from .text_normalization import (
    author_match_set,
    canonical_author_set,
    clean_title_for_matching,
    norm,
    normalize_author_key,
    normalize_author_string,
    primary_author,
    smart_title,
    split_author_like_string,
    strip_series_suffix,
)


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


def textually_distinct_titles(a: str, b: str) -> bool:
    left = smart_title(a)
    right = smart_title(b)
    return bool(left and right and norm(left) != norm(right))


def textually_distinct_authors(a: str, b: str) -> bool:
    left = normalize_author_string(a)
    right = normalize_author_string(b)
    return bool(left and right and left != right)


def canonically_distinct_authors(
    a: str,
    b: str,
    alias_map: Mapping[str, str] | None = None,
) -> bool:
    left = canonical_author_set(a, alias_map=alias_map)
    right = canonical_author_set(b, alias_map=alias_map)
    return bool(left and right and left != right)


def summarize_embedded_mismatch(
    embedded: EmbeddedMeta,
    calibre_title: str,
    calibre_authors: str,
    suggested_title: str,
    suggested_authors: str,
    alias_map: Mapping[str, str] | None = None,
) -> str:
    tags: list[str] = []
    if embedded.embedded_title and calibre_title and textually_distinct_titles(embedded.embedded_title, calibre_title):
        tags.append("embedded_title_vs_calibre")
    if embedded.embedded_authors and calibre_authors and textually_distinct_authors(embedded.embedded_authors, calibre_authors):
        tags.append("embedded_authors_text_vs_calibre")
    if embedded.embedded_authors and calibre_authors and canonically_distinct_authors(
        embedded.embedded_authors,
        calibre_authors,
        alias_map=alias_map,
    ):
        tags.append("embedded_authors_canonical_vs_calibre")
    if embedded.embedded_title and suggested_title and textually_distinct_titles(embedded.embedded_title, suggested_title):
        tags.append("embedded_title_vs_suggested")
    if embedded.embedded_authors and suggested_authors and textually_distinct_authors(embedded.embedded_authors, suggested_authors):
        tags.append("embedded_authors_text_vs_suggested")
    if embedded.embedded_authors and suggested_authors and canonically_distinct_authors(
        embedded.embedded_authors,
        suggested_authors,
        alias_map=alias_map,
    ):
        tags.append("embedded_authors_canonical_vs_suggested")
    return ";".join(tags)


def title_similarity(a: str, b: str) -> float:
    left = norm(a)
    right = norm(b)
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    return SequenceMatcher(None, left, right).ratio()


def bare_title_similarity(a: str, b: str) -> float:
    return title_similarity(strip_series_suffix(a), strip_series_suffix(b))


def author_similarity(
    a: str,
    b: str,
    alias_map: Mapping[str, str] | None = None,
) -> float:
    left = set(canonical_author_set(a, alias_map=alias_map))
    right = set(canonical_author_set(b, alias_map=alias_map))
    if not left or not right:
        return 0.0
    inter = len(left & right)
    union = len(left | right)
    return inter / union if union else 0.0


def author_coverage(
    file_authors: str,
    other_authors: str,
    alias_map: Mapping[str, str] | None = None,
) -> float:
    left = set(canonical_author_set(file_authors, alias_map=alias_map))
    right = set(canonical_author_set(other_authors, alias_map=alias_map))
    if not left or not right:
        return 0.0
    inter = len(left & right)
    return inter / len(left)


def contributor_count(authors: str, alias_map: Mapping[str, str] | None = None) -> int:
    return len(canonical_author_set(authors, alias_map=alias_map))


def normalize_author_csv(authors: str, alias_map: Mapping[str, str] | None = None) -> str:
    return " | ".join(canonical_author_set(authors, alias_map=alias_map))


def normalize_primary_author_value(authors: str, alias_map: Mapping[str, str] | None = None) -> str:
    primary = primary_author(authors)
    return normalize_author_key(primary, alias_map=alias_map) if primary else ""


def explain_author_mismatch(
    file_authors: str,
    other_authors: str,
    alias_map: Mapping[str, str] | None = None,
) -> str:
    file_keys = set(canonical_author_set(file_authors, alias_map=alias_map))
    other_keys = set(canonical_author_set(other_authors, alias_map=alias_map))
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


def confidence_tier(score: float) -> str:
    if score >= 90:
        return "high"
    if score >= 75:
        return "medium"
    return "low"


def is_partial_author_match(
    authors_a: str,
    authors_b: str,
    alias_map: Mapping[str, str] | None = None,
) -> bool:
    left = author_match_set(authors_a, alias_map=alias_map)
    right = author_match_set(authors_b, alias_map=alias_map)
    return bool(left and right and left & right)


def primary_author_overlap(
    authors_a: str,
    authors_b: str,
    alias_map: Mapping[str, str] | None = None,
) -> bool:
    left = split_author_like_string(authors_a)
    right = split_author_like_string(authors_b)
    if not left or not right:
        return False
    return normalize_author_key(left[0], alias_map=alias_map) == normalize_author_key(right[0], alias_map=alias_map)
