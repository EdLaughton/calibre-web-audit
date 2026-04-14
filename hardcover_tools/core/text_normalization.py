from __future__ import annotations

import html
import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Set, Tuple

ISBN_SUFFIX_PATTERN = r"\s*\((?:97[89]\d{10}|\d{10})\)\s*$"
YEAR_SUFFIX_PATTERN = r"\s*\((?:19|20)\d{2}\)\s*$"

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

MARKETING_SUFFIX_PATTERNS = [
    r"\s*:\s*A Novel\s*$",
    r"\s*\(With Bonus Chapter\)\s*$",
    r"\s*:\s*The World of [^:]+$",
    r"\s*The Brand New Must-Read [^-]+$",
    r"\s*Series By [^-]+ collection Set\s*$",
]

SERIES_PREFIX_PATTERNS = [
    r"^\s*(?:book|bk|vol(?:ume)?|part|episode|issue)\s+\d+(?:\.\d+)?\s*[-:–—]\s*",
    r"^\s*\d+(?:\.\d+)?\s*[-:–—]\s*",
    r"^\s*[^-:]{1,80}?\s+\d+(?:\.\d+)?\s*[-:–—]\s*",
]

TRAILING_METADATA_KEYWORDS = {
    "book",
    "series",
    "trilogy",
    "chronicles",
    "files",
    "saga",
    "cycle",
    "world",
    "verse",
    "bonus chapter",
    "collection",
    "collection set",
    "box set",
    "omnibus",
    "edition",
    "must read",
}

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

AUTHOR_ALIAS_MAP: Dict[str, str] = {}


def norm(value: str) -> str:
    text = value or ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[’'`]", "", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def smart_title(title: str) -> str:
    text = html.unescape((title or "").replace("\xa0", " "))
    return re.sub(r"\s+", " ", text.strip()).strip("[] ").strip()


def load_author_alias_map(path: Optional[Path]) -> Dict[str, str]:
    if not path:
        return {}
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("author aliases JSON must be an object mapping alias -> canonical author")
    output: Dict[str, str] = {}
    for key, value in raw.items():
        normalized_key = norm(str(key or ""))
        normalized_value = norm(str(value or ""))
        if normalized_key and normalized_value:
            output[normalized_key] = normalized_value
    return output


def normalize_author_key(name: str, alias_map: Optional[Mapping[str, str]] = None) -> str:
    key = norm(name)
    aliases = AUTHOR_ALIAS_MAP if alias_map is None else dict(alias_map)
    return aliases.get(key, key)


def clean_title_for_matching(title: str) -> str:
    raw = smart_title(title)
    if not raw:
        return ""
    current = raw
    if current.startswith("[") and current.endswith("]"):
        current = current[1:-1].strip()

    def tail_has_metadata_keywords(value: str) -> bool:
        normalized = norm(value)
        if not normalized:
            return False
        words = set(normalized.split())
        padded = f" {normalized} "
        for keyword in TRAILING_METADATA_KEYWORDS:
            canonical = norm(keyword)
            if not canonical:
                continue
            if " " in canonical:
                if f" {canonical} " in padded:
                    return True
            elif canonical in words:
                return True
        return (
            bool(re.search(r"(?:book|series|vol(?:ume)?|part|episode|issue)\s+\d+(?:\.\d+)?", normalized))
            or bool(re.search(r"(?:collection\s+set|box\s+set|omnibus|edition)", normalized))
            or bool(re.search(r"a\s+novel", normalized))
        )

    def looks_title_like(value: str) -> bool:
        candidate = smart_title(value)
        if not candidate:
            return False
        normalized = norm(candidate)
        if len(normalized) < 3:
            return False
        if not re.search(r"[A-Za-z]", candidate):
            return False
        return True

    def strip_leading_series_prefix(value: str) -> str:
        current_value = value
        for pattern in SERIES_PREFIX_PATTERNS:
            new_value = re.sub(pattern, "", current_value, flags=re.I).strip()
            if new_value != current_value and looks_title_like(new_value):
                return new_value
        return current_value

    changed = True
    while changed and current:
        changed = False
        new_title = re.sub(ISBN_SUFFIX_PATTERN, "", current, flags=re.I).strip()
        if new_title != current:
            current = new_title
            changed = True
            continue
        new_title = re.sub(YEAR_SUFFIX_PATTERN, "", current, flags=re.I).strip()
        if new_title != current:
            current = new_title
            changed = True
            continue
        for pattern in MARKETING_SUFFIX_PATTERNS:
            new_title = re.sub(pattern, "", current, flags=re.I).strip()
            if new_title != current:
                current = new_title
                changed = True
                break
        if changed:
            continue
        new_title = strip_leading_series_prefix(current)
        if new_title != current:
            current = new_title
            changed = True
            continue
        match = re.search(r"\s*\(([^()]*)\)\s*$", current)
        if match:
            inner = match.group(1).strip()
            inner_norm = re.sub(r"\s+", "", norm(inner))
            if re.fullmatch(r"(?:97[89]\d{10}|\d{10}|(?:19|20)\d{2})", inner_norm) or tail_has_metadata_keywords(inner):
                current = current[: match.start()].strip()
                changed = True
                continue
        match = re.search(r"\s*[-–—:]\s*([^-–—:]+)\s*$", current)
        if match and tail_has_metadata_keywords(match.group(1).strip()):
            current = current[: match.start()].strip()
            changed = True
            continue

    current = re.sub(r"\s+", " ", current)
    current = re.sub(r"\s*[:;,.]+\s*$", "", current)
    return current.strip(" -:;,.[]")


def title_query_variants(title: str) -> List[str]:
    variants: List[str] = []
    raw = smart_title(title)
    for candidate in [
        raw,
        clean_title_for_matching(title),
        re.sub(r"\s*\([^)]*\)\s*$", "", raw).strip(" -:;,.[ ]"),
        re.sub(r":\s*(A Novel|With Bonus Chapter|The World of .+)$", "", raw, flags=re.I).strip(" -:;,.[ ]"),
        re.sub(r"^\s*(?:book|bk|vol(?:ume)?|part|episode|issue)\s+\d+(?:\.\d+)?\s*[-:–—]\s*", "", raw, flags=re.I).strip(" -:;,.[ ]"),
        re.sub(r"^\s*\d+(?:\.\d+)?\s*[-:–—]\s*", "", raw, flags=re.I).strip(" -:;,.[ ]"),
        re.sub(r"^\s*[^-:]{1,80}?\s+\d+(?:\.\d+)?\s*[-:–—]\s*", "", raw, flags=re.I).strip(" -:;,.[ ]"),
        re.sub(r"\s*[-–—:]\s*[^-–—:]*(?:series|book)\s+\d+[^-–—:]*$", "", raw, flags=re.I).strip(" -:;,.[ ]"),
    ]:
        candidate = smart_title(candidate)
        candidate = re.sub(r"\s+", " ", candidate).strip(" -:;,.[]")
        if candidate and candidate not in variants:
            variants.append(candidate)
    return variants


def normalize_search_query_title(title: str) -> str:
    current = smart_title(title)
    current = html.unescape(current)
    current = clean_title_for_matching(current)
    current = re.sub(r"\s*\([^)]{0,80}\)\s*$", "", current).strip(" -:;,.[]")
    current = re.sub(
        r"\s*[:–—-]\s*(?:a novel|with bonus chapter|the world of .+)$",
        "",
        current,
        flags=re.I,
    ).strip(" -:;,.[]")
    current = re.sub(r"\s+", " ", current)
    return current.strip()


def normalize_person_name(name: str) -> str:
    current = (name or "").strip().strip(";").strip(",")
    current = re.sub(r"\s*\[[^\]]+\]\s*$", "", current).strip()
    if "," in current:
        parts = [part.strip() for part in current.split(",") if part.strip()]
        if len(parts) >= 2:
            current = " ".join(parts[1:] + [parts[0]])
    current = re.sub(r"\s+", " ", current).strip()
    return current


def split_author_like_string(value: str) -> List[str]:
    current = (value or "").strip()
    current = re.sub(r"\s*\[[^\]]+\]\s*$", "", current).strip()
    parts = re.split(r"\s*;\s*|\s*&\s*", current) if (";" in current or " & " in current) else [current]
    output: List[str] = []
    for part in parts:
        normalized = normalize_person_name(part)
        if normalized and normalized not in output:
            output.append(normalized)
    return output


def normalize_author_string(value: str) -> str:
    return " & ".join(split_author_like_string(value))


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
    if "author" in role_norm or "writer" in role_norm:
        return True
    return False


def authors_from_contributions(
    contributions: List[Dict[str, object]],
    alias_map: Optional[Mapping[str, str]] = None,
) -> str:
    primary_names: List[str] = []
    fallback_names: List[str] = []
    seen_primary: Set[str] = set()
    seen_fallback: Set[str] = set()
    for contribution in contributions or []:
        author = (contribution.get("author") or {}) if isinstance(contribution, dict) else {}
        author_name = normalize_person_name(str((author or {}).get("name") or "").strip())
        if not author_name:
            continue
        author_key = normalize_author_key(author_name, alias_map=alias_map)
        if author_key not in seen_fallback:
            fallback_names.append(author_name)
            seen_fallback.add(author_key)
        role = str(contribution.get("contribution") or "") if isinstance(contribution, dict) else ""
        if is_primary_author_contribution(role) and author_key not in seen_primary:
            primary_names.append(author_name)
            seen_primary.add(author_key)
    chosen = primary_names or fallback_names
    return normalize_author_string(" & ".join(chosen))


def canonical_author_set(
    value: str,
    alias_map: Optional[Mapping[str, str]] = None,
) -> Tuple[str, ...]:
    return tuple(
        sorted(
            {
                normalize_author_key(part, alias_map=alias_map)
                for part in split_author_like_string(value)
                if normalize_author_key(part, alias_map=alias_map)
            }
        )
    )


def _author_initial_surname_key(name: str) -> str:
    parts = [part for part in norm(name).split() if part]
    if len(parts) < 2:
        return parts[0] if parts else ""
    surname = parts[-1]
    initials = "".join(part[0] for part in parts[:-1] if part)
    return f"{initials} {surname}".strip()


def author_match_set(value: str, alias_map: Optional[Mapping[str, str]] = None) -> Set[str]:
    output: Set[str] = set()
    for part in split_author_like_string(value):
        canonical = normalize_author_key(part, alias_map=alias_map)
        if canonical:
            output.add(canonical)
            alt = _author_initial_surname_key(canonical)
            if alt:
                output.add(alt)
    return output


def primary_author(authors: str) -> str:
    parts = split_author_like_string(authors)
    return parts[0] if parts else ""


def strip_series_suffix(title: str) -> str:
    return clean_title_for_matching(title)
