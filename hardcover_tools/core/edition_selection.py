from __future__ import annotations

import re
from typing import Any, Optional

from . import _legacy_backend
from .models import HardcoverEdition
from .text_normalization import norm, smart_title

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


choose_preferred_edition_info = _legacy_backend.choose_preferred_edition_info
