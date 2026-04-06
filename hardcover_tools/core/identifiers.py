from __future__ import annotations

import re

HARDCOVER_ID = "hardcover-id"
HARDCOVER_SLUG = "hardcover-slug"
HARDCOVER_EDITION = "hardcover-edition"

CANONICAL_HARDCOVER_IDENTIFIERS = (
    HARDCOVER_ID,
    HARDCOVER_SLUG,
    HARDCOVER_EDITION,
)

LEGACY_IDENTIFIER_ALIASES = {
    "hardcover": HARDCOVER_ID,
    "hardcover-book": HARDCOVER_ID,
    "hardcover-book-id": HARDCOVER_ID,
    "hardcover-book-slug": HARDCOVER_SLUG,
    "hardcover-edition-id": HARDCOVER_EDITION,
    "hardcover_edition_id": HARDCOVER_EDITION,
    "hardcover_edition": HARDCOVER_EDITION,
}


def canonicalize_identifier_name(name: str) -> str:
    normalized = str(name or "").strip().lower().replace("_", "-")
    return LEGACY_IDENTIFIER_ALIASES.get(normalized, normalized)


def extract_numeric_id(value: str) -> str:
    match = re.search(r"\b(\d{3,})\b", str(value or ""))
    return match.group(1) if match else ""


def clean_isbn(value: str) -> str:
    return (value or "").strip().upper().replace("-", "").replace(" ", "")
