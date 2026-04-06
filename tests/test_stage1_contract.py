from hardcover_tools.core.edition_selection import (
    is_edition_write_blocked_row,
    normalize_edition_format,
)
from hardcover_tools.core.identifiers import (
    CANONICAL_HARDCOVER_IDENTIFIERS,
    HARDCOVER_EDITION,
    HARDCOVER_ID,
    HARDCOVER_SLUG,
    canonicalize_identifier_name,
)
from hardcover_tools.core.matching import confidence_tier
from hardcover_tools.core.text_normalization import (
    canonical_author_set,
    clean_title_for_matching,
)


def test_identifier_contract_uses_canonical_names() -> None:
    assert CANONICAL_HARDCOVER_IDENTIFIERS == (
        HARDCOVER_ID,
        HARDCOVER_SLUG,
        HARDCOVER_EDITION,
    )
    assert canonicalize_identifier_name("hardcover-edition-id") == HARDCOVER_EDITION
    assert canonicalize_identifier_name("hardcover_edition_id") == HARDCOVER_EDITION


def test_clean_title_for_matching_strips_fixture_suffixes() -> None:
    assert clean_title_for_matching("Ready Player Two (9781524761356)") == "Ready Player Two"
    assert clean_title_for_matching("Royal Assassin (The Illustrated Edition)") == "Royal Assassin"
    assert clean_title_for_matching("Darkest Fear: A Novel") == "Darkest Fear"


def test_canonical_author_set_normalizes_last_name_first() -> None:
    assert canonical_author_set("Cline, Ernest") == ("ernest cline",)
    assert canonical_author_set("Robin Hobb & Stephen Youll & John Howe") == (
        "john howe",
        "robin hobb",
        "stephen youll",
    )


def test_edition_helpers_preserve_stage1_write_guards() -> None:
    assert normalize_edition_format("Kindle", "Ebook") == "ebook"
    assert normalize_edition_format("Audible Audio", "") == "audiobook"
    assert is_edition_write_blocked_row(format_normalized="audiobook", language="English")
    assert is_edition_write_blocked_row(format_normalized="ebook", language="")


def test_confidence_tier_thresholds_remain_explicit() -> None:
    assert confidence_tier(90) == "high"
    assert confidence_tier(75) == "medium"
    assert confidence_tier(74.99) == "low"
