from hardcover_tools.core.text_normalization import (
    clean_title_for_matching,
    normalize_search_query_title,
    title_query_variants,
)


def test_clean_title_for_matching_strips_marketing_and_series_suffixes() -> None:
    assert clean_title_for_matching("A Day of Fallen Night (With Bonus Chapter)") == "A Day of Fallen Night"
    assert clean_title_for_matching("The Olympian Affair (Book 2)") == "The Olympian Affair"
    assert clean_title_for_matching("Dragonfall The Brand New Must-Read Fantasy Debut") == "Dragonfall"


def test_normalize_search_query_title_preserves_under_80_carve_out() -> None:
    assert (
        normalize_search_query_title("Book Title (Collector Annotated Special Library Issue)")
        == "Book Title"
    )
    assert (
        normalize_search_query_title(f"Book Title ({'x' * 81})")
        == f"Book Title ({'x' * 81})"
    )


def test_title_query_variants_include_raw_and_cleaned_without_duplicates() -> None:
    variants = title_query_variants("Darkest Fear: A Novel")

    assert variants[0] == "Darkest Fear: A Novel"
    assert "Darkest Fear" in variants
    assert len(variants) == len(set(variants))
