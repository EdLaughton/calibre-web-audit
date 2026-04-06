from hardcover_tools.core.discovery_engine import annotate_discovery_candidates


def test_blank_language_likely_english_row_is_promoted_to_shortlist() -> None:
    candidates = [
        {
            "discovery_bucket": "unowned_standalone",
            "display_title": "The Last Kingdom",
            "display_authors": "Bernard Cornwell",
            "authors": "Bernard Cornwell",
            "preferred_edition_format_normalized": "ebook",
            "preferred_edition_language": "",
            "edition_candidates_considered": 2,
            "users_count": 30,
            "users_read_count": 12,
            "gap_kind": "",
            "series_id": "",
        }
    ]

    annotated = annotate_discovery_candidates(candidates)

    assert annotated[0]["eligible_for_shortlist_boolean"] is True
    assert annotated[0]["discovery_priority_bucket"] == "shortlist_blank_language_likely_english"


def test_blank_language_metadata_junk_row_is_suppressed() -> None:
    candidates = [
        {
            "discovery_bucket": "unowned_standalone",
            "display_title": "Articles About The Dark Forest",
            "display_authors": "Books LLC",
            "authors": "Books LLC",
            "owned_author_names": "Books LLC",
            "preferred_edition_format_normalized": "ebook",
            "preferred_edition_language": "",
            "edition_candidates_considered": 3,
            "users_count": 5,
            "users_read_count": 1,
            "gap_kind": "",
            "series_id": "",
        }
    ]

    annotated = annotate_discovery_candidates(candidates)

    assert annotated[0]["eligible_for_shortlist_boolean"] is False
    assert annotated[0]["discovery_priority_bucket"] == "suppressed_blank_language_metadata_junk"


def test_translated_sibling_is_suppressed_when_english_series_sibling_exists() -> None:
    candidates = [
        {
            "discovery_bucket": "missing_series",
            "display_title": "The Dark Tower",
            "display_authors": "Stephen King",
            "authors": "Stephen King",
            "preferred_edition_format_normalized": "ebook",
            "preferred_edition_language": "English",
            "edition_candidates_considered": 2,
            "users_count": 50,
            "users_read_count": 25,
            "gap_kind": "",
            "series_id": "77",
        },
        {
            "discovery_bucket": "missing_series",
            "display_title": "La Torre Oscura",
            "display_authors": "Stephen King",
            "authors": "Stephen King",
            "preferred_edition_format_normalized": "ebook",
            "preferred_edition_language": "",
            "edition_candidates_considered": 2,
            "users_count": 10,
            "users_read_count": 4,
            "gap_kind": "",
            "series_id": "77",
        },
    ]

    annotated = annotate_discovery_candidates(candidates)
    translated = next(row for row in annotated if row["display_title"] == "La Torre Oscura")

    assert translated["eligible_for_shortlist_boolean"] is False
    assert translated["discovery_priority_bucket"] == "suppressed_translated_sibling"
