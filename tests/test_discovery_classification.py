import csv
from collections import Counter
from pathlib import Path

from hardcover_tools.core.discovery_engine import annotate_discovery_candidates


FIXTURE_PATH = Path(__file__).resolve().parents[1] / "candidates.csv"


def _load_fixture_rows() -> list[dict[str, str]]:
    with FIXTURE_PATH.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_discovery_fixture_priority_assignments_are_preserved() -> None:
    fixture_rows = _load_fixture_rows()
    annotated = annotate_discovery_candidates(fixture_rows)

    expected = Counter(
        (
            str(row.get("discovery_bucket") or ""),
            str(row.get("display_book_id") or ""),
            str(row.get("series_id") or ""),
            str(row.get("discovery_priority_bucket") or ""),
            str(row.get("shortlist_reason") or ""),
        )
        for row in fixture_rows
    )
    actual = Counter(
        (
            str(row.get("discovery_bucket") or ""),
            str(row.get("display_book_id") or ""),
            str(row.get("series_id") or ""),
            str(row.get("discovery_priority_bucket") or ""),
            str(row.get("shortlist_reason") or ""),
        )
        for row in annotated
    )

    assert actual == expected


def test_blank_language_discovery_split_matches_fixture() -> None:
    fixture_rows = _load_fixture_rows()
    annotated = annotate_discovery_candidates(fixture_rows)

    expected = Counter(
        str(row.get("discovery_priority_bucket") or "")
        for row in fixture_rows
        if not str(row.get("preferred_edition_language") or "").strip()
    )
    actual = Counter(
        str(row.get("discovery_priority_bucket") or "")
        for row in annotated
        if not str(row.get("preferred_edition_language") or "").strip()
    )

    assert actual == expected
