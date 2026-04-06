from __future__ import annotations

import csv

from hardcover_tools.core.runtime_io import write_csv


def test_write_csv_preserves_first_seen_key_order(tmp_path) -> None:
    path = tmp_path / "sample.csv"
    rows = [
        {"b": "1", "a": "2"},
        {"a": "3", "c": "4"},
    ]

    write_csv(path, rows)

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)

    assert header == ["b", "a", "c"]
