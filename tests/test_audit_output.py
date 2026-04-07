from __future__ import annotations

import csv

from hardcover_tools.core.output import AUDIT_OPERATOR_COLUMNS, build_audit_outputs
from tests.test_audit_reporting import _build_row, _parity_rows


def _read_csv(path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_build_audit_outputs_writes_operator_sheet_and_summary_rollups(tmp_path) -> None:
    probe_row = _build_row(
        5,
        title="Reaper (9780316554916)",
        authors="Will Wight",
        action="update_calibre_metadata",
        confidence_score=90.0,
        confidence_tier="high",
        calibre_hardcover_id="446337",
        suggested_hardcover_id="446337",
        current_hardcover_match_ok="yes",
        preferred_edition_id="30407439",
        preferred_edition_title="Reaper",
        suggested_calibre_title="Reaper",
        suggested_calibre_authors="Will Wight",
        suggested_hardcover_title="Reaper",
        suggested_hardcover_authors="Will Wight",
        suggested_hardcover_slug="reaper",
        calibre_hardcover_slug="reaper",
        reason="Actual ebook file title differs materially from the calibre title",
    )
    probe_row.file_work_title = "Reape"
    probe_row.file_work_authors = "Will Wight"
    probe_row.file_work_title_basis = "embedded"
    probe_row.file_work_authors_basis = "embedded"
    probe_row.ebook_meta_tool_used = "host-ebook-meta"

    rows = _parity_rows() + [probe_row]
    output_paths = build_audit_outputs(rows, tmp_path)

    actions_rows = _read_csv(output_paths.actions)
    operator_rows = _read_csv(output_paths.actions_operator)
    write_plan_rows = _read_csv(output_paths.write_plan)
    summary_text = output_paths.summary.read_text(encoding="utf-8")
    readme_text = output_paths.readme.read_text(encoding="utf-8")

    assert len(operator_rows) == len(actions_rows)
    assert len(write_plan_rows) == len(rows)
    assert list(operator_rows[0].keys()) == AUDIT_OPERATOR_COLUMNS
    assert [row["calibre_book_id"] for row in operator_rows] == [row["calibre_book_id"] for row in actions_rows]

    probe_operator_row = next(row for row in operator_rows if row["calibre_book_id"] == "5")
    assert probe_operator_row["metadata_probe_warning"] == "possible_file_work_title_truncation"
    assert 'file_work_title="Reape"' in probe_operator_row["metadata_probe_details"]

    assert "## Operator rollups" in summary_text
    assert "- Duplicate-review rows: **2**" in summary_text
    assert "- Title metadata cleanup rows: **1**" in summary_text
    assert "## Metadata probe diagnostics" in summary_text
    assert "- possible_file_work_title_truncation: **1**" in summary_text
    assert "sample: calibre_id=5 | Reaper (9780316554916)" in summary_text
    assert "## Operator hints" in summary_text
    assert "`actions_operator.csv`" in summary_text

    assert "actions_operator.csv" in readme_text
    assert "compact triage-first layout" in readme_text
