import csv
import shutil
import sqlite3
from pathlib import Path

from hardcover_tools.core.apply_engine import run_apply
from hardcover_tools.core.config import ApplyCliConfig


FIXTURE_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DB = FIXTURE_ROOT / "metadata.db"
FIXTURE_WRITE_PLAN = FIXTURE_ROOT / "write_plan.csv"


def _fixture_rows_by_book_id() -> dict[str, dict[str, str]]:
    with FIXTURE_WRITE_PLAN.open(newline="", encoding="utf-8") as handle:
        return {row["calibre_book_id"]: row for row in csv.DictReader(handle)}


def _write_plan_subset(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for fieldname in row.keys():
            if fieldname not in fieldnames:
                fieldnames.append(fieldname)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _copy_metadata_db(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    destination = tmp_path / "metadata.db"
    shutil.copyfile(FIXTURE_DB, destination)
    return destination


def _run_apply(
    tmp_path: Path,
    rows: list[dict[str, str]],
    *,
    dry_run: bool = False,
    include_calibre_title_author: bool = False,
    apply_actions: tuple[str, ...] = (),
) -> tuple[int, Path, Path]:
    metadata_db = _copy_metadata_db(tmp_path)
    write_plan = tmp_path / "write_plan.csv"
    output_dir = tmp_path / "output"
    _write_plan_subset(write_plan, rows)
    exit_code = run_apply(
        ApplyCliConfig(
            library_root=tmp_path,
            metadata_db=metadata_db,
            output_dir=output_dir,
            write_plan=write_plan,
            limit=None,
            dry_run=dry_run,
            apply_safe_only=True,
            apply_actions=apply_actions,
            include_calibre_title_author=include_calibre_title_author,
            include_identifiers_only=not include_calibre_title_author,
        )
    )
    return exit_code, metadata_db, output_dir


def _identifier_map(metadata_db: Path, calibre_book_id: int) -> dict[str, str]:
    connection = sqlite3.connect(str(metadata_db))
    try:
        rows = connection.execute(
            "SELECT type, val FROM identifiers WHERE book = ? ORDER BY type",
            (calibre_book_id,),
        ).fetchall()
        return {str(identifier_type): str(value) for identifier_type, value in rows}
    finally:
        connection.close()


def _book_authors(metadata_db: Path, calibre_book_id: int) -> list[str]:
    connection = sqlite3.connect(str(metadata_db))
    try:
        rows = connection.execute(
            """
            SELECT a.name
            FROM books_authors_link bal
            JOIN authors a ON a.id = bal.author
            WHERE bal.book = ?
            ORDER BY bal.id
            """,
            (calibre_book_id,),
        ).fetchall()
        return [str(row[0]) for row in rows]
    finally:
        connection.close()


def _book_title(metadata_db: Path, calibre_book_id: int) -> str:
    connection = sqlite3.connect(str(metadata_db))
    try:
        row = connection.execute(
            "SELECT title FROM books WHERE id = ?",
            (calibre_book_id,),
        ).fetchone()
        assert row is not None
        return str(row[0])
    finally:
        connection.close()


def _apply_log_rows(output_dir: Path) -> list[dict[str, str]]:
    with (output_dir / "apply" / "apply_log.csv").open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_apply_writes_canonical_identifiers_and_removes_legacy_aliases(tmp_path: Path) -> None:
    fixture_rows = _fixture_rows_by_book_id()
    metadata_db = _copy_metadata_db(tmp_path)
    connection = sqlite3.connect(str(metadata_db))
    try:
        connection.execute(
            "INSERT INTO identifiers (book, type, val) VALUES (?, ?, ?)",
            (353, "hardcover-edition-id", "999999"),
        )
        connection.commit()
    finally:
        connection.close()

    write_plan = tmp_path / "write_plan.csv"
    output_dir = tmp_path / "output"
    _write_plan_subset(write_plan, [fixture_rows["353"]])
    exit_code = run_apply(
        ApplyCliConfig(
            library_root=tmp_path,
            metadata_db=metadata_db,
            output_dir=output_dir,
            write_plan=write_plan,
            limit=None,
            dry_run=False,
            apply_safe_only=True,
            apply_actions=(),
            include_calibre_title_author=False,
            include_identifiers_only=True,
        )
    )

    assert exit_code == 0
    identifiers = _identifier_map(metadata_db, 353)
    assert identifiers["hardcover-id"] == "644"
    assert identifiers["hardcover-slug"] == "a-game-of-thrones"
    assert identifiers["hardcover-edition"] == "30404326"
    assert "hardcover-edition-id" not in identifiers
    assert (output_dir / "apply" / "apply_log.csv").exists()
    assert (output_dir / "apply" / "summary.md").exists()


def test_apply_dry_run_rolls_back_database_changes(tmp_path: Path) -> None:
    fixture_rows = _fixture_rows_by_book_id()
    exit_code, metadata_db, output_dir = _run_apply(tmp_path, [fixture_rows["353"]], dry_run=True)

    assert exit_code == 0
    identifiers = _identifier_map(metadata_db, 353)
    assert "hardcover-edition" not in identifiers
    statuses = {row["status"] for row in _apply_log_rows(output_dir)}
    assert "would_apply" in statuses


def test_apply_title_and_author_changes_require_explicit_flag(tmp_path: Path) -> None:
    fixture_rows = _fixture_rows_by_book_id()

    exit_code_without, metadata_db_without, _ = _run_apply(
        tmp_path / "without_title_author",
        [fixture_rows["390"]],
        include_calibre_title_author=False,
    )
    assert exit_code_without == 0
    assert _book_title(metadata_db_without, 390) == "Royal Assassin (The Illustrated Edition)"
    assert _book_authors(metadata_db_without, 390) == ["Robin Hobb", "Stephen Youll", "John Howe"]

    exit_code_with, metadata_db_with, _ = _run_apply(
        tmp_path / "with_title_author",
        [fixture_rows["390"]],
        include_calibre_title_author=True,
    )
    assert exit_code_with == 0
    assert _book_title(metadata_db_with, 390) == "Royal Assassin"
    assert _book_authors(metadata_db_with, 390) == ["Robin Hobb"]


def test_apply_rolls_back_entire_transaction_on_failure(tmp_path: Path) -> None:
    fixture_rows = _fixture_rows_by_book_id()
    failing_row = dict(fixture_rows["353"])
    failing_row["calibre_book_id"] = "999999"

    exit_code, metadata_db, output_dir = _run_apply(
        tmp_path,
        [fixture_rows["353"], failing_row],
        apply_actions=("keep_hardcover_id",),
    )

    assert exit_code == 1
    identifiers = _identifier_map(metadata_db, 353)
    assert "hardcover-edition" not in identifiers
    status_counts = {row["status"] for row in _apply_log_rows(output_dir)}
    assert "rolled_back" in status_counts
    assert "error" in status_counts


def test_replace_without_slug_column_clears_stale_slug(tmp_path: Path) -> None:
    fixture_rows = _fixture_rows_by_book_id()
    exit_code, metadata_db, _ = _run_apply(
        tmp_path,
        [fixture_rows["496"]],
        apply_actions=("replace_hardcover_id",),
    )

    assert exit_code == 0
    identifiers = _identifier_map(metadata_db, 496)
    assert identifiers["hardcover-id"] == "26045"
    assert identifiers["hardcover-edition"] == "29662822"
    assert "hardcover-slug" not in identifiers
