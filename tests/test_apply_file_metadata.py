from __future__ import annotations

import csv
import sqlite3
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

from hardcover_tools.core.apply_engine import run_apply
from hardcover_tools.core.config import ApplyCliConfig

DC_NS = "http://purl.org/dc/elements/1.1/"
OPF_NS = "http://www.idpf.org/2007/opf"


def _write_plan_row(
    *,
    calibre_book_id: int = 1,
    action_type: str = "replace_hardcover_id",
    current_calibre_author: str = "Robin Hobb & Stephen Youll & John Howe",
    current_calibre_title: str = "Royal Assassin (The Illustrated Edition)",
    current_hardcover_author: str = "Robin Hobb",
    current_hardcover_id: str = "999",
    current_hardcover_slug: str = "stale-royal-assassin",
    current_hardcover_edition_id: str = "",
    new_calibre_author: str = "Robin Hobb",
    new_calibre_title: str = "Royal Assassin",
    new_hardcover_id: str = "200",
    new_hardcover_slug: str = "royal-assassin",
    new_hardcover_edition_id: str = "6002",
    safe_to_apply_boolean: str = "true",
    safe_to_apply_reason: str = "fixture marked safe",
) -> dict[str, str]:
    return {
        "action_type": action_type,
        "calibre_book_id": str(calibre_book_id),
        "confidence": "92",
        "current_calibre_author": current_calibre_author,
        "current_calibre_title": current_calibre_title,
        "current_hardcover_author": current_hardcover_author,
        "current_hardcover_edition_id": current_hardcover_edition_id,
        "current_hardcover_id": current_hardcover_id,
        "current_hardcover_slug": current_hardcover_slug,
        "current_hardcover_title": "Royal Assassin",
        "new_calibre_author": new_calibre_author,
        "new_calibre_title": new_calibre_title,
        "new_hardcover_edition_id": new_hardcover_edition_id,
        "new_hardcover_id": new_hardcover_id,
        "new_hardcover_slug": new_hardcover_slug,
        "reason": "fixture row",
        "relink_confidence": "",
        "relink_reason": "",
        "safe_to_apply_boolean": safe_to_apply_boolean,
        "safe_to_apply_reason": safe_to_apply_reason,
        "title": current_calibre_title,
    }


def _write_plan(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _create_metadata_db(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE books (
            id INTEGER PRIMARY KEY,
            title TEXT,
            author_sort TEXT,
            last_modified TEXT,
            path TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE identifiers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            book INTEGER,
            type TEXT,
            val TEXT,
            UNIQUE(book, type)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE authors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            sort TEXT,
            link TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE books_authors_link (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            book INTEGER,
            author INTEGER
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            book INTEGER,
            format TEXT,
            name TEXT
        )
        """
    )
    return connection


def _create_sidecar_opf(path: Path) -> None:
    path.write_text(
        """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf" version="2.0">
  <metadata>
    <dc:title>Royal Assassin (The Illustrated Edition)</dc:title>
    <dc:creator opf:file-as="Hobb, Robin">Robin Hobb</dc:creator>
    <dc:creator opf:file-as="Youll, Stephen">Stephen Youll</dc:creator>
    <dc:creator opf:file-as="Howe, John">John Howe</dc:creator>
    <dc:identifier opf:scheme="isbn">9780000000001</dc:identifier>
    <dc:identifier opf:scheme="hardcover-id">999</dc:identifier>
    <dc:identifier opf:scheme="hardcover-slug">stale-royal-assassin</dc:identifier>
  </metadata>
</package>
""",
        encoding="utf-8",
    )


def _create_epub(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("mimetype", "application/epub+zip")
        archive.writestr(
            "META-INF/container.xml",
            """<?xml version='1.0' encoding='utf-8'?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
""",
        )
        archive.writestr(
            "OEBPS/content.opf",
            """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf" version="2.0">
  <metadata>
    <dc:title>Royal Assassin (The Illustrated Edition)</dc:title>
    <dc:creator opf:file-as="Hobb, Robin">Robin Hobb</dc:creator>
    <dc:creator opf:file-as="Youll, Stephen">Stephen Youll</dc:creator>
    <dc:creator opf:file-as="Howe, John">John Howe</dc:creator>
    <dc:identifier opf:scheme="isbn">9780000000001</dc:identifier>
    <dc:identifier opf:scheme="hardcover-id">999</dc:identifier>
    <dc:identifier opf:scheme="hardcover-slug">stale-royal-assassin</dc:identifier>
  </metadata>
  <manifest>
    <item id="c1" href="chapter1.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="c1"/>
  </spine>
</package>
""",
        )
        archive.writestr("OEBPS/chapter1.xhtml", "<html><body><p>Example</p></body></html>")


def _opf_metadata_from_bytes(content: bytes) -> tuple[str, list[str], dict[str, str]]:
    root = ET.fromstring(content)
    metadata = root.find(f".//{{{OPF_NS}}}metadata")
    assert metadata is not None
    title = metadata.findtext(f"{{{DC_NS}}}title", default="")
    authors = [element.text or "" for element in metadata.findall(f"{{{DC_NS}}}creator")]
    identifiers = {}
    for element in metadata.findall(f"{{{DC_NS}}}identifier"):
        scheme = element.attrib.get(f"{{{OPF_NS}}}scheme") or element.attrib.get("scheme") or element.attrib.get("id") or ""
        if scheme:
            identifiers[str(scheme)] = str(element.text or "")
    return title, authors, identifiers


def _read_sidecar_metadata(path: Path) -> tuple[str, list[str], dict[str, str]]:
    return _opf_metadata_from_bytes(path.read_bytes())


def _read_epub_metadata(path: Path) -> tuple[str, list[str], dict[str, str]]:
    with zipfile.ZipFile(path, "r") as archive:
        content = archive.read("OEBPS/content.opf")
    return _opf_metadata_from_bytes(content)


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


def _create_apply_library(
    tmp_path: Path,
    *,
    create_sidecar: bool = False,
    create_epub: bool = False,
    file_format: str = "EPUB",
) -> tuple[Path, Path, Path, Path, Path]:
    library_root = tmp_path / "library"
    book_dir = library_root / "Robin Hobb" / "Royal Assassin (1)"
    book_dir.mkdir(parents=True, exist_ok=True)

    metadata_db = library_root / "metadata.db"
    connection = _create_metadata_db(metadata_db)
    try:
        connection.execute(
            "INSERT INTO books (id, title, author_sort, last_modified, path) VALUES (?, ?, ?, ?, ?)",
            (
                1,
                "Royal Assassin (The Illustrated Edition)",
                "Hobb, Robin & Youll, Stephen & Howe, John",
                "2026-01-01 00:00:00",
                "Robin Hobb/Royal Assassin (1)",
            ),
        )
        for author_name, author_sort in [
            ("Robin Hobb", "Hobb, Robin"),
            ("Stephen Youll", "Youll, Stephen"),
            ("John Howe", "Howe, John"),
        ]:
            cursor = connection.execute(
                "INSERT INTO authors (name, sort, link) VALUES (?, ?, '')",
                (author_name, author_sort),
            )
            connection.execute(
                "INSERT INTO books_authors_link (book, author) VALUES (?, ?)",
                (1, int(cursor.lastrowid)),
            )
        for identifier_type, value in [
            ("hardcover-id", "999"),
            ("hardcover-slug", "stale-royal-assassin"),
        ]:
            connection.execute(
                "INSERT INTO identifiers (book, type, val) VALUES (?, ?, ?)",
                (1, identifier_type, value),
            )
        if create_epub:
            connection.execute(
                "INSERT INTO data (book, format, name) VALUES (?, ?, ?)",
                (1, "EPUB", "Royal Assassin - Robin Hobb"),
            )
            _create_epub(book_dir / "Royal Assassin - Robin Hobb.epub")
        elif file_format:
            connection.execute(
                "INSERT INTO data (book, format, name) VALUES (?, ?, ?)",
                (1, file_format.upper(), "Royal Assassin - Robin Hobb"),
            )
            (book_dir / f"Royal Assassin - Robin Hobb.{file_format.lower()}").write_text("fixture", encoding="utf-8")
        connection.commit()
    finally:
        connection.close()

    sidecar_path = book_dir / "metadata.opf"
    if create_sidecar:
        _create_sidecar_opf(sidecar_path)
    write_plan = tmp_path / "write_plan.csv"
    output_dir = tmp_path / "output"
    return library_root, metadata_db, write_plan, output_dir, sidecar_path


def _run_apply(
    tmp_path: Path,
    rows: list[dict[str, str]],
    *,
    create_sidecar: bool = False,
    create_epub: bool = False,
    file_format: str = "EPUB",
    dry_run: bool = False,
    include_calibre_title_author: bool = False,
    write_db: bool = True,
    write_sidecar_opf: bool = False,
    write_epub_opf: bool = False,
    prefer_sidecar_opf: bool = True,
) -> tuple[int, Path, Path, Path, Path]:
    library_root, metadata_db, write_plan, output_dir, sidecar_path = _create_apply_library(
        tmp_path,
        create_sidecar=create_sidecar,
        create_epub=create_epub,
        file_format=file_format,
    )
    _write_plan(write_plan, rows)
    exit_code = run_apply(
        ApplyCliConfig(
            library_root=library_root,
            metadata_db=metadata_db,
            output_dir=output_dir,
            write_plan=write_plan,
            limit=None,
            dry_run=dry_run,
            apply_safe_only=True,
            apply_actions=(),
            include_calibre_title_author=include_calibre_title_author,
            include_identifiers_only=not include_calibre_title_author,
            write_db=write_db,
            write_sidecar_opf=write_sidecar_opf,
            write_epub_opf=write_epub_opf,
            prefer_sidecar_opf=prefer_sidecar_opf,
        )
    )
    return exit_code, metadata_db, output_dir, sidecar_path, library_root


def _apply_log_row(output_dir: Path) -> dict[str, str]:
    with (output_dir / "apply" / "apply_log.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    return rows[0]


def test_apply_defaults_to_db_only_even_with_file_targets_present(tmp_path: Path) -> None:
    exit_code, metadata_db, output_dir, sidecar_path, library_root = _run_apply(
        tmp_path,
        [_write_plan_row()],
        create_sidecar=True,
        create_epub=True,
    )

    assert exit_code == 0
    identifiers = _identifier_map(metadata_db, 1)
    assert identifiers["hardcover-id"] == "200"
    assert identifiers["hardcover-slug"] == "royal-assassin"
    assert identifiers["hardcover-edition"] == "6002"

    title, authors, identifier_map = _read_sidecar_metadata(sidecar_path)
    assert title == "Royal Assassin (The Illustrated Edition)"
    assert authors == ["Robin Hobb", "Stephen Youll", "John Howe"]
    assert identifier_map["hardcover-id"] == "999"
    assert identifier_map["hardcover-slug"] == "stale-royal-assassin"

    epub_path = library_root / "Robin Hobb" / "Royal Assassin (1)" / "Royal Assassin - Robin Hobb.epub"
    epub_title, epub_authors, epub_identifier_map = _read_epub_metadata(epub_path)
    assert epub_title == "Royal Assassin (The Illustrated Edition)"
    assert epub_authors == ["Robin Hobb", "Stephen Youll", "John Howe"]
    assert epub_identifier_map["hardcover-id"] == "999"

    log_row = _apply_log_row(output_dir)
    assert log_row["db_write_status"] == "applied"
    assert log_row["file_write_status"] == "not_requested"


def test_apply_dry_run_with_sidecar_file_writes_logs_without_mutating_files(tmp_path: Path) -> None:
    exit_code, metadata_db, output_dir, sidecar_path, _library_root = _run_apply(
        tmp_path,
        [_write_plan_row()],
        create_sidecar=True,
        dry_run=True,
        include_calibre_title_author=True,
        write_sidecar_opf=True,
    )

    assert exit_code == 0
    identifiers = _identifier_map(metadata_db, 1)
    assert identifiers["hardcover-id"] == "999"
    assert "hardcover-edition" not in identifiers

    title, authors, identifier_map = _read_sidecar_metadata(sidecar_path)
    assert title == "Royal Assassin (The Illustrated Edition)"
    assert authors == ["Robin Hobb", "Stephen Youll", "John Howe"]
    assert identifier_map["hardcover-id"] == "999"

    log_row = _apply_log_row(output_dir)
    assert log_row["status"] == "would_apply"
    assert log_row["db_write_status"] == "would_apply"
    assert log_row["file_write_target"] == "sidecar_opf"
    assert log_row["file_write_status"] == "would_apply"


def test_apply_writes_sidecar_opf_and_preserves_unrelated_identifiers(tmp_path: Path) -> None:
    exit_code, metadata_db, output_dir, sidecar_path, _library_root = _run_apply(
        tmp_path,
        [_write_plan_row()],
        create_sidecar=True,
        include_calibre_title_author=True,
        write_sidecar_opf=True,
    )

    assert exit_code == 0
    identifiers = _identifier_map(metadata_db, 1)
    assert identifiers["hardcover-id"] == "200"
    assert identifiers["hardcover-slug"] == "royal-assassin"
    assert identifiers["hardcover-edition"] == "6002"

    title, authors, identifier_map = _read_sidecar_metadata(sidecar_path)
    assert title == "Royal Assassin"
    assert authors == ["Robin Hobb"]
    assert identifier_map["isbn"] == "9780000000001"
    assert identifier_map["hardcover-id"] == "200"
    assert identifier_map["hardcover-slug"] == "royal-assassin"
    assert identifier_map["hardcover-edition"] == "6002"

    log_row = _apply_log_row(output_dir)
    assert log_row["db_write_status"] == "applied"
    assert log_row["file_write_target"] == "sidecar_opf"
    assert log_row["file_write_status"] == "applied"
    assert "hardcover-id" in log_row["file_changed_fields"]
    assert "calibre_title" in log_row["file_changed_fields"]


def test_apply_writes_internal_epub_metadata_when_requested(tmp_path: Path) -> None:
    exit_code, metadata_db, output_dir, _sidecar_path, library_root = _run_apply(
        tmp_path,
        [_write_plan_row()],
        create_epub=True,
        include_calibre_title_author=True,
        write_epub_opf=True,
    )

    assert exit_code == 0
    identifiers = _identifier_map(metadata_db, 1)
    assert identifiers["hardcover-id"] == "200"
    assert identifiers["hardcover-edition"] == "6002"

    epub_path = library_root / "Robin Hobb" / "Royal Assassin (1)" / "Royal Assassin - Robin Hobb.epub"
    title, authors, identifier_map = _read_epub_metadata(epub_path)
    assert title == "Royal Assassin"
    assert authors == ["Robin Hobb"]
    assert identifier_map["isbn"] == "9780000000001"
    assert identifier_map["hardcover-id"] == "200"
    assert identifier_map["hardcover-slug"] == "royal-assassin"
    assert identifier_map["hardcover-edition"] == "6002"

    log_row = _apply_log_row(output_dir)
    assert log_row["file_write_target"] == "epub_opf"
    assert log_row["file_write_status"] == "applied"


def test_apply_prefers_sidecar_opf_by_default_when_multiple_targets_exist(tmp_path: Path) -> None:
    exit_code, _metadata_db, output_dir, sidecar_path, library_root = _run_apply(
        tmp_path,
        [_write_plan_row()],
        create_sidecar=True,
        create_epub=True,
        write_sidecar_opf=True,
        write_epub_opf=True,
    )

    assert exit_code == 0

    sidecar_title, sidecar_authors, sidecar_identifier_map = _read_sidecar_metadata(sidecar_path)
    assert sidecar_title == "Royal Assassin (The Illustrated Edition)"
    assert sidecar_authors == ["Robin Hobb", "Stephen Youll", "John Howe"]
    assert sidecar_identifier_map["hardcover-id"] == "200"
    assert sidecar_identifier_map["hardcover-slug"] == "royal-assassin"
    assert sidecar_identifier_map["hardcover-edition"] == "6002"

    epub_path = library_root / "Robin Hobb" / "Royal Assassin (1)" / "Royal Assassin - Robin Hobb.epub"
    epub_title, epub_authors, epub_identifier_map = _read_epub_metadata(epub_path)
    assert epub_title == "Royal Assassin (The Illustrated Edition)"
    assert epub_authors == ["Robin Hobb", "Stephen Youll", "John Howe"]
    assert epub_identifier_map["hardcover-id"] == "999"
    assert epub_identifier_map["hardcover-slug"] == "stale-royal-assassin"
    assert "hardcover-edition" not in epub_identifier_map

    log_row = _apply_log_row(output_dir)
    assert log_row["file_write_target"] == "sidecar_opf"
    assert log_row["file_write_status"] == "applied"


def test_apply_can_prefer_internal_epub_opf_when_requested(tmp_path: Path) -> None:
    exit_code, _metadata_db, output_dir, sidecar_path, library_root = _run_apply(
        tmp_path,
        [_write_plan_row()],
        create_sidecar=True,
        create_epub=True,
        write_sidecar_opf=True,
        write_epub_opf=True,
        prefer_sidecar_opf=False,
    )

    assert exit_code == 0

    sidecar_title, sidecar_authors, sidecar_identifier_map = _read_sidecar_metadata(sidecar_path)
    assert sidecar_title == "Royal Assassin (The Illustrated Edition)"
    assert sidecar_authors == ["Robin Hobb", "Stephen Youll", "John Howe"]
    assert sidecar_identifier_map["hardcover-id"] == "999"
    assert sidecar_identifier_map["hardcover-slug"] == "stale-royal-assassin"
    assert "hardcover-edition" not in sidecar_identifier_map

    epub_path = library_root / "Robin Hobb" / "Royal Assassin (1)" / "Royal Assassin - Robin Hobb.epub"
    epub_title, epub_authors, epub_identifier_map = _read_epub_metadata(epub_path)
    assert epub_title == "Royal Assassin (The Illustrated Edition)"
    assert epub_authors == ["Robin Hobb", "Stephen Youll", "John Howe"]
    assert epub_identifier_map["hardcover-id"] == "200"
    assert epub_identifier_map["hardcover-slug"] == "royal-assassin"
    assert epub_identifier_map["hardcover-edition"] == "6002"

    log_row = _apply_log_row(output_dir)
    assert log_row["file_write_target"] == "epub_opf"
    assert log_row["file_write_status"] == "applied"


def test_apply_skips_unsupported_file_formats_cleanly(tmp_path: Path) -> None:
    exit_code, metadata_db, output_dir, _sidecar_path, library_root = _run_apply(
        tmp_path,
        [_write_plan_row()],
        file_format="PDF",
        write_db=False,
        write_epub_opf=True,
    )

    assert exit_code == 0
    identifiers = _identifier_map(metadata_db, 1)
    assert identifiers["hardcover-id"] == "999"
    assert "hardcover-edition" not in identifiers
    assert (library_root / "Robin Hobb" / "Royal Assassin (1)" / "Royal Assassin - Robin Hobb.pdf").read_text(encoding="utf-8") == "fixture"

    log_row = _apply_log_row(output_dir)
    assert log_row["status"] == "skipped_file_write"
    assert log_row["db_write_status"] == "skipped_mode"
    assert log_row["file_write_status"] == "skipped_unsupported_format"
    assert "no supported EPUB-family format found" in log_row["file_write_reason"]


def test_apply_mixed_db_and_file_mode_reports_clear_summary_counts(tmp_path: Path) -> None:
    exit_code, _metadata_db, output_dir, _sidecar_path, _library_root = _run_apply(
        tmp_path,
        [_write_plan_row()],
        create_sidecar=True,
        write_sidecar_opf=True,
    )

    assert exit_code == 0
    log_row = _apply_log_row(output_dir)
    assert log_row["db_write_status"] == "applied"
    assert log_row["file_write_target"] == "sidecar_opf"
    assert log_row["file_write_status"] == "applied"

    summary_text = (output_dir / "apply" / "summary.md").read_text(encoding="utf-8")
    assert "- metadata.db writes requested: **yes**" in summary_text
    assert "- Sidecar OPF writes requested: **yes**" in summary_text
    assert "## metadata.db write statuses" in summary_text
    assert "## File write statuses" in summary_text
    assert "## File write targets" in summary_text
