from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .identifiers import clean_isbn
from .models import BookRecord
from .text_normalization import normalize_author_string

PREFERRED_FORMATS = ["EPUB", "KEPUB", "AZW3", "MOBI", "PDF", "TXT", "DOCX", "HTML", "HTM"]


def preferred_format_key(file_format: str) -> Tuple[int, str]:
    normalized = (file_format or "").upper()
    return (PREFERRED_FORMATS.index(normalized), normalized) if normalized in PREFERRED_FORMATS else (999, normalized)


def choose_primary_file(paths_by_format: Dict[str, str]) -> Tuple[str, str]:
    if not paths_by_format:
        return ("", "")
    file_format = sorted(paths_by_format, key=lambda value: preferred_format_key(value))[0]
    return paths_by_format[file_format], file_format


def load_calibre_books(metadata_db: Path, library_root: Path) -> List[BookRecord]:
    connection = sqlite3.connect(str(metadata_db))
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    books: Dict[int, Dict[str, Any]] = {}

    for row in cursor.execute("SELECT id, title, series_index, path FROM books ORDER BY id"):
        books[row["id"]] = {
            "id": row["id"],
            "title": row["title"] or "",
            "series_index": row["series_index"],
            "path": row["path"] or "",
            "authors": [],
            "series": "",
            "languages": [],
            "identifiers": {},
            "files": {},
        }

    try:
        for row in cursor.execute(
            """
            SELECT bal.book AS book_id, a.name AS author_name
            FROM books_authors_link bal
            JOIN authors a ON a.id = bal.author
            ORDER BY bal.book, bal.id
            """
        ):
            if row["book_id"] in books and row["author_name"]:
                books[row["book_id"]]["authors"].append(row["author_name"])
    except sqlite3.Error:
        pass

    try:
        for row in cursor.execute(
            """
            SELECT bsl.book AS book_id, s.name AS series_name
            FROM books_series_link bsl
            JOIN series s ON s.id = bsl.series
            ORDER BY bsl.book
            """
        ):
            if row["book_id"] in books and row["series_name"] and not books[row["book_id"]]["series"]:
                books[row["book_id"]]["series"] = row["series_name"]
    except sqlite3.Error:
        pass

    try:
        for row in cursor.execute(
            """
            SELECT bll.book AS book_id, l.lang_code AS lang_code
            FROM books_languages_link bll
            JOIN languages l ON l.id = bll.lang_code
            ORDER BY bll.book
            """
        ):
            if row["book_id"] in books and row["lang_code"]:
                books[row["book_id"]]["languages"].append(row["lang_code"])
    except sqlite3.Error:
        pass

    try:
        for row in cursor.execute("SELECT book, type, val FROM identifiers ORDER BY book"):
            if row["book"] in books and row["type"] and row["val"]:
                books[row["book"]]["identifiers"][str(row["type"]).strip().lower()] = str(row["val"]).strip()
    except sqlite3.Error:
        pass

    try:
        for row in cursor.execute("SELECT book, format, name FROM data ORDER BY book"):
            if row["book"] not in books or not row["format"] or not row["name"]:
                continue
            relative = Path(books[row["book"]]["path"]) / f"{row['name']}.{str(row['format']).lower()}"
            books[row["book"]]["files"][str(row["format"]).upper()] = str(library_root / relative)
    except sqlite3.Error:
        pass

    output: List[BookRecord] = []
    for book in books.values():
        file_path, file_format = choose_primary_file(book["files"])
        authors = normalize_author_string(" & ".join([author for author in book["authors"] if author]).strip())
        languages = ",".join(sorted(set([value for value in book["languages"] if value])))
        identifiers = book["identifiers"]
        hardcover_id = identifiers.get("hardcover-id") or identifiers.get("hardcover_id") or identifiers.get("hardcover") or ""
        hardcover_slug = identifiers.get("hardcover-slug") or identifiers.get("hardcover_slug") or ""
        hardcover_edition = identifiers.get("hardcover-edition") or identifiers.get("hardcover_edition") or ""
        isbn_candidates: List[str] = []
        asin_candidates: List[str] = []
        for key, value in identifiers.items():
            cleaned = clean_isbn(value)
            if "isbn" in key and cleaned:
                isbn_candidates.append(cleaned)
            if ("asin" in key or "amazon" in key) and cleaned:
                asin_candidates.append(cleaned)
        output.append(
            BookRecord(
                calibre_book_id=int(book["id"]),
                calibre_title=book["title"],
                calibre_authors=authors,
                calibre_series=book["series"],
                calibre_series_index=float(book["series_index"]) if book["series_index"] is not None else None,
                calibre_language=languages,
                calibre_hardcover_id=hardcover_id,
                calibre_hardcover_slug=hardcover_slug,
                calibre_hardcover_edition_id=hardcover_edition,
                file_path=file_path,
                file_format=file_format,
                all_identifiers=identifiers,
                isbn_candidates=sorted(set(isbn_candidates)),
                asin_candidates=sorted(set(asin_candidates)),
            )
        )

    connection.close()
    return output
