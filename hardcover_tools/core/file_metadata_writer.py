from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence
from xml.etree import ElementTree as ET

from .identifiers import (
    CANONICAL_HARDCOVER_IDENTIFIERS,
    HARDCOVER_EDITION,
    HARDCOVER_ID,
    HARDCOVER_SLUG,
    canonicalize_identifier_name,
)
from .text_normalization import split_author_like_string

DC_NS = "http://purl.org/dc/elements/1.1/"
OPF_NS = "http://www.idpf.org/2007/opf"
CONTAINER_NS = {"container": "urn:oasis:names:tc:opendocument:xmlns:container"}
EPUB_WRITE_FORMATS = ("EPUB", "KEPUB", "OEBZIP")

ET.register_namespace("dc", DC_NS)
ET.register_namespace("opf", OPF_NS)


@dataclass(frozen=True)
class FileMetadataPayload:
    title: str
    authors: tuple[str, ...]
    include_title_author: bool
    identifiers: Mapping[str, str]


@dataclass(frozen=True)
class BookFileTargets:
    book_dir: Optional[Path]
    sidecar_opf_path: Optional[Path]
    epub_path: Optional[Path]
    epub_format: str
    available_formats: tuple[str, ...] = ()
    discovery_reason: str = ""


@dataclass(frozen=True)
class SelectedFileTarget:
    target_kind: str
    target_path: Optional[Path]
    status: str
    reason: str


@dataclass(frozen=True)
class FileWriteResult:
    target_kind: str
    target_path: Optional[Path]
    status: str
    reason: str
    changed_fields: tuple[str, ...] = ()


class FileMutationSession:
    def __init__(self) -> None:
        self._backups: Dict[Path, Path] = {}

    def _ensure_backup(self, path: Path) -> None:
        if path in self._backups:
            return
        handle, backup_path = tempfile.mkstemp(prefix="hardcover-tools-backup-", suffix=path.suffix)
        os.close(handle)
        backup = Path(backup_path)
        shutil.copy2(path, backup)
        self._backups[path] = backup

    def replace_file(self, path: Path, content: bytes) -> None:
        self._ensure_backup(path)
        handle, temp_path = tempfile.mkstemp(prefix="hardcover-tools-write-", suffix=path.suffix, dir=str(path.parent))
        os.close(handle)
        temp = Path(temp_path)
        try:
            temp.write_bytes(content)
            os.replace(temp, path)
        finally:
            if temp.exists():
                temp.unlink()

    def replace_epub_member(self, archive_path: Path, member_path: str, content: bytes) -> None:
        self._ensure_backup(archive_path)
        handle, temp_path = tempfile.mkstemp(prefix="hardcover-tools-epub-", suffix=archive_path.suffix, dir=str(archive_path.parent))
        os.close(handle)
        temp_archive = Path(temp_path)
        try:
            with zipfile.ZipFile(archive_path, "r") as source, zipfile.ZipFile(temp_archive, "w") as target:
                for info in source.infolist():
                    payload = content if info.filename == member_path else source.read(info.filename)
                    target.writestr(info, payload)
            os.replace(temp_archive, archive_path)
        finally:
            if temp_archive.exists():
                temp_archive.unlink()

    def restore_all(self) -> None:
        for target, backup in reversed(list(self._backups.items())):
            shutil.copy2(backup, target)

    def cleanup(self) -> None:
        for backup in self._backups.values():
            try:
                backup.unlink()
            except FileNotFoundError:
                pass
        self._backups.clear()


def _local_name(tag: str) -> str:
    return str(tag or "").split("}", 1)[-1]


def _child_elements(element: ET.Element, local_name: str) -> List[ET.Element]:
    return [child for child in list(element) if _local_name(child.tag) == local_name]


def _first_child(element: ET.Element, local_name: str) -> Optional[ET.Element]:
    for child in list(element):
        if _local_name(child.tag) == local_name:
            return child
    return None


def _opf_metadata_element(root: ET.Element) -> ET.Element:
    metadata = _first_child(root, "metadata")
    if metadata is not None:
        return metadata
    return ET.SubElement(root, f"{{{OPF_NS}}}metadata")


def _identifier_key(element: ET.Element) -> str:
    for attr_name in (f"{{{OPF_NS}}}scheme", "scheme", "id"):
        value = str(element.attrib.get(attr_name) or "").strip()
        if not value:
            continue
        canonical = canonicalize_identifier_name(value)
        if canonical in CANONICAL_HARDCOVER_IDENTIFIERS:
            return canonical
    return ""


def _author_sort_fallback(name: str) -> str:
    current = " ".join(str(name or "").strip().split())
    if not current:
        return ""
    if "," in current:
        return current
    parts = current.split(" ")
    if len(parts) == 1:
        return current
    return f"{parts[-1]}, {' '.join(parts[:-1])}"


def _metadata_text(element: Optional[ET.Element]) -> str:
    return " ".join(str(element.text or "").split()) if element is not None else ""


def _update_text_element(
    metadata: ET.Element,
    *,
    local_name: str,
    value: str,
    changed_fields: List[str],
    changed_field_name: str,
) -> None:
    if not value:
        return
    existing = _child_elements(metadata, local_name)
    if existing:
        current = _metadata_text(existing[0])
        if current != value:
            existing[0].text = value
            changed_fields.append(changed_field_name)
        for extra in existing[1:]:
            metadata.remove(extra)
    else:
        element = ET.Element(f"{{{DC_NS}}}{local_name}")
        element.text = value
        metadata.append(element)
        changed_fields.append(changed_field_name)


def _update_creators(metadata: ET.Element, authors: Sequence[str], changed_fields: List[str]) -> None:
    if not authors:
        return
    existing = _child_elements(metadata, "creator")
    current_authors = [_metadata_text(element) for element in existing]
    normalized_authors = list(authors)
    if current_authors == normalized_authors:
        return
    for extra in existing[len(normalized_authors):]:
        metadata.remove(extra)
    for index, author_name in enumerate(normalized_authors):
        if index < len(existing):
            element = existing[index]
        else:
            element = ET.Element(f"{{{DC_NS}}}creator")
            metadata.append(element)
        element.text = author_name
        element.set(f"{{{OPF_NS}}}file-as", _author_sort_fallback(author_name))
    changed_fields.append("calibre_authors")


def _update_identifiers(metadata: ET.Element, payload: FileMetadataPayload, changed_fields: List[str]) -> None:
    identifier_elements = _child_elements(metadata, "identifier")
    by_key: Dict[str, List[ET.Element]] = {}
    for element in identifier_elements:
        key = _identifier_key(element)
        if key:
            by_key.setdefault(key, []).append(element)

    for canonical in (HARDCOVER_ID, HARDCOVER_SLUG, HARDCOVER_EDITION):
        desired = str(payload.identifiers.get(canonical) or "").strip()
        matches = by_key.get(canonical, [])
        current = _metadata_text(matches[0]) if matches else ""
        if desired:
            element = matches[0] if matches else ET.Element(f"{{{DC_NS}}}identifier")
            if current != desired or not matches:
                element.text = desired
                element.set(f"{{{OPF_NS}}}scheme", canonical)
                if not matches:
                    metadata.append(element)
                changed_fields.append(canonical)
            for extra in matches[1:]:
                metadata.remove(extra)
        else:
            if matches:
                for element in matches:
                    metadata.remove(element)
                changed_fields.append(canonical)


def mutate_opf_bytes(original: bytes, payload: FileMetadataPayload) -> tuple[bytes, tuple[str, ...]]:
    root = ET.fromstring(original)
    metadata = _opf_metadata_element(root)
    changed_fields: List[str] = []
    if payload.include_title_author:
        _update_text_element(
            metadata,
            local_name="title",
            value=payload.title,
            changed_fields=changed_fields,
            changed_field_name="calibre_title",
        )
        _update_creators(metadata, payload.authors, changed_fields)
    _update_identifiers(metadata, payload, changed_fields)

    deduped_changes: List[str] = []
    seen: set[str] = set()
    for field in changed_fields:
        if field in seen:
            continue
        seen.add(field)
        deduped_changes.append(field)
    if not deduped_changes:
        return original, ()
    updated = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    return updated, tuple(deduped_changes)


def _read_epub_package_bytes(path: Path) -> tuple[bytes, str]:
    with zipfile.ZipFile(path, "r") as archive:
        names = archive.namelist()
        package_path = ""
        if "META-INF/container.xml" in names:
            container_root = ET.fromstring(archive.read("META-INF/container.xml"))
            rootfile = container_root.find(".//container:rootfile", CONTAINER_NS)
            if rootfile is not None:
                package_path = str(rootfile.attrib.get("full-path") or "")
        if not package_path:
            candidates = [name for name in names if name.lower().endswith(".opf")]
            if len(candidates) == 1:
                package_path = candidates[0]
        if not package_path or package_path not in names:
            raise FileNotFoundError("EPUB package OPF not found")
        return archive.read(package_path), package_path


def resolve_book_file_targets(
    connection: sqlite3.Connection,
    library_root: Path,
    calibre_book_id: int,
) -> BookFileTargets:
    try:
        book_row = connection.execute("SELECT path FROM books WHERE id = ?", (calibre_book_id,)).fetchone()
    except sqlite3.Error:
        return BookFileTargets(None, None, None, "", discovery_reason="books.path is unavailable in metadata.db")
    if book_row is None:
        return BookFileTargets(None, None, None, "", discovery_reason="book path was not found in metadata.db")
    relative = str(book_row["path"] if isinstance(book_row, sqlite3.Row) else book_row[0] or "").strip()
    if not relative:
        return BookFileTargets(None, None, None, "", discovery_reason="book path was empty in metadata.db")
    book_dir = (library_root / relative).resolve()

    sidecar_opf_path: Optional[Path] = None
    metadata_opf = book_dir / "metadata.opf"
    if metadata_opf.exists():
        sidecar_opf_path = metadata_opf
    else:
        opf_candidates = sorted(path for path in book_dir.glob("*.opf") if path.is_file())
        if len(opf_candidates) == 1:
            sidecar_opf_path = opf_candidates[0]

    available_formats: List[str] = []
    epub_path: Optional[Path] = None
    epub_format = ""
    try:
        data_rows = connection.execute(
            "SELECT format, name FROM data WHERE book = ? ORDER BY id",
            (calibre_book_id,),
        ).fetchall()
    except sqlite3.Error:
        data_rows = []
    format_candidates: List[tuple[str, Path]] = []
    for row in data_rows:
        row_format = str(row["format"] if isinstance(row, sqlite3.Row) else row[0] or "").upper().strip()
        row_name = str(row["name"] if isinstance(row, sqlite3.Row) else row[1] or "").strip()
        if not row_format or not row_name:
            continue
        available_formats.append(row_format)
        candidate = book_dir / f"{row_name}.{row_format.lower()}"
        if candidate.exists():
            format_candidates.append((row_format, candidate))
            continue
        for path in sorted(book_dir.glob(f"{row_name}.*")):
            if path.suffix.lower().lstrip(".") == row_format.lower():
                format_candidates.append((row_format, path))
                break
    for supported in EPUB_WRITE_FORMATS:
        for row_format, path in format_candidates:
            if row_format == supported:
                epub_format = row_format
                epub_path = path
                break
        if epub_path is not None:
            break

    return BookFileTargets(
        book_dir=book_dir,
        sidecar_opf_path=sidecar_opf_path,
        epub_path=epub_path,
        epub_format=epub_format,
        available_formats=tuple(available_formats),
        discovery_reason="",
    )


def select_file_write_target(
    targets: BookFileTargets,
    *,
    allow_sidecar_opf: bool,
    allow_epub_opf: bool,
    prefer_sidecar_opf: bool,
) -> SelectedFileTarget:
    choices: List[tuple[str, Path]] = []
    if allow_sidecar_opf and targets.sidecar_opf_path is not None:
        choices.append(("sidecar_opf", targets.sidecar_opf_path))
    if allow_epub_opf and targets.epub_path is not None:
        choices.append(("epub_opf", targets.epub_path))
    if choices:
        if len(choices) == 1:
            kind, path = choices[0]
            return SelectedFileTarget(kind, path, "selected", "selected available target")
        if prefer_sidecar_opf:
            for kind, path in choices:
                if kind == "sidecar_opf":
                    return SelectedFileTarget(kind, path, "selected", "selected preferred sidecar OPF target")
        for kind, path in choices:
            if kind == "epub_opf":
                return SelectedFileTarget(kind, path, "selected", "selected preferred internal EPUB OPF target")

    reasons: List[str] = []
    status = "skipped_no_target"
    if targets.discovery_reason:
        reasons.append(targets.discovery_reason)
    if allow_sidecar_opf and targets.sidecar_opf_path is None:
        reasons.append("sidecar OPF not found")
    if allow_epub_opf:
        if targets.epub_path is None:
            if targets.available_formats:
                unsupported = ", ".join(sorted(set(targets.available_formats)))
                reasons.append(f"no supported EPUB-family format found (available: {unsupported})")
                status = "skipped_unsupported_format"
            else:
                reasons.append("no EPUB-family file recorded in metadata.db")
    return SelectedFileTarget("", None, status, "; ".join(reasons) or "no requested file target available")


def write_sidecar_opf(
    path: Path,
    payload: FileMetadataPayload,
    *,
    dry_run: bool,
    session: FileMutationSession,
) -> FileWriteResult:
    original = path.read_bytes()
    updated, changed_fields = mutate_opf_bytes(original, payload)
    if not changed_fields:
        return FileWriteResult("sidecar_opf", path, "no_changes", "sidecar OPF already matched the selected apply scope")
    if dry_run:
        return FileWriteResult("sidecar_opf", path, "would_apply", "dry-run would update sidecar OPF", changed_fields)
    session.replace_file(path, updated)
    return FileWriteResult("sidecar_opf", path, "applied", "sidecar OPF updated", changed_fields)


def write_epub_opf(
    path: Path,
    payload: FileMetadataPayload,
    *,
    dry_run: bool,
    session: FileMutationSession,
) -> FileWriteResult:
    original_opf, member_path = _read_epub_package_bytes(path)
    updated_opf, changed_fields = mutate_opf_bytes(original_opf, payload)
    if not changed_fields:
        return FileWriteResult("epub_opf", path, "no_changes", "internal EPUB OPF already matched the selected apply scope")
    if dry_run:
        return FileWriteResult("epub_opf", path, "would_apply", "dry-run would update internal EPUB OPF", changed_fields)
    session.replace_epub_member(path, member_path, updated_opf)
    return FileWriteResult("epub_opf", path, "applied", "internal EPUB OPF updated", changed_fields)


def apply_selected_file_metadata(
    selection: SelectedFileTarget,
    payload: FileMetadataPayload,
    *,
    dry_run: bool,
    session: FileMutationSession,
) -> FileWriteResult:
    if selection.status != "selected" or selection.target_path is None:
        return FileWriteResult(selection.target_kind, selection.target_path, selection.status, selection.reason)
    if selection.target_kind == "sidecar_opf":
        return write_sidecar_opf(selection.target_path, payload, dry_run=dry_run, session=session)
    if selection.target_kind == "epub_opf":
        return write_epub_opf(selection.target_path, payload, dry_run=dry_run, session=session)
    return FileWriteResult(selection.target_kind, selection.target_path, "skipped_no_target", selection.reason or "unknown target")


def build_file_metadata_payload(
    *,
    title: str,
    authors: Sequence[str],
    include_title_author: bool,
    hardcover_id: str,
    hardcover_slug: str,
    hardcover_edition_id: str,
) -> FileMetadataPayload:
    return FileMetadataPayload(
        title=title.strip(),
        authors=tuple(split_author_like_string(" & ".join(author for author in authors if author))),
        include_title_author=bool(include_title_author),
        identifiers={
            HARDCOVER_ID: str(hardcover_id or "").strip(),
            HARDCOVER_SLUG: str(hardcover_slug or "").strip(),
            HARDCOVER_EDITION: str(hardcover_edition_id or "").strip(),
        },
    )


__all__ = [
    "BookFileTargets",
    "EPUB_WRITE_FORMATS",
    "FileMetadataPayload",
    "FileMutationSession",
    "FileWriteResult",
    "SelectedFileTarget",
    "apply_selected_file_metadata",
    "build_file_metadata_payload",
    "mutate_opf_bytes",
    "resolve_book_file_targets",
    "select_file_write_target",
]
