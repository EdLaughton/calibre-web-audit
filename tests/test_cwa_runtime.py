from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from hardcover_tools.core.config import parse_apply_args, parse_audit_args
from hardcover_tools.core.runtime_defaults import CACHE_FILENAME
from hardcover_tools.core.runtime_support import resolve_runtime_paths


def _write_cwa_dirs_json(path: Path, library_root: Path) -> None:
    path.write_text(
        json.dumps({"calibre_library_dir": str(library_root)}, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_cwa_app_db(
    path: Path,
    *,
    calibre_dir: Path,
    split_enabled: bool = False,
    split_dir: Path | None = None,
) -> None:
    connection = sqlite3.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE settings (
                config_calibre_dir TEXT,
                config_calibre_split INTEGER,
                config_calibre_split_dir TEXT
            )
            """
        )
        connection.execute(
            """
            INSERT INTO settings (config_calibre_dir, config_calibre_split, config_calibre_split_dir)
            VALUES (?, ?, ?)
            """,
            (
                str(calibre_dir),
                1 if split_enabled else 0,
                str(split_dir) if split_dir is not None else None,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def test_resolve_runtime_paths_from_cwa_dirs_json_non_split(tmp_path: Path) -> None:
    library_root = tmp_path / "library"
    library_root.mkdir()
    (library_root / "metadata.db").write_text("", encoding="utf-8")
    dirs_json = tmp_path / "dirs.json"
    _write_cwa_dirs_json(dirs_json, library_root)

    runtime_paths = resolve_runtime_paths(
        library_root=Path("."),
        metadata_db=None,
        output_dir=None,
        cache_path=None,
        cwa_app_db=None,
        cwa_dirs_json=dirs_json,
        library_root_explicit=False,
        metadata_db_explicit=False,
    )

    assert runtime_paths.library_root == library_root.resolve()
    assert runtime_paths.metadata_db == (library_root / "metadata.db").resolve()
    assert runtime_paths.output_dir.parent == library_root.resolve()
    assert runtime_paths.cache_path == (library_root / CACHE_FILENAME).resolve()
    assert runtime_paths.resolution_source == "cwa:dirs_json"


def test_resolve_runtime_paths_from_cwa_app_db_non_split(tmp_path: Path) -> None:
    library_root = tmp_path / "library"
    library_root.mkdir()
    (library_root / "metadata.db").write_text("", encoding="utf-8")
    app_db = tmp_path / "app.db"
    _write_cwa_app_db(app_db, calibre_dir=library_root)

    runtime_paths = resolve_runtime_paths(
        library_root=Path("."),
        metadata_db=None,
        output_dir=None,
        cache_path=None,
        cwa_app_db=app_db,
        cwa_dirs_json=None,
        library_root_explicit=False,
        metadata_db_explicit=False,
    )

    assert runtime_paths.library_root == library_root.resolve()
    assert runtime_paths.metadata_db == (library_root / "metadata.db").resolve()
    assert runtime_paths.output_dir.parent == library_root.resolve()
    assert runtime_paths.resolution_source == "cwa:app_db"


def test_resolve_runtime_paths_from_cwa_split_library(tmp_path: Path) -> None:
    db_root = tmp_path / "db-root"
    db_root.mkdir()
    (db_root / "metadata.db").write_text("", encoding="utf-8")
    split_library_root = tmp_path / "split-library"
    split_library_root.mkdir()
    dirs_json_library = tmp_path / "ignored-library"
    dirs_json_library.mkdir()
    dirs_json = tmp_path / "dirs.json"
    app_db = tmp_path / "app.db"
    _write_cwa_dirs_json(dirs_json, dirs_json_library)
    _write_cwa_app_db(
        app_db,
        calibre_dir=db_root,
        split_enabled=True,
        split_dir=split_library_root,
    )

    runtime_paths = resolve_runtime_paths(
        library_root=Path("."),
        metadata_db=None,
        output_dir=None,
        cache_path=None,
        cwa_app_db=app_db,
        cwa_dirs_json=dirs_json,
        library_root_explicit=False,
        metadata_db_explicit=False,
    )

    assert runtime_paths.library_root == split_library_root.resolve()
    assert runtime_paths.metadata_db == (db_root / "metadata.db").resolve()
    assert runtime_paths.output_dir.parent == split_library_root.resolve()
    assert runtime_paths.resolution_source == "cwa:app_db+dirs_json"


def test_explicit_cli_paths_override_cwa_runtime_hints(tmp_path: Path) -> None:
    explicit_library_root = tmp_path / "explicit-library"
    explicit_library_root.mkdir()
    explicit_metadata_db = tmp_path / "explicit-metadata.db"
    explicit_metadata_db.write_text("", encoding="utf-8")
    app_db_library = tmp_path / "cwa-library"
    app_db_library.mkdir()
    (app_db_library / "metadata.db").write_text("", encoding="utf-8")
    app_db = tmp_path / "app.db"
    _write_cwa_app_db(app_db, calibre_dir=app_db_library)

    runtime_paths = resolve_runtime_paths(
        library_root=explicit_library_root,
        metadata_db=explicit_metadata_db,
        output_dir=None,
        cache_path=None,
        cwa_app_db=app_db,
        cwa_dirs_json=None,
        library_root_explicit=True,
        metadata_db_explicit=True,
    )

    assert runtime_paths.library_root == explicit_library_root.resolve()
    assert runtime_paths.metadata_db == explicit_metadata_db.resolve()
    assert runtime_paths.resolution_source == "cli"


def test_parse_audit_args_supports_opt_in_cwa_runtime_flags() -> None:
    config = parse_audit_args(
        [
            "--cwa-app-db",
            "/tmp/cwa/app.db",
            "--cwa-dirs-json",
            "/tmp/cwa/dirs.json",
        ]
    )

    assert config.cwa_app_db == Path("/tmp/cwa/app.db")
    assert config.cwa_dirs_json == Path("/tmp/cwa/dirs.json")
    assert config.library_root_explicit is False
    assert config.metadata_db_explicit is False


def test_parse_apply_args_tracks_explicit_runtime_path_flags() -> None:
    config = parse_apply_args(
        [
            "--library-root",
            "/tmp/library",
            "--metadata-db",
            "/tmp/metadata.db",
            "--cwa-app-db",
            "/tmp/cwa/app.db",
        ]
    )

    assert config.library_root == Path("/tmp/library")
    assert config.metadata_db == Path("/tmp/metadata.db")
    assert config.cwa_app_db == Path("/tmp/cwa/app.db")
    assert config.library_root_explicit is True
    assert config.metadata_db_explicit is True
