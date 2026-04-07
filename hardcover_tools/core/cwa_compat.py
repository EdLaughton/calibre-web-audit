from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class CwaAppDbSettings:
    library_root: Optional[Path]
    split_library_enabled: bool
    split_library_root: Optional[Path]
    metadata_db_root: Optional[Path]


@dataclass(frozen=True)
class CwaRuntimeHints:
    library_root: Optional[Path]
    metadata_db: Optional[Path]
    source_label: str
    split_library_enabled: bool = False


def _normalize_path(value: object) -> Optional[Path]:
    text = str(value or "").strip()
    if not text:
        return None
    return Path(text).expanduser().resolve()


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def load_cwa_dirs_library_root(path: Path) -> Path:
    candidate = path.expanduser().resolve()
    if not candidate.exists():
        raise FileNotFoundError(f"CWA dirs.json not found at {candidate}")
    try:
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"could not parse CWA dirs.json at {candidate}: {exc}") from exc
    library_root = _normalize_path(payload.get("calibre_library_dir"))
    if library_root is None:
        raise ValueError(f"CWA dirs.json at {candidate} does not contain calibre_library_dir")
    return library_root


def load_cwa_app_db_settings(path: Path) -> CwaAppDbSettings:
    candidate = path.expanduser().resolve()
    if not candidate.exists():
        raise FileNotFoundError(f"CWA app.db not found at {candidate}")
    connection = sqlite3.connect(str(candidate))
    try:
        row = connection.execute(
            """
            SELECT config_calibre_dir, config_calibre_split, config_calibre_split_dir
            FROM settings
            LIMIT 1
            """
        ).fetchone()
    except sqlite3.Error as exc:
        raise ValueError(f"could not read CWA settings from {candidate}: {exc}") from exc
    finally:
        connection.close()
    if row is None:
        raise ValueError(f"CWA app.db at {candidate} does not contain a settings row")

    metadata_db_root = _normalize_path(row[0])
    split_library_enabled = _to_bool(row[1])
    split_library_root = _normalize_path(row[2]) if split_library_enabled else None
    library_root = split_library_root if split_library_enabled else metadata_db_root
    return CwaAppDbSettings(
        library_root=library_root,
        split_library_enabled=split_library_enabled,
        split_library_root=split_library_root,
        metadata_db_root=metadata_db_root,
    )


def resolve_cwa_runtime_hints(
    *,
    app_db: Optional[Path],
    dirs_json: Optional[Path],
) -> Optional[CwaRuntimeHints]:
    if app_db is None and dirs_json is None:
        return None

    app_settings = load_cwa_app_db_settings(app_db) if app_db else None
    dirs_library_root = load_cwa_dirs_library_root(dirs_json) if dirs_json else None

    source_parts: list[str] = []
    if app_db is not None:
        source_parts.append("app_db")
    if dirs_json is not None:
        source_parts.append("dirs_json")
    source_label = "cwa:" + "+".join(source_parts)

    if app_settings and app_settings.split_library_enabled:
        metadata_db = (
            app_settings.metadata_db_root / "metadata.db"
            if app_settings.metadata_db_root is not None
            else None
        )
        return CwaRuntimeHints(
            library_root=app_settings.split_library_root,
            metadata_db=metadata_db,
            source_label=source_label,
            split_library_enabled=True,
        )

    if dirs_library_root is not None:
        return CwaRuntimeHints(
            library_root=dirs_library_root,
            metadata_db=dirs_library_root / "metadata.db",
            source_label=source_label,
            split_library_enabled=False,
        )

    metadata_db = (
        app_settings.metadata_db_root / "metadata.db"
        if app_settings and app_settings.metadata_db_root is not None
        else None
    )
    return CwaRuntimeHints(
        library_root=app_settings.library_root if app_settings else None,
        metadata_db=metadata_db,
        source_label=source_label,
        split_library_enabled=False,
    )


__all__ = [
    "CwaAppDbSettings",
    "CwaRuntimeHints",
    "load_cwa_app_db_settings",
    "load_cwa_dirs_library_root",
    "resolve_cwa_runtime_hints",
]
