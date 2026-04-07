from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .cwa_compat import resolve_cwa_runtime_hints
from .runtime_defaults import CACHE_FILENAME, LEGACY_CACHE_FILENAME
from .runtime_io import default_output_dir, ensure_dir, find_metadata_db


class HardcoverTokenError(ValueError):
    """Raised when HARDCOVER_TOKEN is missing or malformed."""


@dataclass(frozen=True)
class RuntimePaths:
    library_root: Path
    metadata_db: Path
    output_dir: Path
    cache_path: Path
    legacy_cache_json_path: Path
    log_path: Path
    resolution_source: str = "cli"


def validate_hardcover_token(raw_value: str) -> str:
    token = raw_value.strip()
    if not token:
        raise HardcoverTokenError("HARDCOVER_TOKEN environment variable is not set")
    if token.startswith("Bearer "):
        raise HardcoverTokenError("HARDCOVER_TOKEN should be the raw token only, without the 'Bearer ' prefix")
    return token


def resolve_runtime_paths(
    *,
    library_root: Path,
    metadata_db: Optional[Path],
    output_dir: Optional[Path],
    cache_path: Optional[Path],
    cwa_app_db: Optional[Path] = None,
    cwa_dirs_json: Optional[Path] = None,
    library_root_explicit: bool = False,
    metadata_db_explicit: bool = False,
) -> RuntimePaths:
    cwa_runtime = resolve_cwa_runtime_hints(app_db=cwa_app_db, dirs_json=cwa_dirs_json)
    resolution_source = "cli"

    resolved_library_root = library_root.resolve()
    if cwa_runtime is not None and not library_root_explicit:
        if cwa_runtime.library_root is None:
            raise ValueError(
                "could not resolve library root from the provided CWA runtime files; "
                "provide --library-root explicitly or fix the CWA app.db/dirs.json paths"
            )
        resolved_library_root = cwa_runtime.library_root.resolve()
        resolution_source = cwa_runtime.source_label

    explicit_metadata = metadata_db.resolve() if metadata_db else None
    if explicit_metadata is not None and metadata_db_explicit:
        resolved_metadata_db = explicit_metadata
    elif cwa_runtime is not None and cwa_runtime.metadata_db is not None:
        resolved_metadata_db = cwa_runtime.metadata_db.resolve()
        if not resolved_metadata_db.exists():
            raise FileNotFoundError(f"metadata.db not found at {resolved_metadata_db}")
        resolution_source = cwa_runtime.source_label
    else:
        resolved_metadata_db = find_metadata_db(resolved_library_root, explicit_metadata)

    resolved_output_dir = output_dir.resolve() if output_dir else default_output_dir(resolved_library_root)
    ensure_dir(resolved_output_dir)
    resolved_cache_path = cache_path.resolve() if cache_path else (resolved_library_root / CACHE_FILENAME)
    ensure_dir(resolved_cache_path.parent)
    legacy_cache_json_path = resolved_library_root / LEGACY_CACHE_FILENAME
    return RuntimePaths(
        library_root=resolved_library_root,
        metadata_db=resolved_metadata_db,
        output_dir=resolved_output_dir,
        cache_path=resolved_cache_path,
        legacy_cache_json_path=legacy_cache_json_path,
        log_path=resolved_output_dir / "run.log",
        resolution_source=resolution_source,
    )
