from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .legacy_runtime import CACHE_FILENAME, LEGACY_CACHE_FILENAME, legacy


@dataclass(frozen=True)
class RuntimePaths:
    library_root: Path
    metadata_db: Path
    output_dir: Path
    cache_path: Path
    legacy_cache_json_path: Path
    log_path: Path


def validate_hardcover_token(raw_value: str) -> str:
    token = raw_value.strip()
    if not token:
        raise ValueError("HARDCOVER_TOKEN environment variable is not set")
    if token.startswith("Bearer "):
        raise ValueError("HARDCOVER_TOKEN should be the raw token only, without the 'Bearer ' prefix")
    return token


def resolve_runtime_paths(
    *,
    library_root: Path,
    metadata_db: Optional[Path],
    output_dir: Optional[Path],
    cache_path: Optional[Path],
) -> RuntimePaths:
    resolved_library_root = library_root.resolve()
    explicit_metadata = metadata_db.resolve() if metadata_db else None
    resolved_metadata_db = legacy.find_metadata_db(resolved_library_root, explicit_metadata)
    resolved_output_dir = output_dir.resolve() if output_dir else legacy.default_output_dir(resolved_library_root)
    legacy.ensure_dir(resolved_output_dir)
    resolved_cache_path = cache_path.resolve() if cache_path else (resolved_library_root / CACHE_FILENAME)
    legacy.ensure_dir(resolved_cache_path.parent)
    legacy_cache_json_path = resolved_library_root / LEGACY_CACHE_FILENAME
    return RuntimePaths(
        library_root=resolved_library_root,
        metadata_db=resolved_metadata_db,
        output_dir=resolved_output_dir,
        cache_path=resolved_cache_path,
        legacy_cache_json_path=legacy_cache_json_path,
        log_path=resolved_output_dir / "run.log",
    )
