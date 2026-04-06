from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


class TeeStream:
    def __init__(self, *streams: Any):
        self.streams = streams

    def write(self, data: str) -> int:
        for stream in self.streams:
            stream.write(data)
            stream.flush()
        return len(data)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()


def default_output_dir_name() -> str:
    return f"audit_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def default_output_dir(library_root: Path) -> Path:
    return library_root / default_output_dir_name()


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def find_metadata_db(library_root: Path, explicit_path: Optional[Path]) -> Path:
    if explicit_path:
        return explicit_path
    metadata_db = library_root / "metadata.db"
    if metadata_db.exists():
        return metadata_db
    raise FileNotFoundError(f"metadata.db not found at {metadata_db}")


__all__ = [
    "TeeStream",
    "default_output_dir",
    "default_output_dir_name",
    "ensure_dir",
    "find_metadata_db",
    "write_csv",
    "write_jsonl",
]
