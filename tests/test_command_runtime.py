from __future__ import annotations

import sys
from pathlib import Path

import pytest

from hardcover_tools.core.command_runtime import open_command_runtime
from hardcover_tools.core.config import ApplyCliConfig, parse_audit_args
from hardcover_tools.core.runtime_support import HardcoverTokenError


def _apply_config(tmp_path: Path) -> ApplyCliConfig:
    library_root = tmp_path / "library"
    library_root.mkdir()
    metadata_db = library_root / "metadata.db"
    metadata_db.write_text("", encoding="utf-8")
    return ApplyCliConfig(
        library_root=library_root,
        metadata_db=metadata_db,
        output_dir=tmp_path / "output",
        write_plan=None,
        limit=None,
        dry_run=True,
        apply_safe_only=True,
        apply_actions=(),
        include_calibre_title_author=False,
        include_identifiers_only=True,
    )


def test_open_command_runtime_teees_output_restores_streams_and_runs_cleanups(tmp_path: Path) -> None:
    config = _apply_config(tmp_path)
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    cleanup_calls: list[str] = []

    with open_command_runtime(config, command_name="apply") as context:
        context.add_cleanup(lambda: cleanup_calls.append("cleanup"))
        print("runtime hello")

    assert cleanup_calls == ["cleanup"]
    assert sys.stdout is original_stdout
    assert sys.stderr is original_stderr
    assert "runtime hello" in context.runtime_paths.log_path.read_text(encoding="utf-8")


def test_open_command_runtime_requires_valid_hardcover_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = parse_audit_args(["--library-root", str(tmp_path / "library")])
    monkeypatch.delenv("HARDCOVER_TOKEN", raising=False)

    with pytest.raises(HardcoverTokenError):
        with open_command_runtime(config, command_name="audit", require_hardcover_token=True):
            raise AssertionError("unreachable")


def test_command_runtime_registers_hardcover_client_for_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    library_root = tmp_path / "library"
    library_root.mkdir()
    (library_root / "metadata.db").write_text("", encoding="utf-8")
    config = parse_audit_args(["--library-root", str(library_root)])
    monkeypatch.setenv("HARDCOVER_TOKEN", "test-token")

    lifecycle: list[object] = []

    class FakeHardcoverClient:
        def __init__(self, **kwargs) -> None:
            lifecycle.append(kwargs["token"])

        def close(self) -> None:
            lifecycle.append("closed")

    monkeypatch.setattr("hardcover_tools.core.command_runtime.HardcoverClient", FakeHardcoverClient)

    with open_command_runtime(config, command_name="audit", require_hardcover_token=True) as context:
        context.create_hardcover_client(config)

    assert lifecycle == ["test-token", "closed"]
