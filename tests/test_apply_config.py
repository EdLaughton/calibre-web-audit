from __future__ import annotations

import pytest

from hardcover_tools.core.config import parse_apply_args


def test_parse_apply_args_defaults_to_db_only_identifier_updates() -> None:
    config = parse_apply_args(["--library-root", "/tmp/library"])

    assert str(config.library_root) == "/tmp/library"
    assert config.write_db is True
    assert config.write_sidecar_opf is False
    assert config.write_epub_opf is False
    assert config.prefer_sidecar_opf is True
    assert config.include_identifiers_only is True
    assert config.include_calibre_title_author is False


def test_parse_apply_args_expands_write_ebook_metadata_flag() -> None:
    config = parse_apply_args(["--library-root", "/tmp/library", "--write-ebook-metadata"])

    assert config.write_db is True
    assert config.write_sidecar_opf is True
    assert config.write_epub_opf is True
    assert config.prefer_sidecar_opf is True


def test_parse_apply_args_supports_files_only_sidecar_mode() -> None:
    config = parse_apply_args(
        [
            "--library-root",
            "/tmp/library",
            "--files-only",
            "--write-sidecar-opf",
        ]
    )

    assert config.write_db is False
    assert config.write_sidecar_opf is True
    assert config.write_epub_opf is False


def test_parse_apply_args_rejects_files_only_without_file_write_flags() -> None:
    with pytest.raises(SystemExit):
        parse_apply_args(["--library-root", "/tmp/library", "--files-only"])


def test_parse_apply_args_rejects_db_only_with_file_write_flags() -> None:
    with pytest.raises(SystemExit):
        parse_apply_args(["--library-root", "/tmp/library", "--db-only", "--write-sidecar-opf"])


def test_parse_apply_args_rejects_preference_without_file_write_mode() -> None:
    with pytest.raises(SystemExit):
        parse_apply_args(["--library-root", "/tmp/library", "--prefer-internal-epub-opf"])
