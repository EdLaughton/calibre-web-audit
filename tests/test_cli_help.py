from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("module_name", "expected_text"),
    [
        ("hardcover_tools.cli.audit", "Audit a local Calibre library against Hardcover and ebook files"),
        ("hardcover_tools.cli.discovery", "Discover missing series entries and books by authors already owned in Calibre"),
        ("hardcover_tools.cli.apply", "Apply approved Hardcover identifier and optional calibre metadata updates from an audit write plan"),
    ],
)
def test_cli_help_runs_from_repo_checkout(module_name: str, expected_text: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", module_name, "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert expected_text in " ".join(result.stdout.split())
