from __future__ import annotations

import re
from pathlib import Path

import hardcover_tools


def _pyproject_text() -> str:
    return (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8")


def test_pyproject_uses_package_version_attr() -> None:
    pyproject = _pyproject_text()

    assert 'dynamic = ["version"]' in pyproject
    assert 'version = {attr = "hardcover_tools.__version__"}' in pyproject
    assert re.fullmatch(r"\d+\.\d+\.\d+", hardcover_tools.__version__)


def test_pyproject_declares_console_scripts() -> None:
    pyproject = _pyproject_text()

    assert 'hardcover-audit = "hardcover_tools.cli.audit:main"' in pyproject
    assert 'hardcover-discovery = "hardcover_tools.cli.discovery:main"' in pyproject
    assert 'hardcover-apply = "hardcover_tools.cli.apply:main"' in pyproject


def test_readme_documents_console_scripts_and_dev_install_quoting() -> None:
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text(encoding="utf-8")

    assert "hardcover-audit --help" in readme
    assert "hardcover-discovery --help" in readme
    assert "hardcover-apply --help" in readme
    assert "python -m pip install -e '.[dev]'" in readme
