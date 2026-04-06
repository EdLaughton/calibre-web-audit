from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence, Tuple

from .legacy_runtime import (
    DEFAULT_CACHE_TTL_HOURS,
    DEFAULT_EDITION_CACHE_TTL_HOURS,
    DEFAULT_EMPTY_CACHE_TTL_HOURS,
    DEFAULT_PROGRESS_EVERY,
    DEFAULT_SEARCH_CACHE_TTL_HOURS,
    HARDCOVER_DEFAULT_USER_AGENT,
)


@dataclass
class RuntimeCliConfig:
    library_root: Path
    metadata_db: Optional[Path]
    output_dir: Optional[Path]
    cache_path: Optional[Path]
    limit: Optional[int]
    verbose: bool
    ebook_meta_command: Optional[str]
    docker_ebook_meta_container: Optional[str]
    container_library_root: str
    author_aliases_json: Optional[Path]
    ebook_meta_host_timeout: int
    ebook_meta_docker_timeout: int
    hardcover_timeout: int
    hardcover_retries: int
    hardcover_user_agent: str
    hardcover_min_interval: float
    cache_ttl_hours: float
    search_cache_ttl_hours: float
    empty_cache_ttl_hours: float
    edition_cache_ttl_hours: float
    progress_every: int
    debug_hardcover: bool

    @classmethod
    def from_namespace(cls, namespace: argparse.Namespace) -> "RuntimeCliConfig":
        return cls(
            library_root=Path(namespace.library_root),
            metadata_db=Path(namespace.metadata_db) if namespace.metadata_db else None,
            output_dir=Path(namespace.output_dir) if namespace.output_dir else None,
            cache_path=Path(namespace.cache_path) if namespace.cache_path else None,
            limit=namespace.limit,
            verbose=bool(namespace.verbose),
            ebook_meta_command=namespace.ebook_meta_command,
            docker_ebook_meta_container=namespace.docker_ebook_meta_container,
            container_library_root=namespace.container_library_root,
            author_aliases_json=Path(namespace.author_aliases_json) if namespace.author_aliases_json else None,
            ebook_meta_host_timeout=int(namespace.ebook_meta_host_timeout),
            ebook_meta_docker_timeout=int(namespace.ebook_meta_docker_timeout),
            hardcover_timeout=int(namespace.hardcover_timeout),
            hardcover_retries=int(namespace.hardcover_retries),
            hardcover_user_agent=str(namespace.hardcover_user_agent),
            hardcover_min_interval=float(namespace.hardcover_min_interval),
            cache_ttl_hours=float(namespace.cache_ttl_hours),
            search_cache_ttl_hours=float(namespace.search_cache_ttl_hours),
            empty_cache_ttl_hours=float(namespace.empty_cache_ttl_hours),
            edition_cache_ttl_hours=float(namespace.edition_cache_ttl_hours),
            progress_every=int(namespace.progress_every),
            debug_hardcover=bool(namespace.debug_hardcover),
        )


AuditCliConfig = RuntimeCliConfig
DiscoveryCliConfig = RuntimeCliConfig


@dataclass
class ApplyCliConfig:
    library_root: Path
    metadata_db: Optional[Path]
    output_dir: Optional[Path]
    write_plan: Optional[Path]
    limit: Optional[int]
    dry_run: bool
    apply_safe_only: bool
    apply_actions: Tuple[str, ...]
    include_calibre_title_author: bool
    include_identifiers_only: bool

    @classmethod
    def from_namespace(cls, namespace: argparse.Namespace) -> "ApplyCliConfig":
        return cls(
            library_root=Path(namespace.library_root),
            metadata_db=Path(namespace.metadata_db) if namespace.metadata_db else None,
            output_dir=Path(namespace.output_dir) if namespace.output_dir else None,
            write_plan=Path(namespace.write_plan) if namespace.write_plan else None,
            limit=namespace.limit,
            dry_run=bool(namespace.dry_run),
            apply_safe_only=bool(namespace.apply_safe_only),
            apply_actions=tuple(
                action.strip()
                for action in str(namespace.apply_actions or "").split(",")
                if action.strip()
            ),
            include_calibre_title_author=bool(namespace.include_calibre_title_author),
            include_identifiers_only=bool(
                namespace.include_identifiers_only or not namespace.include_calibre_title_author
            ),
        )


def _add_shared_runtime_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--library-root",
        type=Path,
        default=Path("."),
        help="Calibre library root containing metadata.db",
    )
    parser.add_argument("--metadata-db", type=Path, default=None)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Root output directory. Defaults to a timestamped folder under --library-root.",
    )
    parser.add_argument(
        "--cache-path",
        type=Path,
        default=None,
        help="Persistent Hardcover cache SQLite path. Defaults to <library-root>/hardcover_cache.sqlite.",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--ebook-meta-command", type=str, default=None)
    parser.add_argument("--docker-ebook-meta-container", type=str, default=None)
    parser.add_argument("--container-library-root", type=str, default="/calibre-library")
    parser.add_argument(
        "--author-aliases-json",
        type=Path,
        default=None,
        help="Optional JSON file mapping author aliases to canonical names",
    )
    parser.add_argument("--ebook-meta-host-timeout", type=int, default=15)
    parser.add_argument("--ebook-meta-docker-timeout", type=int, default=20)
    parser.add_argument("--hardcover-timeout", type=int, default=15)
    parser.add_argument("--hardcover-retries", type=int, default=2)
    parser.add_argument("--hardcover-user-agent", type=str, default=HARDCOVER_DEFAULT_USER_AGENT)
    parser.add_argument("--hardcover-min-interval", type=float, default=1.0)
    parser.add_argument("--cache-ttl-hours", type=float, default=DEFAULT_CACHE_TTL_HOURS)
    parser.add_argument("--search-cache-ttl-hours", type=float, default=DEFAULT_SEARCH_CACHE_TTL_HOURS)
    parser.add_argument("--empty-cache-ttl-hours", type=float, default=DEFAULT_EMPTY_CACHE_TTL_HOURS)
    parser.add_argument("--edition-cache-ttl-hours", type=float, default=DEFAULT_EDITION_CACHE_TTL_HOURS)
    parser.add_argument("--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY)
    parser.add_argument(
        "--debug-hardcover",
        action="store_true",
        help="Emit low-level Hardcover cache and HTTP logs in addition to the compact verbose audit log",
    )


def build_audit_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit a local Calibre library against Hardcover and ebook files",
    )
    _add_shared_runtime_args(parser)
    return parser


def build_discovery_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover missing series entries and books by authors already owned in Calibre",
    )
    _add_shared_runtime_args(parser)
    return parser


def build_apply_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Apply approved Hardcover identifier and optional calibre metadata updates from an audit write plan",
    )
    parser.add_argument(
        "--library-root",
        type=Path,
        default=Path("."),
        help="Calibre library root containing metadata.db",
    )
    parser.add_argument("--metadata-db", type=Path, default=None)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Root output directory. Defaults to a timestamped folder under --library-root.",
    )
    parser.add_argument(
        "--write-plan",
        type=Path,
        default=None,
        help="Path to audit/write_plan.csv. Defaults to audit/write_plan.csv under --library-root, with a root-level write_plan.csv fallback.",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the selected mutations inside a transaction and roll them back after logging",
    )
    parser.add_argument(
        "--apply-safe-only",
        action="store_true",
        default=True,
        help="Restrict apply to rows marked safe_to_apply_boolean=True. This is the default behavior.",
    )
    parser.add_argument(
        "--apply-actions",
        type=str,
        default="",
        help="Comma-separated action types to include. Defaults to all supported safe actions present in the write plan.",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--include-calibre-title-author",
        action="store_true",
        help="Also update calibre title and author fields when the write-plan row requests them",
    )
    mode_group.add_argument(
        "--include-identifiers-only",
        action="store_true",
        help="Apply only Hardcover identifiers. This is the default behavior.",
    )
    return parser


def parse_audit_args(argv: Optional[Sequence[str]] = None) -> AuditCliConfig:
    parser = build_audit_parser()
    namespace = parser.parse_args(argv)
    return AuditCliConfig.from_namespace(namespace)


def parse_discovery_args(argv: Optional[Sequence[str]] = None) -> DiscoveryCliConfig:
    parser = build_discovery_parser()
    namespace = parser.parse_args(argv)
    return DiscoveryCliConfig.from_namespace(namespace)


def parse_apply_args(argv: Optional[Sequence[str]] = None) -> ApplyCliConfig:
    parser = build_apply_parser()
    namespace = parser.parse_args(argv)
    return ApplyCliConfig.from_namespace(namespace)


def build_placeholder_parser(command_name: str, description: str) -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog=f"python -m hardcover_tools.cli.{command_name}",
        description=description,
    )
