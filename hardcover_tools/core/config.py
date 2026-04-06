from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence, Tuple

from .runtime_defaults import (
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


@dataclass
class DiscoveryCliConfig(RuntimeCliConfig):
    export_bookshelf: bool = False
    push_bookshelf: bool = False
    dry_run: bool = False
    bookshelf_url: Optional[str] = None
    bookshelf_api_key: Optional[str] = None
    bookshelf_root_folder: Optional[str] = None
    bookshelf_quality_profile_id: Optional[int] = None
    bookshelf_metadata_profile_id: Optional[int] = None
    bookshelf_trigger_search: bool = False
    bookshelf_mode: str = "book"
    bookshelf_approval: str = "shortlist-only"

    @classmethod
    def from_namespace(cls, namespace: argparse.Namespace) -> "DiscoveryCliConfig":
        base = RuntimeCliConfig.from_namespace(namespace)
        return cls(
            **base.__dict__,
            export_bookshelf=bool(namespace.export_bookshelf or namespace.push_bookshelf),
            push_bookshelf=bool(namespace.push_bookshelf),
            dry_run=bool(namespace.dry_run),
            bookshelf_url=str(namespace.bookshelf_url).strip() if namespace.bookshelf_url else None,
            bookshelf_api_key=str(namespace.bookshelf_api_key).strip() if namespace.bookshelf_api_key else None,
            bookshelf_root_folder=str(namespace.bookshelf_root_folder).strip()
            if namespace.bookshelf_root_folder
            else None,
            bookshelf_quality_profile_id=(
                int(namespace.bookshelf_quality_profile_id)
                if namespace.bookshelf_quality_profile_id is not None
                else None
            ),
            bookshelf_metadata_profile_id=(
                int(namespace.bookshelf_metadata_profile_id)
                if namespace.bookshelf_metadata_profile_id is not None
                else None
            ),
            bookshelf_trigger_search=bool(namespace.bookshelf_trigger_search),
            bookshelf_mode=str(namespace.bookshelf_mode),
            bookshelf_approval=str(namespace.bookshelf_approval),
        )


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
    write_db: bool = True
    write_sidecar_opf: bool = False
    write_epub_opf: bool = False
    prefer_sidecar_opf: bool = True

    @classmethod
    def from_namespace(cls, namespace: argparse.Namespace) -> "ApplyCliConfig":
        write_sidecar_opf = bool(namespace.write_sidecar_opf or namespace.write_ebook_metadata)
        write_epub_opf = bool(namespace.write_epub_opf or namespace.write_ebook_metadata)
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
            write_db=not bool(namespace.files_only),
            write_sidecar_opf=write_sidecar_opf,
            write_epub_opf=write_epub_opf,
            prefer_sidecar_opf=not bool(namespace.prefer_internal_epub_opf),
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
    parser.add_argument(
        "--export-bookshelf",
        action="store_true",
        help="Write an opt-in Bookshelf queue from discovery candidates without contacting Bookshelf unless --push-bookshelf is also set.",
    )
    parser.add_argument(
        "--push-bookshelf",
        action="store_true",
        help="Look up eligible discovery rows in Bookshelf and add them conservatively. Implies queue export.",
    )
    parser.add_argument(
        "--bookshelf-url",
        type=str,
        default=None,
        help="Base URL for the Bookshelf instance, for example http://bookshelf.local:8787",
    )
    parser.add_argument(
        "--bookshelf-api-key",
        type=str,
        default=None,
        help="Bookshelf API key passed as X-Api-Key for live push operations",
    )
    parser.add_argument(
        "--bookshelf-root-folder",
        type=str,
        default=None,
        help="Root folder path to include on Bookshelf add payloads",
    )
    parser.add_argument(
        "--bookshelf-quality-profile-id",
        type=int,
        default=None,
        help="Bookshelf quality profile ID required for add payloads",
    )
    parser.add_argument(
        "--bookshelf-metadata-profile-id",
        type=int,
        default=None,
        help="Bookshelf metadata profile ID required for add payloads",
    )
    parser.add_argument(
        "--bookshelf-trigger-search",
        action="store_true",
        help="After a successful Bookshelf add, enqueue an explicit search command. Disabled by default.",
    )
    parser.add_argument(
        "--bookshelf-mode",
        choices=("book", "author", "auto"),
        default="book",
        help="Choose whether queued discovery rows should be added as Books, Authors, or auto-resolved. Default: book.",
    )
    parser.add_argument(
        "--bookshelf-approval",
        choices=("shortlist-only", "safe-only", "all-approved"),
        default="shortlist-only",
        help="Control which discovery buckets are eligible for Bookshelf export/push. Default: shortlist-only.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="For Bookshelf export/push, write queue and lookup logs without posting add or search-trigger requests.",
    )
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
        help="Simulate the selected metadata.db and file-write actions, log the would-apply results, and leave the library unchanged",
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
    write_mode_group = parser.add_mutually_exclusive_group()
    write_mode_group.add_argument(
        "--db-only",
        action="store_true",
        help="Explicitly keep apply limited to metadata.db updates. This is the default behavior.",
    )
    write_mode_group.add_argument(
        "--files-only",
        action="store_true",
        help="Skip metadata.db mutation and only attempt the requested file metadata writes.",
    )
    parser.add_argument(
        "--write-ebook-metadata",
        action="store_true",
        help="Enable opt-in file-level metadata writes using the preferred available target among sidecar OPF and internal EPUB OPF.",
    )
    parser.add_argument(
        "--write-sidecar-opf",
        action="store_true",
        help="Allow opt-in writes to a Calibre sidecar metadata.opf or single available .opf file.",
    )
    parser.add_argument(
        "--write-epub-opf",
        action="store_true",
        help="Allow opt-in writes to internal OPF metadata for EPUB-family files (EPUB/KEPUB/OEBZIP).",
    )
    preference_group = parser.add_mutually_exclusive_group()
    preference_group.add_argument(
        "--prefer-sidecar-opf",
        action="store_true",
        help="When multiple file metadata targets are available, prefer the sidecar OPF target. This is the default.",
    )
    preference_group.add_argument(
        "--prefer-internal-epub-opf",
        action="store_true",
        help="When multiple file metadata targets are available, prefer the internal EPUB OPF target.",
    )
    return parser


def parse_audit_args(argv: Optional[Sequence[str]] = None) -> AuditCliConfig:
    parser = build_audit_parser()
    namespace = parser.parse_args(argv)
    return AuditCliConfig.from_namespace(namespace)


def parse_discovery_args(argv: Optional[Sequence[str]] = None) -> DiscoveryCliConfig:
    parser = build_discovery_parser()
    namespace = parser.parse_args(argv)
    if namespace.dry_run and not (namespace.export_bookshelf or namespace.push_bookshelf):
        parser.error("--dry-run requires --export-bookshelf or --push-bookshelf")
    if namespace.push_bookshelf:
        required_flags = (
            ("--bookshelf-url", namespace.bookshelf_url),
            ("--bookshelf-api-key", namespace.bookshelf_api_key),
            ("--bookshelf-root-folder", namespace.bookshelf_root_folder),
            ("--bookshelf-quality-profile-id", namespace.bookshelf_quality_profile_id),
            ("--bookshelf-metadata-profile-id", namespace.bookshelf_metadata_profile_id),
        )
        missing = [flag for flag, value in required_flags if value in (None, "")]
        if missing:
            parser.error("--push-bookshelf requires " + ", ".join(missing))
    return DiscoveryCliConfig.from_namespace(namespace)


def parse_apply_args(argv: Optional[Sequence[str]] = None) -> ApplyCliConfig:
    parser = build_apply_parser()
    namespace = parser.parse_args(argv)
    file_write_requested = bool(
        namespace.write_ebook_metadata or namespace.write_sidecar_opf or namespace.write_epub_opf
    )
    if namespace.db_only and file_write_requested:
        parser.error("--db-only cannot be combined with file metadata write flags")
    if namespace.files_only and not file_write_requested:
        parser.error("--files-only requires at least one file metadata write flag")
    if namespace.prefer_internal_epub_opf and not file_write_requested:
        parser.error("--prefer-internal-epub-opf requires file metadata writing to be enabled")
    if namespace.prefer_sidecar_opf and not file_write_requested:
        parser.error("--prefer-sidecar-opf requires file metadata writing to be enabled")
    return ApplyCliConfig.from_namespace(namespace)


def build_placeholder_parser(command_name: str, description: str) -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog=f"python -m hardcover_tools.cli.{command_name}",
        description=description,
    )
