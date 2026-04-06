from __future__ import annotations

import os
import sys

from .audit_reporting import (
    build_edition_manual_review_queue,
    build_same_id_edition_write_candidates,
)
from .audit_pipeline import audit_books
from .calibre_db import load_calibre_books
from .config import AuditCliConfig
from .ebook_meta import EbookMetaRunner
from .hardcover_client import HardcoverClient
from .identifiers import extract_numeric_id
from .output import build_audit_outputs
from .runtime_io import TeeStream
from .runtime_support import resolve_runtime_paths, validate_hardcover_token
from . import text_normalization
from .text_normalization import load_author_alias_map


def run_audit(config: AuditCliConfig) -> int:
    try:
        token = validate_hardcover_token(os.environ.get("HARDCOVER_TOKEN", ""))
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    text_normalization.AUTHOR_ALIAS_MAP = load_author_alias_map(config.author_aliases_json)
    runtime_paths = resolve_runtime_paths(
        library_root=config.library_root,
        metadata_db=config.metadata_db,
        output_dir=config.output_dir,
        cache_path=config.cache_path,
    )

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    with runtime_paths.log_path.open("w", encoding="utf-8") as log_handle:
        sys.stdout = TeeStream(original_stdout, log_handle)
        sys.stderr = TeeStream(original_stderr, log_handle)
        try:
            print(f"Using library root: {runtime_paths.library_root}")
            print(f"Using metadata DB: {runtime_paths.metadata_db}")
            print(f"Writing outputs to: {runtime_paths.output_dir}")
            print(f"Using cache DB: {runtime_paths.cache_path}")
            if runtime_paths.legacy_cache_json_path.exists():
                print(f"Legacy JSON cache detected: {runtime_paths.legacy_cache_json_path}")
            print(f"Writing log to: {runtime_paths.log_path}")

            records = load_calibre_books(runtime_paths.metadata_db, runtime_paths.library_root)
            print(f"Loaded {len(records)} calibre records")
            if config.verbose:
                state = "on" if config.debug_hardcover else "off"
                print(
                    "Verbose audit logging enabled "
                    f"(progress every {config.progress_every} books; low-level Hardcover debug={state})"
                )

            hc = HardcoverClient(
                token=token,
                cache_path=runtime_paths.cache_path,
                timeout=config.hardcover_timeout,
                retries=config.hardcover_retries,
                user_agent=config.hardcover_user_agent,
                min_interval=config.hardcover_min_interval,
                verbose=config.verbose,
                cache_ttl_hours=config.cache_ttl_hours,
                search_cache_ttl_hours=config.search_cache_ttl_hours,
                empty_cache_ttl_hours=config.empty_cache_ttl_hours,
                edition_cache_ttl_hours=config.edition_cache_ttl_hours,
                legacy_cache_json_path=runtime_paths.legacy_cache_json_path,
                debug_hardcover=config.debug_hardcover,
            )
            ebook_meta_runner = EbookMetaRunner(
                library_root=runtime_paths.library_root,
                ebook_meta_command=config.ebook_meta_command,
                docker_container_name=config.docker_ebook_meta_container,
                container_library_root=config.container_library_root,
                host_timeout=config.ebook_meta_host_timeout,
                docker_timeout=config.ebook_meta_docker_timeout,
            )

            print("Starting main audit pass...")
            rows = audit_books(
                records,
                hardcover_client=hc,
                ebook_meta_runner=ebook_meta_runner,
                limit=config.limit,
                verbose=config.verbose,
                progress_every=config.progress_every,
            )
            output_paths = build_audit_outputs(rows, runtime_paths.output_dir)

            existing_edition_count = sum(
                1 for row in rows if extract_numeric_id(row.current_hardcover_edition_id)
            )
            same_id_write_count = sum(
                1
                for row in build_same_id_edition_write_candidates(rows)
                if row.get("safe_for_current_id_write_pass")
            )
            review_queue_count = len(build_edition_manual_review_queue(rows))
            print(f"Rows with an existing stored hardcover-edition: {existing_edition_count}")
            print(f"Safe same-current-id edition write candidates: {same_id_write_count}")
            print(f"Edition manual review queue size: {review_queue_count}")
            hc.print_stats_summary()
            print("Done.")
            print(f"Audit summary: {output_paths['summary']}")
            print(f"Audit operator sheet: {output_paths['actions_operator']}")
            print(f"Audit actions: {output_paths['actions']}")
            print(f"Audit write plan: {output_paths['write_plan']}")
            return 0
        finally:
            try:
                if "hc" in locals() and hc is not None:
                    hc.close()
            except Exception:
                pass
            sys.stdout = original_stdout
            sys.stderr = original_stderr
