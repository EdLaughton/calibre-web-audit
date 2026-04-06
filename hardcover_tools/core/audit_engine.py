from __future__ import annotations

import os
import sys
import time

from .audit_insights import (
    build_metadata_probe_rollup,
    build_reason_family_rollups,
    format_action_family_counts,
)
from .audit_reporting import (
    build_compact_audit_actions,
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
            print("== Audit setup ==")
            print(f"Library root: {runtime_paths.library_root}")
            print(f"Metadata DB: {runtime_paths.metadata_db}")
            print(f"Output dir: {runtime_paths.output_dir}")
            print(f"Cache DB: {runtime_paths.cache_path}")
            if runtime_paths.legacy_cache_json_path.exists():
                print(f"Legacy JSON cache detected: {runtime_paths.legacy_cache_json_path}")
            print(f"Run log: {runtime_paths.log_path}")

            records = load_calibre_books(runtime_paths.metadata_db, runtime_paths.library_root)
            print(f"Calibre records loaded: {len(records)}")
            if config.limit is not None:
                print(f"Audit row limit: {config.limit}")
            state = "on" if config.debug_hardcover else "off"
            if config.verbose:
                print(
                    "Log mode: verbose per-book decisions with periodic summaries "
                    f"(every {config.progress_every} books; low-level Hardcover debug={state})"
                )
            else:
                print(
                    "Log mode: concise progress summaries only "
                    f"(every {config.progress_every} books; use --verbose for per-book decisions; low-level Hardcover debug={state})"
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

            print("")
            print("== Audit pass ==")
            audit_started_at = time.monotonic()
            rows = audit_books(
                records,
                hardcover_client=hc,
                ebook_meta_runner=ebook_meta_runner,
                limit=config.limit,
                verbose=config.verbose,
                progress_every=config.progress_every,
                show_progress_summary=True,
            )
            audit_elapsed = time.monotonic() - audit_started_at
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

            audit_actions = build_compact_audit_actions(list(rows))
            review_source_counts = {
                label: sum(1 for row in audit_actions if str(row.get("review_source") or "") == label)
                for label in ("duplicate_review", "author_normalisation_review")
            }
            reason_rollups = dict(build_reason_family_rollups(rows, audit_actions))
            metadata_probe_counts, _metadata_probe_samples = build_metadata_probe_rollup(rows)
            action_mix = format_action_family_counts(rows)
            rate = (len(rows) / audit_elapsed) if audit_elapsed > 0 else 0.0

            print("")
            print("== Audit summary ==")
            print(f"Audit pass complete: books={len(rows)} elapsed={audit_elapsed:.1f}s rate={rate:.2f}/s")
            print(f"Decision mix: {action_mix}")
            alert_bits = []
            if reason_rollups.get("Blank-language edition guardrails", 0):
                alert_bits.append(f"blank_lang:{reason_rollups['Blank-language edition guardrails']}")
            if reason_rollups.get("Default-ebook gap guardrails", 0):
                alert_bits.append(f"default_gap:{reason_rollups['Default-ebook gap guardrails']}")
            if reason_rollups.get("Relink-block rows", 0):
                alert_bits.append(f"relink_block:{reason_rollups['Relink-block rows']}")
            if review_source_counts.get("duplicate_review", 0):
                alert_bits.append(f"duplicate_review:{review_source_counts['duplicate_review']}")
            if review_source_counts.get("author_normalisation_review", 0):
                alert_bits.append(f"author_norm_review:{review_source_counts['author_normalisation_review']}")
            if metadata_probe_counts:
                alert_bits.append(f"metadata_probe:{sum(metadata_probe_counts.values())}")
            print(f"Alerts: {', '.join(alert_bits) if alert_bits else 'none flagged'}")
            print(
                "Edition follow-up: "
                f"stored_edition={existing_edition_count} "
                f"same_id_write_candidates={same_id_write_count} "
                f"edition_review_queue={review_queue_count}"
            )
            print(f"Action rows: {len(audit_actions)} | Full-library write-plan rows: {len(rows)}")
            hc.print_stats_summary()
            print("")
            print("== Next review ==")
            print(f"Review first: {output_paths['actions_operator']}")
            print(f"Summary: {output_paths['summary']}")
            print(f"Forensic actions: {output_paths['actions']}")
            print(f"Apply sheet: {output_paths['write_plan']}")
            print(f"Run overview: {output_paths['readme']}")
            return 0
        finally:
            try:
                if "hc" in locals() and hc is not None:
                    hc.close()
            except Exception:
                pass
            sys.stdout = original_stdout
            sys.stderr = original_stderr
