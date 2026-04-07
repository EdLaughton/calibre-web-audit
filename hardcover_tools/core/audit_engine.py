from __future__ import annotations

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
from .command_results import AuditCommandResult
from .command_runtime import CommandRuntimeContext, open_command_runtime
from .config import AuditCliConfig
from .identifiers import extract_numeric_id
from .output import build_audit_outputs
from .runtime_support import HardcoverTokenError
from . import text_normalization
from .text_normalization import load_author_alias_map


def _configure_author_aliases(config: AuditCliConfig) -> None:
    text_normalization.AUTHOR_ALIAS_MAP = load_author_alias_map(config.author_aliases_json)


def _print_audit_setup(context: CommandRuntimeContext, config: AuditCliConfig, record_count: int) -> None:
    runtime_paths = context.runtime_paths
    print("== Audit setup ==")
    print(f"Library root: {runtime_paths.library_root}")
    print(f"Metadata DB: {runtime_paths.metadata_db}")
    if runtime_paths.resolution_source != "cli":
        print(f"Runtime source: {runtime_paths.resolution_source}")
    print(f"Output dir: {runtime_paths.output_dir}")
    print(f"Cache DB: {runtime_paths.cache_path}")
    if runtime_paths.legacy_cache_json_path.exists():
        print(f"Legacy JSON cache detected: {runtime_paths.legacy_cache_json_path}")
    print(f"Run log: {runtime_paths.log_path}")
    print(f"Calibre records loaded: {record_count}")
    if config.limit is not None:
        print(f"Audit row limit: {config.limit}")
    state = "on" if config.debug_hardcover else "off"
    if config.verbose:
        print(
            "Log mode: verbose per-book decisions with periodic summaries "
            f"(every {config.progress_every} books; low-level Hardcover debug={state})"
        )
        return
    print(
        "Log mode: concise progress summaries only "
        f"(every {config.progress_every} books; use --verbose for per-book decisions; low-level Hardcover debug={state})"
    )


def _execute_audit(context: CommandRuntimeContext, config: AuditCliConfig) -> AuditCommandResult:
    records = load_calibre_books(context.runtime_paths.metadata_db, context.runtime_paths.library_root)
    _print_audit_setup(context, config, len(records))

    hardcover_client = context.create_hardcover_client(config)
    ebook_meta_runner = context.create_ebook_meta_runner(config)

    print("")
    print("== Audit pass ==")
    audit_started_at = time.monotonic()
    rows = audit_books(
        records,
        hardcover_client=hardcover_client,
        ebook_meta_runner=ebook_meta_runner,
        limit=config.limit,
        verbose=config.verbose,
        progress_every=config.progress_every,
        show_progress_summary=True,
    )
    audit_elapsed = time.monotonic() - audit_started_at
    outputs = build_audit_outputs(rows, context.runtime_paths.output_dir)

    existing_edition_count = sum(1 for row in rows if extract_numeric_id(row.current_hardcover_edition_id))
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
    alert_bits: list[str] = []
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

    return AuditCommandResult(
        outputs=outputs,
        row_count=len(rows),
        action_row_count=len(audit_actions),
        audit_elapsed_seconds=audit_elapsed,
        audit_rate_per_second=(len(rows) / audit_elapsed) if audit_elapsed > 0 else 0.0,
        action_mix=action_mix,
        alert_bits=tuple(alert_bits),
        existing_edition_count=existing_edition_count,
        same_id_write_candidate_count=same_id_write_count,
        review_queue_count=review_queue_count,
        hardcover_stats_lines=tuple(hardcover_client.stats_summary_lines()),
    )


def _print_audit_result(result: AuditCommandResult) -> None:
    print("")
    print("== Audit summary ==")
    print(
        "Audit pass complete: "
        f"books={result.row_count} "
        f"elapsed={result.audit_elapsed_seconds:.1f}s "
        f"rate={result.audit_rate_per_second:.2f}/s"
    )
    print(f"Decision mix: {result.action_mix}")
    print(f"Alerts: {', '.join(result.alert_bits) if result.alert_bits else 'none flagged'}")
    print(
        "Edition follow-up: "
        f"stored_edition={result.existing_edition_count} "
        f"same_id_write_candidates={result.same_id_write_candidate_count} "
        f"edition_review_queue={result.review_queue_count}"
    )
    print(f"Action rows: {result.action_row_count} | Full-library write-plan rows: {result.row_count}")
    for line in result.hardcover_stats_lines:
        print(line)
    print("")
    print("== Next review ==")
    print(f"Review first: {result.outputs.actions_operator}")
    print(f"Summary: {result.outputs.summary}")
    print(f"Forensic actions: {result.outputs.actions}")
    print(f"Apply sheet: {result.outputs.write_plan}")
    print(f"Run overview: {result.outputs.readme}")


def run_audit(config: AuditCliConfig) -> int:
    try:
        _configure_author_aliases(config)
        with open_command_runtime(config, command_name="audit", require_hardcover_token=True) as context:
            result = _execute_audit(context, config)
            _print_audit_result(result)
            return result.exit_code
    except HardcoverTokenError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
