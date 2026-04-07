from __future__ import annotations

from dataclasses import dataclass

from .output import ApplyOutputPaths, AuditOutputPaths, DiscoveryOutputPaths


@dataclass(frozen=True)
class AuditCommandResult:
    outputs: AuditOutputPaths
    row_count: int
    action_row_count: int
    audit_elapsed_seconds: float
    audit_rate_per_second: float
    action_mix: str
    alert_bits: tuple[str, ...]
    existing_edition_count: int
    same_id_write_candidate_count: int
    review_queue_count: int
    hardcover_stats_lines: tuple[str, ...]
    exit_code: int = 0


@dataclass(frozen=True)
class DiscoveryCommandResult:
    outputs: DiscoveryOutputPaths
    row_count: int
    shortlist_count: int
    non_shortlist_count: int
    hardcover_stats_lines: tuple[str, ...]
    bookshelf_queue_count: int = 0
    bookshelf_push_log_count: int = 0
    bookshelf_metadata_backend: str = ""
    exit_code: int = 0


@dataclass(frozen=True)
class ApplyCommandResult:
    outputs: ApplyOutputPaths
    selected_row_count: int
    attempted_row_count: int
    transaction_status: str
    exit_code: int = 0
