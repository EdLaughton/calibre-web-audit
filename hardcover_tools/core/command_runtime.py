from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Protocol

from .ebook_meta import EbookMetaRunner
from .hardcover_client import HardcoverClient
from .runtime_io import TeeStream
from .runtime_support import RuntimePaths, resolve_runtime_paths, validate_hardcover_token


class RuntimeConfigLike(Protocol):
    library_root: Path
    metadata_db: Optional[Path]
    output_dir: Optional[Path]
    cwa_app_db: Optional[Path]
    cwa_dirs_json: Optional[Path]
    library_root_explicit: bool
    metadata_db_explicit: bool


class HardcoverRuntimeConfigLike(RuntimeConfigLike, Protocol):
    cache_path: Optional[Path]
    verbose: bool
    ebook_meta_command: Optional[str]
    docker_ebook_meta_container: Optional[str]
    container_library_root: str
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
    debug_hardcover: bool


@dataclass
class CommandRuntimeContext:
    command_name: str
    runtime_paths: RuntimePaths
    hardcover_token: Optional[str] = None
    _cleanup_callbacks: list[Callable[[], Any]] = field(default_factory=list)

    def add_cleanup(self, callback: Callable[[], Any]) -> None:
        self._cleanup_callbacks.append(callback)

    def close_registered_resources(self) -> None:
        while self._cleanup_callbacks:
            callback = self._cleanup_callbacks.pop()
            try:
                callback()
            except Exception:
                pass

    def create_hardcover_client(self, config: HardcoverRuntimeConfigLike) -> HardcoverClient:
        if not self.hardcover_token:
            raise RuntimeError("Hardcover client requested without a validated HARDCOVER_TOKEN")
        client = HardcoverClient(
            token=self.hardcover_token,
            cache_path=self.runtime_paths.cache_path,
            timeout=config.hardcover_timeout,
            retries=config.hardcover_retries,
            user_agent=config.hardcover_user_agent,
            min_interval=config.hardcover_min_interval,
            verbose=config.verbose,
            cache_ttl_hours=config.cache_ttl_hours,
            search_cache_ttl_hours=config.search_cache_ttl_hours,
            empty_cache_ttl_hours=config.empty_cache_ttl_hours,
            edition_cache_ttl_hours=config.edition_cache_ttl_hours,
            legacy_cache_json_path=self.runtime_paths.legacy_cache_json_path,
            debug_hardcover=config.debug_hardcover,
        )
        self.add_cleanup(client.close)
        return client

    def create_ebook_meta_runner(self, config: HardcoverRuntimeConfigLike) -> EbookMetaRunner:
        return EbookMetaRunner(
            library_root=self.runtime_paths.library_root,
            ebook_meta_command=config.ebook_meta_command,
            docker_container_name=config.docker_ebook_meta_container,
            container_library_root=config.container_library_root,
            host_timeout=config.ebook_meta_host_timeout,
            docker_timeout=config.ebook_meta_docker_timeout,
        )


def _cache_path_for_config(config: RuntimeConfigLike) -> Optional[Path]:
    return getattr(config, "cache_path", None)


@contextmanager
def open_command_runtime(
    config: RuntimeConfigLike,
    *,
    command_name: str,
    require_hardcover_token: bool = False,
) -> Iterator[CommandRuntimeContext]:
    hardcover_token = None
    if require_hardcover_token:
        hardcover_token = validate_hardcover_token(os.environ.get("HARDCOVER_TOKEN", ""))

    runtime_paths = resolve_runtime_paths(
        library_root=config.library_root,
        metadata_db=config.metadata_db,
        output_dir=config.output_dir,
        cache_path=_cache_path_for_config(config),
        cwa_app_db=config.cwa_app_db,
        cwa_dirs_json=config.cwa_dirs_json,
        library_root_explicit=config.library_root_explicit,
        metadata_db_explicit=config.metadata_db_explicit,
    )

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    with runtime_paths.log_path.open("w", encoding="utf-8") as log_handle:
        sys.stdout = TeeStream(original_stdout, log_handle)
        sys.stderr = TeeStream(original_stderr, log_handle)
        context = CommandRuntimeContext(
            command_name=command_name,
            runtime_paths=runtime_paths,
            hardcover_token=hardcover_token,
        )
        try:
            yield context
        finally:
            context.close_registered_resources()
            sys.stdout = original_stdout
            sys.stderr = original_stderr
