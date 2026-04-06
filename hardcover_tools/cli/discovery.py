from __future__ import annotations

from typing import Optional, Sequence

from hardcover_tools.core.config import parse_discovery_args
from hardcover_tools.core.discovery_engine import run_discovery


def main(argv: Optional[Sequence[str]] = None) -> int:
    return run_discovery(parse_discovery_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
