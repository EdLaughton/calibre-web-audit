from __future__ import annotations

from typing import Optional, Sequence

from hardcover_tools.core.apply_engine import run_apply
from hardcover_tools.core.config import parse_apply_args


def main(argv: Optional[Sequence[str]] = None) -> int:
    return run_apply(parse_apply_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
