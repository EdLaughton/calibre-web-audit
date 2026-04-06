from __future__ import annotations

from typing import Optional, Sequence

from hardcover_tools.core.audit_engine import run_audit
from hardcover_tools.core.config import parse_audit_args


def main(argv: Optional[Sequence[str]] = None) -> int:
    return run_audit(parse_audit_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
