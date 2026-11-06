"""Generate a Markdown evidence summary from a saved v2 reference JSON.

Usage: python -m enzymeinfocollector.report_cli integrated.json -o report.md
No HTTP requests are performed.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from .evidence_report import build_evidence_report
from .sources import DataSourceError, _atomic_write

MAX_INPUT_BYTES = 32 * 1024 * 1024


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Saved 2.0-reference-preview JSON")
    parser.add_argument("-o", "--output", type=Path,
                        help="Write Markdown atomically; otherwise stdout")
    args = parser.parse_args(argv)
    try:
        if args.input.stat().st_size > MAX_INPUT_BYTES:
            raise DataSourceError("Input JSON exceeds the 32 MB limit")
        data = json.loads(args.input.read_text(encoding="utf-8"))
        output = build_evidence_report(data)
        if args.output:
            _atomic_write(args.output, output.encode("utf-8"))
        else:
            print(output, end="")
    except (DataSourceError, ValueError, TypeError, OSError, UnicodeError) as exc:
        print(f"Evidence report failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
