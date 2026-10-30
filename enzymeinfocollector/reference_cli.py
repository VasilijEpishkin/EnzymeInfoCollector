"""Run an opt-in, bounded UniProt→Rhea RDF→ChEBI integration check.

Example:
  python -m enzymeinfocollector.reference_cli --accession P00330 -o result.json

This command fetches live data and is intentionally separate from the
legacy EC-oriented enzymeinfocollector executable and offline CI.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from .reference_dataset import collect_live_reference, reference_json
from .sources import DataSourceError, HttpCache, _atomic_write, default_cache_dir


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accession", action="append", required=True,
                        help="Canonical UniProt accession; repeat for multiple proteins")
    parser.add_argument("--cache-dir", type=Path, default=default_cache_dir())
    parser.add_argument("--contact-email", help="Contact email for upstream User-Agent")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("-o", "--output", type=Path,
                        help="Write complete JSON to a file instead of stdout")
    args = parser.parse_args(argv)
    try:
        cache = HttpCache(args.cache_dir, contact_email=args.contact_email)
        result = collect_live_reference(args.accession, cache, refresh=args.refresh)
        serialized = reference_json(result)
        if args.output is not None:
            _atomic_write(args.output, serialized.encode("utf-8"))
        else:
            print(serialized, end="")
    except (DataSourceError, ValueError, OSError, TypeError) as error:
        print(f"Reference integration failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
