"""Experimental evidence-aware Protein↔Rhea JSON export.

Usage:
  python -m enzymeinfocollector.evidence_cli --accession P00330
  python -m enzymeinfocollector.evidence_cli --accession P00330 --output evidence.json

This opt-in command does not alter the existing EC-oriented CLI.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .rhea_directions import fetch_rhea_directions
from .sources import DataSourceError, HttpCache, _atomic_write, default_cache_dir
from .uniprot_catalytic import fetch_uniprot_catalytic_entry


def build_export(
    accessions: tuple[str, ...],
    cache: HttpCache,
    *,
    refresh: bool = False,
) -> dict[str, Any]:
    if not accessions or len(accessions) > 10 or len(set(accessions)) != len(accessions):
        raise ValueError("Select 1–10 unique UniProt accessions")
    directions = fetch_rhea_directions(cache, refresh=refresh)
    entries = [
        fetch_uniprot_catalytic_entry(accession, cache, directions.lookup, refresh=refresh)
        for accession in accessions
    ]
    return {
        "schema_version": "2.0-preview",
        "relationship_semantics": "direct UniProtKB catalytic activity annotation; not EC-derived",
        "unknown_is_not_negative": True,
        "source_attribution": {
            "UniProtKB": "UniProt Consortium, CC BY 4.0, https://www.uniprot.org/",
            "Rhea": "Rhea database, CC BY 4.0, https://www.rhea-db.org/",
        },
        "rhea_directions_snapshot": {
            "url": directions.source_url,
            "sha256": directions.sha256,
            "retrieved_at": directions.retrieved_at,
            "last_modified": directions.last_modified,
        },
        "proteins": [
            {
                "accession": entry.accession,
                "reviewed": entry.reviewed,
                "entry_version": entry.entry_version,
                "sequence_version": entry.sequence_version,
                "source_release": asdict(entry.source_release),
                "catalytic_comments_without_rhea": entry.catalytic_comments_without_rhea,
                "assertions": [
                    {
                        **asdict(assertion),
                        "relation_type": "DIRECT_SOURCE_ANNOTATION",
                        "evidence_interpretation": "ECO and sources as annotated; not necessarily experimental",
                    }
                    for assertion in entry.assertions
                ],
            }
            for entry in entries
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accession", action="append", required=True, help="UniProt accession; repeat up to 10 times")
    parser.add_argument("--cache-dir", type=Path, default=default_cache_dir())
    parser.add_argument("--contact-email", help="Contact email in User-Agent for data providers")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--output", type=Path, help="JSON path; defaults to standard output")
    args = parser.parse_args(argv)
    try:
        result = build_export(
            tuple(args.accession),
            HttpCache(args.cache_dir, contact_email=args.contact_email),
            refresh=args.refresh,
        )
        output = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        if args.output:
            _atomic_write(args.output, output.encode("utf-8"))
        else:
            print(output, end="")
    except (ValueError, DataSourceError, OSError) as error:
        print(f"Evidence export failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
