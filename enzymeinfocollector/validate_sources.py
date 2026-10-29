"""Opt-in live validation of UniProt catalytic annotations against Rhea.

Usage: python -m enzymeinfocollector.validate_sources [--refresh]
This command does network I/O. It is NOT part of the offline CI suite.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, TextIO

from .domain import ReactionDirection
from .rhea_directions import fetch_rhea_directions
from .sources import DataSourceError, HttpCache, default_cache_dir
from .uniprot_catalytic import fetch_uniprot_catalytic_entry

RHEA_SPROT_MAPPING_URL = "https://ftp.expasy.org/databases/rhea/tsv/rhea2uniprot_sprot.tsv"
DEFAULT_ACCESSIONS = ("P00330", "P00331", "P04040", "P00734", "P00519")
_MAPPING_COLUMNS = ("RHEA_ID", "DIRECTION", "MASTER_ID", "ID")
_CODES = {
    "UN": ReactionDirection.UNDEFINED,
    "LR": ReactionDirection.LEFT_TO_RIGHT,
    "RL": ReactionDirection.RIGHT_TO_LEFT,
    "BI": ReactionDirection.BIDIRECTIONAL,
}


def parse_sprot_mapping(
    stream: TextIO,
    accessions: set[str],
    direction_lookup: dict[str, tuple[str, ReactionDirection]],
) -> tuple[dict[str, set[str]], int]:
    """Compare by Rhea *master* ID; raw directional IDs are not extra reactions."""
    reader = csv.DictReader(stream, delimiter="\t")
    if tuple(reader.fieldnames or ()) != _MAPPING_COLUMNS:
        raise DataSourceError("Unexpected Rhea Swiss-Prot mapping TSV header")
    by_accession = {accession: set() for accession in accessions}
    row_count = 0
    for line, row in enumerate(reader, start=2):
        row_count += 1
        if None in row or any(row.get(column) is None for column in _MAPPING_COLUMNS):
            raise DataSourceError(f"Malformed Rhea Swiss-Prot mapping row {line}")
        accession = row["ID"]
        if accession not in accessions:
            continue
        identifier = "RHEA:" + row["RHEA_ID"]
        master = "RHEA:" + row["MASTER_ID"]
        mapped = direction_lookup.get(identifier)
        if mapped is None or mapped != (master, _CODES.get(row["DIRECTION"])):
            raise DataSourceError(f"Rhea mapping/direction inconsistency on line {line}")
        by_accession[accession].add(master)
    if row_count == 0:
        raise DataSourceError("Empty Rhea Swiss-Prot mapping")
    return by_accession, row_count


def validate_sample(
    cache: HttpCache,
    accessions: tuple[str, ...] = DEFAULT_ACCESSIONS,
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
    with cache.open_text(RHEA_SPROT_MAPPING_URL, ttl_seconds=24 * 60 * 60, refresh=refresh) as (stream, meta):
        mapped, row_count = parse_sprot_mapping(stream, set(accessions), directions.lookup)
    records = []
    for entry in entries:
        direct_master = {assertion.reaction.master_id for assertion in entry.assertions}
        tsv_master = mapped[entry.accession]
        records.append({
            "accession": entry.accession,
            "reviewed": entry.reviewed,
            "uniprot_release": entry.source_release.release,
            "entry_version": entry.entry_version,
            "sequence_version": entry.sequence_version,
            "uniprot_json_sha256": entry.source_release.sha256,
            "direct_rhea_ids": len({assertion.reaction.rhea_id for assertion in entry.assertions}),
            "direct_master_reactions": len(direct_master),
            "rhea_tsv_master_reactions": len(tsv_master),
            "only_uniprot_master": sorted(direct_master - tsv_master),
            "only_rhea_tsv_master": sorted(tsv_master - direct_master),
            "catalytic_comments_without_rhea": entry.catalytic_comments_without_rhea,
            "eco_attributions": sum(
                evidence.eco_code is not None
                for assertion in entry.assertions for evidence in assertion.evidence
            ),
            "pubmed_attributions": sum(
                bool(evidence.citation_ids)
                for assertion in entry.assertions for evidence in assertion.evidence
            ),
        })
    return {
        "status": "ok" if all(
            not row["only_uniprot_master"] and not row["only_rhea_tsv_master"]
            for row in records
        ) else "mismatch",
        "comparison_basis": "Rhea master ID, not directional Rhea ID",
        "sources": {
            "rhea_directions": {
                "url": directions.source_url,
                "sha256": directions.sha256,
                "retrieved_at": directions.retrieved_at,
                "last_modified": directions.last_modified,
            },
            "rhea_sprot_mapping": {
                "url": RHEA_SPROT_MAPPING_URL,
                "sha256": meta.get("body_sha256"),
                "retrieved_at": meta.get("fetched_at"),
                "last_modified": meta.get("last_modified"),
                "rows": row_count,
            },
        },
        "proteins": records,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accession", action="append", help="UniProt accession; repeat up to 10 times")
    parser.add_argument("--cache-dir", type=Path, default=default_cache_dir())
    parser.add_argument("--refresh", action="store_true", help="Fetch fresh snapshots, ignoring TTL")
    args = parser.parse_args(argv)
    cache = HttpCache(args.cache_dir)
    try:
        report = validate_sample(cache, tuple(args.accession) if args.accession else DEFAULT_ACCESSIONS, refresh=args.refresh)
    except (ValueError, DataSourceError, OSError) as error:
        print(f"Validation failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
