"""Opt-in evidence export and independent Rhea Swiss-Prot reconciliation.

Use:
  python -m enzymeinfocollector.evidence_cli collect --accession P00330 -o evidence.json
  python -m enzymeinfocollector.evidence_cli validate --accession P00330

Existing v1 CLI and its output formats are unchanged. Validation uses the
official Rhea Swiss-Prot mapping, never a Protein×Reaction EC join.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Iterable, TextIO

from .domain import ReactionDirection
from .evidence_sources import (
    DirectEvidenceCollector,
    DirectEvidenceDataset,
    ONE_WEEK,
    RHEA_DIRECTIONS_URL,
    RheaDirectionsIndex,
    parse_rhea_directions,
)
from .sources import DataSourceError, HttpCache, _atomic_write, default_cache_dir

RHEA_SPROT_URL = "https://ftp.expasy.org/databases/rhea/tsv/rhea2uniprot_sprot.tsv"
_MAPPING_COLUMNS = ["RHEA_ID", "DIRECTION", "MASTER_ID", "ID"]
_DIRECTION_CODES = {
    "UN": ReactionDirection.UNDEFINED,
    "LR": ReactionDirection.LEFT_TO_RIGHT,
    "RL": ReactionDirection.RIGHT_TO_LEFT,
    "BI": ReactionDirection.BIDIRECTIONAL,
}
DEFAULT_ACCESSIONS = ("P00330", "P00331", "P00558", "P00918", "P00533")


def evidence_export(dataset: DirectEvidenceDataset) -> dict[str, Any]:
    """Return a JSON-compatible value with complete per-assertion evidence."""
    return {
        "schema_version": "2.0-preview",
        "relationship_semantics": "direct UniProtKB catalytic activity; never inferred by EC",
        "unknown_is_not_negative": True,
        "sources": {
            "UniProtKB": {
                "release": dataset.uniprot_release,
                "license": "CC-BY-4.0",
                "attribution": "UniProt Consortium",
            },
            "Rhea": {
                "directions_snapshot": asdict(dataset.rhea_directions_snapshot),
                "license": "CC-BY-4.0",
                "attribution": "Rhea database",
            },
        },
        "proteins": [asdict(protein) for protein in dataset.proteins],
    }


def _rhea_id(raw: str | None, context: str) -> str:
    if not isinstance(raw, str) or not raw.isascii() or not raw.isdigit() or int(raw) <= 0:
        raise DataSourceError(f"Invalid {context} in Rhea Swiss-Prot mapping: {raw!r}")
    return "RHEA:" + raw


def parse_sprot_mapping(
    stream: TextIO,
    accessions: set[str],
    directions: RheaDirectionsIndex,
) -> tuple[dict[str, set[str]], int]:
    """Read target rows in a large mapping file without an EC-based join.

    Every RHEA_ID/DIRECTION/MASTER_ID triple must agree with the published
    Rhea directions table. Duplicate identical rows are harmless.
    """
    reader = csv.DictReader(stream, delimiter="\t")
    if reader.fieldnames != _MAPPING_COLUMNS:
        raise DataSourceError("Unexpected Rhea Swiss-Prot TSV header")
    by_accession = {accession: set() for accession in accessions}
    row_count = 0
    for line, row in enumerate(reader, start=2):
        row_count += 1
        if None in row or any(row.get(key) is None for key in _MAPPING_COLUMNS):
            raise DataSourceError(f"Malformed Rhea Swiss-Prot mapping row {line}")
        accession = row["ID"]
        if accession not in accessions:
            continue
        rhea_id = _rhea_id(row["RHEA_ID"], "RHEA_ID")
        master_id = _rhea_id(row["MASTER_ID"], "MASTER_ID")
        direction = _DIRECTION_CODES.get(row["DIRECTION"])
        if direction is None:
            raise DataSourceError(f"Invalid Rhea direction on row {line}")
        variant = directions.lookup(rhea_id)
        if variant.master_id != master_id or variant.direction is not direction:
            raise DataSourceError(f"Rhea direction/master mismatch on mapping row {line}")
        by_accession[accession].add(rhea_id)
    if not row_count:
        raise DataSourceError("Rhea Swiss-Prot mapping is empty")
    return by_accession, row_count


def validate_sprot(
    dataset: DirectEvidenceDataset,
    cache: HttpCache,
    *,
    refresh: bool = False,
) -> dict[str, Any]:
    """Independently compare full master+directional ID sets for reviewed entries.

    This is a live technical consistency check, not evidence of biological
    completeness, true negatives, or equality of EC-classified protein pairs.
    """
    if not dataset.proteins:
        raise ValueError("No proteins to validate")
    if any(not protein.reviewed for protein in dataset.proteins):
        raise ValueError("Swiss-Prot mapping cannot validate unreviewed TrEMBL entries")

    directions_response = cache.get(
        RHEA_DIRECTIONS_URL, ttl_seconds=ONE_WEEK, refresh=refresh
    )
    if directions_response.metadata.get("stale"):
        raise DataSourceError("Refusing stale Rhea directions snapshot")
    digest = hashlib.sha256(directions_response.text.encode("utf-8")).hexdigest()
    if digest != dataset.rhea_directions_snapshot.sha256:
        raise DataSourceError("Rhea directions snapshot changed since UniProt ingestion")
    directions = parse_rhea_directions(
        directions_response.text, dataset.rhea_directions_snapshot
    )

    accessions = {protein.accession for protein in dataset.proteins}
    with cache.open_text(
        RHEA_SPROT_URL, ttl_seconds=ONE_WEEK, refresh=refresh
    ) as (stream, metadata):
        if metadata.get("stale"):
            raise DataSourceError("Refusing stale Rhea Swiss-Prot mapping")
        # Stream the full TSV once. The SHA-256 is of the UTF-8 decoded stream;
        # not an asserted official Rhea release identifier.
        digest_tsv = hashlib.sha256()
        class _HashLines:
            def __iter__(self):
                for line in stream:
                    digest_tsv.update(line.encode("utf-8"))
                    yield line
        mapped, rows = parse_sprot_mapping(_HashLines(), accessions, directions)

    report = []
    for protein in dataset.proteins:
        rest_ids = {item.reaction.rhea_id for item in protein.assertions}
        rhea_ids = mapped[protein.accession]
        report.append({
            "accession": protein.accession,
            "reviewed": protein.reviewed,
            "uniprot_release": protein.source_release.release,
            "entry_version": protein.entry_version,
            "sequence_version": protein.sequence_version,
            "rest_reaction_ids": len(rest_ids),
            "rhea_mapping_ids": len(rhea_ids),
            "only_in_uniprot": sorted(rest_ids - rhea_ids),
            "only_in_rhea_mapping": sorted(rhea_ids - rest_ids),
        })
    return {
        "status": "ok" if all(
            not record["only_in_uniprot"] and not record["only_in_rhea_mapping"]
            for record in report
        ) else "mismatch",
        "comparison": "exact master and directional Rhea IDs, not EC joins",
        "mapping": {
            "source_url": RHEA_SPROT_URL,
            "retrieved_at": metadata.get("fetched_at"),
            "utf8_sha256": digest_tsv.hexdigest(),
            "rows": rows,
        },
        "rhea_directions_sha256": digest,
        "proteins": report,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("collect", "validate"):
        sub = commands.add_parser(name)
        sub.add_argument("--accession", action="append", required=True)
        sub.add_argument("--cache-dir", type=Path, default=default_cache_dir())
        sub.add_argument("--contact-email")
        sub.add_argument("--refresh", action="store_true")
        sub.add_argument("-o", "--output", type=Path, help="Write JSON to file instead of stdout")
    args = parser.parse_args(argv)
    cache = HttpCache(args.cache_dir, contact_email=args.contact_email)
    try:
        dataset = DirectEvidenceCollector(cache).collect(args.accession, refresh=args.refresh)
        result = evidence_export(dataset) if args.command == "collect" else validate_sprot(dataset, cache, refresh=args.refresh)
        text = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        if args.output is not None:
            _atomic_write(args.output, text.encode("utf-8"))
        else:
            print(text, end="")
        return 2 if args.command == "validate" and result["status"] == "mismatch" else 0
    except (DataSourceError, ValueError, OSError) as error:
        print(f"Evidence {args.command} failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
