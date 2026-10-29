"""Rhea-authored KEGG/MetaCyc reaction cross-references (metadata, NOT evidence).

This module never calls KEGG or BioCyc. Rhea mapping tables carry Rhea
directional IDs; preserve those directions rather than flattening to EC or
pretending a cross-reference is a protein–reaction catalytic assertion.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
import hashlib
import io
import re
from typing import Any, Iterable, Mapping

from .domain import ReactionDirection, ReactionVariant, SourceRelease
from .evidence_sources import (
    ONE_WEEK,
    RHEA_DIRECTIONS_URL,
    RheaDirectionsIndex,
    parse_rhea_directions,
)
from .sources import DataSourceError, HttpCache

RHEA_CROSSREF_URLS = {
    "KEGG": "https://ftp.expasy.org/databases/rhea/tsv/rhea2kegg_reaction.tsv",
    "MetaCyc": "https://ftp.expasy.org/databases/rhea/tsv/rhea2metacyc.tsv",
}
_DIRECTION_CODES = {
    "UN": ReactionDirection.UNDEFINED,
    "LR": ReactionDirection.LEFT_TO_RIGHT,
    "RL": ReactionDirection.RIGHT_TO_LEFT,
    "BI": ReactionDirection.BIDIRECTIONAL,
}
_HEADER = ["RHEA_ID", "DIRECTION", "MASTER_ID", "ID"]


@dataclass(frozen=True, slots=True)
class ReactionExternalCrossReference:
    """Rhea's reported mapping; not chemical identity or catalysis evidence."""

    reaction: ReactionVariant
    database: str
    external_id: str
    source_release: SourceRelease


@dataclass(frozen=True, slots=True)
class RheaCrossReferenceDataset:
    rhea_directions_snapshot: SourceRelease
    mapping_snapshots: tuple[SourceRelease, ...]
    crossrefs: tuple[ReactionExternalCrossReference, ...]


def _fetched_at(metadata: Mapping[str, Any], label: str) -> str:
    value = metadata.get("fetched_at")
    if not isinstance(value, str):
        raise DataSourceError(f"Missing fetched_at for {label}")
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.utcoffset() is None:
            raise ValueError("timezone missing")
    except ValueError as error:
        raise DataSourceError(f"Invalid fetched_at for {label}") from error
    return value


def _snapshot(text: str, metadata: Mapping[str, Any], label: str, url: str) -> SourceRelease:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return SourceRelease(
        database="Rhea",
        release=f"{label}-sha256:{digest[:16]}",
        retrieved_at=_fetched_at(metadata, label),
        sha256=digest,
        source_url=url,
    )


def _numeric_rhea(value: str, label: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[1-9][0-9]*", value) is None:
        raise DataSourceError(f"Invalid {label} in Rhea cross-reference table: {value!r}")
    return f"RHEA:{value}"


def parse_rhea_crossrefs(
    text: str,
    database: str,
    source_release: SourceRelease,
    directions: RheaDirectionsIndex,
) -> tuple[ReactionExternalCrossReference, ...]:
    """Parse actual Rhea TSV columns RHEA_ID/DIRECTION/MASTER_ID/ID.

    A mapping can be many-to-many and need not mean identical protonation,
    stereochemistry or directionality between source reaction databases.
    """
    if database not in RHEA_CROSSREF_URLS:
        raise ValueError(f"Unsupported external reaction database: {database!r}")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    if reader.fieldnames != _HEADER:
        raise DataSourceError(f"Unexpected Rhea {database} cross-reference TSV header")
    records: list[ReactionExternalCrossReference] = []
    seen: set[tuple[str, str]] = set()
    for line_number, row in enumerate(reader, 2):
        if None in row or any(row.get(key) is None for key in _HEADER):
            raise DataSourceError(f"Malformed Rhea {database} cross-reference row {line_number}")
        rhea_id = _numeric_rhea(row["RHEA_ID"], "RHEA_ID")
        master_id = _numeric_rhea(row["MASTER_ID"], "MASTER_ID")
        direction = _DIRECTION_CODES.get(row["DIRECTION"])
        if direction is None:
            raise DataSourceError(f"Invalid Rhea {database} direction on row {line_number}")
        variant = directions.lookup(rhea_id)
        if variant.master_id != master_id or variant.direction is not direction:
            raise DataSourceError(
                f"Rhea {database} cross-reference row {line_number} disagrees with directions snapshot"
            )
        external_id = row["ID"]
        if not isinstance(external_id, str) or not external_id or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in external_id
        ):
            raise DataSourceError(f"Invalid Rhea {database} external ID on row {line_number}")
        if database == "KEGG" and re.fullmatch(r"R[0-9]{5}", external_id) is None:
            raise DataSourceError(f"Invalid KEGG reaction ID on row {line_number}")
        key = (rhea_id, external_id)
        if key in seen:
            continue
        seen.add(key)
        records.append(ReactionExternalCrossReference(variant, database, external_id, source_release))
    if not records:
        raise DataSourceError(f"Rhea {database} cross-reference table is empty")
    return tuple(records)


class RheaCrossReferenceCollector:
    """Retrieve only Rhea TSVs; no KEGG or MetaCyc license-dependent calls."""

    def __init__(self, cache: HttpCache):
        self.cache = cache

    def collect(
        self,
        databases: Iterable[str] = ("KEGG", "MetaCyc"),
        *,
        refresh: bool = False,
    ) -> RheaCrossReferenceDataset:
        if isinstance(databases, str):
            raise ValueError("databases must be an iterable, not a string")
        selected = sorted(set(databases))
        if any(name not in RHEA_CROSSREF_URLS for name in selected):
            raise ValueError(f"Unsupported cross-reference database in {selected!r}")
        if not selected:
            raise ValueError("At least one cross-reference database is required")

        directions_response = self.cache.get(
            RHEA_DIRECTIONS_URL, ttl_seconds=ONE_WEEK, refresh=refresh
        )
        if directions_response.metadata.get("stale"):
            raise DataSourceError("Refusing stale Rhea directions cache")
        directions_snapshot = _snapshot(
            directions_response.text,
            directions_response.metadata,
            "directions",
            RHEA_DIRECTIONS_URL,
        )
        directions = parse_rhea_directions(directions_response.text, directions_snapshot)

        snapshots: list[SourceRelease] = []
        crossrefs: list[ReactionExternalCrossReference] = []
        for database in selected:
            url = RHEA_CROSSREF_URLS[database]
            response = self.cache.get(url, ttl_seconds=ONE_WEEK, refresh=refresh)
            if response.metadata.get("stale"):
                raise DataSourceError(f"Refusing stale Rhea {database} cross-reference cache")
            snapshot = _snapshot(
                response.text, response.metadata, f"xrefs-{database.lower()}", url
            )
            snapshots.append(snapshot)
            crossrefs.extend(parse_rhea_crossrefs(response.text, database, snapshot, directions))

        return RheaCrossReferenceDataset(
            rhea_directions_snapshot=directions_snapshot,
            mapping_snapshots=tuple(snapshots),
            crossrefs=tuple(sorted(
                crossrefs,
                key=lambda record: (
                    record.database,
                    int(record.reaction.master_id.split(":")[1]),
                    int(record.reaction.rhea_id.split(":")[1]),
                    record.external_id,
                ),
            )),
        )
