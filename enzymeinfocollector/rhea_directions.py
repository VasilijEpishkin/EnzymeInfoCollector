"""Strict, cached Rhea master/directional identifier normalization."""

from __future__ import annotations

import csv
import hashlib
import io
import re
from dataclasses import dataclass
from typing import Any

from .domain import ReactionDirection
from .sources import DataSourceError, HttpCache

RHEA_DIRECTIONS_URL = "https://ftp.expasy.org/databases/rhea/tsv/rhea-directions.tsv"
_COLUMNS = ("RHEA_ID_MASTER", "RHEA_ID_LR", "RHEA_ID_RL", "RHEA_ID_BI")


@dataclass(frozen=True, slots=True)
class RheaDirectionsSnapshot:
    lookup: dict[str, tuple[str, ReactionDirection]]
    sha256: str
    retrieved_at: str
    source_url: str
    last_modified: str | None = None


def _rhea_id(value: str | None, line: int) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]*", value):
        raise DataSourceError(f"Invalid Rhea ID on directions TSV line {line}: {value!r}")
    return "RHEA:" + value


def parse_rhea_directions(tsv: str) -> dict[str, tuple[str, ReactionDirection]]:
    """Do not infer directional IDs arithmetically; use the published TSV."""
    reader = csv.DictReader(io.StringIO(tsv), delimiter="\t")
    if tuple(reader.fieldnames or ()) != _COLUMNS:
        raise DataSourceError("Unexpected Rhea directions TSV header")
    lookup: dict[str, tuple[str, ReactionDirection]] = {}
    for line, row in enumerate(reader, start=2):
        if None in row or any(row.get(column) is None for column in _COLUMNS):
            raise DataSourceError(f"Malformed Rhea directions TSV row {line}")
        master = _rhea_id(row["RHEA_ID_MASTER"], line)
        for column, direction in (
            ("RHEA_ID_MASTER", ReactionDirection.UNDEFINED),
            ("RHEA_ID_LR", ReactionDirection.LEFT_TO_RIGHT),
            ("RHEA_ID_RL", ReactionDirection.RIGHT_TO_LEFT),
            ("RHEA_ID_BI", ReactionDirection.BIDIRECTIONAL),
        ):
            identifier = _rhea_id(row[column], line)
            if identifier in lookup:
                raise DataSourceError(f"Duplicate Rhea direction ID {identifier} on line {line}")
            lookup[identifier] = (master, direction)
    if not lookup:
        raise DataSourceError("Empty Rhea directions TSV")
    return lookup


def fetch_rhea_directions(
    cache: HttpCache, *, refresh: bool = False
) -> RheaDirectionsSnapshot:
    response = cache.get(RHEA_DIRECTIONS_URL, ttl_seconds=24 * 60 * 60, refresh=refresh)
    metadata: dict[str, Any] = response.metadata
    return RheaDirectionsSnapshot(
        lookup=parse_rhea_directions(response.text),
        sha256=metadata.get("body_sha256") or hashlib.sha256(response.text.encode("utf-8")).hexdigest(),
        retrieved_at=metadata["fetched_at"],
        source_url=RHEA_DIRECTIONS_URL,
        last_modified=metadata.get("last_modified"),
    )
