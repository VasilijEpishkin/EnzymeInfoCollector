"""Opt-in ChEBI 2.0 compound enrichment for Rhea chemical participants.

ChEBI describes chemical entities, not evidence that a protein catalyzes a
reaction. The REST workflow is a bounded, content-addressed *API snapshot*,
not a pinned ChEBI monthly bulk release. No EC joins are performed.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
import hashlib
import json
import re
from typing import Any, Iterable, Mapping
from urllib.parse import quote

from .domain import SourceRelease
from .sources import DataSourceError, HttpCache

CHEBI_COMPOUND_URL = "https://www.ebi.ac.uk/chebi/backend/api/public/compound/{chebi_id}/?format=json"
CHEBI_TTL_SECONDS = 7 * 24 * 60 * 60
MAX_COMPOUNDS = 100


def normalize_chebi_id(value: str) -> str:
    """Normalize a ChEBI ID (or Rhea's unprefixed numeric ChEBI ID)."""
    if not isinstance(value, str):
        raise ValueError("ChEBI ID must be a string")
    match = re.fullmatch(r"(?:CHEBI:)?([1-9][0-9]*)", value.strip(), re.IGNORECASE)
    if match is None:
        raise ValueError(f"Invalid ChEBI ID: {value!r}")
    return f"CHEBI:{match.group(1)}"


def _normalize_rhea_id(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("Rhea ID must be a string")
    match = re.fullmatch(r"(?:RHEA:)?([1-9][0-9]*)", value.strip(), re.IGNORECASE)
    if match is None:
        raise ValueError(f"Invalid Rhea ID: {value!r}")
    return f"RHEA:{match.group(1)}"


def _timestamp(metadata: Mapping[str, Any]) -> str:
    value = metadata.get("fetched_at")
    if not isinstance(value, str):
        raise DataSourceError("ChEBI response is missing fetched_at provenance")
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.utcoffset() is None:
            raise ValueError("missing timezone")
    except ValueError as error:
        raise DataSourceError("Invalid ChEBI fetched_at timestamp") from error
    return value


def _optional_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise DataSourceError(f"ChEBI {label} must be a string or null")
    return value if value.strip() else None


@dataclass(frozen=True, slots=True)
class CompoundSnapshot:
    """Primary ChEBI chemical identity and response-level provenance."""

    chebi_id: str
    name: str
    formula: str | None
    charge: int | None
    smiles: str | None
    inchi: str | None
    inchikey: str | None
    secondary_ids: tuple[str, ...]
    modified_on: str | None
    source_release: SourceRelease


@dataclass(frozen=True, slots=True)
class ChEBIResolution:
    """An ID explicitly confirmed as primary or secondary by ChEBI itself."""

    requested_id: str
    primary_id: str


@dataclass(frozen=True, slots=True)
class RheaCompoundLink:
    """Membership only: the v1 Rhea query does NOT provide side/stoichiometry."""

    rhea_id: str
    reported_chebi_id: str
    primary_chebi_id: str


@dataclass(frozen=True, slots=True)
class ChEBICollection:
    compounds: tuple[CompoundSnapshot, ...]
    resolutions: tuple[ChEBIResolution, ...]
    rhea_links: tuple[RheaCompoundLink, ...] = ()


def parse_chebi_compound(
    payload: Mapping[str, Any],
    requested_id: str,
    source_release: SourceRelease,
) -> CompoundSnapshot:
    """Parse the ChEBI 2.0 public compound JSON; reject mismatched IDs.

    The API's 'chemical_data' and 'default_structure' fields are optional;
    absence must never be replaced by a guessed SMILES or charge.
    """
    requested_id = normalize_chebi_id(requested_id)
    if not isinstance(payload, dict):
        raise DataSourceError("ChEBI compound response must be a JSON object")
    try:
        primary_id = normalize_chebi_id(payload.get("chebi_accession"))
    except ValueError as error:
        raise DataSourceError("ChEBI compound has an invalid primary accession") from error
    numeric_id = payload.get("id")
    if numeric_id is not None and (
        type(numeric_id) is not int or numeric_id != int(primary_id.split(":")[1])
    ):
        raise DataSourceError("ChEBI numeric ID disagrees with primary accession")
    name = payload.get("name")
    if not isinstance(name, str) or not name.strip():
        raise DataSourceError("ChEBI compound is missing a name")

    secondary_raw = payload.get("secondary_ids", [])
    if not isinstance(secondary_raw, list):
        raise DataSourceError("ChEBI secondary_ids must be a list")
    try:
        secondary_ids = tuple(sorted(
            {normalize_chebi_id(value) for value in secondary_raw},
            key=lambda value: int(value.split(":")[1]),
        ))
    except ValueError as error:
        raise DataSourceError("ChEBI secondary_ids contains an invalid ID") from error
    if primary_id in secondary_ids:
        raise DataSourceError("ChEBI primary ID is also listed as secondary")
    if requested_id != primary_id and requested_id not in secondary_ids:
        raise DataSourceError(
            f"ChEBI returned {primary_id} for {requested_id} without confirming it as a secondary ID"
        )

    chemical = payload.get("chemical_data")
    if chemical is None:
        chemical = {}
    if not isinstance(chemical, dict):
        raise DataSourceError("ChEBI chemical_data must be an object or null")
    structure = payload.get("default_structure")
    if structure is None:
        structure = {}
    if not isinstance(structure, dict):
        raise DataSourceError("ChEBI default_structure must be an object or null")
    charge = chemical.get("charge")
    if charge is not None and type(charge) is not int:
        raise DataSourceError("ChEBI charge must be an integer or null")
    modified_on = _optional_text(payload.get("modified_on"), "modified_on")
    if modified_on is not None:
        try:
            modified_stamp = datetime.fromisoformat(modified_on.replace("Z", "+00:00"))
            if modified_stamp.utcoffset() is None:
                raise ValueError("missing timezone")
        except ValueError as error:
            raise DataSourceError("ChEBI modified_on must include a timezone") from error

    return CompoundSnapshot(
        chebi_id=primary_id,
        name=name.strip(),
        formula=_optional_text(chemical.get("formula"), "formula"),
        charge=charge,
        smiles=_optional_text(structure.get("smiles"), "smiles"),
        inchi=_optional_text(structure.get("standard_inchi"), "standard_inchi"),
        inchikey=_optional_text(structure.get("standard_inchi_key"), "standard_inchi_key"),
        secondary_ids=secondary_ids,
        modified_on=modified_on,
        source_release=source_release,
    )


def _same_chemical_record(left: CompoundSnapshot, right: CompoundSnapshot) -> bool:
    """Compare fields, not per-HTTP-response timestamps or hashes."""
    return replace(left, source_release=right.source_release) == right


class ChEBICollector:
    """Bounded ChEBI REST lookups via the existing cache (opt-in, no new deps)."""

    def __init__(self, cache: HttpCache):
        self.cache = cache

    def collect(self, chebi_ids: Iterable[str], *, refresh: bool = False) -> ChEBICollection:
        if isinstance(chebi_ids, str):
            raise ValueError("chebi_ids must be an iterable, not a string")
        requested = sorted(
            {normalize_chebi_id(value) for value in chebi_ids},
            key=lambda value: int(value.split(":")[1]),
        )
        if len(requested) > MAX_COMPOUNDS:
            raise ValueError(f"A ChEBI REST batch may contain at most {MAX_COMPOUNDS} unique IDs")
        by_primary: dict[str, CompoundSnapshot] = {}
        resolutions: list[ChEBIResolution] = []
        for chebi_id in requested:
            url = CHEBI_COMPOUND_URL.format(chebi_id=quote(chebi_id, safe=":"))
            response = self.cache.get(url, ttl_seconds=CHEBI_TTL_SECONDS, refresh=refresh)
            if response.metadata.get("stale"):
                raise DataSourceError(f"Refusing stale ChEBI cache for {chebi_id}")
            fetched_at = _timestamp(response.metadata)
            digest = hashlib.sha256(response.text.encode("utf-8")).hexdigest()
            source_release = SourceRelease(
                database="ChEBI",
                release=f"api-response-sha256:{digest[:16]}",
                retrieved_at=fetched_at,
                sha256=digest,
                source_url=url,
            )
            try:
                payload = json.loads(response.text)
            except (ValueError, TypeError) as error:
                raise DataSourceError(f"Invalid ChEBI JSON for {chebi_id}") from error
            compound = parse_chebi_compound(payload, chebi_id, source_release)
            previous = by_primary.get(compound.chebi_id)
            if previous is not None and not _same_chemical_record(previous, compound):
                raise DataSourceError(f"Conflicting ChEBI responses for {compound.chebi_id}")
            by_primary.setdefault(compound.chebi_id, compound)
            resolutions.append(ChEBIResolution(chebi_id, compound.chebi_id))
        return ChEBICollection(
            compounds=tuple(sorted(by_primary.values(), key=lambda item: int(item.chebi_id.split(":")[1]))),
            resolutions=tuple(resolutions),
        )

    def enrich_rhea_records(
        self,
        rhea_records: Iterable[Mapping[str, Any]],
        *,
        refresh: bool = False,
    ) -> ChEBICollection:
        """Enrich records returned by sources.fetch_rhea, preserving Rhea IDs.

        The v1 'chebi_ids' field is a list of participants without reaction
        side or stoichiometry. This method must not construct ReactionParticipant.
        """
        links: set[tuple[str, str]] = set()
        for record in rhea_records:
            if not isinstance(record, Mapping):
                raise ValueError("Rhea records must be mappings")
            rhea_id = _normalize_rhea_id(record.get("rhea_id"))
            chebi_ids = record.get("chebi_ids")
            if not isinstance(chebi_ids, list):
                raise ValueError("Rhea chebi_ids must be a list")
            for chebi_id in chebi_ids:
                links.add((rhea_id, normalize_chebi_id(chebi_id)))
        dataset = self.collect((chebi for _, chebi in links), refresh=refresh)
        resolved = {entry.requested_id: entry.primary_id for entry in dataset.resolutions}
        rhea_links = tuple(
            RheaCompoundLink(rhea, chebi, resolved[chebi])
            for rhea, chebi in sorted(
                links,
                key=lambda pair: (int(pair[0].split(":")[1]), int(pair[1].split(":")[1])),
            )
        )
        return replace(dataset, rhea_links=rhea_links)
