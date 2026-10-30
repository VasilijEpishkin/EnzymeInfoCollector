"""Direct UniProtKB catalytic-activity assertions reconciled with Rhea directions.

This module is opt-in: the v1 EC-centric collector and its output formats are
unchanged. No ProteinReactionAssertion is synthesized from a shared EC number.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import io
import json
import re
from types import MappingProxyType
from typing import Any, Iterable, Mapping
from urllib.parse import quote

from .domain import (
    Evidence,
    ProteinReactionAssertion,
    ReactionDirection,
    ReactionVariant,
    SourceRelease,
)
from .sources import CachedResponse, DataSourceError, HttpCache, UNIPROT_ACCESSION

UNIPROT_ENTRY_URL = "https://rest.uniprot.org/uniprotkb/{accession}.json"
UNIPROT_RELEASE_URL = (
    "https://ftp.uniprot.org/pub/databases/uniprot/current_release/"
    "knowledgebase/complete/reldate.txt"
)
RHEA_DIRECTIONS_URL = "https://ftp.expasy.org/databases/rhea/tsv/rhea-directions.tsv"
ONE_DAY = 24 * 60 * 60
ONE_WEEK = 7 * ONE_DAY

_DIRECTION_COLUMNS = {
    "RHEA_ID_MASTER": ReactionDirection.UNDEFINED,
    "RHEA_ID_LR": ReactionDirection.LEFT_TO_RIGHT,
    "RHEA_ID_RL": ReactionDirection.RIGHT_TO_LEFT,
    "RHEA_ID_BI": ReactionDirection.BIDIRECTIONAL,
}
_PHYSIOLOGICAL_DIRECTIONS = {
    "left-to-right": ReactionDirection.LEFT_TO_RIGHT,
    "right-to-left": ReactionDirection.RIGHT_TO_LEFT,
    "bidirectional": ReactionDirection.BIDIRECTIONAL,
}


@dataclass(frozen=True, slots=True)
class RheaDirectionsIndex:
    """Content-addressed direction mapping; not an official Rhea release label."""

    source_release: SourceRelease
    variants: Mapping[str, ReactionVariant]

    def lookup(self, rhea_id: str) -> ReactionVariant:
        try:
            return self.variants[rhea_id]
        except KeyError as error:
            raise DataSourceError(f"Rhea ID {rhea_id!r} absent from the directions snapshot") from error


@dataclass(frozen=True, slots=True)
class ProteinEvidenceRecord:
    accession: str
    reviewed: bool
    entry_version: int
    sequence_version: int
    source_release: SourceRelease
    assertions: tuple[ProteinReactionAssertion, ...]
    # These are UniProt classifications/organism annotations, NOT catalysis evidence.
    taxon_id: int | None = None
    organism_name: str | None = None
    ec_numbers: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DirectEvidenceDataset:
    uniprot_release: str
    rhea_directions_snapshot: SourceRelease
    proteins: tuple[ProteinEvidenceRecord, ...]


def _json_object(text: str, accession: str) -> dict[str, Any]:
    try:
        data = json.loads(text)
    except (ValueError, TypeError) as error:
        raise DataSourceError(f"Invalid UniProt JSON for {accession}") from error
    if not isinstance(data, dict):
        raise DataSourceError(f"UniProt JSON for {accession} is not an object")
    return data


def _require_fresh(response: CachedResponse, label: str) -> CachedResponse:
    if response.metadata.get("stale"):
        raise DataSourceError(f"Refusing stale HTTP cache for {label}: evidence may be out of sync")
    return response


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


def parse_uniprot_release(text: str) -> tuple[str, datetime]:
    """Parse official UniProt release ID and its Swiss-Prot publication date."""
    match = re.search(
        r"^UniProt Knowledgebase Release (\d{4}_\d{2}) consists of:",
        text,
        re.MULTILINE,
    )
    if match is None:
        raise DataSourceError("UniProt release metadata has an unexpected format")
    release = match.group(1)
    published = re.search(
        rf"^UniProtKB/Swiss-Prot Release {re.escape(release)} of (\d{{2}}-[A-Za-z]{{3}}-\d{{4}})$",
        text,
        re.MULTILINE,
    )
    if published is None:
        raise DataSourceError("UniProt Swiss-Prot release date is missing")
    try:
        date = datetime.strptime(published.group(1), "%d-%b-%Y").replace(tzinfo=timezone.utc)
    except ValueError as error:
        raise DataSourceError("UniProt release date is invalid") from error
    return release, date


def parse_rhea_directions(text: str, source_release: SourceRelease) -> RheaDirectionsIndex:
    """Validate Rhea's master/LR/RL/BI TSV and index all four identifiers."""
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    if reader.fieldnames != list(_DIRECTION_COLUMNS):
        raise DataSourceError("Unexpected Rhea directions TSV header")
    variants: dict[str, ReactionVariant] = {}
    count = 0
    for row in reader:
        count += 1
        if None in row or any(not row.get(field) for field in _DIRECTION_COLUMNS):
            raise DataSourceError(f"Incomplete Rhea directions row {count}")
        master = row["RHEA_ID_MASTER"]
        try:
            for column, direction in _DIRECTION_COLUMNS.items():
                variant = ReactionVariant(row[column], master, direction)
                if variant.rhea_id in variants:
                    raise DataSourceError(f"Duplicate Rhea ID {variant.rhea_id}")
                variants[variant.rhea_id] = variant
        except ValueError as error:
            raise DataSourceError(f"Invalid Rhea directions row {count}: {error}") from error
    if not count:
        raise DataSourceError("Rhea directions TSV is empty")
    return RheaDirectionsIndex(source_release, MappingProxyType(variants))


def _rhea_references(value: Any, context: str) -> tuple[str, ...]:
    """Read only numeric Rhea IDs, never RHEA-COMP or ChEBI references."""
    if value is None:
        return ()
    if not isinstance(value, list):
        raise DataSourceError(f"{context}: reactionCrossReferences must be a list")
    ids: list[str] = []
    for reference in value:
        if not isinstance(reference, dict):
            raise DataSourceError(f"{context}: invalid cross-reference")
        if reference.get("database") != "Rhea":
            continue
        identifier = reference.get("id")
        if isinstance(identifier, str) and identifier.startswith("RHEA-COMP:"):
            continue
        if not isinstance(identifier, str) or re.fullmatch(r"RHEA:[1-9]\d*", identifier) is None:
            raise DataSourceError(f"{context}: invalid Rhea cross-reference {identifier!r}")
        if identifier not in ids:
            ids.append(identifier)
    return tuple(ids)


def _evidence_records(value: Any, context: str) -> tuple[Evidence, ...]:
    if value is None:
        value = []
    if not isinstance(value, list):
        raise DataSourceError(f"{context}: evidences must be a list")
    records: list[Evidence] = []
    for evidence in value:
        if not isinstance(evidence, dict):
            raise DataSourceError(f"{context}: evidence must be an object")
        source = evidence.get("source")
        citation_id = evidence.get("id")
        if source is not None and (not isinstance(source, str) or not source.strip()):
            raise DataSourceError(f"{context}: invalid citation source")
        if citation_id is not None and (not isinstance(citation_id, str) or not citation_id.strip()):
            raise DataSourceError(f"{context}: invalid citation identifier")
        citations = (f"{source}:{citation_id}",) if source and citation_id else ()
        try:
            records.append(
                Evidence(
                    source_database="UniProtKB",
                    eco_code=evidence.get("evidenceCode"),
                    citation_ids=citations,
                )
            )
        except (TypeError, ValueError) as error:
            raise DataSourceError(f"{context}: invalid ECO evidence: {error}") from error
    # The source's catalytic annotation is direct even if its evidence list is
    # empty. This placeholder does NOT assert experimental support.
    return tuple(records) or (Evidence(source_database="UniProtKB"),)


def _positive_version(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise DataSourceError(f"UniProt {label} must be a positive integer")
    return value


_EC_NUMBER = re.compile(r"[1-7]\.(?:[0-9]+|-|n[0-9]+)\.(?:[0-9]+|-|n[0-9]+)\.(?:[0-9]+|-|n[0-9]+)", re.IGNORECASE)


def _protein_metadata(data: Mapping[str, Any]) -> tuple[int | None, str | None, tuple[str, ...]]:
    """Read exact organism taxon and classified ECs from UniProtKB JSON.

    EC classification is an attribute of the protein description, not a
    certificate that the protein catalyzes every Rhea reaction with that EC.
    Unavailable fields remain unknown; malformed populated fields fail closed.
    """
    organism = data.get("organism")
    if organism is not None and not isinstance(organism, dict):
        raise DataSourceError("UniProt organism must be an object")
    taxon_id = None
    organism_name = None
    if organism is not None:
        taxon_id = organism.get("taxonId")
        if taxon_id is not None and (type(taxon_id) is not int or taxon_id <= 0):
            raise DataSourceError("UniProt taxonId must be a positive integer")
        organism_name = organism.get("scientificName")
        if organism_name is not None and (not isinstance(organism_name, str) or not organism_name.strip()):
            raise DataSourceError("UniProt scientificName must be nonempty")
    description = data.get("proteinDescription")
    if description is not None and not isinstance(description, dict):
        raise DataSourceError("UniProt proteinDescription must be an object")
    ec_numbers: set[str] = set()
    if description:
        names: list[dict[str, Any]] = []
        for key in ("recommendedName",):
            name = description.get(key)
            if name is not None:
                if not isinstance(name, dict):
                    raise DataSourceError(f"UniProt {key} must be an object")
                names.append(name)
        for key in ("alternativeNames", "submissionNames"):
            values = description.get(key, [])
            if not isinstance(values, list):
                raise DataSourceError(f"UniProt {key} must be a list")
            for value in values:
                if not isinstance(value, dict):
                    raise DataSourceError(f"UniProt {key} contains an invalid entry")
                names.append(value)
        for name in names:
            assignments = name.get("ecNumbers", [])
            if not isinstance(assignments, list):
                raise DataSourceError("UniProt ecNumbers must be a list")
            for assignment in assignments:
                if not isinstance(assignment, dict):
                    raise DataSourceError("UniProt EC assignment must be an object")
                value = assignment.get("value")
                if not isinstance(value, str) or _EC_NUMBER.fullmatch(value) is None:
                    raise DataSourceError(f"Invalid UniProt EC assignment {value!r}")
                ec_numbers.add(value)
    return taxon_id, organism_name.strip() if organism_name else None, tuple(sorted(ec_numbers))


def parse_uniprot_catalytic_activities(
    data: Mapping[str, Any],
    source_release: SourceRelease,
    directions: RheaDirectionsIndex,
) -> ProteinEvidenceRecord:
    """Extract direct assertions only from UniProt catalytic activity comments.

    A master reaction and its physiological directions are *separate* source
    assertions with their own evidence. No evidence is inherited across them.
    """
    if not isinstance(data, dict):
        raise DataSourceError("UniProt entry must be a JSON object")
    accession = data.get("primaryAccession")
    if not isinstance(accession, str) or UNIPROT_ACCESSION.fullmatch(accession) is None:
        raise DataSourceError("UniProt entry has an invalid primary accession")
    audit = data.get("entryAudit")
    if not isinstance(audit, dict):
        raise DataSourceError(f"UniProt {accession} is missing entryAudit")
    entry_version = _positive_version(audit.get("entryVersion"), "entryVersion")
    sequence_version = _positive_version(audit.get("sequenceVersion"), "sequenceVersion")
    entry_type = data.get("entryType")
    if not isinstance(entry_type, str):
        raise DataSourceError(f"UniProt {accession} is missing entryType")
    reviewed = entry_type == "UniProtKB reviewed (Swiss-Prot)"
    comments = data.get("comments", [])
    if not isinstance(comments, list):
        raise DataSourceError(f"UniProt {accession}: comments must be a list")

    grouped: dict[str, list[Evidence]] = {}
    for comment_number, comment in enumerate(comments, 1):
        if not isinstance(comment, dict):
            raise DataSourceError(f"UniProt {accession}: malformed comment")
        if comment.get("commentType") != "CATALYTIC ACTIVITY":
            continue
        context = f"UniProt {accession} catalytic comment {comment_number}"
        reaction = comment.get("reaction")
        if not isinstance(reaction, dict):
            raise DataSourceError(f"{context}: missing reaction")
        master_ids = _rhea_references(reaction.get("reactionCrossReferences"), context)
        if not master_ids:
            # Not all catalytic annotations have a Rhea cross-reference.
            # Keep them out of the Rhea-linked result; do not invent an edge.
            if comment.get("physiologicalReactions"):
                raise DataSourceError(f"{context}: physiological Rhea link without a master Rhea link")
            continue
        if len(master_ids) != 1:
            raise DataSourceError(f"{context}: expected one Rhea master ID, got {master_ids}")
        master_id = master_ids[0]
        master_variant = directions.lookup(master_id)
        if master_variant.direction is not ReactionDirection.UNDEFINED or master_variant.master_id != master_id:
            raise DataSourceError(f"{context}: {master_id} is not a Rhea master ID")
        grouped.setdefault(master_id, []).extend(_evidence_records(reaction.get("evidences"), context))

        physiological = comment.get("physiologicalReactions", [])
        if not isinstance(physiological, list):
            raise DataSourceError(f"{context}: physiologicalReactions must be a list")
        for physiological_reaction in physiological:
            if not isinstance(physiological_reaction, dict):
                raise DataSourceError(f"{context}: malformed physiological reaction")
            reference = physiological_reaction.get("reactionCrossReference")
            if not isinstance(reference, dict):
                raise DataSourceError(f"{context}: missing physiological Rhea reference")
            ids = _rhea_references([reference], context)
            if len(ids) != 1:
                raise DataSourceError(f"{context}: physiological reaction requires one numeric Rhea ID")
            variant = directions.lookup(ids[0])
            if variant.master_id != master_id or variant.direction is ReactionDirection.UNDEFINED:
                raise DataSourceError(f"{context}: physiological Rhea ID does not match its master")
            direction_type = physiological_reaction.get("directionType")
            if direction_type is not None:
                expected = _PHYSIOLOGICAL_DIRECTIONS.get(direction_type)
                if expected is None or expected is not variant.direction:
                    raise DataSourceError(f"{context}: physiological direction disagrees with Rhea mapping")
            grouped.setdefault(variant.rhea_id, []).extend(
                _evidence_records(physiological_reaction.get("evidences"), context)
            )

    assertions = tuple(
        ProteinReactionAssertion(
            accession=accession,
            reaction=directions.lookup(rhea_id),
            source_release=source_release,
            evidence=tuple(evidences),
        )
        for rhea_id, evidences in sorted(grouped.items(), key=lambda item: int(item[0].split(":")[1]))
    )
    taxon_id, organism_name, ec_numbers = _protein_metadata(data)
    return ProteinEvidenceRecord(
        accession=accession,
        reviewed=reviewed,
        entry_version=entry_version,
        sequence_version=sequence_version,
        source_release=source_release,
        assertions=assertions,
        taxon_id=taxon_id,
        organism_name=organism_name,
        ec_numbers=ec_numbers,
    )


class DirectEvidenceCollector:
    """Opt-in API for accession-specific evidence, with the existing HttpCache."""

    def __init__(self, cache: HttpCache):
        self.cache = cache

    def collect(self, accessions: Iterable[str], *, refresh: bool = False) -> DirectEvidenceDataset:
        if isinstance(accessions, str):
            raise ValueError("accessions must be an iterable of accession strings")
        unique: list[str] = []
        for accession in accessions:
            if not isinstance(accession, str) or UNIPROT_ACCESSION.fullmatch(accession) is None:
                raise ValueError(f"Invalid canonical UniProt accession {accession!r}")
            if accession not in unique:
                unique.append(accession)
            if len(unique) > 100:
                raise ValueError("A single direct-evidence batch may contain at most 100 accessions")
        if not unique:
            raise ValueError("At least one accession is required")

        release_response = _require_fresh(
            self.cache.get(UNIPROT_RELEASE_URL, ttl_seconds=ONE_DAY, refresh=refresh),
            "UniProt release metadata",
        )
        release, published_at = parse_uniprot_release(release_response.text)
        directions_response = _require_fresh(
            self.cache.get(RHEA_DIRECTIONS_URL, ttl_seconds=ONE_WEEK, refresh=refresh),
            "Rhea directions",
        )
        digest = hashlib.sha256(directions_response.text.encode("utf-8")).hexdigest()
        rhea_snapshot = SourceRelease(
            database="Rhea",
            release=f"directions-sha256:{digest[:16]}",
            retrieved_at=_fetched_at(directions_response.metadata, "Rhea directions"),
            sha256=digest,
            source_url=RHEA_DIRECTIONS_URL,
        )
        directions = parse_rhea_directions(directions_response.text, rhea_snapshot)

        proteins: list[ProteinEvidenceRecord] = []
        for accession in unique:
            url = UNIPROT_ENTRY_URL.format(accession=quote(accession, safe=""))
            response = _require_fresh(
                self.cache.get(url, ttl_seconds=ONE_DAY, refresh=refresh),
                f"UniProt {accession}",
            )
            fetched_at = _fetched_at(response.metadata, f"UniProt {accession}")
            fetched_stamp = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
            if fetched_stamp < published_at:
                # An old cache entry must not be labelled with a newer release.
                response = _require_fresh(
                    self.cache.get(url, ttl_seconds=ONE_DAY, refresh=True),
                    f"UniProt {accession} (release refresh)",
                )
                fetched_at = _fetched_at(response.metadata, f"UniProt {accession}")
                fetched_stamp = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
                if fetched_stamp < published_at:
                    raise DataSourceError(f"UniProt {accession} response predates release {release}")
            entry = _json_object(response.text, accession)
            if entry.get("primaryAccession") != accession:
                raise DataSourceError(f"UniProt response accession mismatch: requested {accession}")
            source_release = SourceRelease(
                database="UniProtKB",
                release=release,
                retrieved_at=fetched_at,
                sha256=hashlib.sha256(response.text.encode("utf-8")).hexdigest(),
                source_url=url,
            )
            proteins.append(parse_uniprot_catalytic_activities(entry, source_release, directions))
        return DirectEvidenceDataset(
            uniprot_release=release,
            rhea_directions_snapshot=rhea_snapshot,
            proteins=tuple(proteins),
        )
