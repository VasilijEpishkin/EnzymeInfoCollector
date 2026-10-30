"""Deterministic, source-aware Protein–Reaction–Compound reference projection.

This adapter consumes *already validated* v2 domain records. It does not
perform any HTTP requests, infer Protein–Reaction edges from EC classification,
or infer substrate side/stoichiometry from Rhea v1's flat ChEBI ID list.
"""

from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from typing import Any, Iterable, Mapping

from .compound_sources import ChEBICollection, ChEBICollector, normalize_chebi_id
from .domain import SourceRelease
from .evidence_sources import DirectEvidenceCollector, DirectEvidenceDataset
from .rhea_participants import RheaParticipantSnapshot, fetch_rhea_participants
from .sources import HttpCache
from .sources import DataSourceError


SCHEMA_VERSION = "2.0-reference-preview"


def _rhea_id(value: Any) -> str:
    if not isinstance(value, str):
        raise DataSourceError("Rhea record missing rhea_id")
    value = value.removeprefix("RHEA:")
    if not value.isascii() or not value.isdigit() or int(value) <= 0:
        raise DataSourceError(f"Invalid Rhea record ID: {value!r}")
    return "RHEA:" + value


def _num_key(value: str) -> int:
    return int(value.split(":")[1])


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def build_reference_dataset(
    evidence: DirectEvidenceDataset,
    rhea_records: Iterable[Mapping[str, Any]],
    compounds: ChEBICollection,
    *,
    rhea_source: SourceRelease,
) -> dict[str, Any]:
    """Build a deterministic reference projection with explicit unresolved IDs.

    Only Rhea rows whose *master ID* equals the master ID of a direct UniProt
    assertion are joined. EC classifications are never used as join keys.
    A Rhea v1 chebi_ids list asserts membership only; left/right and
    stoichiometric coefficient are UNKNOWN and must not be manufactured.

    The semantic SHA-256 covers the entire canonical JSON payload, including
    source retrieval provenance. Exactly identical snapshots and entries
    produce byte-for-byte identical output irrespective of record order.
    """
    if not isinstance(evidence, DirectEvidenceDataset):
        raise TypeError("evidence must be a DirectEvidenceDataset")
    if not isinstance(compounds, ChEBICollection):
        raise TypeError("compounds must be a ChEBICollection")
    if not isinstance(rhea_source, SourceRelease) or rhea_source.database != "Rhea":
        raise ValueError("rhea_source must be a Rhea SourceRelease")
    if not evidence.proteins:
        raise ValueError("At least one protein evidence record is required")

    by_accession = {}
    direct_masters: set[str] = set()
    for protein in evidence.proteins:
        if protein.accession in by_accession:
            raise DataSourceError(f"Duplicate protein accession: {protein.accession}")
        if protein.source_release.release != evidence.uniprot_release:
            raise DataSourceError(f"UniProt release mismatch for {protein.accession}")
        by_accession[protein.accession] = protein
        direct_masters.update(assertion.reaction.master_id for assertion in protein.assertions)

    rows: dict[str, tuple[str | None, tuple[str, ...]]] = {}
    for row in rhea_records:
        if not isinstance(row, Mapping):
            raise DataSourceError("Rhea records must be mappings")
        master = _rhea_id(row.get("rhea_id"))
        if master not in direct_masters:
            continue  # Rhea's independent EC catalogue must never create edges.
        equation = row.get("equation")
        if equation is not None and (not isinstance(equation, str) or not equation.strip()):
            raise DataSourceError(f"Invalid Rhea equation for {master}")
        raw_chebi = row.get("chebi_ids")
        if not isinstance(raw_chebi, list):
            raise DataSourceError(f"Invalid Rhea chebi_ids for {master}")
        try:
            members = tuple(sorted(
                {normalize_chebi_id(item) for item in raw_chebi}, key=_num_key
            ))
        except ValueError as error:
            raise DataSourceError(f"Invalid ChEBI membership in {master}") from error
        normalized = (equation, members)
        if master in rows and rows[master] != normalized:
            raise DataSourceError(f"Conflicting Rhea records for {master}")
        rows[master] = normalized

    resolution: dict[str, str] = {}
    for alias in compounds.resolutions:
        if alias.requested_id in resolution and resolution[alias.requested_id] != alias.primary_id:
            raise DataSourceError(f"Conflicting ChEBI resolutions for {alias.requested_id}")
        resolution[alias.requested_id] = alias.primary_id
    chemical_by_primary = {}
    for compound in compounds.compounds:
        if compound.chebi_id in chemical_by_primary:
            raise DataSourceError(f"Duplicate ChEBI primary compound: {compound.chebi_id}")
        chemical_by_primary[compound.chebi_id] = compound

    reported_primary_ids: set[str] = set()
    unresolved_chebi_ids: set[str] = set()
    reaction_rows = []
    for master in sorted(direct_masters, key=_num_key):
        if master not in rows:
            reaction_rows.append({
                "master_id": master,
                "status": "missing_rhea_record",
                "equation": None,
                "compound_membership": [],
            })
            continue
        equation, members = rows[master]
        memberships = []
        for identifier in members:
            primary = resolution.get(identifier)
            if primary is not None and primary not in chemical_by_primary:
                raise DataSourceError(f"ChEBI resolution {identifier} points to a missing compound {primary}")
            if primary is None:
                unresolved_chebi_ids.add(identifier)
            else:
                reported_primary_ids.add(primary)
            memberships.append({
                "reported_chebi_id": identifier,
                "primary_chebi_id": primary,
                "resolution_status": "resolved" if primary else "unresolved",
                "side": None,  # Rhea v1 flat list does not encode reaction sides.
                "coefficient": None,
            })
        reaction_rows.append({
            "master_id": master,
            "status": "rhea_record_found",
            "equation": equation,
            "compound_membership": memberships,
        })

    proteins = []
    for accession, protein in sorted(by_accession.items()):
        proteins.append({
            "accession": accession,
            "reviewed": protein.reviewed,
            "entry_version": protein.entry_version,
            "sequence_version": protein.sequence_version,
            "source_release": asdict(protein.source_release),
            "direct_assertions": [
                asdict(assertion) for assertion in sorted(
                    protein.assertions, key=lambda item: _num_key(item.reaction.rhea_id)
                )
            ],
        })
    payload = {
        "schema_version": SCHEMA_VERSION,
        "semantics": {
            "protein_reaction": "direct UniProtKB catalytic annotations only",
            "ec_join": "prohibited",
            "unknown_is_not_negative": True,
            "compound_membership": "Rhea flat ChEBI list; no substrate side or stoichiometry",
        },
        "provenance": {
            "uniprot_release": evidence.uniprot_release,
            "rhea_directions": asdict(evidence.rhea_directions_snapshot),
            "rhea_reaction_records": asdict(rhea_source),
            "chebi_snapshots": [
                asdict(compounds_map.source_release) for _, compounds_map in
                sorted(chemical_by_primary.items(), key=lambda item: _num_key(item[0]))
                if compounds_map.chebi_id in reported_primary_ids
            ],
            "attribution": {
                "UniProtKB": "UniProt Consortium, CC BY 4.0",
                "Rhea": "Rhea database, CC BY 4.0",
                "ChEBI": "ChEBI, CC BY 4.0",
            },
        },
        "proteins": proteins,
        "reactions": reaction_rows,
        "compounds": [
            asdict(chemical_by_primary[identifier])
            for identifier in sorted(reported_primary_ids, key=_num_key)
        ],
        "unresolved": {
            "missing_rhea_master_ids": sorted(direct_masters - set(rows), key=_num_key),
            "unresolved_chebi_ids": sorted(unresolved_chebi_ids, key=_num_key),
        },
    }
    payload["semantic_sha256"] = hashlib.sha256(_canonical_bytes(payload)).hexdigest()
    return payload


def reference_json(dataset: Mapping[str, Any]) -> str:
    """Stable UTF-8 JSON for archived snapshots, including trailing newline."""
    return json.dumps(dataset, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def build_verified_reference_dataset(
    evidence: DirectEvidenceDataset,
    reactions: RheaParticipantSnapshot,
    compounds: ChEBICollection,
) -> dict[str, Any]:
    """Build a Protein–Reaction–Compound result with verified RDF sides.

    The existing flat-list builder is intentionally left intact for
    backwards-compatible tests. This function uses only authoritative
    Rhea RDF rh:side, rh:contains and rh:coefficient information.
    """
    required = {
        assertion.reaction.master_id
        for protein in evidence.proteins
        for assertion in protein.assertions
    }
    available = {reaction.master_id for reaction in reactions.reactions}
    if not required:
        raise ValueError("No directly annotated Rhea reactions to inspect")
    if available != required or len(available) != len(reactions.reactions):
        raise DataSourceError(
            f"Rhea participant coverage mismatch: missing={sorted(required - available)}, "
            f"unexpected={sorted(available - required)}"
        )

    inputs = []
    for reaction in reactions.reactions:
        inputs.append({
            "rhea_id": reaction.master_id,
            "equation": reaction.equation,
            "chebi_ids": sorted(
                {participant.chebi_id for participant in reaction.participants},
                key=_num_key,
            ),
        })
    result = build_reference_dataset(
        evidence, inputs, compounds, rhea_source=reactions.source_release
    )
    resolution = {
        record.requested_id: record.primary_id
        for record in compounds.resolutions
    }
    by_master = {reaction.master_id: reaction for reaction in reactions.reactions}
    for row in result["reactions"]:
        reaction = by_master[row["master_id"]]
        row["compound_membership"] = [
            {
                "reported_chebi_id": member.chebi_id,
                "primary_chebi_id": resolution.get(member.chebi_id),
                "resolution_status": "resolved" if member.chebi_id in resolution else "unresolved",
                "side": member.side.value,
                "coefficient": format(member.coefficient, "f"),
            }
            for member in sorted(
                reaction.participants,
                key=lambda item: (
                    item.side.value,
                    _num_key(item.chebi_id),
                    item.coefficient,
                ),
            )
        ]
    result["semantics"]["compound_membership"] = (
        "Rhea RDF authoritative left/right equation sides and stoichiometry; "
        "not automatically physiological substrate/product roles"
    )
    result["provenance"]["rhea_participant_queries"] = [
        asdict(query) for query in reactions.query_snapshots
    ]
    del result["semantic_sha256"]
    result["semantic_sha256"] = hashlib.sha256(_canonical_bytes(result)).hexdigest()
    return result


def collect_live_reference(
    accessions: Iterable[str],
    cache: HttpCache,
    *,
    refresh: bool = False,
) -> dict[str, Any]:
    """End-to-end, opt-in live check; does not modify the v1 CLI.

    Direct UniProt links select which Rhea masters to fetch. Thus no EC
    cross-product can create a Protein–Reaction relation. Maximum 25 Rhea
    master reactions; ChEBI adapter independently caps its requests at 100.
    """
    evidence = DirectEvidenceCollector(cache).collect(accessions, refresh=refresh)
    master_ids = {
        assertion.reaction.master_id
        for protein in evidence.proteins
        for assertion in protein.assertions
    }
    if not master_ids:
        raise DataSourceError("No directly annotated Rhea reactions; cannot build reference")
    rdf = fetch_rhea_participants(master_ids, cache, refresh=refresh)
    chebi_ids = {
        participant.chebi_id
        for reaction in rdf.reactions
        for participant in reaction.participants
    }
    compounds = ChEBICollector(cache).collect(chebi_ids, refresh=refresh)
    return build_verified_reference_dataset(evidence, rdf, compounds)
