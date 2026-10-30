"""Authoritative, bounded Rhea RDF participant/stoichiometry adapter.

Reads Rhea RDF SPARQL side membership and rh:coefficient, not textual
equations or the flat ChEBI list in the legacy EC collector. Left/right
are equation sides, not necessarily substrates/products in vivo.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import re
from typing import Any, Iterable
from urllib.parse import urlencode

from .domain import ReactionParticipant, ReactionSide, SourceRelease
from .sources import DataSourceError, HttpCache

RHEA_SPARQL_URL = "https://sparql.rhea-db.org/sparql"
MAX_MASTER_IDS = 25
BATCH_SIZE = 5
MAX_ROWS = 400
_ID_PATTERN = re.compile(r"RHEA:[1-9][0-9]*")
_REACTION_IRI = re.compile(r"https?://rdf\.rhea-db\.org/([1-9][0-9]*)")
_SIDE_IRI = re.compile(r"https?://rdf\.rhea-db\.org/([1-9][0-9]*)_([LR])")
_CHEBI_IRI = re.compile(r"https?://purl\.obolibrary\.org/obo/CHEBI_([1-9][0-9]*)")


@dataclass(frozen=True, slots=True)
class RheaReaction:
    master_id: str
    equation: str
    participants: tuple[ReactionParticipant, ...]


@dataclass(frozen=True, slots=True)
class RheaParticipantSnapshot:
    reactions: tuple[RheaReaction, ...]
    source_release: SourceRelease
    query_snapshots: tuple[SourceRelease, ...]


def normalize_master_id(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("Rhea master ID must be a string")
    result = value if value.startswith("RHEA:") else "RHEA:" + value
    if _ID_PATTERN.fullmatch(result) is None:
        raise ValueError(f"Invalid Rhea master ID: {value!r}")
    return result


def build_rhea_participants_query(master_ids: Iterable[str]) -> str:
    """Query only the explicit Rhea master reactions provided by the caller."""
    unique = sorted({normalize_master_id(item) for item in master_ids},
                    key=lambda item: int(item.split(":")[1]))
    if not 1 <= len(unique) <= BATCH_SIZE:
        raise ValueError(f"Supply 1–{BATCH_SIZE} unique Rhea IDs per SPARQL query")
    values = " ".join(f"<http://rdf.rhea-db.org/{item.split(':')[1]}>" for item in unique)
    return (
        "PREFIX rh: <http://rdf.rhea-db.org/>\n"
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
        "SELECT DISTINCT ?reaction ?equation ?side ?order ?coefficient ?chebi WHERE {\n"
        f"  VALUES ?reaction {{ {values} }}\n"
        "  ?reaction rh:equation ?equation ; rh:side ?side .\n"
        "  ?side rh:curatedOrder ?order .\n"
        "  ?contains rdfs:subPropertyOf rh:contains ; rh:coefficient ?coefficient .\n"
        "  ?side ?contains ?participant .\n"
        "  ?participant rh:compound ?compound .\n"
        "  OPTIONAL { ?compound rh:chebi ?chebi }\n"
        f"}} LIMIT {MAX_ROWS}"
    )


def _binding(row: dict[str, Any], field: str, kind: str) -> str:
    binding = row.get(field)
    if not isinstance(binding, dict) or binding.get("type") != kind:
        raise DataSourceError(f"Rhea RDF {field} binding is missing or not {kind}")
    value = binding.get("value")
    if not isinstance(value, str):
        raise DataSourceError(f"Rhea RDF {field} binding is invalid")
    return value


def parse_rhea_participants(
    body: str, expected_masters: Iterable[str]
) -> tuple[RheaReaction, ...]:
    """Reject incomplete/saturated responses and unsupported compound types."""
    expected = {normalize_master_id(item) for item in expected_masters}
    if not expected:
        raise ValueError("Expected Rhea master IDs must not be empty")
    try:
        document = json.loads(body)
        rows = document["results"]["bindings"]
    except (TypeError, ValueError, KeyError) as error:
        raise DataSourceError("Invalid Rhea SPARQL JSON response") from error
    if not isinstance(rows, list):
        raise DataSourceError("Rhea SPARQL bindings must be a list")
    if len(rows) >= MAX_ROWS:
        raise DataSourceError("Rhea SPARQL result reached its limit; refusing partial reaction")
    grouped: dict[str, dict[tuple[str, str], ReactionParticipant]] = {}
    equations: dict[str, str] = {}
    found_sides: dict[str, set[str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise DataSourceError("Invalid Rhea SPARQL row")
        reaction = _binding(row, "reaction", "uri")
        master_match = _REACTION_IRI.fullmatch(reaction)
        if master_match is None:
            raise DataSourceError(f"Unexpected Rhea reaction URI {reaction}")
        master = "RHEA:" + master_match.group(1)
        if master not in expected:
            raise DataSourceError(f"Unexpected Rhea reaction {master}")
        side_iri = _binding(row, "side", "uri")
        side_match = _SIDE_IRI.fullmatch(side_iri)
        if side_match is None or side_match.group(1) != master_match.group(1):
            raise DataSourceError(f"Unexpected Rhea side URI {side_iri}")
        side = ReactionSide.LEFT if side_match.group(2) == "L" else ReactionSide.RIGHT
        order = _binding(row, "order", "literal")
        if order != ("1" if side is ReactionSide.LEFT else "2"):
            raise DataSourceError(f"Rhea curatedOrder disagrees with side for {master}")
        equation = _binding(row, "equation", "literal")
        if not equation.strip():
            raise DataSourceError(f"Missing Rhea equation for {master}")
        if master in equations and equations[master] != equation:
            raise DataSourceError(f"Conflicting Rhea equation for {master}")
        equations[master] = equation
        chebi_iri = _binding(row, "chebi", "uri")
        chebi = _CHEBI_IRI.fullmatch(chebi_iri)
        if chebi is None:
            raise DataSourceError(f"Unsupported ChEBI URI for {master}: {chebi_iri}")
        coefficient = _binding(row, "coefficient", "literal")
        try:
            participant = ReactionParticipant(
                master_id=master,
                chebi_id="CHEBI:" + chebi.group(1),
                side=side,
                coefficient=Decimal(coefficient),
            )
        except (ValueError, TypeError) as error:
            raise DataSourceError(f"Invalid Rhea stoichiometric coefficient for {master}") from error
        key = (side.value, participant.chebi_id)
        members = grouped.setdefault(master, {})
        if key in members and members[key] != participant:
            raise DataSourceError(f"Conflicting stoichiometry for {master} / {key}")
        members[key] = participant
        found_sides.setdefault(master, set()).add(side.value)
    if set(grouped) != expected:
        raise DataSourceError(f"Missing Rhea reactions: {sorted(expected - set(grouped))}")
    result = []
    for master in sorted(expected, key=lambda item: int(item.split(":")[1])):
        if found_sides[master] != {"left", "right"}:
            raise DataSourceError(f"Rhea reaction {master} has an incomplete side")
        members = tuple(sorted(grouped[master].values(), key=lambda item:
                               (item.side.value, int(item.chebi_id.split(":")[1]))))
        result.append(RheaReaction(master, equations[master], members))
    return tuple(result)


def fetch_rhea_participants(
    masters: Iterable[str], cache: HttpCache, *, refresh: bool = False
) -> RheaParticipantSnapshot:
    """Fetch at most 25 master reactions from the official Rhea SPARQL API."""
    unique = sorted({normalize_master_id(item) for item in masters},
                    key=lambda item: int(item.split(":")[1]))
    if not 1 <= len(unique) <= MAX_MASTER_IDS:
        raise ValueError(f"Supply 1–{MAX_MASTER_IDS} unique Rhea masters")
    reactions: list[RheaReaction] = []
    query_snapshots: list[SourceRelease] = []
    for index in range(0, len(unique), BATCH_SIZE):
        batch = unique[index:index+BATCH_SIZE]
        query = build_rhea_participants_query(batch)
        url = RHEA_SPARQL_URL + "?" + urlencode({"query": query, "format": "json"})
        response = cache.get(url, ttl_seconds=24 * 60 * 60, refresh=refresh)
        if response.metadata.get("stale"):
            raise DataSourceError("Refusing stale Rhea SPARQL response")
        fetched = response.metadata.get("fetched_at")
        if not isinstance(fetched, str):
            raise DataSourceError("Rhea SPARQL response missing retrieval time")
        try:
            stamp = datetime.fromisoformat(fetched.replace("Z", "+00:00"))
            if stamp.utcoffset() is None:
                raise ValueError("missing timezone")
        except ValueError as error:
            raise DataSourceError("Invalid Rhea SPARQL retrieval time") from error
        sha = hashlib.sha256(response.text.encode("utf-8")).hexdigest()
        query_snapshots.append(SourceRelease(
            "Rhea", "sparql-response-sha256:" + sha[:16], fetched, sha256=sha, source_url=url
        ))
        reactions.extend(parse_rhea_participants(response.text, batch))
    source_digest = hashlib.sha256(
        "".join(source.sha256 for source in query_snapshots).encode("ascii")
    ).hexdigest()
    source_release = SourceRelease(
        "Rhea", "sparql-response-set-sha256:" + source_digest[:16],
        max(item.retrieved_at for item in query_snapshots), sha256=source_digest,
        source_url=RHEA_SPARQL_URL,
    )
    return RheaParticipantSnapshot(tuple(reactions), source_release, tuple(query_snapshots))
