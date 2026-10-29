"""Bounded UniProt SPARQL adapter for direct catalytic-activity Rhea links.

This adapter is intentionally opt-in: it does not alter the legacy EC collector.
It fetches explicit protein annotations, never a Protein-EC × Reaction-EC join.
"""

from __future__ import annotations

import json
import re
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable

from .domain import (
    Evidence,
    ProteinReactionAssertion,
    ReactionDirection,
    ReactionVariant,
    SourceRelease,
)
from .sources import DataSourceError, HttpCache

UNIPROT_SPARQL_URL = "https://sparql.uniprot.org/sparql"
SPARQL_RESULT_LIMIT = 1000
SPARQL_BATCH_SIZE = 10
_UNIPROT_IRI = re.compile(r"https?://purl\.uniprot\.org/uniprot/([A-Za-z0-9-]+)$")
_RHEA_IRI = re.compile(r"https?://rdf\.rhea-db\.org/([0-9]+)$")


@dataclass(frozen=True, slots=True)
class DirectRheaLink:
    accession: str
    rhea_id: str
    annotation_iri: str


def _bindings(payload: str) -> list[dict[str, Any]]:
    try:
        document = json.loads(payload)
        rows = document["results"]["bindings"]
        if not isinstance(rows, list):
            raise ValueError("bindings must be a list")
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("binding must be an object")
        return rows
    except (ValueError, KeyError, TypeError) as error:
        raise DataSourceError("UniProt SPARQL returned an invalid result document") from error


def _iri(row: dict[str, Any], key: str, pattern: re.Pattern[str]) -> str:
    value = row.get(key)
    if not isinstance(value, dict) or value.get("type") != "uri":
        raise DataSourceError(f"UniProt SPARQL binding {key} is missing or is not a URI")
    iri = value.get("value")
    match = pattern.fullmatch(iri) if isinstance(iri, str) else None
    if not match:
        raise DataSourceError(f"Unexpected UniProt SPARQL {key} URI: {value.get('value')!r}")
    return match.group(1)


def build_direct_rhea_query(accessions: Iterable[str], *, limit: int = SPARQL_RESULT_LIMIT) -> str:
    """Query explicit catalytic activity annotations only, bounded by accession and LIMIT."""
    unique = sorted(set(accessions))
    if not unique or len(unique) > SPARQL_BATCH_SIZE:
        raise ValueError(f"Supply 1–{SPARQL_BATCH_SIZE} accessions per request")
    if type(limit) is not int or not 1 <= limit <= SPARQL_RESULT_LIMIT:
        raise ValueError("Invalid SPARQL result limit")
    for accession in unique:
        if not re.fullmatch(r"[A-Za-z0-9-]+", accession):
            raise ValueError(f"Invalid accession: {accession!r}")
    values = " ".join(f"<http://purl.uniprot.org/uniprot/{item}>" for item in unique)
    return (
        "PREFIX up: <http://purl.uniprot.org/core/>\n"
        "SELECT DISTINCT ?protein ?annotation ?rhea WHERE {\n"
        f"  VALUES ?protein {{ {values} }}\n"
        "  ?protein up:annotation ?annotation .\n"
        "  ?annotation a up:Catalytic_Activity_Annotation ;\n"
        "              up:catalyticActivity/up:catalyzedReaction ?rhea .\n"
        f"}} LIMIT {limit}"
    )


def fetch_direct_rhea_links(
    accessions: Iterable[str],
    cache: HttpCache,
    *,
    refresh: bool = False,
    limit: int = SPARQL_RESULT_LIMIT,
) -> tuple[list[DirectRheaLink], dict[str, Any]]:
    """Return directly annotated pairs; reject a saturated LIMIT as incomplete."""
    requested = tuple(accessions)
    query = build_direct_rhea_query(requested, limit=limit)
    allowed = set(requested)
    url = UNIPROT_SPARQL_URL + "?" + urllib.parse.urlencode(
        {"query": query, "format": "json"}
    )
    response = cache.get(url, ttl_seconds=24 * 60 * 60, refresh=refresh)
    rows = _bindings(response.text)
    if len(rows) >= limit:
        raise DataSourceError("UniProt SPARQL hit the result limit; split the accession batch")
    links: set[DirectRheaLink] = set()
    for row in rows:
        accession = _iri(row, "protein", _UNIPROT_IRI)
        if accession not in allowed:
            raise DataSourceError(f"Unexpected UniProt accession in SPARQL result: {accession}")
        rhea_id = _iri(row, "rhea", _RHEA_IRI)
        annotation = row.get("annotation")
        if not isinstance(annotation, dict) or annotation.get("type") != "uri":
            raise DataSourceError("UniProt catalytic annotation URI is missing")
        iri = annotation.get("value", "")
        if not isinstance(iri, str) or not (
            iri.startswith(f"http://purl.uniprot.org/uniprot/{accession}#")
            or iri.startswith(f"https://purl.uniprot.org/uniprot/{accession}#")
        ):
            raise DataSourceError("Invalid UniProt catalytic annotation URI")
        links.add(DirectRheaLink(accession, f"RHEA:{rhea_id}", iri))
    return sorted(links, key=lambda item: (item.accession, item.rhea_id, item.annotation_iri)), response.metadata


def assertions_from_direct_links(
    links: Iterable[DirectRheaLink],
    *,
    source_release: SourceRelease,
    direction_lookup: dict[str, tuple[str, ReactionDirection]],
) -> list[ProteinReactionAssertion]:
    """Create provenance-marked direct links; missing directions are an error.

    A direct catalytic annotation is not necessarily experimental proof.
    Evidence is marked unknown until ECO and citations are fetched in a
    separate, validated attribution adapter.
    """
    grouped: dict[tuple[str, str], set[str]] = defaultdict(set)
    for link in links:
        grouped[(link.accession, link.rhea_id)].add(link.annotation_iri)
    assertions = []
    for (accession, rhea_id), annotations in sorted(grouped.items()):
        if rhea_id not in direction_lookup:
            raise DataSourceError(f"Rhea direction mapping missing for {rhea_id}")
        master_id, direction = direction_lookup[rhea_id]
        for annotation_iri in sorted(annotations):
            assertions.append(
                ProteinReactionAssertion(
                    accession=accession,
                    reaction=ReactionVariant(rhea_id, master_id, direction),
                    source_release=source_release,
                    annotation_id=annotation_iri,
                    evidence=(
                        Evidence(
                            source_database="UniProtKB",
                            # The annotation resource is NOT the reified statement URI.
                            source_statement_iri=None,
                        ),
                    ),
                )
            )
    return assertions
