"""Evidence-aware UniProtKB REST catalytic activity ingestion.

UniProt JSON exposes evidence on individual catalytic-activity reactions,
including ECO codes and PubMed/rule sources. This is not an EC-based join.
The legacy CLI is unchanged; this is an opt-in programmatic adapter.
"""

from __future__ import annotations

import json
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any

from .domain import Evidence, ProteinReactionAssertion, ReactionDirection, ReactionVariant, SourceRelease
from .sources import DataSourceError, HttpCache

UNIPROT_ENTRY_URL = "https://rest.uniprot.org/uniprotkb/"
_ACCESSION = re.compile(r"[A-Za-z0-9]+(?:-[0-9]+)?")
_RHEA = re.compile(r"RHEA:[1-9][0-9]*")
_ECO = re.compile(r"ECO:[0-9]{7}")


@dataclass(frozen=True, slots=True)
class UniProtCatalyticEntry:
    accession: str
    source_release: SourceRelease
    entry_version: int | None
    sequence_version: int | None
    reviewed: bool
    assertions: tuple[ProteinReactionAssertion, ...]
    catalytic_comments_without_rhea: int


def _object(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DataSourceError(f"UniProt {context} must be an object")
    return value


def _list(value: Any, context: str) -> list[Any]:
    if not isinstance(value, list):
        raise DataSourceError(f"UniProt {context} must be a list")
    return value


def _version(value: Any, name: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 1:
        raise DataSourceError(f"Invalid UniProt {name}")
    return value


def _evidences(value: Any) -> tuple[Evidence, ...]:
    """Preserve every ECO attribution and non-PubMed source reference."""
    if value is None:
        return (Evidence(source_database="UniProtKB"),)
    rows = _list(value, "reaction evidences")
    if not rows:
        return (Evidence(source_database="UniProtKB"),)
    evidences = []
    for row in rows:
        item = _object(row, "evidence")
        code = item.get("evidenceCode")
        if code is not None and (not isinstance(code, str) or not _ECO.fullmatch(code)):
            raise DataSourceError(f"Invalid UniProt evidence code: {code!r}")
        source, identifier = item.get("source"), item.get("id")
        if (source is None) != (identifier is None):
            raise DataSourceError("UniProt evidence source and ID must be supplied together")
        citation_ids: tuple[str, ...] = ()
        source_references: tuple[str, ...] = ()
        if source is not None:
            if not isinstance(source, str) or not source.strip() or not isinstance(identifier, str) or not identifier.strip():
                raise DataSourceError("Invalid UniProt evidence source reference")
            if source == "PubMed":
                citation_ids = ("PMID:" + identifier,)
            else:
                source_references = (source + ":" + identifier,)
        evidences.append(
            Evidence(
                source_database="UniProtKB",
                eco_code=code,
                citation_ids=citation_ids,
                source_references=source_references,
            )
        )
    return tuple(evidences)


def parse_uniprot_catalytic_entry(
    payload: str,
    metadata: dict[str, Any],
    requested_accession: str,
    direction_lookup: dict[str, tuple[str, ReactionDirection]],
) -> UniProtCatalyticEntry:
    """Parse one canonical UniProtKB entry with explicit Rhea cross-references.

    A JSON comment index is a *local, release-scoped* identifier, not an
    official UniProt RDF statement IRI. No evidence is invented when absent.
    """
    if not _ACCESSION.fullmatch(requested_accession):
        raise ValueError("Invalid UniProt accession")
    try:
        document = _object(json.loads(payload), "entry")
    except (ValueError, TypeError) as error:
        raise DataSourceError("Invalid UniProt entry JSON") from error
    accession = document.get("primaryAccession")
    if accession != requested_accession:
        raise DataSourceError(
            f"UniProt canonical accession mismatch: requested {requested_accession}, received {accession!r}"
        )
    release = metadata.get("uniprot_release")
    if not isinstance(release, str) or not release.strip():
        raise DataSourceError("UniProt release header missing from HTTP cache; refresh the entry")
    fetched_at = metadata.get("fetched_at")
    if not isinstance(fetched_at, str):
        raise DataSourceError("UniProt retrieval timestamp missing from HTTP cache")
    source_release = SourceRelease(
        database="UniProtKB",
        release=release,
        retrieved_at=fetched_at,
        sha256=metadata.get("body_sha256"),
        source_url=metadata.get("url") or UNIPROT_ENTRY_URL + requested_accession + ".json",
    )
    entry_type = document.get("entryType", "")
    if not isinstance(entry_type, str) or not entry_type.startswith("UniProtKB "):
        raise DataSourceError("Unrecognized UniProtKB entry type")
    reviewed = entry_type.startswith("UniProtKB reviewed (Swiss-Prot)")
    audit = _object(document.get("entryAudit", {}), "entryAudit")
    comments = _list(document.get("comments", []), "comments")
    assertions: list[ProteinReactionAssertion] = []
    without_rhea = 0
    for index, comment in enumerate(comments):
        comment = _object(comment, "comment")
        if comment.get("commentType") != "CATALYTIC ACTIVITY":
            continue
        reaction = _object(comment.get("reaction", {}), "catalytic reaction")
        cross_references = _list(reaction.get("reactionCrossReferences", []), "reaction cross-references")
        rhea_ids: set[str] = set()
        for ref in cross_references:
            ref = _object(ref, "reaction cross-reference")
            if ref.get("database") != "Rhea":
                continue
            identifier = ref.get("id")
            if isinstance(identifier, str) and _RHEA.fullmatch(identifier):
                rhea_ids.add(identifier)
            elif not isinstance(identifier, str) or not identifier.startswith("RHEA-COMP:"):
                raise DataSourceError(f"Unexpected Rhea cross-reference: {identifier!r}")
        if not rhea_ids:
            without_rhea += 1
            continue
        evidence = _evidences(reaction.get("evidences"))
        for rhea_id in sorted(rhea_ids):
            if rhea_id not in direction_lookup:
                raise DataSourceError(f"Rhea direction mapping missing for {rhea_id}")
            master, direction = direction_lookup[rhea_id]
            assertions.append(
                ProteinReactionAssertion(
                    accession=accession,
                    reaction=ReactionVariant(rhea_id, master, direction),
                    source_release=source_release,
                    evidence=evidence,
                    annotation_id=f"uniprotkb-json:{accession}:catalytic-activity:{index}",
                )
            )
    return UniProtCatalyticEntry(
        accession=accession,
        source_release=source_release,
        entry_version=_version(audit.get("entryVersion"), "entryVersion"),
        sequence_version=_version(audit.get("sequenceVersion"), "sequenceVersion"),
        reviewed=reviewed,
        assertions=tuple(assertions),
        catalytic_comments_without_rhea=without_rhea,
    )


def fetch_uniprot_catalytic_entry(
    accession: str,
    cache: HttpCache,
    direction_lookup: dict[str, tuple[str, ReactionDirection]],
    *,
    refresh: bool = False,
) -> UniProtCatalyticEntry:
    if not _ACCESSION.fullmatch(accession):
        raise ValueError("Invalid UniProt accession")
    url = UNIPROT_ENTRY_URL + urllib.parse.quote(accession, safe="") + ".json"
    response = cache.get(url, ttl_seconds=24 * 60 * 60, refresh=refresh)
    return parse_uniprot_catalytic_entry(response.text, response.metadata, accession, direction_lookup)
