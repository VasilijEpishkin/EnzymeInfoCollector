"""Bounded EC/name → UniProt candidate discovery → direct-evidence pipeline.

The EC lookup discovers *candidate proteins*, not Protein–Reaction edges.
Those edges remain sourced exclusively from UniProtKB catalytic activity
comments with direct Rhea cross-references.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
from typing import Any
from urllib.parse import urlencode

from .evidence_sources import DirectEvidenceCollector
from .graph_export import project_graph
from .reference_dataset import (
    _canonical_bytes, collect_reference_from_evidence,
)
from .sources import (
    DataSourceError, ENZYME_URL, HttpCache, UNIPROT_ACCESSION,
    find_enzyme_entries, parse_enzyme_dat, validate_ec_number,
)

SEARCH_URL = "https://rest.uniprot.org/uniprotkb/search"
ONE_DAY = 24 * 60 * 60
ONE_WEEK = 7 * ONE_DAY
MAX_DISCOVERY = 10
SEARCH_HEADER = ["Entry", "Reviewed", "Organism (ID)", "EC number"]


def _checked_response(cache: HttpCache, url: str, *, refresh: bool, ttl: int):
    response = cache.get(url, ttl_seconds=ttl, refresh=refresh)
    if response.metadata.get("stale"):
        raise DataSourceError("Refusing stale candidate discovery source: " + url.split("?")[0])
    return response


def resolve_ec(
    cache: HttpCache, *, ec: str | None = None, enzyme: str | None = None,
    refresh: bool = False,
) -> tuple[str, dict[str, Any]]:
    """Resolve an exact name using legacy ExPASy parsing, or validate EC.

    Ambiguous names and transferred/deleted ECs are *not* silently resolved
    to an arbitrary different classification.
    """
    if (ec is None) == (enzyme is None):
        raise ValueError("Specify exactly one EC number or enzyme name")
    if ec is not None:
        return validate_ec_number(ec), {"kind": "ec", "input": ec, "resolved_by": "validated_ec"}
    name = enzyme.strip() if isinstance(enzyme, str) else ""
    if not name or len(name) > 150:
        raise ValueError("Enzyme name must contain 1–150 characters")
    response = _checked_response(cache, ENZYME_URL, refresh=refresh, ttl=ONE_WEEK)
    entries = parse_enzyme_dat(response.text)
    if not entries:
        raise DataSourceError("ExPASy ENZYME catalogue is empty")
    matches = find_enzyme_entries(entries, name)
    if not matches:
        raise DataSourceError(f"No ExPASy ENZYME entry matches name {name!r}")
    if len(matches) != 1:
        identifiers = [str(x["ec_number"]) for x in matches[:12]]
        raise DataSourceError(
            f"Ambiguous enzyme name {name!r}: {len(matches)} EC matches, "
            f"first matches {identifiers}; retry with --ec"
        )
    match = matches[0]
    if match.get("status") != "active":
        raise DataSourceError(
            f"ExPASy EC {match['ec_number']} has status {match.get('status')!r}; "
            "resolve the transferred/deleted classification explicitly before querying"
        )
    normalized = validate_ec_number(match["ec_number"])
    return normalized, {
        "kind": "enzyme",
        "input": name,
        "resolved_by": "ExPASy ENZYME",
        "ec": normalized,
        "enzyme_name": match.get("enzyme_name"),
        "catalogue": {
            "url": ENZYME_URL, "fetched_at": response.metadata.get("fetched_at"),
            "sha256": hashlib.sha256(response.text.encode("utf-8")).hexdigest(),
        },
    }


def discover_accessions(
    cache: HttpCache, ec: str, *,
    max_proteins: int = 3, reviewed_only: bool = True,
    taxon_id: int | None = None, taxon_scope: str = "exact",
    refresh: bool = False,
) -> dict[str, Any]:
    """Request at most limit+1 TSV records; never download a bulk stream.

    Search order is UniProt's current API ranking (not biologically
    authoritative). An extra row detects truncation.
    """
    ec = validate_ec_number(ec)
    if type(max_proteins) is not int or not 1 <= max_proteins <= MAX_DISCOVERY:
        raise ValueError(f"max_proteins must be 1–{MAX_DISCOVERY}")
    if taxon_id is not None and (type(taxon_id) is not int or taxon_id <= 0):
        raise ValueError("taxon_id must be a positive integer")
    if taxon_scope not in ("exact", "descendants"):
        raise ValueError("taxon_scope must be exact or descendants")

    query = f"(ec:{ec})"
    if reviewed_only:
        query += " AND reviewed:true"
    if taxon_id is not None:
        field = "organism_id" if taxon_scope == "exact" else "taxonomy_id"
        query += f" AND {field}:{taxon_id}"
    params = urlencode({
        "query": query, "format": "tsv",
        "fields": "accession,reviewed,organism_id,ec",
        "size": max_proteins + 1,
    })
    url = f"{SEARCH_URL}?{params}"
    response = _checked_response(cache, url, refresh=refresh, ttl=ONE_DAY)
    reader = csv.DictReader(io.StringIO(response.text), delimiter="\t")
    if reader.fieldnames != SEARCH_HEADER:
        raise DataSourceError(f"UniProt candidate search columns changed: {reader.fieldnames!r}")
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in reader:
        if None in row or any(row.get(field) is None for field in SEARCH_HEADER):
            raise DataSourceError("UniProt candidate search contains malformed TSV rows")
        accession = row["Entry"]
        if not UNIPROT_ACCESSION.fullmatch(accession):
            raise DataSourceError(f"Invalid UniProt candidate accession {accession!r}")
        if accession in seen:
            raise DataSourceError(f"Duplicate UniProt candidate accession {accession}")
        seen.add(accession)
        raw_review = row["Reviewed"].lower()
        if raw_review not in ("reviewed", "unreviewed"):
            raise DataSourceError("UniProt candidate reviewed status is missing or invalid")
        if reviewed_only and raw_review != "reviewed":
            raise DataSourceError("UniProt returned unreviewed protein to reviewed-only search")
        tax_raw = row["Organism (ID)"]
        if not re.fullmatch(r"[1-9][0-9]*", tax_raw):
            raise DataSourceError("UniProt returned invalid organism taxonomy ID")
        if taxon_id is not None and taxon_scope == "exact" and int(tax_raw) != taxon_id:
            raise DataSourceError("UniProt exact taxon filter returned mismatched taxonomy")
        records.append({
            "accession": accession,
            "reviewed": raw_review == "reviewed",
            "taxon_id": int(tax_raw),
            "ec_numbers": [x.strip() for x in row["EC number"].split(";") if x.strip()],
        })
        if len(records) > max_proteins:
            # The HTTP response is already bounded to limit+1 records.
            break
    if not records:
        raise DataSourceError(
            "No UniProt proteins matched the specified EC/reviewed/taxonomy criteria"
        )
    return {
        "query": query, "ec": ec, "reviewed_only": reviewed_only,
        "taxon_id": taxon_id, "taxon_scope": taxon_scope,
        "max_proteins": max_proteins, "truncated": len(records) > max_proteins,
        "selection_order": "UniProt search response order (not a biological ranking)",
        "selected": records[:max_proteins],
        "source": {
            "database": "UniProtKB", "url": url,
            "fetched_at": response.metadata.get("fetched_at"),
            "sha256": hashlib.sha256(response.text.encode("utf-8")).hexdigest(),
        },
    }


def collect_discovered_graph(
    cache: HttpCache, *, ec: str | None = None, enzyme: str | None = None,
    max_proteins: int = 3, reviewed_only: bool = True,
    taxon_id: int | None = None, taxon_scope: str = "exact",
    refresh: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return verified reference and Cytoscape graph for selected candidates.

    Absent Rhea annotations are reported explicitly; no candidate is
    interpreted as a confirmed protein–reaction match based on EC.
    """
    resolved_ec, resolved = resolve_ec(cache, ec=ec, enzyme=enzyme, refresh=refresh)
    discovery = discover_accessions(
        cache, resolved_ec, max_proteins=max_proteins,
        reviewed_only=reviewed_only, taxon_id=taxon_id,
        taxon_scope=taxon_scope, refresh=refresh,
    )
    accessions = [item["accession"] for item in discovery["selected"]]
    evidence = DirectEvidenceCollector(cache).collect(accessions, refresh=refresh)
    observed_accessions = [protein.accession for protein in evidence.proteins]
    if observed_accessions != accessions:
        raise DataSourceError("UniProt candidate accessions disagree with direct evidence responses")
    unlinked = [protein.accession for protein in evidence.proteins if not protein.assertions]
    linked = [protein.accession for protein in evidence.proteins if protein.assertions]
    if not linked:
        raise DataSourceError(
            f"No selected proteins had a direct UniProt→Rhea catalytic annotation; "
            f"checked {accessions}. EC classifications do not imply catalysis"
        )
    reference = collect_reference_from_evidence(evidence, cache, refresh=refresh)
    info = {
        "request": resolved,
        "selection": discovery,
        "direct_rhea_annotated": linked,
        "without_direct_rhea_annotation": unlinked,
        "interpretation": (
            "EC identifies candidate proteins; only UniProtKB direct catalytic "
            "annotations establish Protein-Reaction links. Missing links are unknown."
        ),
    }
    reference["provenance"]["discovery"] = info
    del reference["semantic_sha256"]
    reference["semantic_sha256"] = hashlib.sha256(_canonical_bytes(reference)).hexdigest()
    graph = project_graph(reference)
    return reference, graph
