"""Deterministic, read-only graph projection of a validated v2 reference result.

Only direct UniProt catalytic assertions become ANNOTATED_CATALYSIS edges.
An EC coincidence or external KEGG/MetaCyc cross-reference never does.
GraphML and Cytoscape JSON are alternate views of the same normalized input.
"""

from __future__ import annotations

import json
import re
from typing import Any, Mapping
from xml.etree import ElementTree as ET

from .sources import DataSourceError

GRAPHML_NS = "http://graphml.graphdrawing.org/xmlns"
_SUPPORTED_SCHEMA = "2.0-reference-preview"
_RHEA = re.compile(r"RHEA:[1-9][0-9]*")
_CHEBI = re.compile(r"CHEBI:[1-9][0-9]*")
_UNIPROT = re.compile(r"[A-Z0-9]+(?:-[0-9]+)?")


def _object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DataSourceError(f"{name} must be an object")
    return value


def _rows(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise DataSourceError(f"{name} must be a list")
    return value


def _identifier(value: Any, pattern: re.Pattern[str], name: str) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise DataSourceError(f"Invalid {name}: {value!r}")
    return value


def _add_node(nodes: dict[str, dict[str, Any]], key: str, record: dict[str, Any]) -> None:
    existing = nodes.get(key)
    if existing is not None and existing != record:
        raise DataSourceError(f"Conflicting graph node {key}")
    nodes[key] = record


def build_evidence_graph(
    reference: Mapping[str, Any], *, protein: str | None = None
) -> dict[str, Any]:
    """Produce typed nodes/edges, scoped to one protein if requested."""
    if not isinstance(reference, dict) or reference.get("schema_version") != _SUPPORTED_SCHEMA:
        raise DataSourceError("Expected v2 reference projection")
    proteins = _rows(reference.get("proteins"), "proteins")
    reactions = _rows(reference.get("reactions"), "reactions")
    compounds = _rows(reference.get("compounds"), "compounds")
    if protein is not None:
        _identifier(protein, _UNIPROT, "UniProt accession")
    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    chosen: list[dict[str, Any]] = []
    for item in proteins:
        item = _object(item, "protein")
        accession = _identifier(item.get("accession"), _UNIPROT, "UniProt accession")
        if protein is not None and accession != protein:
            continue
        chosen.append(item)
        _add_node(nodes, "protein:" + accession, {
            "id": "protein:" + accession,
            "type": "Protein",
            "label": accession,
            "accession": accession,
            "reviewed": item.get("reviewed"),
            "entry_version": item.get("entry_version"),
            "sequence_version": item.get("sequence_version"),
        })
    if protein is not None and not chosen:
        raise DataSourceError(f"Protein {protein} is not present in this result")
    active_master: set[str] = set()
    for item in chosen:
        accession = item["accession"]
        for assertion in _rows(item.get("direct_assertions"), "direct_assertions"):
            assertion = _object(assertion, "direct assertion")
            variant = _object(assertion.get("reaction"), "reaction variant")
            rhea_id = _identifier(variant.get("rhea_id"), _RHEA, "Rhea directional ID")
            master = _identifier(variant.get("master_id"), _RHEA, "Rhea master ID")
            direction = variant.get("direction")
            if direction not in ("undefined", "left_to_right", "right_to_left", "bidirectional"):
                raise DataSourceError(f"Invalid Rhea direction for {rhea_id}: {direction!r}")
            active_master.add(master)
            variant_key = "variant:" + rhea_id
            master_key = "reaction:" + master
            _add_node(nodes, variant_key, {
                "id": variant_key, "type": "ReactionVariant", "label": rhea_id,
                "rhea_id": rhea_id, "master_id": master, "direction": direction,
            })
            _add_node(nodes, master_key, {
                "id": master_key, "type": "Reaction", "label": master,
                "master_id": master,
            })
            evidence = _rows(assertion.get("evidence"), "assertion evidence")
            if not evidence:
                raise DataSourceError(f"Assertion {accession}↔{rhea_id} has no evidence record")
            source = _object(assertion.get("source_release"), "assertion source release")
            edges.append({
                "source": "protein:" + accession,
                "target": variant_key,
                "type": "ANNOTATED_CATALYSIS",
                "rhea_directional_id": rhea_id,
                "direction": direction,
                "evidence": evidence,
                "source_release": source,
                "annotation_id": assertion.get("annotation_id"),
            })
            edges.append({
                "source": variant_key,
                "target": master_key,
                "type": "VARIANT_OF",
            })

    reaction_map: dict[str, dict[str, Any]] = {}
    for item in reactions:
        item = _object(item, "reaction")
        master = _identifier(item.get("master_id"), _RHEA, "Rhea master ID")
        if master in reaction_map:
            raise DataSourceError(f"Duplicate master reaction {master}")
        reaction_map[master] = item
    chemicals: dict[str, dict[str, Any]] = {}
    for item in compounds:
        item = _object(item, "compound")
        chebi = _identifier(item.get("chebi_id"), _CHEBI, "ChEBI compound ID")
        if chebi in chemicals:
            raise DataSourceError(f"Duplicate ChEBI compound {chebi}")
        chemicals[chebi] = item

    for master in sorted(active_master):
        item = reaction_map.get(master)
        if item is None:
            raise DataSourceError(f"Rhea master {master} missing from reference result")
        status = item.get("status")
        if status not in ("missing_rhea_record", "rhea_record_found"):
            raise DataSourceError(f"Invalid Rhea record status for {master}")
        memberships = _rows(item.get("compound_membership"), "compound memberships")
        if status == "missing_rhea_record" and memberships:
            raise DataSourceError(f"Missing reaction {master} has participants")
        for participant in memberships:
            participant = _object(participant, "Rhea compound member")
            original = _identifier(participant.get("reported_chebi_id"), _CHEBI, "reported ChEBI ID")
            resolved = participant.get("primary_chebi_id")
            if resolved is not None:
                resolved = _identifier(resolved, _CHEBI, "primary ChEBI ID")
                if resolved not in chemicals:
                    raise DataSourceError(f"Resolved ChEBI compound {resolved} lacks metadata")
            compound_id = resolved or original
            chemical = chemicals.get(compound_id)
            node_id = "compound:" + compound_id
            _add_node(nodes, node_id, {
                "id": node_id, "type": "Compound", "label": (
                    chemical.get("name") if chemical else compound_id
                ),
                "chebi_id": compound_id,
                "formula": chemical.get("formula") if chemical else None,
                "charge": chemical.get("charge") if chemical else None,
                "resolution_status": "resolved" if resolved else "unresolved",
            })
            side = participant.get("side")
            coeff = participant.get("coefficient")
            if side is not None and side not in ("left", "right"):
                raise DataSourceError(f"Invalid Rhea side: {side!r}")
            if (side is None) != (coeff is None):
                raise DataSourceError("Partial side/coefficient metadata is not allowed")
            edges.append({
                "source": "reaction:" + master,
                "target": node_id,
                "type": "HAS_PARTICIPANT",
                "reported_chebi_id": original,
                "side": side,
                "coefficient": coeff,
            })
    edges.sort(key=lambda x: (
        x["source"], x["target"], x["type"], x.get("side") or "",
        x.get("reported_chebi_id") or "",
        json.dumps(x, ensure_ascii=False, sort_keys=True),
    ))
    for index, edge in enumerate(edges, 1):
        edge["id"] = "e" + str(index)
    return {
        "schema_version": "2.0-graph-preview",
        "source_reference_sha256": reference.get("semantic_sha256"),
        "semantics": "source-annotated catalysis; no EC-derived edges",
        "nodes": [nodes[key] for key in sorted(nodes)],
        "edges": edges,
    }


def cytoscape_json(graph: Mapping[str, Any]) -> str:
    """Create Cytoscape.js elements with inspectable evidence metadata."""
    return json.dumps({
        "schema_version": graph["schema_version"],
        "source_reference_sha256": graph.get("source_reference_sha256"),
        "elements": {
            "nodes": [{"data": item} for item in graph["nodes"]],
            "edges": [{"data": item} for item in graph["edges"]],
        },
    }, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def _encode(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def graphml(graph: Mapping[str, Any]) -> str:
    """GraphML with typed edges and full evidence stored as JSON attributes."""
    ET.register_namespace("", GRAPHML_NS)
    root = ET.Element("{" + GRAPHML_NS + "}graphml")
    attribute_keys: dict[tuple[str, str], str] = {}
    for target, records in (("node", graph["nodes"]), ("edge", graph["edges"])):
        fields = sorted({field for record in records for field in record
                         if field not in ("id", "source", "target")})
        for field in fields:
            key = f"attr_{target}_{len(attribute_keys)}"
            attribute_keys[(target, field)] = key
            ET.SubElement(root, "{" + GRAPHML_NS + "}key", {
                "id": key, "for": target,
                "attr.name": field, "attr.type": "string",
            })
    g = ET.SubElement(root, "{" + GRAPHML_NS + "}graph", {
        "id": "G", "edgedefault": "directed",
    })
    for target, records in (("node", graph["nodes"]), ("edge", graph["edges"])):
        for record in records:
            attributes = {"id": record["id"]}
            if target == "edge":
                attributes["source"] = record["source"]
                attributes["target"] = record["target"]
            element = ET.SubElement(g, "{" + GRAPHML_NS + "}" + target, attributes)
            for field in sorted(record):
                if field in ("id", "source", "target") or record[field] is None:
                    continue
                data = ET.SubElement(element, "{" + GRAPHML_NS + "}data", {
                    "key": attribute_keys[(target, field)],
                })
                data.text = _encode(record[field])
    return ET.tostring(root, encoding="unicode", xml_declaration=False) + "\n"
