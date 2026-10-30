"""Typed graph projections of the verified Protein–Rhea–ChEBI JSON.

Graph edges are derived from direct UniProt catalytic annotations and the
verified Rhea participant list. Never infer catalysis from an EC match.
This module is stdlib-only, offline, and does not mutate its input.
"""

from __future__ import annotations

import json
from typing import Any, Mapping
from xml.etree.ElementTree import Element, SubElement, tostring

GRAPH_VERSION = "1.0"


def _required_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _required_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    return value


def _required_id(value: Any, prefix: str) -> str:
    if not isinstance(value, str) or not value.startswith(prefix) or not value[len(prefix):]:
        raise ValueError(f"Invalid ID: {value!r}")
    return value


def graph_from_reference(document: Mapping[str, Any]) -> dict[str, Any]:
    """Return a deterministic typed graph; refuse incomplete chemical sides.

    Multiple directional assertions remain separate. Evidence is retained in
    edge attributes as JSON-compatible nested objects. A graph of unsupported
    legacy flat ChEBI membership is rejected, not upgraded to authoritative
    stoichiometry.
    """
    if not isinstance(document, dict) or document.get("schema_version") != "2.0-reference-preview":
        raise ValueError("Unsupported reference schema")
    semantics = _required_dict(document.get("semantics"), "semantics")
    if "authoritative" not in str(semantics.get("compound_membership", "")).lower():
        raise ValueError("Graph export requires authoritative Rhea sides and stoichiometry")
    proteins = _required_list(document.get("proteins"), "proteins")
    reactions = _required_list(document.get("reactions"), "reactions")
    compounds = _required_list(document.get("compounds"), "compounds")
    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[str, dict[str, Any]] = {}

    def node(identifier: str, kind: str, **fields: Any) -> None:
        value = {"id": identifier, "type": kind, **fields}
        existing = nodes.get(identifier)
        if existing is not None and existing != value:
            raise ValueError(f"Conflicting graph node {identifier}")
        nodes[identifier] = value

    def edge(identifier: str, source: str, target: str, relation: str, **fields: Any) -> None:
        if identifier in edges:
            raise ValueError(f"Duplicate graph edge {identifier}")
        edges[identifier] = {
            "id": identifier, "source": source, "target": target, "type": relation, **fields
        }

    for row in compounds:
        item = _required_dict(row, "compound")
        chebi = _required_id(item.get("chebi_id"), "CHEBI:")
        node(f"compound:{chebi}", "Compound", chebi_id=chebi,
             name=item.get("name"), formula=item.get("formula"),
             charge=item.get("charge"), source_release=item.get("source_release"))

    for row in reactions:
        item = _required_dict(row, "reaction")
        master = _required_id(item.get("master_id"), "RHEA:")
        node(f"reaction:{master}", "Reaction", master_id=master,
             equation=item.get("equation"), status=item.get("status"))
        for i, membership in enumerate(_required_list(item.get("compound_membership"), "memberships")):
            part = _required_dict(membership, "membership")
            primary = part.get("primary_chebi_id")
            reported = _required_id(part.get("reported_chebi_id"), "CHEBI:")
            if primary is None:
                continue  # unresolved identities remain in the audit manifest, never fabricate a node
            primary = _required_id(primary, "CHEBI:")
            target = f"compound:{primary}"
            if target not in nodes:
                raise ValueError(f"Missing compound for {primary}")
            side, coefficient = part.get("side"), part.get("coefficient")
            if side not in ("left", "right") or not isinstance(coefficient, str) or not coefficient:
                raise ValueError(f"Invalid authoritative participant {master} / {reported}")
            edge(f"participant:{master}:{i}", f"reaction:{master}", target, "HAS_PARTICIPANT",
                 reported_chebi_id=reported, side=side, coefficient=coefficient)

    for row in proteins:
        item = _required_dict(row, "protein")
        accession = item.get("accession")
        if not isinstance(accession, str) or not accession:
            raise ValueError("Invalid UniProt accession")
        source = f"protein:{accession}"
        node(source, "Protein", accession=accession, reviewed=item.get("reviewed"),
             entry_version=item.get("entry_version"), sequence_version=item.get("sequence_version"),
             source_release=item.get("source_release"))
        for i, assertion in enumerate(_required_list(item.get("direct_assertions"), "direct_assertions")):
            assertion = _required_dict(assertion, "assertion")
            reaction = _required_dict(assertion.get("reaction"), "reaction variant")
            master = _required_id(reaction.get("master_id"), "RHEA:")
            direction_id = _required_id(reaction.get("rhea_id"), "RHEA:")
            target = f"reaction:{master}"
            if target not in nodes:
                raise ValueError(f"Direct assertion points to missing Rhea master {master}")
            edge(f"catalyzes:{accession}:{direction_id}:{i}", source, target, "CATALYZES",
                 rhea_directional_id=direction_id, direction=reaction.get("direction"),
                 evidence=_required_list(assertion.get("evidence"), "evidence"),
                 source_release=assertion.get("source_release"))
    graph = {
        "graph_version": GRAPH_VERSION,
        "source_schema": document["schema_version"],
        "reference_sha256": document.get("semantic_sha256"),
        "provenance": document.get("provenance"),
        "unresolved": document.get("unresolved"),
        "nodes": [nodes[k] for k in sorted(nodes)],
        "edges": [edges[k] for k in sorted(edges)],
    }
    return graph


def cytoscape_json(graph: Mapping[str, Any]) -> dict[str, Any]:
    """Cytoscape.js elements; full typed edge attributes remain inspectable."""
    return {
        "metadata": {k: graph.get(k) for k in (
            "graph_version", "source_schema", "reference_sha256", "provenance", "unresolved"
        )},
        "elements": {
            "nodes": [{"data": x} for x in graph["nodes"]],
            "edges": [{"data": x} for x in graph["edges"]],
        },
    }


def graphml_string(graph: Mapping[str, Any]) -> str:
    """XML GraphML, including canonical JSON for non-scalar metadata fields."""
    namespace = "http://graphml.graphdrawing.org/xmlns"
    root = Element("graphml", {"xmlns": namespace})
    keys = (
        ("node", "kind", "string"),
        ("node", "attributes_json", "string"),
        ("edge", "relation", "string"),
        ("edge", "attributes_json", "string"),
    )
    for i, (scope, name, attr_type) in enumerate(keys):
        SubElement(root, "key", {"id": f"k{i}", "for": scope,
                                   "attr.name": name, "attr.type": attr_type})
    graph_node = SubElement(root, "graph", {"id": "evidence", "edgedefault": "directed"})
    for item in graph["nodes"]:
        element = SubElement(graph_node, "node", {"id": item["id"]})
        SubElement(element, "data", {"key": "k0"}).text = item["type"]
        SubElement(element, "data", {"key": "k1"}).text = json.dumps(
            item, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
    for item in graph["edges"]:
        element = SubElement(graph_node, "edge", {
            "id": item["id"], "source": item["source"], "target": item["target"]
        })
        SubElement(element, "data", {"key": "k2"}).text = item["type"]
        SubElement(element, "data", {"key": "k3"}).text = json.dumps(
            item, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode") + "\n"
