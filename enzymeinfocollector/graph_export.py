"""Evidence-preserving GraphML and Cytoscape JSON projections of v2 results.

No database, network or extra runtime dependencies. Graph edges are created
only from *direct* UniProt catalytic annotations, never shared EC values.
Rhea left/right equation sides are not labelled substrates/products.
"""

from __future__ import annotations

import json
import re
from typing import Any, Mapping
from xml.etree import ElementTree as ET

from .sources import DataSourceError

GRAPH_SCHEMA = "2.0-graph-preview"
GRAPHML_NS = "http://graphml.graphdrawing.org/xmlns"
_UNIPROT = re.compile(r"[A-Z0-9]+(?:-[0-9]+)?")
_RHEA = re.compile(r"RHEA:[1-9][0-9]*")
_CHEBI = re.compile(r"CHEBI:[1-9][0-9]*")


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DataSourceError(f"{label} must be an object")
    return value


def _rows(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise DataSourceError(f"{label} must be a list")
    return value


def _identifier(value: Any, pattern: re.Pattern[str], label: str) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise DataSourceError(f"Invalid {label}: {value!r}")
    return value


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def project_graph(reference: Mapping[str, Any], *, protein: str | None = None) -> dict[str, Any]:
    """Convert a verified reference result to typed nodes and directed edges.

    Contains only source-justified relationships: Protein CATALYZES Reaction
    (with individual directional Rhea ID and ECO/citations) and Reaction
    HAS_PARTICIPANT Compound (side/coefficient from Rhea RDF if known).
    Unresolved compound IDs get explicit placeholder nodes.
    """
    if not isinstance(reference, dict) or reference.get("schema_version") != "2.0-reference-preview":
        raise DataSourceError("Expected a v2 verified reference JSON result")
    provenance = _object(reference.get("provenance"), "provenance")
    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[str, dict[str, Any]] = {}

    def add_node(node: dict[str, Any]) -> None:
        identifier = node["id"]
        if identifier in nodes and nodes[identifier] != node:
            raise DataSourceError(f"Conflicting graph node {identifier}")
        nodes[identifier] = node

    def add_edge(edge: dict[str, Any]) -> None:
        identifier = edge["id"]
        if identifier in edges:
            raise DataSourceError(f"Duplicate graph edge {identifier}")
        if edge["source"] not in nodes or edge["target"] not in nodes:
            raise DataSourceError(f"Graph edge {identifier} has a missing endpoint")
        edges[identifier] = edge

    reactions = {}
    for item in _rows(reference.get("reactions"), "reactions"):
        row = _object(item, "reaction")
        master = _identifier(row.get("master_id"), _RHEA, "Rhea master ID")
        if master in reactions:
            raise DataSourceError(f"Duplicate Rhea master {master}")
        reactions[master] = row
        add_node({
            "id": f"reaction:{master}", "node_type": "Reaction",
            "label": master, "rhea_master_id": master,
            "equation": row.get("equation"), "status": row.get("status"),
        })

    compounds = {}
    for item in _rows(reference.get("compounds"), "compounds"):
        compound = _object(item, "compound")
        chebi = _identifier(compound.get("chebi_id"), _CHEBI, "ChEBI ID")
        if chebi in compounds:
            raise DataSourceError(f"Duplicate ChEBI ID {chebi}")
        compounds[chebi] = compound
        add_node({
            "id": f"compound:{chebi}", "node_type": "Compound",
            "label": compound.get("name") or chebi, "chebi_id": chebi,
            "formula": compound.get("formula"), "charge": compound.get("charge"),
            "source_release": compound.get("source_release"),
        })

    seen_proteins = set()
    pending_catalytic = []
    for item in _rows(reference.get("proteins"), "proteins"):
        protein_record = _object(item, "protein")
        accession = _identifier(protein_record.get("accession"), _UNIPROT, "UniProt accession")
        if accession in seen_proteins:
            raise DataSourceError(f"Duplicate protein {accession}")
        seen_proteins.add(accession)
        add_node({
            "id": f"protein:{accession}", "node_type": "Protein",
            "label": accession, "accession": accession,
            "reviewed": protein_record.get("reviewed"),
            "entry_version": protein_record.get("entry_version"),
            "sequence_version": protein_record.get("sequence_version"),
            "taxon_id": protein_record.get("taxon_id"),
            "organism_name": protein_record.get("organism_name"),
            "ec_numbers": protein_record.get("ec_numbers", []),
            "source_release": protein_record.get("source_release"),
        })
        for raw_assertion in _rows(protein_record.get("direct_assertions"), "direct_assertions"):
            assertion = _object(raw_assertion, "direct assertion")
            reaction = _object(assertion.get("reaction"), "assertion reaction")
            master = _identifier(reaction.get("master_id"), _RHEA, "asserted Rhea master")
            directed = _identifier(reaction.get("rhea_id"), _RHEA, "asserted Rhea direction")
            if master not in reactions:
                raise DataSourceError(f"Direct assertion references absent Rhea master {master}")
            pending_catalytic.append({
                "id": f"catalysis:{accession}:{directed}",
                "source": f"protein:{accession}",
                "target": f"reaction:{master}",
                "relation": "CATALYZES",
                "rhea_directional_id": directed,
                "direction": reaction.get("direction"),
                "evidence": _rows(assertion.get("evidence"), "assertion evidence"),
                "source_release": assertion.get("source_release"),
                "annotation_id": assertion.get("annotation_id"),
            })

    for edge in pending_catalytic:
        add_edge(edge)

    for master, row in sorted(reactions.items()):
        for index, raw_member in enumerate(_rows(row.get("compound_membership"), "compound_membership")):
            member = _object(raw_member, "compound membership")
            raw_id = _identifier(member.get("reported_chebi_id"), _CHEBI, "reported ChEBI ID")
            primary_id = member.get("primary_chebi_id")
            if primary_id is not None:
                primary_id = _identifier(primary_id, _CHEBI, "resolved ChEBI ID")
                if primary_id not in compounds:
                    raise DataSourceError(f"Resolved ChEBI ID {primary_id} lacks a compound record")
            target_id = primary_id or raw_id
            if primary_id is None:
                add_node({
                    "id": f"compound:{raw_id}", "node_type": "UnresolvedCompound",
                    "label": raw_id, "chebi_id": raw_id,
                    "formula": None, "charge": None, "source_release": None,
                })
            side = member.get("side")
            coefficient = member.get("coefficient")
            if side not in ("left", "right", None):
                raise DataSourceError(f"Invalid Rhea equation side for {master}")
            if coefficient is not None and (not isinstance(coefficient, str) or not coefficient):
                raise DataSourceError(f"Invalid Rhea coefficient for {master}")
            add_edge({
                "id": f"participant:{master}:{index}:{raw_id}",
                "source": f"reaction:{master}",
                "target": f"compound:{target_id}",
                "relation": "HAS_PARTICIPANT",
                "reported_chebi_id": raw_id,
                "primary_chebi_id": primary_id,
                "resolution_status": member.get("resolution_status"),
                "side": side,
                "coefficient": coefficient,
            })

    result = {
        "schema_version": GRAPH_SCHEMA,
        "semantics": {
            "catalysis": "direct UniProtKB annotation only, never EC join",
            "sides": "Rhea equation left/right; not automatically physiological substrates/products",
            "unknown_is_not_negative": True,
        },
        "provenance": provenance,
        "elements": {
            "nodes": [{"data": nodes[k]} for k in sorted(nodes)],
            "edges": [{"data": edges[k]} for k in sorted(edges)],
        },
    }
    return select_protein_neighborhood(result, protein) if protein is not None else result


def select_protein_neighborhood(graph: Mapping[str, Any], protein: str) -> dict[str, Any]:
    """Select one UniProt protein, its annotated reactions and their compounds.

    Filtering is by already-asserted edges; no new catalysis claims or EC
    joins are introduced. Original source provenance remains attached.
    """
    accession = _identifier(protein, _UNIPROT, "UniProt accession")
    root = "protein:" + accession
    elements = _object(graph.get("elements"), "graph elements")
    nodes = _rows(elements.get("nodes"), "nodes")
    edges = _rows(elements.get("edges"), "edges")
    if not any(_object(item.get("data"), "graph node").get("id") == root for item in nodes):
        raise DataSourceError(f"Protein {accession} is absent from the graph result")
    catalysis = [item for item in edges if item["data"].get("relation") == "CATALYZES"
                 and item["data"].get("source") == root]
    reaction_ids = {item["data"]["target"] for item in catalysis}
    participants = [item for item in edges if item["data"].get("relation") == "HAS_PARTICIPANT"
                    and item["data"].get("source") in reaction_ids]
    active = {root} | reaction_ids | {item["data"]["target"] for item in participants}
    return {
        **graph,
        "scope": {"protein": accession},
        "elements": {
            "nodes": [item for item in nodes if item["data"]["id"] in active],
            "edges": [item for item in edges if item in catalysis or item in participants],
        },
    }


def to_graphml(graph: Mapping[str, Any]) -> str:
    """GraphML with globally typed string attributes (complex values as JSON)."""
    elements = _object(graph.get("elements"), "graph elements")
    nodes = _rows(elements.get("nodes"), "nodes")
    edges = _rows(elements.get("edges"), "edges")

    ET.register_namespace("", GRAPHML_NS)
    def tag(name: str) -> str:
        return f"{{{GRAPHML_NS}}}{name}"
    root = ET.Element(tag("graphml"))
    for category, rows in (("node", nodes), ("edge", edges)):
        attributes = set()
        for row in rows:
            attributes.update(_object(row.get("data"), category).keys())
        attributes.difference_update({"id", "source", "target"})
        for key in sorted(attributes):
            ET.SubElement(root, tag("key"), {
                "id": f"{category}_{key}", "for": category,
                "attr.name": key, "attr.type": "string",
            })
    ET.SubElement(root, tag("key"), {
        "id": "graph_provenance", "for": "graph",
        "attr.name": "provenance", "attr.type": "string",
    })
    target = ET.SubElement(root, tag("graph"), {"id": "enzyme-evidence", "edgedefault": "directed"})
    ET.SubElement(target, tag("data"), {"key": "graph_provenance"}).text = _json(graph.get("provenance"))
    for category, rows in (("node", nodes), ("edge", edges)):
        for row in rows:
            data = _object(row.get("data"), category)
            attrib = {"id": data["id"]}
            if category == "edge":
                attrib.update({"source": data["source"], "target": data["target"]})
            element = ET.SubElement(target, tag(category), attrib)
            for key in sorted(data):
                if key in ("id", "source", "target"):
                    continue
                value = data[key]
                ET.SubElement(element, tag("data"), {"key": f"{category}_{key}"}).text = (
                    value if isinstance(value, str) else _json(value)
                )
    return ET.tostring(root, encoding="unicode", xml_declaration=False) + "\n"


def graph_json(graph: Mapping[str, Any]) -> str:
    return json.dumps(graph, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
