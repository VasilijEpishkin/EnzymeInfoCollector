"""Offline graph projection and GraphML compatibility tests."""

from __future__ import annotations

from copy import deepcopy
import json
import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from enzymeinfocollector.graph_cli import main
from enzymeinfocollector.graph_export import (
    GRAPHML_NS, graph_json, project_graph, to_graphml,
)
from enzymeinfocollector.sources import DataSourceError


def reference():
    return {
        "schema_version": "2.0-reference-preview",
        "provenance": {
            "uniprot_release": "2026_03", "rhea_reaction_records": {
                "database": "Rhea", "sha256": "a" * 64,
            },
        },
        "proteins": [{
            "accession": "P04040", "reviewed": True,
            "entry_version": 10, "sequence_version": 2,
            "source_release": {"database": "UniProtKB", "release": "2026_03"},
            "direct_assertions": [
                {
                    "reaction": {
                        "master_id": "RHEA:20309",
                        "rhea_id": "RHEA:20309", "direction": "undefined",
                    },
                    "evidence": [{
                        "source_database": "UniProtKB",
                        "eco_code": "ECO:0000269",
                        "citation_ids": ["PubMed:1234567"],
                    }],
                    "annotation_id": None,
                    "source_release": {"database": "UniProtKB", "release": "2026_03"},
                },
                {
                    "reaction": {
                        "master_id": "RHEA:20309",
                        "rhea_id": "RHEA:20310", "direction": "left_to_right",
                    },
                    "evidence": [{"eco_code": None, "citation_ids": []}],
                    "source_release": {"database": "UniProtKB", "release": "2026_03"},
                },
            ],
        }],
        "reactions": [{
            "master_id": "RHEA:20309", "equation": "2 H2O2 = O2 + 2 H2O",
            "status": "rhea_record_found",
            "compound_membership": [
                {
                    "reported_chebi_id": "CHEBI:16240", "primary_chebi_id": "CHEBI:16240",
                    "resolution_status": "resolved", "side": "left", "coefficient": "2",
                },
                {
                    "reported_chebi_id": "CHEBI:15377", "primary_chebi_id": "CHEBI:15377",
                    "resolution_status": "resolved", "side": "right", "coefficient": "2",
                },
                {
                    "reported_chebi_id": "CHEBI:99999", "primary_chebi_id": None,
                    "resolution_status": "unresolved", "side": None, "coefficient": None,
                },
            ],
        }],
        "compounds": [
            {
                "chebi_id": "CHEBI:16240", "name": "hydrogen peroxide",
                "formula": "H2O2", "charge": 0, "source_release": {"database": "ChEBI"},
            },
            {
                "chebi_id": "CHEBI:15377", "name": "water",
                "formula": "H2O", "charge": 0, "source_release": {"database": "ChEBI"},
            },
        ],
        "unresolved": {
            "missing_rhea_master_ids": [],
            "unresolved_chebi_ids": ["CHEBI:99999"],
        },
    }


class EvidenceGraphTests(unittest.TestCase):
    def test_edges_are_direct_only_and_preserve_per_direction_evidence(self):
        graph = project_graph(reference())
        self.assertEqual(graph["schema_version"], "2.0-graph-preview")
        nodes = {item["data"]["id"]: item["data"] for item in graph["elements"]["nodes"]}
        edges = {item["data"]["id"]: item["data"] for item in graph["elements"]["edges"]}
        self.assertEqual(len(nodes), 5)  # one protein, reaction, 2 ChEBI and 1 unresolved
        self.assertEqual(len(edges), 5)  # two direct assertions, three participants
        catalysis = [x for x in edges.values() if x["relation"] == "CATALYZES"]
        self.assertEqual(len(catalysis), 2)
        self.assertEqual({x["rhea_directional_id"] for x in catalysis},
                         {"RHEA:20309", "RHEA:20310"})
        self.assertEqual(next(x for x in catalysis if x["rhea_directional_id"] == "RHEA:20309")
                         ["evidence"][0]["citation_ids"], ["PubMed:1234567"])
        self.assertEqual(nodes["compound:CHEBI:99999"]["node_type"], "UnresolvedCompound")
        self.assertEqual(edges["participant:RHEA:20309:0:CHEBI:16240"]["coefficient"], "2")
        self.assertEqual(edges["participant:RHEA:20309:0:CHEBI:16240"]["side"], "left")
        self.assertTrue(graph["semantics"]["unknown_is_not_negative"])
        self.assertEqual(graph["provenance"]["uniprot_release"], "2026_03")

    def test_graphml_roundtrip_preserves_types_and_citations(self):
        graph = project_graph(reference())
        xml_text = to_graphml(graph)
        root = ET.fromstring(xml_text)
        ns = {"g": GRAPHML_NS}
        nodes = root.findall(".//g:node", ns)
        edges = root.findall(".//g:edge", ns)
        self.assertEqual(len(nodes), 5)
        self.assertEqual(len(edges), 5)
        keys = {
            element.attrib["id"]: element.attrib.get("attr.name")
            for element in root.findall(".//g:key", ns)
        }
        self.assertEqual(keys["edge_evidence"], "evidence")
        cata = next(e for e in edges if e.attrib["id"] == "catalysis:P04040:RHEA:20309")
        edge_data = {
            keys[item.attrib["key"]]: item.text
            for item in cata.findall("g:data", ns)
        }
        self.assertEqual(json.loads(edge_data["evidence"])[0]["eco_code"], "ECO:0000269")
        self.assertEqual(json.loads(edge_data["evidence"])[0]["citation_ids"], ["PubMed:1234567"])
        self.assertEqual(cata.attrib["source"], "protein:P04040")
        self.assertEqual(cata.attrib["target"], "reaction:RHEA:20309")
        provenance = root.find(".//g:graph/g:data", ns)
        self.assertEqual(json.loads(provenance.text)["uniprot_release"], "2026_03")

    def test_graphml_preserves_scientific_semantics_and_reference_scope(self):
        from enzymeinfocollector.graph_export import select_protein_neighborhood
        item = reference()
        item["semantic_sha256"] = "a" * 64
        graph = select_protein_neighborhood(project_graph(item), "P04040")
        xml = ET.fromstring(to_graphml(graph))
        ns = {"g": GRAPHML_NS}
        keys = {
            element.attrib["id"]: element.attrib.get("attr.name")
            for element in xml.findall(".//g:key", ns)
        }
        values = {
            keys[element.attrib["key"]]: element.text
            for element in xml.findall(".//g:graph/g:data", ns)
        }
        self.assertEqual(values["schema_version"], "2.0-graph-preview")
        self.assertEqual(values["reference_sha256"], "a" * 64)
        self.assertEqual(json.loads(values["semantics"])["unknown_is_not_negative"], True)
        self.assertEqual(json.loads(values["scope"]), {"protein": "P04040"})

    def test_graph_json_stable_across_equivalent_record_order(self):
        one = reference()
        two = deepcopy(one)
        two["proteins"][0]["direct_assertions"].reverse()
        two["compounds"].reverse()
        self.assertEqual(graph_json(project_graph(one)), graph_json(project_graph(two)))

    def test_missing_direct_master_or_compound_not_silently_added(self):
        item = reference()
        item["proteins"][0]["direct_assertions"][0]["reaction"]["master_id"] = "RHEA:99999"
        with self.assertRaisesRegex(DataSourceError, "absent Rhea master"):
            project_graph(item)
        item = reference()
        item["reactions"][0]["compound_membership"][0]["primary_chebi_id"] = "CHEBI:77777"
        with self.assertRaisesRegex(DataSourceError, "lacks a compound record"):
            project_graph(item)

    def test_ec_does_not_generate_catalysis_edges(self):
        item = reference()
        item["proteins"][0]["direct_assertions"] = []
        item["proteins"][0]["ec_numbers"] = ["1.11.1.6"]
        item["reactions"][0]["ec_numbers"] = ["1.11.1.6"]
        edges = [x["data"] for x in project_graph(item)["elements"]["edges"]]
        self.assertFalse(any(x["relation"] == "CATALYZES" for x in edges))

    def test_cli_exports_both_formats(self):
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory, "verified.json")
            input_path.write_text(json.dumps(reference()), encoding="utf-8")
            graphml = Path(directory, "network.graphml")
            cytoscape = Path(directory, "network.json")
            self.assertEqual(main([str(input_path), "--format", "graphml", "-o", str(graphml)]), 0)
            self.assertEqual(main([str(input_path), "--format", "cytoscape-json", "-o", str(cytoscape)]), 0)
            self.assertEqual(len(ET.fromstring(graphml.read_text()).findall(
                ".//{http://graphml.graphdrawing.org/xmlns}edge"
            )), 5)
            self.assertEqual(len(json.loads(cytoscape.read_text())["elements"]["nodes"]), 5)

    def test_invalid_input_is_rejected(self):
        with self.assertRaisesRegex(DataSourceError, "v2 verified"):
            project_graph({"schema_version": "1", "proteins": [], "reactions": [], "compounds": []})
        for invalid in ("CHEBI:../../etc/passwd", "CHEBI:0"):
            item = reference()
            item["reactions"][0]["compound_membership"][0]["reported_chebi_id"] = invalid
            with self.subTest(invalid=invalid), self.assertRaisesRegex(DataSourceError, "ChEBI ID"):
                project_graph(item)


    def test_scoped_neighborhood_contains_only_selected_protein_and_related_chemistry(self):
        item = reference()
        other = deepcopy(item["proteins"][0])
        other["accession"] = "P00331"
        other["direct_assertions"] = []
        other["ec_numbers"] = ["1.11.1.6"]
        item["proteins"].append(other)
        full = project_graph(item)
        selected = project_graph(item, protein="P04040")
        self.assertEqual(len(full["elements"]["nodes"]), 6)
        self.assertEqual(len(selected["elements"]["nodes"]), 5)
        self.assertFalse(any(n["data"]["id"] == "protein:P00331"
                             for n in selected["elements"]["nodes"]))
        self.assertTrue(all(
            e["data"]["source"] != "protein:P00331"
            for e in selected["elements"]["edges"]
        ))
        self.assertEqual(selected["scope"], {"protein": "P04040"})
        self.assertEqual(selected["provenance"], full["provenance"])
        self.assertEqual(len(selected["elements"]["edges"]), 5)

        isolated = project_graph(item, protein="P00331")
        self.assertEqual([n["data"]["id"] for n in isolated["elements"]["nodes"]],
                         ["protein:P00331"])
        self.assertEqual(isolated["elements"]["edges"], [])

    def test_scoped_export_unknown_protein_and_cli_file_preservation(self):
        with self.assertRaisesRegex(DataSourceError, "absent"):
            project_graph(reference(), protein="P00001")
        with self.assertRaisesRegex(DataSourceError, "UniProt accession"):
            project_graph(reference(), protein="P04040>bad")
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory, "verified.json")
            output = Path(directory, "scoped.json")
            input_path.write_text(json.dumps(reference()), encoding="utf-8")
            rc = main([str(input_path), "--format", "cytoscape-json",
                       "--protein", "P04040", "-o", str(output)])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(output.read_text())["scope"], {"protein": "P04040"})
            original = output.read_bytes()
            rc = main([str(input_path), "--format", "cytoscape-json",
                       "--protein", "P00001", "-o", str(output)])
            self.assertEqual(rc, 1)
            self.assertEqual(output.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
