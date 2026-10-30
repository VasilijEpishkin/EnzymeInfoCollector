"""Offline graph export checks; reference data are synthetic."""

from __future__ import annotations

from copy import deepcopy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from xml.etree import ElementTree as ET

from enzymeinfocollector.graph_cli import main
from enzymeinfocollector.graph_export import build_evidence_graph, cytoscape_json, graphml
from enzymeinfocollector.sources import DataSourceError


def fixture():
    return {
        "schema_version": "2.0-reference-preview",
        "semantic_sha256": "a" * 64,
        "proteins": [
            {
                "accession": "P00330",
                "reviewed": True,
                "entry_version": 45,
                "sequence_version": 2,
                "direct_assertions": [
                    {
                        "reaction": {
                            "rhea_id": "RHEA:10741", "master_id": "RHEA:10740",
                            "direction": "left_to_right",
                        },
                        "evidence": [
                            {"eco_code": "ECO:0000269", "citation_ids": ["PubMed:12345"]}
                        ],
                        "source_release": {
                            "database": "UniProtKB", "release": "2026_03",
                            "retrieved_at": "2026-10-30T08:00:00Z",
                        },
                        "annotation_id": None,
                    }
                ],
            },
            {
                "accession": "P00331",
                "reviewed": False,
                "entry_version": 12,
                "sequence_version": 2,
                # Same EC in the source dataset must NOT generate an edge.
                "ec": "1.1.1.1",
                "direct_assertions": [],
            },
        ],
        "reactions": [
            {
                "master_id": "RHEA:10740",
                "status": "rhea_record_found",
                "equation": "synthetic",
                "compound_membership": [
                    {
                        "reported_chebi_id": "CHEBI:5585",
                        "primary_chebi_id": "CHEBI:15377",
                        "side": "left",
                        "coefficient": "2",
                    },
                    {
                        "reported_chebi_id": "CHEBI:5585",
                        "primary_chebi_id": "CHEBI:15377",
                        "side": "right",
                        "coefficient": "1",
                    },
                ],
            },
        ],
        "compounds": [
            {
                "chebi_id": "CHEBI:15377", "name": "water",
                "formula": "H2O", "charge": 0,
            },
        ],
    }


class EvidenceGraphTests(unittest.TestCase):
    def test_only_direct_annotated_catalysis_becomes_an_edge(self):
        g = build_evidence_graph(fixture())
        links = [x for x in g["edges"] if x["type"] == "ANNOTATED_CATALYSIS"]
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0]["source"], "protein:P00330")
        self.assertEqual(links[0]["target"], "variant:RHEA:10741")
        self.assertEqual(links[0]["direction"], "left_to_right")
        self.assertEqual(links[0]["evidence"][0]["eco_code"], "ECO:0000269")
        self.assertFalse(any(x["source"] == "protein:P00331" for x in g["edges"]))
        self.assertEqual({x["type"] for x in g["nodes"]},
                         {"Protein", "ReactionVariant", "Reaction", "Compound"})

    def test_both_rhea_sides_remain_distinct_edges(self):
        g = build_evidence_graph(fixture())
        links = [x for x in g["edges"] if x["type"] == "HAS_PARTICIPANT"]
        self.assertEqual(len(links), 2)
        self.assertEqual(sorted((x["side"], x["coefficient"]) for x in links),
                         [("left", "2"), ("right", "1")])
        self.assertEqual({x["target"] for x in links}, {"compound:CHEBI:15377"})

    def test_protein_neighborhood_omits_unrelated_protein(self):
        graph = build_evidence_graph(fixture(), protein="P00330")
        self.assertFalse(any(x["id"] == "protein:P00331" for x in graph["nodes"]))
        with self.assertRaisesRegex(DataSourceError, "not present"):
            build_evidence_graph(fixture(), protein="P00918")

    def test_graphml_roundtrip_preserves_evidence_and_side_and_ids(self):
        graph = build_evidence_graph(fixture())
        text = graphml(graph)
        parsed = ET.fromstring(text)
        ns = {"g": "http://graphml.graphdrawing.org/xmlns"}
        keys = {x.get("id"): x.get("attr.name") for x in parsed.findall("g:key", ns)}
        edges = parsed.findall(".//g:edge", ns)
        self.assertEqual(len(edges), len(graph["edges"]))
        self.assertEqual({x.get("id") for x in parsed.findall(".//g:node", ns)},
                         {x["id"] for x in graph["nodes"]})
        self.assertFalse({x.get("id") for x in parsed.findall("g:key", ns)}
                         & {x.get("id") for x in edges})
        decoded = [{keys[x.attrib["key"]]: x.text for x in edge.findall("g:data", ns)}
                   for edge in edges]
        catalysis, = [x for x in decoded if x.get("type") == "ANNOTATED_CATALYSIS"]
        self.assertEqual(json.loads(catalysis["evidence"])[0]["eco_code"], "ECO:0000269")
        members = [x for x in decoded if x.get("type") == "HAS_PARTICIPANT"]
        self.assertEqual(sorted(x["side"] for x in members), ["left", "right"])

    def test_cytoscape_json_contains_complete_annotation(self):
        data = json.loads(cytoscape_json(build_evidence_graph(fixture())))
        self.assertIn("elements", data)
        edges = [x["data"] for x in data["elements"]["edges"]]
        self.assertEqual([x["evidence"] for x in edges if x["type"] == "ANNOTATED_CATALYSIS"][0],
                         fixture()["proteins"][0]["direct_assertions"][0]["evidence"])
        self.assertEqual(data["source_reference_sha256"], "a" * 64)

    def test_order_of_input_records_does_not_change_export(self):
        raw = fixture()
        reordered = deepcopy(raw)
        reordered["proteins"].reverse()
        reordered["reactions"][0]["compound_membership"].reverse()
        self.assertEqual(cytoscape_json(build_evidence_graph(raw)),
                         cytoscape_json(build_evidence_graph(reordered)))
        self.assertEqual(graphml(build_evidence_graph(raw)),
                         graphml(build_evidence_graph(reordered)))

    def test_invalid_or_incomplete_reference_fails_closed(self):
        broken = fixture()
        del broken["proteins"][0]["direct_assertions"][0]["evidence"]
        with self.assertRaises(DataSourceError):
            build_evidence_graph(broken)
        broken = fixture()
        broken["reactions"][0]["compound_membership"][0]["side"] = None
        with self.assertRaisesRegex(DataSourceError, "Partial"):
            build_evidence_graph(broken)
        broken = fixture()
        broken["compounds"] = []
        with self.assertRaisesRegex(DataSourceError, "lacks metadata"):
            build_evidence_graph(broken)
        broken = fixture()
        broken["schema_version"] = "1.0"
        with self.assertRaisesRegex(DataSourceError, "Expected v2"):
            build_evidence_graph(broken)

    def test_graph_cli_exports_graphml_and_atomic_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            src, dst = root / "reference.json", root / "graph.graphml"
            src.write_text(json.dumps(fixture()), encoding="utf-8")
            rc = main(["--input", str(src), "--format", "graphml", "--protein", "P00330",
                       "-o", str(dst)])
            self.assertEqual(rc, 0)
            self.assertIn("graphml", dst.read_text(encoding="utf-8"))
            before = dst.read_bytes()
            rc = main(["--input", str(src), "--format", "graphml", "--protein", "P00918",
                       "-o", str(dst)])
            self.assertEqual(rc, 1)
            self.assertEqual(dst.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
