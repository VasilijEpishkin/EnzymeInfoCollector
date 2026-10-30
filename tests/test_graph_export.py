"""Offline typed graph export contracts."""

import json
import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree

from enzymeinfocollector.graph_cli import main
from enzymeinfocollector.graph_export import (
    cytoscape_json, graph_from_reference, graphml_string,
)

def fixture():
    return {
        "schema_version": "2.0-reference-preview",
        "semantic_sha256": "x" * 64,
        "semantics": {"compound_membership": "Rhea RDF authoritative left/right equation sides and stoichiometry"},
        "provenance": {"uniprot_release": "synthetic"},
        "unresolved": {"unresolved_chebi_ids": []},
        "compounds": [{"chebi_id":"CHEBI:15377", "name":"water", "formula":"H2O", "charge":0, "source_release":{"database":"ChEBI"}}],
        "reactions": [{"master_id":"RHEA:10000","equation":"water ↔ water","status":"rhea_record_found",
                       "compound_membership":[{"reported_chebi_id":"CHEBI:15377","primary_chebi_id":"CHEBI:15377","side":"left","coefficient":"2.5"},
                                              {"reported_chebi_id":"CHEBI:15377","primary_chebi_id":"CHEBI:15377","side":"right","coefficient":"1"}]}],
        "proteins": [{"accession":"P00330","reviewed":True,"entry_version":7,"sequence_version":2,
                      "source_release":{"database":"UniProtKB"},"direct_assertions":[
            {"reaction":{"master_id":"RHEA:10000","rhea_id":"RHEA:10001","direction":"left_to_right"},
             "evidence":[{"eco_code":"ECO:0000269","citation_ids":["PubMed:123456"]}],
             "source_release":{"release":"synthetic"}}]}],
    }

class GraphExportTests(unittest.TestCase):
    def test_retains_evidence_and_separate_direction_and_stoichiometry(self):
        graph=graph_from_reference(fixture())
        self.assertEqual(len(graph["nodes"]), 3)
        self.assertEqual(len(graph["edges"]), 3)
        catalysis=[e for e in graph["edges"] if e["type"]=="CATALYZES"][0]
        self.assertEqual(catalysis["rhea_directional_id"],"RHEA:10001")
        self.assertEqual(catalysis["evidence"][0]["citation_ids"],["PubMed:123456"])
        participants=[e for e in graph["edges"] if e["type"]=="HAS_PARTICIPANT"]
        self.assertEqual({x["side"] for x in participants},{"left","right"})
        self.assertEqual({x["coefficient"] for x in participants},{"2.5","1"})

    def test_graphml_roundtrip_keeps_edge_data_and_types(self):
        graph=graph_from_reference(fixture())
        xml=graphml_string(graph)
        root=ElementTree.fromstring(xml)
        ns={"g":"http://graphml.graphdrawing.org/xmlns"}
        nodes=root.findall(".//g:node",ns)
        edges=root.findall(".//g:edge",ns)
        self.assertEqual((len(nodes),len(edges)),(3,3))
        records=[json.loads(edge.find("g:data[@key='k3']",ns).text) for edge in edges]
        self.assertEqual(records,graph["edges"])
        self.assertEqual(ElementTree.fromstring(xml).tag,"{http://graphml.graphdrawing.org/xmlns}graphml")

    def test_cytoscape_json_and_repeatability(self):
        a=cytoscape_json(graph_from_reference(fixture()))
        b=cytoscape_json(graph_from_reference(fixture()))
        self.assertEqual(a,b)
        self.assertEqual(a["metadata"]["reference_sha256"],"x"*64)
        self.assertEqual(len(a["elements"]["nodes"]),3)
        self.assertEqual(len(a["elements"]["edges"]),3)

    def test_rejects_legacy_flat_membership(self):
        data=fixture()
        data["semantics"]["compound_membership"]="Rhea flat ChEBI list; no substrate side or stoichiometry"
        with self.assertRaisesRegex(ValueError,"authoritative"):
            graph_from_reference(data)

    def test_rejects_dangling_direct_assertions(self):
        data=fixture()
        data["reactions"]=[]
        with self.assertRaisesRegex(ValueError,"missing Rhea master"):
            graph_from_reference(data)

    def test_cli_writes_json_and_failure_is_atomic(self):
        with tempfile.TemporaryDirectory() as temp:
            inp=Path(temp)/"ref.json";out=Path(temp)/"graph.json"
            inp.write_text(json.dumps(fixture()),encoding="utf-8")
            self.assertEqual(main([str(inp),"--format","cytoscape-json","-o",str(out)]),0)
            self.assertEqual(len(json.loads(out.read_text())["elements"]["edges"]),3)
            inp.write_text("{}",encoding="utf-8")
            self.assertEqual(main([str(inp),"--format","cytoscape-json","-o",str(out)]),1)
            self.assertEqual(len(json.loads(out.read_text())["elements"]["edges"]),3)

if __name__=="__main__":
    unittest.main()
