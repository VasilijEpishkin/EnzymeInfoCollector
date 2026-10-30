"""Offline regression: scientific metadata flows UniProt→v2 reference→graph.

Taxonomy/EC classification must never manufacture catalytic assertions.
"""

from __future__ import annotations

from dataclasses import replace
import unittest

from enzymeinfocollector.domain import SourceRelease
from enzymeinfocollector.evidence_sources import (
    parse_rhea_directions, parse_uniprot_catalytic_activities,
)
from enzymeinfocollector.graph_export import project_graph
from enzymeinfocollector.reference_dataset import build_reference_dataset


class TaxonECTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.release = SourceRelease("UniProtKB", "synthetic", "2026-10-30T08:00:00Z")
        tsv = (
            "RHEA_ID_MASTER\tRHEA_ID_LR\tRHEA_ID_RL\tRHEA_ID_BI\n"
            "20309\t20310\t20311\t20312\n"
        )
        cls.directions = parse_rhea_directions(tsv, cls.release)

    @staticmethod
    def entry():
        return {
            "primaryAccession": "P04040",
            "entryType": "UniProtKB reviewed (Swiss-Prot)",
            "entryAudit": {"entryVersion": 101, "sequenceVersion": 3},
            "organism": {"taxonId": 9606, "scientificName": "Homo sapiens"},
            "proteinDescription": {
                "recommendedName": {
                    "fullName": {"value": "Catalase"},
                    "ecNumbers": [{"value": "1.11.1.6"}],
                },
                "alternativeNames": [{
                    "fullName": {"value": "Peroxidase"},
                    "ecNumbers": [{"value": "1.11.1.6"}, {"value": "1.11.1.n1"}],
                }],
            },
            "comments": [{
                "commentType": "CATALYTIC ACTIVITY",
                "reaction": {"reactionCrossReferences": [
                    {"database": "Rhea", "id": "RHEA:20309"}
                ]},
            }],
        }

    def test_uniprot_metadata_is_parsed_without_creating_ec_edges(self):
        record = parse_uniprot_catalytic_activities(
            self.entry(), self.release, self.directions
        )
        self.assertEqual(record.taxon_id, 9606)
        self.assertEqual(record.organism_name, "Homo sapiens")
        self.assertEqual(record.ec_numbers, ("1.11.1.6", "1.11.1.n1"))
        self.assertEqual(len(record.assertions), 1)

    def test_missing_metadata_remains_unknown(self):
        entry = self.entry()
        entry.pop("organism")
        entry.pop("proteinDescription")
        record = parse_uniprot_catalytic_activities(entry, self.release, self.directions)
        self.assertIsNone(record.taxon_id)
        self.assertIsNone(record.organism_name)
        self.assertEqual(record.ec_numbers, ())

    def test_malformed_metadata_fails_closed(self):
        from enzymeinfocollector.sources import DataSourceError
        for field, value in (
            ("organism", {"taxonId": "9606", "scientificName": "Human"}),
            ("organism", {"taxonId": True, "scientificName": "Human"}),
            ("proteinDescription", {"recommendedName": {"ecNumbers": "1.11.1.6"}}),
            ("proteinDescription", {"recommendedName": {"ecNumbers": [{"value": "unsafe"}]}}),
        ):
            entry = self.entry()
            entry[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(DataSourceError):
                parse_uniprot_catalytic_activities(entry, self.release, self.directions)

    def test_unsupported_entry_type_is_not_silently_treated_as_unreviewed(self):
        from enzymeinfocollector.sources import DataSourceError
        entry = self.entry()
        entry["entryType"] = "Unrecognized UniProtKB entryType"
        with self.assertRaisesRegex(DataSourceError, "unsupported entryType"):
            parse_uniprot_catalytic_activities(entry, self.release, self.directions)

    def test_classification_does_not_create_catalytic_assertions(self):
        entry = self.entry()
        entry["comments"] = []
        record = parse_uniprot_catalytic_activities(entry, self.release, self.directions)
        self.assertEqual(record.ec_numbers, ("1.11.1.6", "1.11.1.n1"))
        self.assertEqual(record.assertions, ())

    def test_graph_records_keep_metadata_without_inferring_reactions(self):
        reference = {
            "schema_version": "2.0-reference-preview",
            "provenance": {"uniprot_release": "synthetic"},
            "proteins": [{
                "accession": "P04040", "reviewed": True,
                "entry_version": 101, "sequence_version": 3,
                "taxon_id": 9606, "organism_name": "Homo sapiens",
                "ec_numbers": ["1.11.1.6"],
                "source_release": {"database": "UniProtKB"},
                "direct_assertions": [],
            }],
            "reactions": [], "compounds": [],
        }
        graph = project_graph(reference)
        self.assertEqual(len(graph["elements"]["edges"]), 0)
        protein = graph["elements"]["nodes"][0]["data"]
        self.assertEqual(protein["taxon_id"], 9606)
        self.assertEqual(protein["organism_name"], "Homo sapiens")
        self.assertEqual(protein["ec_numbers"], ["1.11.1.6"])


if __name__ == "__main__":
    unittest.main()
