"""Offline guardrails for name/EC discovery workflow benchmark."""
from __future__ import annotations
import unittest
from benchmarks.validation_v1.phase3_discovery import classify_failure, score_reference


def case():
    return {"id": "catalase-ec", "input": {"ec": "1.11.1.6", "taxon_id": 9606,
            "reviewed_only": True, "max_proteins": 1}, "accession": "P04040",
            "expected": "linked", "truncated": False}


def gold():
    return {"P04040": {"uniprot_sha256": "a" * 64, "master_ids": ["RHEA:20309"]}}


def reference():
    return {
        "schema_version": "2.0-reference-preview",
        "semantics": {"ec_join": "prohibited"},
        "provenance": {"discovery": {
            "request": {"kind": "ec", "input": "1.11.1.6"},
            "selection": {"selected": [{"accession": "P04040", "taxon_id": 9606,
                                        "reviewed": True}],
                          "reviewed_only": True, "taxon_id": 9606, "max_proteins": 1,
                          "truncated": False, "ec": "1.11.1.6"},
            "direct_rhea_annotated": ["P04040"],
            "without_direct_rhea_annotation": [],
        }},
        "proteins": [{
            "accession": "P04040", "taxon_id": 9606, "reviewed": True,
            "source_release": {"sha256": "a" * 64},
            "direct_assertions": [{
                "accession": "P04040",
                "reaction": {"master_id": "RHEA:20309",
                             "rhea_id": "RHEA:20310", "direction": "left_to_right"},
                "evidence": [{"source_database": "UniProtKB", "eco_code": None}],
            }],
        }],
        "reactions": [{
            "master_id": "RHEA:20309", "equation": "synthetic",
            "status": "rhea_record_found", "compound_membership": [],
        }],
        "compounds": [],
    }


class Phase3DiscoveryTests(unittest.TestCase):
    def test_source_pinned_selected_protein_graph(self):
        self.assertEqual(score_reference(case(), reference(), gold())["status"], "pass")

    def test_name_and_ec_both_supported(self):
        scenario = case()
        scenario["input"].pop("ec")
        scenario["input"]["enzyme"] = "Catalase"
        data = reference()
        data["provenance"]["discovery"]["request"] = {
            "kind": "enzyme", "input": "Catalase", "ec": "1.11.1.6"}
        self.assertEqual(score_reference(scenario, data, gold())["status"], "pass")

    def test_changed_candidate_is_selection_drift(self):
        data = reference()
        data["provenance"]["discovery"]["selection"]["selected"][0]["accession"] = "P00330"
        self.assertEqual(score_reference(case(), data, gold())["status"], "selection_drift")

    def test_source_drift_not_scored(self):
        data = reference()
        data["proteins"][0]["source_release"]["sha256"] = "b" * 64
        self.assertEqual(score_reference(case(), data, gold())["status"], "source_drift")

    def test_false_ec_join_fails(self):
        data = reference()
        data["proteins"][0]["direct_assertions"][0]["reaction"]["master_id"] = "RHEA:99999"
        self.assertEqual(score_reference(case(), data, gold())["status"], "mismatch")

    def test_wrong_taxon_fails(self):
        data = reference()
        data["provenance"]["discovery"]["selection"]["selected"][0]["taxon_id"] = 10090
        self.assertEqual(score_reference(case(), data, gold())["status"], "invalid_output")

    def test_expected_no_direct_distinct_from_service_outage(self):
        scenario = {"id": "pig", "expected": "no_direct", "accession": "P00690"}
        good = classify_failure(scenario, 1,
            "No selected proteins had a direct UniProt→Rhea catalytic annotation; checked ['P00690']", False)
        self.assertEqual(good["status"], "pass")
        service = classify_failure(scenario, 1,
            "Evidence discovery failed: Request failed for rest.uniprot.org: HTTP Error 503", False)
        self.assertEqual(service["status"], "upstream_failure")
        self.assertEqual(classify_failure(scenario, 1, "No selected proteins had a direct UniProt", True)["status"],
                         "unexpected_error")


if __name__ == "__main__":
    unittest.main()
