"""Offline phase-2 validation: directional evidence and exact Rhea chemistry."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import unittest

from benchmarks.validation_v1.phase2_evidence import compare as evidence_compare
from benchmarks.validation_v1.phase2_chemistry import compare as chemistry_compare


CASES = Path(__file__).resolve().parents[1] / "benchmarks" / "validation_v1" / "chemistry_cases.json"


class Phase2EvidenceTests(unittest.TestCase):
    def sample(self):
        source = {
            "primaryAccession": "P00558",
            "comments": [{"commentType": "CATALYTIC ACTIVITY",
                          "reaction": {
                              "reactionCrossReferences": [
                                  {"database": "Rhea", "id": "RHEA:14801"},
                                  {"database": "ChEBI", "id": "CHEBI:15377"}],
                              "evidences": [{"evidenceCode": "ECO:0000269",
                                             "source": "PubMed", "id": "123"}]},
                          "physiologicalReactions": [
                              {"directionType": "left-to-right",
                               "reactionCrossReference": {"database": "Rhea", "id": "RHEA:14802"},
                               "evidences": [{"evidenceCode": "ECO:0000305"}]},
                              {"directionType": "right-to-left",
                               "reactionCrossReference": {"database": "Rhea", "id": "RHEA:14803"}}]}]}
        ]
        raw = json.dumps(source).encode("utf-8")
        sha = hashlib.sha256(raw).hexdigest()
        def assertion(number, direction, evidence):
            return {"accession": "P00558",
                    "reaction": {"rhea_id": "RHEA:" + number, "master_id": "RHEA:14801",
                                 "direction": direction},
                    "evidence": evidence}
        doc = {"schema_version": "2.0-preview",
               "proteins": [{"accession": "P00558",
                             "source_release": {"sha256": sha},
                             "assertions": [
                                 assertion("14801", "undefined",
                                           [{"source_database": "UniProtKB", "eco_code": "ECO:0000269",
                                             "citation_ids": ["PubMed:123"]}]),
                                 assertion("14802", "left_to_right",
                                           [{"source_database": "UniProtKB", "eco_code": "ECO:0000305",
                                             "citation_ids": []}]),
                                 assertion("14803", "right_to_left",
                                           [{"source_database": "UniProtKB", "eco_code": None,
                                             "citation_ids": []}])
                             ]}]}
        return raw, doc, sha

    def test_matching_master_direction_ecos_citations_and_unknown(self):
        raw, doc, sha = self.sample()
        result = evidence_compare(raw, doc, sha, "P00558")
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["rhea_ids_checked"], 3)
        self.assertEqual(result["evidence_records_matched"], 3)

    def test_no_inherited_master_evidence_in_direction(self):
        raw, doc, sha = self.sample()
        doc["proteins"][0]["assertions"][2]["evidence"][0]["eco_code"] = "ECO:0000269"
        self.assertEqual(evidence_compare(raw, doc, sha, "P00558")["changed_ids"], ["RHEA:14803"])

    def test_source_drift_excluded_before_scoring(self):
        raw, doc, sha = self.sample()
        self.assertEqual(evidence_compare(raw + b" ", doc, sha, "P00558")["status"], "source_drift")

    def test_duplicate_assertions_rejected(self):
        raw, doc, sha = self.sample()
        doc["proteins"][0]["assertions"].append(copy.deepcopy(doc["proteins"][0]["assertions"][0]))
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            evidence_compare(raw, doc, sha, "P00558")


class Phase2ChemistryTests(unittest.TestCase):
    def test_official_reference_matrix(self):
        manifest = json.loads(CASES.read_text(encoding="utf-8"))
        self.assertEqual(len(manifest["cases"]), 5)
        self.assertEqual(sum(len(x["members"]) for x in manifest["cases"]), 27)
        self.assertEqual(len({m["master_id"] for x in manifest["cases"] for m in x["members"]}), 6)

    def sample(self):
        case = next(x for x in json.loads(CASES.read_text(encoding="utf-8"))["cases"]
                    if x["accession"] == "P04040")
        members = [{"reported_chebi_id": x["reported_chebi_id"],
                    "primary_chebi_id": x["reported_chebi_id"],
                    "resolution_status": "resolved",
                    "side": x["side"], "coefficient": x["coefficient"]}
                   for x in case["members"]]
        doc = {"schema_version": "2.0-reference-preview",
               "proteins": [{"accession": case["accession"]}],
               "provenance": {"rhea_participant_queries": [
                   {"sha256": case["expected_query_sha256"][0]}]},
               "reactions": [{"master_id": "RHEA:20309", "status": "rhea_record_found",
                              "compound_membership": members}]}
        return case, doc

    def test_exact_sides_stoichiometry_and_chebi(self):
        case, doc = self.sample()
        self.assertEqual(chemistry_compare(case, doc)["status"], "pass")

    def test_wrong_coefficient_is_reported(self):
        case, doc = self.sample()
        doc["reactions"][0]["compound_membership"][0]["coefficient"] = "1"
        self.assertEqual(chemistry_compare(case, doc)["status"], "mismatch")

    def test_changed_source_is_not_counted_as_chemistry_failure(self):
        case, doc = self.sample()
        doc["provenance"]["rhea_participant_queries"][0]["sha256"] = "a" * 64
        self.assertEqual(chemistry_compare(case, doc)["status"], "source_drift")

    def test_unresolved_chebi_cannot_claim_resolved_status(self):
        case, doc = self.sample()
        member = doc["reactions"][0]["compound_membership"][0]
        member["primary_chebi_id"] = None
        self.assertEqual(chemistry_compare(case, doc)["status"], "invalid_output")


if __name__ == "__main__":
    unittest.main()
