"""Offline regression tests for the v2 human-readable evidence report."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from enzymeinfocollector.evidence_report import build_evidence_report
from enzymeinfocollector.report_cli import main
from enzymeinfocollector.sources import DataSourceError


def sample():
    return {
        "schema_version": "2.0-reference-preview",
        "provenance": {
            "uniprot_release": "2026_03",
            "rhea_reaction_records": {"database": "Rhea", "sha256": "a" * 64},
            "discovery": {
                "request": {"input": "1.11.1.6", "ec": "1.11.1.6"},
                "selection": {
                    "truncated": True, "selection_mode": "explicit_accession",
                    "selected": [{"accession": "P04040",
                                  "protein_name": "Catalase <img onerror=evil>",
                                  "gene_names": "CAT"}],
                    "source": {"sha256": "c" * 64},
                },
                "without_direct_rhea_annotation": ["P00002"],
            },
        },
        "proteins": [
            {"accession": "P00002", "reviewed": False, "taxon_id": 9606,
             "ec_numbers": ["1.11.1.6"], "direct_assertions": []},
            {"accession": "P04040", "reviewed": True, "taxon_id": 9606,
             "ec_numbers": ["1.11.1.6"],
             "source_release": {"database": "UniProtKB", "release": "2026_03"},
             "direct_assertions": [
                 {"reaction": {"master_id": "RHEA:20309", "rhea_id": "RHEA:20310",
                               "direction": "left_to_right"},
                  "evidence": [{"source_database": "UniProtKB", "eco_code": None,
                                "citation_ids": []}],
                  "source_release": {"database": "UniProtKB", "release": "2026_03"}},
                 {"reaction": {"master_id": "RHEA:20309", "rhea_id": "RHEA:20309",
                               "direction": "undefined"},
                  "evidence": [{"source_database": "UniProtKB",
                                "eco_code": "ECO:0000269", "citation_ids": ["PubMed:1234567"]}],
                  "source_release": {"database": "UniProtKB", "release": "2026_03"}},
             ]},
        ],
        "reactions": [{
            "master_id": "RHEA:20309", "equation": "2 H2O2 = 2 H2O + O2",
            "status": "rhea_record_found",
            "compound_membership": [
                {"reported_chebi_id": "CHEBI:16240", "primary_chebi_id": "CHEBI:16240",
                 "resolution_status": "resolved", "side": "left", "coefficient": "2"},
                {"reported_chebi_id": "CHEBI:15377", "primary_chebi_id": "CHEBI:15377",
                 "resolution_status": "resolved", "side": "right", "coefficient": "2"},
                {"reported_chebi_id": "CHEBI:25805", "primary_chebi_id": "CHEBI:25805",
                 "resolution_status": "resolved", "side": "right", "coefficient": "1"},
                {"reported_chebi_id": "CHEBI:99999", "primary_chebi_id": None,
                 "resolution_status": "unresolved", "side": None, "coefficient": None},
            ],
        }],
        "compounds": [
            {"chebi_id": "CHEBI:15377", "name": "water", "formula": "H2O"},
            {"chebi_id": "CHEBI:16240", "name": "hydrogen peroxide", "formula": "H2O2"},
            {"chebi_id": "CHEBI:25805", "name": "oxygen", "formula": "O2"},
        ],
        "unresolved": {"missing_rhea_master_ids": [],
                       "unresolved_chebi_ids": ["CHEBI:99999"]},
        "semantic_sha256": "b" * 64,
    }


class EvidenceReportTests(unittest.TestCase):
    def test_report_preserves_actual_catalase_equation_and_stoichiometry(self):
        text = build_evidence_report(sample())
        self.assertIn("2 H2O2 = 2 H2O + O2", text)
        self.assertIn("left | 2 | CHEBI:16240 | CHEBI:16240", text)
        self.assertIn("right | 2 | CHEBI:15377 | CHEBI:15377", text)
        self.assertIn("right | 1 | CHEBI:25805 | CHEBI:25805", text)
        self.assertIn("физиологические субстраты/продукты", text)

    def test_evidence_remains_on_its_own_direction_only(self):
        text = build_evidence_report(sample())
        left = text.split("#### RHEA:20309 · направленный RHEA:20309")[1].split("####")[0]
        right = text.split("#### RHEA:20309 · направленный RHEA:20310")[1].split("## ")[0]
        self.assertIn("PubMed:1234567", left)
        self.assertIn("ECO:0000269", left)
        self.assertNotIn("PubMed:1234567", right)
        self.assertIn("ECO: не указано", right)

    def test_truncation_unknown_and_unresolved_reported(self):
        text = build_evidence_report(sample())
        self.assertIn("Выдача UniProt усечена", text)
        self.assertIn("не отрицательный результат", text)
        self.assertIn("это не доказательство отсутствия активности", text)
        self.assertIn("CHEBI:99999", text)

    def test_deterministic_under_reordering_and_safe_markdown(self):
        original = sample()
        result = build_evidence_report(original)
        self.assertNotIn("<img", result)
        self.assertIn(r"\<img", result)
        reverse = deepcopy(original)
        reverse["proteins"].reverse()
        reverse["reactions"][0]["compound_membership"].reverse()
        reverse["compounds"].reverse()
        reverse["proteins"][0]["direct_assertions"].reverse()
        self.assertEqual(result, build_evidence_report(reverse))

    def test_bad_schema_and_missing_asserted_reaction_fail_closed(self):
        invalid = sample()
        invalid["schema_version"] = "1"
        with self.assertRaisesRegex(DataSourceError, "2.0-reference-preview"):
            build_evidence_report(invalid)
        invalid = sample()
        invalid["proteins"][1]["direct_assertions"][0]["reaction"]["master_id"] = "RHEA:99999"
        with self.assertRaisesRegex(DataSourceError, "missing Rhea record"):
            build_evidence_report(invalid)

    def test_cli_atomic_write_and_no_overwrite_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "input.json"
            output_path = Path(directory) / "report.md"
            input_path.write_text(json.dumps(sample()), encoding="utf-8")
            self.assertEqual(main([str(input_path), "-o", str(output_path)]), 0)
            self.assertIn("RHEA:20310", output_path.read_text(encoding="utf-8"))
            output_path.write_text("previous", encoding="utf-8")
            input_path.write_text('{"schema_version":"invalid"}', encoding="utf-8")
            self.assertEqual(main([str(input_path), "-o", str(output_path)]), 1)
            self.assertEqual(output_path.read_text(encoding="utf-8"), "previous")


if __name__ == "__main__":
    unittest.main()
