"""Network-free regression tests for a human-readable evidence report."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from enzymeinfocollector.report_cli import render_report, main
from enzymeinfocollector.sources import DataSourceError


def fixture():
    source = {
        "database": "Synthetic", "release": "fixture-only",
        "retrieved_at": "2026-11-06T00:00:00Z", "sha256": "a" * 64,
    }
    return {
        "schema_version": "2.0-reference-preview",
        "semantic_sha256": "b" * 64,
        "provenance": {
            "uniprot_release": "synthetic",
            "rhea_directions": source,
            "rhea_reaction_records": source,
            "chebi_snapshots": [source],
            "attribution": {"Synthetic": "fixture only"},
            "discovery": {
                "selection": {"selected": "P04040", "truncated": True},
                "without_direct_rhea_annotation": ["P00002"],
            },
        },
        "proteins": [
            {"accession": "P00002", "reviewed": False, "ec_numbers": ["1.11.1.6"],
             "source_release": source, "direct_assertions": []},
            {"accession": "P04040", "reviewed": True, "taxon_id": 9606,
             "organism_name": "Homo sapiens", "ec_numbers": ["1.11.1.6"],
             "entry_version": 4, "sequence_version": 1, "source_release": source,
             "direct_assertions": [
                 {"reaction": {"master_id": "RHEA:10000", "rhea_id": "RHEA:10001",
                               "direction": "left_to_right"},
                  "annotation_id": "synthetic:one", "source_release": source,
                  "evidence": [{"source_database": "Synthetic",
                                "eco_code": "ECO:0000269", "assertion_method": "manual",
                                "citation_ids": ["PubMed:111"]}]},
                 {"reaction": {"master_id": "RHEA:10000", "rhea_id": "RHEA:10002",
                               "direction": "right_to_left"},
                  "annotation_id": "synthetic:two", "source_release": source,
                  "evidence": [{"source_database": "Synthetic",
                                "eco_code": None, "citation_ids": ["PubMed:222"]}]},
             ]},
        ],
        "reactions": [{
            "master_id": "RHEA:10000", "status": "rhea_record_found",
            "equation": "2 H2O2 = 2 H2O + O2",
            "compound_membership": [
                {"reported_chebi_id": "CHEBI:16240", "primary_chebi_id": "CHEBI:16240",
                 "resolution_status": "resolved", "side": "left", "coefficient": "2"},
                {"reported_chebi_id": "CHEBI:15377", "primary_chebi_id": "CHEBI:15377",
                 "resolution_status": "resolved", "side": "right", "coefficient": "2"},
                {"reported_chebi_id": "CHEBI:99999", "primary_chebi_id": None,
                 "resolution_status": "unresolved", "side": "right", "coefficient": "1"},
            ],
        }],
        "compounds": [
            {"chebi_id": "CHEBI:15377", "name": "water", "source_release": source},
            {"chebi_id": "CHEBI:16240", "name": "hydrogen peroxide", "source_release": source},
        ],
        "unresolved": {
            "missing_rhea_master_ids": [],
            "unresolved_chebi_ids": ["CHEBI:99999"],
        },
    }


class ReportTests(unittest.TestCase):
    def test_directional_evidence_does_not_leak_between_assertions(self):
        report = render_report(fixture(), accession="P04040")
        assert "2 H2O2 = 2 H2O + O2" in report
        one = report.split("#### RHEA:10000 / RHEA:10001")[1].split("####")[0]
        two = report.split("#### RHEA:10000 / RHEA:10002")[1].split("## Неопределённость")[0]
        self.assertIn("ECO:0000269", one)
        self.assertIn("PubMed:111", one)
        self.assertNotIn("PubMed:222", one)
        self.assertIn("PubMed:222", two)
        self.assertNotIn("PubMed:111", two)
        self.assertNotIn("ECO:0000269", two)
        self.assertIn("CHEBI:99999", report)
        self.assertIn("| left | 2 |", report)
        self.assertIn("| right | 2 |", report)

    def test_unknown_and_truncation_are_not_negative_activity(self):
        report = render_report(fixture())
        self.assertIn("UniProt-кандидатов ограничена", report)
        self.assertIn("Прямые UniProt–Rhea аннотации в этом результате отсутствуют", report)
        self.assertIn("не отрицательное доказательство", report)
        self.assertIn("SHA-256 HTTP-ответа", report)

    def test_determinism_under_record_reordering(self):
        original = fixture()
        reordered = deepcopy(original)
        reordered["proteins"].reverse()
        reordered["proteins"][0]["direct_assertions"].reverse()
        reordered["compounds"].reverse()
        reordered["reactions"][0]["compound_membership"].reverse()
        reordered["provenance"]["chebi_snapshots"].reverse()
        self.assertEqual(render_report(original), render_report(reordered))

    def test_untrusted_markdown_is_escaped(self):
        data = fixture()
        data["compounds"][0]["name"] = "<script>alert(1)</script>| [link](javascript:evil)"
        report = render_report(data)
        self.assertNotIn("<script>", report)
        self.assertNotIn("| [link](javascript:evil)", report)
        self.assertIn("&lt;script", report)
        self.assertIn(r"\|", report)

    def test_missing_reaction_fails_closed(self):
        data = fixture()
        data["reactions"].clear()
        with self.assertRaisesRegex(DataSourceError, "Missing Rhea master"):
            render_report(data)

    def test_unknown_accession_fails(self):
        with self.assertRaisesRegex(DataSourceError, "not in reference"):
            render_report(fixture(), accession="Z99999")

    def test_cli_atomic_write_and_preserves_file_on_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            inp = Path(tmp) / "reference.json"
            out = Path(tmp) / "report.md"
            inp.write_text(json.dumps(fixture()), encoding="utf-8")
            self.assertEqual(main([str(inp), "--accession", "P04040", "-o", str(out)]), 0)
            self.assertIn("P04040", out.read_text(encoding="utf-8"))
            out.write_text("existing", encoding="utf-8")
            inp.write_text('{"schema_version": "wrong"}', encoding="utf-8")
            self.assertEqual(main([str(inp), "-o", str(out)]), 1)
            self.assertEqual(out.read_text(encoding="utf-8"), "existing")


if __name__ == "__main__":
    unittest.main()
