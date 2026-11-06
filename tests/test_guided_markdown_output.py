"""Offline smoke tests for one-command v2 Markdown reports.

Both commands reuse the existing evidence renderer and atomic writer;
no upstream API requests, score inference, or legacy CLI changes.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from enzymeinfocollector.discover_cli import main as discovery_main
from enzymeinfocollector.reference_cli import main as reference_main


def reference_fixture():
    source = {
        "database": "UniProtKB",
        "release": "synthetic",
        "retrieved_at": "2026-11-06T10:00:00Z",
        "sha256": "a" * 64,
    }
    return {
        "schema_version": "2.0-reference-preview",
        "semantic_sha256": "b" * 64,
        "provenance": {
            "uniprot_release": "synthetic",
            "discovery": {
                "selection": {
                    "selected": [{"accession": "P04040", "protein_name": "Catalase"}],
                    "truncated": True,
                },
                "without_direct_rhea_annotation": [],
            },
        },
        "proteins": [{
            "accession": "P04040",
            "reviewed": True,
            "taxon_id": 9606,
            "ec_numbers": ["1.11.1.6"],
            "source_release": source,
            "direct_assertions": [{
                "accession": "P04040",
                "reaction": {
                    "master_id": "RHEA:20309",
                    "rhea_id": "RHEA:20310",
                    "direction": "left_to_right",
                },
                "source_release": source,
                "annotation_id": "synthetic:1",
                "evidence": [{
                    "source_database": "UniProtKB",
                    "eco_code": "ECO:0000269",
                    "citation_ids": ["PubMed:12345"],
                }],
            }],
        }],
        "reactions": [{
            "master_id": "RHEA:20309",
            "equation": "2 H2O2 = 2 H2O + O2",
            "status": "rhea_record_found",
            "compound_membership": [],
        }],
        "compounds": [],
        "unresolved": {
            "missing_rhea_master_ids": [],
            "unresolved_chebi_ids": [],
        },
    }


class GuidedMarkdownTests(unittest.TestCase):
    def test_accession_to_markdown_without_intermediate_json(self):
        with tempfile.TemporaryDirectory() as temp, patch(
            "enzymeinfocollector.reference_cli.collect_live_reference",
            return_value=reference_fixture(),
        ) as collect:
            output = Path(temp) / "evidence.md"
            result = reference_main([
                "--accession", "P04040", "--format", "report-md",
                "-o", str(output),
            ])
            self.assertEqual(result, 0)
            report = output.read_text(encoding="utf-8")
            self.assertIn("#### RHEA:20309 / RHEA:20310", report)
            self.assertIn("ECO:0000269", report)
            self.assertIn("PubMed:12345", report)
            self.assertIn("Классификация EC (не связь катализа)", report)
            collect.assert_called_once()

    def test_ec_selection_to_markdown_without_intermediate_json(self):
        reference = reference_fixture()
        with tempfile.TemporaryDirectory() as temp, patch(
            "enzymeinfocollector.discover_cli.collect_discovered_graph",
            return_value=(reference, {"elements": {"nodes": [], "edges": []}}),
        ) as collect:
            output = Path(temp) / "evidence.md"
            result = discovery_main([
                "--ec", "1.11.1.6", "--taxon-id", "9606",
                "--select-accession", "P04040",
                "--expected-search-sha", "c" * 64,
                "--format", "report-md", "-o", str(output),
            ])
            self.assertEqual(result, 0)
            report = output.read_text(encoding="utf-8")
            self.assertIn("UniProt P04040", report)
            self.assertIn("Catalase", report)
            self.assertIn("UniProt-кандидатов ограничена", report)
            self.assertIn("RHEA:20310", report)
            self.assertEqual(collect.call_args.kwargs["select_accession"], "P04040")
            self.assertEqual(collect.call_args.kwargs["expected_search_sha"], "c" * 64)

    def test_failed_markdown_render_preserves_existing_files(self):
        invalid = deepcopy(reference_fixture())
        invalid["reactions"] = []  # Direct Rhea assertion must have its master record.
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "evidence.md"
            path.write_text("previous report", encoding="utf-8")
            with patch(
                "enzymeinfocollector.reference_cli.collect_live_reference",
                return_value=invalid,
            ):
                self.assertEqual(reference_main([
                    "--accession", "P04040", "--format", "report-md",
                    "-o", str(path),
                ]), 1)
            self.assertEqual(path.read_text(encoding="utf-8"), "previous report")
            with patch(
                "enzymeinfocollector.discover_cli.collect_discovered_graph",
                return_value=(invalid, {}),
            ):
                self.assertEqual(discovery_main([
                    "--ec", "1.11.1.6", "--format", "report-md",
                    "-o", str(path),
                ]), 1)
            self.assertEqual(path.read_text(encoding="utf-8"), "previous report")


if __name__ == "__main__":
    unittest.main()
