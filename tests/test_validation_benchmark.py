"""Offline validation of the benchmark itself (not live biological validation)."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from benchmarks.validation_v1.compare import evaluate

MANIFEST = Path(__file__).resolve().parents[1] / "benchmarks" / "validation_v1" / "cases.json"


class ValidationBenchmarkTests(unittest.TestCase):
    def test_official_matrix_has_seven_ec_classes_and_unverified_trembl(self):
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        cases = manifest["cases"]
        self.assertEqual(len(cases), 20)
        self.assertEqual(len({x["accession"] for x in cases}), 20)
        self.assertEqual({str(i) for i in range(1, 8)},
                         {x.split(".")[0] for c in cases for x in c["ec"]})
        self.assertEqual(sum(c["master_ids"] is not None for c in cases), 19)
        self.assertEqual(sum(c["reviewed"] is False for c in cases), 1)
        self.assertEqual(sum(c["master_ids"] == [] for c in cases), 5)

    def test_missing_runs_never_generate_quality_metrics(self):
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            result = evaluate(manifest, Path(directory))
        self.assertEqual(result["summary"]["scored"], 0)
        self.assertIsNone(result["summary"]["source_agreement_precision"])
        self.assertIsNone(result["summary"]["source_agreement_recall"])
        self.assertEqual(result["summary"]["unverified_gold"], 1)

    def test_source_consistency_tp_fp_fn_and_drift(self):
        sha = "a" * 64
        gold = {"schema": "enzymeinfo-uniprot-direct-benchmark-v1", "cases": [
            {"accession": "P04040", "master_ids": ["RHEA:20309", "RHEA:14801"],
             "taxon_id": 9606, "reviewed": True, "uniprot_sha256": sha}
        ]}
        def doc(source_sha, ids):
            return {"schema_version": "2.0-preview", "unknown_is_not_negative": True,
                    "proteins": [{"accession": "P04040", "taxon_id": 9606,
                                  "reviewed": True, "source_release": {"sha256": source_sha},
                                  "assertions": [
                                      {"accession": "P04040", "reaction": {
                                          "rhea_id": x, "master_id": x, "direction": "undefined"},
                                       "evidence": [{"source_database": "UniProtKB"}]}
                                      for x in ids
                                  ]}]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "P04040.json"
            path.write_text(json.dumps(doc(sha, ["RHEA:20309", "RHEA:99999"])), encoding="utf-8")
            result = evaluate(gold, Path(directory))
            self.assertEqual((result["summary"]["tp"], result["summary"]["fp"],
                              result["summary"]["fn"]), (1, 1, 1))
            self.assertEqual(result["cases"][0]["status"], "mismatch")
            path.write_text(json.dumps(doc("b" * 64, ["RHEA:20309"])), encoding="utf-8")
            result = evaluate(gold, Path(directory))
            self.assertEqual(result["cases"][0]["status"], "source_drift")
            self.assertEqual(result["summary"]["scored"], 0)

    def test_invalid_direction_is_excluded_from_metrics(self):
        sha = "a" * 64
        gold = {"schema": "enzymeinfo-uniprot-direct-benchmark-v1", "cases": [
            {"accession": "P04040", "master_ids": ["RHEA:20309"],
             "taxon_id": 9606, "reviewed": True, "uniprot_sha256": sha}
        ]}
        doc = {"schema_version": "2.0-preview", "unknown_is_not_negative": True,
               "proteins": [{"accession": "P04040", "taxon_id": 9606,
                             "reviewed": True, "source_release": {"sha256": sha},
                             "assertions": [
                                 {"accession": "P04040",
                                  "reaction": {"rhea_id": "RHEA:20309",
                                               "master_id": "RHEA:20309",
                                               "direction": "left_to_right"},
                                  "evidence": [{"source_database": "UniProtKB"}]}
                             ]}]}
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "P04040.json").write_text(json.dumps(doc), encoding="utf-8")
            result = evaluate(gold, Path(directory))
        self.assertEqual(result["cases"][0]["status"], "invalid_output")
        self.assertEqual(result["summary"]["scored"], 0)


if __name__ == "__main__":
    unittest.main()
