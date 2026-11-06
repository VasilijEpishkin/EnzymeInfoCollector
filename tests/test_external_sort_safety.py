"""JSONL external-sort run format: no executable deserialization."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from enzymeinfocollector.sources import (
    DataSourceError, _merge_uniprot_runs, _read_uniprot_run, _write_uniprot_run,
)


def fixture(accession: str, name: str) -> dict:
    return {
        "accession": accession,
        "entry_name": accession + "_TEST",
        "protein_name": name,
        "gene_names": ["pgk", "gene-beta"],
        "organism": "Homo sapiens",
        "organism_id": "9606",
        "ec_numbers": ["2.7.2.3"],
        "length": 15,
        "reviewed": True,
        "sequence": "ACDEFGHIKLMNPQR",
    }


class ExternalSortSafetyTests(unittest.TestCase):
    def test_sort_run_roundtrip_preserves_scientific_fields_and_unicode(self):
        original = [fixture("P0A799", "фосфоглицераткиназа"),
                    fixture("P00330", "Alcohol dehydrogenase")]
        with tempfile.TemporaryDirectory() as directory:
            path = _write_uniprot_run(Path(directory), 0, original[:])
            self.assertEqual(path.suffix, ".jsonl")
            self.assertNotIn(b"\x80\x04", path.read_bytes())
            with path.open("r", encoding="utf-8") as stream:
                rows = list(_read_uniprot_run(stream))
        self.assertEqual([row["accession"] for row in rows], ["P00330", "P0A799"])
        self.assertEqual(rows[1]["protein_name"], "фосфоглицераткиназа")
        self.assertEqual(rows[1]["gene_names"], ["pgk", "gene-beta"])
        self.assertEqual(rows[1]["reviewed"], True)
        self.assertEqual(rows[1]["sequence"], "ACDEFGHIKLMNPQR")

    def test_merge_sorted_runs_without_pickle(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            a = _write_uniprot_run(folder, 0, [fixture("P00558", "X"),
                                              fixture("P00330", "Y")])
            b = _write_uniprot_run(folder, 1, [fixture("P04040", "Z"),
                                              fixture("P0A799", "K")])
            merged = _merge_uniprot_runs([a, b], folder)
            self.assertEqual(len(merged), 1)
            self.assertTrue(merged[0].suffix == ".jsonl")
            self.assertFalse(a.exists())
            self.assertFalse(b.exists())
            with merged[0].open("r", encoding="utf-8") as stream:
                records = list(_read_uniprot_run(stream))
            self.assertEqual([x["accession"] for x in records],
                             ["P00330", "P00558", "P04040", "P0A799"])

    def test_tampered_run_content_rejected_not_executed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "foreign.jsonl"
            path.write_text("cos\nsystem\n(S'echo danger'\ntR.\n", encoding="utf-8")
            with path.open("r", encoding="utf-8") as stream:
                with self.assertRaisesRegex(DataSourceError, "Corrupt UniProt external-sort"):
                    list(_read_uniprot_run(stream))

    def test_invalid_json_shape_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.jsonl"
            for bad in ([], {"accession": 123}, {"wrong": "schema"}):
                path.write_text(json.dumps(bad) + "\n", encoding="utf-8")
                with path.open("r", encoding="utf-8") as stream:
                    with self.assertRaisesRegex(DataSourceError, "Invalid UniProt external-sort"):
                        list(_read_uniprot_run(stream))


if __name__ == "__main__":
    unittest.main()
