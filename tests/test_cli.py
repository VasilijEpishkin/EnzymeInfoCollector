from __future__ import annotations

import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from enzymeinfocollector.cli import _read_query_file, main, write_output
from enzymeinfocollector.collector import EnzymeCollector
from enzymeinfocollector.sources import CachedResponse


class PipelineCache:
    def get(self, url: str, **_kwargs: object) -> CachedResponse:
        if url.endswith("/enzyme.dat"):
            body = "ID   1.1.1.1\nDE   Alcohol dehydrogenase.\nDR   P00330, ADH1_YEAST;\n//\n"
        elif "uniprotkb/stream" in url:
            body = "Entry\tEntry Name\tProtein names\tGene Names\tOrganism\tOrganism (ID)\tEC number\tLength\tReviewed\tSequence\nP00330\tADH1_YEAST\tAlcohol dehydrogenase 1\tADH1\tSaccharomyces cerevisiae\t4932\t1.1.1.1\t348\treviewed\tMSTAGKVIKCK\n"
        elif "rhea-reaction-smiles.tsv" in url:
            body = "RHEA:10001\tC>>O\n"
        elif "rhea-directions.tsv" in url:
            body = "RHEA_ID_MASTER\tRHEA_ID_LR\tRHEA_ID_RL\tRHEA_ID_BI\n10000\t10001\t10002\t10003\n"
        elif "rhea-db.org/rhea/" in url:
            body = "Reaction identifier\tEquation\tChEBI name\tChEBI identifier\tEC number\nRHEA:10000\tA = B\tA; B\tCHEBI:1; CHEBI:2\tEC 1.1.1.1\n"
        else:
            raise AssertionError(f"Unexpected URL {url}")
        return CachedResponse(body, {"url": url, "fetched_at": "2026-10-22T00:00:00Z", "cached": False, "stale": False})


class CliTests(unittest.TestCase):
    def test_reads_input_csv_using_existing_protein_column(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "enzymes.csv"
            path.write_text("Protein\nAlcohol dehydrogenase\n", encoding="utf-8")
            self.assertEqual(_read_query_file(path), [("enzyme", "Alcohol dehydrogenase")])

    @unittest.skipUnless(importlib.util.find_spec("openpyxl"), "openpyxl is an optional XLSX extra")
    def test_reads_xlsx_input(self) -> None:
        from openpyxl import Workbook

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "enzymes.xlsx"
            workbook = Workbook()
            sheet = workbook.active
            sheet.append(["Protein", "EC number"])
            sheet.append(["Alcohol dehydrogenase", "1.1.1.1"])
            workbook.save(path)
            workbook.close()
            self.assertEqual(_read_query_file(path), [("enzyme", "Alcohol dehydrogenase"), ("ec", "1.1.1.1")])

    def test_collects_and_exports_provenance_and_reaction_smiles(self) -> None:
        payload = EnzymeCollector(PipelineCache()).collect([("ec", "1.1.1.1")])  # type: ignore[arg-type]
        enzyme = payload["results"][0]["enzymes"][0]
        self.assertEqual(enzyme["proteins"][0]["accession"], "P00330")
        self.assertIsNone(enzyme["reactions"][0]["reaction_smiles"])
        self.assertEqual(enzyme["reactions"][0]["reaction_smiles_directions"]["left_to_right"], "C>>O")
        self.assertIn("enzyme.dat", enzyme["provenance"]["enzyme_nomenclature"]["url"])
        with tempfile.TemporaryDirectory() as directory:
            json_path = Path(directory) / "result.json"
            csv_path = Path(directory) / "result.csv"
            write_output(json_path, "auto", payload)
            write_output(csv_path, "auto", payload)
            saved = json.loads(json_path.read_text(encoding="utf-8"))
            with csv_path.open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(saved["schema_version"], "1.0")
            self.assertEqual(json.loads(rows[0]["reaction_smiles_directions"])["left_to_right"], "C>>O")
            self.assertEqual(rows[0]["uniprot_accession"], "P00330")

    def test_csv_keeps_unmatched_queries(self) -> None:
        payload = {
            "schema_version": "1.0",
            "results": [{"input": {"type": "enzyme", "value": "No such enzyme"}, "status": "not_found", "enzymes": []}],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.csv"
            write_output(path, "auto", payload)
            with path.open(encoding="utf-8", newline="") as stream:
                row = next(csv.DictReader(stream))
            self.assertEqual(row["input_value"], "No such enzyme")
            self.assertEqual(row["status"], "not_found")

    def test_fasta_exports_unique_sequences_with_source_metadata(self) -> None:
        payload = EnzymeCollector(PipelineCache()).collect([("ec", "1.1.1.1")])  # type: ignore[arg-type]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "proteins.fasta"
            write_output(path, "auto", payload)
            records = path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(records[0], ">P00330 EC=1.1.1.1 OS=Saccharomyces_cerevisiae source=UniProtKB")
        self.assertEqual(records[1], "MSTAGKVIKCK")

    def test_cli_refuses_to_overwrite_its_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "enzymes.csv"
            original = "Protein\nAlcohol dehydrogenase\n"
            path.write_text(original, encoding="utf-8")
            self.assertEqual(main(["--file", str(path), "--output", str(path)]), 1)
            self.assertEqual(path.read_text(encoding="utf-8"), original)


if __name__ == "__main__":
    unittest.main()
