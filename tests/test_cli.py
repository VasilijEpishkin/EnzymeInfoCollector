from __future__ import annotations

import csv
import importlib.util
import json
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from unittest.mock import patch

from enzymeinfocollector.cli import _read_query_file, main, write_output, write_stream_output
from enzymeinfocollector.collector import EnzymeCollector
from enzymeinfocollector.sources import CachedResponse


class PipelineCache:
    def __init__(self):
        self.urls: list[str] = []

    def get(self, url: str, **_kwargs: object) -> CachedResponse:
        self.urls.append(url)
        if url.endswith("/enzyme.dat"):
            body = "ID   1.1.1.1\nDE   Alcohol dehydrogenase.\nDR   P00330, ADH1_YEAST;\n//\n"
        elif "uniprotkb/stream" in url:
            body = "Entry\tEntry Name\tProtein names\tGene Names\tOrganism\tOrganism (ID)\tEC number\tLength\tReviewed\tSequence\nP00330\tADH1_YEAST\tAlcohol dehydrogenase 1\tADH1\tSaccharomyces cerevisiae\t4932\t1.1.1.1\t348\treviewed\tMSTAGKVIKCK\nP12345\tADH2_YEAST\tAlcohol dehydrogenase 2\tADH2\tSaccharomyces cerevisiae\t4932\t1.1.1.1\t380\treviewed\tMSTAGKVIKCK\n"
        elif "rhea-reaction-smiles.tsv" in url:
            body = "RHEA:10001\tC>>O\nRHEA:10005\tN>>O\n"
        elif "rhea-directions.tsv" in url:
            body = "RHEA_ID_MASTER\tRHEA_ID_LR\tRHEA_ID_RL\tRHEA_ID_BI\n10000\t10001\t10002\t10003\n10004\t10005\t10006\t10007\n"
        elif "rhea-db.org/rhea/" in url:
            body = "Reaction identifier\tEquation\tChEBI name\tChEBI identifier\tEC number\nRHEA:10000\tA = B\tA; B\tCHEBI:1; CHEBI:2\tEC 1.1.1.1\nRHEA:10004\tC = D\tC; D\tCHEBI:3; CHEBI:4\tEC 1.1.1.1\n"
        else:
            raise AssertionError(f"Unexpected URL {url}")
        return CachedResponse(body, {"url": url, "fetched_at": "2026-10-22T00:00:00Z", "cached": False, "stale": False})


class TransferCache:
    def __init__(self, entries: str):
        self.entries = entries
        self.urls: list[str] = []

    def get(self, url: str, **_kwargs: object) -> CachedResponse:
        self.urls.append(url)
        if url.endswith("/enzyme.dat"):
            body = self.entries
        elif "uniprotkb/stream" in url:
            body = "Entry\tEntry Name\tProtein names\tGene Names\tOrganism\tOrganism (ID)\tEC number\tLength\tReviewed\tSequence\nP00330\tTARGET\tTarget enzyme\ttarget\tTest organism\t1\t7.9.9.5\t11\treviewed\tMSTAGKVIKCK\n"
        elif "rhea-reaction-smiles.tsv" in url:
            body = ""
        elif "rhea-directions.tsv" in url:
            body = "RHEA_ID_MASTER\tRHEA_ID_LR\tRHEA_ID_RL\tRHEA_ID_BI\n"
        elif "rhea-db.org/rhea/" in url:
            body = "Reaction identifier\tEquation\tChEBI name\tChEBI identifier\tEC number\n"
        else:
            raise AssertionError(f"Unexpected URL {url}")
        return CachedResponse(body, {"url": url, "fetched_at": "2026-10-22T00:00:00Z", "cached": False, "stale": False})


class UnsortedPipelineCache(PipelineCache):
    def get(self, url: str, **kwargs: object) -> CachedResponse:
        response = super().get(url, **kwargs)
        if "uniprotkb/stream" in url:
            header, *rows = response.text.splitlines()
            return CachedResponse("\n".join([header, *reversed(rows)]) + "\n", response.metadata)
        return response


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
        pipeline_cache = PipelineCache()
        payload = EnzymeCollector(pipeline_cache).collect([("ec", "1.1.1.1")])  # type: ignore[arg-type]
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
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["uniprot_accession"], "P00330")
            self.assertEqual(rows[1]["uniprot_accession"], "P12345")
            self.assertEqual(rows[0]["association_basis"], "EC number")
            ec_reactions = json.loads(rows[0]["ec_reactions"])
            self.assertEqual([reaction["rhea_id"] for reaction in ec_reactions], ["10000", "10004"])

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

    def test_streamed_exports_match_legacy_order_and_transfer_rows(self) -> None:
        scenarios = [
            (UnsortedPipelineCache(), [("ec", "1.1.1.1")]),
            (TransferCache("""ID   7.9.9.9
DE   Transferred entry: 7.9.9.1 and 7.9.9.2 and 7.9.9.3.
//
ID   7.9.9.1
DE   Transferred entry: 7.9.9.5.
//
ID   7.9.9.2
DE   Transferred entry: 7.9.9.5.
//
ID   7.9.9.3
DE   Deleted entry.
//
ID   7.9.9.5
DE   Terminal enzyme.
//
"""), [("ec", "7.9.9.9")]),
        ]
        for cache, queries in scenarios:
            with self.subTest(queries=queries), tempfile.TemporaryDirectory() as directory:
                legacy_payload = EnzymeCollector(cache).collect(queries)  # type: ignore[arg-type]
                for suffix, output_format in (("csv", "csv"), ("fasta", "fasta")):
                    legacy_path = Path(directory) / f"legacy.{suffix}"
                    streamed_path = Path(directory) / f"streamed.{suffix}"
                    write_output(legacy_path, output_format, legacy_payload)
                    with patch("enzymeinfocollector.sources.UNIPROT_SORT_CHUNK_SIZE", 1):
                        write_stream_output(streamed_path, output_format, EnzymeCollector(cache), queries)  # type: ignore[arg-type]
                    self.assertEqual(streamed_path.read_bytes(), legacy_path.read_bytes())

    def test_streamed_export_preserves_destination_on_collection_failure(self) -> None:
        class BrokenCache:
            def get(self, *_args: object, **_kwargs: object) -> CachedResponse:
                raise OSError("source unavailable")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.csv"
            path.write_bytes(b"previous output\n")
            with self.assertRaisesRegex(OSError, "source unavailable"):
                write_stream_output(path, "csv", EnzymeCollector(BrokenCache()), [("ec", "1.1.1.1")])  # type: ignore[arg-type]
            self.assertEqual(path.read_bytes(), b"previous output\n")
            self.assertEqual(list(Path(directory).glob(".result.csv.*.tmp")), [])

    def test_streamed_export_preserves_destination_on_mid_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.csv"
            path.write_bytes(b"previous output\n")
            with patch("enzymeinfocollector.cli._csv_export_row", side_effect=OSError("disk full")):
                with self.assertRaisesRegex(OSError, "disk full"):
                    write_stream_output(path, "csv", EnzymeCollector(PipelineCache()), [("ec", "1.1.1.1")])  # type: ignore[arg-type]
            self.assertEqual(path.read_bytes(), b"previous output\n")
            self.assertEqual(list(Path(directory).glob(".result.csv.*.tmp")), [])

    def test_cli_routes_csv_exports_through_streaming_writer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.csv"
            with patch("enzymeinfocollector.cli.HttpCache", return_value=PipelineCache()):
                self.assertEqual(main(["--ec", "1.1.1.1", "--output", str(path)]), 0)
            with path.open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
        self.assertEqual([row["uniprot_accession"] for row in rows], ["P00330", "P12345"])

    def test_collector_passes_server_side_uniprot_filters(self) -> None:
        cache = PipelineCache()
        EnzymeCollector(cache, include_sequences=False, reviewed_only=True, taxon_ids=[9606]).collect([("ec", "1.1.1.1")])  # type: ignore[arg-type]
        uniprot_url = next(url for url in cache.urls if "uniprotkb/stream" in url)
        params = urllib.parse.parse_qs(urllib.parse.urlparse(uniprot_url).query)
        self.assertEqual(params["query"], ["ec:1.1.1.1 AND reviewed:true AND (taxonomy_id:9606)"])
        self.assertNotIn("sequence", params["fields"][0].split(","))

    def test_resolves_transfer_chains_and_branches_and_exports_provenance(self) -> None:
        cache = TransferCache("""ID   7.9.9.9
DE   Transferred entry: 7.9.9.1 and 7.9.9.2 and 7.9.9.3 and 7.9.9.4.
//
ID   7.9.9.1
DE   Transferred entry: 7.9.9.5.
//
ID   7.9.9.2
DE   Transferred entry: 7.9.9.5.
//
ID   7.9.9.3
DE   Deleted entry.
//
ID   7.9.9.5
DE   Terminal enzyme.
//
""")
        payload = EnzymeCollector(cache).collect([("ec", "7.9.9.9")])  # type: ignore[arg-type]
        source = payload["results"][0]["enzymes"][0]
        resolution = source["resolution"]
        self.assertEqual(resolution["status"], "partial")
        self.assertEqual(resolution["terminal_ec_numbers"], ["7.9.9.5"])
        self.assertEqual(len(resolution["resolved_enzymes"]), 1)
        self.assertEqual(len(resolution["resolved_enzymes"][0]["resolution_paths"]), 2)
        self.assertEqual({path["status"] for path in resolution["unresolved_targets"]}, {"deleted", "missing"})
        self.assertEqual(sum("uniprotkb/stream" in url for url in cache.urls), 1)
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "result.csv"
            write_output(csv_path, "auto", payload)
            with csv_path.open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
        source_row = next(row for row in rows if row["record_kind"] == "enzyme")
        target_row = next(row for row in rows if row["record_kind"] == "resolved_target")
        self.assertEqual(list(rows[0]).index("ec_number"), 3)
        self.assertEqual(source_row["enzyme_status"], "transferred")
        self.assertEqual(source_row["resolution_status"], "partial")
        self.assertEqual(target_row["requested_ec"], "7.9.9.9")
        self.assertEqual(target_row["ec_number"], "7.9.9.5")
        with tempfile.TemporaryDirectory() as directory:
            fasta_path = Path(directory) / "resolved.fasta"
            write_output(fasta_path, "auto", payload)
            fasta = fasta_path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(fasta[0], ">P00330 EC=7.9.9.5 OS=Test_organism source=UniProtKB")
        self.assertEqual(fasta[1], "MSTAGKVIKCK")
        with tempfile.TemporaryDirectory() as directory:
            json_path = Path(directory) / "resolved.json"
            write_output(json_path, "auto", payload)
            saved = json.loads(json_path.read_text(encoding="utf-8"))
        self.assertEqual(saved["results"][0]["enzymes"][0]["resolution"]["status"], "partial")

    def test_csv_keeps_ec_numbers_missing_from_expasy(self) -> None:
        payload = EnzymeCollector(PipelineCache()).collect([("ec", "2.2.2.2")])  # type: ignore[arg-type]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.csv"
            write_output(path, "auto", payload)
            with path.open(encoding="utf-8", newline="") as stream:
                row = next(csv.DictReader(stream))
        self.assertEqual(row["ec_number"], "2.2.2.2")
        self.assertEqual(row["enzyme_status"], "not_found_in_expasy")

    def test_transfer_cycle_is_reported_without_recursion_failure(self) -> None:
        cache = TransferCache("""ID   7.9.9.9
DE   Transferred entry: 7.9.9.1.
//
ID   7.9.9.1
DE   Transferred entry: 7.9.9.9.
//
""")
        payload = EnzymeCollector(cache).collect([("ec", "7.9.9.9")])  # type: ignore[arg-type]
        resolution = payload["results"][0]["enzymes"][0]["resolution"]
        self.assertEqual(resolution["status"], "unresolved")
        self.assertEqual(resolution["paths"], [{"ec_numbers": ["7.9.9.9", "7.9.9.1", "7.9.9.9"], "status": "cycle"}])

    def test_long_transfer_chain_does_not_depend_on_recursion_depth(self) -> None:
        collector = EnzymeCollector(None)  # type: ignore[arg-type]
        source = {"ec_number": "1.1.1.1", "status": "transferred", "transferred_to": ["1.1.1.2"]}
        entries = [
            {"ec_number": f"1.1.1.{number}", "status": "transferred", "transferred_to": [f"1.1.1.{number + 1}"]}
            for number in range(2, 1102)
        ]
        collector._enzyme_index = {entry["ec_number"].casefold(): entry for entry in entries}
        resolution = collector._resolve_transfers(source)
        self.assertEqual(resolution["status"], "unresolved")
        self.assertEqual(resolution["paths"][0]["status"], "missing")
        self.assertEqual(len(resolution["paths"][0]["ec_numbers"]), 1102)

    def test_transfer_path_explosion_is_marked_as_truncated(self) -> None:
        collector = EnzymeCollector(None)  # type: ignore[arg-type]
        source = {"ec_number": "1.1.0.0", "status": "transferred", "transferred_to": ["1.1.1.1", "1.1.1.2"]}
        entries = []
        for level in range(1, 16):
            for branch in (1, 2):
                ec_number = f"1.1.{level}.{branch}"
                if level == 15:
                    entries.append({
                        "ec_number": ec_number,
                        "status": "transferred",
                        "transferred_to": [f"1.1.16.{branch}", f"1.1.16.{3 - branch}"],
                    })
                else:
                    entries.append({
                        "ec_number": ec_number,
                        "status": "transferred",
                        "transferred_to": [f"1.1.{level + 1}.1", f"1.1.{level + 1}.2"],
                    })
        collector._enzyme_index = {entry["ec_number"].casefold(): entry for entry in entries}
        collector._collect_active_enzyme = lambda enzyme: {**enzyme, "proteins": [], "reactions": [], "provenance": {}}  # type: ignore[method-assign]
        resolution = collector._resolve_transfers(source)
        self.assertTrue(resolution["path_limit_reached"])
        self.assertEqual(resolution["paths"][-1]["status"], "truncated")
        self.assertEqual(resolution["status"], "partial")
        self.assertEqual(resolution["resolved_enzymes"], [])
        self.assertNotIn("truncated", {path["status"] for path in resolution["unresolved_targets"]})

    def test_cli_refuses_to_overwrite_its_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "enzymes.csv"
            original = "Protein\nAlcohol dehydrogenase\n"
            path.write_text(original, encoding="utf-8")
            self.assertEqual(main(["--file", str(path), "--output", str(path)]), 1)
            self.assertEqual(path.read_text(encoding="utf-8"), original)


if __name__ == "__main__":
    unittest.main()
