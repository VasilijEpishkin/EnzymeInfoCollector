from __future__ import annotations

import io
import tempfile
import unittest
import urllib.error
import urllib.parse
from pathlib import Path
from unittest.mock import patch

from enzymeinfocollector.sources import (
    CachedResponse,
    DataSourceError,
    HttpCache,
    fetch_reaction_smiles,
    fetch_reaction_directions,
    fetch_rhea,
    fetch_uniprot,
    iter_uniprot,
    find_enzyme_entries,
    parse_enzyme_dat,
    validate_ec_number,
)


class FakeCache:
    def __init__(self, response: str):
        self.response = response
        self.urls: list[str] = []

    def get(self, url: str, **_kwargs: object) -> CachedResponse:
        self.urls.append(url)
        return CachedResponse(self.response, {"url": url, "fetched_at": "2026-10-22T00:00:00Z", "cached": False, "stale": False})


class FakeHttpResponse:
    def __init__(self, body: bytes, headers: dict[str, str] | None = None):
        self.body = io.BytesIO(body)
        self.headers = headers or {}

    def read(self, size: int = -1) -> bytes:
        return self.body.read(size)

    def __enter__(self) -> "FakeHttpResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class InterruptedHttpResponse(FakeHttpResponse):
    def read(self, size: int = -1) -> bytes:
        raise urllib.error.URLError("connection dropped")


class ResetHttpResponse(FakeHttpResponse):
    def __init__(self, body: bytes):
        super().__init__(body)
        self.first_read = True

    def read(self, size: int = -1) -> bytes:
        if self.first_read:
            self.first_read = False
            return self.body.read(4)
        raise ConnectionResetError("connection reset")


class SourceTests(unittest.TestCase):
    def test_parse_enzyme_dat_records_and_uniprot_refs(self) -> None:
        data = """ID   1.1.1.1
DE   Alcohol dehydrogenase.
AN   Alcohol:NAD+ oxidoreductase.
CA   An alcohol + NAD(+) = an aldehyde + NADH.
DR   P00330, ADH1_YEAST;
//
ID   3.5.1.n3
DE   Example enzyme.
//
"""
        entries = parse_enzyme_dat(data)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["enzyme_name"], "Alcohol dehydrogenase")
        self.assertEqual(entries[0]["alternative_names"], ["Alcohol:NAD+ oxidoreductase"])
        self.assertEqual(entries[0]["uniprot_accessions"], ["P00330"])
        self.assertEqual(entries[1]["ec_number"], "3.5.1.n3")

    def test_parse_transferred_entries_without_treating_them_as_active(self) -> None:
        entries = parse_enzyme_dat("""ID   1.1.1.5
DE   Transferred entry: 1.1.1.303 and 1.1.1.304.
//
""")
        self.assertEqual(entries[0]["status"], "transferred")
        self.assertEqual(entries[0]["transferred_to"], ["1.1.1.303", "1.1.1.304"])

    def test_validate_ec_number_accepts_preliminary_and_rejects_partial(self) -> None:
        self.assertEqual(validate_ec_number("EC 1.1.1.1"), "1.1.1.1")
        self.assertEqual(validate_ec_number("3.5.1.n3"), "3.5.1.n3")
        with self.assertRaises(ValueError):
            validate_ec_number("1.1.-.-")

    def test_exact_name_precedes_broader_substring_matches(self) -> None:
        entries = parse_enzyme_dat("""ID   1.1.1.1
DE   Alcohol dehydrogenase.
//
ID   1.1.1.2
DE   Alcohol dehydrogenase (NADP+).
//
""")
        self.assertEqual([row["ec_number"] for row in find_enzyme_entries(entries, " alcohol  dehydrogenase ")], ["1.1.1.1"])
        self.assertEqual([row["ec_number"] for row in find_enzyme_entries(entries, "dehydrogenase")], ["1.1.1.1", "1.1.1.2"])

    def test_parse_uniprot_tsv(self) -> None:
        cache = FakeCache("""Entry\tEntry Name\tProtein names\tGene Names\tOrganism\tOrganism (ID)\tEC number\tLength\tReviewed\tSequence
P00330\tADH1_YEAST\tAlcohol dehydrogenase 1\tADH1 ADH1B\tSaccharomyces cerevisiae\t4932\t1.1.1.1\t348\treviewed\tMSTAGKVIKCK
""")
        proteins, source = fetch_uniprot("1.1.1.1", cache)  # type: ignore[arg-type]
        self.assertEqual(proteins[0]["accession"], "P00330")
        self.assertEqual(proteins[0]["length"], 348)
        self.assertEqual(proteins[0]["gene_names"], ["ADH1", "ADH1B"])
        self.assertTrue(proteins[0]["reviewed"])
        self.assertEqual(source["url"], cache.urls[0])
        self.assertIn("uniprotkb/stream", cache.urls[0])

    def test_parse_rhea_and_directional_reaction_smiles(self) -> None:
        cache = FakeCache("""Reaction identifier\tEquation\tChEBI name\tChEBI identifier\tEC number
RHEA:10001\tA = B\tA; B\tCHEBI:1; CHEBI:2\tEC 1.1.1.1
""")
        reactions, _ = fetch_rhea("1.1.1.1", cache)  # type: ignore[arg-type]
        self.assertEqual(reactions[0]["rhea_id"], "10001")
        self.assertEqual(reactions[0]["chebi_ids"], ["1", "2"])
        smiles_cache = FakeCache("RHEA:10001\tC>>O\n")
        smiles, _ = fetch_reaction_smiles(smiles_cache)  # type: ignore[arg-type]
        self.assertEqual(smiles["10001"], "C>>O")
        directions_cache = FakeCache("RHEA_ID_MASTER\tRHEA_ID_LR\tRHEA_ID_RL\tRHEA_ID_BI\n10000\t10001\t10002\t10003\n")
        directions, _ = fetch_reaction_directions(directions_cache)  # type: ignore[arg-type]
        self.assertEqual(directions["10000"]["left_to_right"], "10001")

    def test_rhea_result_limit_fails_instead_of_silently_returning_partial_data(self) -> None:
        response = "Reaction identifier\tEquation\tChEBI name\tChEBI identifier\tEC number\n" + "".join(
            f"RHEA:{index}\tA = B\tA; B\tCHEBI:1; CHEBI:2\tEC 1.1.1.1\n"
            for index in range(10_000)
        )
        with self.assertRaisesRegex(DataSourceError, "limit"):
            fetch_rhea("1.1.1.1", FakeCache(response))  # type: ignore[arg-type]

    def test_http_cache_retries_and_reuses_fresh_response(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=1)
            error = urllib.error.HTTPError("https://example.org/data", 503, "busy", {"Retry-After": "0"}, None)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", side_effect=[error, FakeHttpResponse(b"release 1")]) as request:
                with patch("enzymeinfocollector.sources.time.sleep"):
                    first = cache.get("https://example.org/data", ttl_seconds=3600)
                second = cache.get("https://example.org/data", ttl_seconds=3600)
            self.assertEqual(first.text, "release 1")
            self.assertFalse(first.metadata["cached"])
            self.assertTrue(second.metadata["cached"])
            self.assertEqual(request.call_count, 2)

    def test_http_cache_does_not_retry_local_cache_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=2)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"release 1")) as request:
                with patch("enzymeinfocollector.sources._atomic_write", side_effect=OSError("disk full")):
                    with self.assertRaisesRegex(OSError, "disk full"):
                        cache.get("https://example.org/data", ttl_seconds=3600)
            self.assertEqual(request.call_count, 1)

    def test_http_cache_opens_large_responses_from_disk_and_reuses_them(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"release 1")) as request:
                with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, metadata):
                    self.assertEqual(stream.read(), "release 1")
                    self.assertFalse(metadata["cached"])
                with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, metadata):
                    self.assertEqual(stream.read(), "release 1")
                    self.assertTrue(metadata["cached"])
            self.assertEqual(request.call_count, 1)

    def test_http_cache_stream_uses_stale_response_after_transient_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"release 1")):
                with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, _):
                    self.assertEqual(stream.read(), "release 1")
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", side_effect=urllib.error.URLError("offline")):
                with cache.open_text("https://example.org/data", ttl_seconds=0) as (stream, metadata):
                    self.assertEqual(stream.read(), "release 1")
                    self.assertTrue(metadata["stale"])

    def test_http_cache_stream_rejects_oversized_body_without_publishing_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            with patch("enzymeinfocollector.sources.MAX_RESPONSE_BYTES", 3):
                with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"four")):
                    with self.assertRaisesRegex(DataSourceError, "exceeds"):
                        with cache.open_text("https://example.org/data", ttl_seconds=3600):
                            self.fail("oversized response must not be opened")
            self.assertEqual(list(Path(directory).glob("*.body")), [])
            self.assertEqual(list(Path(directory).glob("*.json")), [])

    def test_http_cache_stream_retries_interrupted_download_without_publishing_partial_body(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=1)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", side_effect=[InterruptedHttpResponse(b"partial"), FakeHttpResponse(b"complete")]) as request:
                with patch("enzymeinfocollector.sources.time.sleep"):
                    with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, _):
                        self.assertEqual(stream.read(), "complete")
            self.assertEqual(request.call_count, 2)
            body = next(Path(directory).glob("*.body"))
            self.assertEqual(body.read_bytes(), b"complete")

    def test_http_cache_stream_retries_connection_reset_during_read(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=1)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", side_effect=[ResetHttpResponse(b"partial"), FakeHttpResponse(b"complete")]) as request:
                with patch("enzymeinfocollector.sources.time.sleep"):
                    with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, _):
                        self.assertEqual(stream.read(), "complete")
            self.assertEqual(request.call_count, 2)

    def test_http_cache_stream_drops_old_metadata_if_new_metadata_cannot_be_published(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"old", {"ETag": '"old"'})):
                with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, _):
                    self.assertEqual(stream.read(), "old")
            new_response = FakeHttpResponse(b"new", {"ETag": '"new"'})
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=new_response):
                with patch("enzymeinfocollector.sources._atomic_write", side_effect=OSError("disk full")):
                    with self.assertRaisesRegex(OSError, "disk full"):
                        with cache.open_text("https://example.org/data", ttl_seconds=0, refresh=True):
                            self.fail("cache publication should fail")
            self.assertEqual(list(Path(directory).glob("*.json")), [])
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"recovered")) as request:
                with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, _):
                    self.assertEqual(stream.read(), "recovered")
            self.assertNotIn("If-None-Match", request.call_args.args[0].header_items())

    def test_uniprot_merge_bounds_open_runs_and_keeps_accession_order(self) -> None:
        cache = FakeCache("Entry\tEntry Name\tProtein names\tGene Names\tOrganism\tOrganism (ID)\tEC number\tLength\tReviewed\tSequence\n" + "".join(
            f"P{number:05d}\tname\tprotein\tgene\torg\t1\t1.1.1.1\t1\treviewed\tM\n"
            for number in (8, 2, 6, 1, 7, 3, 5, 4, 9)
        ))
        with patch("enzymeinfocollector.sources.UNIPROT_SORT_CHUNK_SIZE", 1), patch("enzymeinfocollector.sources.UNIPROT_MERGE_FAN_IN", 2):
            with iter_uniprot("1.1.1.1", cache) as (proteins, _):  # type: ignore[arg-type]
                self.assertEqual([protein["accession"] for protein in proteins], [f"P{number:05d}" for number in range(1, 10)])

    def test_uniprot_can_omit_sequences_and_filter_by_review_and_taxon(self) -> None:
        cache = FakeCache("""Entry\tEntry Name\tProtein names\tGene Names\tOrganism\tOrganism (ID)\tEC number\tLength\tReviewed
P00330\tADH1_YEAST\tAlcohol dehydrogenase 1\tADH1\tSaccharomyces cerevisiae\t4932\t1.1.1.1\t348\treviewed
""")
        proteins, _ = fetch_uniprot(
            "1.1.1.1",
            cache,  # type: ignore[arg-type]
            include_sequence=False,
            reviewed_only=True,
            taxon_ids=[9606, 10090, 9606],
        )
        params = urllib.parse.parse_qs(urllib.parse.urlparse(cache.urls[0]).query)
        self.assertNotIn("sequence", params["fields"][0].split(","))
        self.assertEqual(params["query"], ["ec:1.1.1.1 AND reviewed:true AND (taxonomy_id:9606 OR taxonomy_id:10090)"])
        self.assertEqual(proteins[0]["sequence"], "")

    def test_cache_uses_recent_snapshot_only_for_transient_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"release 1")):
                cache.get("https://example.org/data", ttl_seconds=3600)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", side_effect=urllib.error.URLError("offline")):
                stale = cache.get("https://example.org/data", ttl_seconds=0)
            self.assertEqual(stale.text, "release 1")
            self.assertTrue(stale.metadata["stale"])


if __name__ == "__main__":
    unittest.main()
