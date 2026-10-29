"""Offline tests for master-level Rhea mapping comparison and HTTP metadata."""

import hashlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from enzymeinfocollector.domain import ReactionDirection
from enzymeinfocollector.sources import DataSourceError, HttpCache
from enzymeinfocollector.validate_sources import parse_sprot_mapping

HEADER = "RHEA_ID\tDIRECTION\tMASTER_ID\tID\n"
LOOKUP = {
    "RHEA:10000": ("RHEA:10000", ReactionDirection.UNDEFINED),
    "RHEA:10001": ("RHEA:10000", ReactionDirection.LEFT_TO_RIGHT),
    "RHEA:10002": ("RHEA:10000", ReactionDirection.RIGHT_TO_LEFT),
}


class FakeResponse:
    def __init__(self, content: bytes):
        self.stream = io.BytesIO(content)
        self.headers = {
            "X-UniProt-Release": "2026_03",
            "X-UniProt-Release-Date": "02-September-2026",
            "Last-Modified": "Wed, 02 Sep 2026 14:00:00 GMT",
        }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stream.close()

    def read(self, size=-1):
        return self.stream.read(size)


class ValidateSourcesTests(unittest.TestCase):
    def test_directional_mapping_collapses_to_one_master(self):
        tsv = HEADER + "10000\tUN\t10000\tP00330\n10001\tLR\t10000\tP00330\n10002\tRL\t10000\tP00331\n"
        mapped, rows = parse_sprot_mapping(io.StringIO(tsv), {"P00330", "P00331"}, LOOKUP)
        self.assertEqual(rows, 3)
        self.assertEqual(mapped["P00330"], {"RHEA:10000"})
        self.assertEqual(mapped["P00331"], {"RHEA:10000"})

    def test_inconsistent_mapping_is_rejected(self):
        with self.assertRaisesRegex(DataSourceError, "inconsistency"):
            parse_sprot_mapping(io.StringIO(HEADER + "10001\tRL\t10000\tP00330\n"), {"P00330"}, LOOKUP)

    def test_http_cache_preserves_release_headers_and_payload_checksum(self):
        payload = b'{"primaryAccession":"P00330"}'
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), retries=0)
            url = "https://rest.uniprot.org/uniprotkb/P00330.json"
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeResponse(payload)) as request:
                first = cache.get(url, ttl_seconds=3600)
                second = cache.get(url, ttl_seconds=3600)
            self.assertEqual(request.call_count, 1)
            for response in (first, second):
                self.assertEqual(response.metadata["uniprot_release"], "2026_03")
                self.assertEqual(response.metadata["body_sha256"], hashlib.sha256(payload).hexdigest())

    def test_stream_cache_preserves_checksum(self):
        payload = b"RHEA_ID\tDIRECTION\tMASTER_ID\tID\n"
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), retries=0)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeResponse(payload)):
                with cache.open_text("https://ftp.expasy.org/databases/rhea/tsv/sample.tsv", ttl_seconds=3600) as (stream, meta):
                    self.assertEqual(stream.read(), payload.decode())
                    self.assertEqual(meta["body_sha256"], hashlib.sha256(payload).hexdigest())


if __name__ == "__main__":
    unittest.main()
