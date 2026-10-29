"""Offline tests for Rhea-authored KEGG/MetaCyc reaction mappings."""

from __future__ import annotations

import hashlib
import unittest

from enzymeinfocollector.domain import ProteinReactionAssertion, ReactionDirection, SourceRelease
from enzymeinfocollector.evidence_sources import parse_rhea_directions, RHEA_DIRECTIONS_URL
from enzymeinfocollector.reaction_xrefs import (
    RHEA_CROSSREF_URLS,
    RheaCrossReferenceCollector,
    parse_rhea_crossrefs,
)
from enzymeinfocollector.sources import CachedResponse, DataSourceError


DIRECTIONS = (
    "RHEA_ID_MASTER\tRHEA_ID_LR\tRHEA_ID_RL\tRHEA_ID_BI\n"
    "10000\t10001\t10002\t10003\n"
    "10004\t10005\t10006\t10007\n"
)
KEGG_TSV = (
    "RHEA_ID\tDIRECTION\tMASTER_ID\tID\n"
    "10003\tBI\t10000\tR02938\n"
    "10007\tBI\t10004\tR04010\n"
)
METACYC_TSV = (
    "RHEA_ID\tDIRECTION\tMASTER_ID\tID\n"
    "10001\tLR\t10000\tPENTANAMIDASE-RXN\n"
    "10004\tUN\t10004\tTHIOCYANATE-ISOMERASE-RXN\n"
)


class StubCache:
    def __init__(self):
        self.calls: list[tuple[str, int, bool]] = []
        self.stale_url: str | None = None
        self.payloads = {
            RHEA_DIRECTIONS_URL: DIRECTIONS,
            RHEA_CROSSREF_URLS["KEGG"]: KEGG_TSV,
            RHEA_CROSSREF_URLS["MetaCyc"]: METACYC_TSV,
        }

    def get(self, url: str, *, ttl_seconds: int, refresh: bool = False) -> CachedResponse:
        self.calls.append((url, ttl_seconds, refresh))
        return CachedResponse(
            self.payloads[url],
            {"fetched_at": "2026-10-29T12:00:00Z", "stale": url == self.stale_url},
        )


class ReactionXrefsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.release = SourceRelease("Rhea", "xrefs-test", "2026-10-29T12:00:00Z")
        self.directions = parse_rhea_directions(DIRECTIONS, self.release)

    def test_parses_real_four_column_rhea_mapping_and_preserves_direction(self) -> None:
        kegg = parse_rhea_crossrefs(KEGG_TSV, "KEGG", self.release, self.directions)
        metacyc = parse_rhea_crossrefs(METACYC_TSV, "MetaCyc", self.release, self.directions)
        self.assertEqual(
            (kegg[0].reaction.rhea_id, kegg[0].reaction.master_id, kegg[0].reaction.direction),
            ("RHEA:10003", "RHEA:10000", ReactionDirection.BIDIRECTIONAL),
        )
        self.assertEqual(kegg[0].external_id, "R02938")
        self.assertEqual(
            (metacyc[0].reaction.rhea_id, metacyc[0].reaction.direction),
            ("RHEA:10001", ReactionDirection.LEFT_TO_RIGHT),
        )
        self.assertEqual(metacyc[1].reaction.direction, ReactionDirection.UNDEFINED)
        self.assertEqual(metacyc[1].reaction.rhea_id, metacyc[1].reaction.master_id)
        self.assertNotIsInstance(kegg[0], ProteinReactionAssertion)
        self.assertFalse(hasattr(kegg[0], "accession"))
        self.assertEqual(kegg[0].source_release.database, "Rhea")

    def test_duplicate_identical_rows_are_deduplicated(self) -> None:
        tsv = KEGG_TSV + "10003\tBI\t10000\tR02938\n"
        self.assertEqual(len(parse_rhea_crossrefs(tsv, "KEGG", self.release, self.directions)), 2)

    def test_cross_reference_mismatches_fail_closed(self) -> None:
        cases = [
            ("RHEA_ID\tID\n10003\tR02938\n", "header"),
            (KEGG_TSV.replace("10003\tBI\t10000", "10003\tLR\t10000"), "disagrees"),
            (KEGG_TSV.replace("10003\tBI\t10000", "10003\tBI\t10004"), "disagrees"),
            (KEGG_TSV.replace("R02938", "C00001"), "KEGG reaction ID"),
            (KEGG_TSV.replace("R02938", "R00000 invalid"), "external ID"),
            (KEGG_TSV.replace("10003\tBI", "99999\tBI"), "absent"),
            ("RHEA_ID\tDIRECTION\tMASTER_ID\tID\n", "empty"),
            (KEGG_TSV + "10003\tBI\t10000\tR02938\textra\n", "Malformed"),
        ]
        for tsv, error in cases:
            with self.subTest(error=error), self.assertRaisesRegex(DataSourceError, error):
                parse_rhea_crossrefs(tsv, "KEGG", self.release, self.directions)

    def test_only_rhea_tsv_urls_are_requested(self) -> None:
        cache = StubCache()
        dataset = RheaCrossReferenceCollector(cache).collect(refresh=True)
        self.assertEqual([call[0] for call in cache.calls], [
            RHEA_DIRECTIONS_URL,
            RHEA_CROSSREF_URLS["KEGG"],
            RHEA_CROSSREF_URLS["MetaCyc"],
        ])
        self.assertEqual(len(dataset.crossrefs), 4)
        self.assertEqual(len(dataset.mapping_snapshots), 2)
        self.assertEqual(
            dataset.mapping_snapshots[0].sha256,
            hashlib.sha256(KEGG_TSV.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(
            dataset.rhea_directions_snapshot.sha256,
            hashlib.sha256(DIRECTIONS.encode("utf-8")).hexdigest(),
        )
        self.assertTrue(all(item.source_release.source_url in RHEA_CROSSREF_URLS.values()
                            for item in dataset.crossrefs))

    def test_invalid_database_before_http_and_stale_cache_rejected(self) -> None:
        cache = StubCache()
        collector = RheaCrossReferenceCollector(cache)
        for invalid in ("KEGG", ["BRENDA"], []):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                collector.collect(invalid)
        self.assertEqual(cache.calls, [])
        cache.stale_url = RHEA_CROSSREF_URLS["KEGG"]
        with self.assertRaisesRegex(DataSourceError, "stale"):
            collector.collect()

    def test_single_database_is_supported(self) -> None:
        cache = StubCache()
        dataset = RheaCrossReferenceCollector(cache).collect(["MetaCyc"])
        self.assertEqual(len(cache.calls), 2)
        self.assertEqual(len(dataset.mapping_snapshots), 1)
        self.assertEqual({entry.database for entry in dataset.crossrefs}, {"MetaCyc"})


if __name__ == "__main__":
    unittest.main()
