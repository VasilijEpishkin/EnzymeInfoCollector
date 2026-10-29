"""Offline regression tests for direct UniProt↔Rhea evidence extraction."""

from __future__ import annotations

from dataclasses import asdict
import copy
import json
import unittest

from enzymeinfocollector.domain import Evidence, ReactionDirection, SourceRelease
from enzymeinfocollector.evidence_sources import (
    DirectEvidenceCollector,
    RHEA_DIRECTIONS_URL,
    UNIPROT_ENTRY_URL,
    UNIPROT_RELEASE_URL,
    parse_rhea_directions,
    parse_uniprot_catalytic_activities,
    parse_uniprot_release,
)
from enzymeinfocollector.sources import CachedResponse, DataSourceError


DIRECTIONS_TSV = (
    "RHEA_ID_MASTER\tRHEA_ID_LR\tRHEA_ID_RL\tRHEA_ID_BI\n"
    "10736\t10737\t10738\t10739\n"
    "10740\t10741\t10742\t10743\n"
    "14801\t14802\t14803\t14804\n"
)
RELEASE_TEXT = (
    "UniProt Knowledgebase Release 2026_03 consists of:\n"
    "UniProtKB/Swiss-Prot Release 2026_03 of 02-Sep-2026\n"
    "UniProtKB/TrEMBL Release 2026_03 of 02-Sep-2026\n"
)
SAMPLE = {
    "primaryAccession": "P00330",
    "entryType": "UniProtKB reviewed (Swiss-Prot)",
    "entryAudit": {"entryVersion": 239, "sequenceVersion": 5},
    "comments": [
        {
            "commentType": "CATALYTIC ACTIVITY",
            "reaction": {
                "ecNumber": "1.1.1.1",
                "reactionCrossReferences": [
                    {"database": "Rhea", "id": "RHEA:10740"},
                    {"database": "Rhea", "id": "RHEA-COMP:9863"},
                    {"database": "ChEBI", "id": "CHEBI:15378"},
                ],
                "evidences": [
                    {"evidenceCode": "ECO:0000269", "source": "PubMed", "id": "6985717"},
                    {"evidenceCode": "ECO:0000305"},
                ],
            },
            "physiologicalReactions": [
                {
                    "directionType": "left-to-right",
                    "reactionCrossReference": {"database": "Rhea", "id": "RHEA:10741"},
                    "evidences": [{"evidenceCode": "ECO:0000305", "source": "PubMed", "id": "12702265"}],
                },
                {
                    "directionType": "right-to-left",
                    "reactionCrossReference": {"database": "Rhea", "id": "RHEA:10742"},
                    "evidences": [{"evidenceCode": "ECO:0000305"}],
                },
            ],
        },
        {"commentType": "FUNCTION", "texts": [{"value": "irrelevant"}]},
    ],
}


class StubCache:
    def __init__(self, entry: dict | None = None, *, fetched_at: str = "2026-10-29T12:00:00Z"):
        self.calls: list[tuple[str, int, bool]] = []
        self.entry = entry if entry is not None else SAMPLE
        self.fetched_at = fetched_at
        self.stale_url: str | None = None

    def get(self, url: str, *, ttl_seconds: int, refresh: bool = False) -> CachedResponse:
        self.calls.append((url, ttl_seconds, refresh))
        text = (
            RELEASE_TEXT if url == UNIPROT_RELEASE_URL else
            DIRECTIONS_TSV if url == RHEA_DIRECTIONS_URL else
            json.dumps(self.entry)
        )
        return CachedResponse(
            text,
            {"fetched_at": self.fetched_at, "stale": url == self.stale_url},
        )


class EvidenceSourcesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.release = SourceRelease("UniProtKB", "2026_03", "2026-10-29T12:00:00Z")
        self.rhea_release = SourceRelease("Rhea", "directions-sha256:test", "2026-10-29T12:00:00Z")
        self.index = parse_rhea_directions(DIRECTIONS_TSV, self.rhea_release)

    def test_master_and_both_physiological_directions_are_distinct_assertions(self) -> None:
        result = parse_uniprot_catalytic_activities(SAMPLE, self.release, self.index)
        self.assertEqual(result.accession, "P00330")
        self.assertTrue(result.reviewed)
        self.assertEqual((result.entry_version, result.sequence_version), (239, 5))
        self.assertEqual(
            [item.reaction.rhea_id for item in result.assertions],
            ["RHEA:10740", "RHEA:10741", "RHEA:10742"],
        )
        self.assertEqual([item.reaction.master_id for item in result.assertions], ["RHEA:10740"] * 3)
        self.assertEqual(
            [item.reaction.direction for item in result.assertions],
            [ReactionDirection.UNDEFINED, ReactionDirection.LEFT_TO_RIGHT, ReactionDirection.RIGHT_TO_LEFT],
        )
        self.assertEqual(len(result.assertions[0].evidence), 2)
        self.assertEqual(result.assertions[0].evidence[0].citation_ids, ("PubMed:6985717",))
        self.assertEqual(result.assertions[0].evidence[1].citation_ids, ())
        self.assertEqual(result.assertions[1].evidence[0].citation_ids, ("PubMed:12702265",))
        self.assertEqual(result.assertions[2].evidence[0].citation_ids, ())
        self.assertNotIn("RHEA-COMP:9863", [item.reaction.rhea_id for item in result.assertions])
        self.assertEqual(json.loads(json.dumps(asdict(result)))["assertions"][0]["reaction"]["rhea_id"], "RHEA:10740")

    def test_ec_only_annotations_do_not_create_direct_edges(self) -> None:
        data = copy.deepcopy(SAMPLE)
        data["comments"] = [
            {"commentType": "CATALYTIC ACTIVITY", "reaction": {"ecNumber": "1.1.1.1"}},
            {"commentType": "FUNCTION", "reaction": {"reactionCrossReferences": [{"database": "Rhea", "id": "RHEA:10736"}]}},
        ]
        result = parse_uniprot_catalytic_activities(data, self.release, self.index)
        self.assertEqual(result.assertions, ())

    def test_missing_eco_is_unknown_not_negative_and_no_pubmed_is_valid(self) -> None:
        data = copy.deepcopy(SAMPLE)
        data["comments"][0]["reaction"]["evidences"] = []
        data["comments"][0]["physiologicalReactions"] = []
        result = parse_uniprot_catalytic_activities(data, self.release, self.index)
        self.assertEqual(result.assertions[0].evidence, (Evidence(source_database="UniProtKB"),))

    def test_repeated_direct_annotations_merge_evidence_without_ec_join(self) -> None:
        data = copy.deepcopy(SAMPLE)
        data["comments"].append(copy.deepcopy(data["comments"][0]))
        result = parse_uniprot_catalytic_activities(data, self.release, self.index)
        self.assertEqual(len(result.assertions), 3)
        self.assertEqual(len(result.assertions[0].evidence), 4)
        self.assertEqual(len(result.assertions[1].evidence), 2)

    def test_bad_rhea_mapping_or_direction_fails_closed(self) -> None:
        data = copy.deepcopy(SAMPLE)
        data["comments"][0]["physiologicalReactions"][0]["directionType"] = "right-to-left"
        with self.assertRaisesRegex(DataSourceError, "disagrees"):
            parse_uniprot_catalytic_activities(data, self.release, self.index)
        data = copy.deepcopy(SAMPLE)
        data["comments"][0]["physiologicalReactions"][0]["reactionCrossReference"]["id"] = "RHEA:14802"
        with self.assertRaisesRegex(DataSourceError, "does not match"):
            parse_uniprot_catalytic_activities(data, self.release, self.index)
        data = copy.deepcopy(SAMPLE)
        data["comments"][0]["reaction"]["reactionCrossReferences"][0]["id"] = "RHEA:99999"
        with self.assertRaisesRegex(DataSourceError, "absent"):
            parse_uniprot_catalytic_activities(data, self.release, self.index)

    def test_reaction_crossreferences_reject_malformed_payload(self) -> None:
        data = copy.deepcopy(SAMPLE)
        data["comments"][0]["reaction"]["reactionCrossReferences"] = {"id": "RHEA:10740"}
        with self.assertRaisesRegex(DataSourceError, "must be a list"):
            parse_uniprot_catalytic_activities(data, self.release, self.index)
        data = copy.deepcopy(SAMPLE)
        data["comments"][0]["reaction"]["evidences"] = ["not an evidence object"]
        with self.assertRaisesRegex(DataSourceError, "evidence must be an object"):
            parse_uniprot_catalytic_activities(data, self.release, self.index)

    def test_rhea_tsv_requires_all_columns_and_unique_identifiers(self) -> None:
        with self.assertRaisesRegex(DataSourceError, "header"):
            parse_rhea_directions("RHEA_ID_MASTER\tRHEA_ID_LR\n10736\t10737\n", self.rhea_release)
        with self.assertRaisesRegex(DataSourceError, "Duplicate"):
            parse_rhea_directions(DIRECTIONS_TSV + "10736\t99990\t99991\t99992\n", self.rhea_release)
        with self.assertRaisesRegex(DataSourceError, "Incomplete"):
            parse_rhea_directions(DIRECTIONS_TSV + "99990\t99991\t\t99993\n", self.rhea_release)

    def test_release_metadata_is_strict(self) -> None:
        self.assertEqual(parse_uniprot_release(RELEASE_TEXT)[0], "2026_03")
        with self.assertRaisesRegex(DataSourceError, "unexpected format"):
            parse_uniprot_release("garbage")
        with self.assertRaisesRegex(DataSourceError, "release date"):
            parse_uniprot_release("UniProt Knowledgebase Release 2026_03 consists of:\n")

    def test_collector_fetches_release_directions_and_specific_accession_only(self) -> None:
        cache = StubCache()
        dataset = DirectEvidenceCollector(cache).collect(["P00330", "P00330"])
        self.assertEqual(dataset.uniprot_release, "2026_03")
        self.assertEqual(len(dataset.proteins), 1)
        self.assertEqual(len(dataset.proteins[0].assertions), 3)
        self.assertEqual(len(cache.calls), 3)
        self.assertEqual(cache.calls[0][0], UNIPROT_RELEASE_URL)
        self.assertEqual(cache.calls[1][0], RHEA_DIRECTIONS_URL)
        self.assertEqual(cache.calls[2][0], UNIPROT_ENTRY_URL.format(accession="P00330"))
        self.assertEqual(dataset.rhea_directions_snapshot.database, "Rhea")
        self.assertEqual(len(dataset.rhea_directions_snapshot.sha256 or ""), 64)

    def test_collector_rejects_stale_and_mismatched_entry(self) -> None:
        cache = StubCache()
        cache.stale_url = UNIPROT_ENTRY_URL.format(accession="P00330")
        with self.assertRaisesRegex(DataSourceError, "Refusing stale"):
            DirectEvidenceCollector(cache).collect(["P00330"])
        cache = StubCache(entry={**SAMPLE, "primaryAccession": "P00331"})
        with self.assertRaisesRegex(DataSourceError, "accession mismatch"):
            DirectEvidenceCollector(cache).collect(["P00330"])

    def test_collector_refetches_pre_release_cache(self) -> None:
        cache = StubCache(fetched_at="2026-08-01T00:00:00Z")
        with self.assertRaisesRegex(DataSourceError, "predates release"):
            DirectEvidenceCollector(cache).collect(["P00330"])
        self.assertTrue(cache.calls[-1][2])

    def test_accession_validation_happens_before_network(self) -> None:
        cache = StubCache()
        for bad in ([], ["not-an-accession"], "P00330", ["P00330", "P00330/../../"]):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                DirectEvidenceCollector(cache).collect(bad)
        self.assertEqual(cache.calls, ( ) if isinstance(cache.calls, tuple) else [])

    def test_unreviewed_entry_retains_unknown_evidence(self) -> None:
        data = copy.deepcopy(SAMPLE)
        data["entryType"] = "UniProtKB unreviewed (TrEMBL)"
        data["comments"][0]["reaction"]["evidences"] = [{"evidenceCode": "ECO:0000256", "source": "UniRule", "id": "UR000000001"}]
        data["comments"][0]["physiologicalReactions"] = []
        result = parse_uniprot_catalytic_activities(data, self.release, self.index)
        self.assertFalse(result.reviewed)
        self.assertEqual(result.assertions[0].evidence[0].citation_ids, ("UniRule:UR000000001",))


if __name__ == "__main__":
    unittest.main()
