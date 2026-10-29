"""Offline evidence-aware UniProt REST adapter tests (synthetic responses)."""

import json
import unittest

from enzymeinfocollector.domain import ReactionDirection
from enzymeinfocollector.sources import DataSourceError
from enzymeinfocollector.uniprot_catalytic import parse_uniprot_catalytic_entry

META = {
    "uniprot_release": "2026_03",
    "fetched_at": "2026-10-29T00:00:00Z",
    "body_sha256": "a" * 64,
    "url": "https://rest.uniprot.org/uniprotkb/P00330.json",
}
DIRECTIONS = {
    "RHEA:10740": ("RHEA:10740", ReactionDirection.UNDEFINED),
    "RHEA:10741": ("RHEA:10740", ReactionDirection.LEFT_TO_RIGHT),
}
DOCUMENT = {
    "primaryAccession": "P00330",
    "entryType": "UniProtKB reviewed (Swiss-Prot)",
    "entryAudit": {"entryVersion": 239, "sequenceVersion": 5},
    "comments": [
        {
            "commentType": "CATALYTIC ACTIVITY",
            "reaction": {
                "reactionCrossReferences": [
                    {"database": "Rhea", "id": "RHEA:10740"},
                    {"database": "Rhea", "id": "RHEA-COMP:10136"},
                    {"database": "ChEBI", "id": "CHEBI:15378"},
                ],
                "evidences": [
                    {"evidenceCode": "ECO:0000269", "source": "PubMed", "id": "6985717"},
                    {"evidenceCode": "ECO:0000255", "source": "PROSITE-ProRule", "id": "PRU10013"},
                    {"evidenceCode": "ECO:0000305"},
                ],
            },
        },
        {"commentType": "CATALYTIC ACTIVITY", "reaction": {"name": "No Rhea mapping"}},
        {"commentType": "FUNCTION", "texts": []},
    ],
}


class UniProtCatalyticTests(unittest.TestCase):
    def parse(self, document=None, metadata=None, lookup=None):
        return parse_uniprot_catalytic_entry(
            json.dumps(DOCUMENT if document is None else document),
            META if metadata is None else metadata,
            "P00330",
            DIRECTIONS if lookup is None else lookup,
        )

    def test_reaction_evidence_and_non_pmid_source_are_preserved(self):
        entry = self.parse()
        self.assertEqual(entry.accession, "P00330")
        self.assertTrue(entry.reviewed)
        self.assertEqual(entry.source_release.release, "2026_03")
        self.assertEqual(entry.source_release.sha256, "a" * 64)
        self.assertEqual(entry.entry_version, 239)
        self.assertEqual(entry.sequence_version, 5)
        self.assertEqual(entry.catalytic_comments_without_rhea, 1)
        self.assertEqual(len(entry.assertions), 1)
        assertion = entry.assertions[0]
        self.assertEqual(assertion.reaction.rhea_id, "RHEA:10740")
        self.assertEqual(len(assertion.evidence), 3)
        self.assertEqual(assertion.evidence[0].citation_ids, ("PMID:6985717",))
        self.assertEqual(assertion.evidence[1].source_references, ("PROSITE-ProRule:PRU10013",))
        self.assertEqual(assertion.evidence[2].eco_code, "ECO:0000305")
        self.assertIsNone(assertion.evidence[0].source_statement_iri)
        self.assertTrue(assertion.annotation_id.startswith("uniprotkb-json:"))

    def test_missing_evidence_is_unknown_not_negative(self):
        document = json.loads(json.dumps(DOCUMENT))
        del document["comments"][0]["reaction"]["evidences"]
        entry = self.parse(document)
        self.assertEqual(len(entry.assertions[0].evidence), 1)
        self.assertIsNone(entry.assertions[0].evidence[0].eco_code)

    def test_multiple_rhea_references_and_directions(self):
        document = json.loads(json.dumps(DOCUMENT))
        document["comments"][0]["reaction"]["reactionCrossReferences"].append(
            {"database": "Rhea", "id": "RHEA:10741"}
        )
        entry = self.parse(document)
        self.assertEqual(len(entry.assertions), 2)
        self.assertEqual(
            {a.reaction.direction for a in entry.assertions},
            {ReactionDirection.UNDEFINED, ReactionDirection.LEFT_TO_RIGHT},
        )

    def test_unreviewed_automatic_annotation_keeps_rule_source(self):
        document = json.loads(json.dumps(DOCUMENT))
        document["entryType"] = "UniProtKB unreviewed (TrEMBL)"
        document["comments"][0]["reaction"]["evidences"] = [
            {"evidenceCode": "ECO:0000256", "source": "ARBA", "id": "ARBA00050856"}
        ]
        entry = self.parse(document)
        self.assertFalse(entry.reviewed)
        self.assertEqual(entry.assertions[0].evidence[0].citation_ids, ())
        self.assertEqual(entry.assertions[0].evidence[0].source_references, ("ARBA:ARBA00050856",))

    def test_missing_release_or_direction_is_error(self):
        with self.assertRaisesRegex(DataSourceError, "release header"):
            self.parse(metadata={"fetched_at": "2026-10-29T00:00:00Z"})
        with self.assertRaisesRegex(DataSourceError, "direction mapping"):
            self.parse(lookup={})

    def test_accession_mismatch_and_malformed_evidence_are_error(self):
        document = json.loads(json.dumps(DOCUMENT))
        document["primaryAccession"] = "P00331"
        with self.assertRaisesRegex(DataSourceError, "mismatch"):
            self.parse(document)
        document["primaryAccession"] = "P00330"
        document["comments"][0]["reaction"]["evidences"][0]["evidenceCode"] = "ECO:bad"
        with self.assertRaisesRegex(DataSourceError, "evidence code"):
            self.parse(document)

    def test_unmapped_rhea_comment_is_not_a_negative_result(self):
        document = json.loads(json.dumps(DOCUMENT))
        document["comments"] = [document["comments"][1]]
        entry = self.parse(document)
        self.assertEqual(entry.assertions, ())
        self.assertEqual(entry.catalytic_comments_without_rhea, 1)


if __name__ == "__main__":
    unittest.main()
