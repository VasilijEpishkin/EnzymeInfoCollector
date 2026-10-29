"""Offline JSON evidence export tests."""

import json
import unittest
from unittest.mock import patch

from enzymeinfocollector.domain import (
    Evidence, ProteinReactionAssertion, ReactionDirection, ReactionVariant, SourceRelease,
)
from enzymeinfocollector.evidence_cli import build_export
from enzymeinfocollector.rhea_directions import RheaDirectionsSnapshot
from enzymeinfocollector.uniprot_catalytic import UniProtCatalyticEntry


class EvidenceExportTests(unittest.TestCase):
    def test_export_contains_only_direct_annotations_and_attribution(self):
        release = SourceRelease("UniProtKB", "synthetic", "2026-10-29T00:00:00Z")
        assertion = ProteinReactionAssertion(
            "P00330",
            ReactionVariant("RHEA:10740", "RHEA:10740", ReactionDirection.UNDEFINED),
            release,
            (Evidence("UniProtKB", "ECO:0000269", citation_ids=("PMID:1234",)),),
            annotation_id="uniprotkb-json:P00330:catalytic-activity:0",
        )
        entry = UniProtCatalyticEntry("P00330", release, 239, 5, True, (assertion,), 1)
        directions = RheaDirectionsSnapshot(
            {"RHEA:10740": ("RHEA:10740", ReactionDirection.UNDEFINED)},
            "a" * 64, "2026-10-29T00:00:00Z", "https://example.org/directions.tsv",
        )
        with patch("enzymeinfocollector.evidence_cli.fetch_rhea_directions", return_value=directions), patch(
            "enzymeinfocollector.evidence_cli.fetch_uniprot_catalytic_entry", return_value=entry
        ):
            result = build_export(("P00330",), cache=None)
        serialized = json.loads(json.dumps(result))
        self.assertEqual(serialized["schema_version"], "2.0-preview")
        self.assertTrue(serialized["unknown_is_not_negative"])
        self.assertEqual(len(serialized["proteins"][0]["assertions"]), 1)
        self.assertEqual(
            serialized["proteins"][0]["assertions"][0]["relation_type"], "DIRECT_SOURCE_ANNOTATION"
        )
        self.assertEqual(serialized["proteins"][0]["assertions"][0]["evidence"][0]["citation_ids"], ["PMID:1234"])
        self.assertEqual(serialized["proteins"][0]["catalytic_comments_without_rhea"], 1)

    def test_duplicate_accessions_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "unique"):
            build_export(("P00330", "P00330"), cache=None)


if __name__ == "__main__":
    unittest.main()
