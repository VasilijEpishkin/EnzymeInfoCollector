"""Offline contract tests: all identifiers and biological relationships are synthetic."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal
import json
from pathlib import Path
import unittest

from enzymeinfocollector.domain import (
    AssertionMethod,
    Evidence,
    ProteinECClassification,
    ProteinReactionAssertion,
    ProteinSequenceSnapshot,
    Reaction,
    ReactionDirection,
    ReactionECClassification,
    ReactionParticipant,
    ReactionSide,
    ReactionVariant,
    SourceRelease,
)


FIXTURE = Path(__file__).parent / "fixtures" / "evidence_graph" / "ambiguous_ec.json"


class DomainContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.release = SourceRelease(
            database="UniProtKB",
            release="synthetic-test-release",
            retrieved_at="2026-10-01T00:00:00Z",
        )

    def test_shared_ec_does_not_generate_protein_reaction_cartesian_product(self) -> None:
        fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        self.assertTrue(fixture["synthetic"])
        release = SourceRelease(**fixture["source_release"])
        protein_ec = [
            ProteinECClassification(source_release=release, **row)
            for row in fixture["protein_ec"]
        ]
        reaction_ec = [
            ReactionECClassification(source_release=release, **row)
            for row in fixture["reaction_ec"]
        ]
        assertions = [
            ProteinReactionAssertion(
                accession=row["accession"],
                reaction=ReactionVariant(
                    rhea_id=row["rhea_id"],
                    master_id=row["master_id"],
                    direction=row["direction"],
                ),
                source_release=release,
                evidence=tuple(Evidence(**e) for e in row["evidence"]),
            )
            for row in fixture["protein_reactions"]
        ]
        self.assertEqual({item.ec_number for item in protein_ec}, {"1.1.1.1"})
        self.assertEqual({item.ec_number for item in reaction_ec}, {"1.1.1.1"})
        self.assertEqual(len(protein_ec), 2)
        self.assertEqual(len(reaction_ec), 2)
        self.assertEqual(
            {a.snapshot_key[:2] for a in assertions},
            {("P00001", "RHEA:10001")},
        )
        self.assertNotIn(("P00002", "RHEA:10005"), {a.snapshot_key[:2] for a in assertions})

    def test_evidence_can_have_multiple_sources_and_no_pmid(self) -> None:
        variant = ReactionVariant("10001", "10000", ReactionDirection.LEFT_TO_RIGHT)
        assertion = ProteinReactionAssertion(
            accession="P00001",
            reaction=variant,
            source_release=self.release,
            evidence=(
                Evidence("UniProtKB", "ECO:0000269", AssertionMethod.MANUAL, citation_ids=("PMID:1234",)),
                Evidence("UniProtKB", "ECO:0000250", AssertionMethod.MANUAL),
            ),
        )
        self.assertEqual(assertion.reaction.rhea_id, "RHEA:10001")
        self.assertEqual(len(assertion.evidence), 2)
        self.assertEqual(assertion.evidence[1].citation_ids, ())
        self.assertEqual(assertion.snapshot_key, ("P00001", "RHEA:10001", "UniProtKB", "synthetic-test-release"))
        with self.assertRaises(FrozenInstanceError):
            assertion.accession = "P00002"

    def test_missing_eco_is_unknown_evidence_not_negative_result(self) -> None:
        evidence = Evidence(source_database="Rhea", assertion_method="unknown")
        self.assertIsNone(evidence.eco_code)
        self.assertEqual(evidence.citation_ids, ())
        with self.assertRaisesRegex(ValueError, "at least one Evidence"):
            ProteinReactionAssertion(
                "P00001",
                ReactionVariant("RHEA:10001", "RHEA:10000", "left_to_right"),
                self.release,
                (),
            )

    def test_rhea_master_and_directional_ids_are_distinct(self) -> None:
        reaction = Reaction("10000", "A = B")
        lr = ReactionVariant("RHEA:10001", reaction.master_id, "left_to_right")
        rl = ReactionVariant("RHEA:10002", reaction.master_id, "right_to_left")
        self.assertEqual(lr.master_id, rl.master_id)
        self.assertNotEqual(lr.rhea_id, rl.rhea_id)
        self.assertNotEqual(lr.direction, rl.direction)
        with self.assertRaisesRegex(ValueError, "Invalid RHEA"):
            ReactionVariant("not-rhea", "10000", "undefined")

    def test_reaction_participant_preserves_side_and_stoichiometry(self) -> None:
        left = ReactionParticipant("10000", "15377", ReactionSide.LEFT, Decimal("2"))
        right = ReactionParticipant("RHEA:10000", "CHEBI:15377", "right", "1.5")
        self.assertEqual(left.chebi_id, "CHEBI:15377")
        self.assertEqual(left.coefficient, Decimal("2"))
        self.assertEqual(right.coefficient, Decimal("1.5"))
        self.assertNotEqual(left.side, right.side)
        for invalid in ("0", "-1", "NaN", "Infinity", "garbage"):
            with self.subTest(coefficient=invalid), self.assertRaisesRegex(ValueError, "positive finite"):
                ReactionParticipant("10000", "15377", "left", invalid)

    def test_sequence_snapshot_version_and_hash(self) -> None:
        snapshot = ProteinSequenceSnapshot(
            accession="P00001",
            sequence="MSTAGKVIKCK",
            sequence_version=2,
            entry_version=17,
            source_release=self.release,
        )
        self.assertEqual(len(snapshot.sequence_sha256), 64)
        self.assertEqual(snapshot.isoform, "canonical")
        self.assertEqual(
            snapshot.sequence_sha256,
            ProteinSequenceSnapshot("P00001", "MSTAGKVIKCK", 3, self.release).sequence_sha256,
        )
        with self.assertRaisesRegex(ValueError, "sequence_version"):
            ProteinSequenceSnapshot("P00001", "MSTAGKVIKCK", 0, self.release)
        with self.assertRaisesRegex(ValueError, "sequence must"):
            ProteinSequenceSnapshot("P00001", "MST AG", 1, self.release)

    def test_source_release_requires_timezone_and_valid_hash(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone"):
            SourceRelease("UniProtKB", "2026_03", "2026-10-01T12:00:00")
        with self.assertRaisesRegex(ValueError, "sha256"):
            SourceRelease("UniProtKB", "2026_03", "2026-10-01T12:00:00Z", "abc")
        self.assertEqual(
            SourceRelease("UniProtKB", "2026_03", "2026-10-01T12:00:00+00:00", "a" * 64).sha256,
            "a" * 64,
        )

    def test_evidence_codes_are_validated(self) -> None:
        with self.assertRaisesRegex(ValueError, "eco_code"):
            Evidence("UniProtKB", "experimental")
        with self.assertRaisesRegex(ValueError, "citation ID"):
            Evidence("UniProtKB", citation_ids=("",))


if __name__ == "__main__":
    unittest.main()
