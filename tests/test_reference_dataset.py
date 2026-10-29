"""Deterministic synthetic reference projections; no HTTP or real evidence claims."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import unittest

from enzymeinfocollector.compound_sources import (
    ChEBICollection, ChEBIResolution, CompoundSnapshot,
)
from enzymeinfocollector.domain import (
    Evidence, ProteinReactionAssertion, ReactionDirection, ReactionVariant, SourceRelease,
)
from enzymeinfocollector.evidence_sources import DirectEvidenceDataset, ProteinEvidenceRecord
from enzymeinfocollector.reference_dataset import build_reference_dataset, reference_json
from enzymeinfocollector.sources import DataSourceError


UNIPROT_RELEASE = SourceRelease("UniProtKB", "synthetic", "2026-10-29T00:00:00Z")
RHEA_RELEASE = SourceRelease("Rhea", "synthetic-snapshot", "2026-10-29T00:00:00Z", "a" * 64)
CHEBI_RELEASE = SourceRelease("ChEBI", "api-response-sha256:synthetic", "2026-10-29T00:00:00Z")


def sample():
    assertion = ProteinReactionAssertion(
        "P00001",
        ReactionVariant("RHEA:10001", "RHEA:10000", ReactionDirection.LEFT_TO_RIGHT),
        UNIPROT_RELEASE,
        (Evidence("UniProtKB", "ECO:0000269", citation_ids=("PubMed:1234567",)),),
    )
    evidence = DirectEvidenceDataset(
        "synthetic", RHEA_RELEASE, (
            ProteinEvidenceRecord("P00002", False, 2, 1, UNIPROT_RELEASE, ()),
            ProteinEvidenceRecord("P00001", True, 3, 1, UNIPROT_RELEASE, (assertion,)),
        ),
    )
    water = CompoundSnapshot(
        chebi_id="CHEBI:15377",
        name="water",
        formula="H2O",
        charge=0,
        smiles="[H]O[H]",
        inchi="InChI=1S/H2O/h1H2",
        inchikey="XLYOFNOQVPJJNP-UHFFFAOYSA-N",
        secondary_ids=("CHEBI:5585",),
        modified_on=None,
        source_release=CHEBI_RELEASE,
    )
    compounds = ChEBICollection(
        compounds=(water,),
        resolutions=(ChEBIResolution("CHEBI:5585", "CHEBI:15377"),),
    )
    rhea_records = (
        {"rhea_id": "10004", "equation": "unrelated EC class", "chebi_ids": ["12345"],
         "ec_numbers": ["1.1.1.1"]},
        {"rhea_id": "10000", "equation": "synthetic test reaction", "chebi_ids": ["5585", "99999"],
         "ec_numbers": ["1.1.1.1"]},
    )
    return evidence, rhea_records, compounds


class ReferenceDatasetTests(unittest.TestCase):
    def build(self, evidence=None, records=None, compounds=None, rhea_source=RHEA_RELEASE):
        base_evidence, base_records, base_compounds = sample()
        return build_reference_dataset(
            evidence if evidence is not None else base_evidence,
            records if records is not None else base_records,
            compounds if compounds is not None else base_compounds,
            rhea_source=rhea_source,
        )

    def test_only_direct_uniprot_assertion_survives_same_ec(self):
        data = self.build()
        self.assertEqual([entry["accession"] for entry in data["proteins"]], ["P00001", "P00002"])
        self.assertEqual(len(data["proteins"][0]["direct_assertions"]), 1)
        self.assertEqual(data["proteins"][1]["direct_assertions"], [])
        self.assertEqual([entry["master_id"] for entry in data["reactions"]], ["RHEA:10000"])
        self.assertTrue(data["semantics"]["unknown_is_not_negative"])
        self.assertEqual(data["semantics"]["ec_join"], "prohibited")

    def test_chebi_alias_resolved_and_unknown_not_silently_dropped(self):
        data = self.build()
        links = data["reactions"][0]["compound_membership"]
        self.assertEqual([entry["reported_chebi_id"] for entry in links],
                         ["CHEBI:5585", "CHEBI:99999"])
        self.assertEqual(links[0]["primary_chebi_id"], "CHEBI:15377")
        self.assertEqual(links[1]["resolution_status"], "unresolved")
        self.assertEqual(data["unresolved"]["unresolved_chebi_ids"], ["CHEBI:99999"])
        self.assertEqual(data["compounds"][0]["name"], "water")
        self.assertEqual(data["compounds"][0]["source_release"]["database"], "ChEBI")

    def test_never_guesses_stoichiometry_or_substrate_side(self):
        data = self.build()
        for member in data["reactions"][0]["compound_membership"]:
            self.assertIsNone(member["side"])
            self.assertIsNone(member["coefficient"])

    def test_reordering_snapshots_is_bitwise_stable(self):
        original = self.build()
        evidence, records, compounds = sample()
        reordered = replace(evidence, proteins=tuple(reversed(evidence.proteins)))
        permuted = self.build(
            evidence=reordered, records=tuple(reversed(records)),
            compounds=replace(compounds, resolutions=tuple(reversed(compounds.resolutions))),
        )
        self.assertEqual(reference_json(original), reference_json(permuted))
        self.assertEqual(original["semantic_sha256"], permuted["semantic_sha256"])
        digest = original.pop("semantic_sha256")
        canonical = json.dumps(original, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        self.assertEqual(hashlib.sha256(canonical).hexdigest(), digest)

    def test_missing_reaction_is_explicit_unresolved(self):
        data = self.build(records=[])
        self.assertEqual(data["unresolved"]["missing_rhea_master_ids"], ["RHEA:10000"])
        self.assertEqual(data["reactions"][0]["status"], "missing_rhea_record")
        self.assertEqual(data["reactions"][0]["compound_membership"], [])

    def test_conflicting_same_rhea_id_rejected(self):
        _, records, _ = sample()
        conflicting = tuple(records) + (
            {"rhea_id": "10000", "equation": "conflicting equation", "chebi_ids": ["5585"]},
        )
        with self.assertRaisesRegex(DataSourceError, "Conflicting"):
            self.build(records=conflicting)

    def test_mismatched_source_release_rejected(self):
        evidence, _, _ = sample()
        other = replace(evidence.proteins[0],
                        source_release=SourceRelease("UniProtKB", "different", "2026-10-29T00:00:00Z"))
        with self.assertRaisesRegex(DataSourceError, "UniProt release mismatch"):
            self.build(evidence=replace(evidence, proteins=(other, evidence.proteins[1])))

    def test_alias_must_resolve_to_a_real_compound_record(self):
        evidence, records, compounds = sample()
        missing = replace(compounds, compounds=())
        with self.assertRaisesRegex(DataSourceError, "missing compound"):
            self.build(evidence=evidence, records=records, compounds=missing)

    def test_invalid_ids_rejected(self):
        for record in (
            {"rhea_id": "RHEA:0", "chebi_ids": []},
            {"rhea_id": "RHEA:../../", "chebi_ids": []},
        ):
            with self.subTest(record=record), self.assertRaises(DataSourceError):
                self.build(records=[record])

    def test_preserves_original_evidence_and_snapshot_provenance(self):
        data = self.build()
        assertion = data["proteins"][0]["direct_assertions"][0]
        self.assertEqual(assertion["reaction"]["rhea_id"], "RHEA:10001")
        self.assertEqual(assertion["evidence"][0]["eco_code"], "ECO:0000269")
        self.assertEqual(assertion["evidence"][0]["citation_ids"], ("PubMed:1234567",))
        self.assertEqual(data["provenance"]["rhea_reaction_records"]["sha256"], "a" * 64)
        self.assertEqual(len(data["semantic_sha256"]), 64)


if __name__ == "__main__":
    unittest.main()
