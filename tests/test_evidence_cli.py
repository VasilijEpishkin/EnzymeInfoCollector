"""Offline regression tests for the unified direct-evidence exporter and validator."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from enzymeinfocollector.domain import (
    Evidence, ProteinReactionAssertion, ReactionDirection, ReactionVariant, SourceRelease,
)
from enzymeinfocollector.evidence_cli import (
    RHEA_SPROT_URL, evidence_export, main, parse_sprot_mapping, validate_sprot,
)
from enzymeinfocollector.evidence_sources import (
    DirectEvidenceDataset, ProteinEvidenceRecord, RHEA_DIRECTIONS_URL, parse_rhea_directions,
)
from enzymeinfocollector.sources import CachedResponse, DataSourceError


DIRECTIONS = (
    "RHEA_ID_MASTER\tRHEA_ID_LR\tRHEA_ID_RL\tRHEA_ID_BI\n"
    "10740\t10741\t10742\t10743\n"
)
MAPPING = (
    "RHEA_ID\tDIRECTION\tMASTER_ID\tID\n"
    "10740\tUN\t10740\tP00330\n"
    "10741\tLR\t10740\tP00330\n"
)


def make_dataset(*, reviewed=True, with_direction=True):
    release = SourceRelease("UniProtKB", "2026_03", "2026-10-29T12:00:00Z", sha256="a" * 64)
    from hashlib import sha256
    rhea_release = SourceRelease(
        "Rhea", "directions-sha256:test", "2026-10-29T12:00:00Z",
        sha256=sha256(DIRECTIONS.encode()).hexdigest(),
    )
    assertions = [
        ProteinReactionAssertion(
            "P00330",
            ReactionVariant("RHEA:10740", "RHEA:10740", ReactionDirection.UNDEFINED),
            release,
            (Evidence("UniProtKB", "ECO:0000269", citation_ids=("PubMed:6985717",)),),
        )
    ]
    if with_direction:
        assertions.append(ProteinReactionAssertion(
            "P00330",
            ReactionVariant("RHEA:10741", "RHEA:10740", ReactionDirection.LEFT_TO_RIGHT),
            release,
            (Evidence("UniProtKB"),),
        ))
    return DirectEvidenceDataset("2026_03", rhea_release, (
        ProteinEvidenceRecord("P00330", reviewed, 239, 5, release, tuple(assertions)),
    ))


class StubCache:
    def __init__(self, mapping=MAPPING, directions=DIRECTIONS):
        self.mapping = mapping
        self.directions = directions
        self.stale_url = None
        self.calls = []

    def get(self, url, *, ttl_seconds, refresh=False):
        self.calls.append(url)
        assert url == RHEA_DIRECTIONS_URL
        return CachedResponse(self.directions, {
            "fetched_at": "2026-10-29T12:00:00Z",
            "stale": self.stale_url == url,
        })

    @contextmanager
    def open_text(self, url, *, ttl_seconds, refresh=False):
        self.calls.append(url)
        assert url == RHEA_SPROT_URL
        yield io.StringIO(self.mapping), {
            "fetched_at": "2026-10-29T12:00:00Z",
            "stale": self.stale_url == url,
        }


class EvidenceCLITests(unittest.TestCase):
    def test_full_export_is_json_serializable_and_retains_per_direction_evidence(self):
        result = json.loads(json.dumps(evidence_export(make_dataset())))
        self.assertEqual(result["schema_version"], "2.0-preview")
        self.assertTrue(result["unknown_is_not_negative"])
        assertions = result["proteins"][0]["assertions"]
        self.assertEqual([x["reaction"]["rhea_id"] for x in assertions], ["RHEA:10740", "RHEA:10741"])
        self.assertEqual(assertions[0]["evidence"][0]["citation_ids"], ["PubMed:6985717"])
        self.assertEqual(assertions[1]["evidence"][0]["citation_ids"], [])
        self.assertIsNone(assertions[1]["evidence"][0]["eco_code"])
        self.assertNotEqual(assertions[0]["evidence"], assertions[1]["evidence"])

    def test_exact_master_and_directional_mapping(self):
        result = validate_sprot(make_dataset(), StubCache())
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["proteins"][0]["rest_reaction_ids"], 2)
        self.assertEqual(result["proteins"][0]["rhea_mapping_ids"], 2)
        self.assertEqual(result["mapping"]["rows"], 2)
        self.assertEqual(len(result["mapping"]["utf8_sha256"]), 64)

    def test_difference_is_reported_not_silently_dropped(self):
        result = validate_sprot(make_dataset(with_direction=False), StubCache())
        self.assertEqual(result["status"], "mismatch")
        self.assertEqual(result["proteins"][0]["only_in_rhea_mapping"], ["RHEA:10741"])
        self.assertEqual(result["proteins"][0]["only_in_uniprot"], [])

    def test_direct_id_mismatch_is_also_reported(self):
        mapping = MAPPING.replace("10741\tLR\t10740\tP00330\n", "")
        result = validate_sprot(make_dataset(), StubCache(mapping=mapping))
        self.assertEqual(result["proteins"][0]["only_in_uniprot"], ["RHEA:10741"])

    def test_tr_embl_rejected_from_swiss_prot_mapping(self):
        cache = StubCache()
        with self.assertRaisesRegex(ValueError, "TrEMBL"):
            validate_sprot(make_dataset(reviewed=False), cache)
        self.assertEqual(cache.calls, [])

    def test_saturated_or_stale_mapping_cannot_claim_success(self):
        cache = StubCache()
        cache.stale_url = RHEA_SPROT_URL
        with self.assertRaisesRegex(DataSourceError, "stale"):
            validate_sprot(make_dataset(), cache)
        cache = StubCache(mapping="RHEA_ID\tDIRECTION\tMASTER_ID\tID\n")
        with self.assertRaisesRegex(DataSourceError, "empty"):
            validate_sprot(make_dataset(), cache)

    def test_direction_snapshot_mismatch_fails_closed(self):
        cache = StubCache(directions=DIRECTIONS.replace("10741", "10749"))
        with self.assertRaisesRegex(DataSourceError, "snapshot changed"):
            validate_sprot(make_dataset(), cache)

    def test_tsv_invalid_variant_fails_closed(self):
        directions = parse_rhea_directions(DIRECTIONS, make_dataset().rhea_directions_snapshot)
        for mapping, message in (
            (MAPPING.replace("10741\tLR", "10741\tRL"), "direction/master mismatch"),
            (MAPPING.replace("10741\tLR\t10740", "10741\tLR\t10742"), "direction/master mismatch"),
            (MAPPING.replace("RHEA_ID\tDIRECTION\tMASTER_ID\tID", "RHEA_ID\tID"), "header"),
            (MAPPING + "10741\tLR\t10740\tP00330\textra\n", "Malformed"),
        ):
            with self.subTest(message=message), self.assertRaisesRegex(DataSourceError, message):
                parse_sprot_mapping(io.StringIO(mapping), {"P00330"}, directions)

    def test_output_file_is_valid_json_and_uses_atomic_writer(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence.json"
            with patch("enzymeinfocollector.evidence_cli.DirectEvidenceCollector") as factory:
                factory.return_value.collect.return_value = make_dataset()
                result = main(["collect", "--accession", "P00330", "--output", str(output),
                               "--cache-dir", directory])
            self.assertEqual(result, 0)
            document = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(document["proteins"][0]["accession"], "P00330")
            self.assertEqual(document["proteins"][0]["assertions"][0]["reaction"]["rhea_id"], "RHEA:10740")


if __name__ == "__main__":
    unittest.main()
