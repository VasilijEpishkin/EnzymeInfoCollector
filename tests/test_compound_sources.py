"""Offline contract tests for ChEBI 2.0 compound enrichment."""

from __future__ import annotations

from dataclasses import asdict
import copy
import hashlib
import json
import unittest

from enzymeinfocollector.compound_sources import (
    CHEBI_COMPOUND_URL,
    CHEBI_TTL_SECONDS,
    ChEBICollector,
    normalize_chebi_id,
    parse_chebi_compound,
)
from enzymeinfocollector.domain import SourceRelease
from enzymeinfocollector.sources import CachedResponse, DataSourceError


# Representative fields from the official ChEBI 2.0 REST water response.
WATER = {
    "id": 15377,
    "chebi_accession": "CHEBI:15377",
    "name": "water",
    "secondary_ids": ["CHEBI:5585", "CHEBI:10743"],
    "chemical_data": {"formula": "H2O", "charge": 0, "mass": "18.015"},
    "default_structure": {
        "smiles": "[H]O[H]",
        "standard_inchi": "InChI=1S/H2O/h1H2",
        "standard_inchi_key": "XLYOFNOQVPJJNP-UHFFFAOYSA-N",
    },
    "modified_on": "2022-06-28T16:33:13Z",
}


def compound_url(chebi_id: str) -> str:
    return CHEBI_COMPOUND_URL.format(chebi_id=chebi_id)


class StubCache:
    def __init__(self, payload: dict | None = None):
        self.payload = payload if payload is not None else WATER
        self.calls: list[tuple[str, int, bool]] = []
        self.stale_url: str | None = None
        self.text_override: str | None = None

    def get(self, url: str, *, ttl_seconds: int, refresh: bool = False) -> CachedResponse:
        self.calls.append((url, ttl_seconds, refresh))
        return CachedResponse(
            self.text_override if self.text_override is not None else json.dumps(self.payload),
            {"fetched_at": "2026-10-29T12:00:00Z", "stale": url == self.stale_url},
        )


class ChEBIEnrichmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.release = SourceRelease("ChEBI", "api-response-sha256:example", "2026-10-29T12:00:00Z")

    def test_normalizes_ids_without_inventing_chemistry(self) -> None:
        self.assertEqual(normalize_chebi_id("15377"), "CHEBI:15377")
        self.assertEqual(normalize_chebi_id("chebi:15377"), "CHEBI:15377")
        for invalid in ("", "CHEBI:0", "CHEBI:-1", "CHEBI:15377/../", "CHEBI:12 3", None):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                normalize_chebi_id(invalid)

    def test_parses_real_chebi_2_structure_and_secondary_ids(self) -> None:
        compound = parse_chebi_compound(WATER, "CHEBI:5585", self.release)
        self.assertEqual(compound.chebi_id, "CHEBI:15377")
        self.assertEqual(compound.name, "water")
        self.assertEqual(compound.formula, "H2O")
        self.assertEqual(compound.charge, 0)
        self.assertEqual(compound.smiles, "[H]O[H]")
        self.assertEqual(compound.inchi, "InChI=1S/H2O/h1H2")
        self.assertEqual(compound.inchikey, "XLYOFNOQVPJJNP-UHFFFAOYSA-N")
        self.assertEqual(compound.secondary_ids, ("CHEBI:5585", "CHEBI:10743"))
        self.assertEqual(compound.modified_on, "2022-06-28T16:33:13Z")
        self.assertEqual(asdict(compound)["source_release"]["database"], "ChEBI")

    def test_optional_structure_and_formula_stay_unknown(self) -> None:
        payload = copy.deepcopy(WATER)
        payload["chemical_data"] = None
        payload["default_structure"] = None
        compound = parse_chebi_compound(payload, "CHEBI:15377", self.release)
        self.assertIsNone(compound.charge)
        self.assertIsNone(compound.formula)
        self.assertIsNone(compound.smiles)
        self.assertIsNone(compound.inchi)
        self.assertIsNone(compound.inchikey)

    def test_rejects_unrelated_primary_and_malformed_payloads(self) -> None:
        with self.assertRaisesRegex(DataSourceError, "without confirming"):
            parse_chebi_compound(WATER, "CHEBI:12345", self.release)
        for patch, message in [
            ({"id": 42}, "numeric ID"),
            ({"secondary_ids": "CHEBI:5585"}, "secondary_ids"),
            ({"chemical_data": {"charge": "0"}}, "charge"),
            ({"default_structure": []}, "default_structure"),
            ({"modified_on": "yesterday"}, "modified_on"),
            ({"name": ""}, "missing a name"),
        ]:
            payload = copy.deepcopy(WATER)
            payload.update(patch)
            with self.subTest(patch=patch), self.assertRaisesRegex(DataSourceError, message):
                parse_chebi_compound(payload, "CHEBI:15377", self.release)

    def test_cache_and_content_hash_provenance(self) -> None:
        cache = StubCache()
        result = ChEBICollector(cache).collect(["CHEBI:15377", "15377"], refresh=True)
        self.assertEqual(len(cache.calls), 1)
        self.assertEqual(cache.calls[0], (compound_url("CHEBI:15377"), CHEBI_TTL_SECONDS, True))
        self.assertEqual(result.resolutions[0].primary_id, "CHEBI:15377")
        self.assertEqual(len(result.compounds), 1)
        release = result.compounds[0].source_release
        self.assertEqual(release.database, "ChEBI")
        self.assertEqual(release.sha256, hashlib.sha256(json.dumps(WATER).encode()).hexdigest())
        self.assertTrue(release.release.startswith("api-response-sha256:"))
        self.assertEqual(release.source_url, compound_url("CHEBI:15377"))

    def test_secondary_and_primary_queries_deduplicate_to_one_compound(self) -> None:
        cache = StubCache()
        result = ChEBICollector(cache).collect(["CHEBI:15377", "CHEBI:5585"])
        self.assertEqual(len(result.compounds), 1)
        self.assertEqual(
            [(entry.requested_id, entry.primary_id) for entry in result.resolutions],
            [("CHEBI:5585", "CHEBI:15377"), ("CHEBI:15377", "CHEBI:15377")],
        )
        self.assertEqual(len(cache.calls), 2)

    def test_enriches_rhea_participant_ids_without_inventing_sides_or_protein_edges(self) -> None:
        cache = StubCache()
        dataset = ChEBICollector(cache).enrich_rhea_records([
            {"rhea_id": "10740", "chebi_ids": ["15377", "5585", "15377"]},
            {"rhea_id": "RHEA:10741", "chebi_ids": ["15377"]},
        ])
        self.assertEqual(len(dataset.compounds), 1)
        self.assertEqual(len(dataset.rhea_links), 3)
        self.assertEqual(
            [(link.rhea_id, link.reported_chebi_id, link.primary_chebi_id) for link in dataset.rhea_links],
            [
                ("RHEA:10740", "CHEBI:5585", "CHEBI:15377"),
                ("RHEA:10740", "CHEBI:15377", "CHEBI:15377"),
                ("RHEA:10741", "CHEBI:15377", "CHEBI:15377"),
            ],
        )
        self.assertFalse(hasattr(dataset.rhea_links[0], "side"))
        self.assertFalse(hasattr(dataset.rhea_links[0], "accession"))

    def test_bad_input_and_overlarge_batches_do_not_fetch(self) -> None:
        cache = StubCache()
        collector = ChEBICollector(cache)
        for ids in ("CHEBI:15377", ["CHEBI:0"], [str(i) for i in range(1, 102)]):
            with self.subTest(ids=ids), self.assertRaises(ValueError):
                collector.collect(ids)
        with self.assertRaises(ValueError):
            collector.enrich_rhea_records([{"rhea_id": "10740", "chebi_ids": "15377"}])
        with self.assertRaises(ValueError):
            collector.enrich_rhea_records([{"rhea_id": "invalid", "chebi_ids": ["15377"]}])
        self.assertEqual(cache.calls, [])

    def test_stale_and_invalid_json_fail_closed(self) -> None:
        cache = StubCache()
        cache.stale_url = compound_url("CHEBI:15377")
        with self.assertRaisesRegex(DataSourceError, "stale"):
            ChEBICollector(cache).collect(["15377"])
        cache = StubCache()
        cache.text_override = "{bad json"
        with self.assertRaisesRegex(DataSourceError, "Invalid ChEBI JSON"):
            ChEBICollector(cache).collect(["15377"])

    def test_empty_participant_list_does_not_fetch(self) -> None:
        cache = StubCache()
        dataset = ChEBICollector(cache).enrich_rhea_records([{"rhea_id": "10740", "chebi_ids": []}])
        self.assertEqual(dataset.compounds, ())
        self.assertEqual(dataset.rhea_links, ())
        self.assertEqual(cache.calls, [])


if __name__ == "__main__":
    unittest.main()
