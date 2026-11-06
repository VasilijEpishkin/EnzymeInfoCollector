"""Offline tests for bounded EC/name discovery and evidence-only joins."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from enzymeinfocollector.compound_sources import (
    ChEBICollection, ChEBIResolution, CompoundSnapshot,
)
from enzymeinfocollector.discover_cli import main
from enzymeinfocollector.domain import (
    Evidence, ProteinReactionAssertion, ReactionDirection,
    ReactionParticipant, ReactionSide, ReactionVariant, SourceRelease,
)
from enzymeinfocollector.evidence_discovery import (
    collect_discovered_graph, discover_accessions, preview_candidates, resolve_ec,
)
from enzymeinfocollector.evidence_sources import DirectEvidenceDataset, ProteinEvidenceRecord
from enzymeinfocollector.rhea_participants import RheaParticipantSnapshot, RheaReaction
from enzymeinfocollector.sources import CachedResponse, DataSourceError


RELEASE = SourceRelease("UniProtKB", "synthetic", "2026-10-30T08:00:00Z")
RHEA = SourceRelease("Rhea", "synthetic", "2026-10-30T08:00:00Z")
CHEBI = SourceRelease("ChEBI", "synthetic", "2026-10-30T08:00:00Z")
HEADER = "Entry\tReviewed\tOrganism (ID)\tEC number\tProtein names\tGene Names\n"


class SearchCache:
    def __init__(self, rows=None, *, stale=False):
        self.rows = rows if rows is not None else [
            ("P04040", "reviewed", "9606", "1.11.1.6"),
            ("Q42547", "reviewed", "3702", "1.11.1.6"),
            ("P00330", "reviewed", "559292", "1.11.1.6"),
            ("P00331", "reviewed", "559292", "1.11.1.6"),
        ]
        self.urls = []
        self.stale = stale

    def get(self, url, *, ttl_seconds, refresh=False):
        self.urls.append(url)
        if "/uniprotkb/search?" in url:
            count = int(parse_qs(urlsplit(url).query)["size"][0])
            rows = self.rows[:count]
            body = HEADER + "".join("\t".join((*x, "Synthetic protein", "GENE")) + "\n" for x in rows)
        elif url.endswith("/enzyme/enzyme.dat"):
            body = "synthetic ExPASy file"
        else:
            raise AssertionError("Unexpected URL " + url)
        return CachedResponse(
            body,
            {
                "url": url, "fetched_at": "2026-10-30T08:00:00Z",
                "stale": self.stale,
            },
        )


def fake_evidence(accessions):
    def assertion(accession, master):
        return ProteinReactionAssertion(
            accession,
            ReactionVariant("RHEA:" + str(master), "RHEA:" + str(master),
                            ReactionDirection.UNDEFINED),
            RELEASE, (Evidence("UniProtKB", "ECO:0000269"),),
        )

    records = []
    for accession in accessions:
        assertions = (
            (assertion(accession, "20309"),) if accession == "P04040" else
            (assertion(accession, "10000"),) if accession == "Q42547" else ()
        )
        records.append(ProteinEvidenceRecord(
            accession, True, 1, 1, RELEASE, assertions,
            taxon_id=9606 if accession == "P04040" else 3702,
            organism_name="synthetic",
            ec_numbers=("1.11.1.6",),
        ))
    return DirectEvidenceDataset("synthetic", RHEA, tuple(records))


def chemistry():
    reactions = [
        RheaReaction(
            "RHEA:20309", "synthetic: A = B",
            (ReactionParticipant("RHEA:20309", "CHEBI:15377", ReactionSide.LEFT, Decimal("2")),),
        ),
        RheaReaction(
            "RHEA:10000", "synthetic: C = D",
            (ReactionParticipant("RHEA:10000", "CHEBI:15377", ReactionSide.RIGHT, Decimal("1")),),
        ),
    ]
    collection = ChEBICollection(
        compounds=(CompoundSnapshot(
            chebi_id="CHEBI:15377", name="water", formula="H2O",
            charge=0, smiles=None, inchi=None, inchikey=None,
            secondary_ids=(), modified_on=None, source_release=CHEBI,
        ),),
        resolutions=(ChEBIResolution("CHEBI:15377", "CHEBI:15377"),),
    )
    return reactions, collection


class DiscoveryTests(unittest.TestCase):
    def test_search_is_bounded_at_http_request_not_full_uniprot_stream(self):
        cache = SearchCache()
        result = discover_accessions(cache, "1.11.1.6", max_proteins=2)
        self.assertEqual(len(result["selected"]), 2)
        self.assertEqual([x["accession"] for x in result["selected"]], ["P04040", "Q42547"])
        self.assertTrue(result["truncated"])
        self.assertEqual(parse_qs(urlsplit(cache.urls[0]).query)["size"], ["3"])
        self.assertEqual(len(result["source"]["sha256"]), 64)

    def test_exact_taxon_vs_taxonomic_descendants(self):
        cache = SearchCache(rows=[("P04040", "reviewed", "9606", "1.11.1.6")])
        result = discover_accessions(cache, "1.11.1.6", taxon_id=9606)
        self.assertIn("organism_id%3A9606", cache.urls[0])
        self.assertEqual(result["taxon_scope"], "exact")
        result = discover_accessions(cache, "1.11.1.6", taxon_id=9606,
                                     taxon_scope="descendants")
        self.assertIn("taxonomy_id%3A9606", cache.urls[1])

    def test_candidate_preview_is_bounded_and_contains_no_catalytic_claims(self):
        cache = SearchCache()
        result = preview_candidates(cache, ec="1.11.1.6", max_proteins=2)
        self.assertEqual(result["schema_version"], "2.0-candidates-preview")
        self.assertTrue(result["semantics"]["candidate_not_catalytic_evidence"])
        self.assertTrue(result["semantics"]["search_order_not_biological_rank"])
        self.assertEqual([row["accession"] for row in result["selection"]["selected"]],
                         ["P04040", "Q42547"])
        self.assertEqual(result["selection"]["selected"][0]["protein_name"], "Synthetic protein")
        self.assertEqual(result["selection"]["selected"][0]["gene_names"], "GENE")
        self.assertTrue(result["selection"]["truncated"])
        self.assertEqual(len(cache.urls), 1)  # UniProt search; no Rhea/ChEBI

    def test_candidate_preview_cli_writes_json_without_graph_requests(self):
        cache = SearchCache()
        with tempfile.TemporaryDirectory() as directory, \
             patch("enzymeinfocollector.discover_cli.HttpCache", return_value=cache), \
             patch("enzymeinfocollector.discover_cli.collect_discovered_graph") as collect:
            target = Path(directory, "candidates.json")
            self.assertEqual(main(["--ec", "1.11.1.6",
                                   "--max-proteins", "2", "--format", "candidates-json",
                                   "--output", str(target)]), 0)
            payload = json.loads(target.read_text(encoding="utf-8"))
            self.assertTrue(payload["selection"]["truncated"])
            self.assertEqual(len(payload["selection"]["selected"]), 2)
            collect.assert_not_called()
            self.assertEqual(len(cache.urls), 1)

    def test_explicit_selection_must_be_in_current_bounded_pool(self):
        cache = SearchCache()
        with patch("enzymeinfocollector.evidence_discovery.DirectEvidenceCollector") as collector:
            with self.assertRaisesRegex(DataSourceError, "not in the current"):
                collect_discovered_graph(cache, ec="1.11.1.6",
                                         max_proteins=2, select_accession="P05023")
            collector.assert_not_called()
        with self.assertRaisesRegex(ValueError, "Invalid canonical"):
            collect_discovered_graph(SearchCache(), ec="1.11.1.6",
                                     select_accession="../etc/passwd")

    def test_preview_snapshot_can_be_pinned_to_reject_search_drift(self):
        cache = SearchCache()
        sha = discover_accessions(cache, "1.11.1.6", max_proteins=2)["source"]["sha256"]
        with patch("enzymeinfocollector.evidence_discovery.DirectEvidenceCollector") as collector:
            with self.assertRaisesRegex(DataSourceError, "snapshot changed"):
                collect_discovered_graph(cache, ec="1.11.1.6",
                    max_proteins=2, select_accession="Q42547",
                    expected_search_sha="0" * 64)
            collector.assert_not_called()
        with self.assertRaisesRegex(ValueError, "Expected candidate-search SHA"):
            collect_discovered_graph(cache, ec="1.11.1.6",
                max_proteins=2, select_accession="Q42547",
                expected_search_sha="wrong")
        self.assertEqual(len(sha), 64)

    def test_explicit_selected_accession_cannot_inherit_other_ec_evidence(self):
        cache = SearchCache()
        reactions, compounds = chemistry()
        with patch("enzymeinfocollector.evidence_discovery.DirectEvidenceCollector") as collector, \
             patch("enzymeinfocollector.reference_dataset.fetch_rhea_participants") as rhea, \
             patch("enzymeinfocollector.reference_dataset.ChEBICollector") as chebi:
            collector.return_value.collect.return_value = fake_evidence(["Q42547"])
            rhea.return_value = RheaParticipantSnapshot(
                (next(item for item in reactions if item.master_id == "RHEA:10000"),),
                RHEA, (RHEA,),
            )
            chebi.return_value.collect.return_value = compounds
            preview = preview_candidates(cache, ec="1.11.1.6", max_proteins=2)
            ref, graph = collect_discovered_graph(
                cache, ec="1.11.1.6", max_proteins=2,
                select_accession="Q42547",
                expected_search_sha=preview["selection"]["source"]["sha256"],
            )
        collector.return_value.collect.assert_called_once_with(
            ["Q42547"], refresh=False)
        selection = ref["provenance"]["discovery"]["selection"]
        self.assertEqual(selection["selection_mode"], "explicit_accession")
        self.assertEqual([x["accession"] for x in selection["candidate_pool"]],
                         ["P04040", "Q42547"])
        self.assertEqual([x["accession"] for x in selection["selected"]], ["Q42547"])
        self.assertEqual([x["data"]["source"] for x in graph["elements"]["edges"]
                          if x["data"]["relation"] == "CATALYZES"], ["protein:Q42547"])
        self.assertEqual({x["data"]["rhea_directional_id"] for x in graph["elements"]["edges"]
                          if x["data"]["relation"] == "CATALYZES"}, {"RHEA:10000"})

    def test_unreviewed_query_toggle_is_explicit(self):
        cache = SearchCache(rows=[("P04040", "unreviewed", "9606", "1.11.1.6")])
        with self.assertRaisesRegex(DataSourceError, "unreviewed protein"):
            discover_accessions(cache, "1.11.1.6")
        data = discover_accessions(cache, "1.11.1.6", reviewed_only=False)
        self.assertFalse(data["selected"][0]["reviewed"])
        self.assertNotIn("reviewed%3Atrue", cache.urls[-1])

    def test_invalid_uniprot_candidates_fail_closed(self):
        for item in (
            ("../bad", "reviewed", "9606", "1.11.1.6"),
            ("P04040", "reviewed", "0", "1.11.1.6"),
            ("P04040", "unknown", "9606", "1.11.1.6"),
            ("P04040", "reviewed", "10090", "1.11.1.6"),
        ):
            with self.subTest(item=item), self.assertRaises(DataSourceError):
                discover_accessions(SearchCache(rows=[item]), "1.11.1.6", taxon_id=9606)

    def test_empty_stale_and_caps_fail_early(self):
        with self.assertRaisesRegex(DataSourceError, "No UniProt"):
            discover_accessions(SearchCache(rows=[]), "1.11.1.6")
        with self.assertRaisesRegex(DataSourceError, "stale"):
            discover_accessions(SearchCache(stale=True), "1.11.1.6")
        for value in (0, 11, True):
            with self.assertRaises(ValueError):
                discover_accessions(SearchCache(), "1.11.1.6", max_proteins=value)

    def test_expasys_name_resolution_ambiguity_and_transfers(self):
        cache = SearchCache()
        active = {
            "enzyme_name": "Catalase", "ec_number": "1.11.1.6",
            "status": "active", "alternative_names": [],
        }
        with patch("enzymeinfocollector.evidence_discovery.parse_enzyme_dat", return_value=[active]), \
             patch("enzymeinfocollector.evidence_discovery.find_enzyme_entries",
                   return_value=[active]) as finder:
            ec, metadata = resolve_ec(cache, enzyme="Catalase")
        self.assertEqual(ec, "1.11.1.6")
        self.assertEqual(metadata["resolved_by"], "ExPASy ENZYME")
        self.assertEqual(metadata["catalogue"]["url"], cache.urls[0])
        finder.assert_called_once()
        second = {**active, "ec_number": "1.11.1.7"}
        with patch("enzymeinfocollector.evidence_discovery.parse_enzyme_dat", return_value=[active, second]), \
             patch("enzymeinfocollector.evidence_discovery.find_enzyme_entries",
                   return_value=[active, second]):
            with self.assertRaisesRegex(DataSourceError, "Ambiguous"):
                resolve_ec(cache, enzyme="Catalase")
        with patch("enzymeinfocollector.evidence_discovery.parse_enzyme_dat", return_value=[active]), \
             patch("enzymeinfocollector.evidence_discovery.find_enzyme_entries",
                   return_value=[{**active, "status": "transferred"}]):
            with self.assertRaisesRegex(DataSourceError, "transferred"):
                resolve_ec(cache, enzyme="Catalase")
        with patch("enzymeinfocollector.evidence_discovery.parse_enzyme_dat", return_value=[active]), \
             patch("enzymeinfocollector.evidence_discovery.find_enzyme_entries",
                   return_value=[{**active, "status": "deleted"}]):
            with self.assertRaisesRegex(DataSourceError, "deleted"):
                resolve_ec(cache, enzyme="Catalase")


    def test_ec_is_classification_only_two_direct_masters_not_cartesian(self):
        cache = SearchCache()
        reactions, compounds = chemistry()
        with patch("enzymeinfocollector.evidence_discovery.DirectEvidenceCollector") as client, \
             patch("enzymeinfocollector.reference_dataset.fetch_rhea_participants") as rhea, \
             patch("enzymeinfocollector.reference_dataset.ChEBICollector") as chebi:
            client.return_value.collect.return_value = fake_evidence(
                ["P04040", "Q42547", "P00330"]
            )
            rhea.return_value = RheaParticipantSnapshot(
                tuple(reactions), RHEA, (RHEA,),
            )
            chebi.return_value.collect.return_value = compounds
            ref, graph = collect_discovered_graph(cache, ec="1.11.1.6", max_proteins=3)
        self.assertEqual(rhea.call_args.args[0], {"RHEA:20309", "RHEA:10000"})
        self.assertEqual(len([x for x in graph["elements"]["edges"]
                              if x["data"]["relation"] == "CATALYZES"]), 2)
        self.assertEqual(len([x for x in graph["elements"]["edges"]
                              if x["data"]["relation"] == "HAS_PARTICIPANT"]), 2)
        self.assertEqual(len(ref["reactions"]), 2)
        self.assertEqual(ref["provenance"]["discovery"]["without_direct_rhea_annotation"],
                         ["P00330"])
        self.assertEqual(ref["provenance"]["discovery"]["selection"]["truncated"], True)
        self.assertEqual(ref["provenance"]["discovery"]["direct_rhea_annotated"],
                         ["P04040", "Q42547"])
        self.assertEqual(len(graph["reference_sha256"]), 64)
        self.assertEqual(len(ref["semantic_sha256"]), 64)

    def test_no_direct_associations_is_never_transformed_from_ec(self):
        cache = SearchCache(rows=[("P00330", "reviewed", "559292", "1.11.1.6")])
        with patch("enzymeinfocollector.evidence_discovery.DirectEvidenceCollector") as client:
            client.return_value.collect.return_value = fake_evidence(["P00330"])
            with self.assertRaisesRegex(DataSourceError, "No selected proteins had a direct"):
                collect_discovered_graph(cache, ec="1.11.1.6")
        self.assertEqual(len(cache.urls), 1)

    def test_discovery_cli_format_and_atomic_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "network.json")
            with patch("enzymeinfocollector.discover_cli.collect_discovered_graph") as fn:
                fn.return_value = (
                    {"provenance": {"discovery": {"selection": {
                        "selected": [{"accession": "P04040"}], "truncated": False,
                    }, "without_direct_rhea_annotation": []}}},
                    {"schema_version": "2.0-graph-preview", "elements": {
                        "nodes": [], "edges": [],
                    }, "provenance": {}},
                )
                self.assertEqual(main(["--ec", "1.11.1.6", "--output", str(path)]), 0)
                self.assertIn("2.0-graph-preview", path.read_text(encoding="utf-8"))
                original = path.read_bytes()
                fn.side_effect = DataSourceError("upstream unavailable")
                self.assertEqual(main(["--ec", "1.11.1.6", "-o", str(path)]), 1)
                self.assertEqual(path.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
