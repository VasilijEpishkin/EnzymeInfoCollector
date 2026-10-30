"""Offline, bounded Rhea RDF participant parser tests."""

from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from enzymeinfocollector.domain import ReactionSide
from enzymeinfocollector.rhea_participants import (
    BATCH_SIZE, MAX_MASTER_IDS, RHEA_SPARQL_URL,
    build_rhea_participants_query, parse_rhea_participants,
    fetch_rhea_participants,
)
from enzymeinfocollector.sources import CachedResponse, DataSourceError


def uri(value):
    return {"type": "uri", "value": value}


def literal(value):
    return {"type": "literal", "value": str(value)}


def row(side, coefficient, chebi, *, master="10740", order=None):
    return {
        "reaction": uri("http://rdf.rhea-db.org/" + master),
        "side": uri(f"http://rdf.rhea-db.org/{master}_{side}"),
        "order": literal(order if order is not None else ("1" if side == "L" else "2")),
        "coefficient": literal(coefficient),
        "chebi": uri("http://purl.obolibrary.org/obo/CHEBI_" + str(chebi)),
        "equation": literal("synthetic reaction"),
    }


def payload(rows):
    return json.dumps({"head": {"vars": []}, "results": {"bindings": rows}})


SAMPLE = [
    row("L", "2", 15377),
    row("L", "1", 57540),
    row("R", "3.5", 17087),
]


class StubCache:
    def __init__(self, raw=None):
        self.raw = raw or payload(SAMPLE)
        self.urls = []
        self.stale = False

    def get(self, url, *, ttl_seconds, refresh=False):
        self.urls.append(url)
        return CachedResponse(
            self.raw,
            {"fetched_at": "2026-10-30T08:00:00Z", "stale": self.stale},
        )


class RheaParticipantsTests(unittest.TestCase):
    def test_query_uses_reaction_ids_not_ec_and_explicit_coefficients(self):
        query = build_rhea_participants_query(["10740", "RHEA:10740"])
        self.assertIn("rh:coefficient ?coefficient", query)
        self.assertIn("rh:curatedOrder ?order", query)
        self.assertIn("?compound rh:chebi ?chebi", query)
        self.assertNotIn("ec:", query.lower())
        self.assertIn("LIMIT 400", query)
        with self.assertRaises(ValueError):
            build_rhea_participants_query(["10740> } UNION { ?s ?p ?o"])
        with self.assertRaises(ValueError):
            build_rhea_participants_query([str(i+10000) for i in range(BATCH_SIZE+1)])

    def test_parses_left_right_and_decimal_coefficients(self):
        reaction, = parse_rhea_participants(payload(SAMPLE), ["RHEA:10740"])
        self.assertEqual(reaction.master_id, "RHEA:10740")
        self.assertEqual(len(reaction.participants), 3)
        self.assertEqual(
            [(x.side.value, x.chebi_id, str(x.coefficient)) for x in reaction.participants],
            [("left", "CHEBI:15377", "2"), ("left", "CHEBI:57540", "1"),
             ("right", "CHEBI:17087", "3.5")],
        )
        self.assertEqual(
            {p.side for p in reaction.participants},
            {ReactionSide.LEFT, ReactionSide.RIGHT},
        )

    def test_absent_or_unknown_compound_not_silently_created(self):
        invalid = [dict(SAMPLE[0])]
        invalid[0].pop("chebi")
        with self.assertRaisesRegex(DataSourceError, "chebi"):
            parse_rhea_participants(payload(invalid), ["10740"])
        bad = [dict(x) for x in SAMPLE]
        bad[0] = dict(bad[0], chebi=uri("https://invalid.org/chebi/15377"))
        with self.assertRaisesRegex(DataSourceError, "ChEBI"):
            parse_rhea_participants(payload(bad), ["10740"])

    def test_suspicious_side_and_coefficient_fail_closed(self):
        for value in ("-2", "NaN", "Infinity", "0"):
            bad = [dict(x) for x in SAMPLE]
            bad[0] = dict(bad[0], coefficient=literal(value))
            with self.subTest(value=value), self.assertRaisesRegex(DataSourceError, "coefficient"):
                parse_rhea_participants(payload(bad), ["10740"])
        bad = [dict(x) for x in SAMPLE]
        bad[0] = dict(bad[0], order=literal("2"))
        with self.assertRaisesRegex(DataSourceError, "curatedOrder"):
            parse_rhea_participants(payload(bad), ["10740"])

    def test_missing_side_missing_master_and_unexpected_master_fail(self):
        with self.assertRaisesRegex(DataSourceError, "incomplete side"):
            parse_rhea_participants(payload([SAMPLE[0], SAMPLE[1]]), ["10740"])
        with self.assertRaisesRegex(DataSourceError, "Missing Rhea reactions"):
            parse_rhea_participants(payload(SAMPLE), ["10740", "10744"])
        with self.assertRaisesRegex(DataSourceError, "Unexpected Rhea reaction"):
            parse_rhea_participants(payload(SAMPLE), ["10744"])

    def test_conflicting_duplicate_stoichiometry_fails(self):
        invalid = SAMPLE + [row("L", "9", 15377)]
        with self.assertRaisesRegex(DataSourceError, "Conflicting stoichiometry"):
            parse_rhea_participants(payload(invalid), ["10740"])

    def test_cache_fetch_retains_provenance_and_refuses_stale(self):
        cache = StubCache()
        snapshot = fetch_rhea_participants(["10740"], cache)
        self.assertEqual(len(cache.urls), 1)
        self.assertTrue(cache.urls[0].startswith(RHEA_SPARQL_URL + "?"))
        self.assertEqual(len(snapshot.reactions), 1)
        self.assertEqual(len(snapshot.source_release.sha256), 64)
        self.assertEqual(len(snapshot.query_snapshots), 1)
        cache.stale = True
        with self.assertRaisesRegex(DataSourceError, "stale"):
            fetch_rhea_participants(["10740"], cache)

    def test_max_25_master_ids_before_fetch(self):
        cache = StubCache()
        with self.assertRaises(ValueError):
            fetch_rhea_participants([str(i+10000) for i in range(MAX_MASTER_IDS+1)], cache)
        self.assertEqual(cache.urls, [])

    def test_incomplete_result_limit_rejected(self):
        expanded = SAMPLE * 134
        with self.assertRaisesRegex(DataSourceError, "limit"):
            parse_rhea_participants(payload(expanded), ["10740"])


if __name__ == "__main__":
    unittest.main()
