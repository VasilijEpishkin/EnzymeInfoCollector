"""Offline direct UniProt→Rhea SPARQL adapter tests; synthetic data only."""

import json
import unittest

from enzymeinfocollector.direct_rhea import (
    assertions_from_direct_links,
    build_direct_rhea_query,
    fetch_direct_rhea_links,
)
from enzymeinfocollector.domain import ReactionDirection, SourceRelease
from enzymeinfocollector.sources import CachedResponse, DataSourceError


class StaticCache:
    def __init__(self, rows):
        self.rows = rows

    def get(self, url, **kwargs):
        self.url = url
        return CachedResponse(
            json.dumps({"head": {"vars": ["protein", "annotation", "rhea"]}, "results": {"bindings": self.rows}}),
            {"url": url, "fetched_at": "2026-10-29T00:00:00Z"},
        )


def uri(value):
    return {"type": "uri", "value": value}


class DirectRheaTests(unittest.TestCase):
    def test_bounded_query_and_injection_rejection(self):
        query = build_direct_rhea_query(["P00331", "P00330", "P00330"])
        self.assertIn("up:Catalytic_Activity_Annotation", query)
        self.assertEqual(query.count("<http://purl.uniprot.org/uniprot/P00330>"), 1)
        with self.assertRaises(ValueError):
            build_direct_rhea_query(["P00330> } UNION { ?s ?p ?o"])
        with self.assertRaises(ValueError):
            build_direct_rhea_query(["P00330"] * 11, limit=0)

    def test_direct_links_not_ec_cartesian_product(self):
        rows = [
            {"protein": uri("http://purl.uniprot.org/uniprot/P00330"),
             "annotation": uri("http://purl.uniprot.org/uniprot/P00330#SOME_ANNOTATION"),
             "rhea": uri("http://rdf.rhea-db.org/10740")},
            {"protein": uri("http://purl.uniprot.org/uniprot/P00331"),
             "annotation": uri("http://purl.uniprot.org/uniprot/P00331#OTHER_ANNOTATION"),
             "rhea": uri("http://rdf.rhea-db.org/33199")},
        ]
        links, _ = fetch_direct_rhea_links(["P00330", "P00331"], StaticCache(rows))
        self.assertEqual([(x.accession, x.rhea_id) for x in links],
                         [("P00330", "RHEA:10740"), ("P00331", "RHEA:33199")])
        release = SourceRelease("UniProtKB", "synthetic", "2026-10-29T00:00:00Z")
        lookup = {"RHEA:10740": ("RHEA:10740", ReactionDirection.UNDEFINED),
                  "RHEA:33199": ("RHEA:33198", ReactionDirection.LEFT_TO_RIGHT)}
        assertions = assertions_from_direct_links(links, source_release=release, direction_lookup=lookup)
        self.assertEqual(len(assertions), 2)
        self.assertEqual(assertions[0].evidence[0].eco_code, None)
        self.assertIsNone(assertions[0].evidence[0].source_statement_iri)
        self.assertEqual(assertions[0].annotation_id, "http://purl.uniprot.org/uniprot/P00330#SOME_ANNOTATION")
        self.assertEqual(assertions[0].evidence[0].citation_ids, ())
        self.assertEqual(assertions[1].reaction.master_id, "RHEA:33198")

    def test_limit_and_malformed_uri_are_rejected(self):
        row = {"protein": uri("http://purl.uniprot.org/uniprot/P00330"),
               "annotation": uri("http://purl.uniprot.org/uniprot/P00330#x"),
               "rhea": uri("http://rdf.rhea-db.org/10740")}
        with self.assertRaisesRegex(DataSourceError, "result limit"):
            fetch_direct_rhea_links(["P00330"], StaticCache([row]), limit=1)
        with self.assertRaisesRegex(DataSourceError, "Unexpected"):
            fetch_direct_rhea_links(["P00330"], StaticCache([{**row, "rhea": uri("https://evil.example/10740")}]))
        with self.assertRaisesRegex(DataSourceError, "Unexpected UniProt accession"):
            fetch_direct_rhea_links(["P00330"], StaticCache([{**row, "protein": uri("http://purl.uniprot.org/uniprot/P00331")}]))
        with self.assertRaisesRegex(DataSourceError, "Invalid UniProt catalytic annotation URI"):
            fetch_direct_rhea_links(["P00330"], StaticCache([{**row, "annotation": uri("http://evil.example/annotation")}]))
        with self.assertRaisesRegex(DataSourceError, "direction mapping"):
            assertions_from_direct_links(
                [fetch_direct_rhea_links(["P00330"], StaticCache([row]))[0][0]],
                source_release=SourceRelease("UniProtKB", "synthetic", "2026-10-29T00:00:00Z"),
                direction_lookup={},
            )


if __name__ == "__main__":
    unittest.main()
