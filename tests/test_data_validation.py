"""Регрессионные тесты целостности данных из ExPASy, UniProtKB и Rhea.

Все ответы синтетические: тесты не зависят от состояния внешних API.
"""
from __future__ import annotations

import unittest

from enzymeinfocollector.cli import main
from enzymeinfocollector.collector import EnzymeCollector
from enzymeinfocollector.sources import (
    CachedResponse,
    DataSourceError,
    fetch_reaction_smiles,
    fetch_rhea,
    fetch_uniprot,
    iter_uniprot,
)


class StaticCache:
    def __init__(self, body: str):
        self.body = body

    def get(self, url: str, **_options: object) -> CachedResponse:
        return CachedResponse(self.body, {"url": url, "fetched_at": "2026-10-01T00:00:00Z"})


UNIPROT_HEADER = (
    "Entry\tEntry Name\tProtein names\tGene Names\tOrganism\t"
    "Organism (ID)\tEC number\tLength\tReviewed\tSequence\n"
)


class DataValidationTests(unittest.TestCase):
    def test_missing_requested_uniprot_sequence_fails_in_eager_and_streaming_modes(self) -> None:
        body = UNIPROT_HEADER.replace("\tSequence\n", "\n") + (
            "P00330\tADH1_YEAST\tAlcohol dehydrogenase\tADH1\tS. cerevisiae\t"
            "4932\t1.1.1.1\t11\treviewed\n"
        )
        cache = StaticCache(body)
        with self.assertRaisesRegex(DataSourceError, "Sequence column"):
            fetch_uniprot("1.1.1.1", cache)
        with self.assertRaisesRegex(DataSourceError, "Sequence column"):
            with iter_uniprot("1.1.1.1", cache) as (proteins, _):
                list(proteins)

        # Annotations-only mode deliberately omits Sequence.
        proteins, _ = fetch_uniprot("1.1.1.1", cache, include_sequence=False)
        self.assertEqual(proteins[0]["sequence"], "")
        with iter_uniprot("1.1.1.1", cache, include_sequence=False) as (stream, _):
            self.assertEqual(next(stream)["accession"], "P00330")

    def test_blank_uniprot_accession_is_not_silently_ignored(self) -> None:
        body = UNIPROT_HEADER + (
            "\tADH1_YEAST\tAlcohol dehydrogenase\tADH1\tS. cerevisiae\t"
            "4932\t1.1.1.1\t11\treviewed\tMSTAGKVIKCK\n"
        )
        cache = StaticCache(body)
        with self.assertRaisesRegex(DataSourceError, "without an accession"):
            fetch_uniprot("1.1.1.1", cache)
        with self.assertRaisesRegex(DataSourceError, "without an accession"):
            with iter_uniprot("1.1.1.1", cache) as (stream, _):
                list(stream)

    def test_valid_empty_uniprot_result_remains_acceptable(self) -> None:
        cache = StaticCache(UNIPROT_HEADER)
        proteins, _ = fetch_uniprot("1.1.1.1", cache)
        self.assertEqual(proteins, [])
        with iter_uniprot("1.1.1.1", cache) as (stream, _):
            self.assertEqual(list(stream), [])

    def test_rhea_requires_equation_column_and_valid_reaction_id(self) -> None:
        missing = "Reaction identifier\tEC number\nRHEA:10001\t1.1.1.1\n"
        with self.assertRaisesRegex(DataSourceError, "Equation column"):
            fetch_rhea("1.1.1.1", StaticCache(missing))
        malformed = "Reaction identifier\tEquation\nnot-an-id\tA = B\n"
        with self.assertRaisesRegex(DataSourceError, "invalid reaction ID"):
            fetch_rhea("1.1.1.1", StaticCache(malformed))

    def test_rhea_smiles_rejects_empty_or_conflicting_entries(self) -> None:
        with self.assertRaisesRegex(DataSourceError, "empty SMILES"):
            fetch_reaction_smiles(StaticCache("RHEA:10001\t\n"))
        with self.assertRaisesRegex(DataSourceError, "conflicting SMILES"):
            fetch_reaction_smiles(StaticCache("RHEA:10001\tC>>O\nRHEA:10001\tN>>O\n"))
        data, _ = fetch_reaction_smiles(StaticCache("RHEA:10001\tC>>O\nRHEA:10001\tC>>O\n"))
        self.assertEqual(data, {"10001": "C>>O"})

    def test_empty_expasy_catalog_fails_instead_of_reporting_not_found(self) -> None:
        with self.assertRaisesRegex(DataSourceError, "ENZYME catalog is empty"):
            EnzymeCollector(StaticCache("<html>service unavailable</html>")).collect(
                [("enzyme", "Alcohol dehydrogenase")]
            )

    def test_nonfinite_timeout_is_rejected_without_using_network(self) -> None:
        from contextlib import redirect_stderr
        import io

        for value in ("nan", "inf", "-inf"):
            with self.subTest(timeout=value), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    main(["--ec", "1.1.1.1", "--timeout", value])
                self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
