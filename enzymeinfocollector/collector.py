"""Orchestrate the authoritative source lookups and preserve their provenance."""

from __future__ import annotations

from typing import Any

from .sources import (
    ENZYME_URL,
    HttpCache,
    fetch_reaction_smiles,
    fetch_reaction_directions,
    fetch_rhea,
    fetch_uniprot,
    find_enzyme_entries,
    parse_enzyme_dat,
    validate_ec_number,
    utc_now,
)


class EnzymeCollector:
    def __init__(
        self,
        cache: HttpCache,
        *,
        refresh: bool = False,
        include_sequences: bool = True,
        reviewed_only: bool = False,
        taxon_ids: list[int] | None = None,
    ):
        self.cache = cache
        self.refresh = refresh
        self.include_sequences = include_sequences
        self.reviewed_only = reviewed_only
        self.taxon_ids = taxon_ids
        self._enzyme_entries: list[dict[str, Any]] | None = None
        self._enzyme_source: dict[str, Any] | None = None
        self._smiles: dict[str, str] | None = None
        self._smiles_source: dict[str, Any] | None = None
        self._directions: dict[str, dict[str, str]] | None = None
        self._directions_source: dict[str, Any] | None = None

    def _load_enzyme_entries(self) -> None:
        if self._enzyme_entries is not None:
            return
        response = self.cache.get(ENZYME_URL, ttl_seconds=7 * 24 * 60 * 60, refresh=self.refresh)
        self._enzyme_entries = parse_enzyme_dat(response.text)
        self._enzyme_source = response.metadata

    def _load_smiles(self) -> None:
        if self._smiles is None:
            self._smiles, self._smiles_source = fetch_reaction_smiles(self.cache, refresh=self.refresh)
            self._directions, self._directions_source = fetch_reaction_directions(self.cache, refresh=self.refresh)

    def collect(self, queries: list[tuple[str, str]]) -> dict[str, Any]:
        self._load_enzyme_entries()
        results = []
        for query_type, raw_value in queries:
            if query_type == "ec":
                value = validate_ec_number(raw_value)
                matches = [item for item in self._enzyme_entries or [] if item["ec_number"].casefold() == value.casefold()]
                if not matches:
                    matches = [{
                        "ec_number": value,
                        "enzyme_name": "",
                        "alternative_names": [],
                        "catalytic_activity": [],
                        "comments": [],
                        "uniprot_accessions": [],
                        "status": "not_found_in_expasy",
                    }]
            else:
                value = raw_value.strip()
                matches = find_enzyme_entries(self._enzyme_entries or [], value)

            if not matches:
                results.append({"input": {"type": query_type, "value": value}, "status": "not_found", "enzymes": []})
                continue

            enzymes = []
            for enzyme in matches:
                if enzyme["status"] != "active":
                    enzymes.append(self._enzyme_result(enzyme, [], [], None, None))
                    continue
                ec_number = enzyme["ec_number"]
                proteins, protein_source = fetch_uniprot(
                    ec_number,
                    self.cache,
                    refresh=self.refresh,
                    include_sequence=self.include_sequences,
                    reviewed_only=self.reviewed_only,
                    taxon_ids=self.taxon_ids,
                )
                reactions, reaction_source = fetch_rhea(ec_number, self.cache, refresh=self.refresh)
                smiles_by_id: dict[str, str] = {}
                if reactions:
                    self._load_smiles()
                    smiles_by_id = self._smiles or {}
                for reaction in reactions:
                    reaction_id = reaction["rhea_id"]
                    reaction["reaction_smiles"] = smiles_by_id.get(reaction_id)
                    directed_ids = (self._directions or {}).get(reaction_id, {})
                    reaction["reaction_smiles_directions"] = {
                        direction: smiles_by_id[identifier]
                        for direction, identifier in directed_ids.items()
                        if identifier in smiles_by_id
                    }
                enzymes.append(self._enzyme_result(enzyme, proteins, reactions, protein_source, reaction_source))

            results.append({
                "input": {"type": query_type, "value": value},
                "status": "found",
                "enzymes": enzymes,
            })

        return {
            "schema_version": "1.0",
            "collected_at": utc_now(),
            "sources": {
                "enzyme_nomenclature": self._source_summary(self._enzyme_source),
                "protein_annotations": "https://rest.uniprot.org/uniprotkb/stream",
                "reactions": "https://www.rhea-db.org/help/rest-api",
                "reaction_smiles": self._source_summary(self._smiles_source),
                "reaction_directions": self._source_summary(self._directions_source),
            },
            "results": results,
        }

    def _enzyme_result(
        self,
        enzyme: dict[str, Any],
        proteins: list[dict[str, Any]],
        reactions: list[dict[str, Any]],
        protein_source: dict[str, Any] | None,
        reaction_source: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            **enzyme,
            "proteins": proteins,
            "reactions": reactions,
            "provenance": {
                "enzyme_nomenclature": EnzymeCollector._source_summary(self._enzyme_source),
                "protein_annotations": EnzymeCollector._source_summary(protein_source),
                "reactions": EnzymeCollector._source_summary(reaction_source),
                "reaction_smiles": EnzymeCollector._source_summary(self._smiles_source),
                "reaction_directions": EnzymeCollector._source_summary(self._directions_source),
            },
        }

    @staticmethod
    def _source_summary(metadata: dict[str, Any] | None) -> dict[str, Any] | str | None:
        if metadata is None:
            return None
        return {key: value for key, value in metadata.items() if key in {"url", "fetched_at", "last_modified", "etag", "cached", "stale", "warning"}}
