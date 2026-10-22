"""Orchestrate the authoritative source lookups and preserve their provenance."""

from __future__ import annotations

from typing import Any, Callable

from .sources import (
    ENZYME_URL,
    HttpCache,
    fetch_reaction_smiles,
    fetch_reaction_directions,
    fetch_rhea,
    fetch_uniprot,
    iter_uniprot,
    find_enzyme_entries,
    parse_enzyme_dat,
    validate_ec_number,
    utc_now,
)

MAX_TRANSFER_PATHS = 10_000


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
        self._enzyme_index: dict[str, dict[str, Any]] | None = None
        self._active_results: dict[str, dict[str, Any]] = {}

    def _load_enzyme_entries(self) -> None:
        if self._enzyme_entries is not None:
            return
        response = self.cache.get(ENZYME_URL, ttl_seconds=7 * 24 * 60 * 60, refresh=self.refresh)
        self._enzyme_entries = parse_enzyme_dat(response.text)
        self._enzyme_index = {item["ec_number"].casefold(): item for item in self._enzyme_entries}
        self._enzyme_source = response.metadata

    def _load_smiles(self) -> None:
        if self._smiles is None:
            self._smiles, self._smiles_source = fetch_reaction_smiles(self.cache, refresh=self.refresh)
            self._directions, self._directions_source = fetch_reaction_directions(self.cache, refresh=self.refresh)

    def collect(
        self,
        queries: list[tuple[str, str]],
        *,
        on_record: Callable[[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None, str, str, dict[str, Any] | None], None] | None = None,
    ) -> dict[str, Any]:
        self._load_enzyme_entries()
        results = []
        for query_type, raw_value in queries:
            if query_type == "ec":
                value = validate_ec_number(raw_value)
                match = (self._enzyme_index or {}).get(value.casefold())
                matches = [match] if match else []
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
                result = {"input": {"type": query_type, "value": value}, "status": "not_found", "enzymes": []}
                results.append(result)
                if on_record:
                    on_record(result, None, None, "", "", None)
                continue

            result_info = {"input": {"type": query_type, "value": value}, "status": "found"}
            enzymes = []
            for enzyme in matches:
                if enzyme["status"] == "transferred":
                    source_result = self._enzyme_result(enzyme, [], [], None, None)
                    if on_record:
                        resolution, targets = self._transfer_plan(enzyme)
                        source_result["resolution"] = resolution
                        on_record(result_info, source_result, None, "enzyme", "", resolution)
                        for target, target_paths in targets:
                            target_result = self._stream_active_enzyme(
                                target,
                                lambda record, protein: on_record(
                                    result_info, record, protein, "resolved_target", enzyme["ec_number"], resolution
                                ),
                                record_extra={"requested_ec": enzyme["ec_number"], "resolution_paths": target_paths},
                            )
                            resolution["resolved_enzymes"].append(target_result)
                    else:
                        source_result["resolution"] = self._resolve_transfers(enzyme)
                    enzymes.append(source_result)
                    continue
                if enzyme["status"] != "active":
                    record = self._enzyme_result(enzyme, [], [], None, None)
                    enzymes.append(record)
                    if on_record:
                        on_record(result_info, record, None, "enzyme", "", None)
                    continue
                if on_record:
                    record = self._stream_active_enzyme(
                        enzyme,
                        lambda active, protein: on_record(result_info, active, protein, "enzyme", "", None),
                    )
                    enzymes.append(record)
                else:
                    enzymes.append(self._collect_active_enzyme(enzyme))

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

    def _collect_active_enzyme(self, enzyme: dict[str, Any]) -> dict[str, Any]:
        ec_number = enzyme["ec_number"]
        if ec_number.casefold() not in self._active_results:
            proteins, protein_source = fetch_uniprot(
                ec_number,
                self.cache,
                refresh=self.refresh,
                include_sequence=self.include_sequences,
                reviewed_only=self.reviewed_only,
                taxon_ids=self.taxon_ids,
            )
            reactions, reaction_source = fetch_rhea(ec_number, self.cache, refresh=self.refresh)
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
            self._active_results[ec_number.casefold()] = self._enzyme_result(
                enzyme, proteins, reactions, protein_source, reaction_source
            )
        return self._active_results[ec_number.casefold()]

    def _stream_active_enzyme(
        self,
        enzyme: dict[str, Any],
        on_protein: Callable[[dict[str, Any], dict[str, Any] | None], None],
        *,
        record_extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        reactions, reaction_source = fetch_rhea(enzyme["ec_number"], self.cache, refresh=self.refresh)
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
        with iter_uniprot(
            enzyme["ec_number"],
            self.cache,
            refresh=self.refresh,
            include_sequence=self.include_sequences,
            reviewed_only=self.reviewed_only,
            taxon_ids=self.taxon_ids,
        ) as (proteins, protein_source):
            record = self._enzyme_result(enzyme, [], reactions, protein_source, reaction_source)
            if record_extra:
                record.update(record_extra)
            found = False
            for protein in proteins:
                found = True
                on_protein(record, protein)
            if not found:
                on_protein(record, None)
            return record

    def _resolve_transfers(self, source: dict[str, Any]) -> dict[str, Any]:
        resolution, targets = self._transfer_plan(source)
        resolution["resolved_enzymes"] = [
            {**self._collect_active_enzyme(entry), "requested_ec": source["ec_number"], "resolution_paths": paths}
            for entry, paths in targets
        ]
        return resolution

    def _transfer_plan(
        self, source: dict[str, Any]
    ) -> tuple[dict[str, Any], list[tuple[dict[str, Any], list[list[str]]]]]:
        paths: list[dict[str, Any]] = []
        redirect_entries: dict[str, dict[str, Any]] = {}
        terminal_paths: dict[str, list[list[str]]] = {}
        path_limit_reached = False

        pending = [
            (target, [source["ec_number"], target])
            for target in reversed(source["transferred_to"])
        ]
        while pending:
            ec_number, path = pending.pop()
            if len(paths) >= MAX_TRANSFER_PATHS:
                paths.append({"ec_numbers": path, "status": "truncated"})
                path_limit_reached = True
                break
            key = ec_number.casefold()
            if key in {item.casefold() for item in path[:-1]}:
                paths.append({"ec_numbers": path, "status": "cycle"})
                continue
            entry = (self._enzyme_index or {}).get(key)
            if entry is None:
                paths.append({"ec_numbers": path, "status": "missing"})
            elif entry["status"] == "active":
                terminal_paths.setdefault(entry["ec_number"], []).append(path)
                paths.append({"ec_numbers": path, "status": "active"})
            elif entry["status"] == "deleted":
                paths.append({"ec_numbers": path, "status": "deleted"})
            else:
                redirect_entries.setdefault(key, entry)
                if entry["transferred_to"]:
                    pending.extend(
                        (target, [*path, target])
                        for target in reversed(entry["transferred_to"])
                    )
                else:
                    paths.append({"ec_numbers": path, "status": "unresolved_transfer"})

        targets = [
            ((self._enzyme_index or {})[ec.casefold()], target_paths)
            for ec, target_paths in sorted(terminal_paths.items())
        ]
        unresolved = [path for path in paths if path["status"] in {"missing", "deleted", "cycle", "unresolved_transfer"}]
        if path_limit_reached or (targets and unresolved):
            status = "partial"
        else:
            status = "resolved" if targets else "unresolved"
        resolution = {
            "requested_ec": source["ec_number"],
            "status": status,
            "paths": paths,
            "path_limit_reached": path_limit_reached,
            "terminal_ec_numbers": sorted(terminal_paths),
            "redirect_entries": list(redirect_entries.values()),
            "unresolved_targets": unresolved,
            "resolved_enzymes": [],
        }
        return resolution, targets

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
