"""Orchestrate the authoritative source lookups and preserve their provenance."""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable

from .sources import (
    ENZYME_URL,
    DataSourceError,
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
    """Collect enzyme records; use one instance per concurrent collection."""

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
        if not self._enzyme_entries:
            raise DataSourceError("ExPASy ENZYME catalog is empty or has an unexpected format")
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
        """Collect results; callback mode streams proteins and returns metadata-only enzyme records.

        Treat callback arguments as borrowed, read-only views valid only during the callback.
        Query results and enzyme records may be reused, and transfer resolution is updated
        as target records arrive. Copy values that must be retained or modified.
        """
        queries = list(queries)
        self._enzyme_entries = None
        self._enzyme_index = None
        self._enzyme_source = None
        self._smiles = None
        self._smiles_source = None
        self._directions = None
        self._directions_source = None
        self._active_results.clear()
        try:
            if on_record is None:
                return self._collect(queries)
            with TemporaryDirectory(prefix="enzymeinfocollector-collect-") as directory:
                return self._collect(queries, on_record, Path(directory), {}, {})
        finally:
            self._active_results.clear()

    def _collect(
        self,
        queries: list[tuple[str, str]],
        on_record: Callable[[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None, str, str, dict[str, Any] | None], None] | None = None,
        sort_directory: Path | None = None,
        sort_cache: dict[str, tuple[list[Path], dict[str, Any]]] | None = None,
        reaction_cache: dict[str, tuple[list[dict[str, Any]], dict[str, Any]]] | None = None,
    ) -> dict[str, Any]:
        self._load_enzyme_entries()
        prepared_queries, occurrences, transfer_plans = (
            self._prepare_stream_queries(queries) if on_record else (None, Counter(), {})
        )
        reused_ecs = {ec_number for ec_number, count in occurrences.items() if count > 1}
        results = []
        for query_index, (query_type, raw_value) in enumerate(queries):
            if prepared_queries is not None:
                _, value, matches = prepared_queries[query_index]
            elif query_type == "ec":
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
                        resolution_template, targets = transfer_plans[id(enzyme)]
                        resolution = deepcopy(resolution_template)
                        resolution["resolved_enzymes"] = []
                        source_result["resolution"] = resolution
                        on_record(result_info, source_result, None, "enzyme", "", resolution)
                        for target, target_paths in targets:
                            target_result = self._stream_active_enzyme(
                                target,
                                lambda record, protein: on_record(
                                    result_info, record, protein, "resolved_target", enzyme["ec_number"], resolution
                                ),
                                record_extra={
                                    "requested_ec": enzyme["ec_number"],
                                    "resolution_paths": deepcopy(target_paths),
                                },
                                sort_directory=sort_directory,
                                sort_cache=sort_cache,
                                reaction_cache=reaction_cache,
                                reused_ecs=reused_ecs,
                                occurrences=occurrences,
                            )
                            resolution["resolved_enzymes"].append(target_result)
                    else:
                        source_result["resolution"] = self._resolve_transfers(enzyme)
                    enzymes.append(source_result)
                    continue
                if enzyme["status"] not in {"active", "not_found_in_expasy"}:
                    record = self._enzyme_result(enzyme, [], [], None, None)
                    enzymes.append(record)
                    if on_record:
                        on_record(result_info, record, None, "enzyme", "", None)
                    continue
                if on_record:
                    record = self._stream_active_enzyme(
                        enzyme,
                        lambda active, protein: on_record(result_info, active, protein, "enzyme", "", None),
                        sort_directory=sort_directory,
                        sort_cache=sort_cache,
                        reaction_cache=reaction_cache,
                        reused_ecs=reused_ecs,
                        occurrences=occurrences,
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

    def _prepare_stream_queries(
        self, queries: list[tuple[str, str]]
    ) -> tuple[
        list[tuple[str, str, list[dict[str, Any]]]],
        Counter[str],
        dict[int, tuple[dict[str, Any], list[tuple[dict[str, Any], list[list[str]]]]]],
    ]:
        occurrences: Counter[str] = Counter()
        prepared = []
        transfer_plans = {}
        for query_type, raw_value in queries:
            if query_type == "ec":
                match = (self._enzyme_index or {}).get(validate_ec_number(raw_value).casefold())
                matches = [match] if match else []
                value = validate_ec_number(raw_value)
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
            prepared.append((query_type, value, matches))
            for enzyme in matches:
                if enzyme["status"] in {"active", "not_found_in_expasy"}:
                    occurrences[enzyme["ec_number"].casefold()] += 1
                elif enzyme["status"] == "transferred":
                    plan = transfer_plans.get(id(enzyme))
                    if plan is None:
                        plan = self._transfer_plan(enzyme)
                        transfer_plans[id(enzyme)] = plan
                    _, targets = plan
                    occurrences.update(target["ec_number"].casefold() for target, _ in targets)
        return prepared, occurrences, transfer_plans

    def _collect_active_enzyme(self, enzyme: dict[str, Any]) -> dict[str, Any]:
        ec_number = enzyme["ec_number"]
        cache_key = ec_number.casefold()
        if cache_key not in self._active_results:
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
            self._active_results[cache_key] = self._enzyme_result(
                enzyme, proteins, reactions, protein_source, reaction_source
            )
            return self._active_results[cache_key]
        return deepcopy(self._active_results[cache_key])

    def _stream_active_enzyme(
        self,
        enzyme: dict[str, Any],
        on_protein: Callable[[dict[str, Any], dict[str, Any] | None], None],
        *,
        record_extra: dict[str, Any] | None = None,
        sort_directory: Path | None = None,
        sort_cache: dict[str, tuple[list[Path], dict[str, Any]]] | None = None,
        reaction_cache: dict[str, tuple[list[dict[str, Any]], dict[str, Any]]] | None = None,
        reused_ecs: set[str] | None = None,
        occurrences: Counter[str] | None = None,
    ) -> dict[str, Any]:
        ec_key = enzyme["ec_number"].casefold()
        reuse = reused_ecs is not None and ec_key in reused_ecs
        release_runs = reuse and occurrences is not None and occurrences[ec_key] == 1
        reaction_cache = reaction_cache if reuse else None
        cached_reactions = reaction_cache.get(ec_key) if reaction_cache is not None else None
        if cached_reactions is None:
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
            if reaction_cache is not None:
                reaction_cache[ec_key] = (reactions, reaction_source)
        else:
            reactions, reaction_source = cached_reactions
        with iter_uniprot(
            enzyme["ec_number"],
            self.cache,
            refresh=self.refresh,
            include_sequence=self.include_sequences,
            reviewed_only=self.reviewed_only,
            taxon_ids=self.taxon_ids,
            sort_cache=sort_cache if reuse else None,
            sort_directory=sort_directory if reuse else None,
            release_cached_runs=release_runs,
        ) as (proteins, protein_source):
            record = self._enzyme_result(
                enzyme,
                [],
                deepcopy(reactions) if reuse else reactions,
                protein_source,
                reaction_source,
            )
            if record_extra:
                record.update(record_extra)
            found = False
            for protein in proteins:
                found = True
                on_protein(record, protein)
            if not found:
                on_protein(record, None)
        if reuse and occurrences is not None:
            occurrences[ec_key] -= 1
            if release_runs and reaction_cache is not None:
                reaction_cache.pop(ec_key, None)
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

        path = [source["ec_number"]]
        active = {source["ec_number"].casefold()}
        pending = [iter(source["transferred_to"])]
        while pending:
            try:
                ec_number = next(pending[-1])
            except StopIteration:
                pending.pop()
                if len(path) > 1:
                    active.remove(path.pop().casefold())
                continue
            if len(paths) >= MAX_TRANSFER_PATHS:
                paths.append({"ec_numbers": [*path, ec_number], "status": "truncated"})
                path_limit_reached = True
                break
            key = ec_number.casefold()
            if key in active:
                paths.append({"ec_numbers": [*path, ec_number], "status": "cycle"})
                continue
            entry = (self._enzyme_index or {}).get(key)
            if entry is None:
                paths.append({"ec_numbers": [*path, ec_number], "status": "missing"})
            elif entry["status"] == "active":
                terminal_paths.setdefault(entry["ec_number"], []).append([*path, ec_number])
                paths.append({"ec_numbers": [*path, ec_number], "status": "active"})
            elif entry["status"] == "deleted":
                paths.append({"ec_numbers": [*path, ec_number], "status": "deleted"})
            else:
                redirect_entries.setdefault(key, entry)
                if entry["transferred_to"]:
                    path.append(ec_number)
                    active.add(key)
                    pending.append(iter(entry["transferred_to"]))
                else:
                    paths.append({"ec_numbers": [*path, ec_number], "status": "unresolved_transfer"})

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
            **deepcopy(enzyme),
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
