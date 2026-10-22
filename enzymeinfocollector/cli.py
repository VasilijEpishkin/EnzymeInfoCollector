"""User-facing CLI for collecting and exporting enzyme information."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from .collector import EnzymeCollector
from .sources import DataSourceError, HttpCache, default_cache_dir, validate_ec_number

LOGGER = logging.getLogger("enzymeinfocollector")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="enzymeinfocollector",
        description="Collect curated enzyme nomenclature, UniProt proteins, and Rhea reactions.",
    )
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument("--enzyme", action="append", help="Enzyme name; can be repeated")
    inputs.add_argument("--ec", action="append", help="Full EC number; can be repeated")
    inputs.add_argument("--file", type=Path, help="CSV, TSV, TXT, or XLSX input file")
    inputs.add_argument("--replay-manifest", type=Path, help="Repeat queries and filters from a saved request manifest")
    parser.add_argument("--output", "-o", type=Path, default=Path("enzyme_results.json"), help="Output file (default: enzyme_results.json)")
    parser.add_argument("--save-manifest", type=Path, help="Atomically save the normalized queries and collection filters as JSON")
    parser.add_argument("--format", choices=("auto", "json", "csv", "fasta"), default="auto", help="Output format; auto uses the output extension")
    parser.add_argument("--stream-json", action="store_true", help="Reduce memory use for JSON output at the cost of extra disk I/O")
    parser.add_argument("--cache-dir", type=Path, default=default_cache_dir(), help="HTTP cache directory")
    parser.add_argument("--contact-email", default=os.environ.get("ENZYMEINFOCOLLECTOR_EMAIL"), help="Contact email added to upstream User-Agent headers")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds (default: 30)")
    parser.add_argument("--retries", type=int, default=3, help="Retries for transient upstream errors (default: 3)")
    parser.add_argument("--refresh", action="store_true", help="Bypass cached responses")
    parser.add_argument("--no-sequences", action="store_true", help="Fetch protein annotations without amino acid sequences")
    parser.add_argument("--reviewed-only", action="store_true", help="Restrict UniProt proteins to reviewed entries")
    parser.add_argument("--taxon-id", type=_positive_int, action="append", help="Restrict UniProt proteins to this NCBI taxon and descendants; may be repeated")
    parser.add_argument("--verbose", action="store_true", help="Log progress and cache use to stderr")
    commands = parser.add_subparsers(dest="command")
    cache = commands.add_parser("cache", help="Inspect or prune the local HTTP cache")
    cache.add_argument("--cache-dir", type=Path, default=argparse.SUPPRESS, help="HTTP cache directory")
    cache_actions = cache.add_subparsers(dest="cache_action", required=True)
    info = cache_actions.add_parser("info", help="Summarize cached responses")
    info.add_argument("--cache-dir", type=Path, default=argparse.SUPPRESS, help="HTTP cache directory")
    prune = cache_actions.add_parser("prune", help="Preview removal of responses older than 30 days")
    prune.add_argument("--cache-dir", type=Path, default=argparse.SUPPRESS, help="HTTP cache directory")
    prune.add_argument("--older-than-days", type=_cache_retention_days, default=30, help="Minimum cache age to remove (at least 30 days)")
    prune.add_argument("--apply", action="store_true", help="Delete eligible response bodies and metadata")
    return parser


def read_queries(args: argparse.Namespace) -> list[tuple[str, str]]:
    if args.enzyme:
        queries = [("enzyme", value.strip()) for value in args.enzyme]
    elif args.ec:
        queries = [("ec", validate_ec_number(value)) for value in args.ec]
    else:
        queries = _read_query_file(args.file)
    if not queries or any(not value for _, value in queries):
        raise ValueError("Input must contain at least one non-empty enzyme name or EC number")
    return _unique_queries(queries)


def _unique_queries(queries: list[tuple[str, str]]) -> list[tuple[str, str]]:
    unique: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for query in queries:
        key = (query[0], query[1].casefold())
        if key not in seen:
            unique.append(query)
            seen.add(key)
    return unique


def _read_request_manifest(path: Path) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(f"Could not read request manifest {path}: {error}") from error
    if not isinstance(manifest, dict) or set(manifest) != {"manifest_version", "queries", "options"}:
        raise ValueError("Request manifest must contain manifest_version, queries, and options")
    if type(manifest["manifest_version"]) is not int or manifest["manifest_version"] != 1:
        raise ValueError(f"Unsupported request manifest version: {manifest['manifest_version']!r}")
    if not isinstance(manifest["queries"], list):
        raise ValueError("Request manifest queries must be a list")
    queries = []
    for item in manifest["queries"]:
        if not isinstance(item, dict) or set(item) != {"type", "value"}:
            raise ValueError("Each manifest query must contain only type and value")
        query_type, value = item["type"], item["value"]
        if not isinstance(query_type, str) or query_type not in {"enzyme", "ec"} or not isinstance(value, str) or not value.strip():
            raise ValueError("Manifest queries need type 'enzyme' or 'ec' and a non-empty string value")
        queries.append((query_type, validate_ec_number(value) if query_type == "ec" else value.strip()))
    queries = _unique_queries(queries)
    if not queries:
        raise ValueError("Request manifest must contain at least one query")
    options = manifest["options"]
    if not isinstance(options, dict) or set(options) != {"include_sequences", "reviewed_only", "taxon_ids"}:
        raise ValueError("Request manifest options must contain include_sequences, reviewed_only, and taxon_ids")
    if type(options["include_sequences"]) is not bool or type(options["reviewed_only"]) is not bool:
        raise ValueError("Manifest include_sequences and reviewed_only options must be booleans")
    taxon_ids = options["taxon_ids"]
    if taxon_ids is not None and (
        not isinstance(taxon_ids, list) or any(type(taxon_id) is not int or taxon_id <= 0 for taxon_id in taxon_ids)
    ):
        raise ValueError("Manifest taxon_ids must be null or a list of positive integers")
    return queries, options


def _write_request_manifest(path: Path, queries: list[tuple[str, str]], options: dict[str, Any]) -> None:
    manifest = {
        "manifest_version": 1,
        "queries": [{"type": query_type, "value": value} for query_type, value in queries],
        "options": options,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temp_path = Path(temp_name)
    try:
        with temp_path.open("w", encoding="utf-8", newline="\n") as stream:
            json.dump(manifest, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def _read_query_file(path: Path | None) -> list[tuple[str, str]]:
    if path is None or not path.is_file():
        raise ValueError(f"Input file does not exist: {path}")
    suffix = path.suffix.casefold()
    if suffix == ".xlsx":
        try:
            from openpyxl import load_workbook
        except ImportError as error:
            raise ValueError("XLSX input requires openpyxl; install it with `pip install enzymeinfocollector[xlsx]`") from error
        try:
            workbook = load_workbook(path, read_only=True, data_only=True)
        except (OSError, ValueError, zipfile.BadZipFile) as error:
            raise ValueError(f"Could not read Excel workbook {path}: {error}") from error
        try:
            rows = workbook.active.iter_rows(values_only=True)
            return _queries_from_rows(list(rows))
        finally:
            workbook.close()
    if suffix in {".csv", ".tsv"}:
        delimiter = "\t" if suffix == ".tsv" else ","
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            return _queries_from_rows(list(csv.reader(stream, delimiter=delimiter)))
    if suffix == ".txt":
        with path.open("r", encoding="utf-8-sig") as stream:
            values = [line.strip() for line in stream if line.strip() and not line.lstrip().startswith("#")]
            return [("ec", validate_ec_number(value)) if value.casefold().startswith("ec ") or (value[:1].isdigit() and value.count(".") >= 2) else ("enzyme", value) for value in values]
    raise ValueError("Input file must have .csv, .tsv, .txt, or .xlsx extension")


def _queries_from_rows(rows: list[tuple[Any, ...]] | list[list[Any]]) -> list[tuple[str, str]]:
    if not rows:
        return []
    header = [str(value or "").strip().casefold() for value in rows[0]]
    enzyme_columns = [index for index, name in enumerate(header) if name in {"protein", "enzyme", "enzyme name", "protein name"}]
    ec_columns = [index for index, name in enumerate(header) if name in {"ec", "ec number", "ec_number"}]
    if not enzyme_columns and not ec_columns:
        raise ValueError("Input table needs a Protein/Enzyme or EC/EC number column")
    queries = []
    for row in rows[1:]:
        for index in enzyme_columns:
            value = _cell(row, index)
            if value:
                queries.append(("enzyme", value))
        for index in ec_columns:
            value = _cell(row, index)
            if value:
                queries.append(("ec", validate_ec_number(value)))
    return queries


def _cell(row: tuple[Any, ...] | list[Any], index: int) -> str:
    return str(row[index]).strip() if index < len(row) and row[index] is not None else ""


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _cache_retention_days(value: str) -> int:
    parsed = _positive_int(value)
    if parsed < 30:
        raise argparse.ArgumentTypeError("must be at least 30 to preserve offline cache fallback")
    return parsed


def write_output(path: Path, output_format: str, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    selected_format = _select_format(path, output_format)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temp_path = Path(temp_name)
    try:
        if selected_format == "json":
            with temp_path.open("w", encoding="utf-8", newline="\n") as stream:
                json.dump(payload, stream, ensure_ascii=False, indent=2)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
        elif selected_format == "csv":
            _write_csv(temp_path, payload)
        else:
            _write_fasta(temp_path, payload)
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def write_stream_output(path: Path, output_format: str, collector: EnzymeCollector, queries: list[tuple[str, str]]) -> dict[str, Any]:
    """Collect CSV/FASTA records directly into an atomic temporary output."""
    selected_format = _select_format(path, output_format)
    if selected_format not in {"csv", "fasta"}:
        raise ValueError("Streaming output supports CSV and FASTA")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temp_path = Path(temp_name)
    try:
        written: set[str] = set()
        with temp_path.open("w", encoding="utf-8", newline="" if selected_format == "csv" else "\n") as stream:
            writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS, extrasaction="ignore") if selected_format == "csv" else None
            if writer:
                writer.writeheader()

            def write_record(
                result: dict[str, Any],
                record: dict[str, Any] | None,
                protein: dict[str, Any] | None,
                record_kind: str,
                requested_ec: str,
                resolution: dict[str, Any] | None,
            ) -> None:
                if writer:
                    writer.writerow(_csv_export_row(result, record, protein, record_kind, requested_ec, resolution))
                elif record and protein:
                    _write_fasta_record(stream, record, protein, written)

            payload = collector.collect(queries, on_record=write_record)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
        return payload
    finally:
        temp_path.unlink(missing_ok=True)


def write_stream_json_output(path: Path, collector: EnzymeCollector, queries: list[tuple[str, str]]) -> dict[str, Any]:
    """Write JSON without retaining streamed protein records in memory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f".{path.name}.spool.", dir=path.parent) as spool_directory:
        spool_path = Path(spool_directory) / "proteins.jsonl"
        protein_ranges: dict[int, list[tuple[int, int]]] = {}
        with spool_path.open("wb") as spool:
            current_record_id: int | None = None
            current_start = 0
            current_count = 0

            def finish_record() -> None:
                nonlocal current_record_id, current_count
                if current_record_id is not None:
                    protein_ranges.setdefault(current_record_id, []).append((current_start, current_count))
                    current_record_id = None
                    current_count = 0

            def spool_record(
                _result: dict[str, Any],
                record: dict[str, Any] | None,
                protein: dict[str, Any] | None,
                _record_kind: str,
                _requested_ec: str,
                _resolution: dict[str, Any] | None,
            ) -> None:
                nonlocal current_record_id, current_start, current_count
                if record is None:
                    finish_record()
                    return
                record_id = id(record)
                if current_record_id != record_id:
                    finish_record()
                    current_record_id = record_id
                    current_start = spool.tell()
                    protein_ranges.setdefault(record_id, [])
                if protein is not None:
                    spool.write(json.dumps(protein, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                    spool.write(b"\n")
                    current_count += 1

            payload = collector.collect(queries, on_record=spool_record)
            finish_record()

        descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        os.close(descriptor)
        temp_path = Path(temp_name)
        try:
            with temp_path.open("w", encoding="utf-8", newline="\n") as stream, spool_path.open("rb") as spool:
                _dump_json_with_protein_spool(payload, stream, spool, protein_ranges)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_path, path)
        finally:
            temp_path.unlink(missing_ok=True)
    return payload


def _dump_json_with_protein_spool(
    value: Any,
    stream: Any,
    spool: Any,
    protein_ranges: dict[int, list[tuple[int, int]]],
    level: int = 0,
) -> None:
    if isinstance(value, dict):
        if not value:
            stream.write("{}")
            return
        stream.write("{\n")
        for index, (key, item) in enumerate(value.items()):
            stream.write("  " * (level + 1))
            json.dump(key, stream, ensure_ascii=False)
            stream.write(": ")
            if key == "proteins" and id(value) in protein_ranges:
                _write_spooled_proteins(stream, spool, protein_ranges[id(value)], protein_ranges, level + 1)
            else:
                _dump_json_with_protein_spool(item, stream, spool, protein_ranges, level + 1)
            stream.write(",\n" if index + 1 < len(value) else "\n")
        stream.write("  " * level + "}")
    elif isinstance(value, (list, tuple)):
        if not value:
            stream.write("[]")
            return
        stream.write("[\n")
        for index, item in enumerate(value):
            stream.write("  " * (level + 1))
            _dump_json_with_protein_spool(item, stream, spool, protein_ranges, level + 1)
            stream.write(",\n" if index + 1 < len(value) else "\n")
        stream.write("  " * level + "]")
    else:
        json.dump(value, stream, ensure_ascii=False)


def _write_spooled_proteins(
    stream: Any,
    spool: Any,
    ranges: list[tuple[int, int]],
    protein_ranges: dict[int, list[tuple[int, int]]],
    level: int,
) -> None:
    protein_count = sum(count for _, count in ranges)
    if not protein_count:
        stream.write("[]")
        return
    stream.write("[\n")
    written = 0
    for start, count in ranges:
        spool.seek(start)
        for _ in range(count):
            protein = json.loads(spool.readline())
            stream.write("  " * (level + 1))
            _dump_json_with_protein_spool(protein, stream, spool, protein_ranges, level + 1)
            written += 1
            stream.write(",\n" if written < protein_count else "\n")
    stream.write("  " * level + "]")


def _select_format(path: Path, output_format: str) -> str:
    if output_format != "auto":
        return output_format
    suffix = path.suffix.casefold()
    return "csv" if suffix == ".csv" else "fasta" if suffix in {".fa", ".faa", ".fasta"} else "json"


CSV_FIELDS = (
    "input_type", "input_value", "status", "ec_number", "enzyme_name", "alternative_names",
    "catalytic_activity", "uniprot_accession", "entry_name", "protein_name", "gene_names",
    "organism", "organism_id", "reviewed", "length", "sequence", "ec_reactions", "association_basis",
    "enzyme_source", "uniprot_source", "rhea_source", "rhea_smiles_source", "rhea_directions_source",
    "record_kind", "requested_ec", "enzyme_status", "transferred_to", "resolution_status", "resolution_paths", "unresolved_targets", "path_limit_reached",
)


def _write_csv(path: Path, payload: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for result in payload["results"]:
            if not result["enzymes"]:
                writer.writerow(_csv_export_row(result, None, None, "", "", None))
                continue
            for enzyme in result["enzymes"]:
                records = [(enzyme, "enzyme", "", enzyme.get("resolution"))]
                resolution = enzyme.get("resolution")
                if resolution:
                    records.extend((target, "resolved_target", resolution["requested_ec"], resolution) for target in resolution["resolved_enzymes"])
                for record, record_kind, requested_ec, resolution in records:
                    proteins = record["proteins"] or [None]
                    for protein in proteins:
                        writer.writerow(_csv_export_row(result, record, protein, record_kind, requested_ec, resolution))
        stream.flush()
        os.fsync(stream.fileno())


def _csv_export_row(
    result: dict[str, Any],
    record: dict[str, Any] | None,
    protein: dict[str, Any] | None,
    record_kind: str,
    requested_ec: str,
    resolution: dict[str, Any] | None,
) -> dict[str, Any]:
    row = {"input_type": result["input"]["type"], "input_value": result["input"]["value"], "status": result["status"]}
    if not record:
        return row
    return {
        **row,
        "record_kind": record_kind,
        "requested_ec": requested_ec,
        "ec_number": record["ec_number"],
        "enzyme_status": record["status"],
        "transferred_to": _json_cell(record.get("transferred_to", [])),
        "resolution_status": resolution["status"] if resolution else "",
        "resolution_paths": _json_cell(record.get("resolution_paths", resolution["paths"] if resolution else [])),
        "unresolved_targets": _json_cell(resolution["unresolved_targets"]) if resolution and record_kind == "enzyme" else "",
        "path_limit_reached": resolution["path_limit_reached"] if resolution else "",
        "enzyme_name": record["enzyme_name"],
        "alternative_names": _json_cell(record["alternative_names"]),
        "catalytic_activity": _json_cell(record["catalytic_activity"]),
        "uniprot_accession": protein["accession"] if protein else "",
        "entry_name": protein["entry_name"] if protein else "",
        "protein_name": protein["protein_name"] if protein else "",
        "gene_names": _json_cell(protein["gene_names"]) if protein else "",
        "organism": protein["organism"] if protein else "",
        "organism_id": protein["organism_id"] if protein else "",
        "reviewed": protein["reviewed"] if protein else "",
        "length": protein["length"] if protein else "",
        "sequence": protein["sequence"] if protein else "",
        "ec_reactions": _json_cell(record["reactions"]),
        "association_basis": "EC number" if protein else "",
        "enzyme_source": _json_cell(record["provenance"]["enzyme_nomenclature"]),
        "uniprot_source": _json_cell(record["provenance"]["protein_annotations"]),
        "rhea_source": _json_cell(record["provenance"]["reactions"]),
        "rhea_smiles_source": _json_cell(record["provenance"]["reaction_smiles"]),
        "rhea_directions_source": _json_cell(record["provenance"]["reaction_directions"]),
    }


def _json_cell(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _write_fasta(path: Path, payload: dict[str, Any]) -> None:
    written: set[str] = set()
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for result in payload["results"]:
            for enzyme in result["enzymes"]:
                for record in [enzyme, *(enzyme.get("resolution", {}).get("resolved_enzymes", []))]:
                    for protein in record["proteins"]:
                        _write_fasta_record(stream, record, protein, written)
        stream.flush()
        os.fsync(stream.fileno())


def _write_fasta_record(stream: Any, record: dict[str, Any], protein: dict[str, Any], written: set[str]) -> None:
    accession = protein["accession"]
    sequence = protein["sequence"]
    if not sequence or accession in written:
        return
    written.add(accession)
    ec_numbers = ",".join(protein["ec_numbers"] or [record["ec_number"]])
    organism = "_".join(protein["organism"].split()) or "unknown"
    stream.write(f">{accession} EC={ec_numbers} OS={organism} source=UniProtKB\n")
    for offset in range(0, len(sequence), 80):
        stream.write(sequence[offset:offset + 80] + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(levelname)s: %(message)s")
    if args.command == "cache":
        if args.enzyme or args.ec or args.file or args.replay_manifest or args.save_manifest or args.stream_json:
            parser.error("cache maintenance cannot be combined with collection inputs")
        try:
            cache = HttpCache(args.cache_dir)
            if args.cache_action == "info":
                info = cache.inspect()
                print(f"Cache directory: {args.cache_dir}")
                print(f"Cache keys: {info['entry_count']}; cached bodies: {_format_bytes(info['body_bytes'])}")
                print(f"Lock sidecars: {info['lock_count']}; file bytes: {_format_bytes(info['lock_bytes'])}")
                print(f"Incomplete: {info['orphan_count']}; invalid metadata: {info['invalid_count']}")
                older = sum(entry["state"] == "ok" and entry["age_seconds"] > 30 * 86400 for entry in info["entries"])
                print(f"Older than 30 days: {older}")
                hosts: dict[str, list[int]] = {}
                for entry in info["entries"]:
                    host = entry["host"] or "unknown host"
                    totals = hosts.setdefault(host, [0, 0])
                    totals[0] += 1
                    totals[1] += entry["body_bytes"]
                for host, (count, size) in sorted(hosts.items()):
                    print(f"  {host}: {count} entr{'y' if count == 1 else 'ies'}, {_format_bytes(size)}")
                return 0
            outcome = cache.prune(older_than_days=args.older_than_days, apply=args.apply)
            action = "Removed" if args.apply else "Would remove"
            count = outcome["removed_count"] if args.apply else outcome["eligible_count"]
            size = outcome["removed_bytes"] if args.apply else outcome["eligible_bytes"]
            print(f"{action} {count} cache entr{'y' if count == 1 else 'ies'} ({_format_bytes(size)}).")
            if not args.apply and count:
                print("Run the same command with --apply to delete them.")
            return 0
        except (ValueError, OSError) as error:
            print(f"enzymeinfocollector: {error}", file=sys.stderr)
            return 1
    if not (args.enzyme or args.ec or args.file or args.replay_manifest):
        parser.error("one of --enzyme, --ec, --file, or --replay-manifest is required")
    if args.timeout <= 0 or args.retries < 0:
        parser.error("--timeout must be positive and --retries must be non-negative")
    selected_format = _select_format(args.output, args.format)
    if args.stream_json and selected_format != "json":
        parser.error("--stream-json requires JSON output; CSV and FASTA are already streamed")
    try:
        if args.replay_manifest:
            queries, manifest_options = _read_request_manifest(args.replay_manifest)
            if args.no_sequences and manifest_options["include_sequences"]:
                raise ValueError("--no-sequences conflicts with the replay manifest")
            if args.reviewed_only and not manifest_options["reviewed_only"]:
                raise ValueError("--reviewed-only conflicts with the replay manifest")
            if args.taxon_id is not None and args.taxon_id != manifest_options["taxon_ids"]:
                raise ValueError("--taxon-id conflicts with the replay manifest")
            include_sequences = manifest_options["include_sequences"]
            reviewed_only = manifest_options["reviewed_only"]
            taxon_ids = manifest_options["taxon_ids"]
        else:
            queries = read_queries(args)
            include_sequences = not args.no_sequences
            reviewed_only = args.reviewed_only
            taxon_ids = args.taxon_id
        if args.file and args.output.resolve() == args.file.resolve():
            raise ValueError("Output path must not overwrite the input file")
        if args.replay_manifest and args.output.resolve() == args.replay_manifest.resolve():
            raise ValueError("Output path must not overwrite the replay manifest")
        if args.save_manifest:
            if args.output.resolve() == args.save_manifest.resolve():
                raise ValueError("Manifest path must not overwrite the output file")
            if args.file and args.save_manifest.resolve() == args.file.resolve():
                raise ValueError("Manifest path must not overwrite the input file")
            if args.replay_manifest and args.save_manifest.resolve() == args.replay_manifest.resolve():
                raise ValueError("Saved manifest path must differ from the replay manifest")
        if not include_sequences and (_select_format(args.output, args.format) == "fasta"):
            raise ValueError("FASTA output requires protein sequences; remove --no-sequences")
        if args.save_manifest:
            _write_request_manifest(args.save_manifest, queries, {
                "include_sequences": include_sequences,
                "reviewed_only": reviewed_only,
                "taxon_ids": taxon_ids,
            })
        cache = HttpCache(args.cache_dir, contact_email=args.contact_email, timeout=args.timeout, retries=args.retries)
        collector = EnzymeCollector(
            cache,
            refresh=args.refresh,
            include_sequences=include_sequences,
            reviewed_only=reviewed_only,
            taxon_ids=taxon_ids,
        )
        if selected_format in {"csv", "fasta"}:
            payload = write_stream_output(args.output, args.format, collector, queries)
        elif args.stream_json:
            payload = write_stream_json_output(args.output, collector, queries)
        else:
            payload = collector.collect(queries)
            write_output(args.output, args.format, payload)
        not_in_expasy = sum(
            result["status"] == "not_found"
            or any(enzyme["status"] == "not_found_in_expasy" for enzyme in result["enzymes"])
            for result in payload["results"]
        )
        LOGGER.info("Wrote %d input result(s) to %s (%d without an ExPASy match)", len(payload["results"]), args.output, not_in_expasy)
        if not args.verbose:
            print(f"Wrote {len(payload['results'])} result(s) to {args.output}; without an ExPASy match: {not_in_expasy}", file=sys.stderr)
        return 0
    except (ValueError, DataSourceError, OSError) as error:
        print(f"enzymeinfocollector: {error}", file=sys.stderr)
        return 1


def _format_bytes(value: int) -> str:
    if value < 1024:
        return f"{value} B"
    if value < 1024 * 1024:
        return f"{value / 1024:.1f} KiB"
    return f"{value / (1024 * 1024):.1f} MiB"
