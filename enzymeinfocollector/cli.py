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
    parser.add_argument("--output", "-o", type=Path, default=Path("enzyme_results.json"), help="Output file (default: enzyme_results.json)")
    parser.add_argument("--format", choices=("auto", "json", "csv", "fasta"), default="auto", help="Output format; auto uses the output extension")
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
    unique: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for query in queries:
        key = (query[0], query[1].casefold())
        if key not in seen:
            unique.append(query)
            seen.add(key)
    return unique


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
        if args.enzyme or args.ec or args.file:
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
    if not (args.enzyme or args.ec or args.file):
        parser.error("one of --enzyme, --ec, or --file is required")
    if args.timeout <= 0 or args.retries < 0:
        parser.error("--timeout must be positive and --retries must be non-negative")
    try:
        queries = read_queries(args)
        if args.file and args.output.resolve() == args.file.resolve():
            raise ValueError("Output path must not overwrite the input file")
        if args.no_sequences and (args.format == "fasta" or (args.format == "auto" and args.output.suffix.casefold() in {".fa", ".faa", ".fasta"})):
            raise ValueError("FASTA output requires protein sequences; remove --no-sequences")
        cache = HttpCache(args.cache_dir, contact_email=args.contact_email, timeout=args.timeout, retries=args.retries)
        collector = EnzymeCollector(
            cache,
            refresh=args.refresh,
            include_sequences=not args.no_sequences,
            reviewed_only=args.reviewed_only,
            taxon_ids=args.taxon_id,
        )
        if _select_format(args.output, args.format) in {"csv", "fasta"}:
            payload = write_stream_output(args.output, args.format, collector, queries)
        else:
            payload = collector.collect(queries)
            write_output(args.output, args.format, payload)
        not_found = sum(result["status"] == "not_found" for result in payload["results"])
        LOGGER.info("Wrote %d input result(s) to %s (%d unmatched)", len(payload["results"]), args.output, not_found)
        if not args.verbose:
            print(f"Wrote {len(payload['results'])} result(s) to {args.output}; unmatched: {not_found}", file=sys.stderr)
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
