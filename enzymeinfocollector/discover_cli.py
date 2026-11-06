"""Unified v2 enzyme name / EC → evidence-aware graph CLI.

Examples:
  python -m enzymeinfocollector.discover_cli --ec 1.11.1.6 --taxon-id 9606 -o network.json
  python -m enzymeinfocollector.discover_cli --enzyme Catalase --format graphml -o network.graphml
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

from .evidence_discovery import MAX_DISCOVERY, collect_discovered_graph, preview_candidates
from .graph_export import graph_json, to_graphml
from .reference_dataset import reference_json
from .sources import DataSourceError, HttpCache, _atomic_write, default_cache_dir


def _positive(value: str) -> int:
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Expected a positive integer") from error
    if number <= 0:
        raise argparse.ArgumentTypeError("Expected a positive integer")
    return number


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    query = parser.add_mutually_exclusive_group(required=True)
    query.add_argument("--ec", help="EC class; discovers candidate proteins, NOT catalysis edges")
    query.add_argument("--enzyme", help="ExPASy ENZYME name; ambiguous names require --ec")
    parser.add_argument("--max-proteins", type=_positive, default=3,
                        help=f"Maximum UniProt candidate proteins (1–{MAX_DISCOVERY}; default 3)")
    reviewed = parser.add_mutually_exclusive_group()
    reviewed.add_argument("--reviewed-only", dest="reviewed_only", action="store_true",
                          default=True, help="Include reviewed UniProtKB entries only (default)")
    reviewed.add_argument("--include-unreviewed", dest="reviewed_only", action="store_false",
                          help="Also consider TrEMBL candidates; direct annotation still required")
    parser.add_argument("--taxon-id", type=_positive,
                        help="UniProt NCBI taxonomy ID for candidate discovery")
    parser.add_argument("--taxon-scope", choices=("exact", "descendants"), default="exact",
                        help="Exact organism ID (default) or UniProt taxonomy lineage search")
    parser.add_argument("--format", choices=("cytoscape-json", "graphml", "reference-json", "candidates-json"),
                        default="cytoscape-json",
                        help="Graph (default), GraphML, verified reference, or candidate preview without chemistry")
    parser.add_argument("--select-accession",
                        help="Use only this accession from the current bounded UniProt candidate list")
    parser.add_argument("--expected-search-sha",
                        help="Optional candidate preview selection.source.sha256; fail if the search changed")
    parser.add_argument("--cache-dir", type=Path, default=default_cache_dir())
    parser.add_argument("--contact-email", default=os.environ.get("ENZYMEINFOCOLLECTOR_EMAIL"))
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("-o", "--output", type=Path,
                        help="Atomically write one complete result; otherwise print stdout")
    args = parser.parse_args(argv)
    if args.format == "candidates-json" and args.select_accession is not None:
        parser.error("--select-accession cannot be combined with --format candidates-json")
    if args.expected_search_sha and not args.select_accession:
        parser.error("--expected-search-sha requires --select-accession")
    try:
        cache = HttpCache(args.cache_dir, contact_email=args.contact_email)
        if args.format == "candidates-json":
            preview = preview_candidates(
                cache, ec=args.ec, enzyme=args.enzyme,
                max_proteins=args.max_proteins, reviewed_only=args.reviewed_only,
                taxon_id=args.taxon_id, taxon_scope=args.taxon_scope,
                refresh=args.refresh,
            )
            content = json.dumps(preview, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
            if args.output:
                _atomic_write(args.output, content.encode("utf-8"))
            else:
                print(content, end="")
            if preview["selection"]["truncated"]:
                print("WARNING: UniProt candidate preview is truncated; increase --max-proteins (max 10) "
                      "or narrow taxonomy before choosing.", file=sys.stderr)
            return 0
        reference, graph = collect_discovered_graph(
            cache, ec=args.ec, enzyme=args.enzyme,
            max_proteins=args.max_proteins, reviewed_only=args.reviewed_only,
            taxon_id=args.taxon_id, taxon_scope=args.taxon_scope, refresh=args.refresh,
            select_accession=args.select_accession,
            expected_search_sha=args.expected_search_sha,
        )
        if args.format == "graphml":
            content = to_graphml(graph)
        elif args.format == "reference-json":
            content = reference_json(reference)
        else:
            content = graph_json(graph)
        if args.output:
            _atomic_write(args.output, content.encode("utf-8"))
        else:
            print(content, end="")
        discovery = reference["provenance"]["discovery"]
        selection = discovery["selection"]
        if selection["truncated"]:
            print(
                f"WARNING: UniProt search truncated to {len(selection['selected'])} "
                "candidate proteins; results do not cover all EC-classified proteins.",
                file=sys.stderr,
            )
        if args.select_accession:
            print("NOTE: Explicit accession " + args.select_accession +
                  " selected from bounded UniProt candidates; EC remains classification only.",
                  file=sys.stderr)
        if discovery["without_direct_rhea_annotation"]:
            print(
                "NOTE: selected proteins without direct Rhea links (unknown, not negative): "
                + ", ".join(discovery["without_direct_rhea_annotation"]),
                file=sys.stderr,
            )
        return 0
    except (DataSourceError, ValueError, OSError, TypeError) as error:
        print(f"Evidence discovery failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
