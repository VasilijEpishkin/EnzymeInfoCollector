"""Unified v2 enzyme name / EC → evidence-aware graph CLI.

Examples:
  python -m enzymeinfocollector.discover_cli --ec 1.11.1.6 --taxon-id 9606 -o network.json
  python -m enzymeinfocollector.discover_cli --enzyme Catalase --format graphml -o network.graphml
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from .evidence_discovery import MAX_DISCOVERY, collect_discovered_graph
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
    parser.add_argument("--format", choices=("cytoscape-json", "graphml", "reference-json"),
                        default="cytoscape-json",
                        help="Export graph ready for explorer (default), GraphML or verified reference JSON")
    parser.add_argument("--cache-dir", type=Path, default=default_cache_dir())
    parser.add_argument("--contact-email", default=os.environ.get("ENZYMEINFOCOLLECTOR_EMAIL"))
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("-o", "--output", type=Path,
                        help="Atomically write one complete result; otherwise print stdout")
    args = parser.parse_args(argv)
    try:
        cache = HttpCache(args.cache_dir, contact_email=args.contact_email)
        reference, graph = collect_discovered_graph(
            cache, ec=args.ec, enzyme=args.enzyme,
            max_proteins=args.max_proteins, reviewed_only=args.reviewed_only,
            taxon_id=args.taxon_id, taxon_scope=args.taxon_scope, refresh=args.refresh,
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
