"""Project an existing EnzymeInfoCollector v2 result to GraphML/Cytoscape JSON.

Usage:
  python -m enzymeinfocollector.graph_cli --input integrated.json --format graphml -o graph.graphml
  python -m enzymeinfocollector.graph_cli --input integrated.json --format cytoscape-json --protein P04040
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from .graph_export import build_evidence_graph, cytoscape_json, graphml
from .sources import DataSourceError, _atomic_write


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="v2 reference JSON from reference_cli")
    parser.add_argument("--format", choices=("graphml", "cytoscape-json"), required=True)
    parser.add_argument("--protein", help="Optional UniProt accession neighborhood")
    parser.add_argument("-o", "--output", type=Path, help="Path to output or stdout")
    args = parser.parse_args(argv)
    try:
        data = json.loads(args.input.read_text(encoding="utf-8"))
        projected = build_evidence_graph(data, protein=args.protein)
        text = graphml(projected) if args.format == "graphml" else cytoscape_json(projected)
        if args.output is not None:
            _atomic_write(args.output, text.encode("utf-8"))
        else:
            print(text, end="")
    except (OSError, ValueError, TypeError, DataSourceError) as error:
        print(f"Graph export failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
