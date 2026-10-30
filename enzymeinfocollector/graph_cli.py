"""Export previously verified v2 JSON as GraphML or Cytoscape.js JSON.

Example:
  python -m enzymeinfocollector.graph_cli integrated.json --format graphml -o graph.graphml

The graph is a *projection*, not a new database; this command makes no
network requests and does not infer catalysis from enzyme classification.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from .graph_export import graph_json, project_graph, to_graphml
from .sources import DataSourceError, _atomic_write


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Verified JSON from reference_cli")
    parser.add_argument("--format", choices=("graphml", "cytoscape-json"),
                        default="cytoscape-json")
    parser.add_argument("--protein", help="Limit graph to one UniProt accession neighborhood")
    parser.add_argument("-o", "--output", type=Path,
                        help="Output file; defaults to stdout")
    args = parser.parse_args(argv)
    try:
        document = json.loads(args.input.read_text(encoding="utf-8"))
        graph = project_graph(document, protein=args.protein)
        output = to_graphml(graph) if args.format == "graphml" else graph_json(graph)
        if args.output is not None:
            _atomic_write(args.output, output.encode("utf-8"))
        else:
            print(output, end="")
    except (OSError, UnicodeError, ValueError, TypeError, DataSourceError) as error:
        print(f"Graph export failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
