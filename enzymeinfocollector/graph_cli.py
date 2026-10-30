"""Export a verified local reference JSON to GraphML or Cytoscape JSON.

Usage:
 python -m enzymeinfocollector.graph_cli integrated.json --format graphml -o graph.graphml
 python -m enzymeinfocollector.graph_cli integrated.json --format cytoscape-json -o graph.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from .graph_export import cytoscape_json, graph_from_reference, graphml_string
from .sources import _atomic_write


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--format", choices=("graphml", "cytoscape-json"), required=True)
    parser.add_argument("-o", "--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        document = json.loads(args.input.read_text(encoding="utf-8"))
        graph = graph_from_reference(document)
        result = (graphml_string(graph) if args.format == "graphml" else
                  json.dumps(cytoscape_json(graph), ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        _atomic_write(args.output, result.encode("utf-8"))
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Graph export failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
