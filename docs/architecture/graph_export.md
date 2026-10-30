# GraphML and Cytoscape JSON (v2 preview)

Goal: **an inspectable export of an already validated result**, not a
second source of biochemical truth, not a standalone graph database.

## Commands

~~~bash
python -m enzymeinfocollector.reference_cli --accession P04040 -o integrated.json
python -m enzymeinfocollector.graph_cli --input integrated.json --format graphml -o graph.graphml
python -m enzymeinfocollector.graph_cli --input integrated.json --format cytoscape-json --protein P04040 -o graph.json
~~~

All commands are additive to the old CLI. Graph projection is local/offline
and uses only Python's standard library.

## Typed graph

| Node type | ID | Meaning |
| --- | --- | --- |
| Protein | `protein:P00330` | UniProt entry |
| ReactionVariant | `variant:RHEA:10741` | Source-annotated directional Rhea ID |
| Reaction | `reaction:RHEA:10740` | Rhea master reaction |
| Compound | `compound:CHEBI:15377` | ChEBI primary compound (or explicitly unresolved reported ID) |

| Directed edge | Meaning and attributes |
| --- | --- |
| `ANNOTATED_CATALYSIS` Protein → ReactionVariant | UniProt direct annotation; `evidence` (ECO/citations), `direction`, `source_release`, `annotation_id` |
| `VARIANT_OF` ReactionVariant → Reaction | Rhea master relationship |
| `HAS_PARTICIPANT` Reaction → Compound | Reported ChEBI ID, Rhea left/right side and decimal stoichiometry (nullable only for old flat-list data) |

A common EC classification **never** creates an `ANNOTATED_CATALYSIS`
edge. A ChEBI identity or KEGG cross-reference also cannot establish
protein catalysis.

GraphML preserves evidence/source-release dictionaries as serialized JSON
inside string-valued edge attributes, compatible with basic GraphML tools.
Cytoscape JSON includes arrays in the edge data attributes for an eventual
inspector panel. The two outputs are deterministic projections of the same
ordered node and edge records.

The optional `--protein` filter selects an existing UniProt accession
and limits output to its direct assertions, Rhea masters and compounds.
This is not yet a multi-hop graph search engine.

## Tests / limitations

`python -m unittest discover -s tests -v` is network-free. It verifies:

- EC-only proteins do not acquire protein–reaction catalytic edges;
- both sides of a reaction retain separate participant edges;
- ECO and PubMed evidence survives GraphML and Cytoscape JSON export;
- input record ordering does not change output;
- malformed/missing evidence or chemistry is rejected;
- a failed CLI projection cannot overwrite an existing file.

The next step is a minimal local Cytoscape.js viewer with source evidence
inspection, after reviewing GraphML and JSON on diverse real reactions.
No Neo4j, PostgreSQL, server/API, or heavy runtime dependencies required.
