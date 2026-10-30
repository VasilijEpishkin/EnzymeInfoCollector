# Evidence graph export (GraphML and Cytoscape JSON)

Status: **initial local graph projection**, not a graph database or hosted
visualization. This reads only the existing v2 JSON produced by
`python -m enzymeinfocollector.reference_cli --accession P04040`.

## Semantics

Nodes:
- `Protein`: UniProt accession, reviewed status and entry/sequence version
- `Reaction`: master Rhea ID, equation and status
- `Compound`: canonical ChEBI identity and its source release
- `UnresolvedCompound`: an explicitly reported ChEBI ID whose canonical
  record is unavailable (not silently discarded or fabricated)

Edges:
- `CATALYZES`: **only direct** UniProt protein→Rhea assertions,
  with the source's Rhea directional ID, ECO evidence, citations and
  source release. EC classifications do **not** create catalysis edges.
  Several directional assertions can connect the same protein/master.
- `HAS_PARTICIPANT`: Rhea master→compound with its original reported
  ChEBI ID, left/right equation side, stoichiometric coefficient (decimal
  **string**) and resolution status.

Rhea left/right sides are **arbitrary equation sides** and must not be
presented as in-vivo substrate/product roles without a biologically justified
direction. Source: https://www.rhea-db.org/help/reaction-side-direction

Both output types preserve source metadata. GraphML stores structured
fields as JSON text in explicitly typed GraphML string keys;
Cytoscape JSON exposes `elements.nodes[].data` and
`elements.edges[].data` for local visualization.

## Commands

~~~bash
python -m enzymeinfocollector.reference_cli --accession P04040 -o integrated.json
python -m enzymeinfocollector.graph_cli integrated.json --format graphml -o reaction.graphml
python -m enzymeinfocollector.graph_cli integrated.json --format cytoscape-json -o reaction.json
~~~

These are **opt-in commands** and do not change the legacy
`enzymeinfocollector` CLI, JSON/CSV/FASTA formats, or introduce a database.
Graph projection is network-free and uses only Python's standard library.

## Validation and limitations

- Offline regression suite verifies GraphML XML round-trip, evidence/citation
  preservation, directional assertions, unresolved ChEBI visibility,
  deterministic output, and rejection of missing master/compound references.
- The graph is a **projection** of verified data, not an independent source
  of assertions. Do not treat graph topology as experimental evidence.
- No EC classification nodes or pathway relationships are inferred yet;
  those are future typed relationships with distinct semantics.
- No Cytoscape.js application is included in this PR; browser UI is planned
  separately.
- No mass-balance or atom-mapping chemistry validation is claimed.

## Optional protein neighborhood filter

The graph CLI accepts `--protein P04040` to retain exactly one UniProt
protein, its **already annotated** Rhea master reactions, and the corresponding
compound participants, without introducing EC-derived links. This is an
offline subgraph projection and does not query the remote databases again:

~~~bash
python -m enzymeinfocollector.graph_cli integrated.json \
  --format cytoscape-json --protein P04040 -o neighborhood.json
~~~

A protein with no direct annotations yields a single Protein node and no
catalytic edges. An accession absent from the input is an error; the output
file remains unchanged. The original collection-level provenance is retained
alongside the scoped elements.
