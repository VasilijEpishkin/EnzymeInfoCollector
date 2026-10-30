# Evidence Explorer (local Cytoscape.js interface)

A lightweight **static** reader for the verified v2 graph export. It adds
no Python runtime dependency, no database and no hosted service.

## Start

From the repository root:

~~~bash
python -m http.server 8000 --bind 127.0.0.1
~~~

Open http://127.0.0.1:8000/explorer/ in a browser. Click **Демо** for a
*synthetic* sample (not a biological claim), or generate actual source data:

~~~bash
python -m enzymeinfocollector.reference_cli --accession P04040 -o integrated.json
python -m enzymeinfocollector.graph_cli integrated.json --format cytoscape-json -o network.json
~~~

Click **Открыть граф JSON** and choose network.json. **No file is uploaded**:
the browser reads it locally using the File API. Cytoscape.js itself is loaded
from a version-pinned CDN
(\`cytoscape@3.34.4\`, MIT license); this one library request requires an
internet connection. To run offline, obtain the same licensed JS distribution
and replace the script URL in \`index.html\` with a local path, retaining its
license notice.

## What the explorer supports

- Search by UniProt accession, Rhea master/directional ID and ChEBI ID.
  For a directional Rhea ID, search selects its master reaction node.
- One- or two-hop **bounded neighborhoods**, max 120 visible nodes and 400
  visible edges. The UI never attempts to draw the entire supplied graph.
- Filter by relation type, curated (reviewed) proteins and ECO code on
  catalytic edges. Missing ECO is unknown rather than negative evidence.
- Inspect node attributes, Rhea stoichiometry and equation sides, evidence
  codes, PubMed citations and upstream release/provenance.
- Source links are generated **only** for validated UniProt, Rhea, ChEBI
  and PubMed identifiers. Displayed source data use textContent, not HTML.
- Export the current scoped neighborhood as JSON, including original
  provenance, filter settings and a clear "truncated" marker.

The application is **read-only** and operates exclusively on the JSON output
from \`graph_cli\`. No inferred catalysis edges are introduced by a shared EC
number. It does not claim that Rhea left/right is a biological substrate/
product distinction.

## Limitations

- Only current \`2.0-graph-preview\` Cytoscape JSON is supported. The
  explorer rejects malformed schemas, missing graph endpoints, unsupported
  relationship types and overlarge inputs (8 MB file, 16,000 elements).
- Exact UniProt taxonomy ID and EC-classification filters are available on
  protein nodes when source records contain those attributes. Missing values
  are unknown, not negative observations. Taxonomy matches are exact, **not**
  a search of taxonomic descendants.
- EC classification does not imply direct catalytic evidence. Searching EC
  highlights only proteins actually present in the selected local graph;
  it never creates new Protein–Reaction edges or queries external databases.
- No streaming large graphs or database integration; use the Python
  exporters to scope the data at source.
- The diagram itself is mouse/touch oriented; the search list, filters,
  card data and external links are usable via keyboard. The viewer does
  not yet provide complete keyboard traversal of every graph edge.

## Offline regression tests

~~~bash
node --test explorer/tests/model.test.mjs
node --check explorer/app.mjs
~~~

GitHub Actions executes the model tests under Node.js 22 separately from
the existing Python 3.11/3.13 suite. The Node tests are network-free.
An additional **headless Chrome UI smoke test** loads the synthetic demo and
checks that all five nodes/four edges render in the browser. That smoke check
downloads the pinned Cytoscape.js script from its CDN.

Interactive desktop/mobile review with real user-selected JSON files and
the currently unavailable taxon/EC-classification filters remain follow-ups
before closing issue #4.

**Privacy note:** the user's JSON is read in the page via the local File
API; the application itself does not upload it. Cytoscape.js is loaded from
a version-pinned CDN with SHA-384 subresource integrity (SRI) checked by the
browser, mitigating CDN content tampering. For air-gapped workflows serve a
locally licensed copy of the same library. The CDN may still receive the
browser's network request for that library, but not the selected JSON.
