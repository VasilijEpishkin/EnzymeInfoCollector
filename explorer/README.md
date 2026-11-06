# Evidence Explorer (local Cytoscape.js interface)

A lightweight **static** reader for the verified v2 graph export. It adds
no Python runtime dependency, no database and no hosted service.

## Start

From the repository root:

~~~bash
python -m http.server 8000 --bind 127.0.0.1 --directory explorer
~~~

Open http://127.0.0.1:8000/ in a browser. Only static explorer/ files are served; the repository root and .git/ remain outside the served directory. Click **Демо** for a
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

## Usability and accessibility (first product iteration)

- The **Таблица связей** toggle shows semantic HTML tables of the **same bounded**
  nodes and edges as the canvas. Each row has a keyboard-usable **Детали**
  action that opens the existing evidence/provenance inspector. It does not
  expand the graph or infer any additional catalysis links.
- Search distinguishes missing local matches from protein records excluded
  by Reviewed, exact taxon or EC filters; an excluded current root and
  zero-edge neighborhoods present an explicit reset action and do not imply
  biological inactivity.
- Discovery truncation and proteins without direct Rhea annotations appear
  in a separate limitations notice. Bad imports keep the previously loaded
  graph and identify it as the retained dataset.
- Graph canvas navigation remains mouse/touch oriented. Keyboard users
  should use the table view, search, filters, inspector and subgraph export.
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
taxonomic *subtree* filtering and richer EC-first discovery remain follow-ups
before closing issue #4.

**Privacy note:** the user's JSON is read in the page via the local File
API; the application itself does not upload it. Cytoscape.js is loaded from
a version-pinned CDN with SHA-384 subresource integrity (SRI) checked by the
browser, mitigating CDN content tampering. For air-gapped workflows serve a
locally licensed copy of the same library. The CDN may still receive the
browser's network request for that library, but not the selected JSON.

**Local-serving security:** never run an unrestricted HTTP file server from the repository root when only the explorer is needed. Keep the binding at 127.0.0.1 and serve explorer/ exclusively. Python http.server is a development server, not production hosting.
