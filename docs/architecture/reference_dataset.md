# Skvoznaia proverka UniProt → Rhea RDF → ChEBI (v2 preview)

Status: **optional end-to-end integration and deterministic result projection**.
This is a testable feature of EnzymeInfoCollector, **not a separate
published scientific dataset**. The original v1 CLI is unchanged.

## Data provenance and biological semantics

1. `DirectEvidenceCollector` obtains direct UniProtKB catalytic
   annotations, per-statement ECO/citations and Rhea directional/master
   IDs. Shared EC numbers never create a Protein–Reaction edge.
2. `fetch_rhea_participants` fetches the **specific master reaction IDs
   from those assertions**, up to 25 masters and in batches of 5. Its
   bounded Rhea SPARQL queries use `rh:side`, `rh:curatedOrder`,
   `rh:contains` subproperties, `rh:coefficient` and direct
   `rh:chebi`. Rhea RDF responses missing any requested reaction or
   containing unsupported compound types fail closed. No text-equation
   parsing and no invented stoichiometry.
3. `ChEBICollector` enriches only the ChEBI participant IDs required by
   the selected reactions, up to 100 distinct IDs. Primary/secondary
   ID mappings remain explicit.
4. `build_verified_reference_dataset` produces deterministic JSON with
   protein↔Rhea directional assertions, ECO/citations, reaction
   equation sides, exact decimal coefficients, compounds, unresolved
   IDs and per-source retrieval hashes.

**Left/right are Rhea equation sides, not necessarily in-vivo
substrate/product roles.** A coefficient is represented as a decimal
**string**, preventing float rounding. The source's actual ChEBI ID
is retained before canonical normalization. Existing `build_reference_dataset`
still explicitly represents unknown sides/coefficients as `null`
when only legacy flat reaction lists are available.

Snapshot hashes in this initial API are *HTTP response content hashes*,
not claims of official historical bulk release IDs. The result
`semantic_sha256` includes the source retrieval metadata; identical
record content from different retrieval times is therefore not claimed
to be byte-identical. No bulk source dumps are embedded in the repo.

## Invocation (network I/O; excluded from offline CI)

~~~bash
python -m enzymeinfocollector.reference_cli \
  --accession P00330 \
  --output integrated.json

python -m enzymeinfocollector.reference_cli \
  --accession P00330 --refresh --contact-email you@example.org \
  --output integrated.json
~~~

The command is **opt-in**, uses the existing HTTP cache, and writes files
atomically on success. An upstream failure does not replace an existing
output file. At least one direct Rhea annotation is needed; 25 master
reactions / 100 distinct ChEBI compounds are safety ceilings.
Source exceptions (e.g. complex/polymers without a direct `rh:chebi`)
are **reported as an error**, not silently approximated.

## Offline regression acceptance

`python -m unittest discover -s tests -v` on Python 3.11 and 3.13.

Coverage includes accurate Rhea sides and coefficients, duplicate/
contradictory reaction IDs, missing/truncated RDF responses, absence of
ChEBI direct identity, unknown/empty associations, no EC join, source
hashes, stable projection and atomic file writes.

A live integration smoke test **was performed on 2026-10-30** via GitHub
Actions run 41018862354 (Python 3.11). Using UniProt accession
`P04040`, all three providers responded successfully: **1 protein,
1 directly linked Rhea master reaction, and 3 ChEBI compounds**. The
workflow verified the presence of direct catalytic assertions and actual
Rhea sides/coefficients, with no missing master reactions; the API output
was not committed or published. This establishes one full working path,
not broad coverage of all enzymes. It can be repeated manually from
`Optional live UniProt-Rhea-ChEBI smoke` in GitHub Actions.

Offline unit CI stays network-free; live API drift is not its blocker.

## Next work

- Complete manual end-to-end checks for several Swiss-Prot and TrEMBL
  entries, including missing and unsupported compound cases.
- Use a few **small canonical test fixtures** rather than publishing a
  large independent dataset.
- Export GraphML and Cytoscape JSON projections, with typed edges and
  ECO/citation provenance (issue #3). Do not add Neo4j.
- Make the final user-facing CLI ergonomic after validating semantics.

Sources: [Rhea SPARQL API](https://www.rhea-db.org/help/sparql),
[Rhea reaction sides](https://www.rhea-db.org/help/reaction-side-direction),
[UniProt documentation](https://www.uniprot.org/help/api_queries),
[ChEBI REST](https://www.ebi.ac.uk/chebi/backend/api/public/compound/CHEBI:15377/?format=json).
