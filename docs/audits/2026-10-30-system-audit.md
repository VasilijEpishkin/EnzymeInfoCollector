# EnzymeInfoCollector — system audit (2026-10-30)

**Scope:** GitHub `main` after merged PR #5, #7, #12, #13, #14, #18;
follow-up PR #19 (`feature/ec-taxon-graph-audit`). This is a **bounded
code/data-semantics/repository workflow audit**, not a claim of exhaustive
formal verification or biological validation across every enzyme.

## Architecture and sources reviewed

- v1 CLI, source fetching/cache, collector, streaming output, manifest:
  `cli.py`, `sources.py`, `collector.py`.
- v2 model and direct evidence: `domain.py`, `evidence_sources.py`,
  `evidence_cli.py`.
- Reaction and chemistry: `rhea_participants.py`, `compound_sources.py`,
  `reaction_xrefs.py`, `reference_dataset.py`, `reference_cli.py`.
- Exports/UI: `graph_export.py`, `graph_cli.py`,
  `explorer/model.mjs`, `explorer/app.mjs`, `explorer/index.html`.
- Offline tests, synthetic fixtures, GitHub Actions and public-facing docs.

### Verified invariants

1. **No false protein→reaction join from EC** in canonical v2: the
   direct link comes from UniProt catalytic-activity annotations; EC is
   classification metadata only. This is tested in Python and JS.
2. **Rhea IDs distinguish master from directional variants.** Per-assertion
   ECO/citation evidence stays attached to the relevant direction; lack of
   evidence detail is not a negative biochemical observation.
3. **Rhea RDF is the authority for equation sides and decimal
   coefficients.** Left/right sides are not assumed to be physiological
   substrates/products.
4. **ChEBI primary and secondary identities are resolved explicitly.**
   Unresolved compounds are visible rather than silently removed in the
   canonical graph. Rhea↔KEGG/MetaCyc mappings remain *metadata*, not
   independent direct catalysis evidence; no direct KEGG/BioCyc import.
5. **Provenance** carries retrieved timestamps, source/release details and
   hashes where provided. The new graph/export and local UI preserve them.
6. The original v1 CLI and output formats remain compatible; the v2
   diagnostics, graph and viewer are opt-in. No new mandatory Python runtime
   dependencies or graph DB.

### Findings and remediation

| ID | Severity | Finding | Disposition |
| --- | --- | --- | --- |
| A1 | High | UI EC search had no upstream EC fields and taxonomy filter was unavailable | **Fixed in PR #19**: validated UniProt classifications/taxon propagated to reference, graph and viewer |
| A2 | High | A later SRI HTML edit accidentally replaced new UI controls; Chrome demo failed | **Fixed in PR #19**; added explicit UI-controls regression |
| A3 | Medium | Filter error left previously displayed graph visible, potentially misleading | **Fixed in PR #19**; clears stale drawing/details |
| A4 | Medium | GraphML omitted graph semantics and scoped provenance metadata | **Fixed in PR #19**; graph-level GraphML schema/semantics/scope/reference SHA, round-trip test |
| A5 | Medium | Browser subgraph export discarded reference checksum and scope | **Fixed in PR #19**; preserved and covered in JS tests |
| A6 | Medium | Unknown UniProt `entryType` was interpreted as unreviewed TrEMBL | **Fixed in PR #19**; now explicitly rejects unknown entry types |
| A7 | Medium | CDN library version pinned but lacking cryptographic integrity control | **Mitigated in PR #19** with live-verified SHA-384 SRI and `crossorigin`; fully offline serving still optional |
| A8 | Medium | Draft PR #15 independently recreates graph exporter with incompatible JSON schema and silently drops unresolved compounds | **Superseded** by merged PR #14, preserving unresolved nodes; do not merge both |
| A9 | Medium | v1 accepts enzyme names/EC; v2 evidence graph workflow currently starts from UniProt accessions | **Open**: unified name/EC-first *evidence-aware* discovery API needed, without an EC-based catalysis join |
| A10 | Medium | v1 taxon filter includes descendants; explorer's taxon match is **exact only** | **Documented**; subtree filter needs authoritative, versioned taxonomy lineage |
| A11 | Medium | Live UniProt/Rhea/ChEBI responses have hashes but are not pinned historical release archives | **Open** for strict longitudinal reproduction; raw source archives required only when that use case exists |
| A12 | Low | Rhea RDF records without resolvable ChEBI or unsupported participants currently fail a bounded request | **Documented fail-closed behavior**, not silently changed to absent chemistry |
| A13 | Low | No full mobile/keyboard chart traversal or load testing of large graphs | **Open**; keyboard controls/list navigation work, but full graph accessibility not claimed |
| A14 | Low | GitHub Actions use major-version action tags rather than immutable SHAs | **Open** supply-chain hardening option; SRI covers only the browser CDN |

### Test and integration evidence

- Python: `python -m unittest discover -s tests -v` via GitHub Actions
  on 3.11 and 3.13 (including new strict-entry, EC/taxon and GraphML
  regression tests).
- Node 22: `node --test explorer/tests/model.test.mjs`, JS syntax checks.
- Headless Chrome: synthetic demo displays 5 nodes and 4 edges; explicit
  EC/taxon input presence and SRI hash asserted.
- Live upstream smoke (2026-10-30) on `P04040`: UniProt taxon `9606`,
  organism `Homo sapiens`, EC `1.11.1.6`; Rhea master `20309`,
  three ChEBI compounds, five nodes/four edges in Cytoscape graph.
  The chemical equation is `2 H2O2 = O2 + 2 H2O`.

**Limits of these checks:** one observed positive live end-to-end sample
cannot establish broad biological completeness, balanced atom maps,
correctness for all Rhea participant types or every UniProt release.
Default CI is intentionally offline. It must not be presented as
independent experimental validation.

### Priority follow-ups

1. Finish the **single** canonical evidence-aware discovery flow from
   user-friendly name/EC/taxon input to a bounded accession set, then
   direct UniProt↔Rhea evidence; do not build an EC Cartesian product.
2. Preserve unresolved/unsupported source cases as explicit diagnostic
   status where partial results are acceptable; keep the strict mode
   fail-closed for scientific interpretation.
3. Finish scoped search/CX2 only if required by consumers; GraphML and
   Cytoscape JSON work today. Add browser keyboard/mobile coverage.
4. Consider pinned releases and full licensed bulk snapshots **only**
   if historical reproducibility is a concrete need.
5. Keep SABIO-RK/BRENDA license-gated, separate measurement layers;
   KEGG/MetaCyc direct ingestion requires explicit licensing review.

**Release decision:** PR #19 may be merged after its full Python 3.11/3.13,
Node and headless Chrome checks pass. Do not close graph/explorer Issues
#3/#4 while the follow-ups above remain.
