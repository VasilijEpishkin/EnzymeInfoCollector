# Deterministic evidence-aware reference dataset (PR after core merge)

Status: **offline builder prototype**, not yet a complete five-EC release
dataset. No live dataset or bulk dump is committed.

## Scope and scientific rules

`build_reference_dataset(evidence, rhea_records, compounds, rhea_source=...)`
joins data only through already **directly annotated UniProt→Rhea master IDs**.
It does **not** infer Protein↔Reaction assertions from shared EC classification.

Inputs:
- `DirectEvidenceDataset` from the canonical `DirectEvidenceCollector`
  (UniProt, Rhea directions, ECO/citations, entry/sequence versions).
- Rhea reaction dictionaries from `sources.fetch_rhea`: explicit
  `rhea_id`, `equation`, and flat `chebi_ids`.
- `ChEBICollection` from `ChEBICollector.enrich_rhea_records`: primary
  compound descriptions plus evidence of secondary-ID resolution.
- `SourceRelease(database="Rhea", ...)` for the actual reaction-query snapshot.

The final JSON has `proteins`, `reactions`, `compounds`, `provenance`,
`unresolved`, and `semantic_sha256` fields. The digest covers canonical
UTF-8 JSON **including source retrieval metadata**, excluding the digest
field itself. Reordered identical input records produce byte-identical JSON.

Importing a compound record does not prove a protein catalyzes anything.
Missing reaction rows are reported in `unresolved.missing_rhea_master_ids`;
ChEBI IDs not present in explicitly resolved ChEBI records are preserved
as `unresolved.unresolved_chebi_ids`, not silently dropped.

**Critical limitation:** The current Rhea EC query exposes `chebi_ids`
as a *flat list*, with no authoritative left/right equation side or
stoichiometric coefficient. Every projected chemical membership therefore
has `side: null` and `coefficient: null`. This is *not* yet a full
ReactionParticipant dataset and must not be used as if it had known
substrates, products or mass-balanced stoichiometry.

## Example (illustrative integration)

~~~python
from enzymeinfocollector.compound_sources import ChEBICollector
from enzymeinfocollector.domain import SourceRelease
from enzymeinfocollector.evidence_sources import DirectEvidenceCollector
from enzymeinfocollector.reference_dataset import (
    build_reference_dataset, reference_json,
)
from enzymeinfocollector.sources import HttpCache, default_cache_dir, fetch_rhea

cache = HttpCache(default_cache_dir())
evidence = DirectEvidenceCollector(cache).collect(["P00330", "P00331"])
reactions, metadata = fetch_rhea("1.1.1.1", cache)
compounds = ChEBICollector(cache).enrich_rhea_records(reactions)
rhea_source = SourceRelease(
    database="Rhea",
    release="live-ec-query-not-a-pinned-release",
    retrieved_at=metadata["fetched_at"],
    source_url=metadata["url"],
)
reference = build_reference_dataset(
    evidence, reactions, compounds, rhea_source=rhea_source
)
print(reference_json(reference))
~~~

This example is deliberately **not** a pinned bulk snapshot: timestamps,
upstream responses and ChEBI API results can change. For a reproducible
scientific release, archive the original source files with SHA-256, record
official release IDs where available, and replay from those snapshots.
The builder preserves provenance when supplied; it does not invent it.

## Acceptance and next work

- Offline fixtures verify EC does not create edges, compound secondary IDs
  resolve explicitly, unknowns remain visible, source records cannot
  conflict silently, evidence is preserved and semantic hashes are stable.
- Add a pinned, published UniProt/Rhea/ChEBI reference snapshot covering
  5–10 EC classes, with full source file SHA-256 and legal attributions.
- Add **Rhea reaction participant side + stoichiometry** extraction and
  chemically validated reaction-level joins (not the EC-shaped old format).
- Extend TrEMBL validation to its own mapping and reconcile RDF ECO statement
  provenance where available.
- Keep GraphML/CX2 a projection of these canonical records, not a parallel
  source of truth.

The offline builder is isolated from legacy CLI and adds no runtime
dependencies or network calls to CI.
