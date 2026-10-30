# v2 bounded EC/name discovery into direct evidence graph

Status: opt-in, bounded user-facing query workflow; it does **not** turn
shared EC classifications into Protein–Reaction evidence.

## Supported commands

~~~bash
python -m enzymeinfocollector.discover_cli \
    --ec 1.11.1.6 --taxon-id 9606 -o network.json
python -m enzymeinfocollector.discover_cli \
    --enzyme Catalase --taxon-id 9606 --format graphml -o network.graphml
python -m enzymeinfocollector.discover_cli \
    --ec 1.11.1.6 --taxon-id 9606 --format reference-json -o verified.json
~~~

`--ec` and `--enzyme` are exclusive. Names are resolved with the existing
ExPASy ENZYME parser and name matcher. **Ambiguous enzyme names** and
transferred/deleted EC assignments fail with an explicit message rather than
arbitrarily selecting a different class. A direct EC query can work when
ExPASy has no matching active record.

Candidate UniProtKB search is a **single bounded REST request**, requesting
at most `--max-proteins + 1` TSV rows, not a full UniProt stream. This extra
row detects truncated candidate lists. Default limit: 3; maximum: 10.
Default filter: reviewed only (Swiss-Prot). Use `--include-unreviewed` to
include TrEMBL candidates. Exact NCBI taxon match is the default;
`--taxon-scope descendants` uses UniProt's taxonomy lineage query rather
than treating a simple numeric organism ID as a tree.

## Scientific semantics

1. The ExPASy name/EC and UniProt search return **candidate proteins**.
   Their search rank is *not* a catalytic activity ranking.
2. For **only** the selected accessions, `DirectEvidenceCollector.collect`
   extracts their **direct UniProtKB catalytic activity→Rhea annotations**.
   Evidence is kept at its actual direction (ECO, source publication etc.).
3. `collect_reference_from_evidence` reuses the existing Rhea RDF participants
   and ChEBI compound adapters. It never creates edges from EC equivalence.
   The existing guardrails remain: 25 distinct Rhea masters and 100 ChEBI
   identifiers per batch. Unsupported chemical participant types fail closed.
4. Unlinked candidate proteins are retained as isolated nodes, and listed
   under `provenance.discovery.without_direct_rhea_annotation`; absence
   of a direct Rhea annotation is **unknown**, not a negative observation.
5. If none of the selected proteins has a direct Rhea annotation the
   command reports a clear error rather than creating an EC-induced graph.

A successful result embeds `provenance.discovery`: original name/EC,
selected classification, UniProt search expression, reviewed/taxonomy
filters, selected candidate accessions, truncated indicator, snapshots
(URL, timestamp, SHA-256), and linked/unlinked accessions. The SHA-256 of the
reference result is recomputed after adding this metadata; it flows through
to GraphML or Cytoscape JSON.

## Limits and reproducibility

This is a live, current-source diagnostic/query tool, not a pinned archival
dataset. ExPASy/UniProt/Rhea/ChEBI releases can change; the output records
retrieval timestamps and hashes but is not equivalent to an official
snapshot of all four data sources.

A match to an EC is not proof of specific substrate activity; a Swiss-Prot
status alone does not make a particular ECO code experimental evidence.
Rhea equation left/right sides are not automatically in vivo
substrate/product roles. No negative enzyme-activity assertions are inferred.

Truncation counts are detected with an extra returned candidate; this does
not give the total number of matches or guarantee that the top N are
representative. The current online UniProt ordering determines the
selection, and this order may change.

Each invocation writes **one** output file atomically (or prints stdout).
To export another format, re-run with the same parameters and the cache;
if upstream data change between runs, the results may differ.

No new mandatory Python dependencies, persistence layer, or graph database.
The original v1 CLI is unchanged.

## Validation

`tests/test_evidence_discovery.py` exercises the bounded query API, exact
versus descendant taxonomy fields, reviewed and unreviewed selectors,
ambiguous names, stale-response rejection, no-direct-evidence failure,
atomic CLI output and no EC-based Cartesian join between proteins/reactions.

A bounded live check uses P04040 with EC 1.11.1.6, taxon 9606, reviewed
only and one selected accession. Public GitHub Actions run the offline
Python suite on 3.11/3.13; optional live checks are separate.
