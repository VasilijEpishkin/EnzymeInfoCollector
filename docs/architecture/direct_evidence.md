# Direct UniProt ↔ Rhea evidence — PR2 (opt-in API)

This feature extends the evidence-aware contracts introduced in PR1. The v1
EC-centric CLI/JSON/CSV/FASTA remains unchanged; its EC-level aggregation is
**not** a protein–reaction proof. The new module makes **no EC joins**.

## Data source and interpretation

- UniProtKB REST JSON: \`GET /uniprotkb/{accession}.json\`. Only comments with
  \`commentType == "CATALYTIC ACTIVITY"\` and an explicit numeric Rhea
  cross-reference yield \`ProteinReactionAssertion\` records.
- The \`reaction.reactionCrossReferences\` Rhea ID is the **master/undefined
  direction**. Each \`physiologicalReactions[]\` item can carry its own
  **directional** Rhea ID and its **own** ECO/citations. The master annotation's
  evidence is **not inherited** by its directional variants.
- The Rhea \`rhea-directions.tsv\` snapshot is the authority for master/LR/RL/BI
  reconciliation. Mismatched IDs and directions fail closed.
- ECO codes are retained as published; no assumption that every ECO code is
  experimental. Citations are stored as \`source:id\` (e.g. \`PubMed:6985717\`);
  citations and ECO can be absent. In that case a placeholder
  \`Evidence(source_database="UniProtKB")\` records *unknown evidence detail*,
  **not** an experiment or a negative observation.
- A catalytic activity comment without numeric Rhea ID is **not converted**
  to a graph edge. \`RHEA-COMP\` identifiers are not reaction IDs.
- No RDF statement URI is invented for REST JSON. \`source_statement_iri\`
  stays null until a separate RDF adapter provides a real statement URI.
- Repeated direct comments for one accession/Rhea ID aggregate evidence.
  A protein with no annotated Rhea links has zero assertions (**unknown**).

## Releases, reproducibility and limitations

UniProt's \`reldate.txt\` identifies the current release (e.g. \`2026_03\`).
The UniProt record includes entry/sequence version and the cache's actual
\`fetched_at\` timestamp. If a cached entry predates the release publication
date, the client forces a refresh; stale-cache fallbacks are rejected.

The Rhea directions TSV has no release ID in its header, so its SHA-256 is
stored as a **content-addressed snapshot**, not mislabeled as an official Rhea
release. Retrieval timestamp and source URL are also retained.

**Important:** This live REST workflow is not a pinned historical database
dump; upstream releases can change between requests. For strict reproducible
large-scale datasets, archive raw responses and use a pinned bulk release
instead. The initial API processes up to 100 canonical accessions per call.

## Python usage

\`\`\`python
from dataclasses import asdict
import json

from enzymeinfocollector.evidence_sources import DirectEvidenceCollector
from enzymeinfocollector.sources import HttpCache, default_cache_dir

cache = HttpCache(default_cache_dir(), contact_email="your-email@example.org")
result = DirectEvidenceCollector(cache).collect(["P00330", "P00331"])
print(json.dumps(asdict(result), indent=2))
\`\`\`

This API performs live HTTP requests on first use; subsequent calls use the
existing cache. There are no new runtime dependencies.

## Five-protein live validation (2026-10-29)

Validation queried official UniProtKB JSON, the Rhea direction TSV and
\`rhea2uniprot_sprot.tsv\`. The UniProt release file reported **2026_03**;
the Rhea TSV response had \`Last-Modified: 02 Sep 2026\`.

| UniProt accession | EC numbers in catalytic comments | Master reactions | Distinct master + directional IDs | Rhea mapping IDs | Difference |
| --- | --- | ---: | ---: | ---: | ---: |
| P00330 | 1.1.1.1, 1.1.1.54, 1.1.1.78 | 12 | 24 | 24 | 0 |
| P00331 | 1.1.1.1 | 5 | 9 | 9 | 0 |
| P00558 | 2.7.2.3, 2.7.11.1 | 2 | 4 | 4 | 0 |
| P00918 | 4.2.1.1, 4.2.1.69 | 2 | 2 | 2 | 0 |
| P00533 | 2.7.10.1 | 1 | 1 | 1 | 0 |

For these **five examples only**, the set of Rhea IDs from UniProt REST
(catalytic master + physiological direction) exactly matched the official
Swiss-Prot mapping table. This is **not** a global coverage guarantee.

Additional observed cases:
- P00330: 22 master-reaction evidence records across 12 comments; multiple
  ECO/citations and both left-to-right and right-to-left physiological IDs.
- P00558: two directions for master RHEA:14801, each with its own evidence.
- P00533: at least one evidence record without a PubMed reference.

## Verification

Offline: \`python -m unittest discover -s tests -v\`.

Live manual validation: fetch the five UniProt accession JSON documents,
\`rhea-directions.tsv\` and \`rhea2uniprot_sprot.tsv\` and compare sets of
numeric Rhea IDs **after normalizing the "RHEA:" prefix**. The mapping TSV
includes both master and directional IDs; comparing only master IDs to the
entire mapping is invalid.
