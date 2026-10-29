# Evidence-aware data model — v2 design contract

Status: **PR1 foundation; not yet connected to the legacy collector.**

The current `EnzymeCollector` collects UniProt proteins and Rhea reactions
independently by EC number. Its JSON/CSV/FASTA outputs are **EC-level
aggregations**, not proof that every protein catalyzes every reaction.
The new domain contracts in `enzymeinfocollector/domain.py` do not change
existing CLI behavior. Do not expose them as real annotations until PR2
implements direct-source ingestion.

## Invariants

1. **EC is a classification, never a protein–reaction join key.**
   `ProteinECClassification` and `ReactionECClassification` are independent.
   A direct `ProteinReactionAssertion` must come from a source-specific
   catalytic activity annotation (e.g. UniProt RDF/SPARQL), not from matching EC.
2. **Unknown is not negative.** An absent Protein↔Reaction annotation means
   `unknown`; only an explicit negative experiment could support a negative
   assertion. Predicted associations will be a separate type in later releases.
3. **Master vs directional Rhea IDs are different fields.** `Reaction.master_id`
   identifies the chemical transformation; `ReactionVariant.rhea_id` preserves
   the ID in a source annotation and its direction. Do not collapse LR/RL/BI.
4. **Reaction participants use left/right + stoichiometry.** The master reaction
   does not establish which side is a substrate; derive that from direction.
5. **Evidence belongs to the specific assertion.** Multiple ECO codes and
   citations may exist; `citation_ids` can be empty and `eco_code` nullable.
   Manual curation is not synonymous with experimental evidence.
6. **Snapshots are versioned.** `SourceRelease` records source, release,
   retrieval time and optional raw-file checksum. Sequence identity requires
   accession, isoform, sequence version and sequence SHA-256, not accession alone.
7. **Source assertions and ML predictions are different.** Never put model
   scores into curated evidence; add a separate prediction model later.

## Identifiers and logical relationships

| Concept | Contract / logical key |
| --- | --- |
| Protein entry | UniProt accession |
| Sequence state | accession + isoform + sequence_version + source release; SHA-256 |
| Reaction | Rhea master ID |
| Reaction variant | Rhea directional/undefined ID + master ID + direction |
| Protein–EC | accession + EC + source release |
| Reaction–EC | master Rhea ID + EC + source release |
| Direct protein–reaction assertion | accession + annotated Rhea ID + source release |
| Evidence | one or more records per assertion, ECO/source/citation optional |
| Reaction participant | master Rhea ID + ChEBI ID + left/right + coefficient |

The logical assertion key is not a database primary key. A persistent store
should use a surrogate PK and preserve separate evidence rows, including
historical changes between source releases.

### Example (synthetic; **not** real biochemical evidence)

See `tests/fixtures/evidence_graph/ambiguous_ec.json`.

- Protein P00001 and P00002 are both classified as EC 1.1.1.1.
- Master reactions RHEA:10000 and RHEA:10004 are both classified as EC 1.1.1.1.
- Only **P00001 → RHEA:10001** is explicitly annotated in the synthetic fixture.
- The other three combinations are **unknown**, not inferred edges and not
  negative observations.

The fixture's IDs, ECO and citation examples are illustrative only. No live
source validation or chemical plausibility is claimed.

## Planned extraction and storage (PR2+)

- UniProt RDF/SPARQL: specific catalytic activity statements, ECO attribution,
  optional citation resources, source release; normalize to
  `ProteinReactionAssertion`.
- Rhea: master/directional mapping, ChEBI participants, stoichiometry and
  source-release metadata; reconcile UniProt and Rhea cross-references.
- ExPASy ENZYME: EC nomenclature and transfer history only.
- Persist normalized records independently of GraphML/CX2 and web UI.
- Graph edges will distinguish `CATALYZES` (direct annotation),
  `CLASSIFIED_AS` (EC) and `HAS_PARTICIPANT` (chemical relation).

## Before PR2: real-data validation spike

Use **3–5 documented EC examples** and pin UniProt/Rhea release IDs:

1. Compare UniProt RDF/SPARQL Protein→Catalytic Activity→Rhea with Rhea's
   Swiss-Prot/TrEMBL mapping TSV; record mismatches and missing values.
2. Validate statement-level ECO and optional citations, including cases
   without PMID and cases with multiple evidence records.
3. Reconcile Rhea master and directional IDs using `rhea-directions.tsv`.
4. Check which Protein×Reaction pairs the old EC join would have implied
   without direct evidence. Do **not** call those pairs experimentally false.
5. Record raw-response hashes, sample sizes, failures and source licenses.

Go/no-go for PR2: direct assertions can be traced to source statements with
release/provenance, and any incomplete evidence is explicitly represented.

## Compatibility and verification

- Legacy CLI `--ec`, `--enzyme`, `--file`, JSON, CSV, FASTA and manifests
  retain their current semantics in PR1.
- No new runtime dependencies and no network calls in domain contracts.
- Offline regression suite: `python -m unittest discover -s tests -v`.
- CI runs the suite on supported Python versions.
