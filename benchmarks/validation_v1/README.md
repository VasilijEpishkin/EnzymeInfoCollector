# Validation Benchmark v1

This is a **first reproducible benchmark stage**, not a completed 20-protein end-to-end biological validation.

## Source truth and coverage

- The matrix contains 20 UniProtKB entries inspected directly on 2026-11-02 via official JSON REST API. Every entry has a raw-response SHA-256, reviewed status, NCBI taxon ID, EC classification, and the set of *numeric Rhea master IDs appearing in catalytic-activity comments*. These are source observations, NOT laboratory truth. The matrix covers all seven top-level EC classes, several organisms, multiple-reaction enzymes, and five verified examples with no direct numeric Rhea cross-reference.
- A0A024QG36 was independently checked through official UniProt REST JSON: unreviewed TrEMBL catalase, taxon 61016, EC 1.11.1.6, directly annotated master RHEA:20309, with a recorded raw response SHA-256. TrEMBL remains excluded from Swiss-Prot-only independent mapping validation, but can be scored for JSON extraction.
- RHEA-COMP entries and non-numeric cross-references do not count as reaction master IDs. An empty set of direct Rhea links is **unknown biology**, not experimental inactivity.
- Source SHA-256 is an HTTP response observation digest, NOT a historical release archive. If the current response differs, status is source_drift, not an automatically inferred code regression.

## Run (opt-in live network; not part of default CI)

From the repository root run:

    python benchmarks/validation_v1/run_live.py --results-dir /tmp/enzyme-validation --max-cases 3
    python benchmarks/validation_v1/compare.py --results-dir /tmp/enzyme-validation -o /tmp/enzyme-validation/report.json

Set --max-cases 20 to try all currently verified source entries. This invokes the existing evidence_cli collect once for each accession, using its cache, upstream provenance and Rhea directional-ID reconciliation. A failing request, missing result, incompatible document, unverified gold or changed source snapshot never silently contributes to false negatives.

The offline comparator scores **source extraction consistency** at the protein-to-Rhea-master-ID level:
TP is present in both UniProt reference snapshot and tool output; FP is output only; FN is reference only. No TN is defined. Statuses not_run, unverified_gold, source_drift and invalid_output are excluded. This is NOT biological precision, evidence-quality assessment, kinetic validation or chemical reaction verification.

## Remaining stages and acceptance

1. Extend and check all directional IDs/ECO/citations against official UniProt annotations and the independent Rhea Swiss-Prot mapping. Check unreviewed entries separately, not with the Swiss-Prot-only mapping.
2. Run reference_cli and independently verify Rhea RDF reaction sides, coefficients and ChEBI identities on a small representative subset. First reference: P04040, RHEA:20309, 2 H2O2 = O2 + 2 H2O; ChEBI:16240, CHEBI:15379, CHEBI:15377.
3. Classify explicit unsupported RHEA-COMP/macromolecular and polymer cases; fail closed, never synthesize EC catalysis or guess chemical protonation.
4. Report actual live results including attempted runs, source drift, truncated/unsupported chemistry, TP/FP/FN and SHA. Do not claim a passing 20-case benchmark until measured.

Sources: https://rest.uniprot.org/uniprotkb/P04040.json and the corresponding accession records; https://ftp.expasy.org/databases/rhea/tsv/rhea-directions.tsv . No new mandatory Python dependencies or new database.

## Defect reproduced and fixed by this benchmark

During the 20-case live run, 19 entries completed and the valid unreviewed
accession A0A024QG36 was rejected locally as invalid. The shared UniProt
accession regex accidentally accepted 11-character variants and rejected
valid 10-character identifiers. Updated to match the official UniProt help
pattern for 6-/10-character accessions with a regression test.
See https://www.uniprot.org/help/accession_numbers .

## Observed benchmark outcome

See reports/2026-11-02-live.md for the 20-case source-consistency run and four separate full-reference edge/boundary checks. None of these outcomes imply global biological or reaction chemistry accuracy.

## Phase 2: direct evidence details and Rhea chemistry

Official source/experiment report: [reports/2026-11-02-phase2.md](reports/2026-11-02-phase2.md).
These are **separate** bounded checks. Do not confuse accuracy of transferring
a source annotation with proof of an enzyme's activity.

1. Export one UniProt evidence entry at a time with
   `python -m enzymeinfocollector.evidence_cli collect --accession P00558 -o evidence/P00558.json`.
   Then compare against exactly the official UniProt raw response:
   `python benchmarks/validation_v1/phase2_evidence.py --evidence-dir evidence --fetch --accession P00558`.
   Alternatively pass `--raw-dir raw` with archived byte-exact official
   `P00558.json`. Source SHA mismatches are `source_drift`, not wrong ECO.
2. For reviewed proteins, independently compare all Rhea master+direction
   IDs against the official Swiss-Prot mapping with
   `python -m enzymeinfocollector.evidence_cli validate --accession P00558`.
   Do **not** pass TrEMBL to the Swiss-Prot-specific mapping.
3. Export chemistry with
   `python -m enzymeinfocollector.reference_cli --accession P04040 -o ref/P04040.json`.
   For all five reference cases stored in
   [chemistry_cases.json](chemistry_cases.json), run
   `python benchmarks/validation_v1/phase2_chemistry.py --reference-dir ref`.
   A missing case, changed SPARQL response SHA or participant mismatch
   prevents a complete passing benchmark.
   The compact gold records were independently re-read from the
   official Rhea SPARQL result bindings; no textual-equation parsing.
   This checks reported CHEBI IDs, not experimentally validated chemical
   structures or inferred physiological reaction direction.
