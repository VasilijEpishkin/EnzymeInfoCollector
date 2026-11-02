# Validation Benchmark v1

This is a **first reproducible benchmark stage**, not a completed 20-protein end-to-end biological validation.

## Source truth and coverage

- The matrix contains 19 UniProtKB entries inspected directly on 2026-11-02 via official JSON REST API. Every entry has a raw-response SHA-256, reviewed status, NCBI taxon ID, EC classification, and the set of *numeric Rhea master IDs appearing in catalytic-activity comments*. These are source observations, NOT laboratory truth. The matrix covers all seven top-level EC classes, several organisms, multiple-reaction enzymes, and four verified examples with no direct numeric Rhea cross-reference.
- The twentieth entry, A0A024QG36, was seen only in an official UniProt TSV search as unreviewed, taxon 61016, EC 1.11.1.6. Its full accession record was NOT inspected; its master IDs are null and it cannot be scored.
- RHEA-COMP entries and non-numeric cross-references do not count as reaction master IDs. An empty set of direct Rhea links is **unknown biology**, not experimental inactivity.
- Source SHA-256 is an HTTP response observation digest, NOT a historical release archive. If the current response differs, status is source_drift, not an automatically inferred code regression.

## Run (opt-in live network; not part of default CI)

From the repository root run:

    python benchmarks/validation_v1/run_live.py --results-dir /tmp/enzyme-validation --max-cases 3
    python benchmarks/validation_v1/compare.py --results-dir /tmp/enzyme-validation -o /tmp/enzyme-validation/report.json

Set --max-cases 19 to try all currently verified source entries. This invokes the existing evidence_cli collect once for each accession, using its cache, upstream provenance and Rhea directional-ID reconciliation. A failing request, missing result, incompatible document, unverified gold or changed source snapshot never silently contributes to false negatives.

The offline comparator scores **source extraction consistency** at the protein-to-Rhea-master-ID level:
TP is present in both UniProt reference snapshot and tool output; FP is output only; FN is reference only. No TN is defined. Statuses not_run, unverified_gold, source_drift and invalid_output are excluded. This is NOT biological precision, evidence-quality assessment, kinetic validation or chemical reaction verification.

## Remaining stages and acceptance

1. Extend and check all directional IDs/ECO/citations against official UniProt annotations and the independent Rhea Swiss-Prot mapping. Check unreviewed entries separately, not with the Swiss-Prot-only mapping.
2. Run reference_cli and independently verify Rhea RDF reaction sides, coefficients and ChEBI identities on a small representative subset. First reference: P04040, RHEA:20309, 2 H2O2 = O2 + 2 H2O; ChEBI:16240, CHEBI:15379, CHEBI:15377.
3. Classify explicit unsupported RHEA-COMP/macromolecular and polymer cases; fail closed, never synthesize EC catalysis or guess chemical protonation.
4. Report actual live results including attempted runs, source drift, truncated/unsupported chemistry, TP/FP/FN and SHA. Do not claim a passing 20-case benchmark until measured.

Sources: https://rest.uniprot.org/uniprotkb/P04040.json and the corresponding accession records; https://ftp.expasy.org/databases/rhea/tsv/rhea-directions.tsv . No new mandatory Python dependencies or new database.
