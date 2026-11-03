# Validation phase 3 — name/EC workflows

**Scope:** end-user flow name/EC → bounded UniProt candidate → direct Rhea → chemistry → graph, not a biological recall/accuracy claim.

The [discovery_cases.json](discovery_cases.json) matrix includes seven opt-in scenarios: catalase by name and EC; bacterial phosphoglycerate kinase; human translocase; TrEMBL catalase; pig amylase with no numeric Rhea annotation; and ambiguous exact enzyme name luciferase (four EC matches).

From the repository root, run:

    python -m benchmarks.validation_v1.phase3_discovery --results-dir /tmp/eic-phase3 --max-cases 3 --timeout 90

Maximum is 7. This **does perform external HTTP** and is not part of default CI. For each linked result, compare chosen accession, raw UniProt response SHA, annotated Rhea master IDs, and projected graph CATALYZES edges against the previously checked [cases.json](cases.json). This isolates EC candidate search from direct catalytic evidence.

UniProt candidate ordering is non-authoritative; an unexpected candidate is selection_drift, not a false biological finding. HTTP 503/timeout is upstream_failure, not zero activity. The known absence of numeric direct Rhea annotations in pig amylase must produce an explicit failure without an output file. An ambiguous name must not select a random EC. The benchmarking script distinguishes pass, selection_drift, source_drift, upstream_failure, invalid_output, mismatch, unexpected_error.

Remaining work: live verification of all seven scenarios as API health permits, independent interpretation of ExPASy names, and user evaluation of non-singleton searches. No additional database, dependencies, or alteration to canonical protein–reaction assertions.

## Follow-up live verification (2026-11-03)

See [reports/2026-11-03-discovery-followup.md](reports/2026-11-03-discovery-followup.md) for real former-HTTP-503 reruns, documented Na/K-ATPase candidate selection drift, EC=1.1.1.1 truncation, taxon exact/descendants, ExPASy transfer/deletion cases, GraphML/Cytoscape provenance equivalence and unsupported/no-direct failures. No new global biological accuracy claim.
