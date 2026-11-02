"""Bounded CLI-level discovery validation; independent source gold in cases.json.

Run on demand with network; never part of offline CI. Upstream HTTP errors,
search-selection drift, and missing direct annotations are distinct statuses.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

from enzymeinfocollector.graph_export import project_graph
from enzymeinfocollector.sources import DataSourceError


def score_reference(case: dict, reference: dict, source_cases: dict) -> dict:
    """Check only a selected *single* accession present in the source truth."""
    label = case["id"]
    expected_acc = case["accession"]
    result = {"id": label, "status": "invalid_output"}
    if reference.get("schema_version") != "2.0-reference-preview":
        return {**result, "reason": "Unexpected reference schema"}
    if reference.get("semantics", {}).get("ec_join") != "prohibited":
        return {**result, "reason": "No explicit EC-join prohibition"}
    provenance = reference.get("provenance", {}).get("discovery", {})
    selection = provenance.get("selection", {})
    chosen = selection.get("selected")
    if not isinstance(chosen, list) or len(chosen) != 1:
        return {**result, "reason": "Expected one selected candidate"}
    candidate = chosen[0]
    settings = case["input"]
    if (selection.get("reviewed_only") is not settings["reviewed_only"]
        or selection.get("taxon_id") != settings["taxon_id"]
        or selection.get("max_proteins") != settings["max_proteins"]):
        return {**result, "reason": "Search provenance disagrees with request"}
    if (candidate.get("taxon_id") != settings["taxon_id"]
        or (settings["reviewed_only"] and candidate.get("reviewed") is not True)):
        return {**result, "reason": "Taxon or reviewed filter bypassed"}
    requested = provenance.get("request", {})
    if "enzyme" in settings:
        if requested.get("kind") != "enzyme" or requested.get("input") != settings["enzyme"]:
            return {**result, "reason": "Name resolution lost"}
    else:
        if requested.get("kind") != "ec" or selection.get("ec") != settings["ec"]:
            return {**result, "reason": "EC search provenance lost"}
    if candidate.get("accession") != expected_acc:
        return {**result, "status": "selection_drift", "observed": candidate.get("accession"),
                "reason": "UniProt candidate ordering/source changed; not a biological false negative"}
    if "truncated" in case and selection.get("truncated") is not case["truncated"]:
        return {**result, "status": "selection_drift", "reason": "UniProt search cardinality changed"}
    if selection.get("truncated") not in (True, False):
        return {**result, "reason": "Missing candidate coverage status"}
    if len(reference.get("proteins", [])) != 1:
        return {**result, "reason": "Expected one source protein"}
    protein = reference["proteins"][0]
    if (protein.get("accession") != expected_acc
        or protein.get("taxon_id") != settings["taxon_id"]
        or protein.get("reviewed") is not candidate.get("reviewed")):
        return {**result, "reason": "Selected protein metadata mismatch"}
    gold = source_cases.get(expected_acc)
    if not isinstance(gold, dict) or not gold.get("uniprot_sha256"):
        return {**result, "status": "unverified_gold"}
    sha = protein.get("source_release", {}).get("sha256")
    if sha != gold["uniprot_sha256"]:
        return {**result, "status": "source_drift", "expected_sha256": gold["uniprot_sha256"],
                "observed_sha256": sha}
    actual = {x.get("reaction", {}).get("master_id")
              for x in protein.get("direct_assertions", [])}
    expected = set(gold["master_ids"])
    if not expected or actual != expected:
        return {**result, "status": "mismatch", "expected_masters": sorted(expected),
                "actual_masters": sorted(actual)}
    if set(provenance.get("direct_rhea_annotated", [])) != {expected_acc}:
        return {**result, "reason": "Discovery missing linked-protein metadata"}
    if provenance.get("without_direct_rhea_annotation") != []:
        return {**result, "reason": "Linked protein classified as unlinked"}
    try:
        graph = project_graph(reference)
    except (ValueError, TypeError, DataSourceError) as error:
        return {**result, "reason": "Graph projection failed: " + str(error)}
    catalyses = [x["data"] for x in graph["elements"]["edges"]
                 if x["data"].get("relation") == "CATALYZES"]
    expected_pairs = {(expected_acc, a["reaction"]["master_id"], a["reaction"]["rhea_id"])
                      for a in protein["direct_assertions"]}
    graph_pairs = {(e["source"].removeprefix("protein:"),
                    e["target"].removeprefix("reaction:"), e["rhea_directional_id"])
                   for e in catalyses}
    if graph_pairs != expected_pairs or len(catalyses) != len(expected_pairs):
        return {**result, "status": "mismatch",
                "reason": "Graph catalytic edges disagree with direct source assertions"}
    if not all(x.get("evidence") for x in catalyses):
        return {**result, "reason": "Catalytic graph edge has no evidence placeholder"}
    if set(x["master_id"] for x in reference.get("reactions", [])) != expected:
        return {**result, "status": "mismatch", "reason": "Reaction coverage differs from official source"}
    return {"id": label, "status": "pass", "accession": expected_acc,
            "master_ids": len(expected), "directional_assertions": len(catalyses),
            "truncated": selection["truncated"],
            "note": "Source/graph consistency only; no independent chemistry or biological truth"}


def classify_failure(case: dict, returncode: int, stderr: str, output_exists: bool) -> dict:
    label = case["id"]
    if output_exists:
        return {"id": label, "status": "unexpected_error", "reason": "Failed CLI wrote output"}
    expectations = {
        "no_direct": "No selected proteins had a direct UniProt",
        "ambiguous": "Ambiguous enzyme name",
    }
    if case["expected"] in expectations and expectations[case["expected"]] in stderr:
        if case["expected"] == "no_direct":
            checked = "checked ['" + case["accession"] + "']"
            if checked not in stderr:
                return {"id": label, "status": "unexpected_error",
                        "reason": "No-direct failure did not identify the expected candidate"}
        return {"id": label, "status": "pass", "expected_failure": case["expected"]}
    if "Request failed for " in stderr or "HTTP Error" in stderr or "timed out" in stderr:
        return {"id": label, "status": "upstream_failure", "reason": stderr[-350:]}
    return {"id": label, "status": "unexpected_error", "returncode": returncode,
            "reason": stderr[-350:]}


def run_case(case: dict, results_dir: Path, source_cases: dict, timeout: int) -> dict:
    target = results_dir / (case["id"] + ".json")
    if target.exists():
        target.unlink()
    flags = case["input"]
    cmd = [sys.executable, "-m", "enzymeinfocollector.discover_cli"]
    if "ec" in flags:
        cmd.extend(["--ec", flags["ec"]])
    else:
        cmd.extend(["--enzyme", flags["enzyme"]])
    if "taxon_id" in flags:
        cmd.extend(["--taxon-id", str(flags["taxon_id"])])
    cmd.extend(["--max-proteins", str(flags["max_proteins"]),
                "--format", "reference-json", "--output", str(target)])
    if not flags["reviewed_only"]:
        cmd.append("--include-unreviewed")
    try:
        process = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout,
                                 cwd=Path(__file__).resolve().parents[2])
    except subprocess.TimeoutExpired:
        return {"id": case["id"], "status": "upstream_failure", "reason": "CLI time limit"}
    if process.returncode:
        return classify_failure(case, process.returncode, process.stderr, target.exists())
    if case["expected"] != "linked":
        return {"id": case["id"], "status": "unexpected_error",
                "reason": "Expected explicit failure; command returned success"}
    try:
        return score_reference(case, json.loads(target.read_text(encoding="utf-8")), source_cases)
    except (OSError, ValueError, TypeError, KeyError) as error:
        return {"id": case["id"], "status": "invalid_output", "reason": str(error)}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    directory = Path(__file__).resolve().parent
    p.add_argument("--manifest", type=Path, default=directory / "discovery_cases.json")
    p.add_argument("--gold", type=Path, default=directory / "cases.json")
    p.add_argument("--results-dir", type=Path, required=True)
    p.add_argument("--max-cases", type=int, default=3)
    p.add_argument("--timeout", type=int, default=90)
    p.add_argument("-o", "--output", type=Path)
    args = p.parse_args(argv)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if manifest.get("schema") != "enzymeinfo-discovery-workflows-v1":
        p.error("Unknown benchmark schema")
    cases = manifest["cases"]
    if not 1 <= args.max_cases <= len(cases) or not 10 <= args.timeout <= 300:
        p.error("Invalid max-cases or timeout")
    official = json.loads(args.gold.read_text(encoding="utf-8"))
    if official.get("schema") != "enzymeinfo-uniprot-direct-benchmark-v1":
        p.error("Unknown source-gold schema")
    source_cases = {x["accession"]: x for x in official["cases"]}
    args.results_dir.mkdir(parents=True, exist_ok=True)
    rows = [run_case(case, args.results_dir, source_cases, args.timeout)
            for case in cases[:args.max_cases]]
    report = {"scope": manifest["scope"], "attempted": len(rows),
              "passed": sum(x["status"] == "pass" for x in rows),
              "inconclusive": sum(x["status"] in ("upstream_failure", "source_drift",
                                                "selection_drift", "unverified_gold") for x in rows),
              "cases": rows}
    output = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.write_text(output, encoding="utf-8")
    else:
        print(output, end="")
    return int(any(x["status"] in ("unexpected_error", "invalid_output", "mismatch")
                   for x in rows))


if __name__ == "__main__":
    raise SystemExit(main())
