"""Offline source-snapshot benchmark for evidence_cli collect outputs.

Measures extraction agreement with observed UniProtKB, not biological truth.
Missing runs, unsupported gold, and source drift never enter denominators.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re

RHEA = re.compile(r"RHEA:[1-9][0-9]*\Z")
SHA = re.compile(r"[a-f0-9]{64}\Z")


def evaluate(manifest: dict, results_dir: Path) -> dict:
    if manifest.get("schema") != "enzymeinfo-uniprot-direct-benchmark-v1":
        raise ValueError("Unsupported benchmark manifest")
    rows = []
    tp = fp = fn = 0
    seen = set()
    for case in manifest["cases"]:
        acc = case["accession"]
        if acc in seen:
            raise ValueError("Duplicate accession: " + acc)
        seen.add(acc)
        row = {"accession": acc, "status": "not_run"}
        expected = case["master_ids"]
        if expected is None:
            row["status"] = "unverified_gold"
            rows.append(row)
            continue
        if not isinstance(expected, list) or len(set(expected)) != len(expected) or any(not RHEA.fullmatch(x) for x in expected):
            raise ValueError("Invalid gold Rhea master set for " + acc)
        path = results_dir / (acc + ".json")
        if not path.is_file():
            rows.append(row)
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            if doc.get("schema_version") != "2.0-preview" or doc.get("unknown_is_not_negative") is not True:
                raise ValueError("Expected evidence_cli collect output")
            proteins = doc.get("proteins")
            if not isinstance(proteins, list) or len(proteins) != 1:
                raise ValueError("Expected one protein per file")
            protein = proteins[0]
            if protein.get("accession") != acc or protein.get("taxon_id") != case["taxon_id"] or protein.get("reviewed") is not case["reviewed"]:
                raise ValueError("Accession, taxon, or reviewed status mismatch")
            release = protein.get("source_release")
            if not isinstance(release, dict) or not SHA.fullmatch(release.get("sha256") or ""):
                raise ValueError("No UniProt response checksum")
            current = set()
            for assertion in protein.get("assertions", []):
                reaction = assertion["reaction"]
                master, variant = reaction["master_id"], reaction["rhea_id"]
                if not RHEA.fullmatch(master) or not RHEA.fullmatch(variant):
                    raise ValueError("Invalid Rhea identifier")
                if (reaction["direction"] == "undefined") != (master == variant):
                    raise ValueError("Inconsistent master/directional assertion")
                if assertion["accession"] != acc or not assertion.get("evidence"):
                    raise ValueError("Invalid assertion evidence or accession")
                current.add(master)
            if not SHA.fullmatch(case.get("uniprot_sha256") or ""):
                raise ValueError("No baseline UniProt checksum")
            if release["sha256"] != case["uniprot_sha256"]:
                row.update(status="source_drift", baseline_sha256=case["uniprot_sha256"],
                           current_sha256=release["sha256"])
                rows.append(row)
                continue
            gold = set(expected)
            common, extras, missing = gold & current, current - gold, gold - current
            row.update(status="pass" if not (extras or missing) else "mismatch",
                       tp=len(common), fp=len(extras), fn=len(missing),
                       only_in_output=sorted(extras), only_in_gold=sorted(missing))
            tp += len(common)
            fp += len(extras)
            fn += len(missing)
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
            row.update(status="invalid_output", error=str(error))
        rows.append(row)
    return {
        "scope": "UniProtKB numeric Rhea master-ID source extraction consistency, NOT biological accuracy",
        "summary": {
            "cases": len(rows), "scored": sum(x["status"] in ("pass", "mismatch") for x in rows),
            "passed": sum(x["status"] == "pass" for x in rows),
            "mismatched": sum(x["status"] == "mismatch" for x in rows),
            "not_run": sum(x["status"] == "not_run" for x in rows),
            "unverified_gold": sum(x["status"] == "unverified_gold" for x in rows),
            "source_drift": sum(x["status"] == "source_drift" for x in rows),
            "invalid_output": sum(x["status"] == "invalid_output" for x in rows),
            "tp": tp, "fp": fp, "fn": fn,
            "source_agreement_precision": tp / (tp + fp) if tp + fp else None,
            "source_agreement_recall": tp / (tp + fn) if tp + fn else None,
        }, "cases": rows,
    }


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", type=Path, default=Path(__file__).with_name("cases.json"))
    p.add_argument("--results-dir", type=Path, required=True)
    p.add_argument("-o", "--output", type=Path)
    args = p.parse_args(argv)
    report = evaluate(json.loads(args.manifest.read_text(encoding="utf-8")), args.results_dir)
    output = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(output, encoding="utf-8")
    else:
        print(output, end="")
    return int(report["summary"]["mismatched"] > 0 or report["summary"]["invalid_output"] > 0)


if __name__ == "__main__":
    raise SystemExit(main())
