"""Compare Rhea RDF chemistry reference against observed official SPARQL rows.

A changed query response SHA invalidates the benchmark snapshot: source_drift.
Reference right/left equation sides are NOT physiological substrate/product roles.
"""
from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re

RHEA = re.compile(r"RHEA:[1-9][0-9]*\Z")
CHEBI = re.compile(r"CHEBI:[1-9][0-9]*\Z")
SHA = re.compile(r"[0-9a-f]{64}\Z")


def _tuple(row):
    master, side, chebi, coeff = (row["master_id"], row["side"],
                                 row["reported_chebi_id"], row["coefficient"])
    if not RHEA.fullmatch(master) or side not in ("left", "right") or not CHEBI.fullmatch(chebi):
        raise ValueError("Invalid Rhea participant identity or equation side")
    try:
        decimal = Decimal(str(coeff))
    except (ValueError, TypeError, InvalidOperation) as error:
        raise ValueError("Invalid participant coefficient") from error
    if not decimal.is_finite() or decimal <= 0:
        raise ValueError("Invalid nonpositive/nonfinite coefficient")
    return (master, side, chebi, format(decimal.normalize(), "f"))


def compare(case, document):
    accession = case["accession"]
    sources = document.get("provenance", {}).get("rhea_participant_queries")
    if not isinstance(sources, list) or not sources:
        return {"accession": accession, "status": "invalid_output", "reason": "No Rhea RDF source snapshot"}
    digests = [item.get("sha256") for item in sources if isinstance(item, dict)]
    if any(not isinstance(x, str) or not SHA.fullmatch(x) for x in digests) or len(digests) != len(sources):
        return {"accession": accession, "status": "invalid_output", "reason": "Invalid query SHA list"}
    if sorted(digests) != sorted(case["expected_query_sha256"]):
        return {"accession": accession, "status": "source_drift",
                "expected": case["expected_query_sha256"], "observed": digests}
    try:
        proteins = document["proteins"]
        if (document["schema_version"] != "2.0-reference-preview"
            or len(proteins) != 1 or proteins[0]["accession"] != accession):
            raise ValueError("Reference document accession/schema mismatch")
        found = []
        for rx in document["reactions"]:
            if rx["status"] != "rhea_record_found":
                raise ValueError("Missing Rhea master")
            master = rx["master_id"]
            for member in rx["compound_membership"]:
                found.append(_tuple(dict(master_id=master, **member)))
                primary = member["primary_chebi_id"]
                if primary is not None and not CHEBI.fullmatch(primary):
                    raise ValueError("Invalid canonical ChEBI identifier")
                if (primary is None) != (member["resolution_status"] == "unresolved"):
                    raise ValueError("Inconsistent ChEBI resolution")
        gold = Counter(_tuple(x) for x in case["members"])
        actual = Counter(found)
        extra = list((actual - gold).elements())
        missing = list((gold - actual).elements())
        return {"accession": accession,
                "status": "pass" if not (extra or missing) else "mismatch",
                "master_ids": len(document["reactions"]),
                "participants": sum(gold.values()),
                "missing": missing, "unexpected": extra}
    except (KeyError, TypeError, AttributeError, ValueError) as error:
        return {"accession": accession, "status": "invalid_output", "reason": str(error)}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--reference-dir", type=Path, required=True,
                   help="Reference-cli output files <accession>.json")
    p.add_argument("--manifest", type=Path, default=Path(__file__).with_name("chemistry_cases.json"))
    p.add_argument("-o", "--output", type=Path)
    args = p.parse_args(argv)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if manifest.get("schema") != "enzymeinfo-rhea-chemistry-v1":
        p.error("Unexpected chemistry reference schema")
    rows = []
    for case in manifest["cases"]:
        f = args.reference_dir / (case["accession"] + ".json")
        if not f.is_file():
            rows.append({"accession": case["accession"], "status": "not_run"})
        else:
            rows.append(compare(case, json.loads(f.read_text(encoding="utf-8"))))
    result = {"scope": manifest["scope"], "cases": rows,
              "passed": sum(x["status"] == "pass" for x in rows),
              "total": len(rows)}
    output = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(output, encoding="utf-8")
    else:
        print(output, end="")
    return int(any(x["status"] not in ("pass", "not_run") for x in rows))


if __name__ == "__main__":
    raise SystemExit(main())
