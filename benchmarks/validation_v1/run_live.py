"""Opt-in live benchmark using the existing evidence_cli. CI remains offline."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", type=Path, default=Path(__file__).with_name("cases.json"))
    p.add_argument("--results-dir", type=Path, required=True)
    p.add_argument("--max-cases", type=int, default=3,
                   help="At most 19 verified records; default 3")
    p.add_argument("--refresh", action="store_true")
    p.add_argument("--timeout", type=int, default=120)
    args = p.parse_args(argv)
    if not 1 <= args.max_cases <= 19 or not 10 <= args.timeout <= 300:
        p.error("--max-cases must be 1..19, --timeout 10..300 seconds")
    cases = json.loads(args.manifest.read_text(encoding="utf-8"))["cases"]
    checked = [x for x in cases if x["master_ids"] is not None]
    if len(checked) < args.max_cases:
        p.error("Not enough checked records")
    args.results_dir.mkdir(parents=True, exist_ok=True)
    failures = []
    for case in checked[:args.max_cases]:
        acc = case["accession"]
        cmd = [sys.executable, "-m", "enzymeinfocollector.evidence_cli",
               "collect", "--accession", acc,
               "-o", str(args.results_dir / (acc + ".json"))]
        if args.refresh:
            cmd.append("--refresh")
        try:
            run = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout)
            if run.returncode:
                failures.append({"accession": acc, "returncode": run.returncode,
                                 "stderr": run.stderr[-1000:]})
        except subprocess.TimeoutExpired:
            failures.append({"accession": acc, "error": "timeout"})
    (args.results_dir / "run_status.json").write_text(
        json.dumps({"attempted": args.max_cases, "failures": failures}, indent=2) + "\n",
        encoding="utf-8")
    print("Attempted", args.max_cases, "verified records; failed:", len(failures))
    print("Compare: python benchmarks/validation_v1/compare.py --results-dir", args.results_dir)
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
