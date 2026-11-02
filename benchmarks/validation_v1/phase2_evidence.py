"""Audit direction-specific UniProt ECO/citations against raw official JSON.

Opt-in external retrieval (--fetch) or exact archived raw-response bytes.
Not experimental validation, and not the Rhea independent Swiss-Prot mapping.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
from urllib.request import Request, urlopen

RHEA_ID = re.compile(r"RHEA:[1-9][0-9]*\Z")
DIRECTIONS = {"left-to-right": "left_to_right",
              "right-to-left": "right_to_left", "bidirectional": "bidirectional"}


def _records(records):
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("Evidences must be a list")
    found = []
    for item in records:
        if not isinstance(item, dict):
            raise ValueError("Evidence must be a dictionary")
        eco = item.get("evidenceCode")
        citation = (f"{item['source']}:{item['id']}",) if item.get("source") and item.get("id") else ()
        found.append((eco, citation))
    return found or [(None, ())]


def official_assertions(document):
    """Independent observation of UniProt's actual reaction/physiology fields."""
    result = {}
    for comment in document.get("comments", []):
        if comment.get("commentType") != "CATALYTIC ACTIVITY":
            continue
        reaction = comment["reaction"]
        masters = [x["id"] for x in reaction.get("reactionCrossReferences", [])
                   if x.get("database") == "Rhea" and RHEA_ID.fullmatch(x.get("id", ""))]
        if not masters:
            continue  # no direct numeric Rhea assertion; not a negative observation
        if len(masters) != 1:
            raise ValueError("Ambiguous direct Rhea master in source")
        master = masters[0]

        def add(identifier, direction, records):
            existing = result.setdefault(identifier, {
                "master": master, "direction": direction, "evidence": Counter()})
            if existing["master"] != master or (
                existing["direction"] and direction and existing["direction"] != direction
            ):
                raise ValueError("Conflicting UniProt directional assertion")
            if direction and not existing["direction"]:
                existing["direction"] = direction
            existing["evidence"].update(_records(records))

        add(master, "undefined", reaction.get("evidences"))
        for phys in comment.get("physiologicalReactions", []) or []:
            ref = phys.get("reactionCrossReference", {}).get("id")
            if not isinstance(ref, str) or not RHEA_ID.fullmatch(ref):
                raise ValueError("Invalid physiological Rhea ID")
            direction_type = phys.get("directionType")
            direction = DIRECTIONS.get(direction_type) if direction_type is not None else None
            if direction_type is not None and direction is None:
                raise ValueError("Unrecognized source physiological direction")
            if ref == master:
                raise ValueError("Physiological Rhea variant equal to master")
            add(ref, direction, phys.get("evidences"))
    return result


def exported_assertions(protein):
    result = {}
    for assertion in protein.get("assertions", []):
        if assertion.get("accession") != protein["accession"]:
            raise ValueError("Assertion accession mismatch")
        r = assertion["reaction"]
        identifier, master, direction = r["rhea_id"], r["master_id"], r["direction"]
        if not RHEA_ID.fullmatch(identifier) or not RHEA_ID.fullmatch(master):
            raise ValueError("Invalid output Rhea ID")
        if (identifier == master) != (direction == "undefined"):
            raise ValueError("Invalid master/directional pairing")
        if identifier in result:
            raise ValueError("Duplicate output assertion")
        counter = Counter()
        for ev in assertion["evidence"]:
            if ev["source_database"] != "UniProtKB":
                raise ValueError("Unexpected evidence source")
            counter[(ev.get("eco_code"), tuple(ev.get("citation_ids", [])))] += 1
        if not counter:
            raise ValueError("Missing evidence placeholder")
        result[identifier] = {"master": master, "direction": direction,
                              "evidence": counter}
    return result


def compare(source_bytes, exported, expected_sha, accession):
    """No metric if source/export SHA differs from observed reference snapshot."""
    sha = hashlib.sha256(source_bytes).hexdigest()
    if sha != expected_sha:
        return {"accession": accession, "status": "source_drift", "observed_sha": sha}
    proteins = exported.get("proteins")
    if exported.get("schema_version") != "2.0-preview" or not isinstance(proteins, list) or len(proteins) != 1:
        raise ValueError("Expected one evidence_cli collect entry")
    protein = proteins[0]
    if protein.get("accession") != accession or protein.get("source_release", {}).get("sha256") != sha:
        raise ValueError("Accession or export source SHA mismatch")
    original = json.loads(source_bytes)
    if original.get("primaryAccession") != accession:
        raise ValueError("Official source accession mismatch")
    source = official_assertions(original)
    extracted = exported_assertions(protein)
    unexpected, missing, changed = [], [], []
    compared_records = 0
    for identifier in sorted(set(source) | set(extracted)):
        if identifier not in source:
            unexpected.append(identifier)
        elif identifier not in extracted:
            missing.append(identifier)
        else:
            expected, found = source[identifier], extracted[identifier]
            if (expected["master"] != found["master"]
                or (expected["direction"] is not None and expected["direction"] != found["direction"])
                or expected["evidence"] != found["evidence"]):
                changed.append(identifier)
            else:
                compared_records += sum(expected["evidence"].values())
    return {"accession": accession, "status": "pass" if not (unexpected or missing or changed) else "mismatch",
            "rhea_ids_checked": len(source), "evidence_records_matched": compared_records,
            "extra_ids": unexpected, "missing_ids": missing, "changed_ids": changed,
            "raw_sha256": sha}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accession", action="append", required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True,
                        help="One evidence_cli collect <accession>.json per input")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--raw-dir", type=Path, help="Raw official <accession>.json files")
    mode.add_argument("--fetch", action="store_true", help="Explicitly fetch official live UniProt JSON")
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("cases.json"))
    parser.add_argument("-o", "--output", type=Path)
    args = parser.parse_args(argv)
    if not 1 <= len(args.accession) <= 10 or len(set(args.accession)) != len(args.accession):
        parser.error("Supply 1–10 distinct accessions")
    cases = {x["accession"]: x for x in json.loads(args.manifest.read_text(encoding="utf-8"))["cases"]}
    report = []
    for accession in args.accession:
        if accession not in cases or not cases[accession].get("uniprot_sha256"):
            parser.error("Accession missing source-verified benchmark: " + accession)
        if args.fetch:
            req = Request(f"https://rest.uniprot.org/uniprotkb/{accession}.json",
                          headers={"User-Agent": "EnzymeInfoCollector-phase2-audit"})
            with urlopen(req, timeout=20) as response:
                raw = response.read(15 * 1024 * 1024 + 1)
            if len(raw) > 15 * 1024 * 1024:
                raise ValueError("Official UniProt record too large")
        else:
            raw = (args.raw_dir / (accession + ".json")).read_bytes()
        exported = json.loads((args.evidence_dir / (accession + ".json")).read_text(encoding="utf-8"))
        report.append(compare(raw, exported, cases[accession]["uniprot_sha256"], accession))
    result = {"scope": "UniProt source ECO/citation/direction transfer; not biological validation",
              "passed": sum(x["status"] == "pass" for x in report),
              "total": len(report), "cases": report}
    output = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(output, encoding="utf-8")
    else:
        print(output, end="")
    return int(any(row["status"] != "pass" for row in report))


if __name__ == "__main__":
    raise SystemExit(main())
