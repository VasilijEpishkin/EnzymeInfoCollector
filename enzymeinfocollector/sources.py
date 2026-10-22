"""Small, cache-aware clients for ExPASy ENZYME, UniProtKB, and Rhea."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

ENZYME_URL = "https://ftp.expasy.org/databases/enzyme/enzyme.dat"
UNIPROT_URL = "https://rest.uniprot.org/uniprotkb/stream"
RHEA_URL = "https://www.rhea-db.org/rhea/"
RHEA_DIRECTIONS_URL = "https://ftp.expasy.org/databases/rhea/tsv/rhea-directions.tsv"
RHEA_SMILES_URL = "https://ftp.expasy.org/databases/rhea/tsv/rhea-reaction-smiles.tsv"
EC_PATTERN = re.compile(r"^[1-7]\.(?:\d+|n\d+)\.(?:\d+|n\d+)\.(?:\d+|n\d+)$", re.IGNORECASE)
UNIPROT_ACCESSION = re.compile(r"(?:[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9][A-Z][A-Z0-9]{2}[0-9]|[A-NR-Z][0-9][A-Z0-9]{8}[0-9])", re.IGNORECASE)
MAX_RESPONSE_BYTES = 100 * 1024 * 1024


class DataSourceError(RuntimeError):
    """An upstream service could not provide a complete response."""


@dataclass
class CachedResponse:
    text: str
    metadata: dict[str, Any]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def default_cache_dir() -> Path:
    root = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    return root / "enzymeinfocollector"


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temp.open("wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


class HttpCache:
    def __init__(self, directory: Path, contact_email: str | None = None, timeout: float = 30.0, retries: int = 3):
        self.directory = directory
        self.timeout = timeout
        self.retries = retries
        contact = f"; mailto:{contact_email}" if contact_email else ""
        self.user_agent = f"EnzymeInfoCollector/1.0.0 (+https://github.com/VasilijEpishkin/EnzymeInfoCollector{contact})"

    def get(self, url: str, *, ttl_seconds: int, refresh: bool = False) -> CachedResponse:
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()
        body_path = self.directory / f"{key}.body"
        meta_path = self.directory / f"{key}.json"
        cached_meta: dict[str, Any] | None = None
        cached_age: float | None = None
        try:
            cached_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            body = body_path.read_bytes()
            fetched_at = datetime.fromisoformat(cached_meta["fetched_at"].replace("Z", "+00:00"))
            cached_age = max(0, (datetime.now(timezone.utc) - fetched_at).total_seconds())
            if not refresh and cached_age <= ttl_seconds:
                return CachedResponse(body.decode("utf-8-sig"), {**cached_meta, "cached": True, "stale": False})
        except (OSError, ValueError, KeyError, UnicodeDecodeError):
            body = b""

        headers = {"User-Agent": self.user_agent, "Accept": "text/plain, text/tab-separated-values, */*"}
        if cached_meta:
            if cached_meta.get("etag"):
                headers["If-None-Match"] = cached_meta["etag"]
            if cached_meta.get("last_modified"):
                headers["If-Modified-Since"] = cached_meta["last_modified"]
        last_error: Exception | None = None
        transient_failure = False
        for attempt in range(self.retries + 1):
            request = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = response.read(MAX_RESPONSE_BYTES + 1)
                    if len(payload) > MAX_RESPONSE_BYTES:
                        raise DataSourceError(f"Response from {urllib.parse.urlparse(url).netloc} exceeds 100 MiB")
                    metadata = {
                        "url": url,
                        "fetched_at": utc_now(),
                        "etag": response.headers.get("ETag"),
                        "last_modified": response.headers.get("Last-Modified"),
                        "cached": False,
                        "stale": False,
                    }
                    _atomic_write(body_path, payload)
                    _atomic_write(meta_path, json.dumps(metadata, sort_keys=True).encode("utf-8"))
                    try:
                        text = payload.decode("utf-8-sig")
                    except UnicodeDecodeError as error:
                        raise DataSourceError(f"Response from {urllib.parse.urlparse(url).netloc} is not UTF-8 text") from error
                    return CachedResponse(text, metadata)
            except urllib.error.HTTPError as error:
                if error.code == 304 and body and cached_meta:
                    cached_meta["fetched_at"] = utc_now()
                    _atomic_write(meta_path, json.dumps(cached_meta, sort_keys=True).encode("utf-8"))
                    return CachedResponse(body.decode("utf-8-sig"), {**cached_meta, "cached": True, "stale": False})
                last_error = error
                retry_after = error.headers.get("Retry-After")
                transient_failure = error.code in {429, 500, 502, 503, 504}
                if not transient_failure or attempt >= self.retries:
                    break
                delay = _retry_delay(retry_after, attempt)
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                last_error = error
                transient_failure = True
                if attempt >= self.retries:
                    break
                delay = _retry_delay(None, attempt)
            time.sleep(delay)

        if body and cached_meta and transient_failure and cached_age is not None and cached_age <= 30 * 24 * 60 * 60:
            return CachedResponse(body.decode("utf-8-sig"), {**cached_meta, "cached": True, "stale": True, "warning": str(last_error)})
        raise DataSourceError(f"Request failed for {urllib.parse.urlparse(url).netloc}: {last_error}") from last_error


def _retry_delay(retry_after: str | None, attempt: int) -> float:
    if retry_after:
        try:
            return min(30.0, max(0.0, float(retry_after)))
        except ValueError:
            try:
                stamp = parsedate_to_datetime(retry_after)
                return min(30.0, max(0.0, (stamp - datetime.now(timezone.utc)).total_seconds()))
            except (TypeError, ValueError, OverflowError):
                pass
    return min(8.0, 0.5 * (2**attempt))


def parse_enzyme_dat(text: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    record: dict[str, list[str]] = {}
    previous_key: str | None = None
    for line in text.splitlines():
        if line == "//":
            if record.get("ID"):
                entries.append(_normalize_enzyme_record(record))
            record = {}
            previous_key = None
            continue
        if len(line) >= 5 and line[:2].isalpha() and line[2:5].isspace():
            key = line[:2]
            value = line[5:].strip()
            record.setdefault(key, []).append(value)
            previous_key = key
        elif previous_key and line.strip():
            record[previous_key][-1] = f"{record[previous_key][-1]} {line.strip()}".strip()
    if record.get("ID"):
        entries.append(_normalize_enzyme_record(record))
    return entries


def _normalize_enzyme_record(record: dict[str, list[str]]) -> dict[str, Any]:
    description = record.get("DE", [""])[0].rstrip(".")
    transfer = re.search(r"transferred entry:\s*([\d.n]+(?:\s+and\s+[\d.n]+)*)", description, re.IGNORECASE)
    return {
        "ec_number": record.get("ID", [""])[0],
        "enzyme_name": description,
        "alternative_names": [name.rstrip(".") for name in record.get("AN", []) if name],
        "catalytic_activity": [item.rstrip(".") for item in record.get("CA", []) if item],
        "comments": record.get("CC", []),
        "uniprot_accessions": sorted({
            match.group(1) for row in record.get("DR", [])
            if (match := re.match(r"([A-Z0-9]{6,10})\s*,", row.strip(), re.IGNORECASE)) and UNIPROT_ACCESSION.fullmatch(match.group(1))
        }),
        "status": "deleted" if "deleted entry" in description.casefold() else "transferred" if transfer else "active",
        "transferred_to": re.findall(r"\d+(?:\.\d+){3}|\d+(?:\.\d+){2}\.n\d+", transfer.group(1)) if transfer else [],
    }


def validate_ec_number(value: str) -> str:
    value = value.strip()
    if value[:3].casefold() == "ec ":
        value = value[3:]
    if not EC_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid EC number {value!r}; expected four dot-separated components")
    return value


def find_enzyme_entries(entries: list[dict[str, Any]], name: str) -> list[dict[str, Any]]:
    needle = " ".join(name.casefold().split())
    if not needle:
        raise ValueError("Enzyme name must not be empty")

    def names(entry: dict[str, Any]) -> list[str]:
        return [entry["enzyme_name"], *entry["alternative_names"]]

    exact = [entry for entry in entries if any(" ".join(item.casefold().split()) == needle for item in names(entry))]
    if exact:
        return sorted(exact, key=lambda item: item["ec_number"])
    matches = [entry for entry in entries if any(needle in " ".join(item.casefold().split()) for item in names(entry))]
    return sorted(matches, key=lambda item: item["ec_number"])


def fetch_uniprot(ec_number: str, cache: HttpCache, *, refresh: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    fields = "accession,id,protein_name,gene_names,organism_name,organism_id,ec,sequence,length,reviewed"
    params = urllib.parse.urlencode({"query": f"ec:{ec_number}", "fields": fields, "format": "tsv"})
    url = f"{UNIPROT_URL}?{params}"
    response = cache.get(url, ttl_seconds=24 * 60 * 60, refresh=refresh)
    reader = csv.DictReader(io.StringIO(response.text), delimiter="\t")
    if not reader.fieldnames or not any(name in reader.fieldnames for name in ("Entry", "accession")):
        raise DataSourceError("UniProt response did not contain the expected TSV header")
    records = []
    for row in reader:
        accession = _field(row, "Entry", "accession")
        if not accession:
            continue
        records.append({
            "accession": accession,
            "entry_name": _field(row, "Entry Name", "id"),
            "protein_name": _field(row, "Protein names", "protein_name"),
            "gene_names": _split_gene_names(_field(row, "Gene Names", "gene_names")),
            "organism": _field(row, "Organism", "organism_name"),
            "organism_id": _field(row, "Organism (ID)", "Organism ID", "organism_id"),
            "ec_numbers": _split_values(_field(row, "EC number", "ec")),
            "length": _int_or_none(_field(row, "Length", "length")),
            "reviewed": _field(row, "Reviewed", "reviewed").casefold() == "reviewed",
            "sequence": _field(row, "Sequence", "sequence"),
        })
    records.sort(key=lambda item: item["accession"])
    return records, response.metadata


def fetch_rhea(ec_number: str, cache: HttpCache, *, refresh: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params = urllib.parse.urlencode({
        "query": f"ec:{ec_number}",
        "columns": "rhea-id,equation,chebi,chebi-id,ec",
        "format": "tsv",
        "limit": 10000,
    })
    url = f"{RHEA_URL}?{params}"
    response = cache.get(url, ttl_seconds=24 * 60 * 60, refresh=refresh)
    reader = csv.DictReader(io.StringIO(response.text), delimiter="\t")
    if not reader.fieldnames or not any(name in reader.fieldnames for name in ("Reaction identifier", "Rhea identifier", "Rhea ID", "rhea-id", "RHEA_ID")):
        raise DataSourceError("Rhea response did not contain the expected TSV header")
    records = []
    for row in reader:
        rhea_id = _field(row, "Reaction identifier", "Rhea identifier", "Rhea ID", "rhea-id", "RHEA_ID")
        if not rhea_id:
            continue
        records.append({
            "rhea_id": rhea_id.removeprefix("RHEA:"),
            "equation": _field(row, "Equation", "equation"),
            "chebi_names": _split_values(_field(row, "ChEBI name", "chebi")),
            "chebi_ids": [value.removeprefix("CHEBI:") for value in _split_values(_field(row, "ChEBI identifier", "chebi-id"))],
            "ec_numbers": [value.removeprefix("EC ") for value in _split_values(_field(row, "EC number", "ec"))],
        })
    records.sort(key=lambda item: item["rhea_id"])
    return records, response.metadata


def fetch_reaction_smiles(cache: HttpCache, *, refresh: bool = False) -> tuple[dict[str, str], dict[str, Any]]:
    response = cache.get(RHEA_SMILES_URL, ttl_seconds=7 * 24 * 60 * 60, refresh=refresh)
    smiles_by_id: dict[str, str] = {}
    for row in csv.reader(io.StringIO(response.text), delimiter="\t"):
        match = re.fullmatch(r"(?:RHEA:)?(\d+)", row[0].strip(), re.IGNORECASE) if row else None
        if len(row) >= 2 and match:
            smiles_by_id[match.group(1)] = row[1].strip()
    if not smiles_by_id:
        raise DataSourceError("Rhea reaction-SMILES download contained no reactions")
    return smiles_by_id, response.metadata


def fetch_reaction_directions(cache: HttpCache, *, refresh: bool = False) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
    response = cache.get(RHEA_DIRECTIONS_URL, ttl_seconds=7 * 24 * 60 * 60, refresh=refresh)
    directions: dict[str, dict[str, str]] = {}
    reader = csv.DictReader(io.StringIO(response.text), delimiter="\t")
    if not reader.fieldnames or "RHEA_ID_MASTER" not in reader.fieldnames:
        raise DataSourceError("Rhea directions download did not contain the expected TSV header")
    for row in reader:
        master_id = _field(row, "RHEA_ID_MASTER")
        if not master_id.isdigit():
            continue
        directions[master_id] = {
            "left_to_right": _field(row, "RHEA_ID_LR"),
            "right_to_left": _field(row, "RHEA_ID_RL"),
            "bidirectional": _field(row, "RHEA_ID_BI"),
        }
    if not directions:
        raise DataSourceError("Rhea directions download contained no reaction mappings")
    return directions, response.metadata


def _field(row: dict[str | None, str | None], *names: str) -> str:
    for name in names:
        value = row.get(name)
        if value:
            return value.strip()
    return ""


def _split_values(value: str) -> list[str]:
    return [item.strip() for item in re.split(r"[;|]", value) if item.strip()]


def _split_gene_names(value: str) -> list[str]:
    return [item for group in _split_values(value) for item in group.split()]


def _int_or_none(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
