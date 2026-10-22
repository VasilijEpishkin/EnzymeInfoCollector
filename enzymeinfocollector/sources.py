"""Small, cache-aware clients for ExPASy ENZYME, UniProtKB, and Rhea."""

from __future__ import annotations

import csv
import codecs
import errno
import heapq
import http.client
import hashlib
import io
import json
import os
import pickle
import re
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from contextlib import ExitStack, contextmanager
from typing import Any, Iterator, TextIO

ENZYME_URL = "https://ftp.expasy.org/databases/enzyme/enzyme.dat"
UNIPROT_URL = "https://rest.uniprot.org/uniprotkb/stream"
RHEA_URL = "https://www.rhea-db.org/rhea/"
RHEA_DIRECTIONS_URL = "https://ftp.expasy.org/databases/rhea/tsv/rhea-directions.tsv"
RHEA_SMILES_URL = "https://ftp.expasy.org/databases/rhea/tsv/rhea-reaction-smiles.tsv"
EC_PATTERN = re.compile(r"^[1-7]\.(?:\d+|n\d+)\.(?:\d+|n\d+)\.(?:\d+|n\d+)$", re.IGNORECASE)
UNIPROT_ACCESSION = re.compile(r"(?:[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9][A-Z][A-Z0-9]{2}[0-9]|[A-NR-Z][0-9][A-Z0-9]{8}[0-9])", re.IGNORECASE)
MAX_RESPONSE_BYTES = 100 * 1024 * 1024
RHEA_RESULT_LIMIT = 10_000
UNIPROT_SORT_CHUNK_SIZE = 2_000
UNIPROT_MERGE_FAN_IN = 64


class DataSourceError(RuntimeError):
    """An upstream service could not provide a complete response."""


@dataclass
class CachedResponse:
    text: str
    metadata: dict[str, Any]


@contextmanager
def _cache_lock(directory: Path, key: str) -> Iterator[None]:
    """Serialize readers and publishers for one cache key across processes."""
    directory.mkdir(parents=True, exist_ok=True)
    lock_path = directory / f"{key}.lock"
    with lock_path.open("a+b") as stream:
        if os.name == "nt":
            import msvcrt

            stream.seek(0, os.SEEK_END)
            if stream.tell() == 0:
                stream.seek(0)
                stream.write(b"\0")
                stream.flush()
            stream.seek(0)
            while True:
                try:
                    msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError as error:
                    if error.errno != errno.EACCES:
                        raise
                    time.sleep(0.1)
            try:
                yield
            finally:
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


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


def _read_cache_metadata(path: Path) -> dict[str, Any]:
    metadata = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(metadata, dict) or not isinstance(metadata.get("fetched_at"), str):
        raise ValueError("Invalid cache metadata")
    return metadata


def _cache_fetched_at(metadata: dict[str, Any]) -> datetime:
    fetched_at = datetime.fromisoformat(metadata["fetched_at"].replace("Z", "+00:00"))
    if fetched_at.tzinfo is None:
        raise ValueError("Cache timestamp must include a timezone")
    return fetched_at


class HttpCache:
    def __init__(self, directory: Path, contact_email: str | None = None, timeout: float = 30.0, retries: int = 3):
        self.directory = directory
        self.timeout = timeout
        self.retries = retries
        contact = f"; mailto:{contact_email}" if contact_email else ""
        self.user_agent = f"EnzymeInfoCollector/1.0.0 (+https://github.com/VasilijEpishkin/EnzymeInfoCollector{contact})"

    def get(self, url: str, *, ttl_seconds: int, refresh: bool = False) -> CachedResponse:
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()
        with _cache_lock(self.directory, key):
            return self._get_locked(url, ttl_seconds=ttl_seconds, refresh=refresh)

    def _get_locked(self, url: str, *, ttl_seconds: int, refresh: bool = False) -> CachedResponse:
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()
        body_path = self.directory / f"{key}.body"
        meta_path = self.directory / f"{key}.json"
        cached_meta: dict[str, Any] | None = None
        cached_age: float | None = None
        try:
            cached_meta = _read_cache_metadata(meta_path)
            body = body_path.read_bytes()
            fetched_at = _cache_fetched_at(cached_meta)
            cached_age = max(0, (datetime.now(timezone.utc) - fetched_at).total_seconds())
            if not refresh and cached_age <= ttl_seconds:
                return CachedResponse(body.decode("utf-8-sig"), {**cached_meta, "cached": True, "stale": False})
        except (OSError, ValueError, KeyError, UnicodeDecodeError):
            cached_meta = None
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
                    response_headers = response.headers
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
            except (urllib.error.URLError, TimeoutError) as error:
                last_error = error
                transient_failure = True
                if attempt >= self.retries:
                    break
                delay = _retry_delay(None, attempt)
            else:
                if len(payload) > MAX_RESPONSE_BYTES:
                    raise DataSourceError(f"Response from {urllib.parse.urlparse(url).netloc} exceeds 100 MiB")
                try:
                    text = payload.decode("utf-8-sig")
                except UnicodeDecodeError as error:
                    raise DataSourceError(f"Response from {urllib.parse.urlparse(url).netloc} is not UTF-8 text") from error
                metadata = {
                    "url": url,
                    "fetched_at": utc_now(),
                    "etag": response_headers.get("ETag"),
                    "last_modified": response_headers.get("Last-Modified"),
                    "cached": False,
                    "stale": False,
                }
                _atomic_write(body_path, payload)
                _atomic_write(meta_path, json.dumps(metadata, sort_keys=True).encode("utf-8"))
                return CachedResponse(text, metadata)
            time.sleep(delay)

        if body and cached_meta and transient_failure and cached_age is not None and cached_age <= 30 * 24 * 60 * 60:
            return CachedResponse(body.decode("utf-8-sig"), {**cached_meta, "cached": True, "stale": True, "warning": str(last_error)})
        raise DataSourceError(f"Request failed for {urllib.parse.urlparse(url).netloc}: {last_error}") from last_error

    @contextmanager
    def open_text(self, url: str, *, ttl_seconds: int, refresh: bool = False) -> Iterator[tuple[TextIO, dict[str, Any]]]:
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()
        with _cache_lock(self.directory, key):
            with self._open_text_locked(url, ttl_seconds=ttl_seconds, refresh=refresh) as response:
                yield response

    @contextmanager
    def _open_text_locked(self, url: str, *, ttl_seconds: int, refresh: bool = False) -> Iterator[tuple[TextIO, dict[str, Any]]]:
        """Open a cached response as text without copying the complete body into memory."""
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()
        body_path = self.directory / f"{key}.body"
        meta_path = self.directory / f"{key}.json"
        cached_meta: dict[str, Any] | None = None
        cached_age: float | None = None
        fresh_cache = False
        try:
            cached_meta = _read_cache_metadata(meta_path)
            fetched_at = _cache_fetched_at(cached_meta)
            cached_age = max(0, (datetime.now(timezone.utc) - fetched_at).total_seconds())
            if body_path.stat().st_size > MAX_RESPONSE_BYTES:
                raise DataSourceError("Cached response exceeds 100 MiB")
            _validate_utf8_file(body_path)
            fresh_cache = not refresh and cached_age <= ttl_seconds
        except (OSError, ValueError, KeyError, UnicodeDecodeError):
            cached_meta = None
            cached_age = None
        if fresh_cache and cached_meta:
            with body_path.open("r", encoding="utf-8-sig", newline="") as stream:
                yield stream, {**cached_meta, "cached": True, "stale": False}
            return

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
                response = urllib.request.urlopen(request, timeout=self.timeout)
            except urllib.error.HTTPError as error:
                if error.code == 304 and cached_meta and body_path.is_file():
                    cached_meta["fetched_at"] = utc_now()
                    _atomic_write(meta_path, json.dumps(cached_meta, sort_keys=True).encode("utf-8"))
                    with body_path.open("r", encoding="utf-8-sig", newline="") as stream:
                        yield stream, {**cached_meta, "cached": True, "stale": False}
                    return
                last_error = error
                retry_after = error.headers.get("Retry-After")
                transient_failure = error.code in {429, 500, 502, 503, 504}
                if not transient_failure or attempt >= self.retries:
                    break
                delay = _retry_delay(retry_after, attempt)
            except (urllib.error.URLError, TimeoutError) as error:
                last_error = error
                transient_failure = True
                if attempt >= self.retries:
                    break
                delay = _retry_delay(None, attempt)
            else:
                self.directory.mkdir(parents=True, exist_ok=True)
                descriptor, temp_name = tempfile.mkstemp(prefix=f".{body_path.name}.", suffix=".tmp", dir=self.directory)
                temp_path = Path(temp_name)
                decoder = codecs.getincrementaldecoder("utf-8-sig")()
                size = 0
                try:
                    with os.fdopen(descriptor, "wb") as output, response:
                        while True:
                            try:
                                chunk = response.read(1024 * 1024)
                            except (OSError, http.client.HTTPException) as error:
                                raise urllib.error.URLError(error) from error
                            if not chunk:
                                break
                            size += len(chunk)
                            if size > MAX_RESPONSE_BYTES:
                                raise DataSourceError(f"Response from {urllib.parse.urlparse(url).netloc} exceeds 100 MiB")
                            decoder.decode(chunk)
                            output.write(chunk)
                        decoder.decode(b"", final=True)
                        output.flush()
                        os.fsync(output.fileno())
                    metadata = {
                        "url": url,
                        "fetched_at": utc_now(),
                        "etag": response.headers.get("ETag"),
                        "last_modified": response.headers.get("Last-Modified"),
                        "cached": False,
                        "stale": False,
                    }
                    # Drop validators before replacing the body so a failed metadata write
                    # cannot associate old validators with the new response.
                    meta_path.unlink(missing_ok=True)
                    os.replace(temp_path, body_path)
                    _atomic_write(meta_path, json.dumps(metadata, sort_keys=True).encode("utf-8"))
                except UnicodeDecodeError as error:
                    raise DataSourceError(f"Response from {urllib.parse.urlparse(url).netloc} is not UTF-8 text") from error
                except (urllib.error.URLError, TimeoutError, http.client.HTTPException) as error:
                    last_error = error
                    transient_failure = True
                    if attempt >= self.retries:
                        break
                    time.sleep(_retry_delay(None, attempt))
                    continue
                finally:
                    temp_path.unlink(missing_ok=True)
                with body_path.open("r", encoding="utf-8-sig", newline="") as stream:
                    yield stream, metadata
                return
            time.sleep(delay)

        if cached_meta and body_path.is_file() and transient_failure and cached_age is not None and cached_age <= 30 * 24 * 60 * 60:
            with body_path.open("r", encoding="utf-8-sig", newline="") as stream:
                yield stream, {**cached_meta, "cached": True, "stale": True, "warning": str(last_error)}
            return
        raise DataSourceError(f"Request failed for {urllib.parse.urlparse(url).netloc}: {last_error}") from last_error

    def inspect(self) -> dict[str, Any]:
        entries = []
        if self.directory.is_dir():
            keys = {
                path.stem for path in self.directory.iterdir()
                if path.suffix in {".body", ".json"} and re.fullmatch(r"[0-9a-f]{64}", path.stem)
            }
            for key in sorted(keys):
                with _cache_lock(self.directory, key):
                    body_path = self.directory / f"{key}.body"
                    meta_path = self.directory / f"{key}.json"
                    has_body, has_metadata = body_path.is_file(), meta_path.is_file()
                    size_bytes = body_path.stat().st_size if has_body else 0
                    entry: dict[str, Any] = {"body_bytes": size_bytes, "fetched_at": None, "host": None, "age_seconds": None}
                    if not (has_body and has_metadata):
                        entry["state"] = "orphan"
                    else:
                        try:
                            metadata = _read_cache_metadata(meta_path)
                            fetched_at = _cache_fetched_at(metadata)
                            entry.update({
                                "fetched_at": fetched_at.isoformat(),
                                "host": urllib.parse.urlsplit(metadata.get("url", "")).hostname,
                                "age_seconds": max(0, (datetime.now(timezone.utc) - fetched_at).total_seconds()),
                                "state": "ok",
                            })
                        except (OSError, TypeError, ValueError):
                            entry["state"] = "invalid"
                    entries.append(entry)
            lock_files = [
                path for path in self.directory.iterdir()
                if path.suffix == ".lock" and re.fullmatch(r"[0-9a-f]{64}", path.stem)
            ]
        else:
            lock_files = []
        return {
            "entry_count": len(entries),
            "body_bytes": sum(entry["body_bytes"] for entry in entries),
            "lock_count": len(lock_files),
            "lock_bytes": sum(path.stat().st_size for path in lock_files),
            "orphan_count": sum(entry["state"] == "orphan" for entry in entries),
            "invalid_count": sum(entry["state"] == "invalid" for entry in entries),
            "entries": entries,
        }

    def prune(self, *, older_than_days: int = 30, apply: bool = False) -> dict[str, int]:
        if older_than_days < 30:
            raise ValueError("Cache retention must be at least 30 days")
        cutoff_seconds = older_than_days * 24 * 60 * 60
        candidates = 0
        candidate_bytes = 0
        removed = 0
        removed_bytes = 0
        if not self.directory.is_dir():
            return {"eligible_count": 0, "eligible_bytes": 0, "removed_count": 0, "removed_bytes": 0}
        keys = {
            path.stem for path in self.directory.glob("*.json")
            if re.fullmatch(r"[0-9a-f]{64}", path.stem)
        }
        for key in sorted(keys):
            with _cache_lock(self.directory, key):
                body_path = self.directory / f"{key}.body"
                meta_path = self.directory / f"{key}.json"
                try:
                    if not body_path.is_file() or not meta_path.is_file():
                        continue
                    metadata = _read_cache_metadata(meta_path)
                    fetched_at = _cache_fetched_at(metadata)
                    age_seconds = (datetime.now(timezone.utc) - fetched_at).total_seconds()
                    if age_seconds <= cutoff_seconds:
                        continue
                    size_bytes = body_path.stat().st_size + meta_path.stat().st_size
                except (OSError, TypeError, ValueError):
                    continue
                candidates += 1
                candidate_bytes += size_bytes
                if apply:
                    body_path.unlink()
                    meta_path.unlink()
                    removed += 1
                    removed_bytes += size_bytes
        return {
            "eligible_count": candidates,
            "eligible_bytes": candidate_bytes,
            "removed_count": removed,
            "removed_bytes": removed_bytes,
        }


def _validate_utf8_file(path: Path) -> None:
    decoder = codecs.getincrementaldecoder("utf-8-sig")()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            decoder.decode(chunk)
    decoder.decode(b"", final=True)


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


def fetch_uniprot(
    ec_number: str,
    cache: HttpCache,
    *,
    refresh: bool = False,
    include_sequence: bool = True,
    reviewed_only: bool = False,
    taxon_ids: list[int] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if taxon_ids and any(taxon_id <= 0 for taxon_id in taxon_ids):
        raise ValueError("Taxonomy IDs must be positive integers")
    fields = "accession,id,protein_name,gene_names,organism_name,organism_id,ec,sequence,length,reviewed"
    if not include_sequence:
        fields = fields.removesuffix(",sequence,length,reviewed") + ",length,reviewed"
    query = f"ec:{ec_number}"
    if reviewed_only:
        query += " AND reviewed:true"
    if taxon_ids:
        query += " AND (" + " OR ".join(f"taxonomy_id:{taxon_id}" for taxon_id in sorted(set(taxon_ids))) + ")"
    params = urllib.parse.urlencode({"query": query, "fields": fields, "format": "tsv"})
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


@contextmanager
def iter_uniprot(
    ec_number: str,
    cache: HttpCache,
    *,
    refresh: bool = False,
    include_sequence: bool = True,
    reviewed_only: bool = False,
    taxon_ids: list[int] | None = None,
    sort_cache: dict[str, tuple[list[Path], dict[str, Any]]] | None = None,
    sort_directory: Path | None = None,
    release_cached_runs: bool = False,
) -> Iterator[tuple[Iterator[dict[str, Any]], dict[str, Any]]]:
    """Yield UniProt proteins sorted by accession using bounded in-memory runs."""
    if taxon_ids and any(taxon_id <= 0 for taxon_id in taxon_ids):
        raise ValueError("Taxonomy IDs must be positive integers")
    fields = "accession,id,protein_name,gene_names,organism_name,organism_id,ec,sequence,length,reviewed"
    if not include_sequence:
        fields = fields.removesuffix(",sequence,length,reviewed") + ",length,reviewed"
    query = f"ec:{ec_number}"
    if reviewed_only:
        query += " AND reviewed:true"
    if taxon_ids:
        query += " AND (" + " OR ".join(f"taxonomy_id:{taxon_id}" for taxon_id in sorted(set(taxon_ids))) + ")"
    params = urllib.parse.urlencode({"query": query, "fields": fields, "format": "tsv"})
    url = f"{UNIPROT_URL}?{params}"

    if sort_cache is not None:
        if sort_directory is None:
            raise ValueError("sort_directory is required when sort_cache is provided")
        cached = sort_cache.get(url)
        if cached is None:
            directory = sort_directory / hashlib.sha256(url.encode("utf-8")).hexdigest()
            directory.mkdir(parents=True, exist_ok=True)
            run_paths, metadata = _build_uniprot_sort_runs(
                url, cache, directory, refresh=refresh
            )
            while len(run_paths) > UNIPROT_MERGE_FAN_IN:
                run_paths = _merge_uniprot_runs(run_paths, directory)
            cached = (run_paths, metadata)
            sort_cache[url] = cached
        run_paths, metadata = cached
        try:
            with ExitStack() as stack:
                streams = [stack.enter_context(path.open("rb")) for path in run_paths]
                yield heapq.merge(
                    *(_read_uniprot_run(stream) for stream in streams),
                    key=lambda record: record["accession"],
                ), metadata
        finally:
            if release_cached_runs:
                sort_cache.pop(url, None)
                shutil.rmtree(sort_directory / hashlib.sha256(url.encode("utf-8")).hexdigest(), ignore_errors=True)
        return

    with tempfile.TemporaryDirectory(prefix="enzymeinfocollector-sort-") as directory:
        run_paths, metadata = _build_uniprot_sort_runs(url, cache, Path(directory), refresh=refresh)
        while len(run_paths) > UNIPROT_MERGE_FAN_IN:
            run_paths = _merge_uniprot_runs(run_paths, Path(directory))
        with ExitStack() as stack:
            streams = [stack.enter_context(path.open("rb")) for path in run_paths]
            sorted_records = heapq.merge(
                *(_read_uniprot_run(stream) for stream in streams),
                key=lambda record: record["accession"],
            )
            yield sorted_records, metadata


def _build_uniprot_sort_runs(
    url: str, cache: HttpCache, directory: Path, *, refresh: bool
) -> tuple[list[Path], dict[str, Any]]:
    if hasattr(cache, "open_text"):
        with cache.open_text(url, ttl_seconds=24 * 60 * 60, refresh=refresh) as (stream, metadata):  # type: ignore[attr-defined]
            run_paths = _build_uniprot_runs(stream, directory)
    else:
        response = cache.get(url, ttl_seconds=24 * 60 * 60, refresh=refresh)
        metadata = response.metadata
        run_paths = _build_uniprot_runs(io.StringIO(response.text), directory)
    return run_paths, metadata


def _build_uniprot_runs(stream: TextIO, directory: Path) -> list[Path]:
    reader = csv.DictReader(stream, delimiter="\t")
    if not reader.fieldnames or not any(name in reader.fieldnames for name in ("Entry", "accession")):
        raise DataSourceError("UniProt response did not contain the expected TSV header")
    run_paths: list[Path] = []
    chunk: list[dict[str, Any]] = []
    for row in reader:
        accession = _field(row, "Entry", "accession")
        if not accession:
            continue
        chunk.append({
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
        if len(chunk) == UNIPROT_SORT_CHUNK_SIZE:
            run_paths.append(_write_uniprot_run(directory, len(run_paths), chunk))
            chunk = []
    if chunk:
        run_paths.append(_write_uniprot_run(directory, len(run_paths), chunk))
    return run_paths


def _write_uniprot_run(directory: Path, index: int, records: list[dict[str, Any]]) -> Path:
    path = directory / f"{index}.pickle"
    records.sort(key=lambda item: item["accession"])
    with path.open("wb") as stream:
        for record in records:
            pickle.dump(record, stream, protocol=pickle.HIGHEST_PROTOCOL)
    return path


def _read_uniprot_run(stream: Any) -> Iterator[dict[str, Any]]:
    while True:
        try:
            yield pickle.load(stream)
        except EOFError:
            return


def _merge_uniprot_runs(paths: list[Path], directory: Path) -> list[Path]:
    merged_paths = []
    for group_index in range(0, len(paths), UNIPROT_MERGE_FAN_IN):
        group = paths[group_index:group_index + UNIPROT_MERGE_FAN_IN]
        target = directory / f"merge-{len(merged_paths)}-{group[0].stem}.pickle"
        with ExitStack() as stack:
            streams = [stack.enter_context(path.open("rb")) for path in group]
            merged = heapq.merge(
                *(_read_uniprot_run(stream) for stream in streams),
                key=lambda record: record["accession"],
            )
            with target.open("wb") as output:
                for record in merged:
                    pickle.dump(record, output, protocol=pickle.HIGHEST_PROTOCOL)
        merged_paths.append(target)
        for path in group:
            path.unlink()
    return merged_paths


def fetch_rhea(ec_number: str, cache: HttpCache, *, refresh: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params = urllib.parse.urlencode({
        "query": f"ec:{ec_number}",
        "columns": "rhea-id,equation,chebi,chebi-id,ec",
        "format": "tsv",
        "limit": RHEA_RESULT_LIMIT,
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
    if len(records) >= RHEA_RESULT_LIMIT:
        raise DataSourceError(f"Rhea reached its {RHEA_RESULT_LIMIT:,}-result limit; refusing a potentially incomplete result")
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
