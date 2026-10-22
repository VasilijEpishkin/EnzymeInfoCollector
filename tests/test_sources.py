from __future__ import annotations

import io
import hashlib
import json
import multiprocessing
import os
import threading
import tempfile
import time
import unittest
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from enzymeinfocollector import sources
from enzymeinfocollector.sources import (
    CachedResponse,
    DataSourceError,
    HttpCache,
    _cache_lock,
    fetch_reaction_smiles,
    fetch_reaction_directions,
    fetch_rhea,
    fetch_uniprot,
    iter_uniprot,
    find_enzyme_entries,
    parse_enzyme_dat,
    validate_ec_number,
)


class FakeCache:
    def __init__(self, response: str):
        self.response = response
        self.urls: list[str] = []

    def get(self, url: str, **_kwargs: object) -> CachedResponse:
        self.urls.append(url)
        return CachedResponse(self.response, {"url": url, "fetched_at": "2026-10-22T00:00:00Z", "cached": False, "stale": False})


class FakeHttpResponse:
    def __init__(self, body: bytes, headers: dict[str, str] | None = None):
        self.body = io.BytesIO(body)
        self.headers = headers or {}

    def read(self, size: int = -1) -> bytes:
        return self.body.read(size)

    def __enter__(self) -> "FakeHttpResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class InterruptedHttpResponse(FakeHttpResponse):
    def read(self, size: int = -1) -> bytes:
        raise urllib.error.URLError("connection dropped")


class ResetHttpResponse(FakeHttpResponse):
    def __init__(self, body: bytes):
        super().__init__(body)
        self.first_read = True

    def read(self, size: int = -1) -> bytes:
        if self.first_read:
            self.first_read = False
            return self.body.read(4)
        raise ConnectionResetError("connection reset")


def _cache_lock_process_worker(
    directory: str,
    holder_entered: object,
    contender_attempted: object,
    contender_entered: object,
    release_holder: object,
    active: object,
    maximum: object,
    counter_lock: object,
    holder: bool,
) -> None:
    if not holder:
        holder_entered.wait(timeout=5)  # type: ignore[attr-defined]
        contender_attempted.set()  # type: ignore[attr-defined]
    with _cache_lock(Path(directory), "shared-url"):
        with counter_lock:  # type: ignore[attr-defined]
            active.value += 1  # type: ignore[attr-defined]
            maximum.value = max(maximum.value, active.value)  # type: ignore[attr-defined]
        if holder:
            holder_entered.set()  # type: ignore[attr-defined]
            release_holder.wait(timeout=5)  # type: ignore[attr-defined]
        else:
            contender_entered.set()  # type: ignore[attr-defined]
        with counter_lock:  # type: ignore[attr-defined]
            active.value -= 1  # type: ignore[attr-defined]


def _cache_refresh_process_worker(directory: str, key: str, url: str, ready: object, release: object) -> None:
    root = Path(directory)
    with _cache_lock(root, key):
        ready.set()  # type: ignore[attr-defined]
        release.wait(timeout=10)  # type: ignore[attr-defined]
        fetched_at = datetime.now(timezone.utc).isoformat()
        (root / f"{key}.json").write_text(json.dumps({"url": url, "fetched_at": fetched_at}), encoding="utf-8")


class SourceTests(unittest.TestCase):
    def test_parse_enzyme_dat_records_and_uniprot_refs(self) -> None:
        data = """ID   1.1.1.1
DE   Alcohol dehydrogenase.
AN   Alcohol:NAD+ oxidoreductase.
CA   An alcohol + NAD(+) = an aldehyde + NADH.
DR   P00330, ADH1_YEAST;
//
ID   3.5.1.n3
DE   Example enzyme.
//
"""
        entries = parse_enzyme_dat(data)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["enzyme_name"], "Alcohol dehydrogenase")
        self.assertEqual(entries[0]["alternative_names"], ["Alcohol:NAD+ oxidoreductase"])
        self.assertEqual(entries[0]["uniprot_accessions"], ["P00330"])
        self.assertEqual(entries[1]["ec_number"], "3.5.1.n3")

    def test_parse_transferred_entries_without_treating_them_as_active(self) -> None:
        entries = parse_enzyme_dat("""ID   1.1.1.5
DE   Transferred entry: 1.1.1.303 and 1.1.1.304.
//
""")
        self.assertEqual(entries[0]["status"], "transferred")
        self.assertEqual(entries[0]["transferred_to"], ["1.1.1.303", "1.1.1.304"])

    def test_validate_ec_number_accepts_preliminary_and_rejects_partial(self) -> None:
        self.assertEqual(validate_ec_number("EC 1.1.1.1"), "1.1.1.1")
        self.assertEqual(validate_ec_number("3.5.1.n3"), "3.5.1.n3")
        with self.assertRaises(ValueError):
            validate_ec_number("1.1.-.-")

    def test_exact_name_precedes_broader_substring_matches(self) -> None:
        entries = parse_enzyme_dat("""ID   1.1.1.1
DE   Alcohol dehydrogenase.
//
ID   1.1.1.2
DE   Alcohol dehydrogenase (NADP+).
//
""")
        self.assertEqual([row["ec_number"] for row in find_enzyme_entries(entries, " alcohol  dehydrogenase ")], ["1.1.1.1"])
        self.assertEqual([row["ec_number"] for row in find_enzyme_entries(entries, "dehydrogenase")], ["1.1.1.1", "1.1.1.2"])

    def test_parse_uniprot_tsv(self) -> None:
        cache = FakeCache("""Entry\tEntry Name\tProtein names\tGene Names\tOrganism\tOrganism (ID)\tEC number\tLength\tReviewed\tSequence
P00330\tADH1_YEAST\tAlcohol dehydrogenase 1\tADH1 ADH1B\tSaccharomyces cerevisiae\t4932\t1.1.1.1\t348\treviewed\tMSTAGKVIKCK
""")
        proteins, source = fetch_uniprot("1.1.1.1", cache)  # type: ignore[arg-type]
        self.assertEqual(proteins[0]["accession"], "P00330")
        self.assertEqual(proteins[0]["length"], 348)
        self.assertEqual(proteins[0]["gene_names"], ["ADH1", "ADH1B"])
        self.assertTrue(proteins[0]["reviewed"])
        self.assertEqual(source["url"], cache.urls[0])
        self.assertIn("uniprotkb/stream", cache.urls[0])

    def test_parse_rhea_and_directional_reaction_smiles(self) -> None:
        cache = FakeCache("""Reaction identifier\tEquation\tChEBI name\tChEBI identifier\tEC number
RHEA:10001\tA = B\tA; B\tCHEBI:1; CHEBI:2\tEC 1.1.1.1
""")
        reactions, _ = fetch_rhea("1.1.1.1", cache)  # type: ignore[arg-type]
        self.assertEqual(reactions[0]["rhea_id"], "10001")
        self.assertEqual(reactions[0]["chebi_ids"], ["1", "2"])
        smiles_cache = FakeCache("RHEA:10001\tC>>O\n")
        smiles, _ = fetch_reaction_smiles(smiles_cache)  # type: ignore[arg-type]
        self.assertEqual(smiles["10001"], "C>>O")
        directions_cache = FakeCache("RHEA_ID_MASTER\tRHEA_ID_LR\tRHEA_ID_RL\tRHEA_ID_BI\n10000\t10001\t10002\t10003\n")
        directions, _ = fetch_reaction_directions(directions_cache)  # type: ignore[arg-type]
        self.assertEqual(directions["10000"]["left_to_right"], "10001")

    def test_rhea_result_limit_fails_instead_of_silently_returning_partial_data(self) -> None:
        response = "Reaction identifier\tEquation\tChEBI name\tChEBI identifier\tEC number\n" + "".join(
            f"RHEA:{index}\tA = B\tA; B\tCHEBI:1; CHEBI:2\tEC 1.1.1.1\n"
            for index in range(10_000)
        )
        with self.assertRaisesRegex(DataSourceError, "limit"):
            fetch_rhea("1.1.1.1", FakeCache(response))  # type: ignore[arg-type]

    def test_http_cache_retries_and_reuses_fresh_response(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=1)
            error = urllib.error.HTTPError("https://example.org/data", 503, "busy", {"Retry-After": "0"}, None)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", side_effect=[error, FakeHttpResponse(b"release 1")]) as request:
                with patch("enzymeinfocollector.sources.time.sleep"):
                    first = cache.get("https://example.org/data", ttl_seconds=3600)
                second = cache.get("https://example.org/data", ttl_seconds=3600)
            self.assertEqual(first.text, "release 1")
            self.assertFalse(first.metadata["cached"])
            self.assertTrue(second.metadata["cached"])
            self.assertEqual(request.call_count, 2)

    def test_http_cache_does_not_retry_local_cache_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=2)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"release 1")) as request:
                with patch("enzymeinfocollector.sources._atomic_write", side_effect=OSError("disk full")):
                    with self.assertRaisesRegex(OSError, "disk full"):
                        cache.get("https://example.org/data", ttl_seconds=3600)
            self.assertEqual(request.call_count, 1)

    def test_http_cache_refreshes_when_valid_json_metadata_has_wrong_shape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            url = "https://example.org/data"
            key = hashlib.sha256(url.encode("utf-8")).hexdigest()
            (Path(directory) / f"{key}.body").write_bytes(b"old")
            (Path(directory) / f"{key}.json").write_text("[]", encoding="utf-8")
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"fresh")):
                response = cache.get(url, ttl_seconds=3600)
            self.assertEqual(response.text, "fresh")
            self.assertEqual(json.loads((Path(directory) / f"{key}.json").read_text())["url"], url)

    def test_http_cache_ignores_validators_when_cached_timestamp_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            url = "https://example.org/data"
            key = hashlib.sha256(url.encode("utf-8")).hexdigest()
            (Path(directory) / f"{key}.body").write_bytes(b"old")
            (Path(directory) / f"{key}.json").write_text(
                '{"fetched_at": "not-a-date", "etag": "old-validator"}', encoding="utf-8"
            )
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"fresh")) as request:
                response = cache.get(url, ttl_seconds=3600)
            self.assertEqual(response.text, "fresh")
            self.assertNotIn("If-None-Match", request.call_args.args[0].header_items())

    def test_http_cache_ignores_validators_when_cached_body_is_not_utf8(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            url = "https://example.org/data"
            key = hashlib.sha256(url.encode("utf-8")).hexdigest()
            (Path(directory) / f"{key}.body").write_bytes(bytes([0xFF]))
            (Path(directory) / f"{key}.json").write_text(json.dumps({
                "url": url,
                "fetched_at": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
                "etag": '"old-validator"',
            }), encoding="utf-8")
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"fresh")) as request:
                response = cache.get(url, ttl_seconds=0)
            self.assertEqual(response.text, "fresh")
            self.assertNotIn("If-None-Match", request.call_args.args[0].header_items())

    def test_http_cache_opens_large_responses_from_disk_and_reuses_them(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"release 1")) as request:
                with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, metadata):
                    self.assertEqual(stream.read(), "release 1")
                    self.assertFalse(metadata["cached"])
                with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, metadata):
                    self.assertEqual(stream.read(), "release 1")
                    self.assertTrue(metadata["cached"])
            self.assertEqual(request.call_count, 1)

    def test_http_cache_stream_uses_stale_response_after_transient_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"release 1")):
                with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, _):
                    self.assertEqual(stream.read(), "release 1")
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", side_effect=urllib.error.URLError("offline")):
                with cache.open_text("https://example.org/data", ttl_seconds=0) as (stream, metadata):
                    self.assertEqual(stream.read(), "release 1")
                    self.assertTrue(metadata["stale"])

    def test_http_cache_stream_refreshes_when_fetched_at_has_wrong_type(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            url = "https://example.org/data"
            key = hashlib.sha256(url.encode("utf-8")).hexdigest()
            (Path(directory) / f"{key}.body").write_bytes(b"old")
            (Path(directory) / f"{key}.json").write_text('{"fetched_at": 42}', encoding="utf-8")
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"fresh")):
                with cache.open_text(url, ttl_seconds=3600) as (stream, metadata):
                    self.assertEqual(stream.read(), "fresh")
                    self.assertFalse(metadata["cached"])

    def test_http_cache_refreshes_when_timestamp_has_no_timezone(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            url = "https://example.org/data"
            key = hashlib.sha256(url.encode("utf-8")).hexdigest()
            (Path(directory) / f"{key}.body").write_bytes(b"old")
            (Path(directory) / f"{key}.json").write_text(
                '{"url": "https://example.org/data", "fetched_at": "2026-10-21T12:00:00"}', encoding="utf-8"
            )
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"fresh")):
                self.assertEqual(cache.get(url, ttl_seconds=3600).text, "fresh")

    def test_http_cache_inspect_reports_orphans_and_invalid_metadata_without_urls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory))
            valid_key = hashlib.sha256(b"valid").hexdigest()
            orphan_key = hashlib.sha256(b"orphan").hexdigest()
            invalid_key = hashlib.sha256(b"invalid").hexdigest()
            root = Path(directory)
            (root / f"{valid_key}.body").write_bytes(b"123")
            (root / f"{valid_key}.json").write_text(json.dumps({
                "url": "https://example.org/data?token=private", "fetched_at": "2026-10-22T00:00:00Z",
            }), encoding="utf-8")
            (root / f"{orphan_key}.body").write_bytes(b"orphan")
            (root / f"{invalid_key}.body").write_bytes(b"invalid")
            (root / f"{invalid_key}.json").write_text("[]", encoding="utf-8")
            report = cache.inspect()
        self.assertEqual(report["entry_count"], 3)
        self.assertEqual(report["body_bytes"], 16)
        self.assertEqual(report["lock_count"], 3)
        self.assertEqual(report["orphan_count"], 1)
        self.assertEqual(report["invalid_count"], 1)
        self.assertNotIn("private", repr(report))

    def test_http_cache_prune_defaults_to_dry_run_and_preserves_unknown_or_recent_entries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory))
            root = Path(directory)
            now = datetime.now(timezone.utc)
            keys = {name: hashlib.sha256(name.encode()).hexdigest() for name in ("old", "recent", "invalid", "orphan")}
            for name, age in (("old", 40), ("recent", 5)):
                key = keys[name]
                (root / f"{key}.body").write_bytes(name.encode())
                (root / f"{key}.json").write_text(json.dumps({
                    "url": f"https://example.org/{name}", "fetched_at": (now - timedelta(days=age)).isoformat(),
                }), encoding="utf-8")
            (root / f"{keys['invalid']}.body").write_bytes(b"invalid")
            (root / f"{keys['invalid']}.json").write_text('{"fetched_at":"unknown"}', encoding="utf-8")
            (root / f"{keys['orphan']}.body").write_bytes(b"orphan")
            dry_run = cache.prune()
            old_bytes = (root / f"{keys['old']}.body").stat().st_size + (root / f"{keys['old']}.json").stat().st_size
            self.assertEqual(dry_run, {"eligible_count": 1, "eligible_bytes": old_bytes, "removed_count": 0, "removed_bytes": 0})
            self.assertTrue((root / f"{keys['old']}.body").exists())
            applied = cache.prune(apply=True)
            self.assertEqual(applied["removed_count"], 1)
            self.assertFalse((root / f"{keys['old']}.body").exists())
            self.assertFalse((root / f"{keys['old']}.json").exists())
            for name in ("recent", "invalid", "orphan"):
                self.assertTrue((root / f"{keys[name]}.body").exists())
            self.assertTrue((root / f"{keys['old']}.lock").exists())
            self.assertEqual(cache.inspect()["lock_count"], 4)

    def test_http_cache_prune_waits_for_other_process_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory))
            root = Path(directory)
            url = "https://example.org/data"
            key = hashlib.sha256(url.encode()).hexdigest()
            (root / f"{key}.body").write_bytes(b"keep")
            (root / f"{key}.json").write_text(json.dumps({
                "url": url, "fetched_at": (datetime.now(timezone.utc) - timedelta(days=40)).isoformat(),
            }), encoding="utf-8")
            context = multiprocessing.get_context("spawn")
            ready, release = context.Event(), context.Event()
            process = context.Process(target=_cache_refresh_process_worker, args=(directory, key, url, ready, release))
            process.start()
            self.assertTrue(ready.wait(timeout=5))
            prune_started, prune_finished = threading.Event(), threading.Event()

            def prune() -> dict[str, int]:
                prune_started.set()
                try:
                    return cache.prune(apply=True)
                finally:
                    prune_finished.set()

            try:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    pruning = pool.submit(prune)
                    self.assertTrue(prune_started.wait(timeout=2))
                    self.assertFalse(prune_finished.wait(timeout=0.1))
                    release.set()
                    process.join(timeout=5)
                    self.assertEqual(process.exitcode, 0)
                    result = pruning.result(timeout=5)
            finally:
                release.set()
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=2)
            self.assertEqual(result["removed_count"], 0)
            self.assertTrue((root / f"{key}.body").exists())

    def test_http_cache_prune_rejects_retention_below_stale_fallback_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "at least 30 days"):
                HttpCache(Path(directory)).prune(older_than_days=29)

    def test_http_cache_stream_rejects_oversized_body_without_publishing_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            with patch("enzymeinfocollector.sources.MAX_RESPONSE_BYTES", 3):
                with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"four")):
                    with self.assertRaisesRegex(DataSourceError, "exceeds"):
                        with cache.open_text("https://example.org/data", ttl_seconds=3600):
                            self.fail("oversized response must not be opened")
            self.assertEqual(list(Path(directory).glob("*.body")), [])
            self.assertEqual(list(Path(directory).glob("*.json")), [])

    def test_http_cache_stream_retries_interrupted_download_without_publishing_partial_body(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=1)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", side_effect=[InterruptedHttpResponse(b"partial"), FakeHttpResponse(b"complete")]) as request:
                with patch("enzymeinfocollector.sources.time.sleep"):
                    with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, _):
                        self.assertEqual(stream.read(), "complete")
            self.assertEqual(request.call_count, 2)
            body = next(Path(directory).glob("*.body"))
            self.assertEqual(body.read_bytes(), b"complete")

    def test_http_cache_stream_retries_connection_reset_during_read(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=1)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", side_effect=[ResetHttpResponse(b"partial"), FakeHttpResponse(b"complete")]) as request:
                with patch("enzymeinfocollector.sources.time.sleep"):
                    with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, _):
                        self.assertEqual(stream.read(), "complete")
            self.assertEqual(request.call_count, 2)

    def test_http_cache_stream_drops_old_metadata_if_new_metadata_cannot_be_published(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"old", {"ETag": '"old"'})):
                with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, _):
                    self.assertEqual(stream.read(), "old")
            new_response = FakeHttpResponse(b"new", {"ETag": '"new"'})
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=new_response):
                with patch("enzymeinfocollector.sources._atomic_write", side_effect=OSError("disk full")):
                    with self.assertRaisesRegex(OSError, "disk full"):
                        with cache.open_text("https://example.org/data", ttl_seconds=0, refresh=True):
                            self.fail("cache publication should fail")
            self.assertEqual(list(Path(directory).glob("*.json")), [])
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"recovered")) as request:
                with cache.open_text("https://example.org/data", ttl_seconds=3600) as (stream, _):
                    self.assertEqual(stream.read(), "recovered")
            self.assertNotIn("If-None-Match", request.call_args.args[0].header_items())

    def test_http_cache_serializes_concurrent_refreshes_for_the_same_url(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            barrier = threading.Barrier(3)
            first_read_started = threading.Event()
            release_first_read = threading.Event()
            state = {"active": 0, "maximum": 0, "calls": 0}
            state_lock = threading.Lock()

            def response_for_request(*_args: object, **_kwargs: object) -> FakeHttpResponse:
                with state_lock:
                    state["active"] += 1
                    state["maximum"] = max(state["maximum"], state["active"])
                    state["calls"] += 1
                    sequence = state["calls"]

                class TrackedResponse(FakeHttpResponse):
                    def __init__(self) -> None:
                        super().__init__(f"body-{sequence}".encode(), {"ETag": f'"{sequence}"'})
                        self.released = False

                    def read(self, size: int = -1) -> bytes:
                        if sequence == 1 and not self.released:
                            first_read_started.set()
                            release_first_read.wait(timeout=5)
                        chunk = super().read(size)
                        if not chunk and not self.released:
                            self.released = True
                            with state_lock:
                                state["active"] -= 1
                        return chunk

                return TrackedResponse()

            def refresh() -> str:
                barrier.wait()
                with cache.open_text("https://example.org/concurrent", ttl_seconds=0, refresh=True) as (stream, _):
                    return stream.read()

            with patch("enzymeinfocollector.sources.urllib.request.urlopen", side_effect=response_for_request):
                with ThreadPoolExecutor(max_workers=2) as workers:
                    futures = [workers.submit(refresh) for _ in range(2)]
                    barrier.wait()
                    self.assertTrue(first_read_started.wait(timeout=2))
                    time.sleep(0.05)
                    self.assertEqual(state["calls"], 1)
                    release_first_read.set()
                    bodies = [future.result() for future in futures]

            key = hashlib.sha256(b"https://example.org/concurrent").hexdigest()
            body = (Path(directory) / f"{key}.body").read_text()
            metadata = json.loads((Path(directory) / f"{key}.json").read_text())
            self.assertEqual(state["calls"], 2)
            self.assertEqual(state["maximum"], 1)
            self.assertEqual(set(bodies), {"body-1", "body-2"})
            self.assertEqual(metadata["etag"], f'"{body.removeprefix("body-")}"')

    def test_http_cache_lock_serializes_separate_processes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            context = multiprocessing.get_context("spawn")
            holder_entered = context.Event()
            contender_attempted = context.Event()
            contender_entered = context.Event()
            release_holder = context.Event()
            active = context.Value("i", 0)
            maximum = context.Value("i", 0)
            counter_lock = context.Lock()
            processes = [
                context.Process(
                    target=_cache_lock_process_worker,
                    args=(directory, holder_entered, contender_attempted, contender_entered, release_holder, active, maximum, counter_lock, True),
                ),
                context.Process(
                    target=_cache_lock_process_worker,
                    args=(directory, holder_entered, contender_attempted, contender_entered, release_holder, active, maximum, counter_lock, False),
                ),
            ]
            contender_was_blocked = False
            try:
                for process in processes:
                    process.start()
                self.assertTrue(holder_entered.wait(timeout=2))
                self.assertTrue(contender_attempted.wait(timeout=2))
                contender_was_blocked = not contender_entered.wait(timeout=0.1)
            finally:
                release_holder.set()
                for process in processes:
                    if process.pid is not None:
                        process.join(timeout=5)
                        if process.is_alive():
                            process.terminate()
                            process.join()
            for process in processes:
                self.assertEqual(process.exitcode, 0)
            self.assertTrue(contender_was_blocked)
            self.assertEqual(maximum.value, 1)
            if os.name == "nt":
                lock_file = next(Path(directory).glob("*.lock"))
                self.assertGreaterEqual(lock_file.stat().st_size, 1)

    def test_uniprot_merge_bounds_open_runs_and_keeps_accession_order(self) -> None:
        cache = FakeCache("Entry\tEntry Name\tProtein names\tGene Names\tOrganism\tOrganism (ID)\tEC number\tLength\tReviewed\tSequence\n" + "".join(
            f"P{number:05d}\tname\tprotein\tgene\torg\t1\t1.1.1.1\t1\treviewed\tM\n"
            for number in (8, 2, 6, 1, 7, 3, 5, 4, 9)
        ))
        with patch("enzymeinfocollector.sources.UNIPROT_SORT_CHUNK_SIZE", 1), patch("enzymeinfocollector.sources.UNIPROT_MERGE_FAN_IN", 2):
            with iter_uniprot("1.1.1.1", cache) as (proteins, _):  # type: ignore[arg-type]
                self.assertEqual([protein["accession"] for protein in proteins], [f"P{number:05d}" for number in range(1, 10)])

    def test_reused_uniprot_runs_are_removed_after_final_replay(self) -> None:
        header = "Entry\tEntry Name\tProtein names\tGene Names\tOrganism\tOrganism (ID)\tEC number\tLength\tReviewed\tSequence\n"
        cache = FakeCache(header + "P00002\tn2\tp2\tg2\torg\t1\t1.1.1.1\t2\treviewed\tMM\nP00001\tn1\tp1\tg1\torg\t1\t1.1.1.1\t1\treviewed\tM\n")
        run_cache: dict[str, tuple[list[Path], dict[str, object]]] = {}
        with tempfile.TemporaryDirectory() as directory, patch("enzymeinfocollector.sources.UNIPROT_SORT_CHUNK_SIZE", 1), patch(
            "enzymeinfocollector.sources._build_uniprot_runs",
            wraps=sources._build_uniprot_runs,
        ) as build_runs:
            sort_root = Path(directory)
            for release in (False, True):
                with iter_uniprot(
                    "1.1.1.1",
                    cache,  # type: ignore[arg-type]
                    sort_cache=run_cache,  # type: ignore[arg-type]
                    sort_directory=sort_root,
                    release_cached_runs=release,
                ) as (proteins, _):
                    self.assertEqual([protein["accession"] for protein in proteins], ["P00001", "P00002"])
            self.assertEqual(build_runs.call_count, 1)
            self.assertEqual(run_cache, {})
            self.assertEqual(list(sort_root.iterdir()), [])

    def test_reused_uniprot_runs_are_removed_when_final_replay_fails(self) -> None:
        cache = FakeCache("Entry\tEntry Name\tProtein names\tGene Names\tOrganism\tOrganism (ID)\tEC number\tLength\tReviewed\tSequence\nP00001\tn\tp\tg\torg\t1\t1.1.1.1\t1\treviewed\tM\n")
        run_cache: dict[str, tuple[list[Path], dict[str, object]]] = {}
        with tempfile.TemporaryDirectory() as directory:
            sort_root = Path(directory)
            with iter_uniprot("1.1.1.1", cache, sort_cache=run_cache, sort_directory=sort_root) as (proteins, _):  # type: ignore[arg-type]
                list(proteins)
            with self.assertRaisesRegex(RuntimeError, "callback failed"):
                with iter_uniprot(
                    "1.1.1.1",
                    cache,  # type: ignore[arg-type]
                    sort_cache=run_cache,  # type: ignore[arg-type]
                    sort_directory=sort_root,
                    release_cached_runs=True,
                ) as (proteins, _):
                    next(proteins)
                    raise RuntimeError("callback failed")
            self.assertEqual(run_cache, {})
            self.assertEqual(list(sort_root.iterdir()), [])

    def test_uniprot_can_omit_sequences_and_filter_by_review_and_taxon(self) -> None:
        cache = FakeCache("""Entry\tEntry Name\tProtein names\tGene Names\tOrganism\tOrganism (ID)\tEC number\tLength\tReviewed
P00330\tADH1_YEAST\tAlcohol dehydrogenase 1\tADH1\tSaccharomyces cerevisiae\t4932\t1.1.1.1\t348\treviewed
""")
        proteins, _ = fetch_uniprot(
            "1.1.1.1",
            cache,  # type: ignore[arg-type]
            include_sequence=False,
            reviewed_only=True,
            taxon_ids=[9606, 10090, 9606],
        )
        params = urllib.parse.parse_qs(urllib.parse.urlparse(cache.urls[0]).query)
        self.assertNotIn("sequence", params["fields"][0].split(","))
        self.assertEqual(params["query"], ["ec:1.1.1.1 AND reviewed:true AND (taxonomy_id:9606 OR taxonomy_id:10090)"])
        self.assertEqual(proteins[0]["sequence"], "")

    def test_cache_uses_recent_snapshot_only_for_transient_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = HttpCache(Path(directory), timeout=1, retries=0)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", return_value=FakeHttpResponse(b"release 1")):
                cache.get("https://example.org/data", ttl_seconds=3600)
            with patch("enzymeinfocollector.sources.urllib.request.urlopen", side_effect=urllib.error.URLError("offline")):
                stale = cache.get("https://example.org/data", ttl_seconds=0)
            self.assertEqual(stale.text, "release 1")
            self.assertTrue(stale.metadata["stale"])


if __name__ == "__main__":
    unittest.main()
