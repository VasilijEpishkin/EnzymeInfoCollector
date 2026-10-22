"""Repeat the offline eager/callback/streamed JSON comparison on Linux."""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COUNT = 16
PER_EC = 2000
SEQUENCE = 'ACDEFGHIKLMNPQRSTVWY' * 6
FIXED_META = {'fetched_at': '2026-10-23T00:00:00Z', 'cached': False, 'stale': False}


def build_fixture(root: Path) -> None:
    ecs = [f'1.1.1.{n}' for n in range(1, COUNT + 1)]
    with (root / 'enzyme.dat').open('w', encoding='utf-8') as f:
        for i, ec in enumerate(ecs, 1):
            f.write(f'ID   {ec}\nDE   Synthetic enzyme {i}.\n//\n')
        f.write('ID   7.9.9.1\nDE   Transferred entry: 1.1.1.1 and 9.9.9.9.\n//\n')
        f.write('ID   7.9.9.2\nDE   Transferred entry: 1.1.1.2.\n//\n')
    header = 'Entry\tEntry Name\tProtein names\tGene Names\tOrganism\tOrganism (ID)\tEC number\tLength\tReviewed\tSequence\n'
    for ec_index, ec in enumerate(ecs, 1):
        with (root / f'uniprot-{ec}.tsv').open('w', encoding='utf-8', newline='') as f:
            f.write(header)
            for i in range(PER_EC - 1, -1, -1):
                accession = f'P{ec_index:02d}{i:06d}'
                f.write(f'{accession}\tENTRY_{accession}\tSynthetic protein {ec_index}\tSYN{ec_index}\tTest species\t9606\t{ec}\t120\treviewed\t{SEQUENCE}\n')
        rid = str(10000 + ec_index)
        with (root / f'rhea-{ec}.tsv').open('w', encoding='utf-8', newline='') as f:
            f.write('Reaction identifier\tEquation\tChEBI name\tChEBI identifier\tEC number\n')
            f.write(f'RHEA:{rid}\tA = B\tA; B\tCHEBI:1; CHEBI:2\tEC {ec}\n')
    with (root / 'rhea-reaction-smiles.tsv').open('w', encoding='utf-8') as smiles, (root / 'rhea-directions.tsv').open('w', encoding='utf-8') as directions:
        directions.write('RHEA_ID_MASTER\tRHEA_ID_LR\tRHEA_ID_RL\tRHEA_ID_BI\n')
        for i in range(1, COUNT + 1):
            master, lr, rl, bi = str(10000+i), str(20000+i), str(30000+i), str(40000+i)
            directions.write(f'{master}\t{lr}\t{rl}\t{bi}\n')
            smiles.write(f'{master}\tC>>O\n{lr}\tC>>O\n{rl}\tO>>C\n{bi}\tC<>O\n')
    (root / 'queries.csv').write_text('EC number\n7.9.9.1\n7.9.9.2\n' + ''.join(ec + '\n' for ec in ecs), encoding='utf-8')


class FixtureCache:
    def __init__(self, root: Path):
        from enzymeinfocollector.sources import ENZYME_URL, UNIPROT_URL, RHEA_URL, RHEA_SMILES_URL, RHEA_DIRECTIONS_URL
        self.root = root
        self.enzyme_url, self.uniprot_url, self.rhea_url = ENZYME_URL, UNIPROT_URL, RHEA_URL
        self.smiles_url, self.directions_url = RHEA_SMILES_URL, RHEA_DIRECTIONS_URL
        self.urls = []

    def _path(self, url):
        self.urls.append(url)
        if url == self.enzyme_url:
            return self.root / 'enzyme.dat'
        if url == self.smiles_url:
            return self.root / 'rhea-reaction-smiles.tsv'
        if url == self.directions_url:
            return self.root / 'rhea-directions.tsv'
        parsed = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qs(parsed.query).get('query', [''])[0]
        ec = query.removeprefix('ec:')
        if url.startswith(self.uniprot_url):
            return self.root / f'uniprot-{ec}.tsv'
        if url.startswith(self.rhea_url):
            return self.root / f'rhea-{ec}.tsv'
        raise AssertionError(f'unexpected fixture URL {url}')

    def get(self, url, **_kwargs):
        from enzymeinfocollector.sources import CachedResponse
        path = self._path(url)
        return CachedResponse(path.read_text(encoding='utf-8'), {**FIXED_META, 'url': url})

    @contextlib.contextmanager
    def open_text(self, url, **_kwargs):
        path = self._path(url)
        with path.open('r', encoding='utf-8-sig', newline='') as stream:
            yield stream, {**FIXED_META, 'url': url}


def current_rss_kib(pid: int) -> int:
    for line in Path(f'/proc/{pid}/status').read_text().splitlines():
        if line.startswith('VmRSS:'):
            return int(line.split()[1])
    return 0


def worker(mode: str, fixture_root: Path):
    from enzymeinfocollector.cli import main
    from enzymeinfocollector.collector import EnzymeCollector
    cache = FixtureCache(fixture_root)
    queries = [('ec', '7.9.9.1'), ('ec', '7.9.9.2'), *[('ec', f'1.1.1.{n}') for n in range(1, COUNT + 1)]]
    digest = hashlib.sha256()
    count = 0
    with tempfile.TemporaryDirectory(prefix='eic-bench-output-') as d:
        output = Path(d) / 'result.json'
        print(json.dumps({'ready': True, 'baseline_rss_kib': current_rss_kib(os.getpid())}), flush=True)
        sys.stdin.readline()
        t0 = time.perf_counter()
        if mode == 'callback':
            def callback(_result, _record, protein, _kind, _requested, _resolution):
                nonlocal count
                if protein is not None:
                    digest.update(protein['accession'].encode('ascii') + b'\n')
                    count += 1
            EnzymeCollector(cache).collect(queries, on_record=callback)
            output_size = 0
        else:
            from unittest.mock import patch
            args = ['--file', str(fixture_root / 'queries.csv'), '--output', str(output)]
            if mode == 'stream-json':
                args.append('--stream-json')
            with patch('enzymeinfocollector.cli.HttpCache', return_value=cache), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                if main(args) != 0:
                    raise AssertionError('CLI failed')
            output_size = output.stat().st_size
        elapsed = time.perf_counter() - t0
    print(json.dumps({'mode': mode, 'seconds': elapsed, 'output_bytes': output_size,
                      'protein_events': count, 'accession_sha256': digest.hexdigest()}), flush=True)


def expected_accessions(payload):
    out = []
    for result in payload['results']:
        for enzyme in result['enzymes']:
            if enzyme['status'] == 'transferred':
                for target in enzyme['resolution']['resolved_enzymes']:
                    out.extend(protein['accession'] for protein in target['proteins'])
            else:
                out.extend(protein['accession'] for protein in enzyme.get('proteins', []))
    return out


def parity(fixture_root: Path):
    from enzymeinfocollector.cli import main
    from enzymeinfocollector.collector import EnzymeCollector
    from unittest.mock import patch
    cache = FixtureCache(fixture_root)
    queries = [('ec', '7.9.9.1'), ('ec', '7.9.9.2'), *[('ec', f'1.1.1.{n}') for n in range(1, COUNT + 1)]]
    with tempfile.TemporaryDirectory(prefix='eic-bench-parity-') as d:
        root = Path(d)
        eager_path, stream_path = root/'eager.json', root/'stream.json'
        with patch('enzymeinfocollector.cli.HttpCache', return_value=cache), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            if main(['--file', str(fixture_root/'queries.csv'), '--output', str(eager_path)]) != 0:
                raise AssertionError('eager CLI failed')
        cache = FixtureCache(fixture_root)
        with patch('enzymeinfocollector.cli.HttpCache', return_value=cache), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            if main(['--file', str(fixture_root/'queries.csv'), '--output', str(stream_path), '--stream-json']) != 0:
                raise AssertionError('stream CLI failed')
        eager = json.loads(eager_path.read_text(encoding='utf-8'))
        streamed = json.loads(stream_path.read_text(encoding='utf-8'))
        eager['collected_at'] = streamed['collected_at']
        if eager != streamed:
            raise AssertionError('eager/streamed payloads differ')
        expected = expected_accessions(eager)
        callback_cache = FixtureCache(fixture_root)
        actual = []
        EnzymeCollector(callback_cache).collect(queries, on_record=lambda _r, _e, p, _k, _q, _res: actual.append(p['accession']) if p else None)
        expected_digest = hashlib.sha256(('\n'.join(expected) + '\n').encode('ascii')).hexdigest()
        actual_digest = hashlib.sha256(('\n'.join(actual) + '\n').encode('ascii')).hexdigest()
        if expected != actual or expected_digest != actual_digest:
            raise AssertionError('callback event sequence differs from eager protein sequence')
        if len(expected) != (COUNT + 2) * PER_EC:
            raise AssertionError(f'unexpected protein count {len(expected)}')
        statuses = [e['resolution']['status'] for r in eager['results'] for e in r['enzymes'] if 'resolution' in e]
        if statuses != ['partial', 'resolved']:
            raise AssertionError(f'unexpected transfer statuses {statuses}')
        if eager_path.stat().st_size != stream_path.stat().st_size:
            raise AssertionError('eager and streamed file byte sizes differ')
        print(json.dumps({'parity': 'passed', 'protein_count': len(expected), 'accession_sha256': expected_digest,
                          'json_bytes': eager_path.stat().st_size, 'resolution_statuses': statuses}))


def sampled_worker(mode: str, fixture_root: Path):
    process = subprocess.Popen([sys.executable, __file__, 'worker', mode, str(fixture_root)], cwd=ROOT,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    ready = json.loads(process.stdout.readline())
    if not ready.get('ready'):
        raise RuntimeError('worker did not become ready')
    baseline = ready['baseline_rss_kib']
    process.stdin.write('go\n'); process.stdin.flush()
    peak = baseline
    while process.poll() is None:
        try:
            peak = max(peak, current_rss_kib(process.pid))
        except (FileNotFoundError, ProcessLookupError):
            break
        time.sleep(0.003)
    stdout, stderr = process.communicate()
    if process.returncode:
        raise RuntimeError(stderr)
    result = json.loads(stdout.strip().splitlines()[-1])
    result.update({'baseline_rss_kib': baseline, 'sampled_peak_rss_kib': peak, 'peak_delta_rss_kib': max(0, peak-baseline)})
    return result


def orchestrate():
    if not Path('/proc/self/status').is_file():
        raise SystemExit('This benchmark samples process RSS from /proc and requires Linux.')
    with tempfile.TemporaryDirectory(prefix='eic-bench-fixture-') as directory:
        fixture_root = Path(directory)
        build_fixture(fixture_root)
        parity_proc = subprocess.run([sys.executable, __file__, 'parity', str(fixture_root)], cwd=ROOT, check=True, capture_output=True, text=True)
        parity_result = json.loads(parity_proc.stdout.strip().splitlines()[-1])
        modes = ['eager', 'callback', 'stream-json']
        orders = [modes, modes[1:] + modes[:1], modes[2:] + modes[:2], modes, modes[1:] + modes[:1]]
        results = []
        for order in orders:
            for mode in order:
                results.append(sampled_worker(mode, fixture_root))
        report = {'platform': sys.platform, 'python': sys.version.split()[0], 'fixture': {
                      'active_ecs': COUNT, 'proteins_per_ec': PER_EC, 'direct_active_proteins': COUNT*PER_EC,
                      'including_transfer_replay_proteins': (COUNT+2)*PER_EC, 'sequence_length': len(SEQUENCE),
                      'source_accessions_descending': True, 'fixture_disk_bytes': sum(p.stat().st_size for p in fixture_root.iterdir())},
                  'parity_preflight': parity_result, 'runs': results, 'summary': {}}
        for mode in modes:
            vals = [r for r in results if r['mode'] == mode]
            seconds = [r['seconds'] for r in vals]
            peaks = [r['sampled_peak_rss_kib'] for r in vals]
            deltas = [r['peak_delta_rss_kib'] for r in vals]
            report['summary'][mode] = {'seconds_median': statistics.median(seconds), 'seconds_min_max': [min(seconds), max(seconds)],
                                       'sampled_peak_rss_mib_median': statistics.median(peaks)/1024,
                                       'peak_delta_from_ready_mib_median': statistics.median(deltas)/1024,
                                       'sampled_peak_rss_mib_min_max': [min(peaks)/1024, max(peaks)/1024],
                                       'output_bytes': vals[0]['output_bytes'], 'protein_events': vals[0]['protein_events'],
                                       'accession_sha256': vals[0]['accession_sha256']}
        cb = report['summary']['callback']
        if cb['protein_events'] != parity_result['protein_count'] or cb['accession_sha256'] != parity_result['accession_sha256']:
            raise AssertionError('measured callback digest/count failed parity preflight')
        for mode in ('eager', 'stream-json'):
            if report['summary'][mode]['output_bytes'] != parity_result['json_bytes']:
                raise AssertionError(f'{mode} output size differs from parity output')
        print(json.dumps(report, indent=2))

if __name__ == '__main__':
    sys.path.insert(0, str(ROOT))
    if len(sys.argv) > 1 and sys.argv[1] == 'worker':
        worker(sys.argv[2], Path(sys.argv[3]))
    elif len(sys.argv) > 1 and sys.argv[1] == 'parity':
        parity(Path(sys.argv[2]))
    else:
        orchestrate()
