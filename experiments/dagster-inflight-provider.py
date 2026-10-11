"""One opt-in provider Update interrupted after processing, through real Dagster workers.

Use an already built remote,test-util CLI and the existing qualification credential
references. The fixed new operation runs only in the retained synthetic namespace.
No automatic retry, remote cleanup, namespace creation, or production data is used.
"""
import datetime
import hashlib
from http.client import HTTPConnection
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import traceback

REPO = Path(__file__).resolve().parents[1]
QUALIFICATION = 'docspec-rs-qualification-0e38b494-34a4-47e4-a96e-f8ae27822f00'
PRIOR = REPO / 'docs/reviews/r2-engine-update-resumed-provider-2026-10-11.json'
OPERATION = 'dagster-inflight-update'
PREFIX = f's3://spicy-regs/__r2_data_catalog/docspec-qualification/{QUALIFICATION}/engine-update'
PAUSE_NAMES = ('DOCSPEC_TEST_PAUSE_BEFORE', 'DOCSPEC_TEST_PAUSE_MARKER', 'DOCSPEC_TEST_RESUME_MARKER')


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def http(port, path, body=None, timeout=30, limit=32 << 20):
    connection = HTTPConnection('127.0.0.1', port, timeout=timeout)
    try:
        connection.request('GET' if body is None else 'POST', path, body,
                           {} if body is None else {'content-type': 'application/json'})
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError('native HTTP request did not succeed')
        raw = response.read(limit + 1)
        if len(raw) > limit:
            raise RuntimeError('native HTTP response exceeds fixture bound')
        return {name.lower(): value for name, value in response.getheaders()}, raw
    finally:
        connection.close()


def main():
    if not __debug__ or len(sys.argv) != 2:
        raise ValueError('normal Python execution and one fresh absolute evidence directory are required')
    evidence = Path(sys.argv[1])
    if not evidence.is_absolute() or evidence.exists():
        raise ValueError('evidence directory must be fresh and absolute')
    # Import prerequisites before starting a host or writing any remote object.
    import pyarrow as pa
    import pyarrow.parquet as pq
    sys.path[:0] = [str(REPO / 'adapters/dagster/src'), str(REPO / 'adapters/dagster/tests')]
    from test_rust_integration import RustIntegrationTests
    import dagster

    prior = json.loads(PRIOR.read_text())
    assert prior.get('passed') is True and prior.get('qualification') == QUALIFICATION
    events = [json.loads(line) for line in PRIOR.with_suffix('.jsonl').read_text().splitlines()
              if line.startswith('{')]
    old_manifest = next(event['manifest'] for event in events if event.get('phase') == 'updates_passed')
    assert old_manifest['path'].startswith(PREFIX + '/')
    assert os.environ.get('DOCSPEC_QUALIFICATION_ID') == QUALIFICATION
    assert os.environ.get('DOCSPEC_S3_BUCKET') == 'spicy-regs'
    names = {'uri': 'DOCSPEC_CATALOG_URI', 'warehouse': 'DOCSPEC_CATALOG_WAREHOUSE',
             's3.endpoint': 'DOCSPEC_S3_ENDPOINT', 'token': 'DOCSPEC_CATALOG_TOKEN',
             's3.access-key-id': 'DOCSPEC_S3_ACCESS_KEY_ID',
             's3.secret-access-key': 'DOCSPEC_S3_SECRET_ACCESS_KEY'}
    if any(not os.environ.get(name) for name in names.values()):
        raise ValueError('existing provider environment is incomplete')
    binary = Path(os.environ['DOCSPEC_BINARY']).resolve(strict=True)
    assert binary.is_file() and os.access(binary, os.X_OK)
    evidence.mkdir(mode=0o700)
    config = evidence / 'remote.json'
    config.write_text(json.dumps({'owned_prefix': PREFIX,
        'properties': {'s3.region': 'auto', 's3.path-style-access': 'true'}, 'secret_env': names}, indent=2) + '\n')
    limits = evidence / 'limits.json'
    limits.write_text(json.dumps({'compute_threads': 2, 'memory_bytes': 512 << 20,
        'scratch_bytes': 16 << 20, 'batch_rows': 8, 'max_batch_bytes': 1 << 20,
        'max_metadata_bytes': 4 << 20}) + '\n')
    report_path = evidence / 'receipt.json'
    report = {'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'runtime_deadline_seconds': 300, 'qualification': QUALIFICATION, 'operation': OPERATION,
        'prior_receipt_sha256': digest(PRIOR), 'executable_sha256': digest(binary),
        'runner_sha256': digest(Path(__file__)),
        'protocol_sha256': digest(Path(__file__).with_name('dagster-inflight-provider-protocol.md')),
        'python': sys.version,
        'pyarrow': pa.__version__, 'dagster': dagster.__version__,
        'worker_sha256': digest(REPO / 'adapters/dagster/tests/native_worker.py'),
        'helpers_sha256': digest(REPO / 'adapters/dagster/tests/test_rust_integration.py'),
        'scope': 'One new source-only Update; real Dagster worker and Rust host killed after durable processing; replacement Python worker and fresh local Rust host directory recover exact provider completion. Same computer, independent OS processes; no power-loss, native-commit uncertainty, throughput, remote deletion or restore claim.',
        'passed': False, 'stage': 'starting_host', 'remote_cleanup': False}
    case = RustIntegrationTests('test_killed_python_worker_and_native_host_resume_retained_processing')
    environment = os.environ.copy()
    for name in PAUSE_NAMES:
        environment.pop(name, None)
    # Python workers submit only to loopback and need no provider credentials.
    python_environment = {name: value for name, value in environment.items() if not name.startswith('DOCSPEC_')}
    python_environment['PYTHONPATH'] = os.pathsep.join([str(REPO / 'adapters/dagster/src'),
        str(REPO / 'adapters/dagster/tests'), python_environment.get('PYTHONPATH', '')])
    paused = evidence / 'native-paused'
    resume = evidence / 'native-resume'
    hosts, logs, workers = [], [], []
    failure = None

    def stage(name):
        report['stage'] = name
        report_path.write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps({'stage': name, 'dataset': QUALIFICATION, 'operation': OPERATION}), flush=True)

    def host(name, pause=False):
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        path = evidence / f'{name}.log'
        output = path.open('xb')
        logs.append(output)
        env = environment.copy()
        if pause:
            env.update(DOCSPEC_TEST_PAUSE_BEFORE='validation', DOCSPEC_TEST_PAUSE_MARKER=str(paused),
                       DOCSPEC_TEST_RESUME_MARKER=str(resume))
        process = subprocess.Popen([str(binary), '--workspace', str(evidence / name),
            '--limits', str(limits), '--remote', str(config), 'serve', '--bind', f'127.0.0.1:{port}'],
            stdout=output, stderr=output, env=env)
        hosts.append((process, path))
        case._wait_ready(process, port, path)
        return process, port, path

    def records(port, timeout=30):
        return json.loads(http(port, f'/datasets/{QUALIFICATION}', timeout=timeout)[1])

    def plan(port, payload, timeout=30):
        return json.loads(http(port, '/updates/plan', payload, timeout=timeout)[1])

    def read_exact(port, release, changed):
        headers, raw = http(port, f'/datasets/{QUALIFICATION}/versions/{release["id"]}/arrow/source', limit=1 << 20)
        assert headers['docspec-version'] == release['id']
        assert headers['docspec-manifest-blake3'] == release['manifest']['digest']
        stream = pa.ipc.open_stream(raw)
        expected_fields = [('text', pa.string(), True, b'1'), ('counter', pa.int64(), False, b'2'),
                           ('private_canary', pa.string(), False, b'3')]
        assert len(stream.schema) == len(expected_fields)
        for field, (name, datatype, nullable, field_id) in zip(stream.schema, expected_fields):
            assert (field.name, field.type, field.nullable) == (name, datatype, nullable)
            assert field.metadata[b'PARQUET:field_id'] == field_id
        rows = []
        for batch in stream:
            assert len(rows) + batch.num_rows <= 5
            rows.extend(batch.to_pylist())
        def ordered(values):
            return sorted(values, key=canonical)
        value = 'after host loss' if changed else 'last'
        expected = [{'text': text, 'counter': counter, 'private_canary': 'synthetic-private-canary'}
                    for counter, text in [(1, value), (1, value), (1, None), (3, ''), (4, 'keep')]]
        assert ordered(rows) == ordered(expected)
        return hashlib.sha256(raw).hexdigest()

    def deadline(_signum, _frame):
        raise TimeoutError('provider unfinished-worker qualification deadline reached')
    signal.signal(signal.SIGALRM, deadline)
    signal.signal(signal.SIGTERM, deadline)
    signal.alarm(300)
    try:
        first, port, first_log = host('host-before', pause=True)
        stage('retained_preflight')
        before = records(port)
        old = before['result/update-two']
        assert before['authority']['release'] == old and old['manifest'] == old_manifest
        assert old['dataset']['dataset'] == QUALIFICATION and old['verified'] is True
        assert old['dataset']['definition'] == 'source-update-v1'
        assert old['dataset']['computed'] == {} and old['dataset']['related'] is None and old['dataset'].get('evidence') is None
        assert old['dataset']['relationship'] == {'kind': 'independent'}
        assert set(old['dataset']['groups']) == {'source'}
        for family in ('operation', 'request', 'readback', 'result'):
            assert f'{family}/{OPERATION}' not in before, 'fixed qualification operation already exists; reconcile retained attempt separately'
        report['old_manifest'] = old_manifest
        report['baseline_records_sha256'] = hashlib.sha256(canonical(before).encode()).hexdigest()
        source = evidence / 'producer.parquet'
        schema = pa.schema([pa.field('text', pa.string(), nullable=True), pa.field('counter', pa.int64(), nullable=False),
                            pa.field('private_canary', pa.string(), nullable=False)])
        pq.write_table(pa.Table.from_pydict({'text': ['after host loss', 'after host loss', None],
            'counter': [1, 1, 1], 'private_canary': ['synthetic-private-canary'] * 3}, schema=schema), source)
        assert source.stat().st_size < 64 << 10
        with (evidence / 'hash.json').open('xb') as output, (evidence / 'hash.log').open('xb') as errors:
            hashed = subprocess.run([str(binary), 'hash', str(source)], stdout=output, stderr=errors, timeout=30)
        assert hashed.returncode == 0 and (evidence / 'hash.json').stat().st_size < 4096
        request = {'dataset': QUALIFICATION, 'operation': OPERATION, 'definition': 'source-update-v1',
                   'base': 'update-two', 'keys': ['counter'],
                   'replacements': {'path': str(source), 'digest': json.loads((evidence / 'hash.json').read_text())['digest']},
                   'deletions': None}
        payload = canonical(request)
        request_path = evidence / 'worker-config.json'
        request_path.write_text(json.dumps({'ops': {'docspec_operation': {'config': {
            'route': '/updates/run', 'request_json': payload}}}}) + '\n')
        worker_report = evidence / 'killed-report.json'
        worker_output = (evidence / 'killed-worker.log').open('xb')
        logs.append(worker_output)
        stage('dagster_processing')
        worker = subprocess.Popen([sys.executable, str(REPO / 'adapters/dagster/tests/native_worker.py'),
            str(request_path), f'http://127.0.0.1:{port}', str(worker_report), '240'],
            stdout=worker_output, stderr=worker_output, env=python_environment)
        workers.append(worker)
        wait_until = time.monotonic() + 180
        while not paused.exists():
            assert worker.poll() is None and first.poll() is None
            if time.monotonic() >= wait_until:
                raise TimeoutError('processing did not reach the retained validation boundary')
            time.sleep(0.05)
        stage('capturing_retained_processing')
        # The existing native pause has a 60-second safety limit. Finish capture
        # and the intentional kill promptly; each request shares this 20-second bound.
        capture_until = time.monotonic() + 20
        captured = records(port, timeout=max(1, capture_until - time.monotonic()))
        prepared = plan(port, payload, timeout=max(1, capture_until - time.monotonic()))
        assert time.monotonic() < capture_until
        origin = prepared['reusable_phase_origins']['processing']
        processing = captured[origin['key']]
        assert origin['revision'] > 0 and processing['operation'] == OPERATION and processing['phase'] == 'processing'
        assert processing['metrics']['rows_removed'] == 3 and processing['metrics']['rows_added'] == 3
        assert processing['changes']['key_field_ids'] == [2]
        assert processing['changes']['base_manifest'] == old_manifest
        assert captured['authority']['release'] == old
        assert f'readback/{OPERATION}' not in captured and f'result/{OPERATION}' not in captured
        assert 'validation' in prepared['required_phases'] and 'processing' in prepared['reusable_phases']
        waiting = json.loads(worker_report.read_text())
        assert waiting.get('submission') == 'waiting' and waiting.get('started') is True
        assert waiting['pid'] == worker.pid and worker.poll() is None and first.poll() is None
        assert time.monotonic() - waiting['submitted_at'] < 220
        assert not resume.exists() and time.monotonic() < capture_until
        worker.kill(); worker.wait(timeout=10)
        assert worker.returncode == -signal.SIGKILL and json.loads(worker_report.read_text()) == waiting
        first.kill(); first.wait(timeout=10)
        assert first.returncode == -signal.SIGKILL
        # Only this fixture's small external producer is removed; provider copies
        # and every old/new native object remain retained.
        assert source.parent == evidence and source.name == 'producer.parquet' and not source.is_symlink()
        source.unlink()
        (evidence / 'processing.json').write_text(json.dumps({'origin': origin, 'phase': processing}, indent=2) + '\n')
        report.update(processing_origin=origin, processing_native_pin=processing['table'],
                      killed_worker_pid=worker.pid, killed_host_pid=first.pid)
        stage('fresh_host_recovery')
        second, port, second_log = host('host-after')
        assert second.pid != first.pid and not (evidence / 'host-after').samefile(evidence / 'host-before')
        assert plan(port, payload)['reusable_phase_origins']['processing'] == origin
        recovered = records(port)
        assert recovered[origin['key']] == processing and recovered['authority']['release'] == old
        # Reuse the same maintained native Dagster worker helper. The changed
        # timeout bounds transport waiting; it does not add retries or new identity.
        replacement = case._worker(evidence, request_path, f'http://127.0.0.1:{port}', 'replacement',
                                   timeout_seconds=240, environment=python_environment)
        assert replacement['success'] and replacement['pid'] != worker.pid
        assert replacement['request_sha256'] == waiting['request_sha256']
        assert replacement['summary'] == {'route': '/updates/run', 'dataset': QUALIFICATION, 'operation': OPERATION,
                                          'outcome': 'completed', 'result_id': OPERATION}
        final = records(port)
        selected = final[f'result/{OPERATION}']
        assert final['authority']['release'] == selected and final['result/update-two'] == old
        assert final[origin['key']] == processing and selected['dataset']['groups']['source'] == processing['table']
        assert selected['dataset']['parent_manifest'] == old_manifest
        assert selected['manifest']['path'].startswith(PREFIX + '/') and selected['verified'] is True
        assert json.loads(request_path.read_text())['ops']['docspec_operation']['config']['request_json'] == payload
        stage('cold_exact_readback')
        report['old_arrow_sha256'] = read_exact(port, old, changed=False)
        report['new_arrow_sha256'] = read_exact(port, selected, changed=True)
        assert first_log.stat().st_size < 2 << 20 and second_log.stat().st_size < 2 << 20
        def work(path):
            return [line.removeprefix('DOCSPEC_TEST_WORK ') for line in path.read_text().splitlines()
                    if line.startswith('DOCSPEC_TEST_WORK ')]
        initial_work, resumed_work = work(first_log), work(second_log)
        assert initial_work.count('processing') == 1 and 'validation' not in initial_work and 'readback' not in initial_work
        assert 'admission' not in resumed_work and 'processing' not in resumed_work
        assert resumed_work.count('validation') == 1 and resumed_work.count('readback') == 1
        report.update(stage='completed', new_manifest=selected['manifest'], resumed_work=resumed_work,
                      first_work=initial_work, fresh_host_pid=second.pid, replacement_worker_pid=replacement['pid'],
                      processing_retained_after_restart=True, original_input_absent=not source.exists(),
                      remote_request_counts_measured=False, remote_whole_prefix_bytes_measured=False)
    except BaseException as error:
        failure = error
        report['error_type'] = type(error).__name__
        report['error_locations'] = [{'file': Path(frame.filename).name, 'line': frame.lineno,
                                      'function': frame.name}
                                     for frame in traceback.extract_tb(error.__traceback__)]
    finally:
        signal.alarm(0)
        # Cleanup owns only processes started above. Keep failed evidence and all
        # remote objects; never discover/kill another worker or delete a prefix.
        resume.touch(exist_ok=True)
        for worker in workers:
            if worker.poll() is None:
                worker.kill(); worker.wait(timeout=10)
        for process, _ in hosts:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait(timeout=10)
                    report['forced_cleanup'] = True
                    failure = failure or RuntimeError('host cleanup exceeded bound')
        if hosts and report.get('stage') == 'completed' and hosts[-1][0].returncode != 0:
            failure = failure or RuntimeError('replacement host exited unsuccessfully')
        for output in logs:
            output.close()
        report['passed'] = failure is None and report['stage'] == 'completed'
        report['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        report_path.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'stage': report['stage'], 'receipt': str(report_path)}), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
