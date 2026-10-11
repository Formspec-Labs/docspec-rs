"""Recover the retained unfinished provider operation from the recorded failed probe.

This is an explicit follow-up, never a fresh retry of its expensive processing.
The first probe's pause observation timed out; cleanup killed the worker and host
after readback was retained but before selection. Preserve that narrower boundary.
"""
from http.client import HTTPConnection
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import traceback

REPO = Path(__file__).resolve().parents[1]
PRIOR = REPO / 'docs/reviews/dagster-inflight-provider-fixed-2026-10-11'
INSPECTED = REPO / 'docs/reviews/dagster-inflight-provider-inspect-2026-10-11/retained-records.json'
DATASET = 'docspec-rs-qualification-0e38b494-34a4-47e4-a96e-f8ae27822f00'
OPERATION = 'dagster-inflight-update'


def http(port, path, payload=None):
    connection = HTTPConnection('127.0.0.1', port, timeout=60)
    try:
        connection.request('GET' if payload is None else 'POST', path, payload,
                           {} if payload is None else {'content-type': 'application/json'})
        response = connection.getresponse()
        assert response.status == 200
        raw = response.read((32 << 20) + 1)
        assert len(raw) <= 32 << 20
        return dict(response.getheaders()), raw
    finally:
        connection.close()


def main():
    import pyarrow as pa
    sys.path[:0] = [str(REPO / 'adapters/dagster/src'), str(REPO / 'adapters/dagster/tests')]
    from test_rust_integration import RustIntegrationTests
    folder = Path(sys.argv[1])
    assert __debug__ and folder.is_absolute() and not folder.exists()
    assert os.environ['DOCSPEC_QUALIFICATION_ID'] == DATASET
    prior = json.loads((PRIOR / 'receipt.json').read_text())
    assert prior['operation'] == OPERATION and prior['forced_cleanup'] and not prior['passed']
    observed = json.loads(INSPECTED.read_text())
    assert observed['authority']['release']['id'] == 'update-two'
    assert f'result/{OPERATION}' not in observed and f'readback/{OPERATION}' in observed
    folder.mkdir()
    report = {'passed': False, 'stage': 'fresh_host', 'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'operation': OPERATION, 'prior_failure': str(PRIOR.relative_to(REPO)),
              'scope': 'Actual worker/host loss after retained readback and before publication; same computer, fresh local host directory; no throughput, power-loss or restore claim.'}
    with socket.socket() as reserve:
        reserve.bind(('127.0.0.1', 0)); port = reserve.getsockname()[1]
    helpers = RustIntegrationTests()
    with (folder / 'host.log').open('xb') as output:
        process = subprocess.Popen([os.environ['DOCSPEC_BINARY'], '--workspace', str(folder / 'host'),
            '--limits', str(PRIOR / 'limits.json'), '--remote', str(PRIOR / 'remote.json'),
            'serve', '--bind', f'127.0.0.1:{port}'], stdout=output, stderr=output)
        try:
            helpers._wait_ready(process, port, folder / 'host.log')
            before = json.loads(http(port, f'/datasets/{DATASET}')[1])
            assert before == observed
            payload = json.loads((PRIOR / 'worker-config.json').read_text())['ops']['docspec_operation']['config']['request_json']
            plan = json.loads(http(port, '/updates/plan', payload)[1])
            (folder / 'before-plan.json').write_text(json.dumps(plan, indent=2) + '\n')
            source = PRIOR / 'producer.parquet'
            assert source.is_file() and not source.is_symlink() and source.stat().st_size < 64 << 10
            assert json.loads(payload)['replacements']['path'] == str(source)
            source.unlink()
            environment = {key: value for key, value in os.environ.items() if not key.startswith('DOCSPEC_')}
            environment['PYTHONPATH'] = os.pathsep.join([str(REPO / 'adapters/dagster/src'), str(REPO / 'adapters/dagster/tests')])
            report['stage'] = 'same_operation_resume'
            result = helpers._worker(folder, PRIOR / 'worker-config.json', f'http://127.0.0.1:{port}',
                                     'replacement', timeout_seconds=240, environment=environment)
            assert result['success'] and result['summary']['result_id'] == OPERATION
            original_worker = json.loads((PRIOR / 'killed-report.json').read_text())
            assert original_worker['submission'] == 'waiting'
            assert result['request_sha256'] == original_worker['request_sha256'] and result['pid'] != original_worker['pid']
            after = json.loads(http(port, f'/datasets/{DATASET}')[1])
            selected = after[f'result/{OPERATION}']
            assert after['authority']['release'] == selected
            assert after[f'readback/{OPERATION}'] == before[f'readback/{OPERATION}']
            assert all(after[key] == value for key, value in before.items() if key.startswith('phase/') or key == 'result/update-two')
            assert selected['manifest'] == before[f'readback/{OPERATION}']['manifest']
            report['stage'] = 'exact_independent_readback'
            for version, text in [('update-two', 'last'), (OPERATION, 'after host loss')]:
                headers, raw = http(port, f'/datasets/{DATASET}/versions/{version}/arrow/source')
                assert headers['docspec-version'] == version
                assert headers['docspec-manifest-blake3'] == after[f'result/{version}']['manifest']['digest']
                assert len(raw) < 1 << 20
                table = pa.ipc.open_stream(raw).read_all()
                assert table.schema.names == ['text', 'counter', 'private_canary']
                for field, dtype, nullable, field_id in zip(table.schema, [pa.string(), pa.int64(), pa.string()], [True, False, False], [b'1', b'2', b'3']):
                    assert (field.type, field.nullable, field.metadata[b'PARQUET:field_id']) == (dtype, nullable, field_id)
                expected = [{'text': value, 'counter': counter, 'private_canary': 'synthetic-private-canary'}
                            for counter, value in [(1, text), (1, text), (1, None), (3, ''), (4, 'keep')]]
                assert sorted(map(lambda row: json.dumps(row, sort_keys=True), table.to_pylist())) == sorted(map(lambda row: json.dumps(row, sort_keys=True), expected))
                report[f'{version}_arrow_sha256'] = hashlib.sha256(raw).hexdigest()
            work = [line for line in (folder / 'host.log').read_text().splitlines() if line.startswith('DOCSPEC_TEST_WORK ')]
            assert not work and not source.exists()
            report.update(passed=True, stage='completed', repeated_phase_work=work,
                          retained_readback_unchanged=True, retained_phases_unchanged=True,
                          original_input_absent=True, result=selected, replacement_pid=result['pid'])
        except BaseException as error:
            report.update(error_type=type(error).__name__, error_locations=[{'file': Path(frame.filename).name, 'line': frame.lineno} for frame in traceback.extract_tb(error.__traceback__)])
        finally:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=10)
                report.update(passed=False, forced_cleanup=True)
            report['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            (folder / 'receipt.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({key: report.get(key) for key in ('passed', 'stage', 'error_type')}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
