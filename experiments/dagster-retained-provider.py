"""Opt-in completed provider replay through the existing real Dagster job/relay.

Run only with the root task's existing provider environment and an already built
remote,test-util CLI. This script never creates a new native operation identity.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys

REPO = Path(__file__).resolve().parents[1]
QUALIFICATION = 'docspec-rs-qualification-0e38b494-34a4-47e4-a96e-f8ae27822f00'
PRIOR = REPO / 'docs/reviews/r2-engine-update-resumed-provider-2026-10-11.json'
OPERATION = 'update-one'


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    if len(sys.argv) != 2:
        raise ValueError('one fresh absolute local evidence directory is required')
    evidence = Path(sys.argv[1])
    if not evidence.is_absolute() or evidence.exists():
        raise ValueError('evidence directory must be fresh and absolute')
    prior = json.loads(PRIOR.read_text())
    if prior.get('passed') is not True or prior.get('qualification') != QUALIFICATION:
        raise ValueError('retained provider continuation is not qualified')
    if os.environ.get('DOCSPEC_QUALIFICATION_ID') != QUALIFICATION:
        raise ValueError('exact retained qualification ID is required')
    if os.environ.get('DOCSPEC_S3_BUCKET') != 'spicy-regs':
        raise ValueError('exact retained qualification bucket is required')
    names = {
        'uri': 'DOCSPEC_CATALOG_URI',
        'warehouse': 'DOCSPEC_CATALOG_WAREHOUSE',
        's3.endpoint': 'DOCSPEC_S3_ENDPOINT',
        'token': 'DOCSPEC_CATALOG_TOKEN',
        's3.access-key-id': 'DOCSPEC_S3_ACCESS_KEY_ID',
        's3.secret-access-key': 'DOCSPEC_S3_SECRET_ACCESS_KEY',
    }
    if any(not os.environ.get(name) for name in names.values()):
        raise ValueError('existing provider environment is incomplete')
    binary = Path(os.environ['DOCSPEC_BINARY']).resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError('an already built remote,test-util CLI is required')
    sys.path[:0] = [str(REPO / 'adapters/dagster/src'), str(REPO / 'adapters/dagster/tests')]
    from docspec_dagster.definitions import DocSpecResource, docspec_job
    from test_rust_integration import RustIntegrationTests, lost_response_relay

    case = RustIntegrationTests('test_same_native_request_reconciles_without_more_retained_writes')
    prefix = f's3://spicy-regs/__r2_data_catalog/docspec-qualification/{QUALIFICATION}/engine-update'
    evidence.mkdir(mode=0o700)
    config_path = evidence / 'remote.json'
    config_path.write_text(json.dumps({
        'owned_prefix': prefix,
        'properties': {'s3.region': 'auto', 's3.path-style-access': 'true'},
        'secret_env': names,
    }, indent=2) + '\n')
    limits = evidence / 'limits.json'
    limits.write_text(json.dumps({
        'compute_threads': 1, 'memory_bytes': 256 << 20, 'scratch_bytes': 16 << 20,
        'batch_rows': 8, 'max_batch_bytes': 1 << 20, 'max_metadata_bytes': 4 << 20,
    }) + '\n')
    report_path = evidence / 'receipt.json'
    report = {
        'runtime_deadline_seconds': 180,
        'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'qualification': QUALIFICATION, 'operation': OPERATION,
        'prior_receipt': str(PRIOR), 'prior_receipt_sha256': digest(PRIOR),
        'executable': str(binary), 'executable_sha256': digest(binary),
        'runner_sha256': digest(Path(__file__)),
        'helpers_sha256': digest(REPO / 'adapters/dagster/tests/test_rust_integration.py'),
        'scope': 'Existing completed remote Update acknowledgment through fresh loopback Rust host and actual Dagster job; first completed reply lost, unchanged request retried. No new pipeline operation, payload validation, provider cleanup, or inflight recovery claim.',
        'passed': False, 'stage': 'starting_host',
    }
    report_path.write_text(json.dumps(report, indent=2) + '\n')
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    environment = os.environ.copy()
    for name in ('DOCSPEC_TEST_PAUSE_BEFORE', 'DOCSPEC_TEST_PAUSE_MARKER', 'DOCSPEC_TEST_RESUME_MARKER'):
        environment.pop(name, None)
    log_path = evidence / 'host.log'
    host = None
    failure = None
    # This Unix-only task runner uses one process deadline, not a new service timer.
    def deadline(_signum, _frame):
        raise TimeoutError('retained replay deadline reached')
    signal.signal(signal.SIGALRM, deadline)
    signal.signal(signal.SIGTERM, deadline)
    signal.alarm(180)
    with log_path.open('xb') as log:
        try:
            host = subprocess.Popen([
                str(binary), '--workspace', str(evidence / 'host'), '--limits', str(limits),
                '--remote', str(config_path), 'serve', '--bind', f'127.0.0.1:{port}',
            ], stdout=log, stderr=log, env=environment)
            case._wait_ready(host, port, log_path)
            report['stage'] = 'retained_preflight'
            state = lambda: case._get(port, f'/datasets/{QUALIFICATION}')
            baseline = state()
            stored = baseline[f'request/{OPERATION}']
            expected = baseline[f'result/{OPERATION}']
            latest = baseline['authority']['release']
            assert stored['kind'] == 'update', 'retained request must be Update'
            request = stored['spec']
            assert request['dataset'] == QUALIFICATION and request['operation'] == OPERATION
            assert request['base'] == 'initial' and request['definition'] == 'source-update-v1'
            assert expected['id'] == OPERATION and expected['verified'] is True
            assert expected['dataset']['dataset'] == QUALIFICATION
            assert expected['dataset']['request'] == stored
            assert expected['manifest']['path'].startswith(prefix + '/')
            assert latest['id'] == 'update-two', 'must preserve newer already selected result'
            assert latest['manifest']['path'].startswith(prefix + '/')
            payload = json.dumps(request, separators=(',', ':'))
            assert len(payload.encode()) <= 1 << 20
            # Retain only synthetic request metadata, not credential values.
            (evidence / 'request.json').write_text(payload + '\n')
            canonical = json.dumps(baseline, sort_keys=True, separators=(',', ':')).encode()
            assert len(canonical) <= 32 << 20
            report['baseline_records_sha256'] = hashlib.sha256(canonical).hexdigest()
            report['original_manifest'] = expected['manifest']
            report['latest_manifest'] = latest['manifest']
            report['stage'] = 'dagster_completed_replay'
            print(json.dumps({'phase': report['stage'], 'dataset': QUALIFICATION, 'operation': OPERATION}), flush=True)
            with lost_response_relay(port, state) as relay:
                result = docspec_job.execute_in_process(
                    run_config={'ops': {'docspec_operation': {'config': {
                        'route': '/updates/run', 'request_json': payload,
                    }}}},
                    resources={'docspec': DocSpecResource(
                        base_url=f'http://127.0.0.1:{relay.server_address[1]}',
                        uncertain_retries=1, retry_delay_seconds=0, timeout_seconds=30,
                    )},
                    raise_on_error=False,
                )
                assert result.success, 'actual Dagster replay did not complete'
                assert sum(event.event_type_value == 'STEP_UP_FOR_RETRY' for event in result.all_events) == 1
                assert result.output_for_node('docspec_operation') == {
                    'route': '/updates/run', 'dataset': QUALIFICATION, 'operation': OPERATION,
                    'outcome': 'completed', 'result_id': OPERATION,
                }
                assert relay.errors == [], 'relay did not obtain two real native completions'
                assert relay.requests == [('/updates/run', payload.encode())] * 2
                assert relay.completions == [expected, expected]
                assert relay.baseline == baseline, 'first acknowledgment changed retained records'
            assert state() == baseline, 'retry changed retained records or latest selection'
            assert log_path.stat().st_size <= 1 << 20, 'tiny fixture log exceeded limit'
            work = [line for line in log_path.read_text().splitlines() if line.startswith('DOCSPEC_TEST_WORK ')]
            assert not work, 'completed replay entered native material work'
            report.update(stage='completed', dagster_retries=1, native_phase_work_events=0,
                          observed_records_unchanged=True, remote_request_counts_measured=False,
                          remote_object_inventory_measured=False)
        except BaseException as error:
            # Do not render environment, native errors, or secret-bearing upstream data.
            failure = error
            report['error_type'] = type(error).__name__
        finally:
            signal.alarm(0)
            if host is not None and host.poll() is None:
                host.send_signal(signal.SIGINT)
                try:
                    host.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    host.kill()
                    host.wait(timeout=10)
                    report['forced_host_cleanup'] = True
                    failure = failure or RuntimeError('host did not stop gracefully')
            if host is not None:
                report['host_exit_code'] = host.returncode
                if host.returncode != 0:
                    failure = failure or RuntimeError('host exited unsuccessfully')
            report['passed'] = failure is None and report['stage'] == 'completed'
            report['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            report_path.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'stage': report['stage'], 'receipt': str(report_path)}), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
