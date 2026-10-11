"""Opt-in fresh empty-table detach/register probe using maintained PyIceberg."""
import datetime
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import sys
import time
from urllib.parse import urlsplit
import uuid


def main():
    if len(sys.argv) != 3:
        raise ValueError('usage: probe.py FRESH_ABSOLUTE_EVIDENCE_DIRECTORY FRESH_CANONICAL_UUID')
    evidence = Path(sys.argv[1])
    suffix = str(uuid.UUID(sys.argv[2]))
    if suffix != sys.argv[2] or not evidence.is_absolute() or evidence.exists():
        raise ValueError('fresh absolute evidence directory and canonical UUID required')
    if importlib.metadata.version('pyiceberg') != '0.12.0':
        raise ValueError('this probe is frozen against installed PyIceberg 0.12.0')
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
    from pyiceberg.schema import Schema
    from pyiceberg.table.update import AssertTableUUID, SetPropertiesUpdate
    from pyiceberg.types import NestedField, LongType, StringType
    from tenacity import stop_after_attempt

    config = {key: os.environ[name] for key, name in {
        'uri': 'DOCSPEC_CATALOG_URI', 'warehouse': 'DOCSPEC_CATALOG_WAREHOUSE', 'token': 'DOCSPEC_CATALOG_TOKEN',
    }.items()}
    endpoint = urlsplit(config['uri'])
    if endpoint.scheme != 'https' or not endpoint.hostname or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment or any(not value for value in config.values()):
        raise ValueError('explicit existing HTTPS catalog configuration required')
    namespace = 'docspec_detach_probe_' + suffix.replace('-', '_')
    restored_namespace = namespace + '_restored'
    original = (namespace, 'candidate')
    restored = (restored_namespace, 'candidate')
    location = 's3://spicy-regs/__r2_data_catalog/docspec-qualification/docspec-rs-qualification-' + suffix + '/detach-probe'
    evidence.mkdir(mode=0o700)
    report_path = evidence / 'receipt.json'
    report = {'passed': False, 'stage': 'starting', 'source_table': '.'.join(original), 'restored_table': '.'.join(restored),
              'table_location': location, 'pyiceberg': '0.12.0', 'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'steps': [],
              'step_deadline_seconds': 25, 'total_deadline_seconds': 240,
              'scope': 'One new empty synthetic table; detach its registration without purge, register exact metadata under a fresh name, reject an old-identifier commit. No data files, S3 DELETE, original-evidence mutation, cleanup, or full restore claim.'}
    def save():
        report_path.write_text(json.dumps(report, indent=2) + '\n')
    save()
    started = time.monotonic()
    def deadline(_signum, _frame):
        raise TimeoutError('probe step deadline reached')
    signal.signal(signal.SIGALRM, deadline)
    signal.signal(signal.SIGTERM, deadline)
    def step(name, fn):
        report['stage'] = name
        event = {'stage': name, 'acknowledged': False}
        report['steps'].append(event)
        save()
        remaining = 240 - (time.monotonic() - started)
        if remaining <= 0:
            raise TimeoutError('probe total deadline reached')
        signal.setitimer(signal.ITIMER_REAL, min(25, remaining))
        try:
            result = fn()
            event['acknowledged'] = True
            return result
        except BaseException as error:
            event['error_type'] = type(error).__name__
            # Maintained PyIceberg chains HTTPError. Record status only, never text/config.
            response = getattr(error.__cause__, 'response', None)
            status = getattr(response, 'status_code', None)
            if isinstance(status, int):
                event['http_status'] = status
            raise
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            save()
    def once(catalog, method, *args, **kwargs):
        # Public Tenacity per-call configuration disables the native auth retry;
        # all REST construction/auth/serialization remain in maintained PyIceberg.
        return getattr(RestCatalog, method).retry_with(stop=stop_after_attempt(1))(catalog, *args, **kwargs)
    def native_identity(table):
        native = table.metadata.model_dump(mode='json', by_alias=True)
        encoded = json.dumps(native, sort_keys=True, separators=(',', ':')).encode()
        if len(encoded) > 1 << 20 or table.metadata.snapshots or table.metadata.current_snapshot_id not in (None, -1):
            raise ValueError('expected bounded empty metadata')
        if table.metadata.location != location or not table.metadata_location.startswith(location + '/metadata/'):
            raise ValueError('native metadata escaped fresh synthetic root')
        if any(table.metadata.properties.get(key) != value for key, value in {
            'gc.enabled': 'false', 'write.metadata.delete-after-commit.enabled': 'false', 'commit.retry.num-retries': '0',
        }.items()):
            raise ValueError('provider did not retain non-destructive table properties')
        if [(field.field_id, field.name) for field in table.schema().fields] != [(1, 'counter'), (2, 'text')]:
            raise ValueError('native empty table schema differs')
        return {'metadata_location': table.metadata_location, 'uuid': str(table.metadata.table_uuid),
                'location': location, 'native_metadata_sha256': hashlib.sha256(encoded).hexdigest(), 'native_metadata_json_bytes': len(encoded)}
    clients = []
    failure = None
    try:
        catalog = step('connect', lambda: RestCatalog('docspec-detach-probe', **config))
        clients.append(catalog)
        for name in (namespace, restored_namespace):
            try:
                step('namespace_absence_' + ('source' if name == namespace else 'restored'), lambda name=name: once(catalog, 'load_namespace_properties', (name,)))
            except NoSuchNamespaceError:
                pass
            else:
                raise ValueError('namespace must be definitively absent')
        for name in (namespace, restored_namespace):
            step('create_namespace_' + ('source' if name == namespace else 'restored'), lambda name=name: once(catalog, 'create_namespace', (name,)))
        schema = Schema(NestedField(1, 'counter', LongType(), required=False), NestedField(2, 'text', StringType(), required=False))
        old = step('create_empty_candidate', lambda: once(catalog, 'create_table', original, schema, location=location, properties={
            'format-version': '2', 'gc.enabled': 'false', 'write.metadata.delete-after-commit.enabled': 'false', 'commit.retry.num-retries': '0',
        }))
        before = native_identity(old)
        report['source_identity'] = before
        save()
        step('detach_without_purge', lambda: once(catalog, 'drop_table', original, purge_requested=False))
        try:
            step('old_registration_absent', lambda: once(catalog, 'load_table', original))
        except NoSuchTableError:
            pass
        else:
            raise ValueError('detached table remains registered')
        target = step('register_exact_metadata_once', lambda: once(catalog, 'register_table', restored, old.metadata_location, overwrite=False))
        report['registered_identity'] = native_identity(target)
        if report['registered_identity'] != before:
            raise ValueError('registration changed captured native meaning')
        try:
            step('stale_old_identifier_commit', lambda: once(catalog, 'commit_table', old,
                 requirements=(AssertTableUUID(uuid=old.metadata.table_uuid),),
                 updates=(SetPropertiesUpdate(updates={'docspec.probe.stale': 'must-not-commit'}),)))
        except Exception as error:
            response = getattr(error.__cause__, 'response', None)
            if getattr(response, 'status_code', None) != 404:
                raise
            report['stale_commit_http'] = 404
        else:
            raise ValueError('stale old identifier unexpectedly committed')
        fresh = step('fresh_client', lambda: RestCatalog('docspec-detach-probe-fresh', **config))
        clients.append(fresh)
        observed = step('fresh_restored_readback', lambda: once(fresh, 'load_table', restored))
        report['fresh_identity'] = native_identity(observed)
        if report['fresh_identity'] != before:
            raise ValueError('new registration changed after stale commit')
        report.update(passed=True, stage='completed', old_identifier_reused=False, object_deletions=0, payload_rows=0)
    except BaseException as error:
        failure = error
        report['error_type'] = type(error).__name__
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        for catalog in clients:
            catalog.close()
        report['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save()
    print(json.dumps({'passed': report['passed'], 'stage': report['stage'], 'receipt': str(report_path)}), flush=True)
    return 0 if report['passed'] and failure is None else 1


if __name__ == '__main__':
    raise SystemExit(main())
