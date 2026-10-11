"""Opt-in Basin registration capability probe; no object upload, update, or cleanup."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sys
from urllib.parse import quote, urlencode, urlsplit
import uuid
import httpx

SOURCE_NAMESPACE = 'candidate_09879fc9_eb50_47b8_b4c1_903329db55b3'
SOURCE_NAME = 'candidate'
SOURCE_UUID = '01a1289a-93cb-74fb-bc0d-2e80965a79d1'
LOCATION = 's3://spicy-regs/__r2_data_catalog/docspec-qualification/docspec-rs-qualification-09879fc9-eb50-47b8-b4c1-903329db55b3/candidate-probe'
METADATA = LOCATION + '/metadata/00000-f9e5ced5-800a-4d04-a633-e416a80c8210.metadata.json'
LIMIT = 4 << 20


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def identity(response):
    native = response['metadata']
    if response['metadata-location'] != METADATA or native['table-uuid'] != SOURCE_UUID or native['location'] != LOCATION:
        raise ValueError('source identity differs from retained qualified candidate')
    if native.get('snapshots') or native.get('current-snapshot-id') not in (None, -1):
        raise ValueError('probe requires the retained empty candidate')
    schema = next(s for s in native['schemas'] if s['schema-id'] == native['current-schema-id'])
    if [field['id'] for field in schema['fields']] != [7, 42]:
        raise ValueError('source field identities changed')
    return {'metadata_location': METADATA, 'table_uuid': SOURCE_UUID,
            'location': LOCATION, 'native_metadata_sha256': hashlib.sha256(canonical(native)).hexdigest(),
            'field_ids': [7, 42], 'snapshot_count': 0}


def main():
    if len(sys.argv) != 3:
        raise ValueError('usage: probe.py FRESH_ABSOLUTE_EVIDENCE_DIRECTORY FRESH_CANONICAL_UUID')
    evidence = Path(sys.argv[1])
    suffix = str(uuid.UUID(sys.argv[2]))
    if suffix != sys.argv[2] or not evidence.is_absolute() or evidence.exists():
        raise ValueError('fresh absolute evidence directory and canonical UUID required')
    endpoint = os.environ['DOCSPEC_CATALOG_URI'].rstrip('/')
    parsed = urlsplit(endpoint)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError('explicit HTTPS catalog URL required')
    warehouse = os.environ['DOCSPEC_CATALOG_WAREHOUSE']
    token = os.environ['DOCSPEC_CATALOG_TOKEN']
    if not warehouse or not token or '\n' in token or '\r' in token:
        raise ValueError('existing catalog environment incomplete')
    destination = 'docspec_restore_probe_' + suffix.replace('-', '_')
    evidence.mkdir(mode=0o700)
    report_path = evidence / 'receipt.json'
    report = {'passed': False, 'stage': 'starting', 'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'probe_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'source_table': SOURCE_NAMESPACE + '.' + SOURCE_NAME,
              'destination_table': destination + '.' + SOURCE_NAME,
              'source_metadata': METADATA, 'requests': [], 'deadline_seconds': 150,
              'scope': 'One registration of retained empty synthetic metadata while the original registration remains; no object I/O, append, retry, cleanup, or full restore claim.'}
    def save():
        report_path.write_text(json.dumps(report, indent=2) + '\n')
    save()
    def deadline(_signum, _frame):
        raise TimeoutError('probe deadline exceeded')
    signal.signal(signal.SIGALRM, deadline)
    signal.signal(signal.SIGTERM, deadline)
    signal.alarm(150)
    client = httpx.Client(follow_redirects=False, timeout=15)
    def request(method, path, body=None):
        request_record = {'method': method, 'stage': report['stage'], 'status': None}
        report['requests'].append(request_record)
        save()  # Record the attempted mutation before issuing it.
        headers = {'Authorization': 'Bearer ' + token, 'Accept': 'application/json', 'Accept-Encoding': 'identity'}
        if body is not None:
            headers['Content-Type'] = 'application/json'
        with client.stream(method, endpoint + path, content=None if body is None else canonical(body), headers=headers) as response:
            request_record['status'] = response.status_code
            data = bytearray()
            for part in response.iter_bytes(chunk_size=65536):
                if len(data) + len(part) > LIMIT:
                    raise ValueError('response exceeds bounded metadata allowance')
                data.extend(part)
            result = json.loads(data) if data else {}
        if request_record['status'] >= 400 and isinstance(result, dict):
            error = result.get('error', {})
            if isinstance(error, dict):
                kind = error.get('type')
                if isinstance(kind, str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', kind):
                    request_record['error_type'] = kind
                code = error.get('code')
                if isinstance(code, int):
                    request_record['error_code'] = code
        save()
        return request_record['status'], result
    failure = None
    try:
        report['stage'] = 'config'
        status, config = request('GET', '/v1/config?' + urlencode({'warehouse': warehouse}))
        if status != 200:
            raise ValueError('catalog config unavailable')
        settings = {**config.get('defaults', {}), 'warehouse': warehouse, **config.get('overrides', {})}
        prefix = settings.get('prefix', '')
        if not isinstance(prefix, str) or any(part in ('.', '..') for part in prefix.split('/')):
            raise ValueError('invalid catalog prefix')
        api = '/v1/' + (quote(prefix, safe='') + '/' if prefix else '')
        table_path = lambda namespace: api + 'namespaces/' + quote(namespace, safe='') + '/tables/' + SOURCE_NAME
        report['stage'] = 'source_guard'
        status, source = request('GET', table_path(SOURCE_NAMESPACE))
        if status != 200:
            raise ValueError('retained source registration unavailable')
        before = identity(source)
        report['source_before'] = before
        report['stage'] = 'destination_absence'
        status, _ = request('GET', api + 'namespaces/' + destination)
        if status != 404:
            raise ValueError('destination namespace is not definitively absent')
        report['stage'] = 'create_namespace'
        status, _ = request('POST', api + 'namespaces', {'namespace': [destination], 'properties': {}})
        if status not in (200, 201):
            raise ValueError('namespace creation did not acknowledge success; no retry')
        report['stage'] = 'register_once'
        status, _ = request('POST', api + 'namespaces/' + destination + '/register', {'name': SOURCE_NAME, 'metadata-location': METADATA})
        report['registration_status'] = status
        # These GETs use new HTTP connections, without an object reader or shared table cache.
        report['stage'] = 'reconcile_destination'
        destination_status, restored = request('GET', table_path(destination))
        report['destination_status'] = destination_status
        if destination_status == 200:
            report['destination_identity'] = identity(restored)
        elif destination_status != 404:
            raise ValueError('destination outcome remains unresolved')
        report['stage'] = 'source_unchanged'
        source_status, source_after = request('GET', table_path(SOURCE_NAMESPACE))
        if source_status != 200 or identity(source_after) != before:
            raise ValueError('source registration changed or is unavailable')
        report['source_unchanged'] = True
        report['passed'] = destination_status == 200 and report['destination_identity'] == before
        report['stage'] = 'registered_same_uuid' if report['passed'] else 'registration_rejected'
        if not report['passed'] and status not in (400, 409):
            report['stage'] = 'inconclusive_registration_failure'
    except BaseException as error:
        failure = error
        report['error_type'] = type(error).__name__  # Never print raw upstream messages or credentials.
    finally:
        signal.alarm(0)
        client.close()
        report['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save()
    print(json.dumps({'passed': report['passed'], 'stage': report['stage'], 'receipt': str(report_path)}), flush=True)
    return 0 if report['passed'] and failure is None else 1


if __name__ == '__main__':
    raise SystemExit(main())
