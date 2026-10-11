"""Read-only reconciliation of the exact retained detach/register experiment."""
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

SOURCE = 'docspec_detach_probe_a8359bcc_f82e_46c1_8751_387f74adb839.candidate'
TARGET = 'docspec_detach_probe_a8359bcc_f82e_46c1_8751_387f74adb839_restored.candidate'


def main():
    if len(sys.argv) != 3:
        raise ValueError('usage: reconcile.py ORIGINAL_RECEIPT FRESH_ABSOLUTE_OUTPUT_FILE')
    source, output = map(Path, sys.argv[1:])
    if source.stat().st_size > 64 << 10 or not output.is_absolute() or output.exists():
        raise ValueError('bounded original receipt and fresh absolute output required')
    receipt_bytes = source.read_bytes()
    retained = json.loads(receipt_bytes)
    if (retained.get('source_table'), retained.get('restored_table'), retained.get('stage'), retained.get('error_type')) != (SOURCE, TARGET, 'stale_old_identifier_commit', 'ForbiddenError'):
        raise ValueError('unexpected retained experiment')
    identity = retained['source_identity']
    if identity != retained.get('registered_identity') or not any(step.get('stage') == 'stale_old_identifier_commit' and step.get('http_status') == 403 for step in retained['steps']):
        raise ValueError('exact registration and rejected commit evidence required')
    if importlib.metadata.version('pyiceberg') != '0.12.0':
        raise ValueError('PyIceberg 0.12.0 required')
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.exceptions import NoSuchTableError
    from tenacity import stop_after_attempt
    config = {key: os.environ[name] for key, name in {'uri':'DOCSPEC_CATALOG_URI', 'warehouse':'DOCSPEC_CATALOG_WAREHOUSE', 'token':'DOCSPEC_CATALOG_TOKEN'}.items()}
    uri = urlsplit(config['uri'])
    if uri.scheme != 'https' or not uri.hostname or uri.username or uri.password or uri.query or uri.fragment or any(not value for value in config.values()):
        raise ValueError('explicit existing HTTPS configuration required')
    report = dict(passed=False, started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        original_receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(), runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        source_table=SOURCE, restored_table=TARGET, steps=[], mutations=0, client_object_requests=0,
        scope='Read-only exact registration reconciliation; preserves original strict 404-only probe failure')
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('x') as stream: json.dump(report,stream,indent=2)
    started = time.monotonic()
    def expired(*_): raise TimeoutError('reconciliation deadline')
    signal.signal(signal.SIGALRM, expired); signal.signal(signal.SIGTERM, expired)
    def step(name, operation):
        event = dict(stage=name, acknowledged=False); report['steps'].append(event)
        remaining=90-(time.monotonic()-started)
        if remaining <= 0: raise TimeoutError('reconciliation deadline')
        signal.setitimer(signal.ITIMER_REAL, min(25,remaining))
        try:
            value=operation(); event['acknowledged']=True; return value
        except BaseException as error:
            event['error_type']=type(error).__name__
            status=getattr(getattr(error.__cause__,'response',None),'status_code',None)
            if isinstance(status,int): event['http_status']=status
            raise
        finally:
            signal.setitimer(signal.ITIMER_REAL,0)
            output.write_text(json.dumps(report,indent=2)+'\n')
    catalog=None
    try:
        catalog=step('fresh_client_config',lambda: RestCatalog('docspec-detach-reconciliation',**config))
        def load(identifier):
            return RestCatalog.load_table.retry_with(stop=stop_after_attempt(1))(catalog,tuple(identifier.split('.')))
        try: step('old_registration_absent',lambda:load(SOURCE))
        except NoSuchTableError: report['old_registration_absent']=True
        else: raise ValueError('old registration returned a table')
        table=step('restored_exact_readback',lambda:load(TARGET))
        encoded=json.dumps(table.metadata.model_dump(mode='json',by_alias=True),sort_keys=True,separators=(',',':')).encode()
        if len(encoded)>1<<20: raise ValueError('unexpected metadata size')
        observed=dict(metadata_location=table.metadata_location,uuid=str(table.metadata.table_uuid),location=table.metadata.location,
            native_metadata_sha256=hashlib.sha256(encoded).hexdigest(),native_metadata_json_bytes=len(encoded))
        report['restored_identity']=observed
        if observed!=identity: raise ValueError('restored native identity changed')
        report.update(passed=True,classification='The recorded old-identifier commit was rejected with HTTP403; the old registration remains absent and the exact restored registration is readable and unchanged. This excludes blanket current authentication failure, not commit-specific authorization as the cause of the original403. No claim of a successful404-only frozen probe or full restoration.')
    except BaseException as error: report['error_type']=type(error).__name__
    finally:
        signal.setitimer(signal.ITIMER_REAL,0)
        if catalog is not None:
            try: catalog.close()
            except Exception as error: report['close_error_type']=type(error).__name__; report['passed']=False
        report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
        output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(dict(passed=report['passed'],receipt=str(output))))
    return 0 if report['passed'] else 1


if __name__ == '__main__': raise SystemExit(main())
