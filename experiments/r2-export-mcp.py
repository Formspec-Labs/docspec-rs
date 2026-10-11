"""Opt-in exact retained R2 export, interrupted tiny transfer, trusted MCP read."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import traceback
import tempfile
from urllib.parse import urlsplit

REPO = Path(__file__).resolve().parents[1]
DATASET = 'docspec-rs-qualification-0e38b494-34a4-47e4-a96e-f8ae27822f00'
PIN_RECORDS = 'docs/reviews/dagster-inflight-provider-inspect-2026-10-11/retained-records.json'
EXPECTED = [[1, None], [1, 'last'], [1, 'last'], [3, ''], [4, 'keep']]


def save(path, value):
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as stream:
        temporary=Path(stream.name)
        json.dump(value, stream, indent=2); stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
    try: os.link(temporary,path)
    finally: temporary.unlink()


def client():
    import boto3
    from botocore.config import Config
    s3=boto3.client('s3', endpoint_url=os.environ['DOCSPEC_S3_ENDPOINT'], region_name='auto',
        aws_access_key_id=os.environ['DOCSPEC_S3_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['DOCSPEC_S3_SECRET_ACCESS_KEY'],
        config=Config(connect_timeout=15, read_timeout=15, retries={'total_max_attempts': 1},
            s3={'addressing_style':'path'}, request_checksum_calculation='when_required',
            response_checksum_validation='when_required'))
    attempts={'HEAD':0,'GET':0}
    def sent(request,**_):
        assert request.method in attempts
        attempts[request.method]+=1
    s3.meta.events.register('before-send.s3',sent)
    return s3,attempts


def transfer(folder, mode):
    pin = json.loads((folder / 'pin.json').read_text())
    uri = urlsplit(pin['object']['path'])
    assert uri.scheme == 's3' and uri.netloc == os.environ['DOCSPEC_S3_BUCKET']
    assert uri.path.startswith('/__r2_data_catalog/docspec-qualification/'+DATASET+'/engine-update/')
    assert pin['object']['bytes'] == 824 and pin['object']['digest'] == '05074782b79ff9b8097c81e34d2ea6b7da7011fffc7b4e4da44b6b72539253b2'
    s3,attempts = client(); started = time.monotonic()
    if mode == 'prefix':
        head = s3.head_object(Bucket=uri.netloc, Key=uri.path[1:])
        assert head['ContentLength'] == pin['object']['bytes']
        etag = head['ETag']; start, end = 0, 411
    else:
        previous = json.loads((folder/'prefix.json').read_text())
        assert previous['object'] == pin['object'] and previous['saved_bytes'] == 412
        assert (folder/'comments.part').stat().st_size == 412
        etag = previous['etag']; start, end = 412, 823
    result = s3.get_object(Bucket=uri.netloc, Key=uri.path[1:], Range=f'bytes={start}-{end}', IfMatch=etag)
    with result['Body'] as body:
        assert result['ResponseMetadata']['HTTPStatusCode'] == 206
        assert result['ETag'] == etag and result['ContentLength'] == end-start+1
        assert result['ContentRange'] == f'bytes {start}-{end}/824'
        data = body.read(413)
        assert len(data) == 412 and not body.read(1)
    with (folder/'comments.part').open('xb' if mode == 'prefix' else 'ab') as output:
        output.write(data); output.flush(); os.fsync(output.fileno())
    receipt = dict(pid=os.getpid(), object=pin['object'], etag=etag, range=[start,end],
        saved_bytes=(folder/'comments.part').stat().st_size, seconds=time.monotonic()-started,
        observed_http_attempts=attempts, transferred_bytes=412)
    assert attempts==({'HEAD':1,'GET':1} if mode=='prefix' else {'HEAD':0,'GET':1})
    save(folder/(mode+'.json'), receipt)
    if mode == 'prefix':
        # Parent kills only this process, after exact durable prefix completion.
        while True: time.sleep(1)
    else:
        # No overwrite: complete immutable local member appears atomically.
        os.link(folder/'comments.part', folder/'comments.parquet')
        os.unlink(folder/'comments.part')
        descriptor=os.open(folder,os.O_RDONLY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)


def mcp(folder, sibling):
    sys.path[:0]=[str(sibling/'src'),str(sibling)]
    os.environ['SPICY_REGS_DATA_DIR'] = str(folder)
    os.environ['SPICY_REGS_DOCSPEC_EXPORTS'] = str(folder/'binding.json')
    os.environ['SPICY_REGS_CONNECTION_TTL'] = '0'
    os.environ['SPICY_REGS_MEMORY_LIMIT']='256MB'
    from starlette.testclient import TestClient
    from spicy_regs import docspec_exports, mcp_server
    from tests.test_mcp_query_results import call
    counts = {'metadata':0,'hash':0}
    native_record, native_hash = docspec_exports._record, docspec_exports._hash
    def record(*args,**kwargs):
        counts['metadata'] += 1; return native_record(*args,**kwargs)
    def hashed(*args,**kwargs):
        counts['hash'] += 1; return native_hash(*args,**kwargs)
    docspec_exports._record, docspec_exports._hash = record, hashed
    pin = json.loads((folder/'pin.json').read_text())
    with TestClient(mcp_server.build_app()) as app:
        for _ in range(3):
            reply = call(app,'query_sql', {'sql':'SELECT counter,text FROM comments ORDER BY counter,text NULLS FIRST','max_rows':50})
            assert not reply['isError']; value = reply['structuredContent']
            assert value['rows'] == EXPECTED and value['columns'] == ['counter','text']
            assert value['source']=='local'
            assert not value['truncated'] and not value['truncated_cells']
            assert 'private_canary' not in json.dumps(value) and 'synthetic-private-canary' not in json.dumps(value)
            binding = value['publication']['comments']
            assert binding['export'] == pin and binding['status'] == 'docspec_immutable_export'
            assert binding['evidence'] == dict(scope='release',version='update-two',manifest_blake3=pin['source']['manifest']['digest'],row_mapping=False)
            schema = call(app,'query_sql', {'sql':'DESCRIBE comments'})['structuredContent']
            assert [row[:2] for row in schema['rows']] == [['counter','BIGINT'],['text','VARCHAR']]
        assert counts == {'metadata':2,'hash':1}
    save(folder/'mcp.json', dict(passed=True,counts=counts,rows=EXPECTED,pin=pin,scope='Exact export values and source release reference; no row-receipt mapping'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--binary',type=Path)
    parser.add_argument('--remote',type=Path)
    parser.add_argument('--limits',type=Path)
    parser.add_argument('--spicy-regs',type=Path,default=REPO.parent/'spicy-regs')
    parser.add_argument('--child',choices=['prefix','tail','mcp'])
    args = parser.parse_args(); folder=args.output.resolve()
    def expired(*_): raise TimeoutError('qualification deadline or termination')
    signal.signal(signal.SIGALRM,expired); signal.signal(signal.SIGTERM,expired)
    if args.child:
        signal.alarm(120)
        try:
            if args.child=='mcp': mcp(folder,args.spicy_regs.resolve())
            else: transfer(folder,args.child)
            return 0
        except BaseException as error:
            save(folder/(args.child+'-error.json'),{'error_type':type(error).__name__})
            return 1
    assert args.binary and args.remote and args.limits and not folder.exists()
    import shutil
    assert shutil.disk_usage(folder.parent).free >= 2 << 30
    assert os.environ['DOCSPEC_QUALIFICATION_ID'] == DATASET
    records=json.loads((REPO/PIN_RECORDS).read_text()); pin=records['export-result/download']
    assert pin['source']['version']=='update-two' and pin['rows']==5 and pin['policy']['columns']==['counter','text']
    assert pin['fields']==[dict(id=2,name='counter',data_type='Int64',nullable=False),dict(id=1,name='text',data_type='Utf8',nullable=True)]
    folder.mkdir(mode=0o700); save(folder/'pin.json',pin)
    report=dict(passed=False,started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), scope='Retained824-byte R2 export to resumed local transfer to trusted MCP; no remote mutation/receipt update/throughput claim')
    owned=[]; logs=[]; started=time.monotonic()
    def child(mode):
        log=(folder/(mode+'.log')).open('xb'); logs.append(log)
        process=subprocess.Popen(['/usr/bin/time','-l','-o',str(folder/(mode+'.time.log')),sys.executable,str(Path(__file__).resolve()),
            '--output',str(folder),'--spicy-regs',str(args.spicy_regs.resolve()),'--child',mode],stdout=log,stderr=log,
            env={k:v for k,v in os.environ.items() if mode!='mcp' or not k.startswith('DOCSPEC_')})
        owned.append(process); return process
    try:
        signal.alarm(300)
        first=child('prefix'); deadline=time.monotonic()+45
        while not (folder/'prefix.json').exists():
            assert first.poll() is None
            if time.monotonic()>=deadline: raise TimeoutError('prefix transfer marker absent')
            time.sleep(.05)
        import psutil
        prefix=json.loads((folder/'prefix.json').read_text()); native=psutil.Process(prefix['pid'])
        assert native.ppid()==first.pid and first.poll() is None
        native.kill(); first.wait(timeout=15); assert first.returncode != 0
        second=child('tail'); second.wait(timeout=60); assert second.returncode==0
        tail=json.loads((folder/'tail.json').read_text()); assert tail['range']==[412,823]
        with socket.socket() as reserved:
            reserved.bind(('127.0.0.1',0)); port=reserved.getsockname()[1]
        hostlog=(folder/'host.log').open('xb'); logs.append(hostlog)
        host=subprocess.Popen([str(args.binary.resolve()),'--workspace',str(folder/'host'),'--remote',str(args.remote.resolve()),
            '--limits',str(args.limits.resolve()),'serve','--bind',f'127.0.0.1:{port}'],stdout=hostlog,stderr=hostlog); owned.append(host)
        sys.path.insert(0,str(REPO/'adapters/dagster/tests'))
        from test_rust_integration import RustIntegrationTests
        RustIntegrationTests()._wait_ready(host,port,folder/'host.log')
        save(folder/'binding.json',dict(version=1,metadata_url=f'http://127.0.0.1:{port}',binary=str(args.binary.resolve()),
            tables={'comments':dict(file='comments.parquet',export=pin)}))
        reader=child('mcp'); reader.wait(timeout=135); assert reader.returncode==0
        admission=json.loads((folder/'mcp.json').read_text()); assert admission['passed']
        assert not any(line.startswith('DOCSPEC_TEST_WORK ') for line in (folder/'host.log').read_text().splitlines())
        report.update(passed=True,transfer_seconds=prefix['seconds']+tail['seconds'],
            complete_path_seconds=time.monotonic()-started,transfer_requests={method:prefix['observed_http_attempts'][method]+tail['observed_http_attempts'][method] for method in ('HEAD','GET')},transfer_bytes=824,
            prefix_bytes_repeated=False,admissions=admission['counts'],original_input_needed=False,
            retained_files=['comments.parquet','pin.json','binding.json'],source=pin['source'])
    except BaseException as error:
        report.update(error_type=type(error).__name__,error_locations=[{'file':Path(f.filename).name,'line':f.lineno} for f in traceback.extract_tb(error.__traceback__)])
    finally:
        signal.alarm(0)
        import psutil
        for process in reversed(owned):
            if process.poll() is None:
                try: descendants=psutil.Process(process.pid).children(recursive=True)
                except psutil.NoSuchProcess: descendants=[]
                for descendant in descendants:
                    try: descendant.kill()
                    except psutil.NoSuchProcess: pass
                try: process.terminate()
                except ProcessLookupError: pass
                try: process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    try: process.kill()
                    except ProcessLookupError: pass
                    process.wait(timeout=10)
        for log in logs: log.close()
        report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat(); save(folder/'receipt.json',report)
    print(json.dumps({'passed':report['passed'],'receipt':str(folder/'receipt.json')}))
    return 0 if report['passed'] else 1

if __name__=='__main__': raise SystemExit(main())
