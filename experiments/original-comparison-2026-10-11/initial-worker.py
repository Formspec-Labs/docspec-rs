"""One isolated benchmark worker; native APIs only, no production modifications."""
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
import traceback

START=time.monotonic()
HERE=Path(__file__).resolve().parent
arm,case,outtext=sys.argv[1:]
OUT=Path(outtext)
WORK=OUT/'workspace'
SCRATCH=OUT/'scratch'
SCRATCH.mkdir()
os.environ['TMPDIR']=str(SCRATCH)
os.environ['ARROW_NUM_THREADS']='1'
os.environ['OMP_NUM_THREADS']='1'
os.environ['OPENBLAS_NUM_THREADS']='1'
prep=json.loads((HERE/'preparation.json').read_text())
input_info=prep['cases'][case]
RESULT={'arm':arm,'case':case,'status':'started','phases':{},'load_before':os.getloadavg(),'checks':[]}

def save(path,value):
    path.write_text(json.dumps(value,indent=2,default=str)+'\n')

def usage(path):
    files=[p.stat() for p in path.rglob('*') if p.is_file()]
    return {'logical_bytes':sum(s.st_size for s in files),'allocated_bytes':sum(s.st_blocks*512 for s in files),'files':len(files)}

def stage(label,call):
    started=time.perf_counter()
    try: return call()
    finally:
        RESULT['phases'][label]={'seconds':time.perf_counter()-started,'self_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, 'child_peak_rss_bytes':resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss}
        save(OUT/'result.json',RESULT)

try:
    import pyarrow as pa
    import pyarrow.ipc as ipc
    import pyarrow.parquet as pq
    pa.set_cpu_count(1)
    columns=pq.read_schema(input_info['input']).names
    def oracle(path):
        global expected
        if expected is None:
            expected=pq.read_table(input_info['input'],use_threads=False).sort_by([('benchmark_row','ascending')])
        with pa.memory_map(str(path),'r') as stream:
            actual=ipc.open_stream(stream).read_all()
        RESULT['readback_boundary_usage']=max(RESULT.get('readback_boundary_usage',0),usage(OUT)['logical_bytes'])
        check={'rows':actual.num_rows,'columns':actual.column_names,'bytes':path.stat().st_size,
               'types_and_nullability_equal':expected.schema.equals(actual.schema,check_metadata=False),
               'all_values_equal':False}
        check['all_values_equal']=actual.sort_by([('benchmark_row','ascending')]).equals(expected,check_metadata=False)
        RESULT['checks'].append(check)
        assert check['types_and_nullability_equal'] and check['all_values_equal'], check
        path.unlink() # Only this independently checked disposable handoff.
    expected=None
    if arm=='python':
        import docspec
        RESULT['original_module']=docspec.__file__
        from docspec.runtime import CoreWorkspace
        def initial():
            global workspace
            workspace=CoreWorkspace(WORK,engine_memory_bytes=6<<30)
            return workspace.admit_generation(input_info['generation'],family=input_info['family'],table='table',dataset='benchmark')
        admitted=stage('admit',initial)
        save(OUT/'selection.json',{'state_id':admitted.state_id,'report':admitted.report})
        def read(output, expected_pin=None):
            with workspace.open_state(admitted.state_id,expected_pin=expected_pin) as reader:
                with reader.table() as relation:
                    selected=relation.project(', '.join('"'+name.replace('"','""')+'"' for name in columns))
                    batches=selected.fetch_record_batch(rows_per_batch=4096)
                    with pa.OSFile(str(output),'wb') as sink, ipc.new_stream(sink,batches.schema) as writer:
                        for batch in batches: writer.write_batch(batch)
                    return reader.pin
        pin=stage('read',lambda:read(OUT/'read.arrow'))
        stage('oracle_read',lambda:oracle(OUT/'read.arrow'))
        again=stage('retry',lambda:workspace.admit_generation(input_info['generation'],family=input_info['family'],table='table',dataset='benchmark'))
        assert again.state_id==admitted.state_id
        workspace.close()
        def reopen_read():
            global workspace
            workspace=CoreWorkspace(WORK,engine_memory_bytes=6<<30,create=False)
            try: return read(OUT/'reopen.arrow',expected_pin=pin)
            finally: workspace.close()
        assert stage('reopen_read',reopen_read)==pin
        stage('oracle_reopen',lambda:oracle(OUT/'reopen.arrow'))
    else:
        binary=prep['arms'][arm]['binary']
        request={'dataset':'benchmark','operation':'initial','input':input_info['input'],'expected_digest':input_info['blake3'],
                 'definition':'comparison-v1','sql':'SELECT * FROM input'}
        request_path=OUT/'request.json'
        save(request_path,request)
        def command(label,*args):
            argv=[binary,'--workspace',str(WORK),'--limits',str(HERE/'limits.json'),*map(str,args)]
            save(OUT/(label+'.argv.json'),argv)
            with (OUT/(label+'.stdout.json')).open('w') as stdout, (OUT/(label+'.stderr.log')).open('w') as stderr:
                subprocess.run(argv,stdout=stdout,stderr=stderr,check=True)
            return json.loads((OUT/(label+'.stdout.json')).read_text()) if label in ('admit','retry') else None
        release=stage('admit',lambda:command('admit','run',request_path))
        stage('read',lambda:command('read','read',OUT/'admit.stdout.json',OUT/'read.arrow'))
        stage('oracle_read',lambda:oracle(OUT/'read.arrow'))
        retried=stage('retry',lambda:command('retry','run',request_path))
        assert release==retried, 'completed retry changed the immutable result'
        stage('reopen_read',lambda:command('reopen_read','read',OUT/'admit.stdout.json',OUT/'reopen.arrow'))
        stage('oracle_reopen',lambda:oracle(OUT/'reopen.arrow'))
    RESULT['status']='passed'
    RESULT['same_selection']=True
except BaseException as error:
    RESULT.update(status='failed',error=repr(error))
    traceback.print_exc()
finally:
    RESULT['worker_seconds']=time.monotonic()-START
    RESULT['first_admit_read_seconds']=sum(RESULT['phases'].get(k,{}).get('seconds',0) for k in ('admit','read'))
    RESULT['peak_self_rss_bytes']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    RESULT['peak_child_rss_bytes']=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    RESULT['retained_engine']=usage(WORK)
    RESULT['retained_run']=usage(OUT)
    RESULT['load_after']=os.getloadavg()
    save(OUT/'result.json',RESULT)
    print(json.dumps(RESULT),flush=True)
sys.exit(0 if RESULT['status']=='passed' else 1)
