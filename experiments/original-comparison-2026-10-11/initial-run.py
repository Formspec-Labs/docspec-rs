"""Sequential balanced collection using the existing resource monitor."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent
REPO=HERE.parents[1]
spec=importlib.util.spec_from_file_location('monitor',HERE/'monitor.py')
monitor=importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
monitor.REPO=REPO

def main():
    root=HERE/'runs'
    root.mkdir(exist_ok=False)
    env={'platform':platform.platform(),'load':os.getloadavg(),'free_bytes':shutil.disk_usage(HERE).free,
         'processes':subprocess.check_output(['ps','-axo','pid,pcpu,rss,comm'],text=True),'started':time.time()}
    monitor.save(HERE/'collection-environment.json',env)
    prep=json.loads((HERE/'preparation.json').read_text())
    for arm in prep['arms'].values():
        st=Path(arm['binary']).stat()
        assert (st.st_size,st.st_mtime_ns)==(arm['bytes'],arm['mtime_ns']), 'binary changed since preparation'
    monitor.save(HERE/'execution-sources.json',{
        name: hashlib.sha256(path.read_bytes()).hexdigest() for name,path in {
            **{name:HERE/name for name in ('protocol.md','prepare.py','worker.py','run.py','limits.json','preparation.json')},
            'monitor.py':HERE/'monitor.py',
            'rust-lock':REPO/'Cargo.lock','python-lock':REPO.parent/'DocSpec/uv.lock'
        }.items()})
    results=[]
    for case in ('bill-actions','comments'):
        arms=['python','rust-release','rust-debug']
        for repetition in range(3):
            for arm in arms[repetition:]+arms[:repetition]:
                if shutil.disk_usage(HERE).free < (2<<30)+(512<<20):
                    raise RuntimeError('insufficient free space for next bounded worker plus reserve')
                run=root/f'{case}-{repetition+1}-{arm}'
                run.mkdir()
                argv=[str(HERE/'env/bin/python'),str(HERE/'worker.py'),arm,case,str(run)]
                try:
                    value,metrics=monitor.command(run,run,'worker',argv,512<<20,
                        limits={'memory_bytes':12<<30,'scratch_bytes':512<<20},timeout_seconds=180,
                        scratch_prefixes=('scratch','workspace/scratch','workspace/records/scratch'),progress_seconds=30)
                    results.append({'case':case,'arm':arm,'repetition':repetition+1,'result':value,'metrics':metrics})
                except Exception as error:
                    results.append({'case':case,'arm':arm,'repetition':repetition+1,'error':str(error)})
                    monitor.save(HERE/'results.json',results)
                    raise
                monitor.save(HERE/'results.json',results)
                print(json.dumps({'case':case,'arm':arm,'repetition':repetition+1,
                                 'admit_read_seconds':value['first_admit_read_seconds'],'retry_seconds':value['phases']['retry']['seconds'],
                                 'read_seconds':value['phases']['reopen_read']['seconds']}),flush=True)
    for arm in prep['arms'].values():
        st=Path(arm['binary']).stat()
        assert (st.st_size,st.st_mtime_ns)==(arm['bytes'],arm['mtime_ns']), 'binary changed during collection'
    monitor.save(HERE/'collection-end.json',{'finished':time.time(),'free_bytes':shutil.disk_usage(HERE).free})

if __name__=='__main__': main()
