"""Freeze the completed current optimized build without rereading inputs."""
import hashlib
import json
from pathlib import Path
import subprocess
import time

HERE=Path(__file__).resolve().parent
REPO=HERE.parents[1]

def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()

prep=json.loads((HERE/'preparation.json').read_text())
commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
assert commit==prep['rust_current_commit']
assert not subprocess.check_output(['git','diff','--','src','Cargo.toml','Cargo.lock'],cwd=REPO)
log=(HERE/'current-release-build.stderr.log').read_text()
assert 'Finished `release` profile' in log
binary=REPO/'target/release/docspec'
s=binary.stat()
prep['arms']={'rust-current-release':{'binary':str(binary),'sha256':sha(binary),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,
              'commit':commit,'profile':'release','features':[],'CARGO_PROFILE_RELEASE_DEBUG':'0'}}
prep['build']={'command':'CARGO_PROFILE_RELEASE_DEBUG=0 cargo build --release --locked --bin docspec',
              'cargo_completion':log.splitlines()[-1],
              'rustc':subprocess.check_output(['rustc','--version'],text=True).strip(),
              'cargo':subprocess.check_output(['cargo','--version'],text=True).strip(),
              'lock_sha256':sha(REPO/'Cargo.lock'),'frozen_at':time.time()}
with (HERE/'current-preparation.json').open('x') as out:
    json.dump(prep,out,indent=2); out.write('\n')
print(json.dumps(prep['arms']))
