"""Prepare the declared real-data subsets once; use only retained local assets."""
import hashlib
import importlib.metadata as md
import json
import platform
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from rulespec_artifacts import (LocalMemberSource, Producer, build_artifact_root,
    canonical_json_bytes, describe_member, write_member_manifest)

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
RUST = REPO / 'workspaces/qualification/comments-e2e-oracle-threads4-20261010/20261010T105814-acceptance-f6d5b63f/frozen/docspec'
ORIGINAL = REPO.parent / 'DocSpec'

def save(path, value):
    with path.open('x') as out:
        json.dump(value, out, indent=2, default=str)
        out.write('\n')

def sha(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()

def main():
    started = time.monotonic()
    inputs = HERE / 'inputs'
    inputs.mkdir(exist_ok=False)
    sources = {
        'bill-actions': REPO.parent / 'experiments/docspec-spicyregs-performance-20261007/inputs/bill-actions/bill_actions.parquet',
        'comments': REPO / 'workspaces/qualification/comments-10m-20261008/comments.parquet',
    }
    result = {'preparation_started': time.time(), 'python': sys.version, 'platform': platform.platform(),
              'packages': {k: md.version(k) for k in ('docspec','duckdb','pyarrow','pyiceberg','spicy-docs','msgspec','jsonschema-rs')},
              'original_commit': subprocess.check_output(['git','rev-parse','HEAD'],cwd=ORIGINAL,text=True).strip(),
              'rust_current_commit': subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),
              'arms': {}, 'cases': {}}
    for arm, binary, commit, profile in [('rust-release',RUST,'e40be018b2a1c12c2e1067ba4fe552de3a6711f6','release'),
                                        ('rust-debug',REPO/'target/debug/docspec',result['rust_current_commit'],'dev')]:
        s=binary.stat()
        result['arms'][arm]={'binary':str(binary),'sha256':sha(binary),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'commit':commit,'profile':profile}
    assert result['arms']['rust-release']['sha256']=='3b75f246bc282e60a3fda713b14830f3c16ce9dbe6b5c3afb9ecc621f05a731b'
    producer = Producer('docspec-benchmark','git+https://example.test/docspec-rs@'+result['rust_current_commit'],
                        'urn:docspec:benchmark','1','git+https://example.test/docspec-rs@'+result['rust_current_commit'])
    for name, source in sources.items():
        dest = inputs/name
        dest.mkdir()
        reader=pq.ParquetFile(source)
        groups = list(range(reader.num_row_groups)) if name=='bill-actions' else [i*(reader.num_row_groups-1)//25 for i in range(25)]
        rows=0
        output=dest/'table.parquet'
        writer=None
        for group in groups:
            for batch in reader.iter_batches(batch_size=4096,row_groups=[group],use_threads=False):
                # A new experiment key preserves every duplicate original occurrence.
                keys=pa.array([str(i) for i in range(rows,rows+batch.num_rows)],type=pa.string())
                batch=pa.RecordBatch.from_arrays([*batch.columns,keys],schema=pa.schema([*batch.schema,pa.field('benchmark_row',pa.string())]))
                if writer is None:
                    writer=pq.ParquetWriter(output,batch.schema,compression='zstd')
                writer.write_batch(batch,row_group_size=4096)
                rows+=batch.num_rows
        writer.close()
        reader.close()
        member=describe_member(LocalMemberSource(dest),object_key='table.parquet',role='table',media_type='application/vnd.apache.parquet',record_count=rows)
        with duckdb.connect() as con:
            columns=[[a,b] for a,b,*_ in con.execute('DESCRIBE SELECT * FROM read_parquet(?)',[str(output)]).fetchall()]
        with (dest/'members.json').open('wb') as out:
            manifest=write_member_manifest(out,scope_kind='global',scope_id='benchmark-'+name,object_key='members.json',members=[member])
        root=build_artifact_root(kind='spicy-regs-rollup-generation',spec={'family':'benchmark-'+name,'publicationStatus':'complete-family',
            'tables':{'table.parquet':{'columns':columns,'rows':rows,'identity':['benchmark_row']}}},producer=producer,manifests=[manifest])
        (dest/'artifact.json').write_bytes(canonical_json_bytes(root))
        identity=json.loads(subprocess.check_output([str(RUST),'hash',str(output)],text=True))
        result['cases'][name]={'source':str(source),'source_stat':{'bytes':source.stat().st_size,'mtime_ns':source.stat().st_mtime_ns},
            'source_row_groups':groups,'rows':rows,'input':str(output),'bytes':output.stat().st_size,'sha256':member.sha256,
            'blake3':identity['digest'],'generation':str(dest),'family':'benchmark-'+name,
            'pin':{'logicalId':root['logicalId'],'artifactDigest':root['artifactDigest']},'schema':str(pq.read_schema(output))}
    limits={'compute_threads':12,'memory_bytes':6<<30,'scratch_bytes':512<<20,'batch_rows':4096,'max_batch_bytes':256<<20,'max_metadata_bytes':32<<20}
    save(HERE/'limits.json',limits)
    result['preparation_seconds']=time.monotonic()-started
    save(HERE/'preparation.json',result)
    print(json.dumps({k:{'rows':v['rows'],'bytes':v['bytes']} for k,v in result['cases'].items()}))

if __name__=='__main__': main()
