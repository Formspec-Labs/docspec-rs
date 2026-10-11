"""Bounded complete-path diagnostic driver. No work occurs at import; see adjacent protocol."""
import argparse
import collections
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import sqlite3
import subprocess
import sys
import time

LIMITS = dict(compute_threads=2, memory_bytes=512 << 20, scratch_bytes=64 << 20,
              batch_rows=4096, max_batch_bytes=8 << 20, max_metadata_bytes=8 << 20)
LEVELS = (256, 4096)
REPEATS = 3

def save(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2); stream.write('\n')

def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''): h.update(block)
    return h.hexdigest()

def load_monitor(path, repo):
    spec = importlib.util.spec_from_file_location('frozen_monitor', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.REPO = repo
    return module

def population(n):
    return [(i // 2 if i < n - 2 else None,
             None if i % 7 == 0 else '' if i % 11 == 0 else f'row-{i}') for i in range(n)]

def table(rows):
    import pyarrow as pa
    return pa.Table.from_pydict(dict(id=[r[0] for r in rows], text=[r[1] for r in rows]),
        schema=pa.schema([pa.field('id', pa.int64(), nullable=True), pa.field('text', pa.string(), nullable=True)]))

def counter(rows):
    return collections.Counter(json.dumps(r, sort_keys=True, ensure_ascii=False) for r in rows)

def phase_snapshot(workspace, phases_only=True):
    """Exact native lifecycle rows from the maintained pinned SQL catalog schema."""
    from pyiceberg.table import StaticTable
    assert not (workspace / 'catalog-current.json').exists(), 'restore is outside this protocol'
    with sqlite3.connect(f'file:{workspace / "catalog.sqlite"}?mode=ro', uri=True) as catalog:
        locations = catalog.execute("SELECT metadata_location FROM iceberg_tables WHERE catalog_name='docspec' AND table_name='lifecycle' AND iceberg_type='TABLE'").fetchall()
    result = {}
    for (location,) in locations:
        for row in StaticTable.from_metadata(location).scan().to_arrow().to_pylist():
            key, revision = row['key'], row['revision']
            if not phases_only or key.startswith('phase/') or key.startswith('backfill-'):
                if key not in result or revision > result[key]['revision']:
                    result[key] = dict(revision=revision, json=row['value'])
    return result

def work(log):
    return [line.split('DOCSPEC_TEST_WORK ', 1)[1].strip() for line in log.splitlines() if 'DOCSPEC_TEST_WORK ' in line]

def growth_profile(case, frozen, prepared, command, request, observations):
    """Fixed-population public-API arms; construction is measured, never fabricated."""
    import pyarrow as pa
    import pyarrow.ipc as ipc
    import pyarrow.parquet as pq
    import blake3
    arms = {}
    rows = population(4096)
    expected = [dict(id=i, text=t) for i, t in rows]
    growth = case / 'growth'; growth.mkdir()
    limits = dict(LIMITS, batch_rows=64)
    limits_path = growth / 'limits.json'; save(limits_path, limits)

    def start(name):
        home = growth / name; home.mkdir(); workspace = home / 'workspace'; workspace.mkdir()
        source = home / 'input.parquet'; shutil.copy2(frozen / 'input-4096.parquet', source)
        spec = dict(dataset='cost', operation='initial', input=str(source),
            expected_digest=prepared['assets']['input-4096.parquet']['blake3'],
            sql='SELECT * FROM input', definition='v1')
        def run(label, *parts, **options):
            return command(name+'-'+label, *parts, workspace_override=workspace,
                limits_override=limits_path, **options)
        def req(label, value):
            return request(name+'-'+label, value)
        release = run('initial', 'run', req('initial', spec))
        return home, workspace, spec, release, run, req

    def check(run, req, release, literal, label, combined=False, export=False):
        arrow = growth / (label+'.arrow')
        pin = req(label+'-pin', release)
        run(label, 'read-export' if export else 'read-combined' if combined else 'read', pin, arrow, json_output=False)
        began = time.monotonic()
        with pa.memory_map(str(arrow), 'r') as mapped: actual = ipc.open_stream(mapped).read_all()
        assert counter(actual.to_pylist()) == counter(literal)
        assert actual.schema.field('id').type == pa.int64()
        assert actual.schema.field('text').type in (pa.string(), pa.large_string())
        observations.append(dict(kind=label+'-oracle', seconds=time.monotonic()-began))

    def endpoint(name, depth, workspace, spec, release, run, req):
        label = f'd{depth}'
        records = run(label+'-inspect', 'inspect', 'cost')
        native = phase_snapshot(workspace, phases_only=False)
        retired = {v['record']['key'] for k,v in records.items() if k.startswith('retired/')
            and native.get(v['record']['key'],{}).get('revision') == v['record']['revision']}
        counts = dict(logical_rows=release['dataset']['groups']['source']['rows'],
            source_files=sum(o['path'].endswith('.parquet') for o in release['dataset']['groups']['source']['objects']),
            retained_results=sum(k.startswith('result/') and k not in retired for k in records),
            retained_processing=sum(k.startswith('phase/') and k.endswith('/processing') and k not in retired for k in records),
            retained_evidence_results=sum(k.startswith('result/') and bool(v.get('dataset',{}).get('evidence')) and k not in retired for k,v in records.items()),
            native_records=len(native), native_record_json_bytes=sum(len(v['json'].encode()) for v in native.values()))
        run(label+'-latest', 'latest', 'cost')
        run(label+'-plan', 'plan', req(label+'-plan', dict(spec, operation=label+'-probe')))
        check(run, req, release, expected, name+'-'+label+'-read', combined=True)
        objects = req(label+'-objects', release['dataset']['groups']['source']['objects'])
        run(label+'-retention', 'inspect-retention', objects)
        arms.setdefault(name, []).append(dict(requested_depth=depth, counts=counts))

    # Two fresh physical arms: the smaller batch limit is identical in both.
    for name, target in [('files-one', 16 << 20), ('files-many', 512)]:
        home, workspace, spec, release, run, req = start(name)
        compact = run('compact', 'compact', req('compact', dict(dataset='cost', operation='layout',
            version='initial', group='source', files=[], target_file_bytes=target)))
        assert compact['verified'] and compact['rows'] == 4096
        # Logical release stays fixed; configured reads resolve the selected representation.
        endpoint(name, 1, workspace, spec, release, run, req)
        arms[name][0]['selected_representation_files'] = len(compact['output_files'])
        run('retire-plan', 'plan-compaction-retirement', 'cost', 'end-layout-recovery', 'layout', 'cost profile')
        run('retire', 'retire-compaction', case / (name+'-retire-plan.stdout.json'))
    assert arms['files-many'][0]['selected_representation_files'] > arms['files-one'][0]['selected_representation_files'], 'fragment contrast was not constructed'

    # Selection history changes; identical values and input bytes remain fixed.
    for name in ['versions', 'phases', 'evidence', 'evidence-control']:
        home, workspace, spec, release, run, req = start(name)
        if name == 'evidence':
            occurrences = home / 'occurrences.arrow'
            run('occurrences', 'read-occurrences', req('occurrences-pin',release), occurrences, json_output=False)
            began = time.monotonic()
            with pa.memory_map(str(occurrences),'r') as mapped: native = ipc.open_stream(mapped).read_all()
            assert counter(native.select(['id','text']).to_pylist()) == counter(expected)
            key = (native['__docspec_file'][0].as_py(), native['__docspec_row'][0].as_py())
            observations.append(dict(kind='receipt-key-oracle',seconds=time.monotonic()-began))
        original_groups = release['dataset']['groups']
        pending = []
        for depth in range(1, 9):
            operation = f'growth-{depth}'
            if name == 'phases':
                distinct = dict(spec, operation=operation, sql='SELECT id,text FROM input WHERE '+ ' AND '.join(['TRUE']*depth))
                run(operation, 'pause-after', req(operation, distinct), 'processing', paused=True)
                pending.append(operation)
            elif name == 'evidence':
                receipt = home / f'receipt-{depth}.parquet'
                pq.write_table(pa.Table.from_pydict(dict(source_file=[key[0]], source_row=[key[1]], evidence=[f'receipt-{depth}']),
                    schema=pa.schema([pa.field('source_file',pa.string(),False),pa.field('source_row',pa.int64(),False),pa.field('evidence',pa.string(),True)])), receipt)
                digest = blake3.blake3(receipt.read_bytes()).hexdigest()
                release = run(operation, 'evidence', req(operation, dict(dataset='cost', operation=operation,
                    definition='v1', base=release['id'], replacements=dict(path=str(receipt),digest=digest), deletions=None)))
            else:
                release = run(operation, 'run', req(operation, dict(spec, operation=operation)))
                assert release['dataset']['groups'] == original_groups, 'unchanged selection rewrote material'
            if depth in (1,8):
                endpoint(name, depth, workspace, spec, release, run, req)
                if name == 'evidence':
                    arrow = home / f'receipts-{depth}.arrow'
                    run(f'receipts-{depth}','read-receipts',req(f'receipts-{depth}-pin',release),arrow,json_output=False)
                    began = time.monotonic()
                    with pa.memory_map(str(arrow),'r') as mapped: receipt_rows = ipc.open_stream(mapped).read_all().to_pylist()
                    assert receipt_rows == [dict(source_file=key[0],source_row=key[1],evidence=f'receipt-{depth}')]
                    observations.append(dict(kind=f'receipts-{depth}-oracle',seconds=time.monotonic()-began))
        if pending:
            for operation in pending: run('abandon-'+operation, 'abandon', 'cost', operation, 'profile complete')
        # The same two replacement rows preserve values but replace occurrences.
        # Pending source attempts are explicitly abandoned before new intake.
        replacement = home / 'replacement.parquet'
        pq.write_table(table([r for r in rows if r[0] == 1]), replacement)
        digest = blake3.blake3(replacement.read_bytes()).hexdigest()
        changed = run('fixed-update','update',req('fixed-update',dict(dataset='cost',operation='fixed-update',definition='v1',
            base=release['id'],keys=['id'],replacements=dict(path=str(replacement),digest=digest),deletions=None)))
        check(run,req,changed,expected,name+'-fixed-update-read',combined=True)
        records = run('terminal-inspect', 'inspect', 'cost')
        keys = [k for k in records if k.startswith('phase/') and k.endswith('/processing')]
        run('retire-plan', 'plan-phase-retirement', 'cost', 'end-processing-reuse', req('retire-keys',keys), 'cost profile')
        run('retire', 'retire-phases', case / (name+'-retire-plan.stdout.json'))

    for name in ['versions','phases','evidence','evidence-control']:
        before, after = arms[name]
        field = 'retained_processing' if name == 'phases' else 'retained_results'
        assert after['counts'][field] > before['counts'][field], f'{name} contrast was not constructed'
    assert arms['evidence'][1]['counts']['retained_evidence_results'] > arms['evidence'][0]['counts']['retained_evidence_results']

    # Connected composition/export/readback/maintenance costs on one fixed source.
    name='layout-cost'; home, workspace, spec, release, run, req = start(name)
    scalar = dict(dataset='cost', operation='lengths', base=release['id'], definition='v1',group='length',group_definition='length-v1',
        fields=[dict(id=10,name='text_length',expression='length(text)')])
    combined = run('backfill', 'backfill', req('backfill',scalar))
    check(run,req,combined,[dict(id=i,text=t,text_length=None if t is None else len(t)) for i,t in rows],name+'-combined',combined=True)
    export = run('export','export',req('export',dict(dataset='cost',operation='flat',source=dict(dataset='cost',version=combined['id'],manifest=combined['manifest']),
        channel='flat',expected=None,policy=dict(group='source',rows='all',columns=['id','text']))))
    check(run,req,export,expected,name+'-flat',export=True)
    run('compact','compact',req('compact',dict(dataset='cost',operation='layout',version=combined['id'],group='source',files=[],target_file_bytes=16<<20)))
    run('retention','inspect-retention',req('objects',combined['dataset']['groups']['source']['objects']))
    run('retire-plan','plan-availability-retirement','cost','end-initial','initial','cost profile')
    run('retire','retire-availability',case/(name+'-retire-plan.stdout.json'))
    save(growth / 'axes.json', arms)
    return arms

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare', 'run'])
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--build-receipt', type=Path)
    parser.add_argument('--growth-only', action='store_true', help='Fixed4096-row independent growth and layout costs; omit baseline repeats')
    args = parser.parse_args()
    repo, output = args.repo.resolve(), args.output.resolve()
    if args.mode == 'prepare':
        import pyarrow.parquet as pq
        import blake3
        assert args.binary and args.build_receipt
        assert not output.exists(), 'never replace an earlier series'
        assert shutil.disk_usage(output.parent).free >= 3 << 30
        receipt = json.loads(args.build_receipt.read_text())
        commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
        assert receipt['commit'] == commit and 'test-util' in receipt['features']
        assert receipt['binary_sha256'] == sha(args.binary)
        output.mkdir(); frozen = output / 'frozen'; frozen.mkdir()
        for source, name in [(args.binary, 'docspec'), (Path(__file__), 'runner.py'),
            (Path(__file__).with_name('protocol.md'), 'protocol.md'),
            (repo / 'experiments/comments-input/run.py', 'monitor.py'), (args.build_receipt, 'build-receipt.json')]:
            shutil.copy2(source, frozen / name)
        os.chmod(frozen / 'docspec', 0o555)
        assets = {}
        for n in LEVELS:
            path = frozen / f'input-{n}.parquet'; pq.write_table(table(population(n)), path)
            replacement = frozen / f'replacement-{n}.parquet'; pq.write_table(table([(1, 'fixed'), (1, None)]), replacement)
            for item in (path, replacement):
                h, b, count = hashlib.sha256(), blake3.blake3(), 0
                with item.open('rb') as stream:
                    for block in iter(lambda: stream.read(1 << 20), b''):
                        h.update(block); b.update(block); count += len(block)
                assets[item.name] = dict(sha256=h.hexdigest(), blake3=b.hexdigest(), bytes=count)
        save(output / 'preparation.json', dict(commit=commit, limits=LIMITS, assets=assets,
            frozen={p.name: assets[p.name]['sha256'] if p.name in assets else sha(p) for p in frozen.iterdir()}, platform=platform.platform(),
            python=sys.version, created=time.time(), growth_only=args.growth_only, status='prepared-no-measurement'))
        return
    prepared = json.loads((output / 'preparation.json').read_text()); frozen = output / 'frozen'
    assert not (output / 'completion.json').exists(), 'series already observed; preserve it'
    assert all(sha(frozen / name) == digest for name, digest in prepared['frozen'].items())
    assert prepared['limits'] == LIMITS
    assert prepared.get('growth_only', False) == args.growth_only
    monitor = load_monitor(frozen / 'monitor.py', repo)
    save(output / 'limits.json', LIMITS)
    import pyarrow as pa
    import pyarrow.ipc as ipc
    import pyarrow.parquet as pq
    import psutil
    save(output / 'collection-environment.json', dict(load=os.getloadavg(), free=shutil.disk_usage(output).free,
         processes=[dict(pid=p.pid, name=p.info['name']) for p in psutil.process_iter(['name'])],
         libraries=dict(pyarrow=pa.__version__, psutil=psutil.__version__), cache='OS cache not cleared'))
    deadline = time.monotonic() + (1800 if args.growth_only else 900)
    results = []; failure = None
    try:
        for n in ((4096,) if args.growth_only else LEVELS):
            for repeat in range(1 if args.growth_only else REPEATS):
                case = output / f'n{n}-r{repeat+1}'; case.mkdir(); workspace = case / 'workspace'; workspace.mkdir()
                source = case / 'input.parquet'; replacement = case / 'replacement.parquet'
                shutil.copy2(frozen / f'input-{n}.parquet', source); shutil.copy2(frozen / f'replacement-{n}.parquet', replacement)
                before_rows = population(n); after_rows = [r for r in before_rows if r[0] != 1] + [(1, 'fixed'), (1, None)]
                metrics = {}; observations = []
                def phases():
                    start = time.monotonic(); value = phase_snapshot(workspace)
                    observations.append(dict(kind='native-phase-snapshot', seconds=time.monotonic()-start, records=len(value)))
                    return value
                def command(label, *parts, paused=False, json_output=True, workspace_override=None, limits_override=None):
                    current_workspace = workspace_override or workspace
                    current_limits = limits_override or output / 'limits.json'
                    scratch_prefix = str(current_workspace.relative_to(case) / 'scratch')
                    # Kill only the native child after its durable marker. The time
                    # wrapper stays alive to reap it and preserve resource evidence.
                    original = monitor.tree_rss; observed = {}
                    series_before = monitor.inventory(output)
                    prior_other = sum(v['bytes'] for name,v in series_before.items() if not name.startswith(case.name + '/'))
                    prior_other_allocated = sum(v['allocated_bytes'] for name,v in series_before.items() if not name.startswith(case.name + '/'))
                    def rss(pid):
                        value = original(pid)
                        if paused and not observed and (case / f'{label}.stderr.log').exists() and 'DOCSPEC_TEST_PAUSED' in (case / f'{label}.stderr.log').read_text():
                            children = [child for child in psutil.Process(pid).children(recursive=True)
                                        if child.exe() == str(frozen / 'docspec')]
                            assert len(children) == 1, 'expected one owned native process'
                            observed.update(marker_detected_monotonic=time.monotonic(),
                                            native_pid=children[0].pid,
                                            native_created=children[0].create_time())
                            children[0].kill()
                        return value
                    monitor.tree_rss = rss
                    argv = [str(frozen / 'docspec'), '--workspace', str(current_workspace), '--limits', str(current_limits), *map(str, parts)]
                    try:
                        answer, measured = monitor.command(case, case, label, argv, 1 << 30, limits=LIMITS, scratch_prefixes=(scratch_prefix,),
                            timeout_seconds=120, deadline_monotonic=deadline, json_output=json_output)
                        assert not paused, 'pause did not occur'
                    except RuntimeError:
                        if not paused or not observed: raise
                        measured = json.loads((case / f'{label}.metrics.json').read_text())
                        assert measured['stop_reason'] is None and measured['exit_code'] != 0
                        save(case / f'{label}.interruption.json', observed)
                        answer = None
                    finally: monitor.tree_rss = original
                    measured['known_work_events'] = work((case / f'{label}.stderr.log').read_text())
                    measured['sampled_peak_series_logical_bytes'] = prior_other + measured['sampled_peak_workspace_bytes']
                    measured['sampled_peak_series_allocated_bytes'] = prior_other_allocated + measured['sampled_peak_allocated_bytes']
                    measured['retained_workspace_categories'] = monitor.totals(monitor.inventory(current_workspace))
                    scratch_samples = [json.loads(line).get('workspace', {}).get('scratch', {}).get('allocated_bytes', 0) for line in (case / f'{label}.samples.jsonl').read_text().splitlines()]
                    measured['sampled_peak_scratch_allocated_bytes'] = max(scratch_samples + [measured['after'].get('scratch', {}).get('allocated_bytes', 0)])
                    case_files = monitor.inventory(case)
                    save(case / f'{label}.case-inventory.json', case_files)
                    measured['retained_case_logical_bytes'] = sum(v['bytes'] for v in case_files.values())
                    measured['retained_case_allocated_bytes'] = sum(v['allocated_bytes'] for v in case_files.values())
                    assert sum(v['bytes'] for v in monitor.inventory(output).values()) <= 1 << 30, 'whole series disk ceiling'
                    metrics[label] = measured
                    return answer
                def request(name, value):
                    path = case / f'{name}.request.json'; save(path, value); return path
                def readcheck(label, release_path, expected, combined=False):
                    arrow = case / f'{label}.arrow'
                    command(label, 'read-combined' if combined else 'read', release_path, arrow, json_output=False)
                    oracle_start = time.monotonic()
                    with pa.memory_map(str(arrow), 'r') as mapped: actual = ipc.open_stream(mapped).read_all()
                    assert counter(actual.to_pylist()) == counter(expected), 'literal multiset/null oracle mismatch'
                    assert actual.schema.field('id').type == pa.int64()
                    assert actual.schema.field('text').type in (pa.string(), pa.large_string())
                    assert actual.schema.field('id').nullable and actual.schema.field('text').nullable
                    assert set(actual.schema.names) == ({'id','text','text_length'} if combined else {'id','text'})
                    if combined: assert actual.schema.field('text_length').type in (pa.int32(), pa.int64())
                    observations.append(dict(kind=label+'-literal-oracle', seconds=time.monotonic()-oracle_start))
                    save(case / f'{label}.oracle.json', dict(rows=len(actual), exact_multiset=True, schema=str(actual.schema)))
                if args.growth_only:
                    axes = growth_profile(case, frozen, prepared, command, request, observations)
                    result = dict(profile='fixed4096-growth', metrics=metrics, axes=axes,
                        independent_observation_overhead=observations, retained=monitor.totals(monitor.inventory(case)))
                    save(case / 'result.json', result); results.append(result)
                    continue
                fresh = dict(dataset='cost', operation='fresh', input=str(source), expected_digest=prepared['assets'][f'input-{n}.parquet']['blake3'], sql='SELECT * FROM input', definition='v1')
                first = command('fresh', 'run', request('fresh', fresh))
                unchanged = command('unchanged', 'run', request('unchanged', dict(fresh, operation='unchanged')))
                assert first['dataset']['groups'] == unchanged['dataset']['groups']
                assert first['dataset']['inputs'] == unchanged['dataset']['inputs']
                stable = phases()
                retried = command('unchanged-completed-retry', 'run', case / 'unchanged.request.json')
                assert retried == unchanged and phases() == stable
                assert not metrics['unchanged-completed-retry']['known_work_events']
                readcheck('unchanged-read', case / 'unchanged.stdout.json', [dict(id=i, text=t) for i,t in before_rows])
                mutation = dict(dataset='cost', operation='updated', definition='v1', base='unchanged', keys=['id'],
                    replacements=dict(path=str(replacement), digest=prepared['assets'][f'replacement-{n}.parquet']['blake3']), deletions=None)
                update_path = request('update', mutation)
                command('update-interrupted', 'pause-after', update_path, 'processing', '--kind', 'update', paused=True)
                assert 'processing' in metrics['update-interrupted']['known_work_events']
                saved = phases(); save(case / 'update.handoff.json', saved)
                assert any(json.loads(v['json']).get('operation') == 'updated' and k.endswith('/processing') for k,v in saved.items() if k.startswith('phase/'))
                source.unlink(); replacement.unlink()
                updated = command('update-resume', 'update', update_path)
                resumed = phases(); assert all(resumed[k] == v for k,v in saved.items())
                assert 'processing' not in metrics['update-resume']['known_work_events']
                readcheck('update-read', case / 'update-resume.stdout.json', [dict(id=i, text=t) for i,t in after_rows])
                scalar = dict(dataset='cost', operation='lengths', base='updated', definition='v1', group='length', group_definition='length-v1',
                    fields=[dict(id=10, name='text_length', expression='length(text)')])
                scalar_path = request('scalar', scalar)
                command('scalar-interrupted', 'pause-after', scalar_path, 'processing', '--kind', 'backfill', paused=True)
                assert 'backfill-file' in metrics['scalar-interrupted']['known_work_events']
                saved = phases(); save(case / 'scalar.handoff.json', saved)
                completed = command('scalar-resume', 'backfill', scalar_path)
                resumed = phases(); assert all(resumed[k] == v for k,v in saved.items())
                assert not {'backfill-file', 'backfill-assembly'}.intersection(metrics['scalar-resume']['known_work_events'])
                expected = [dict(id=i, text=t, text_length=None if t is None else len(t)) for i,t in after_rows]
                readcheck('scalar-read', case / 'scalar-resume.stdout.json', expected, combined=True)
                snapshot = monitor.inventory(workspace); save(case / 'final.inventory.json', snapshot)
                save(case / 'final.phases.json', resumed)
                result = dict(population=n, repeat=repeat+1, metrics=metrics, independent_observation_overhead=observations, retained=monitor.totals(snapshot),
                    selected_pins=completed['dataset']['groups'], prior_pins=dict(initial=first['dataset']['groups'], updated=updated['dataset']['groups']),
                    selected_source_object_bytes=sum(o['bytes'] for o in completed['dataset']['groups']['source']['objects']),
                    selected_source_parquet_bytes=sum(o['bytes'] for o in completed['dataset']['groups']['source']['objects'] if o['path'].endswith('.parquet')),
                    input_bytes=prepared['assets'][f'input-{n}.parquet']['bytes'],
                    complete_update_seconds=sum(metrics[s]['elapsed_seconds'] for s in ['update-interrupted','update-resume','update-read']),
                    complete_scalar_seconds=sum(metrics[s]['elapsed_seconds'] for s in ['scalar-interrupted','scalar-resume','scalar-read']))
                save(case / 'result.json', result); results.append(result)
    except BaseException as error:
        failure = repr(error); raise
    finally:
        inventory = monitor.inventory(output)
        save(output / 'completion.json', dict(failure=failure, completed_cases=len(results), results=results,
            final_series_logical_bytes=sum(v['bytes'] for v in inventory.values()),
            final_series_allocated_bytes=sum(v['allocated_bytes'] for v in inventory.values())))

if __name__ == '__main__': main()
