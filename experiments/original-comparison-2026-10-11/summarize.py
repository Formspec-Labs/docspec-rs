"""Derive comparison figures from retained observations without repeating work."""
import json
from pathlib import Path
import statistics

HERE=Path(__file__).resolve().parent

def spread(values):
    return {'median':statistics.median(values),'min':min(values),'max':max(values),'observations':values}

def main():
    records=json.loads((HERE/'current-results.json').read_text())
    prep=json.loads((HERE/'current-preparation.json').read_text())
    summary={'scope':'Current local release, bounded native admission/read/retry/reopen; no export, update or full10M comparison',
             'original_commit':prep['original_commit'],'rust_current_commit':prep['rust_current_commit'],'cases':{}}
    assert len(records)==12
    for case in ('bill-actions','comments'):
        item={'rows':prep['cases'][case]['rows'],'input_bytes':prep['cases'][case]['bytes'],'arms':{}}
        for arm in ('python','rust-current-release'):
            runs=[r for r in records if r['case']==case and r['arm']==arm]
            assert len(runs)==3
            assert all(r['result']['status']=='passed' and r['result']['same_selection'] and
                       r['metrics']['stop_reason'] is None and r['metrics']['exit_code']==0 and
                       len(r['result']['checks'])==2 and
                       all(c['types_and_nullability_equal'] and c['all_values_equal'] and c['rows']==item['rows']
                           for c in r['result']['checks']) for r in runs)
            values={'admit_read_seconds':[r['result']['first_admit_read_seconds'] for r in runs],
                    'whole_worker_seconds':[r['result']['worker_seconds'] for r in runs],
                    'max_observed_rss_bytes':[max(r['metrics']['sampled_peak_tree_rss_bytes'],r['metrics'].get('time_max_rss_bytes') or 0,
                                                 r['result']['peak_self_rss_bytes'],r['result']['peak_child_rss_bytes']) for r in runs],
                    'retained_engine_logical_bytes':[r['result']['retained_engine']['logical_bytes'] for r in runs],
                    'retained_engine_allocated_bytes':[r['result']['retained_engine']['allocated_bytes'] for r in runs],
                    'peak_observed_run_logical_bytes':[max(r['result']['readback_boundary_usage'],r['metrics']['sampled_peak_workspace_bytes']) for r in runs],
                    'min_volume_free_bytes':[r['metrics']['min_volume_free_bytes'] for r in runs]}
            for stage in ('admit','read','retry','reopen_read','oracle_read','oracle_reopen'):
                values[stage+'_seconds']=[r['result']['phases'][stage]['seconds'] for r in runs]
            item['arms'][arm]={k:spread(v) for k,v in values.items()}
            item['arms'][arm]['retained_input_amplification']=item['arms'][arm]['retained_engine_logical_bytes']['median']/item['input_bytes']
        before=item['arms']['python']; after=item['arms']['rust-current-release']
        item['python_divided_by_rust']={key:before[key]['median']/after[key]['median'] for key in
             ('admit_read_seconds','retry_seconds','reopen_read_seconds','whole_worker_seconds','max_observed_rss_bytes','retained_engine_logical_bytes')}
        summary['cases'][case]=item
    (HERE/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    for name,item in summary['cases'].items():
        print(name,'rows',item['rows'])
        for arm,values in item['arms'].items():
            print(arm,{k:round(v['median'],6) for k,v in values.items() if isinstance(v,dict)})
        print('ratios',item['python_divided_by_rust'])

if __name__=='__main__': main()
