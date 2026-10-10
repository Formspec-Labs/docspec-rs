# Full local Comments acceptance: evidence review

**Verdict: APPROVE the defined local acceptance claim. No blocking evidence finding.** The frozen workload completed in three fresh workspaces, including exact independent readback, within its stated time and observed resource allowances. This qualifies the specified local Comments flow; it does not complete the broader DocSpec plan.

Reviewed 2026-10-10 at harness commit `582ea804d0be273037092459ad72b2ab69453532`. This independent, read-only semi-formal audit reconciled retained JSON receipts, command arguments, inventories, raw resource samples and frozen source text. It ran no tests, builds, benchmarks or payload scans, and did not rehash binaries/data or rerun the oracle.

Below, **S** denotes [the retained series](../../workspaces/qualification/comments-e2e-oracle-threads4-20261010/20261010T105814-acceptance-f6d5b63f). Receipt references identify exact JSON keys; frozen code/protocol references identify lines within S.

## Results and bounds

| Fresh run | Full elapsed seconds | Oracle seconds, included | Sampled aggregate RSS, GiB | Sampled combined scratch, GiB | Retained run, GiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| `run-01` | 311.61 | 180.92 | 21.47 | 30.12 | 9.864 |
| `run-02` | 346.31 | 216.00 | 21.53 | 29.00 | 9.869 |
| `run-03` | 392.03 | 261.26 | 20.58 | 26.55 | 9.869 |
| Declared bound | 1,800 | Part of full deadline | 24 | 32 | 12 |

Sources: `S/run-01/result.json`, `S/run-02/result.json`, `S/run-03/result.json`: `elapsed_seconds`, `stages.oracle.elapsed_seconds`, `sampled_peak_tree_rss_bytes`, `sampled_peak_scratch_bytes`, `retained_run.total.bytes`. Each result has `passed: true`, `failure: null`; all expected stages have exit zero and no stop reason. `S/results.json.runs` equals those individual records, and its `full_local_performance_acceptance_passed` is true. The largest separately reported child-process RSS is 21.85 GiB, also below the bound.

## Evidence trace

| Question / invariant | Finding and evidence |
| --- | --- |
| Was the decision frozen before collection? | **CONFIRMED.** `S/frozen/protocol.md:17-23`, `:135-140`, `:209-224` requires three complete fresh runs and records the four-thread oracle correction without changing equality or resource limits. The frozen protocol and oracle match the reviewed adopted files. `S/environment.json` records acceptance mode, twelve engine threads, four oracle threads and 18 GiB DuckDB memory. |
| Did the run use the recorded executable and checker? | **CONFIRMED from retained identities.** `S/execution-capture.json.complete` is true. Its binary size and SHA-256 match the successful `S/frozen/build-receipt.json.binary`: `3b75f246bc282e60a3fda713b14830f3c16ce9dbe6b5c3afb9ecc621f05a731b`, from Rust commit `e40be018b2a1c12c2e1067ba4fe552de3a6711f6`. Environment digests match the capture; captured files are recorded read-only. Every engine and oracle `*.argv.json` points to that series' frozen executable/checker. The harness commit and engine build commit are explicitly distinct. |
| Were these genuinely fresh application workspaces? | **CONFIRMED by metadata.** Each `comments.before.json` contains no files under that run's workspace before its first engine stage. The workspace directories are not symlinks; all native file sets are disjoint and confined to their respective workspace. Admitted-byte receipts point to separate retained copies. Inputs and limits match the series records in every run. Shared `initial` operation/version labels do not imply shared storage. Creation uses new directories (`S/frozen/run.py:240-247`). OS caches were not cleared. |
| Did the whole required flow run? | **CONFIRMED.** Stage receipts cover source admission/shaping, generation seal, join/evidence, summary/evidence, both exports, native/export selections and oracle. Every selected-output JSON equals its corresponding produced release/export JSON. This checks current selection in each fresh run, unlike the earlier diagnostic (`S/frozen/run.py:271-290`; per-run `*-selected.stdout.json`). |
| Was equality complete and independent? | **CONFIRMED by the retained oracle results and reviewed checker.** Every `oracle.stdout.json` reports exact readback, 10,000,000 Comments, 9,988,138 matched and 11,862 unmatched. Check receipts cover admitted byte identities, native membership/types, all shaped values, complete nested Dockets/receipts, exact joined values, both contributor relations, summaries and both flat outputs. Admitted identities match the frozen inputs. The checker retains schema/field-ID checks and signed full-value multiset comparison, with no sample/hash substitution (`S/frozen/oracle.py:81-106`, `:134-226`, `:242-346`). |
| Do resource observations support the gate? | **CONFIRMED within its declared observation limits.** Per-stage metrics equal those embedded in run results. RSS/scratch maxima reconcile with retained `*.samples.jsonl`. The monitor includes the runner and descendants, counts every engine/oracle scratch root across the series, and records free space (`S/frozen/run.py:248-262`; `S/frozen/comments-input-monitor.py:104-228`). Short-lived peaks may escape sampling; this is not continuous enforcement proof. |

## Storage and interpretation

Preflight recorded 85,550,198,784 free bytes against a 75,161,927,680-byte requirement for all retained runs, scratch and reserve (`S/environment.json:free_bytes_before,required_free_bytes`). Maximum per-run additional logical storage was 42,934,349,062 bytes, below the 12 GiB retention plus 32 GiB scratch allowance. The series' largest sampled logical/allocated totals were 60,474,199,604 / 60,598,235,136 bytes while prior fresh runs coexisted with the active run. Final retained series storage was 31,967,025,322 logical and 31,970,017,280 allocated bytes; per-run retention was about 3.86 times the frozen 2,744,544,515-byte input total. Minimum observed volume free space was 19,265,196,032 bytes, above the 2 GiB reserve. Sources: per-run `result.json` storage keys and `S/results.json.retained_series`.

These are different measurements: logical bytes, allocated blocks and volume free space are not interchangeable. APFS allocation does not prove unique physical extents. Read/request-byte counters remain unknown. Timings apply to this shared workstation and frozen input with uncleared OS caches; increasing elapsed times across these runs do not establish an isolated regression or its cause (`S/environment.json`; `S/frozen/protocol.md:103-133`).

The prior `acceptance-20261010.json`, `acceptance-native-statistics-20261010.json` and `acceptance-export-layout-20261010.json` still record failed series. This new pass does not reclassify them. The separate four-thread retained-output diagnostic remains diagnostic evidence and supplies none of these fresh runs.

Scope follows `S/frozen/protocol.md:3-6`, `:25-57`: this is the defined local initial flow over the systematically selected retained population, not statistical representativeness of all agencies. It preserves whole-input Comments provenance, native nested docket receipts bound by generation, and local contributor evidence. It does not establish unavailable original Comments receipt/attachment/source-body custody or per-docket receipt decoding. Remote publication/readback, R2/Basin, acquisition, maintenance/restore, and separate unchanged/update/backfill/interrupted cases remain outside this series. Broader lifecycle/provider qualification remains unfinished under `docs/PLAN.md:152-158` and `docs/requirements/07-operations-qualification.md:149-163`.

**Conclusion:** the retained evidence supports the current local 10-million-Comments acceptance result with high confidence. Keep the exact workload, execution identities, observation limits and exclusions attached to that claim.
