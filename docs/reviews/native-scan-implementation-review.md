# Native scan implementation review

Review date: 2026-10-10. Scope: the working Rust diff against `775fd75`, including the new `src/engine/refresh/plan_probe.rs`. This is an independent static review under the Rust and semi-formal code-review skills. The reviewer ran no builds, tests, benchmarks, or payload reads and changed only this report. The author owns execution and its receipts. Design and author research notes were not used to derive the findings.

Latest static verdict: **APPROVE** after the narrow correction recheck recorded at the end of this report. The original findings and checkpoint verdict below remain as review history.

## Findings

### F1 — Avoid an inventory walk for every single-file provider

- Severity: **WARNING — changes requested**
- Category: performance
- Location: `src/query.rs:110-115`
- Evidence: `projected_files` filters the complete `scan.files()` iterator even when the request names one file. `OccurrenceScan::files` enumerates the values of an existing path-keyed `BTreeMap` (`src/storage/occurrences.rs:296-302`). `Engine::replace_keys` calls `query::file` inside its file loop and again for each changed file (`src/engine/write.rs:455-484`). The patch therefore adds O(F²) path comparisons over a source with F files. It adds no payload scan, but it is unnecessary repeated metadata work on the incremental path.
- Correction: obtain one file's row count through the existing path-keyed map; retain the checked sum for the all-files provider. The author has acknowledged this finding; correction is pending at this review checkpoint.

### F2 — Bind the diagnostic to the recorded manifest bytes

- Severity: **WARNING — changes requested**
- Category: correctness of diagnostic evidence
- Location: `src/engine/refresh/plan_probe.rs:41-48`
- Evidence: the diagnostic reads `version.manifest.path`, then compares dataset, version and owner identifiers. Its `read_json` checks current file length against the allocation limit, but never compares the recorded `ObjectRef.bytes` or digest (`src/engine/refresh/plan_probe.rs:79-94`). A modified manifest can preserve those identifiers while changing the source pin or row hint. `storage::open_pin` validates the native metadata named by that substituted pin, not the enclosing manifest (`src/storage.rs:677-695`). Production refresh already verifies manifest identity through `inputs::read_manifest` (`src/engine/refresh.rs:480-500`; `src/engine/inputs.rs:399-440`).
- Correction: reuse the maintained bounded manifest reader, or compare the already-read bytes to the recorded size and digest before decoding. Do not add a second file read. This does not identify a production query-result defect; it limits what the diagnostic currently proves about the retained request.

### F3 — The optimizer guard has a justified execution-cost tradeoff

- Severity: **OBSERVATION**
- Category: correctness / performance
- Location: `src/budget.rs:100-127`
- Evidence: removing `aggregate_statistics` keeps aggregate values dependent on execution. This also removes otherwise valid metadata-only aggregate rewrites for maintained flat-file providers in these sessions. The pinned implementation can promote an inexact filtered estimate to exact zero after an offset, so advertising source counts as inexact alone is insufficient. The regression and pinned-source trace below justify this guard. No throughput or memory reduction is established by the code change alone.
- Next step: retain the guard and its regression until the pinned upstream path can pass without it. Qualify the complete Comments workload separately.

## Patch summary

The patch replaces `StreamingTable` with a thin `TableProvider` plus DataFusion `DataSourceExec`. The source exposes selected row counts to physical planning while the existing native reader continues to supply values, defaults, field-ID interpretation and stable occurrence identities. It uses maintained `ProjectionExec` and `GlobalLimitExec`; it does not add a query evaluator, join implementation, or custom optimizer rule.

Every changed production registration passes the row count from the same `TablePin` opened for that registration. An unpinned schema-evolution test passes `None`. Captured-file registrations derive their counts from captured `DataFile` descriptors. All are reported as `Precision::Inexact`; unknown or unrepresentable counts become absent, and decoded byte size remains unknown (`src/query.rs:42-64,86-126,229-242`). The count is a planning hint, never a replacement for reading or verification.

`Budget::context` preserves the shared runtime, batch-row setting, target partitions, default functions and existing null-safe-join guard, then removes one known-unsafe statistics rewrite. `join_queries` extracts existing SQL generation so the diagnostic can create physical plans without executing the cardinality probes or main join (`src/engine/refresh.rs:532-686`). Production `join_plan` still executes required cardinality checks before returning the main DataFrame.

## Function trace

| Function or method | File:line | Inputs → output | Verified behavior and caller |
| --- | --- | --- | --- |
| `table`, `occurrences`, `provider` | `src/query.rs:26,34,42` | Native table, shared budget, optional selected rows → provider | Chooses existing source schema interpretation and captures the table without opening payloads. Production callers below supply the matching pin count. |
| `file`, `files`, `projected_files` | `src/query.rs:68,78,86` | Captured scan, optional path and root-column list → provider | Checks named file membership, resolves projection, captures a row hint. `replace_keys` calls the single-file form; evidence association calls all-files (`src/engine/write.rs:455-484`; `src/engine/evidence.rs:816-825`). F1 applies. |
| `NativeTable::{as_any,schema,table_type,scan}` | `src/query.rs:133-174` | Session projection and limit → physical plan | Reuses `DataSourceExec`, a maintained projection, and a maintained limit. Uses the trait's unsupported-filter default. No reader is opened. |
| `NativePartition::{open,as_any,fmt_as,output_partitioning,eq_properties}` | `src/query.rs:199-227` | Partition and task context → cooperative stream / plan properties | Refuses nonzero partitions, exposes one bounded source partition, preserves schema, and delegates to the existing lazy stream. |
| `NativePartition::{partition_statistics,with_fetch,fetch,try_swapping_with_projection}` | `src/query.rs:229-258` | Optional partition → inexact row statistics / unsupported pushdown | Invalid partition refuses; no byte estimate or exact count is invented. The source declines fetch and projection absorption. |
| `NativePartition::execute` | `src/query.rs:266-328` | Captured table or files → stream | Lazily selects `read_batches`, `read_occurrences`, or `read_projected_file`; all-files reads remain sequential. Reader and metadata reservations stay owned by the stream. |
| `Budget::context` | `src/budget.rs:100` | Shared runtime and limits → SQL session | Preserves default features and all default physical rules except `aggregate_statistics`. `Engine::context` delegates here (`src/engine.rs:245-247`). |
| `Engine::compare_admitted` | `src/engine.rs:556` | Before/after selected group pins → comparison | Copies each pin count before shadowing the pin with its opened table; the duplicate-aware comparison SQL is unchanged (`:562-611,634-647`). |
| `aggregation::register_input`, `Engine::process_aggregation` | `src/engine/aggregation.rs:65,184` | Exact input release / newly written affected-key pin → registered source | Version equality and row-limit checks remain; each registration uses its own opened pin (`:71-84,201-225`). |
| `combined_dataset` | `src/engine/backfill.rs:181` | Selected dataset groups → combined stream | Opens source and each computed group from the same dataset whose counts are passed (`:185-210`). Managed reads replace representation pins before this function (`:148-159`). |
| `verify_group_data` | `src/engine/backfill/verification.rs:482` | Source and computed group pins → validation | Matching counts accompany existing source/actual providers (`:580-587`). Full identity/value comparison remains in place (`:577-628`). |
| `verify_native_changes` | `src/engine/changes.rs:79` | Before, after, removed, added pins → validation | All registrations use the loop's exact pin; deep verification still runs the signed aggregate over each relation (`:150-200`). |
| `Engine::read_source_occurrences` | `src/engine/evidence.rs:245` | Admitted release → occurrence stream | Both table and count use the resolved representation pin, not an older canonical count (`:252-270`). |
| `Engine::inherit_receipts` | `src/engine/evidence.rs:383` | Previous receipts and actual removed-source pin → prepared receipts | Registers each table with its own rows; the zero-row replacement and removal scope are unchanged (`:428-455`). |
| `evidence::verify_result`, `verify_inheritance` | `src/engine/evidence.rs:622,705` | Evidence pins → validation | Exact source objects and their counts are paired; association and inheritance checks remain (`:682-697,710-734`). |
| `Engine::process_incremental_join` | `src/engine/incremental.rs:118` | Old/new inputs and actual written key tables → replacement | Counts follow the written discovery pin, each old/new source, the written union key table, and the selected prior output (`:140-238`). |
| `Engine::incremental_evidence` | `src/engine/incremental.rs:457` | Actual removed/added pins → evidence | Registers the same loop pin whose rows it passes (`:487-495`). |
| `Engine::prepare_evidence` | `src/engine/refresh.rs:134` | Processing phase table → contributor computation | Introduces one borrowed local pin used both by `open_pin` and its row hint (`:168-181`). |
| `join_frame`, `join_registered`, `join_plan`, `join_queries` | `src/engine/refresh.rs:503,525,532,549` | Qualified join definition and exact releases → SQL probes / main DataFrame | Version equality remains; key types, names, null policy and projected occurrence columns remain unchanged. The same required cardinality SQL executes before output (`:510-546,568-686`). |
| `verify_related_with_inputs` | `src/engine/refresh.rs:699` | Selected source and contributor pins → validation | Registers actual/evidence with matching rows; preserves schema checks and deep multiset reconstruction (`:721-783`). |
| `retained_comments_join_physical_plans`, `read_json`, `print_nodes` | `src/engine/refresh/plan_probe.rs:17,79,97` | Retained request metadata → printed SQL and physical plans | Creates plans and prints node statistics; never collects or executes them. Metadata JSON has a bounded read; F2 limits exact manifest provenance. |
| `native_current_schema_projection_reorders_equal_types_by_id` | `src/storage/occurrences/tests/schema.rs:210` | Metadata-only schema candidate → comparison | Passes `None` because the candidate is not a selected pin; compares query output with the existing native ID-aware reader (`:272-305`). |

## Data flow and invariants

1. **Counts stay attached to the selected representation.** `storage::pin` checks supplied prepared rows against a checked sum of live native record counts when a pin is created (`src/storage.rs:584-606`). `open_pin` binds native snapshot and schema IDs (`:677-695`). All reviewed production callsites supply that same pin's rows; no caller uses dataset-wide rows for a smaller changed-key or evidence table. Conversion to `usize` and captured-count addition fail closed to unknown statistics on overflow (`src/query.rs:62,113-115`).
2. **Statistics do not become values.** `partition_statistics` creates fresh unknown statistics and adds only an inexact row count (`src/query.rs:238-242`). The known aggregate rewrite is disabled in production contexts (`src/budget.rs:100-127`). Native count/byte/read failures therefore still flow from executed readers through `DataFusionError::External` (`src/query.rs:269-332`).
3. **Planning stays separate from payload access.** Provider construction reads in-memory schema and captured descriptors. `scan` only creates physical nodes. Reader opening occurs in the future inside `execute`, which is first polled by execution (`src/query.rs:146-173,266-327`). The plan diagnostic stops at `create_physical_plan` (`src/engine/refresh/plan_probe.rs:64-75`). Opening a retained native pin does read/hash its metadata file; this is not a payload-free-all-I/O claim (`src/storage.rs:677-686`).
4. **Unsupported predicates remain above the reader.** The pinned `TableProvider` default returns `Unsupported`, and its documented scan flow supplies no pushed filters in that case (dependency references below). The physical `DataSource` default likewise returns `PushedDown::No`. Ignoring `_filters` therefore does not drop a supported filtering obligation. An upstream filter retains the columns it needs; the reader's own source projection remains unchanged.
5. **Projection and limit preserve their established order.** Scan projection maps positions against the provider schema, including reordered roots and internal occurrence columns. `ProjectionExec` owns selection and output cardinality; `GlobalLimitExec` owns limit semantics. With one source partition the global limit's input requirement is satisfied (`src/query.rs:155-172,221-223`). The source declines further fetch/projection absorption (`:245-257`), so the change does not claim native column pruning for arbitrary queries.
6. **Read interpretation and custody stay in existing components.** Native schema planning still selects current-schema field IDs and adds/removes the virtual file column to force ID-aware interpretation (`src/storage/schema.rs:78-103,106-133`). Located reads still materialize stable origins, check decoded file counts, and retain reservations (`src/storage/occurrences.rs:93-130,133-203,387-423`). Whole-file count completion is established at EOF, not by a limited query. No new detached task or buffering queue is introduced.
7. **Cardinality checks keep their correctness boundary.** Many-to-one still probes matching right keys; one-to-one also probes matching left keys; many-to-many has no probe. These existing checks precede writing a possible fanout result (`src/engine/refresh.rs:537-546,642-678`). The patch does not add another processing scan. Retaining the preflight scan is a deliberate correctness boundary, not evidence of complete-path efficiency.

## Pinned dependency trace

Sources below are the installed sources selected by `Cargo.lock` (DataFusion `53.1.0`; Iceberg `0.10.1`), under `/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`.

| Dependency source | Verified consequence |
| --- | --- |
| `datafusion-catalog-53.1.0/src/table.rs:107-114,155-172,285-292` | Unsupported filters are not delegated to `scan`; the default implementation returns unsupported for every expression. |
| `datafusion-datasource-53.1.0/src/source.rs:181-189,298-314,321-322,449-456` | The physical source rejects filter pushdown by default; maintained execution opens the source and splits batches; source statistics are returned directly; source properties are bounded. |
| `datafusion-physical-optimizer-53.1.0/src/join_selection.rs:59-82,85-102,258-313` | Join choice uses byte counts when both are known, otherwise row values including inexact counts. It uses the hints for choice of side/mode, not row production. |
| `datafusion-common-53.1.0/src/stats.rs:452-496` | The offset calculation treats an inexact estimate as an upper bound and returns `Exact(0)` when it is at or below the skip. This is the upstream behavior requiring the guard. |
| `datafusion-physical-plan-53.1.0/src/limit.rs:193-230` | Global limit executes the child stream and computes statistics through the preceding `with_fetch` path. |
| `datafusion-physical-optimizer-53.1.0/src/aggregate_statistics.rs:52-87,100-102` | The named rule can replace the aggregate's entire input with a constant projection when statistics supply every result. |
| `datafusion-physical-optimizer-53.1.0/src/optimizer.rs:91-127` | The default rule list includes that aggregate rewrite and the maintained join-selection rule; the patch preserves order while removing only the former. |
| `datafusion-physical-optimizer-53.1.0/src/enforce_distribution.rs:1106-1125` | Another relevant consumer uses exact/inexact row statistics to decide parallelism; it does not replace data with estimated values. |
| `datafusion-53.1.0/src/execution/context/mod.rs:355-361`; `src/execution/session_state.rs:1104-1137,1231-1237` | The new session construction reproduces the former constructor's config/runtime/default-features setup and then supplies the filtered maintained optimizer list. |

## Test behavior and edge cases

These expectations come from reading the tests and their implementation paths. This reviewer did not execute them. The focused log `docs/reviews/native-statistics-corrected-check.log` records a passing author-run query suite; the full gate was still running at this checkpoint. Later assertions and review corrections need their own applicable execution evidence.

| Test or scenario | Evidence | Expected result and limit |
| --- | --- | --- |
| Stream admission, competition, cancellation and batch rows | `src/query.rs:436-475` | Existing reader retains admission until stream drop, refuses a competing reader, and permits retry. This test calls the inner partition stream directly; wrapper lifetime is also checked by static ownership tracing. |
| Retained pin, duplicate/null filter, renamed projection, empty result schema | `src/query.rs:478-533` | Repeated reads preserve duplicate values and a null; empty output preserves its typed field. Exercises the new provider through SQL. |
| Oversized batch for values and `COUNT(*)` | `src/query.rs:535-560` | Both queries must reach the native reader and return its `oversized_batch` refusal; capacity becomes reusable. The count assertion prevents direct metadata substitution. |
| Filter, offset and count, with/without native hint and with flat Parquet | `src/query.rs:562-604` | Seven input rows include one null; the predicate retains six and offset removes two, so the result is four. `Budget::context` must preserve this result for both provider paths. |
| Smaller join build from both SQL directions | `src/query.rs:607-654` | Inspects maintained `HashJoinExec` children and requires inexact counts `(2,7)` in build/probe order. This is plan evidence, not a throughput measurement. |
| Same-type root reorder | `src/storage/occurrences/tests/schema.rs:210-305` | `SELECT b,a` matches the ID-aware native reader, including nulls and exact values; the unpinned candidate uses absent statistics. |
| Narrow captured-file query with oversized excluded payload | `src/engine/write/tests.rs:478-544` | The key-only source query succeeds under a small byte allowance; the complete selected file refuses. The existing explicit source projection remains effective. |
| Join kinds, null policies, duplicates and empty inputs | `tests/related_refresh.rs:280-336,379-406` | Independent nested-loop expected values cover inner/left/full joins, both null policies, duplicate multiplicities and empty inputs. Maintained optimizer side swaps must preserve these results. |
| Cardinality refusal before output and self-join roles | `tests/related_refresh.rs:410-455` | Many-to-one and one-to-one malformed inputs refuse without native output; self-join output matches the independent oracle. SQL extraction preserves the tested path. |
| Combined computed fields and empty groups | `tests/backfill.rs:55-117` | Expected values retain duplicate/null/empty/Unicode input behavior and empty combined results. New row hints must not change these joins. |
| Unknown/mismatched statistics | `src/query.rs:62,238-242`; caller trace above | Unknown count remains absent. Counts are only planning hints; production callsites pair the same selected pin. No payload recount is added during planning. |
| Captured file outside the scan | `src/query.rs:92-97`; `src/storage/occurrences.rs:318-320` | Refuses earlier than before with the existing `occurrence_tasks` code. There is no dedicated new provider test for this branch; the check is a direct map membership test. |
| Diagnostic exact manifest identity | `src/engine/refresh/plan_probe.rs:41-48,79-94` | Currently insufficient; F2 requests the missing binding. |

The author-run plan logs show the same retained source snapshot IDs and SQL, with the main join changing from a left join whose physical build is Comments to a right join whose physical build is Dockets. The cardinality probe retains its left-semi shape (`docs/reviews/native-join-plan-baseline.log:7-15,32-42`; `docs/reviews/native-join-plan-corrected.log:7-15,36-46`). Until F2 is corrected, treat these as plans for the metadata present at the recorded paths, not an independently verified manifest-identity receipt. Neither log executes the join or proves that the full workload fits the memory or time target.

## Exploration record

- H1: replacing the provider might weaken unsupported-filter, projection or limit behavior. **Refined:** both logical and physical defaults reject pushdown, and maintained operators implement the relational behavior. Pinned statistics promotion required a separate session guard; its implementation matches the identified rule.
- H2: callers might attach whole-source counts to a changed subset or older representation. **Refuted for reviewed production calls:** each count is paired with the same opened pin; captured counts come from the same `OccurrenceScan`. F2 remains a diagnostic identity gap.
- H3: metadata-only planning might accidentally open readers or add repeated work. **Partly confirmed:** no planning payload read was found, but single-file provider construction adds the repeated inventory walk in F1.
- H4: the SQL extraction might change cardinality-check order or occurrence meaning. **Refuted:** probes retain their SQL and order; production executes them before returning the main frame; source identity handling remains in the existing reader.

## Conclusion

**VERDICT: REQUEST CHANGES** at this checkpoint, for F1 and F2.

The physical adapter gives maintained join planning the intended source row information while preserving the existing native reader and verification paths. The two requested corrections are bounded: eliminate repeated per-file inventory traversal and bind the diagnostic to its recorded manifest bytes. No additional production query-result defect was found in the reviewed trace.

Coverage of changed query behavior: **ADEQUATE in the inspected targeted tests and existing integration assertions; final execution gate pending**. Confidence: **HIGH for the identified findings and static data flow; no performance qualification claimed**.

## Correction recheck — 2026-10-10

This follow-up is limited to the F1 and F2 corrections. The reviewer reread their implementations and existing callees, ran no code, and found no new issue in those changes. Earlier line references describe the initial review checkpoint; references in this section describe the corrected source.

**F1 resolved.** `OccurrenceScan::file_rows` uses `self.tasks.get(path)` and returns the captured descriptor's `record_count` (`src/storage/occurrences.rs:300-303`). `projected_files` now uses that lookup once for a named path; a missing path keeps the `occurrence_tasks` refusal and a count of zero remains a present count. The all-files branch alone performs the checked sum (`src/query.rs:92-101`). Creating a provider for each file now performs O(F log F) map lookup work across the population instead of the added O(F²) enumeration. This is a structural complexity correction, not a measured timing claim. Reader execution and explicit root projection are unchanged (`src/query.rs:102-124`).

**F2 resolved.** The diagnostic now calls `refresh::read_release` for each request `VersionRef`, retaining an `Arc` to its metadata reservation and starting the cumulative size counter with the request bytes already read (`src/engine/refresh/plan_probe.rs:29-43`). `read_release` delegates to the existing `inputs::read_manifest`, validates the dataset/version/owner and supported format, and returns the verified selected dataset (`src/engine/refresh.rs:480-500`). The manifest reader charges the declared size, bounds cumulative admitted bytes, checks the file size, reads the admitted bytes once plus an overflow check, hashes that same buffer, and decodes it (`src/engine/inputs.rs:399-440`). The diagnostic therefore rejects a substituted manifest before reading its source pin. It still builds and prints plans without executing payload streams (`src/engine/refresh/plan_probe.rs:60-71`).

The parent reports that the initial full `scripts/check` completed successfully before these corrections; post-fix lint, affected tests and plan recapture are owned by the parent and were in progress at this recheck. The corrected implementation resolves the diagnostic provenance gap, but the older baseline/corrected logs do not retroactively prove that their manifests were verified; use the post-fix recapture for that evidence.

**VERDICT: APPROVE** for the corrected implementation, with no unresolved change-request findings.

Justification:

- F1 removes the unnecessary repeated inventory traversal using existing maintained state.
- F2 reuses the production manifest identity check without adding a second manifest read.
- The prior query, schema, custody and optimizer traces remain applicable; F3 remains an explicit performance tradeoff rather than a blocker.
- Coverage of changed paths: **ADEQUATE in the inspected tests and static traces; post-fix execution belongs to the parent qualification record**.
- Confidence: **HIGH for this static review. Complete-path time and memory performance remain unqualified.**
