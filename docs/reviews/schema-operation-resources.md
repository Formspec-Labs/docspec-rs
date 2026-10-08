# Public schema operations: resource and recovery review

Scope: uncommitted changes against `01b5033`, implementing public root-field
rename, reorder and hide operations. Static review under the semi-formal-code-review
and ms-rust guidance. The reviewer read changed implementations, callers, native
library code, tests and owner-produced logs; ran no builds or tests; and changed only
this report.

## Findings

No unresolved actionable findings remain in this scope. The identified resource
findings are closed, and the owner-run final repository gate passes, including
both default-stack regressions.

| Finding | Severity / category | Evidence and disposition |
| --- | --- | --- |
| Generated internal joins exceeded the default stack | BLOCKER / resources, closed | The original summary failure's extracted trace enters recursive `sqlparser` parsing from `write::any_rows`, through `replace_keys` and aggregate evidence. Typed DataFusion key joins remove that parser call. The next full gate exposed a second overflow in `incremental::incremental_shares_unaffected_output_and_evidence_and_adopts_exact_noop_pins`; its trace reaches generated SQL in `verify_group_data`. That comparison now also uses maintained typed expressions. Both sites are reviewed below and pass in the complete normal-stack related-refresh suite and final repository gate (`schema-operation-check.log`). The earlier hypothesis that schema transition futures alone explained the failure was not established. |
| Known name conflicts escaped pre-admission checks | WARNING / correctness, closed | The initial `check_schema` compared computed names case-sensitively, but scalar compilation uses `eq_ignore_ascii_case` (`src/engine/backfill/expressions.rs:73`). A rename to `TEXT_LENGTH` could advance admission and prepare metadata before permanently failing against computed `text_length`. `src/engine/schema.rs:66` now matches the compiler's rule; `validate` at line 151 also refuses reserved occurrence names. `tests/schema.rs:202` verifies both plan/run refusal and unchanged lifecycle state. |
| Transition comparison cloned complete object inventories before admission | WARNING / resources, closed | `verify_transition` now reserves before constructing the two inventory maps (`src/engine/schema.rs:256`) and stores borrowed paths/ObjectRefs (`:257`). It avoids cloning every path, owner and digest. The same reservation moves into the blocking comparison worker at line 275. |
| Redundant owned native metadata copies | OBSERVATION / efficiency, closed | Preparation and verification now use Iceberg's maintained `Table::metadata_ref()` (`src/engine/schema.rs:121`, `:270`) before the one builder copy. The owner diagnostic measured transition future size changing from 1,728 to 528 bytes and availability from 3,056 to 1,856 bytes. This removes redundant clones; it did not by itself fix the stack failure and does not measure process memory. |
| Native equality needs a generated-clock adjustment | OBSERVATION / maintainability, accepted | Iceberg 0.10.1's `TableMetadataBuilder::build` sets its private `last_updated_ms` (`src/spec/table_metadata_builder.rs:1133`); its public API has no setter. The implementation generates expected UUID/location through the builder, normalizes only the timestamp through bounded serialization, then uses native `Eq` (`src/engine/schema.rs:280`). This checks private references and unordered maps without a project-owned metadata comparison model. No simpler complete supported primitive was found in the pinned source. |
| Metadata-only writes still perform broad reads | OBSERVATION / performance | `process_schema` uses the existing `storage::pin`, which hashes inherited objects (`src/storage.rs:481`). Validation/readback decode values (`src/engine.rs:1457`), and selected scalar groups retain full verification. File reuse establishes avoided payload writes, not population-independent latency or an end-to-end speed result. README and STATUS state this limit. |

## Patch summary

`SchemaSpec` (`src/model.rs:473`) names the exact base, expected writer definition,
next writer definition and ordered existing native root IDs/names. It cannot add
fields or alter types/defaults. `OperationRequest::result_definition`
(`src/model.rs:581`) separates the definition required while preparing from the
definition selected with the result. Format 10 records this distinction.

The feature uses the existing run service, phase records, protected directories,
scalar preparation, release readback and single lifecycle selection. Its native
candidate retains snapshots, schemas, field allocation history and value objects;
the candidate receives a fresh table identity and location. CLI and HTTP expose
the same engine entry points. There is no new journal, schema registry, scheduler,
storage backend or query framework.

## Function trace

| Function / location | Inputs → outputs | Verified behavior and important calls |
| --- | --- | --- |
| `plan_schema`, `evolve_schema`, `schema_until`, `src/engine/schema.rs:12` | `SchemaSpec` → plan, retained release or test interruption | Thin wrappers over `plan_request`/`run_internal`; no transport-specific lifecycle logic. |
| `validate`, `src/engine/schema.rs:151` | Declared edit → acceptance/refusal | Requires distinct existing-ID declarations, nonempty names/order, different nonempty writer definitions and a non-self base. Reserved occurrence names refuse before admission. |
| `fields`, `src/engine/schema.rs:184` | Existing native schema and edit → native proposed schema | Copies only selected root fields, changes their names, preserves their native IDs/types/defaults/nested definitions, and uses the maintained builder to validate identifier fields. Unknown or nested IDs cannot be selected as roots. |
| `check_schema`, `src/engine/schema.rs:36` | Selected authority and request → pre-admission qualification | Called from planning and `prepare_and_select` before boundary advancement (`src/engine.rs:186`, `:606`). Refuses unresolved earlier source admission, removing a consumed field and source/computed name collisions. Related-source exclusion uses the existing `refresh::check_rebuild` (`src/engine/refresh.rs:345`). |
| `admit_schema`, `src/engine/schema.rs:82` | Exact request/operation → retained admission phase | Reconciles the captured manifest against `release`, retains that manifest as phase output, and uses existing `complete`; no external payload copy. |
| `process_schema`, `src/engine/schema.rs:109` and `evolve`, `:212` | Captured base and protected directory → prepared source pin | Reuses `register_output` (`src/engine/write.rs:66`), `register_candidate` (`src/storage/tables.rs:77`) and `storage::pin` (`src/storage.rs:439`). Native `add_current_schema` preserves allocation history and retained snapshots. Shared references keep original owners. |
| `captured_manifest`, `src/engine.rs:1102`; `fingerprint`, `:1249`; `phase_key`, `:1111` | Authority/request → exact captured pin and reusable phase identity | Schema requests bind base, definitions and field order in their material fingerprint. They do not use the empty-input admission key. The small shared capture helper replaces the incremental-only helper. |
| Shared run/selection, `src/engine.rs:572`; `select_with_progress`, `src/engine/history.rs:146` | Prepared source/groups → one selected release | Preserves retained inputs; refreshes scalar bindings by existing IDs. Exact base/manifest, definition and admission-boundary guards run before selection. One commit writes next definition, selected result and succession. Finished work still uses the existing epoch-only acknowledgment rule. |
| `verify_transition`, `src/engine/schema.rs:229` | Before/after pins and edit → exact permitted transition/refusal | Checks format/dataset/operation/base/definitions, inputs/relationship/eligibility, exact adjacent boundary, unchanged row count/snapshot and inherited objects. It reconstructs the native edit, uses the bounded encoder and native equality, and refuses identical old/new UUIDs. |
| `check_available`, `src/engine/schema.rs:296` | Cached schema result → required-parent/native-state check | Reuses the bounded manifest reader; called by ordinary cached readback (`src/engine.rs:1533`) and related dependency traversal (`src/engine/verification.rs:145`). Covers source-only schema results as well as enriched ones. |
| Group verification, `src/engine/backfill/verification.rs:176`, `:355` | Schema result/history → composition and value proof | Checks the declared transition and exact refreshed groups; historical scalar bindings also validate schema hops. Uses `result_definition` for selected bindings. Schema edits have no row-change table (`src/engine/changes.rs:19`). |
| Transport handlers, `src/http.rs:127`, `src/main.rs:253` | JSON/file request → shared engine result | `/schemas/plan`, `/schemas/run`, `plan-schema` and `schema` call engine methods. Test-only CLI pause adds no production decision path. |
| `key_join`, `src/engine/write.rs:531`; `any_rows`, `:559` | Two frames and complete key tuple → null-safe joined frame or bounded existence result | Replaces generated join SQL at the traced stack-failure site. Explicit literal columns and `IsNotDistinctFrom` feed maintained `join_on`; probes retain `LIMIT 1`, while affected-file joins retain the left schema order plus a final Boolean split marker. |
| `verify_group_data`, `src/engine/backfill/verification.rs:365` | Source/group pins and bound recipe → exact coverage/value validation | Existing coverage validation proves nonnull, unique and complete source locators before the independently reconstructed value comparison. At line 483 typed locator equality and a null-safe `IsDistinctFrom` disjunction replace generated comparison SQL, retaining the one-mismatch limit. |

## Data flow and invariants

**Source and computed values remain the same objects.** The edit declares existing
IDs, not positional correspondences. Native schema construction retains each root's
interpretation while allowing only its presentation name/order or omission. The
candidate preserves the original snapshot; its inventory inherits exact references
and owners. Existing scalar input maps continue through `inherited_inputs`, and
per-file completion keys resolve before decoding/evaluating the source. Native
metadata assembly is new; retained Parquet files are shared.

**Preparation and cutover have separate authority checks.** Initial qualification
precedes admission, including refusal while the selected base trails earlier
accepted source work. The operation captures the base manifest and requires its source
boundary to remain current (`src/engine.rs:621`). Offline verification requires an
exact one-boundary advance. Its expected definition remains
active through preparation. `select_with_progress` changes the definition and result
in the same catalog commit (`src/engine/history.rs:187`). A newer admission or old
writer cannot silently cross that boundary. Stable request reconciliation precedes
these live checks, so a selected result remains recoverable after a lost response.

**Resource ownership follows work.** Candidate registration retains its existing
admission across the blocking durable write and native catalog registration
(`src/storage/tables.rs:83`). Transition comparison reserves before its borrowed
maps and moves custody into `Budget::blocking`; cancellation cannot release that
reservation while the worker still owns native clones/JSON. `bounded_json`
(`src/storage/tables.rs:107`) stops encoded output at the metadata limit. Parent
manifest parsing uses `inputs::read_manifest` (`src/engine/inputs.rs:286`), including
the existing worker-owned buffer/metadata permits.

These are application admission and encoded-byte limits. The four-metadata
comparison reservation is not a measured bound on Rust/serde/Iceberg allocations.
Native `open_pin` still hashes and loads metadata through the previously documented
path. Historical schema transition checks can repeat across scalar groups and
history hops. No total-I/O or whole-process memory guarantee follows. The shared
reader's all-requested-fields-absent fallback and native execution/spill limitations
remain unchanged.

**Verification compares complete permitted native state.** Expected metadata comes
from the same maintained builder operation; only generated identity/location and
the update clock vary. Native `Eq` includes state that manually enumerating public
getters could miss. Equal values or equal current schema alone cannot justify a
changed property, reference or field-allocation high-water mark. This proof adds
no custom metadata parser or separate schema authority.

**Typed key matching keeps the existing relational semantics.** DataFusion 53.1's
`DataFrame::join_on` delegates to `LogicalPlanBuilder::join_on`, which combines the
predicates with `AND` (`datafusion-expr/src/logical_plan/builder.rs:969`). Its
maintained equijoin extraction recognizes the pure `IsNotDistinctFrom` conjunction
and uses `NullEquality::NullEqualsNull`
(`datafusion-optimizer/src/extract_equijoin_predicate.rs:86`).
`Column::new(Some("l"), field)` keeps the field name literal; only the fixed relation
name is parsed. Thus dotted and quoted field names do not become nested paths.
Existence probes still select a constant with one-row limit. The affected-file
left join still uses distinct prepared keys, preserves duplicate source rows,
selects source columns in native order, and appends its match marker last. The
shared native reader, writer permits, streamed split and cleanup remain unchanged.
This removes the observed join-parser call without another execution abstraction;
no broad query-speed claim follows.

Scalar verification uses the same literal-column helper. Its preceding coverage
proof ensures each exact source file/row appears once (`backfill/verification.rs:394`),
so an inner join on file **and** row remains sufficient. `IsDistinctFrom` produces
a non-null Boolean for every output comparison; the explicit `Expr::or` tree
preserves the original parenthesized disjunction, including null versus non-null
values. The maintained shared runtime and native stream providers still own
execution admission. This change adds no scans, producer tasks or new cache.

## Test behavior and evidence

The reviewer read the following assertions. Execution was performed by the owner.

| Fixture in `tests/schema.rs` | Behavior and exercised edges |
| --- | --- |
| `rename_reorder_keeps_source_and_computed_files_and_old_reads`, line 91 | Exact source/group Parquet references and modification times survive; current source and computed values match a Rust oracle, and old values still read. Retains scalar input IDs, verifies retry identity and succession, and rejects a forged reordered declaration. |
| `invalid_edits_refuse_before_admission_and_empty_sources_evolve`, line 202 | Consumed-field removal, case-insensitive collision, reserved name, repeated/unknown ID and unchanged next definition leave all lifecycle records unchanged. Empty sources retain valid evolved schema. |
| `hidden_fields_stay_historical_and_new_writes_use_the_selected_schema`, line 245 | Hiding retains files/high-water and historical reads; a new backfill binds the current name/ID, and a subsequent duplicate/null-key update plus combined read matches the explicit value oracle. |
| `rewritten_native_state_cannot_masquerade_as_a_schema_edit`, line 353 | Rehashed metadata with changed properties, allocation high-water or native tag refuses despite otherwise readable values and schema. A forged nonadjacent boundary also refuses. The valid result still verifies. |
| `http_and_cli_schema_routes_use_the_same_retained_request`, line 412 | Repeated HTTP run returns the same result; a stale request returns conflict. CLI planning/retry reopens the workspace and returns the same retained operation. |
| `killed_schema_workers_resume_only_missing_phases`, line 488 | SIGKILL after acknowledged phases, followed by external input removal, resumes only the remaining work markers; readback/selection retries repeat no completed expensive phase. |
| `cached_related_result_checks_schema_input_history`, line 537 | A source-only schema input's missing original native metadata blocks cached dependent selection; restoring it permits resume. Editing related summary output remains an explicit refusal. |
| `schema_cutover_fences_old_workers_and_later_admissions`, line 600 | Earlier unresolved source admission prevents a new schema request without changing lifecycle records. Completing that source permits cutover; old-definition backfill then refuses while the completed source retry remains exact. Later admission blocks stale schema selection while preserving phases and the later input's ability to finish. |
| `schema_phases_resume_and_cached_readback_requires_base_schema`, line 695 | Each durable interruption resumes without the external input; acknowledged phase records remain identical. Missing base native metadata blocks selection after cached readback and succeeds after restoration. |

The focused schema suite passed in
[schema-operation-final-tests.log](schema-operation-final-tests.log) before the
last pending-source/boundary assertions and typed-join changes. Earlier compile
failures and expanded runs remain retained. The first full-gate failure and the
actual parser trace remain in
[schema-operation-check-stack-failed.log](schema-operation-check-stack-failed.log)
and [schema-operation-stack-trace.txt](schema-operation-stack-trace.txt).
An attempted preparation-boxing change did not reduce the measured futures or fix
the failure and was reverted. Shared native metadata handles reduced the diagnostic
future sizes in [schema-operation-futures-shared-metadata.log](schema-operation-futures-shared-metadata.log),
but [schema-operation-shared-metadata-regression.log](schema-operation-shared-metadata-regression.log)
still failed. The original summary fixture then passed with typed joins in
[schema-operation-typed-joins-regression.log](schema-operation-typed-joins-regression.log).

The second failed full gate remains in
[schema-operation-check-second-stack-failed.log](schema-operation-check-second-stack-failed.log),
with its actual trace in
[schema-operation-second-stack-trace.txt](schema-operation-second-stack-trace.txt).
The complete related-refresh suite passed against both typed replacements at the
normal stack limit in
[schema-operation-related-tests.log](schema-operation-related-tests.log), including
both formerly failing paths. The final owner-run `./scripts/check` exited zero;
[schema-operation-check.log](schema-operation-check.log) retains its successful
format check, all-target/all-feature Clippy with warnings denied, all-feature test
suites and doc tests. This includes the final pending-source/adjacent-boundary
assertions, native-state and scalar-value forgery checks, both previously failing
normal-stack paths, and schema CLI/HTTP/recovery cases. No production source changed
after that gate started; the later README clarification requires at least one
retained root, matching `validate`.

The expanded `tests/updates.rs:504` composite tuple fixture uses both a dotted
name and an embedded quote, and independently checks duplicate/null tuple output
and removed rows; it passed in the final gate. Existing group corruption
coverage in `tests/backfill.rs:632` replaces values while preserving valid coverage
and requires `invalid_group_values`, exercising the changed comparison. Existing exact
serializer-limit and canceled metadata/manifest worker tests cover the reused
resource plumbing; no separate schema-operation cancellation hook was introduced.

## Conclusion

VERDICT: APPROVE.

The scoped operation uses existing lifecycle/native mechanisms, preserves exact
payload files and computed meaning, and selects its schema and writer definition
atomically. The resource issues and observed default-stack failures are closed;
the final repository gate passed against the shared-path changes. Tests exercise
the behavior behind the reuse and recovery claims. Coverage of changed paths: ADEQUATE.
Confidence: HIGH for this scoped implementation; performance and whole-process
memory remain unqualified.

Optional additions/defaults, related-output schema changes, global field IDs across
full rebuilds and continuous-ingestion catch-up remain outside this checkpoint.
