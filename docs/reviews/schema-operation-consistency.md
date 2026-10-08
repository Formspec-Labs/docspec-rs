# Public schema operation: consistency review

**VERDICT: APPROVE.** The identified semantic gaps are closed. The final owner-run
repository gate passes, including the corrected source-boundary cases and both
previously failing default-stack paths. Shared replacement and scalar verification
now construct the internal joins with maintained typed expressions.

Reviewed against `01b5033` on 2026-10-08 using the semi-formal code-review and
Rust skills. This is a static review of the current diff and its callers,
maintained Iceberg implementation, and test assertions. The reviewer did not run
Cargo, edit implementation or tests, or create commits.

## Findings and closure

### F3 — Schema could skip an earlier admitted source input — closed in source

- **Severity/category:** BLOCKER, source coverage and cutover correctness.
- **Evidence:** Initially Schema admission used the selected source, incremented
  `authority.boundary`, and required only that no subsequent admission advanced
  its captured boundary (the shared admission and guard at
  `src/engine.rs:607`, `:618`, `:1156`). The original old-worker fixture explicitly
  permitted Build processing before Schema; its corrected version is now at
  `tests/schema.rs:600`. Thus selected S at boundary 1, admitted unfinished U
  at boundary 2, and Schema at boundary 3 can yield a selected result forked only
  from S while claiming boundary 3. Changing the active definition then prevents
  U from resuming. This slice does not adopt pending source input into the new
  definition, contrary to R3's complete-coverage cutover requirement.
- **Closure:** `check_schema` now requires equal admission/selected boundaries
  for a new request (`schema.rs:36`). Both planning and execution supply whether
  the exact operation is already admitted. Existing Schema retries retain their
  own strict boundary checks. The revised fixture requires unchanged authority on
  refusal, resolves the pending source, then uses a pending Backfill to test old
  definition fencing. Completed source retries return their historical receipt
  without reselecting it. The revised fixture passes in the final repository gate.

### F4 — Offline verification must enforce the one-step schema boundary — closed in source

- **Severity/category:** BLOCKER, coverage verification.
- **Evidence:** With F3 fixed, Schema admission starts at the selected boundary
  and advances exactly once. The initial `verify_transition` accepted any greater
  boundary (`schema.rs:250`), permitting a rehashed result to claim arbitrarily
  later coverage without that input having been selected.
- **Closure:** `before.boundary.checked_add(1)` must now equal
  `Some(after.boundary)` (`schema.rs:250`). The native-state corruption fixture
  independently rehashes a false later boundary and requires `invalid_schema`.
  This assertion passes in the final repository gate.

### F5 — Generated internal joins overflowed the default stack — fixed in source

- **Severity/category:** BLOCKER, runtime regression.
- **Evidence:** `schema-operation-check-stack-failed.log` terminates in
  `aggregation::summary_coverage_guards_and_rebase_preserve_completed_work` with
  stack overflow. Initial future-boxing and shared-metadata changes did not close
  it; their failed targeted logs remain retained. The owner traced the failure
  through generated composite key SQL in `write::any_rows`. After fixing that
  path, the next full gate overflowed in incremental refresh with computed groups.
  `schema-operation-second-stack-trace.txt` identifies generated SQL in
  `verify_group_data`; the second failed gate is retained separately.
- **Closure:** `key_join` now constructs literal qualified `Column` nodes and
  `IsNotDistinctFrom` expressions through DataFusion's maintained `join_on`
  (`src/engine/write.rs:531`). Existence probes still select literal 1 and limit
  to one row. Affected-file reads select all left fields in native order plus the
  right match marker. `schema-operation-typed-joins-regression.log` records the
  previously failing fixture passing at default stack. Scalar verification now
  likewise joins typed non-null location columns, compares outputs with typed
  `IsDistinctFrom` joined by `Expr::or`, and selects one literal mismatch
  (`src/engine/backfill/verification.rs:483`). This preserves the comparison's
  Boolean grouping without relying on SQL parentheses. The complete related
  suite passes at normal stack in `schema-operation-related-tests.log`, and the
  final repository gate passes the source, incremental and enriched suites
  together. Both original failure targets pass in that final gate.

### F1 — Cached semantic dependencies need the original schema metadata — closed in source

- **Severity/category:** BLOCKER, recovery correctness.
- **Evidence:** The existing retained-object list includes the composition parent
  manifest, while a schema fork's current native inventory does not need the old
  table-metadata file. Scalar binding verification returns immediately when no
  computed groups exist. A related operation consuming a source-only Schema
  release could finish readback, lose that input's base native metadata, then
  resume through the non-full dependency graph and select a result whose input
  schema-preservation proof could no longer run. A direct Schema retry already
  opens this metadata in `check_schema`; that simpler case is not evidence of
  this cached-dependency gap.
- **Closure:** `schema::check_available` reads the exact parent and checks the
  native transition (`src/engine/schema.rs:296`). Both ordinary cached readback
  (`src/engine.rs:1528`) and non-full related dependency traversal
  (`src/engine/verification.rs:145`) call it. The no-group readback fixture removes
  that old metadata, requires refusal and unchanged selection, restores it and
  resumes (`tests/schema.rs:695`). The specific related-dependency regression
  pauses an aggregate Refresh after readback, removes the Schema input's original
  native metadata, requires refusal with no selected output, and resumes after
  restoration (`tests/schema.rs:537`). Both fixtures pass in the focused log.

### F2 — Planning accepted names that scalar preparation rejects — closed in source

- **Severity/category:** WARNING, API consistency and unnecessary work.
- **Evidence:** The initial `check_schema` compared computed output names exactly,
  whereas scalar compilation compares them case-insensitively. Renaming a source
  field to `TEXT_LENGTH` could pass planning and mutate the admission boundary,
  then fail against the selected `text_length` group. Reserved occurrence names
  similarly lacked schema preflight checks.
- **Closure:** `check_schema` now uses the same case-insensitive collision rule
  (`schema.rs:36`), and `validate` rejects both reserved occurrence constants
  case-insensitively (`schema.rs:151`). The malformed-edit fixture checks plan and
  execution refusal with the complete authority records unchanged
  (`tests/schema.rs:202`).

## Patch summary and function trace

`SchemaSpec` supplies an exact selected base, the expected active writer
definition, a distinct next definition, and an ordered list of existing native
root IDs and their new names (`src/model.rs:462`). Omitted roots disappear from
the new schema. The operation forks native metadata, retaining snapshots and
value objects, then uses the existing preparation, verification and selection
path. The new definition becomes active in the same lifecycle commit as the
selected result. Format and processor versions advance to 10.

| Function or seam | Location | Inputs → outputs and verified behavior |
| --- | --- | --- |
| `plan_schema`, `evolve_schema`, `schema_until` | `src/engine/schema.rs:12` | Typed request → shared plan, release or test-only durable interruption. They do not introduce a separate lifecycle. |
| `SchemaSpec` / `OperationRequest` methods | `src/model.rs:473`, `:546` | Expected definition remains the preparation guard; `result_definition` provides the selected next definition. Schema requests have an exact base and no new payload inputs. |
| `validate` / `fields` | `schema.rs:151`, `:184` | Ordered native IDs/names → a native schema cloned from existing roots. Reject duplicate/unknown IDs, invalid names, unchanged writer definitions and unsupported additions. Native validation preserves identifier constraints. |
| `check_schema` | `schema.rs:36` | Selected source and requested schema → pre-admission refusal for consumed-field removal or computed-name collision. Both plan and execution call it. |
| `admit_schema` | `schema.rs:82` | Exact base release and admitted operation → retained parent-manifest completion. It checks the captured manifest before recording admission. |
| `process_schema` / `evolve` | `schema.rs:109`, `:212` | Exact base native metadata → protected fresh native identity/location with the declared current schema. `storage::pin` retains the original owners/digests for every shared native object. Native metadata handles use maintained shared ownership until the candidate copy is needed. |
| `register_output` / `register_candidate` | `src/engine/write.rs:66`, `src/storage/tables.rs:79` | Candidate-builder closure → native registered table. Reuses the existing admitted blocking metadata serialization and durable create path. |
| `qualified`, `key_join`, `any_rows` / replacement callers | `src/engine/write.rs:528`, `:531`, `:559`, `:221`, `:355` | Literal field names and typed null-safe conditions → equivalent inner existence or left replacement joins. The left schema/order and duplicate occurrences are preserved; probes still return at most one literal row. |
| `prepare_and_select` | `src/engine.rs:572` | Request → retained phases, scalar groups, independently verified release and guarded selection. Schema inherits base inputs, retains scalar input IDs and records fresh composition bindings. |
| `captured_manifest`, `fingerprint` / `phase_key` | `src/engine.rs:1102`, `:1249`, `:1111` | Exact selected native combination, base, old/new definitions and ordered fields → captured base and stable retry/material identity. Schema admission does not use the empty-input admission key shared by unrelated requests. The common capture helper replaces incremental-specific wrappers. |
| `check_authority` / schema capture | `src/engine.rs:1156`, `:618` | Captured epoch, eligibility, definition, exact base manifest and boundary → permission to start/reuse/select. Schema requires the current admission boundary. |
| `select` | `src/engine/history.rs:187` | Verified result and current authority → one commit containing next definition, selected release, result and accepted succession. Old-definition workers subsequently fail their normal guard. |
| `verify_transition` | `schema.rs:229` | Exact before/after selections → native-state proof. Requires inherited inputs/relationship/eligibility, exactly one boundary advance, identical source rows/snapshot/objects, and equality with the permitted native edit. |
| `verify_groups` / binding history | `src/engine/backfill/verification.rs:176`, `:355` | Schema result or an inherited scalar ancestor → transition proof plus exact recipe membership. Schema hops retain input IDs and prove the actual owning request. |
| `verify_group_data` | `src/engine/backfill/verification.rs:365`, `:483` | Independently reconstructed expected values and native actual group → typed location join and null-safe mismatch filter. Preceding complete non-null location coverage establishes one-to-one matching even when business values duplicate. |
| Cached availability | `schema.rs:296`, `src/engine/verification.rs:145` | Cached result or semantic dependency → required parent metadata remains available and still establishes the schema edit. |
| CLI / HTTP | `src/main.rs:83`, `src/http.rs:32` | JSON schema request → the same plan/run methods; retry behavior and refusal codes remain owned by the engine. |

The other touched seams are deliberately small: changes verification permits a
Schema result only without a `ChangePin`; related-source replacement refuses
Schema alongside raw Build/Update; exact-base capture includes Schema; bounded
native JSON serialization is made crate-visible for transition verification.

## Data flow and invariants

1. **A requested field means its native ID.** `fields` clones the selected root,
   changing only its display name and order. Types, nested structure, defaults,
   requiredness and IDs survive unchanged. Identifier fields cannot silently be
   removed. The request cannot add a new field or resurrect a hidden root.
2. **A schema edit preserves physical values and evidence.** Source rows,
   snapshot and every non-metadata `ObjectRef` must equal the base. The verifier
   reconstructs the permitted edit through maintained `TableMetadataBuilder`,
   normalizes only the generated timestamp, and compares native metadata with
   `Eq`. Schema history, high-water IDs, snapshots, references, statistics,
   properties and native layout participate in that equality. The fresh UUID
   must differ; location and generated clock are permitted identity changes.
3. **Scalar meaning follows IDs across the edit.** The shared non-rebuild mapping
   retains aliases and IDs. Per-file material dependencies remain unchanged for
   rename/reorder, allowing exact computed value files to be adopted. Each new
   binding records its Schema request, source metadata and parent. Independent
   verification still proves origin history, complete occurrence coverage and
   actual scalar values.
4. **The expected and resulting definitions have separate uses.** Preparation
   guards use `definition`; release and scalar binding checks use
   `result_definition`. Only successful selection changes active authority to
   `next_definition`. Failed preparation leaves the prior selected result and
   definition active, although admitted work has its own retained boundary.
5. **Source admission fences schema cutover in both directions.** A new Schema
   requires source admission to equal selected coverage before advancing it once.
   Its exact base manifest and captured boundary must remain current. Earlier
   unresolved admission prevents starting; later admission prevents selection. Existing
   epoch-only completion rules may preserve already finished work, while strict
   guards prevent its stale reuse or promotion.
6. **Metadata-only describes writes, not verification cost.** Source and reusable
   scalar Parquet files remain identical, but native object hashing, source reads,
   scalar coverage and full value verification still execute as required. This
   checkpoint makes no latency or memory improvement claim.
7. **Typed replacement joins retain the old semantics.** `Column::new` stores the
   field name literally; only the fixed `l`/`r` qualifiers use name parsing.
   `join_on` combines the conditions with AND. DataFusion's maintained
   `extract_equijoin_predicate` recognizes `IsNotDistinctFrom` and assigns
   `NullEqualsNull`; the initial `NullEqualsNothing` of an unoptimized generic
   join does not replace that expression's semantics. Distinct prepared scope
   keys continue to prevent multiplying each left occurrence. The marker remains
   a non-null Boolean after `is_not_null`, so row splitting is unchanged.
   The scalar comparison uses ordinary equality only for its already validated
   non-null locator fields. Its output comparisons use `IsDistinctFrom`; thus
   successful nulls and opposite Boolean values remain distinguishable.

Maintained behavior inspected: Iceberg 0.10.1
`table_metadata_builder.rs:640` adds schemas without reducing `last_column_id`,
`:687` selects the current schema, and `:1133` checks layout, normalizes native
state and updates the generated timestamp. The independent comparison uses that
maintained representation rather than an incomplete hand-picked metadata list.
Also inspected DataFusion 53.1.0 `dataframe/mod.rs:1346` and `:2407`,
`logical_plan/builder.rs:969`, `column.rs:58`, and
`extract_equijoin_predicate.rs:84` for typed joins, alias replacement, literal
field names and null-safe key extraction.

## Test behavior and edge cases

These are inspected assertions, not reviewer-executed tests.

| Test | Location | Trace and expected behavior |
| --- | --- | --- |
| `rename_reorder_keeps_source_and_computed_files_and_old_reads` | `tests/schema.rs:91` | Quoted rename and reordered fields preserve exact source/computed payload references and mtimes; native and combined values match a Rust oracle; historical reads, succession and stable retries remain exact; changed request order with the same operation conflicts; a rehashed wrong-schema claim refuses. |
| `invalid_edits_refuse_before_admission_and_empty_sources_evolve` | `tests/schema.rs:202` | Consumed removal, case-insensitive name collision, reserved names, duplicate/unknown IDs and unchanged definition refuse without changing authority. An empty source still yields a verified schema result. |
| `hidden_fields_stay_historical_and_new_writes_use_the_selected_schema` | `tests/schema.rs:245` | Hiding a field preserves its old files and ID high-water. Fresh Backfill binds the renamed retained ID. A later Update preserves duplicate/null replacement semantics, computes correct values and retains high-water under the selected schema. |
| `rewritten_native_state_cannot_masquerade_as_a_schema_edit` | `tests/schema.rs:353` | Independently rehashed metadata cannot change properties, high-water, references or declared coverage boundary. The valid release continues to verify. |
| `http_and_cli_schema_routes_use_the_same_retained_request` | `tests/schema.rs:412` | HTTP plan/run and repeat run return the expected result; stale new operations receive 409. After closing the original engine, CLI plan/schema reconcile the same exact release. |
| `killed_schema_workers_resume_only_missing_phases` | `tests/schema.rs:488` | Actual child-process loss at named durable handoffs resumes with the exact expected remaining work-marker list. It checks values and full release verification after deleting the original input locator. |
| `cached_related_result_checks_schema_input_history` | `tests/schema.rs:537` | Cached aggregate readback requires the Schema input's original metadata, refuses selection while missing, then succeeds after restoration. Schema edits on related results refuse explicitly. |
| `schema_cutover_fences_old_workers_and_later_admissions` | `tests/schema.rs:600` | Earlier unresolved source admission refuses Schema without mutation; after source completion the schema includes those values. A pending old-definition Backfill refuses, while a completed Build retry returns its historical result without reselecting. Later source admission still fences pending Schema and remains resumable. |
| `schema_phases_resume_and_cached_readback_requires_base_schema` | `tests/schema.rs:695` | Durable phase interruptions retain exact completion records despite original input-locator loss. Missing base native metadata after readback refuses without changing selection, then restoration allows resume. |

The process-kill fixture covers source-only schema operations at its named durable
handoffs. Enriched values and file reuse have their own functional fixture; this
does not claim arbitrary-instruction crash qualification or concurrent ingestion
catch-up. Public requests currently require a nonempty list of retained roots.
The shared writer change also depends on the existing update, native writer,
incremental join and summary suites. In particular,
`tests/updates.rs:504` checks dotted/quoted composite keys, duplicate rows and partial/all
null keys against independently calculated removed/result rows. Its assertions
remain appropriate for the typed-join implementation.
The prior `tests/scalar_bindings.rs:121` opposite-Boolean forgery and
`tests/backfill.rs:630` native wrong-value/coverage fixtures directly exercise the
changed scalar comparison. Both pass in the final repository gate.

## Execution evidence and conclusion

The owner ran `./scripts/check` to exit 0 on the final source. The reviewer
inspected its retained output in [schema-operation-check.log](schema-operation-check.log)
and the check script. This covers formatting, all-target/all-feature Clippy with
warnings denied, all-feature unit and integration tests, and doc tests. In
particular, it includes the corrected pending-source and adjacent-boundary Schema
fixtures, semantic-history availability, actual worker kills, opposite-Boolean
and ancestor-binding forgeries, the complete related-refresh suite at normal
stack, and dotted/quoted composite-key updates. No implementation changed after
that gate started; the subsequent README clarification requires at least one
retained root. This certificate does not claim a separate no-default-features
execution for this checkpoint.

The earlier focused pass in `schema-operation-final-tests.log` preceded the last
boundary corrections and typed-join fixes; it is supporting history, not the
final-source gate. The initial compile failures, both full-gate stack failures,
unsuccessful targeted attempts and extracted traces remain retained. The
successful complete related suite in `schema-operation-related-tests.log` and
the final repository gate establish closure of the runtime regressions.

**VERDICT: APPROVE.** Native value preservation, stable scalar meaning, exact
cutover coverage and recovery are supported by the inspected implementation and
the owner-run final gate. No unresolved correctness finding remains in this
checkpoint. Changed-path coverage is adequate; confidence is high. This remains
a static review supported by inspected execution evidence, not reviewer-run
testing or a performance qualification.

The scope is existing source-root rename, reorder and hide with strict guarded
selection. Schema edits on related outputs, new optional/default fields and
durable absence rules, nested/type changes, global source IDs across full rebuilds,
continuous-ingestion catch-up and complete R3 qualification remain separate work.
