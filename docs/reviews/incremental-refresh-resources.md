# Incremental related-refresh resource review

No open actionable resource defect remains in the inspected implementation. The patch shares the existing replacement writer, materializes discovery relations once, retains bounded native readers and the shared execution runtime, and binds reusable deltas to their exact output base. The complete owner-produced gate passes after the final change-verification aggregate edit. **VERDICT: APPROVE** for this local incremental equality-join checkpoint.

This is a static review of the uncommitted checkpoint against `af329e9`, applying semi-formal-code-review and the applicable Microsoft Rust performance, library, resilience, and universal guidance. The reviewer read changed production code, tests, documentation, maintained dependency source, and owner-produced logs. The reviewer did not run tests or modify Rust code. Scope is incremental equality-join preparation and its shared writer/verification changes, not general incremental SQL or complete-path performance qualification.

## Findings and closure

### IR-R1 — Discovery metadata was undercharged and retained unused inventories — CLOSED

- **Severity/category:** WARNING / resource correctness.
- **Location:** `src/engine/incremental.rs:247–285`.
- **Scenario:** The first implementation charged only live-file map entries while also building maps over both complete retained object inventories. Metadata/history could grow independently of live files. It also kept a vector of discovery pins that was never subsequently read.
- **Resolution:** The map allowance includes old/new live descriptors and old/new object indexes, checks the finite metadata allowance, and reserves before allocation. Map keys borrow retained paths rather than duplicating their strings. Native scan inventory reservations remain live through streamed discovery. The unused pin vector was removed. This is conservative admission, not an exact allocator or process-memory measurement.

### IR-R2 — Preparation reconstructed the entire old join unnecessarily — CLOSED

- **Severity/category:** WARNING / performance.
- **Location:** `src/engine/incremental.rs:63–75`.
- **Scenario:** Calling full verification on the accepted base before discovery reconstructed the old join in addition to the mandatory new-result reconstruction.
- **Resolution:** Preparation now binds the exact base manifest, checks its bounded retained dependency graph for availability, and hashes the prior source objects whose disappearing keys discovery reads. The shared writer separately verifies old output bytes. Final independent verification still reconstructs the complete target join. The change removes an unnecessary old relational reconstruction without weakening the captured-byte or candidate checks.

### IR-R3 — Disk preflight charged the full retained population for a small update — CLOSED

- **Severity/category:** WARNING / performance and availability.
- **Location:** `src/engine/incremental.rs:208–216`; `src/engine/write.rs:342–350`, `414–434`.
- **Scenario:** Using every current input object's size as replacement output charged roughly two complete retained populations even for a tiny or empty change. It was also not an upper bound on many-to-many join output.
- **Resolution:** The shared writer accepts an optional known replacement estimate. Update/evidence supply estimates; incremental join output uses `None`. Preflight covers affected rewrites, a known estimate when available, and declared scratch. Streamed replacement batches check available disk against scratch plus bounded writer headroom. No additional full materialization or copy was introduced.
- **Limit:** These are compressed-byte estimates and available-space checks, not reserved disk or a hard output-size cap. Discovery writes precede the affected-rewrite preflight; native flushes, compression, deliberate fanout, and other disk users can still cause an I/O failure. Existing completed phases survive, while the incomplete attempt remains unselected. The disk fixture checks refusal/retry at preflight; it does not separately force the later per-batch check.

### IR-R4 — Old output discovery and delta verification repeated full relation scans — CLOSED

- **Severity/category:** WARNING / performance and maintainability.
- **Location:** `src/engine/incremental.rs:165–187`; `src/engine/changes.rs:127–140`.
- **Resolution:** Old contributor discovery now has one `before` relation with two left joins to the unique old occurrence scopes, rather than two full-output branches. Change verification uses one grouped union with separate membership and reconstruction weights, rather than reading before/removed once for each invariant. Each input relation appears once in that aggregate. Both improvements reuse maintained relational operators and reduce duplicated query structure; no elapsed-time gain is claimed.

### IR-R5 — Counters could be mistaken for complete incremental I/O — CLOSED

- **Severity/category:** WARNING / qualification accuracy.
- **Location:** `src/engine/incremental.rs:105–113`, `218–220`; `src/engine/write.rs:373–377`; `docs/STATUS.md:571–576`.
- **Resolution:** STATUS explicitly limits counters to changed-file extraction and replacement-writer work. Scoped input scans, contributor discovery, cardinality checks, metadata and independent verification are omitted. These counters cannot support a Full-versus-Incremental total-read or latency comparison.

### IR-R6 — Native dynamic join filters dropped null-safe matches — CLOSED FOR THE PINNED CONFIGURATION

- **Severity/category:** BLOCKER / correctness, exposed during owner qualification.
- **Location:** `src/budget.rs:96–109`; shared replacement existence probe at `src/engine/write.rs:326–333`.
- **Evidence chain:** The independent source-intent oracle retained a `(NULL, 2)` row after its requested deletion (`docs/reviews/incremental-refresh-source-oracle.log`). Another retained run found stale right-only contributor rows; candidate verification refused selection (`incremental-refresh-empty-side-diagnostic.log`). In pinned DataFusion 53.1, `datafusion-physical-plan-53.1.0/src/joins/hash_join/exec.rs:842–861` allows dynamic filtering for Inner joins without a null-equality exclusion. `shared_bounds.rs:146` constructs ordinary bounds and `:414–456`, `:524–553` combines bounds with membership. The shared writer uses an Inner null-safe join for its existence probe over Parquet, so dropping null matches can incorrectly classify a file as unaffected.
- **Resolution:** Every bounded context disables only `datafusion.optimizer.enable_join_dynamic_filter_pushdown`. Existing memory/spill limits and other pruning settings stay in place. The complete incremental fixture matrix, including direct source-update intent checks, passes in `docs/reviews/incremental-refresh-qualified-tests.log`. This is a narrow maintained configuration change; no custom join implementation or patched dependency is added.
- **Limit:** Re-enabling this optimization requires upstream qualification. Its performance cost has not been measured, as documented in `docs/STATUS.md:584–592`.

## Patch summary

`RefreshMode::Full` remains the default. Explicit Incremental mode requires an exact selected related base, unchanged join recipe and producer roles, matching native input schemas, and current output eligibility. Old/new immutable file differences produce a conservative typed key set. Null-safe membership expands those keys across complete old/new input groups, including keys with null components. The same join generator then computes replacement rows under the recipe's declared null policy and join kind.

The writer extracted from source Update receives an explicit removal scope and a separate replacement frame. It materializes the scope once, probes existing files, shares unaffected descriptors, supplies kept/removed rows from one joined decode, and streams replacement rows once. Actual removed/added native tables describe the writes. Contributor evidence uses the same writer. No applied rows adopts exact prior source/evidence pins while coverage advances.

Incremental operation identity and reusable phase keys include the exact captured base manifest. Full refresh retains its existing material reuse rules. Offline verification checks incremental provenance, change membership and reconstruction, the complete target join, and evidence multiplicities. CLI and HTTP use the existing `RefreshSpec` services; this checkpoint adds no transport-specific lifecycle path or dependency.

## Function trace

| Function / method | File:line | Inputs → output | Verified behavior |
| --- | --- | --- | --- |
| `Budget::context` | `src/budget.rs:96` | Shared limits/runtime → session | Retains shared memory/spill accounting, row/partition settings, and disables the unqualified native optimization. |
| `plan_request`, `prepare_and_select` | `src/engine.rs:159`, `568` | Request and current authority → plan / release | Checks incremental qualification before advancing admission; captures exact base manifest; routes processing/evidence through the existing retained phases. |
| `manifest_ref`, `base_manifest`, `check_incremental`, `qualify` | `src/engine/incremental.rs:8`, `16`, `32`, `436` | Request/base/eligibility → captured identity or refusal | Applies only to Incremental; checks recipe, roles, native schema and eligibility; manifest reads use aggregate metadata admission. |
| `check_authority`, `fingerprint` | `src/engine.rs:1121`, `1212` | Authority/request → eligibility / retry identity | Refuses changed selected manifest even with the same version ID; fingerprint includes mode. |
| `phase_key`, `phase`, `complete` | `src/engine.rs:1076`, `974`, `1045`; `src/engine/refresh.rs:338` | Captured operation and phase → retained completion | Incremental keys include exact base, source references, recipe, processor and eligibility; completion remains the existing epoch-guarded acknowledgment. |
| `process_refresh`, `prepare_evidence` | `src/engine/refresh.rs:81`, `107` | Recipe/phase → prepared native source/evidence | Branches on mode, preserving common recovery handoffs and retained completion reuse. |
| `process_incremental` | `src/engine/incremental.rs:52` | Exact admitted inputs and captured base → Prepared | Checks prior bytes, materializes affected keys, builds complete old/new scopes, materializes old tuple scope through the shared writer, and computes the scoped join. |
| `changed_keys` | `src/engine/incremental.rs:239` | Old/new source pins and typed keys → native key table | Captures inventories once per pair, refuses changed identity at the same path, reserves map metadata, serially reads changed files, and writes only key arrays. |
| `OccurrenceScan::open`, `read_file` | `src/storage/occurrences.rs:209`, `257` | Exact native table/file → admitted occurrence stream | Maintained tasks retain metadata ownership; each selected stream owns its batch admission and shares inventory custody; full-file counts/locators remain checked. |
| `join_frame`, `join_registered` | `src/engine/refresh.rs:429`, `448` | Full/scoped input providers and recipe → frame | One shared generator validates scalar key types and fields, checks matching cardinality before fanout, and builds exact join membership. |
| `process_update`, `replace_keys` | `src/engine/write.rs:112`, `255` | Typed Update or explicit scope/replacements → Applied | Update retains input schema/overlap checks; shared writer owns scope materialization, file probing, paired kept/removed writers, replacement stream, exact pins and metrics. |
| `key_match`, `split_rows` | `src/engine/write.rs:484`, `505` | Typed keys / joined batch → predicate / kept and removed | Quotes identifiers, matches null-safe composite keys, and splits from one checked pre-split batch. |
| `incremental_evidence`, `check_manifest` | `src/engine/incremental.rs:357`, `425` | Applied change tuples and exact base → evidence table | Checks captured manifest and processing delta base, then applies removed/added contributor tuples through the same replacement writer. |
| `write_batches`, `pin` | `src/storage.rs:162`, `440` | Batch stream / native snapshot → files / TablePin | Existing writer admission lives through close; inventory/ownership checks preserve inherited shared objects and verify native row totals. |
| `verify_changes` and callers | `src/engine/changes.rs:6`; `src/engine.rs:764`, `1521` | Captured request, source and delta → success/refusal | Bounded prior manifest read, shallow source verification, exact endpoint/schema/key-ID checks, removed membership, and signed multiset reconstruction. |
| `verify_groups` | `src/engine/backfill/verification.rs:6`, `145–163` | Composition and parent → provenance validity | Incremental result requires a base, matching change/composition manifests, and shared qualification; existing scalar-group preservation checks remain. |
| `verify_related_with_inputs`, `equal_multisets` | `src/engine/refresh.rs:597`, `682` | Candidate and exact inputs → independent semantic result | Reconstructs full output and contributor multiplicities; diagnostics distinguish schema and value mismatches. |

Backfill's operation constructors explicitly set `base_manifest: None` (`src/engine/backfill.rs:187`, `349`); the new delta-specific guard does not alter independent field-group composition. `RefreshMode` and its serde default are defined at `src/model.rs:217–244`. The processor/format change at `src/engine.rs:80–81` invalidates reuse under the prior implementation identity; no migration support is claimed.

## Data flow and invariants

1. **A delta has one exact before state.** Admission records `Operation.base_manifest`; phase keys and `check_manifest` use that immutable object, while final authority checks require it still matches the selected base. Rebase requires different prepared delta work even if target input versions match. The resulting `ChangePin` binds before/after metadata and actual removed/added tables. Pins escape only through existing protected phase/result records.
2. **Affected groups are a conservative superset.** Discovery takes keys from both removed and newly written files on both inputs. Membership uses `IS NOT DISTINCT FROM` independently of the final join's null policy. Complete old occurrences identify all obsolete output tuples, including unmatched outer rows. Complete new occurrences supply newly possible matches. Rewritten files intentionally change physical occurrence identity. There is no assumption that file equality implies logical key equality, or that inserts can affect only previously matched rows.
3. **Payload lifetime stays with existing admissions.** `changed_keys` borrows file paths from retained scans and sequentially flattens file streams. Key batches clone only selected Arrow arrays; the native writer supplies its retained batch reservation. The paired writer uses a capacity-one channel and `try_join!`, with no detached producer. Joined batches are checked before filtering creates kept/removed arrays. Cancellation drops stream/channel/writer futures; blocking inventory/hash workers retain their own existing reservations. No new background queue or custom scheduling layer is introduced.
4. **Metadata is finite but not free.** Discovery keeps both native task inventories plus bounded file/object maps. Loaded input manifests retain their aggregate allowance. The output writer retains finite native descriptors and per-file query plans while probing existing files. These bounds can refuse high-fragmentation histories before payload processing; they are not a claim that every large history fits a configured budget or that metadata cost is independent of file count.
5. **Expensive replacement rows execute once per preparation.** A stored affected-key table feeds scoped relational views. Old tuple scope is materialized before per-file probes. The replacement frame is executed only when writing added rows. Cardinality queries and old/new scoped views can still scan source payloads separately; final verification deliberately reconstructs full semantics. “Once” describes the replacement-output stream, not every underlying input read.
6. **Change validation preserves duplicate counts.** For each complete typed row, `remaining = before - removed` must be nonnegative and `delta = before - removed + added - after` must be zero. One grouped union checks both. Balanced foreign removal/addition cannot pass merely because its net delta is zero. The verifier reads the immediate prior source shallowly rather than recursively following all historical changes.
7. **No-op adoption preserves bytes and advances meaning.** Zero applied removed/added rows returns the exact prior source pin. Evidence follows the same path. The new dataset version still records its newly captured input coverage, so equal values do not conceal a changed producer boundary. Tests distinguish exact pin reuse from simply equal row counts.
8. **The performance claim is narrower than incremental elapsed time.** Fixtures prove unaffected data/evidence files retain their paths, digests and modification times. Discovery still scans complete scoped inputs and existing output files, and verification reads complete relations. Partial counters exclude these costs. Native RSS, output fanout, the previously documented spill-overrun recovery issue, remote publication, cleanup and the 10M acceptance gate remain outside this checkpoint.

## Test behavior and evidence

The reviewer inspected the following assertions and the owner's passing targeted output in `docs/reviews/incremental-refresh-qualified-tests.log`. The final `docs/reviews/incremental-refresh-check.log` confirms the complete gate passes after the single-aggregate edit, including the shared Update writer and its nested-value/nullable-key regressions. A subsequent test-only addition checks computed-group reuse and passes in `docs/reviews/incremental-refresh-computed-groups.log`. No reviewer execution was performed.

| Test / assertion location | Actual assertion path | Assessment |
| --- | --- | --- |
| `incremental_matches_oracle_across_key_moves_nulls_duplicates_and_both_input_orders`, `tests/related_refresh/incremental.rs:124`; source-intent oracle at `:42–91` | Direct Rust computes requested source replacement/deletion membership, then independently computes joined values and contributor pairs for all join kinds/null modes and successive producer changes. Covers first/last partners, duplicate fanout, partial nulls, moved keys and empty right input. | Meaningful oracle independent of generated SQL; exposed the native pruning failure and passes with the qualified configuration. |
| `incremental_shares_unaffected_output_and_evidence_and_adopts_exact_noop_pins`, `:227` | Captures source/evidence paths, digests and modification times; appends and changes another key group; asserts the originals remain unchanged. The enhanced fixture also asserts inherited scalar-group Parquet objects remain present. A no-op input version requires exact source/evidence/computed-group pins, empty changes and advanced coverage. | Proves real file and completed-group reuse, not a timing improvement. |
| `incremental_refuses_unqualified_recipes_before_admission_and_binds_mode`, `:359` | Changed join policy returns `full_refresh_required` with identical lifecycle inspection; missing base refuses; reused operation ID with different mode conflicts. | Exercises qualification before mutation and mode-bound retry identity. |
| `incremental_killed_workers_reuse_processing_evidence_and_readback`, `:387` | Real CLI workers are killed after retained handoffs. Resume work markers contain only remaining phases, prior completion records remain equal, and final rows/evidence match the Rust oracle. | Covers separate preparation/evidence/readback custody and exact completed-result retry. |
| `incremental_phase_reuse_fences_rebased_outputs_and_exact_manifests`, `:430` | Pauses after processing, advances output by Full refresh, and requires new processing for a rebased Incremental request. Checks differing base-manifest deltas and rejects same-version manifest rebinding in both plan and run. | Direct guard against stale delta reuse. |
| `incremental_verifier_refuses_balanced_foreign_removals_and_unsupported_provenance`, `:540` | Rehashes forged manifests containing balanced foreign removals/additions, unsupported Incremental relabeling, and mismatched change parent. Expects specific semantic refusal codes. | Validates membership beyond arithmetic totals and exact provenance binding; passes after aggregate fusion. |
| `incremental_disk_refusal_retains_admission_for_a_smaller_resource_profile`, `:619` | Deliberately declares more scratch than available, expects `disk_capacity`, preserves old selection and reusable admission, then reopens with smaller limits and resumes to exact no-op source pin. | Tests safe preflight failure/reuse; does not claim a hard disk quota or exercised fanout upper bound. |

The earlier compilation/diagnostic attempts and semantic failures are retained alongside `incremental-refresh-qualified-tests.log`, including `incremental-refresh-source-oracle.log` and `incremental-refresh-empty-side-diagnostic.log`. The final gate passes the existing Update suite as well as related-refresh, backfill, lifecycle, recovery, and transport paths. README (`README.md:288–306`) and STATUS (`docs/STATUS.md:555–606`) describe explicit Incremental mode, exact reuse, broad scans, partial counters and unqualified performance accurately.

## Conclusion

**VERDICT: APPROVE.**

- The inspected patch achieves the bounded implementation intent with maintained native readers, the shared writer and the shared execution runtime. It removes duplicated discovery/verification scans and avoids charging the full population as a replacement estimate.
- Coverage of changed paths: **ADEQUATE**, supported by inspected assertions and the passing final owner-produced gate plus the enhanced computed-group reuse fixture.
- Confidence: **HIGH** for this bounded implementation scope. No throughput, whole-process RSS, hard disk quota, or complete lifecycle acceptance is inferred.
