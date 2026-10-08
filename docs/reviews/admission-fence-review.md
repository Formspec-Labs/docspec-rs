# Source coverage and admission-fence review

## Findings and current disposition

The independent review against `86fb677` is complete. No material finding remains open. The first full gate exposed a stack-safety regression; F1 below records its correction and successful qualification. The change separates the source represented by a result from the mutable admission counter used to authorize cutover. It can remove an unnecessary source rebuild after unused admissions are explicitly abandoned, while keeping source values, shared work and historical interpretation intact.

**APPROVE. F1 is resolved.** The reviewer inspected the successful terminal `admission-fence-check-retry.log`, including the formerly failing defaults scenario, all integration/recovery suites and doc-tests. The full `scripts/check` passed formatting, strict all-target/all-feature Clippy and tests with normal stack limits. The executing author confirmed terminal exit 0 and no further production changes. The reviewer did not run Cargo, edit Rust or scan payloads.

The failed first gate remains at `admission-fence-check-stack-failed.log`: `additions_updates_defaults_and_rebuild_preserve_supplied_state_and_history` aborted with stack overflow / SIGABRT. The passing focused tests did not supersede that failure; approval was reopened until the correction passed the complete gate.

**F1 — P1 resource/correctness, resolved:** the retained crash trace (`admission-fence-stack-failed.json`) reaches native Avro planning through `OccurrenceScan::values`, `replace_keys`, `process_update`, `prepare_source`, `prepare_and_select` and `run_internal`. The correction makes `prepare_and_select` a regular function returning `BoxFuture` (`src/engine.rs:722`); its sole caller now awaits that result instead of constructing the box in its own poll frame (`src/engine.rs:573`). The reviewer inspected the whitespace-insensitive diff: admission/phase/selection order, guards, recovery stops and cancellation ownership remain unchanged. The existing allocation is moved, not duplicated.

The reviewer inspected `admission-fence-frame-before.log` and `admission-fence-frame-after.log`: the debug `run_internal` poll-frame allocation, including saved registers, falls from 69,952 to 41,888 bytes; `prepare_and_select` remains 629,808 bytes. The complete defaults integration suite passed with normal stack limits in `admission-fence-defaults-retry.log`, including the formerly aborting scenario. This demonstrates a narrower debug frame; the defaults retry and corrected full gate establish completion of the qualified paths under normal stack limits. It does not measure release throughput, heap size, total stack peak or general future stack margin. The corrected full gate is retained in `admission-fence-check-retry.log`.

Review basis: repository/workspace `AGENTS.md`, R3/R5 and the accepted plan, the Rust and semi-formal code-review skills, the full current diff and important unchanged callers/callees. The full lifecycle scope remains unchanged.

## Function trace

| Function or state | Code reference | Input → output and verified behavior |
| --- | --- | --- |
| `Operation`, `FORMAT`, `supported_format` | `src/engine.rs:59`, `src/engine.rs:94` | Persists optional schema admission fence and result format; absent fields deserialize with the previous format's meaning. Format 11 changes schema coverage, while the material processor identity stays at v10. |
| `SnapshotRows::prefix` | `src/catalog.rs:41` | Prefix → borrowed current records from the existing ordered map; no second record-map clone. |
| `check_abandoned_gap` | `src/engine/retention.rs:14` | One exact view, dataset and numeric interval → proof/refusal. Qualifies request identity/revision/digest and matching later abandonment for each source admission; requires distinct boundaries to fill the entire interval. |
| `check_schema` | `src/engine/schema.rs:60` | New schema admission → gap proof against selected source coverage and current authority, then existing native schema/dependency checks. Resume relies on its original captured operation. |
| `plan_request` / `prepare_and_select` | `src/engine.rs:181`, `src/engine.rs:722` | Plans and admits schema operations through the same proof. New schema operations preserve the selected boundary and capture `authority.boundary`; other source-producing requests still advance authority. Schema admission stores no replacement authority record. |
| `run_internal` / boxed construction | `src/engine.rs:566`, `src/engine.rs:722` | Keeps the same preparation future and result reconciliation, but constructs the existing box outside the caller's poll frame. Targeted stack recovery and the corrected full gate passed with normal stack limits. |
| `check_authority` | `src/engine.rs:1288` | Captured operation plus current authority → permission/refusal. Uses the stored schema fence when present, retaining the old boundary fallback for old operations. Existing definition, epoch, base and exact parent-manifest guards remain. |
| Result assembly | `src/engine.rs:961`, `src/engine/backfill.rs:451` | Emits the operation's persisted result format, so a resumed operation cannot silently acquire different schema-coverage interpretation. |
| `plan_backfill` / `prepare_backfill` | `src/engine/backfill.rs:149`, `src/engine/backfill.rs:314` | Reports current cutover refusal; keeps preparing and recording qualified work independently of pending source input. Checks the gap in the guarded final selection view. |
| `promotion` | `src/engine/backfill.rs:574` | Requires exact source, source coverage, target binding, definition, eligibility and captured admission counter. Only format 10 retains the former counter-equals-source-boundary restriction. |
| `verify_transition` | `src/engine/schema.rs:509` | Format-10 schema result requires parent boundary plus one; format 11 requires equal source boundaries. Both retain exact native schema transformation, source membership, inputs and parent semantics. |
| Manifest and input readers | `src/engine.rs:1714`, `src/engine/refresh.rs:431`, `src/engine/inputs.rs:156`, `src/engine/inputs.rs:270`, `src/engine/backfill/verification.rs:258` | Accept both supported result formats. Ordinary reads continue resolving immutable manifests and requested data without consulting admission history. |

Unchanged paths traced include common selection and catalog compare-and-swap, phase reservation/completion and abandonment fences, schema native transformation and verification, scalar preparation/rebase, result reconciliation, succession proof, and catalog-free manifest loading.

## Data flow and invariants

- **Source coverage stays with the source.** A new schema result keeps its parent's boundary, exact inputs and unchanged source payload membership. Its operation separately captures the current admission counter. The result's boundary is no longer increased merely to represent a metadata edit.
- **An interval cannot be closed by a partial list.** Each counted boundary is inside `(selected, fence]`, qualified against an admitted request and matching abandonment, and unique. Requiring the distinct count to equal `fence - selected` proves every integer in that interval is covered. Missing evidence, live admissions, duplicates, future boundaries, inconsistent identities/revisions or selected results in the gap refuse. Old schema operations are counted because they advanced the former counter; new schema and backfill composition operations are excluded.
- **A later admission does not revive an old cutover.** Schema checks its persisted fence through `check_authority`; backfill checks its existing `Capture.admission_fence`. Neither silently replaces the capture. Abandoning a later operation does not rewind authority. A fresh operation may prove the larger interval and reuse completed material work.
- **Backfill preparation remains independent.** A pending source admission can block final selection while scalar file completions, prepared native output and readback remain retained. If that already-captured pending admission is subsequently abandoned without another admission, the same backfill can prove closure and select. Later admissions still require a fresh capture.
- **Authority and payload interpretation have separate checks.** Gap proof belongs to a producer lifecycle decision, not a portable payload dependency. Schema verification still proves the exact declared native transformation and source relationship; it does not replay producer scheduling history. Old immutable results retain their format-10 interpretation.
- **Reuse does not follow result-format numbering.** Keeping the material processor identity unchanged is justified here: source bytes, scalar expressions, schema transformation and their material keys did not change. Persisting result format separately preserves interrupted-operation/readback semantics. Final composition still verifies the appropriate boundary relationship.
- **Concurrency remains guarded.** Gap proof and the relevant admission/selection commit use one immutable catalog view. The existing local dataset lock and native stale-view rejection protect that decision. Source/head/target/definition checks remain in the same guarded cutover. Sibling backfills retain their existing merge/recomposition behavior.

## Resource and simplicity assessment

For an empty interval the proof returns immediately. Otherwise it scans operation records, selectively decodes matching request/abandonment records and retains a set bounded by the interval. Cost is O(operation-record bytes plus qualifying request bytes, with ordered-map/set lookups); set space is O(interval width). It reserves bounded metadata capacity, retains the exact snapshot across cooperative yields, and reads no source payload. Fresh plans or cutovers can repeat this metadata proof because authority must be checked at that decision boundary. There is no new index, global cache, per-row lifecycle state or payload certificate.

The concrete avoided work is an otherwise unnecessary new source result solely to close an abandoned gap. No latency, throughput, memory-peak or large-history scaling improvement has been measured. The existing metadata/history limits remain a real capacity boundary. Sustained-ingestion convergence, bounded cutover queues/fences, physical maintenance and reclamation remain broader unfinished work.

## Test review and qualification

The inspected additions in `tests/abandonment.rs` cover:

- Abandoned update/schema operations remain fenced while a new schema result preserves source boundary, exact inputs and Parquet objects; independent verification is invoked.
- One live admission among abandoned admissions blocks schema admission without changing lifecycle state; abandoning the remaining admission permits the metadata edit.
- Backfill prepares through readback while source input is pending, then survives reopen and removal of original input locators; completed records and exact source pin remain unchanged on selection.
- A later admission followed by abandonment cannot revive a captured schema/backfill operation; a new request can reuse completed work with a fresh fence.

The assertions match the traced paths and passed in the focused run. Existing schema cutover/source-worker tests and stale native-view tests remain relevant regression coverage. `tests/schema.rs:355` also forges an incremented format-11 boundary and requires independent verification to refuse it.

`tests/admission_formats.rs:84` reconstructs the original serialized operation shape, resumes format-10 build and schema admission, preserves an exact format-10 readback across reopen, then selects format-11 backfill/schema results and deep-verifies the mixed retained history. `tests/admission_formats.rs:166` preserves the old backfill gap refusal and proves a fresh format-11 request reuses completed files. Both passed. This is serialized-shape compatibility evidence, not execution of a historical binary; the fixture deliberately recreates retained state using the current native catalog.

The initial coverage notes are closed:

- Interval proof: `src/engine/retention.rs:266` and subsequent tests exercise complete/empty intervals without input payloads, legacy-schema versus composition classification, missing/duplicate boundaries, missing records, mismatched request/abandonment identities and digests, unknown formats, selected results in a gap, inconsistent revision/order/view boundaries and huge/reversed intervals. The reviewer read their assertions and the passing helper-test log. These are constructed lifecycle-record fixtures; public call-path behavior is covered separately above.
- Format compatibility: serialized-shape recovery, exact readback reuse, mixed-history deep verification and format-11 boundary refusal passed as described above.
- The first full repository gate failed in the defaults integration scenario with stack overflow; see `admission-fence-check-stack-failed.log` and the retained crash/frame evidence above. The corrected defaults retry and full `admission-fence-check-retry.log` both passed; F1 is closed. No broader acceptance is inferred from these runs.

## Conclusion

**VERDICT: APPROVE.** F1 is resolved by the reviewed construction boundary, measured narrower caller frame and successful normal-stack defaults/full-gate results. The cutover semantics fit R3/R5, changed-path coverage is adequate, and no material finding remains open. Confidence is high in the traced and qualified local behavior. Sustained-ingestion convergence, large-history scaling, remote-provider behavior and broader lifecycle/performance acceptance remain unqualified.
