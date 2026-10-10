# R5 qualified history discovery: independent implementation review

**Verdict: APPROVE for the local implementation increment.** No blocking defect found. This is a static review, not an execution result or full R4/R5/R6 qualification. High-churn history cost and an R5-specific active-retirement race remain unmeasured or untested here.

Reviewed the working diff against `e7078a7`, including the new files, on 2026-10-10. Scope: shared history extraction in `changes`, comparison adaptation, incremental join/summary discovery, and related-refresh fixtures. Separate metadata/hash changes and all prior review reports were excluded. Applied the Rust and semi-formal code-review guidance and read R4, R5 and R6 requirements. No builds, tests, formatting, source edits or cleanup were performed.

## Findings

### Observation: key materialization can grow with cumulative historical changes

Category: performance/resource qualification. Location: `src/engine/incremental/history.rs:58`, `src/engine/incremental/history.rs:87`, `src/engine/changes/chain.rs:11`.

The reader correctly streams raw removed and added keys from every qualified hop. The native writer persists those keys before downstream deduplication (`src/engine/incremental.rs:195`; `src/engine/aggregation.rs:233`). Consequently, repeated edits to the same large population can write a key relation proportional to the sum of historical delta rows, rather than just the final affected keys. The link and metadata bounds constrain planning; they do not establish a payload-byte or peak-disk bound. Buffer admission in `src/storage.rs:243` does not make these retained attempt objects DataFusion spill files.

This does not block correctness. Retain high-churn and peak-additional-disk qualification before claiming a complete-path benefit. A later cost crossover or deduplication should be justified against the existing conservative endpoint path. Do not interpret fewer discovery files as a measured latency or storage improvement.

### Observation: R5 lifecycle proof combines a new recovery fixture with existing shared-guard evidence

Category: test coverage. Location: `tests/related_refresh/history_keys.rs:281`, `src/engine/worker.rs:111`, `src/engine/retention/retirement.rs:501`.

The new fixture proves that an already completed processing phase remains reusable after its input's optional history is retired. The source trace and existing worker tests establish the shared admission behavior. There is no new deterministic R5 fixture pausing inside a multi-hop delta reader while retiring that exact certificate, or crashing before processing completion and then retiring history before retry. Do not describe the new fixture as proving those races. This is nonblocking because this patch reuses, rather than replaces, the owned-work and maintenance mechanism.

## Patch summary

The patch extracts local certificate-chain admission from comparison into one shared function. Comparison still applies signed occurrence/value semantics; dependent refresh instead reads both old and new key columns as a conservative affected scope. Missing optional history returns to existing changed-file discovery. Present invalid history propagates an error. The complete refresh keeps its existing recovery phases and input protections.

Main callers are `comparison_plan`'s chain branch (`src/engine/comparison.rs:366`), `process_incremental_join` (`src/engine/incremental.rs:155`) and `process_aggregation` (`src/engine/aggregation.rs:199`). Registration changes in `src/engine/changes.rs:7`, `src/engine/incremental.rs:3` and `tests/related_refresh.rs:1114` make the shared implementation and fixtures reachable. The aggregation helper visibility changes only support fixture reuse.

## Function trace

| Function | Evidence | Inputs and outputs | Verified behavior |
| --- | --- | --- | --- |
| `change_chain` | `src/engine/changes/chain.rs:44` | Captured local view, exact release endpoints, canonical source pins, expected native schema; qualified native deltas or explicit fallback | Captures lifecycle rows once; checks local result/certificate authority, predecessor continuity, causal revisions and schemas before opening delta payloads. |
| `charge` / `Chain` / `Delta` | `src/engine/changes/chain.rs:19`, `:25`, `:38` | Cumulative metadata sizes; native tables, row counts, signs and admission permits | Bounds chain planning and keeps decoded native metadata admitted with its consumer. The row sign is used by comparison, not by refresh key discovery. |
| `forward_relation` | `src/engine/comparison.rs:28` | Qualified deltas, visible fields, address requirement; optional DataFrame | Reconstructs the same signed frames as before extraction and gives them to the existing sequential provider with their retained memory permits. |
| Comparison chain branch | `src/engine/comparison.rs:366` | Comparable, nonidentical source endpoints and options | Preserves unsupported-group/schema/full controls and publishes `ForwardChanges` only for an eligible chain. No error-to-empty conversion was introduced. |
| `changed_keys` | `src/engine/incremental.rs:322` | Exact old/new releases, key names; native key table and partial file count | Resolves identical logical source identity before history work; otherwise attempts shared history and retains the existing endpoint-file fallback. |
| `history_keys` | `src/engine/incremental/history.rs:8` | Admitted releases, expected schema and required key columns; optional native key table | Uses local history as transient processing input. Reads every old/new key without algebraic cancellation, streams one delta inventory at a time and writes with the maintained native writer. |
| `process_incremental_join` | `src/engine/incremental.rs:119` | Captured old/new inputs and prior output; prepared replacement | Unions both changed-side scopes, matches old and new populations, and reconstructs affected joined rows and exact contributor tuples. |
| `process_aggregation` | `src/engine/aggregation.rs:184` | Captured input versions and grouping rules; prepared replacement | Uses the same discovery, recomputes complete affected groups and retains existing global/empty semantics. |
| `local_certificate` / `change_selection` | `src/engine/changes/certificates.rs:109`, `:160` | Captured catalog rows and exact result | Selected result must be verified and match dataset/version/manifest. Certificate and result must have the same local commit revision; retired or absent optional certificates yield no accelerator. |
| Native selected-file provider | `src/query.rs:82`, `:118`, `:371`; `src/storage/occurrences.rs:362` | Captured delta files and ordered key names | Resolves names to native column indices, preserves requested order, pushes field IDs into the native file task and drains files sequentially. |
| Ordinary refresh recovery | `src/engine.rs:750`, `:931`, `:959` | Captured request, processing outputs and coverage | Resolves a reusable processing phase before invoking discovery. Completion retains its own table, applied changes, base/input coverage and inherited protections, without introducing certificate-chain dependencies. |
| Owned work / retirement | `src/engine/worker.rs:111`, `:203`; `src/engine/retention/retirement.rs:501` | Service future and maintenance lock | Refresh owns a shared maintenance guard through actual worker completion. Retirement requires the exclusive guard. Caller cancellation does not abort the already admitted private worker. |

## Data flow and invariants

**Exact authority.** `history_keys` receives releases loaded from captured input coverage, not caller-provided change files (`src/engine/incremental.rs:47`, `:59`). `change_chain` resolves the producer's local lifecycle view and checks the canonical source metadata at both endpoints (`src/engine/changes/chain.rs:75`, `:156`). Local certificate binding checks co-commit revision, dataset ownership and exact result identity (`src/engine/changes/certificates.rs:92`, `:127`, `:175`). Catalog revisions are sequence numbers, not sortable snapshot identifiers (`src/catalog.rs:428`). The changed discovery path does not replace final input succession or output selection guards.

**Continuity and missing versus invalid.** Every predecessor revision decreases, which also prevents cycles; each applied descriptor binds the selected before/after source metadata (`src/engine/changes/chain.rs:112`, `:126`). A receipt-only hop must retain logical source identity (`:143`). Missing/retired optional certificates, unrelated order and bounded-history limits produce typed fallbacks. Invalid bindings, corrupted present descriptors, native-schema mismatches and native-reader failures remain errors; `history_keys` only converts the explicit `Forward::Fallback` enum into `None` (`src/engine/incremental/history.rs:23`).

**Complete affected scope.** Refresh does not consume comparison's net signed rows. Both removed and added tables supply their raw keys, including when equal keys represent new values or new contributors (`src/engine/incremental/history.rs:58`). The existing join union deduplicates only after discovery and matches both old/new sides with null-safe scope membership (`src/engine/incremental.rs:195`, `:209`). The actual join retains its declared null behavior. Summary grouping recomputes complete affected groups (`src/engine/aggregation.rs:233`). Net reversions may overstate work, but cannot omit affected groups by key cancellation.

**Schema interpretation.** The shared chain retains the earlier public Arrow-schema and native field-rule checks, including field defaults (`src/engine/changes/chain.rs:164`). The selected-file provider resolves each delta's required columns by name and native ID, then relabels the returned ordered columns into the shared key schema. It does not assume key columns lead the file. Unsupported endpoint schema changes still follow existing incremental qualification/refusal; this patch does not add schema-mapping acceleration.

**Memory and readers.** `Chain` carries parsed table metadata plus its reservation; `history_keys` retains that reservation through writing, append and final pin creation (`src/engine/incremental/history.rs:39`, `:87`). Native inventories open inside the lazy `then` closure. Pinned `futures-util 0.3.34` confirms that `Then::poll_next` owns only one pending child (`stream/stream/then.rs:64`) and `TryFlatten::poll_next` drops the current child before polling the next (`stream/try_stream/try_flatten.rs:56`). The underlying native provider repeats this sequential lifetime for its files (`src/query.rs:369`). There is no history-sized union of simultaneously active readers.

**Recovery and retention.** Refresh enters through `run_internal` and `admit_work` (`src/engine/refresh.rs:32`; `src/engine.rs:701`). The entire admitted future, not the calling HTTP/CLI future, owns maintenance custody. On cancellation, the worker can finish; abnormal private-worker termination poisons the host before releasing admission (`src/engine/worker.rs:56`, `:67`). On process death, uncompleted history-derived keys are attempt work, not reusable processing. Retry can rediscover through either currently eligible history or endpoint fallback. Completed processing is independent of optional input history, while the original inputs and base remain protected by ordinary retained phases (`src/engine.rs:931`). This is local host behavior, not distributed lease qualification.

**Metrics and complexity.** The new scan count counts delta files whose captured readers are consumed; global groups use known nonempty deltas without payload reads (`src/engine/incremental/history.rs:41`, `:71`, `:92`). Existing counters remain partial discovery/writer counters, as documented in `README.md:729`. Warm chain discovery scales with admitted history and delta-file metadata, but cold lifecycle replay and complete endpoint/input metadata still exist (`src/catalog.rs:208`; `src/engine/refresh.rs:458`). Later group matching, validation, contributor construction and writes can scale with population, fanout or affected-group size. No wall-time, whole-process memory or disk improvement follows from this static review.

## Tests inspected

The following are source-level coverage observations. Expected behavior is grounded in assertions and the call paths above; execution is owned by the parent task.

| Fixture | Assertions and expected behavior under this patch |
| --- | --- |
| `join_history_narrows_rewritten_files_without_cancelling_same_key_edits`, `tests/related_refresh/history_keys.rs:93` | Multiple hops on both sources, same-key edits, duplicates, key moves and new matches agree with an independent oracle and full rebuild. Stable unrelated output origins stay unchanged even though their input rows share rewritten files, distinguishing narrow history scope from the conservative fallback. Expected to pass. |
| `summary_history_preserves_untouched_groups_and_recomputes_same_group_measures`, `tests/related_refresh/history_keys.rs:207` | Group-preserving measure edits and group moves produce exact counts/sums/min/max and contributors; unrelated ordinary and null groups retain origins. Full reconstruction agrees. Expected to pass. |
| `completed_processing_resumes_after_optional_input_history_retirement`, `tests/related_refresh/history_keys.rs:283` | Stops after durable processing, retires and damages optional input history, then confirms plan reuse and byte-identical processing records through resumed selection. Expected to pass because reuse precedes history discovery. |
| `invalid_active_history_refuses_but_retired_history_falls_back_on_same_operation`, `tests/related_refresh/history_keys.rs:375` | Corrupt active descriptor refuses, leaves previous selected result and creates no processing completion. After repairing and retiring history, the same captured request resumes through conservative discovery and agrees with the independent oracle. Expected to pass. |
| `limits_and_wrong_revisions_do_not_become_empty_changes`, `src/engine/changes/chain/tests.rs:34` | Moved shared-chain fixture still tests history bound fallback and rejection of a certificate copied into another catalog revision; forced full remains an independent value oracle. Expected unchanged after extraction. |
| Existing related refresh cases, `tests/related_refresh/incremental.rs:125`, `:409`; `tests/related_refresh/aggregation.rs:174`, `:248` | Exercise both input orders, declared join/null semantics, contributor changes, grouped/global empty transitions and worker recovery after durable phases. Their ordinary update certificates now flow through shared history. No test was treated as proving an unasserted path choice. |
| `reversed_nonleading_keys_refresh_joins_and_summaries_with_exact_evidence`, `tests/related_refresh/selected_keys.rs:91` | Exercises reordered nonleading composite keys and exact evidence through the new native selected-column path. Expected unchanged semantics. |
| Existing comparison forward/default cases, `tests/comparison/forward.rs:121`, `:220`; `tests/comparison.rs:394` | Cover compaction, reversions, retired intermediate results, receipt-only hops, corrupted present evidence and schema-rule reporting. The extraction preserves their eligibility and composition path. |
| Shared lifetime cases, `src/engine/worker/tests.rs:35`, `:181`, `:215`; `tests/comparison.rs:453` | Assert actual worker custody through cancellation and stream admission through EOF/drop. These support the common mechanism; the last comparison fixture uses identical endpoints and does not directly qualify an R5 delta-reader race. |

## Conclusion

VERDICT: APPROVE

- The patch supplies a shared, qualified local history path for affected-key discovery without cancelling same-key edits or adding durable chain dependencies.
- Coverage of the changed correctness paths is adequate for this increment. Dedicated R5 in-flight history-retirement/crash races and scale/resource acceptance remain incomplete.
- Confidence: high in the static authority, data-flow and lifetime traces; runtime behavior and performance require the parent-owned gate and subsequent qualification.
