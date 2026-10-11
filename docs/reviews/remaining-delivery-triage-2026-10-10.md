# Remaining delivery triage

Static triage on 2026-10-10. No execution or Rust changes. This checklist preserves
[PLAN](../PLAN.md), R1–R7 and [research acceptance](../requirements/RESEARCH.md).
It does not turn unsupported behavior or fixture evidence into completed delivery.

## Critical path

1. **Reconcile current status first.** `STATUS.md:2154–2158` still describes only
   a retained source-processing slice. Its full local Comments acceptance section
   already records shaping, joins/summaries, evidence, both outputs, selection and
   exact independent readback. Preserve that passing result and its scope. Also
   change “General incremental SQL remains unfinished” at `STATUS.md:2138` to
   explicitly deferred: R5 requires supported equality joins, one-to-many
   replacement and affected-group recomputation, with correct full fallback.
2. **Close narrow local integration gaps before broadening types.** Stable-field
   rename/reorder comparison is an actionable R3/R4 seam: comparison currently
   requires Arrow schema equality (`src/engine/comparison.rs:279`) and refuses
   incompatible streamed endpoints (`:424`). Normalize compatible fields by their
   retained IDs using existing schema/read mapping; retain separate schema effects.
   Check exact unchanged values across rename/reorder and refuse changed meaning.
   This is a proposed change, not a finding that existing refusals corrupt data.
3. **Complete lifecycle reachability and maintenance integration.** R6's selected
   representation, generation-seal and descendant bindings must have explicit
   promises and retirement, rather than retaining data forever or dropping roots.
   Reuse `retention/inventory.rs:919,1203,1430`, `retention/retirement.rs`, and the
   existing removal service. `compaction.rs:73` already resolves qualified physical
   representations; wire remaining related-input readers through that mechanism
   before adding a parallel compaction path. Contributor compaction and selected
   representation retirement remain implementation work, not provider blockers.
4. **Finish shared dependency behavior where affected consumers exist.** The
   scalar correction implementation (`src/engine/corrections.rs:137,217`) already
   retains qualification and rechecks adoption. R5 receipt/policy changes must
   similarly rerun affected checks while retaining unrelated values. Extend shared
   dependency declarations/guards; conservative recomputation is valid for unknown
   dependencies. Do not invent generic selective-invalidation infrastructure.
5. **Implement remote delivery through retained phases.** R2 upload, transfer reuse,
   independent remote readback, catalog release selection, and actual provider
   recovery are accepted delivery. `src/engine/export.rs:56,273,309` already owns
   local export phases and selection guards; reuse these boundaries. Engine
   catalog injection exists (`src/engine.rs:213`), but that alone is not a remote
   storage implementation. Keep one catalog-selected release authority per PLAN.
6. **Qualify the resulting complete path.** Existing full local Comments timing
   passes; unchanged/update/backfill/resume costs and Q1–Q8 still require their
   stated evidence. Run focused missing boundary cases, then the required gate.
   Do not rerun unrelated benchmarks merely to create a fresh status timestamp.

## Independent small steps

- **Dagster worker loss:** extend the existing real-host fixture from completed
  response loss to killing the Python worker during native processing, then retry
  the original request. Rust owned-worker behavior already exists. Host crash and
  remote-host qualification are separate cases; see `adapters/dagster/README.md`.
- **Continuous backfill evidence:** reuse `tests/continuous_backfill_process.rs`
  and `tests/continuous_backfill_retention.rs` for the explicitly missing live-fence
  death/native-commit/reader-removal boundaries. Recent acknowledged-handoff
  SIGKILL and obsolete-output removal are already implemented and evidenced.
- **Progress/cost evidence:** attach missing counters to existing I/O and phase
  boundaries. R7 requires actual passes/bytes/resource observations; counters need
  no second full scan and unknown whole-process allocations must remain explicit.
- **Consumer declaration:** record actual promised readers, types, evidence access,
  authentication, disclosure and availability before widening distribution support.
  This decision can proceed alongside local code; no new format is needed yet.

## Scope classification and actual prerequisites

| Class | Items and treatment |
| --- | --- |
| Accepted behavior still open | Remote publication/recovery; material policy/evidence invalidation; stable schema meaning and supported type changes; exact historical value/evidence promises; maintenance integration; required operating instructions. Keep R1–R7 open until supported paths and their checks satisfy them. |
| Evidence gaps | Broader durability, Q1 aggregate memory, Q2 ingestion/compaction races, Q3 shared cleanup, Q6 restore/old actors, Q7 independent metadata axes, Q8 actual consumers; continuous-backfill race/resource boundaries; unchanged/update/backfill/resume performance. Existing tests may close individual cases, never the whole matrix by implication. |
| Explicitly deferred | General incremental SQL, universal indexes, automatic failover, automatic orphan sweeping and arithmetic aggregate deltas. They are not completion prerequisites without a new need/scope decision. |
| Supported-boundary decisions | Arbitrary nested/type evolution, computed input groups, broader key/aggregate types, portable change-certificate trust and partial computed promotion need explicit supported cases. R3 still requires stable meaning and qualified evolution; an “unsupported” label cannot erase its obligation. |
| Actual external prerequisites | A selected R2/Basin catalog/storage endpoint, usable scoped credentials, owned qualification namespace, deployment topology and enforced stale-writer/deleter fencing for provider tests. This audit did not inspect credential availability; absence is not established. |
| Actual consumer/product inputs | Named readers and their type/delete/evidence requirements; disclosure policy and row/column scope; release/recovery availability windows; acceptable freshness/backlog and numeric non-full-run targets; supported disaster-recovery boundary. Do not fabricate these numbers or infer public authorization from local readability. |

Nested/combined exports and row filtering are not implemented (`src/export.rs:15`
and `src/engine/export.rs:648`). Their concrete shape depends on the consumer and
disclosure decision; required authorized delivery cannot be marked complete merely
because the current primitive-column export refuses broader requests. The same
distinction applies to broader scalar correction versus a safe full recomputation.

Completion means accepted behavior plus its required evidence and provider/consumer
checks. A concise status can be finished immediately; full plan completion cannot
honestly be obtained by deleting its remaining obligations.
