# Writer progress static review

VERDICT: APPROVE. No actionable static finding. Artifact SHA256 `65fee94ab7866273949a873a9171b8a9b6f3b78741cfab76957df8c1e173e28b`. Sources reviewed under `/tmp/docspec-writer-progress/new`; shared source/callers and maintained local tracing source were also read. No build, formatter, provider request or shared edit was performed.

## Patch summary and trace

| Boundary | Source | Verified behavior |
|---|---|---|
| Phase observation | `src/progress.rs:12–40` | Poll creates timer inside the instrumented future; start, finished/failed, or drop-with-unknown are mutually exclusive. Logs refusal code, never error text. |
| Native writer observation | `src/progress.rs:43–82` | Counters accumulate accepted rows and decoded Arrow bytes; first accepted batch and at most one later sample per second. Close/failure reports exact accumulated totals; drop reports unknown outcome. |
| Shared write path | `src/storage.rs:271–312,372–482` | Existing bounded writer/admissions preserved. Counters advance only after maintained `writer.write` succeeds; final success occurs only after maintained close. No commit, payload reread or new worker. |
| Quiet lifecycle write | `src/catalog.rs:399`; `src/storage.rs:294–304` | Actual catalog commit routes through the same writer with progress disabled. No second writer implementation or lifecycle-row event noise. |
| Engine phase caller | `src/engine.rs:973–980,1093–1099,1547–1563,1827–1853` | Only missing admission/processing/validation/readback branches are wrapped. Existing retained phase/readback branches bypass observation; completion and protection checks remain inside original work. Selection remains separate. |
| Module registration | `src/lib.rs` | Private module and existing unit-test discovery; no public telemetry API or persistence. |

The writer's byte counter uses the existing `get_array_memory_size` measurement after necessary origin materialization (`storage.rs:379–389`), not encoded Parquet size, RSS, unique live allocation or source bytes. The wording consistently calls it decoded bytes accepted. Empty batches do not contribute. Rolling writer close/restart remains inside one logical write call, so the totals include all returned files. Rows buffered before a failure are expressly not advertised as committed.

Phase success reports work-future return, not channel selection or universal durable success. The readback event precedes `select_prepared` (`engine.rs:1888–1891`); its label does not claim selection. A lost catalog response can therefore produce failed work despite a remote commit, and the event explicitly directs callers to retained completions. Drop events use outcome unknown. Runtime custody may keep the phase future alive after a caller disconnects; no caller cancellation claim is introduced.

Maintained tracing `Instrumented` uses pinned drop to enter the span before dropping its inner future (`tracing-0.1/src/instrument.rs:266–289`). The async helper uses `Instrument`, not a span-enter guard held over await. Thus abandonment is attributed under the phase span without leaking span entry across suspension. Operation ID and an already-created synthetic writer UUID are emitted; values, input paths, configuration and provider bodies are absent. There is no heartbeat for a stalled upstream read and no total/proportion/ETA invented.

## Tests and limits

- `src/progress/tests.rs:81–131` runs a real maintained native writer with one accepted three-row batch, holds the next input, independently checks row/Arrow-byte counts, then exercises explicit input failure and dropped future. It asserts no closed/finished event, mutually exclusive failed/abandoned terminal events, and omission of sensitive error text. The fixture has no origin columns, so its expected materialization remains the same Arrow batch (`identity::materialize` returns unchanged when the origin field is absent).
- `src/progress/tests.rs:135–154` checks real writer-close file/row totals, absence of a selected snapshot, and explicit quiet catalog-writer behavior.
- `tests/lifecycle.rs:213–258` extends the existing actual CLI routine-once fixture: first run observes writer/phase work; completed resume and ordinary read observe neither native writer nor phase-work start. Existing inspection counts and literal read checks remain.

The tests have not run during this review. They cover the principal real writer lifetime and completed-retry boundaries; they do not qualify a live provider, resource-performance improvement, total pipeline progress, evidence/backfill phase observations or full R7 completion. The event sampler adds constant arithmetic and a clock read per accepted batch, with formatting only for emitted samples; no speed or RSS claim is made.

Conclusion: the patch provides bounded observations of existing necessary work while keeping recovery authority in existing records. Static coverage is adequate for this scoped change; confidence high on traced behavior, runtime verification remains parent-owned.

Final precision delta approved: SHA256 `65265e3ebcde5c5d940e117ec1da7d4c19c0f8fb675ef9dad252c1e3dd97c223`. Phase helper and all four Engine callers now include exact dataset ID alongside operation; held test supplies both. This removes ambiguity for repeated operation IDs across datasets. The byte-counter comment explicitly documents repeated shared-buffer accounting. No behavior/lifetime or authority change; preceding verdict remains APPROVE for this final artifact.

Test-only lexical guard-lifetime supplement approved: `/tmp/docspec-progress-test-scopes.patch`, SHA256 `86c9ef0cd3e594a5e3b68fb4d7c83795639c26d6ec163083a3481a8541e20616`. The two inspection guards end in explicit blocks before subsequent awaits; all assertions and observed writer behavior are unchanged. This supplements applied `65265e3e…`; it is not a replacement production artifact.

Runtime test correction reviewed: `/tmp/docspec-progress-drop-capture.patch`, SHA256 `3a308e3fb2db23344907aa8936f41b3c8be9c9187a7fe41ce8e702b613034536`, APPROVE. Parent reports held writer test failed only because `native_writer_abandoned` was not captured; success/quiet and actual CLI retry cases passed. The review's earlier span-drop trace was correct but incomplete for a test-scoped dispatcher: maintained `WithDispatch` installs its dispatcher during poll, not Drop. The supplement reinstalls the retained dispatcher only while dropping the actual held future. Production CLI already installs a global subscriber; no production semantics change. Both abandonment and mutually exclusive terminal assertions remain. I did not execute the correction.

## Executed checks

`writer-progress-drop-fixed-2026-10-11.log` passes both real native writer cases:
progress before completion, distinct failure and abandonment, successful close
before snapshot commit, and quiet catalog writing. The first failed capture
remains in `writer-progress-focused-2026-10-11.log`; the fixture now keeps its
subscriber active during explicit future drop. No production behavior changed.
The actual CLI create/resume/read case passes in
`schema-policy-progress-focused-2026-10-11.log`; that invocation's separate schema
refusal-code fixture is corrected in `comparison-schema-refusal-fixed-2026-10-11.log`.
