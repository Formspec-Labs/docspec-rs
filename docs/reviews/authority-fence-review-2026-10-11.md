# Authority epoch fence: independent static review

Reviewed current `src/engine/authority.rs`, `tests/authority.rs` and narrow
Engine/CLI/HTTP registration changes. Verdict: **approve**, no actionable static
finding. No Cargo/provider execution; namespace-generation changes reviewed
separately.

The service fixes `next = expected_epoch + 1` before admission and refuses
overflow. It loads only an existing lifecycle table, never creates a missing
dataset, and acquires the existing dataset lock. Current `next` reconciles the
same monotone transition without a commit; current `expected` commits only the
Authority epoch; any other epoch refuses. Thus an old retry cannot advance or
silently adopt a later transition. Reconciliation after another selection at the
same epoch preserves that new selection.

`Lifecycle::commit` performs the existing exact native-head guard and maintained
snapshot CAS. There is no internal conflict retry with a refreshed expectation.
A concurrent update cannot be overwritten by an old Authority value: it changes
the guarded head and causes a conflict. All authority fields except epoch remain
unchanged; payload/public pin identity and durable phase records are untouched.
The tiny lifecycle commit writes metadata, not a source batch/payload scan.

Existing `check_preparation` and `check_authority` compare the originally admitted
operation epoch. The retained operation cannot relabel its epoch: admission checks
the saved request/operation first and reuses immutable admission. Completed-result
acknowledgments remain independent, while an unselected retained readback must pass
fresh publication authority. A replacement operation captures the new epoch and
can reuse otherwise eligible material.

HTTP uses the same service, denies unknown JSON fields, and maps stale_authority
to409. CLI forwards explicit dataset/expected epoch. The real fixture pauses an
Update at retained readback, checks exact unchanged non-authority records and old
selected/public release, exercises HTTP and idempotent retry, refuses the stale
operation, publishes a replacement, then checks stale-transition409, overflow
and missing dataset refusal. Runtime qualification remains root-owned.

Scope: this fences lifecycle publication through supported Engine services.
It does not revoke credentials, cancel accepted object effects, stop readers,
prove cross-host/global quiescence or qualify destructive remote maintenance.
