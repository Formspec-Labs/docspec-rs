# Restore-only provider continuation review

**Verdict: APPROVE for the retained pre-detach failure.** Static review of patch `3fe3db54c54ba5f35c0acd7562ce692145d97d3576eb9baf031aaaa44e7a5937`; no execution, provider access or shared edits. No actionable finding.

## Scope and trace

| Function | Current source | Verified behavior |
|---|---|---|
| `read_handoff` | `src/engine/remote_build_provider_tests/maintenance.rs:33` | Bounds each exact saved handoff to less than 1 MiB and rejects unknown typed fields. |
| `restore_saved` | `maintenance.rs:790` | Binds dataset, generation, prefix, source/export relationship, live point and the sole removed object before entering restore. Requires complete saved removal and exact equality with native acknowledgment. |
| `inspect_removal` | `src/engine/retention/removal.rs:461` | Reads validated saved intent/progress under admission; does not delete or read payload. |
| `finish_restore` | `maintenance.rs:900` | Reuses saved target generation; uses a fresh control folder and actual current epoch for an independent old actor. Calls existing restore, stops after detach, then resumes with the same false import mode on a replacement host. |
| `old_actor_child` | `maintenance.rs:307` | Loads the source physical table, holds that exact native commit and records native refusal. The forwarding implementation passes the original commit unchanged at `publication_race.rs:133`. |
| restore service | `src/engine/restore/remote.rs:378` | Checks target intent, surviving source authority and captured inventory before detaching; registration recovery and authority fence remain shared production code. |

## Invariants and assertions

The continuation dispatch bypasses Build, export, retirement, capture, credential minting and deletion. It verifies the retained live source/export pins and completed removal metadata (`maintenance.rs:850–886`). The new control directory prevents stale `old.metadata` or outcome files from satisfying the new handshake (`834`, `950`). Owned child teardown kills/reaps on failure (`286`), and hold/forward/overall deadlines remain bounded (`247`, `1001`, `362`). Error receipts emit stable codes/kinds/status only (`44–69`).

After detached recovery, the fixture checks native old-identifier refusal, distinct child PID, exact retained selected pins, literal source/export values and target writability (`1009–1050` and following tail). Namespace-list absence and server-managed namespace properties are production adapter concerns; this continuation does not bypass their checks.

## Limits

This is an opt-in test driver for the already recorded completed deletion and pre-detach restore refusal. It is not a general restart-from-any-stage driver. It requires the source lifecycle to remain present and does not repeat the successful token-expiry qualification. Its receipt checks plus shared service enforce the declared isolated scope; hosted success is pending root execution. Prior failed attempts must remain retained as specified in the protocol (`experiments/remote-maintenance-provider-protocol.md:36`).

Confidence: high for control flow and scope; provider behavior remains unqualified until execution.
