# Continuous scalar process recovery qualification

Implemented actual SIGKILL qualification in `tests/continuous_backfill_process.rs`.
The existing shared process harness waits for the CLI's acknowledged durable pause,
kills that worker, verifies signal 9 on Unix, and starts a fresh CLI for resume.
The only CLI change adds `continuous-backfill` to the test-only pause dispatch.
The suite reuses the continuous suite's fixture helpers; root registers this suite.

The phase matrix covers `continuous-admission`, `files`, `processing`,
`validation`, `readback`, and `selection`. It asserts the exact pending/completed
phase records, immutable retained completions, exact parent-to-child resolution,
selected source and boundary, and the phase trace containing only missing work.
The original ingestion file is removed before recovery. A second fresh CLI retry
returns the exact selected child without running processing phases.

Every output value is compared with direct Rust reconstruction, including null,
empty, Unicode, duplicate-key and null-key occurrences. Native source occurrence
coordinates are compared with computed coverage, requiring exactly one computed
value per source occurrence. The origins file must match retained file completions
and account for all covered rows.

Source advancement before recovery is covered after `files` and `readback`:
replacement and insertion create a successor source, original admission files are
removed, and a new child catches up. The test requires original file evidence to
remain exact, unchanged files to reuse original executions, changed files to bind
new executions, and the phase trace to compute only the new file generations.

Verification has not run in this agent: root owns builds, formatting, tests and the
repository gate. After root registers the module and exposes the shared helpers:

```sh
cargo test --locked --all-features --test integration continuous_backfill_process::
./scripts/check
```

This is local Unix process-loss qualification, not remote-host failover or disaster
recovery. Former-host death while holding a live intake fence remains unqualified:
the existing pause hooks acknowledge child completions, but provide no durable
pause during fence acquisition or accepted-source drain. Pausing after a completion
returns through fence-close handling before the CLI marker, so it cannot prove a
live fence survived SIGKILL. A deterministic test would require an acknowledged
pause immediately after fence acquisition and before drain/close; no new production
hook was added. The suite does not depend on sleeping until a lease or race occurs.
