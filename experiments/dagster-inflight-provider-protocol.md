# Retained provider Update after Python worker and Rust host loss

Frozen before provider execution. Root owns execution and credential resolution.
This is one opt-in recovery case, not a performance benchmark.

**Decision:** Can the existing Dagster adapter and public remote Rust services
finish an unfinished source-only Update from its durable processing completion
when both original processes and the original local producer are gone?

**Hypothesis:** A new Python worker submitting the identical request to a Rust
host with a fresh local directory reuses the exact provider processing record
and native table, performs only the remaining validation/readback/selection,
and leaves the old immutable version readable. New processing, a changed native
pin, inability to recover without the original file, or incorrect values refutes
this claim.

**Arms:** The same operation before and after deliberate `SIGKILL` of its actual
Dagster Python worker and Rust HTTP host. Both use real R2/Basin and the same
configured catalog/prefix. These are independent OS processes on one computer;
this does not qualify two computers or a network partition.

**Case:** Use only the already qualified synthetic namespace
`docspec-rs-qualification-0e38b494-34a4-47e4-a96e-f8ae27822f00`, under its
`engine-update` owned prefix. Require the current selection to match the exact
`update-two` manifest in the passed retained-provider receipt. Use the fixed new
operation `dagster-inflight-update`; refuse if its request or operation already
exists. Do not silently choose another identity after failure.

The prior selected source contains `(counter, text)` values
`[(1, "last"), (1, "last"), (1, null), (3, ""), (4, "keep")]` and the synthetic
private-canary column. Replace key `1` with three rows
`[(1, "after host loss"), (1, "after host loss"), (1, null)]`.
The producer is one locally generated Parquet file smaller than 64 KiB.
Do not export or create another pipeline operation.

**Held constant:** Same request JSON, dataset, definition, captured base, input
digest, remote catalog/prefix, executable, and small resource profile. Record
executable, runner, helper and protocol SHA-256 digests and package versions.
Use the existing test-util pause immediately before validation. It runs only
after processing completion and before validation starts; no native table or
lifecycle implementation is replaced.

**Procedure and observations:**

1. Start the configured Rust host on loopback in a fresh directory. Inspect only
   the existing dataset and enforce the exact selection/scope guards above.
2. Submit `/updates/run` through the real `docspec_job` in a separate Python
   process, with automatic uncertain retries disabled. Wait for the existing
   validation pause marker. Capture the completed processing `PhaseResult` and
   the plan's exact record key/native revision. Require removed/added counts
   `3/3`, key field ID `2`, and the old base manifest. No new readback/result
   may exist and the old selection must remain current.
3. Require the Python report still to say its request is waiting. Kill and reap
   that Python process, then kill and reap the paused Rust host. Do not release
   the pause before termination. Delete only the runner's exact tiny external
   producer file; retain every admitted remote input and native output.
4. Start a new Rust process with a different, fresh local host directory. Before
   resubmission, the plan must name the identical processing key/revision and
   inspection must return the identical `PhaseResult`. Submit identical request
   JSON through a new actual Dagster Python process. Require success and the
   unchanged processing record/native pin. The first host work log must contain
   processing once and no validation/readback; the replacement log must contain
   no admission/processing and exactly one validation and readback.
5. Read the old and new immutable source through their exact HTTP Arrow routes,
   once each. Independently decode using PyArrow. Assert the full literal five
   rows, multiplicity, null/empty distinctions, field order, IDs `1/2/3`, types,
   nullability and version/digest response headers. Retain these stream digests;
   no repeated deep reconstruction is required.

**Bounds and failure handling:** One case, no automatic replay/rekey or remote
cleanup. Existing local Dagster worker tests keep their 60-second default; this
opt-in provider case permits 240 seconds for one HTTP submission and 30 seconds
for worker exit. The complete runner has a 300-second Unix alarm. The initial
processing wait is capped at 180 seconds, native pause capture at 20 seconds,
HTTP metadata at 32 MiB, Arrow output at 1 MiB per immutable version, and decoded
rows at five per version. Rust uses two compute threads, 512 MiB aggregate
configured memory, 16 MiB scratch, batch size eight, and a 4 MiB metadata bound.
These are configured limits, not measured peak RSS or transfer volume.

On timeout/error, kill/reap only owned Python children and stop only the Rust
hosts started by this runner; cleanup has its own bounded waits. Retain failure
receipt, logs, request, exact phase evidence, both host directories and all
remote objects. Failed runs are not overwritten. A later attempt requires
explicit reconciliation of this fixed operation; this script refuses to start
it again. Python workers receive no `DOCSPEC_*` environment values; the Rust
host alone resolves the existing credential environment names. Do not write
credential values to files.

**Decision rule:** Pass only if every assertion holds and the replacement Rust
host shuts down successfully. Failure remains a failed/inconclusive case until
its cause is explained; do not count a completed-response replay as a pass for
this interrupted-work scenario. A pass qualifies this actual provider recovery
boundary through Dagster and public CLI/HTTP startup. It does not qualify unknown
native-commit outcomes, power loss, arbitrary types, remote destructive
maintenance/restore, full prefix storage/request accounting, or throughput.

## Root execution

Use the already retained Python environment containing Dagster and PyArrow, and
an already built CLI with `remote,test-util`. Resolve only the existing provider
credential reference into the same environment names used by
`experiments/dagster-retained-provider.py`; set `DOCSPEC_BINARY` and the exact
`DOCSPEC_QUALIFICATION_ID` above. Run normal Python (no `-O`):

```sh
<qualified-python> experiments/dagster-inflight-provider.py <fresh-absolute-evidence-directory>
```

Capture stdout/stderr outside that directory before invoking the runner so
preflight failures are retained too. No build, provider access, or secret access
was performed while authoring this artifact.

## Observed failure and explicit reconciliation

This one-operation probe is terminal and must not be rerun against its existing
identity. The first preflight failed before a dataset request because the `http`
helper shadowed the module; the direct `HTTPConnection` import corrected it.
The second attempt reached durable processing, but its inspection request blocked
behind the paused operation and exceeded the capture deadline. Cleanup released
the pause, killed the Dagster worker, and eventually killed the Rust host after
readback had completed. The fresh inspection proved the old release remained
selected, the new readback existed, and no new result had been published.

`dagster-provider-reconcile.py` explicitly resumes that retained unfinished
operation. Its original source is removed only after exact inspection; a new
Dagster worker and fresh host directory reuse the completed readback and phases,
publish, and independently read exact old/new values and schema. The failed
processing-only interruption case remains failed; the follow-up qualifies the
observed after-readback/before-publication loss boundary. No processing is repeated
or rekeyed to hide the failed attempt.
