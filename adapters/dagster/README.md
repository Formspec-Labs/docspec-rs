# Schedule Rust DocSpec operations with Dagster

This adapter submits an existing native request to an existing local Rust HTTP
host. Rust owns operation identities, retained phases, selection and recovery.
Dagster owns scheduling and step retries. Python stores no lifecycle state.

Start the Rust host with the normal `docspec --workspace <path> serve --bind
127.0.0.1:8080` command. The adapter does not build, launch or stop it. One host
owns the workspace; simultaneous CLI workers cannot open that same workspace.

## Use

Install this directory in the Python environment used by Dagster. The package
requires Python 3.11–3.14 and pins Dagster 1.13.16. Its HTTP transport uses the
Python standard library and can be tested without Dagster installed.

```python
from pathlib import Path

from docspec_dagster.definitions import DocSpecResource, docspec_job

# This file contains the original Rust RunSpec, including dataset and operation.
# Keep it unchanged when retrying or reconciling this operation.
request_json = Path("run.json").read_text()
result = docspec_job.execute_in_process(
    run_config={
        "ops": {
            "docspec_operation": {
                "config": {"route": "/run", "request_json": request_json}
            }
        }
    },
    resources={
        "docspec": DocSpecResource(base_url="http://127.0.0.1:8080")
    },
)
```

For a Dagster deployment, provide `docspec_job` and `DocSpecResource` through
ordinary `dagster.Definitions(jobs=[...], resources={"docspec": ...})`. Each
worker must reach the same host through its loopback address. Container loopback
addresses name that container; this adapter does not establish cross-host access.

The allowlisted POST routes are `/plan`, `/run`, `/updates/run`, `/schemas/run`,
`/receipts/run`, `/backfills/run`, `/refreshes/run` and `/exports/run`. `/plan`
accepts a native `RunSpec`; each other route accepts its corresponding Rust request type. The
adapter checks bounded JSON and identity fields, and Rust validates the operation.
It does not expose maintenance, deletion, restore or arbitrary HTTP routes.

Only literal loopback IP addresses are accepted, such as `127.0.0.1` or `[::1]`.
Hostnames, credentials, paths, query strings and fragments in the base URL refuse.
Requests bypass proxy environment variables. Redirects are not followed.

## Retry and result meaning

- The original JSON bytes and native operation ID are sent unchanged on each
  retry. Dagster run IDs and retry numbers never become Rust operation IDs.
- A structured Rust refusal stops the step without automatic retry, even when
  the caller configures a Dagster retry policy. This includes capacity and stale
  selection refusals. Resolve the reported cause before retrying deliberately.
- Connection failures, truncated replies and unstructured server failures leave
  the outcome unknown. Dagster retries the same request up to `uncertain_retries`
  (default 2), with `retry_delay_seconds` between attempts (default 1).
- Exhausting retries means the Dagster step failed to learn the outcome. Rust
  may have completed the operation. Resubmit its original request to reconcile;
  do not invent a replacement ID or infer failure from a timeout.
- Invalid, oversized or mismatched responses stop automatic retry and explicitly
  preserve uncertainty. Increase the response allowance when appropriate, then
  retry the same request. A rejected response does not roll back Rust work.
- Cancellation or a dropped connection does not abandon work in Rust. Use the
  engine's supported lifecycle operations when an operation must be abandoned.

The op returns and emits only route, dataset, operation and a small acknowledgment.
A run acknowledgment identifies the completed result. A plan acknowledgment
reports whether Rust already holds a completed result; planning alone does not
execute it. Full pins, file inventories, request JSON and dataset values are not
copied into output metadata. Dagster itself retains op configuration, so requests
must contain metadata only and must not contain credentials or dataset payloads.
Refusal events preserve the code and HTTP status; the message is capped at 2,048
characters with an explicit truncation flag. Direct client exceptions retain the
complete refusal within the response allowance.

## Bounds and verification

Request JSON is limited to 1 MiB, matching the Rust HTTP router. Responses default
to 1 MiB and can be configured up to 32 MiB. A response is read under that limit
whether it has `Content-Length`, uses chunked transfer or closes without a length.
Compressed responses and ambiguous framing refuse. These are serialized metadata
limits, not measured Python memory limits. The socket timeout defaults to 60
seconds per socket operation; it is not a deadline on Rust's retained operation.

From this directory, after arranging the intended environment:

```sh
PYTHONPATH=src python -m unittest discover -s tests -v
```

The transport tests use real loopback HTTP for size limits, framing failures,
identity mismatches, redirects and refusals. When Dagster is installed, native
`execute_in_process` tests exercise resource injection, actual retry events,
unchanged requests and refusal handling. Without Dagster, those native tests are
reported as skipped. Their HTTP peer is a protocol fixture; these tests do not
qualify the Rust lifecycle, multi-host recovery, hosted authentication or R2/Basin.

The optional real-host tests require an already built Rust executable with the
`test-util` feature and a tiny
Parquet fixture (at most 1 MiB). It adds no PyArrow runtime dependency:

```sh
DOCSPEC_BINARY=/absolute/path/to/docspec \
DOCSPEC_TEST_INPUT=/absolute/path/to/tiny.parquet \
PYTHONPATH=src python -m unittest discover -s tests -v
```

These tests obtain the input digest from `docspec hash` and start the real host
in a temporary workspace with the Rust correctness-fixture resource profile.
They cover clean replay, native Dagster retry after a lost completion response,
and a fresh Python worker reconciling that response loss. A loopback relay
forwards the unchanged request to Rust, consumes the entire successful native
response, captures the selected pin, lifecycle rows and retained files, then
closes the Python worker connection without returning response headers. The
recovery request must have exactly the same bytes, and the recovered full native
pin must match. File hashes, sizes and modification times and lifecycle records
must remain unchanged, detecting new retained output and rewrites. This is local
filesystem evidence; it does not measure native input reads or CPU work.

The first replacement-fixture worker exhausts its configured zero retries and
reports `outcome: unknown`, then exits. A separate Python process loads the
original request configuration and reconciles it. Their process IDs must differ.
This response-loss fixture qualifies replacement after a worker failed to learn
native completion. Its Rust host stays alive throughout.

Separate worker-death cases use the native `test-util` pause before validation
starts, after Rust durably retained processing. The real HTTP handler remains
unfinished. The fixture checks native processing records and files, then sends
SIGKILL to the Python worker while it still waits for the response. It deletes
only a private copy of the input and starts a fresh worker with the unchanged
configuration and request digest. One case releases the pause on the existing
Rust host and waits for that exact operation to complete before replacement.
Another sends SIGKILL to that host before releasing the pause and
reopens the same workspace in a fresh host without pause configuration.

The worker records entry and exit at the real HTTP submission boundary, before
Dagster handles its outcome. Inspection and interruption must finish within
30 seconds, below the worker's 60-second socket timeout; the waiting record
must remain unchanged through SIGKILL.

Recovery must preserve the exact processing records and every already retained
file's hash, size and modification time. The native `DOCSPEC_TEST_WORK` trace
must show processing, validation and readback exactly once across interruption,
recovery and completed replay. The completed replay must preserve the full pin,
lifecycle records and retained-file inventory. These are local acknowledged
processing-boundary cases, not arbitrary-instruction crash or power-loss tests.
The test hook is feature-gated and absent from ordinary production builds; it
adds no Python lifecycle state or public HTTP route.

The relay accepts at most two requests, with the same 1 MiB request and 32 MiB
response ceilings used by the adapter. Each replacement worker has a 90-second
fixture deadline and is killed on expiry; expiry fails the test. Native readbacks
and relay socket operations have 30-second timeouts. The file comparison refuses
more than 64 MiB or 2,048 retained files in this tiny fixture. These test limits
are failure bounds, not performance or whole-process memory claims; in-process
Dagster execution has no aggregate elapsed-operation deadline. Host readiness and
graceful shutdown each have a 30-second deadline; forced host cleanup fails the
test. Without both explicit paths, native integration is skipped.

`/receipts/run` uses the same native release acknowledgment as other selected
results. Transport coverage checks its unchanged identity summary. The real-host
fixture sends a well-shaped receipt request with a missing base and requires
Rust's structured `stale_base` refusal; this proves route forwarding and native
refusal handling, not successful receipt materialization. Native receipt
processing remains covered by the Rust evidence-update tests.

The retained [completion-response recovery run](../../docs/reviews/dagster-recovery-check.log) passes
with Python 3.13 and Dagster 1.13.16 against the `ce3ad6b` Rust executable,
including both native completion-response recovery cases without skips.
The [independent review](../../docs/reviews/dagster-recovery-review.md) approves
this scoped local qualification. The
[worker-death and host-restart run](../../docs/reviews/dagster-inflight-tests-2026-10-10.log)
passes the complete adapter suite, including both actual-kill cases. Its
[independent review](../../docs/reviews/dagster-inflight-recovery-review-2026-10-10.md)
approves the repaired timing and live-host completion checks.
Successful receipt materialization through Dagster, other lifecycle routes and
real-provider recovery remain separate qualification work.
