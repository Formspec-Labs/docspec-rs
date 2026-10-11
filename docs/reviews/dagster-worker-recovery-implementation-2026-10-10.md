# Dagster worker recovery: implementation scope

The adapter now includes actual Python worker death before native validation,
plus worker death followed by a Rust host crash and fresh-host recovery. Checks
and independent review await parent execution. No new passing result is claimed.

The existing completion-response relay remains unchanged. It qualifies response
loss only after Rust completes. The new cases use the parent-owned native
`test-util` pause at the existing work-start trace boundary, configured only in
the fixture host process:

- `DOCSPEC_TEST_PAUSE_BEFORE=validation`
- `DOCSPEC_TEST_PAUSE_MARKER=<absolute fixture path>`
- `DOCSPEC_TEST_RESUME_MARKER=<absolute fixture path>`

The Rust HTTP handler has already durably retained processing, but validation
has not started and the handler has neither returned nor been reinvoked. The
fixture waits for the native marker, checks processing records and files, and
requires the worker's initial report to still show only `started`. It sends
actual SIGKILL to that waiting worker and verifies the signal exit status and
unchanged initial report. The original shared input remains intact; only a
private temporary copy is removed before recovery.

In the live-host case, the fixture creates the resume marker and starts a fresh
Python worker. In the host-crash case it kills Rust before creating that marker,
verifies SIGKILL, and starts another host over the same workspace without test
pause variables. The fresh worker loads the original unchanged configuration.
Its PID must differ, and its exact request SHA-256 and route must match the dead
worker's initial report. The retained native operation identity remains
`stable_build` throughout.

Every paused processing record and already retained file must remain exact after
recovery; file comparison covers SHA-256, size and modification time. A further
fresh-worker replay must return the same summary and preserve the complete native
pin, lifecycle records and retained-file inventory. Across all host attempts and
replay, `DOCSPEC_TEST_WORK` must show processing, validation and readback each
exactly once. The native trace establishes phase execution for this fixture;
it does not measure every input read, CPU instruction or resource use.

The implementation adds no Python lifecycle engine, dependency, broad route
matrix or fabricated native completion. It reuses the existing bounded
temporary-workspace fixture, actual host, worker helper, retained-file comparison
and native work trace. The native hook is parent-owned, feature-gated and absent
from production builds. The fixture requires a `test-util` binary; failure to
reach its pause within the fixture deadline fails instead of substituting a
completion-response test.

The complete adapter suite can be run in the verified local environment from
`adapters/dagster/`:

```sh
DOCSPEC_BINARY=/Users/mikewolfd/Work/spicy-stack/docspec-rs/target/debug/docspec \
DOCSPEC_TEST_INPUT=/var/folders/g8/10vdwk2n07z85vqn_vlv2lfc0000gn/T/docspec-dagster-native-jkndowib/comments.parquet \
PYTHONPATH=src /Users/mikewolfd/.cache/uv/archive-v0/h1RKfXWViF4CHEhlYTQUF/bin/python \
  -m unittest discover -s tests -v
```

To target the new cases alone, keep the same environment and use:

```sh
DOCSPEC_BINARY=/Users/mikewolfd/Work/spicy-stack/docspec-rs/target/debug/docspec \
DOCSPEC_TEST_INPUT=/var/folders/g8/10vdwk2n07z85vqn_vlv2lfc0000gn/T/docspec-dagster-native-jkndowib/comments.parquet \
PYTHONPATH=src /Users/mikewolfd/.cache/uv/archive-v0/h1RKfXWViF4CHEhlYTQUF/bin/python \
  -m unittest discover -s tests -p test_rust_integration.py -v -k killed_python_worker
```

The paths existed at inspection time; the binary must be rebuilt by the parent
with `test-util` to include the pause. The input is a tiny synthetic fixture,
not a retained public corpus. Native readback, pause arrival, socket operations
and graceful host shutdown have 30-second fixture bounds. Replacement workers
have a 90-second deadline. Request, response, file-count and byte limits reuse
the existing suite. A worker's start/final report stays bounded by its fixed
identity fields; the host work-trace log must be at most 1 MiB. These correctness
bounds establish no performance or aggregate memory claim.

This slice qualifies only an acknowledged processing boundary in the local Build
flow if it passes. Arbitrary-instruction crashes, other lifecycle routes,
power-loss durability, provider storage, hosted credentials and remote recovery
remain separate work. No tests, builds, formatting, installation or commits were
run by this agent. The parent owns execution and final qualification evidence.
