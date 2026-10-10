# Dagster local completion-response recovery: author scope

Implementation awaits parent-run checks and independent review. No test result
is inferred from source inspection.

The adapter adds `/receipts/run` to its existing allowlist. Its request validation,
transport, bounded response reader, refusal handling and `ReleasePin` summary use
the existing shared code. Python adds no lifecycle storage or decision logic.

The real-host fixture extends clean replay with two distinct cases:

- Native Dagster receives no response after Rust completed and selected the real
  result. Its configured single retry submits exactly the original JSON bytes.
- A Python worker receives the same completion-response loss with retries disabled,
  records unknown outcome and exits. A separate Python process with a different
  process ID loads the unchanged configuration and reconciles the same operation.

A local test relay forwards to the actual Rust host. It consumes a real complete
HTTP 200 response, captures native state, then shuts down the worker connection
without writing a response. It fabricates neither lifecycle state nor a success
pin. At most two requests pass through it. The second native response must equal
that first full response. Selected pin, lifecycle rows, native/object file hashes,
sizes and modification times must match the pre-drop state. Those checks establish
no retained payload/lifecycle rewrite for this fixture; they do not count payload
reads, measure CPU work, or establish recovery from a killed Rust host.

The existing opted-in temporary-workspace fixture is reused. No extra dependency,
Python lifecycle ledger, catalog, public corpus or storage mutation is added.
The worker helper uses the same Python interpreter and injected Dagster resource.

The native `/receipts/run` check expects structured `stale_base` from a well-shaped
request with a missing base. It checks real forwarding and refusal handling.
Protocol tests check the successful identity summary. This slice does not claim
successful native receipt materialization through Dagster.

Fixture bounds: input at most 1 MiB; request at most 1 MiB; response at most 32 MiB;
retained inventory at most 64 MiB and 2,048 files; worker subprocess 90 seconds;
readback/socket and host readiness/shutdown deadlines 30 seconds. Forced cleanup
fails. In-process Dagster has no total elapsed-operation deadline. Bounds on
serialized metadata and fixture files are not measured process-memory bounds.

Parent execution from `adapters/dagster/`, using an already qualified Python
interpreter with Dagster 1.13.16 and an already built Rust host:

```sh
DOCSPEC_BINARY=/absolute/path/to/docspec \
DOCSPEC_TEST_INPUT=/absolute/path/to/tiny.parquet \
PYTHONPATH=src /absolute/path/to/python -m unittest discover -s tests -v
```

No commands that execute tests, builds, formatting, installation or commits were
run by the implementation agent. Parent owns those checks and review evidence.
