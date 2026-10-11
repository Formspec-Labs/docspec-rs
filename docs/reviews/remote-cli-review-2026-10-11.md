# Remote CLI and loopback HTTP startup review

**Verdict: approve this thin startup integration, subject to the parent-owned repository gate.** No actionable correctness or credential-disclosure defect found. One nonblocking test-coverage limit is recorded below. Reviewed the applied, formatted `src/main.rs`, `tests/remote_cli.rs`, test registration and README on 2026-10-11; traced existing remote configuration, Engine reads and HTTP routing. No source changes, builds, tests or provider requests were performed by this reviewer.

## Finding

**W1 — Coverage observation, `tests/remote_cli.rs:104`: successful transport composition is not directly exercised.** The source-read and loopback-serve test deliberately uses an invalid owned scope, so it proves explicit remote dispatch and refusal before host/output effects, but stops before `Engine::with_remote` and HTTP binding. The other new tests cover early failures and the Local default. Existing public-constructor tests, actual provider Engine evidence and the static call trace support this small wiring change; they are not a successful remote CLI/HTTP end-to-end run. Preserve this distinction in qualification claims. No repeat provider workload is requested for unchanged Engine behavior.

## Function trace

| Entry | Inputs and result | Verified behavior |
|---|---|---|
| `main`, `src/main.rs:318–320` | Parsed CLI → early check → limits | Capability and loopback checks run before config, limits, host, input or output access. |
| `check_startup`, `main.rs:878` | CLI backend/command → refusal or success | Missing remote feature refuses. Direct Local hash/verify/restore/evidence/receipt operations and remote non-source reads refuse. Non-loopback serving refuses for either backend. |
| `read_remote_config`, `main.rs:925` | Named JSON file → `RemoteConfig` | Reuses the existing metadata-file size preflight/parser. Unknown fields and invalid types fail; parser values, unknown keys and underlying error chains are replaced by a fixed refusal. The configuration type has no Debug derivation. |
| `open_engine`, `main.rs:936` | Explicit config or absent option → Engine | Calls existing `remote_storage` once and passes its configured catalog/objects to `Engine::with_remote`; absent option preserves `Engine::open`. There is no alternate lifecycle or credential discovery. |
| Local reader branch, `main.rs:356–380` | Exact release → independent batches | Runs only without `--remote`, preserving existing Local behavior. |
| Remote reader branch, `main.rs:848–856` | Exact release → `Engine::read_group` → IPC | Resolves the retained result and configured object backend through Engine. The output opens only after Engine accepts the read; stream completion precedes shutdown. |
| `write_stream`, `main.rs:962` | Batch stream → create-new Arrow IPC file | Shares one batch-at-a-time writer between Local and remote branches. Does not collect the dataset or hash/reverify payloads. Maintained Arrow `StreamWriter::finish` flushes its underlying writer; the extracted helper preserves that behavior. |
| Serve branch, `main.rs:858` | Loopback socket + same Engine → existing router | Uses `docspec::http::router(engine.clone())`; graceful stop retains existing worker shutdown. No new routes or authentication/deployment behavior. |
| HTTP group reader, `src/http.rs:782–807` | Dataset/version/group → release → Engine stream | Existing routes resolve the exact release, call Engine's guarded methods and retain Engine ownership through the response stream. Unsupported capabilities remain governed by those methods. |

## Data flow and invariants

The only new persistent selection at startup is which existing backend is passed into the Engine. `RemoteConfig` contains an owned prefix, native properties and environment-variable names. `storage::remote::remote_storage` resolves those names once; its maintained configuration path enforces explicit credentials, disables ambient sources and redacts adapter construction errors. Neither config, secret values nor native client Debug output are printed by the new code. `init` reports only workspace/catalog kind and does not claim a provider connectivity check.

CLI stream output uses create-new semantics and leaves any partial failed output for the caller to inspect, as the Local implementation already did. This integration adds no bulk materialization, payload verification pass or native metadata scan. Existing first-selection, same-operation recovery, input protection and maintenance refusal rules remain inside Engine. Unsupported direct Local commands never open a remote host; unsupported Engine operations can construct a host but refuse before their lifecycle/data effects.

## Tests and qualification scope

`tests/remote_cli.rs:17` verifies unchanged independent Local hashing without host creation. `:37` covers the remote-disabled build. `:47` supplies missing config/limits/input files and asserts unsupported direct commands refuse first without host/output effects. `:104` checks explicit remote config dispatch and secret-free scope errors. `:131` checks malformed/unknown config errors omit sensitive values and keys. `:154` rejects non-loopback binding without creating a host. The module is registered in `tests/integration.rs:50`, avoiding reliance on binary unit tests that this repository disables.

These tests were inspected, not executed by the reviewer. The parent is running the combined repository gate. README `:131–176` correctly limits this change to explicit CLI/loopback startup, first independent remote Build/recovery and selected source reads. It does not promise hosted access, remote exports, updates, backfill or destructive maintenance. The previous actual-provider Engine run remains evidence for that Engine path, not a newly executed CLI/HTTP run.
