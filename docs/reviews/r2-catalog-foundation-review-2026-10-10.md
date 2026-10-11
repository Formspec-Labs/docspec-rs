# R2 catalog foundation: independent static review

**Verdict: approve the repaired foundation for isolated provider qualification.**
The initial review found missing HTTPS support for the REST client's Reqwest
version and unbounded catalog requests. The author fixed both through one optional
Reqwest dependency and the maintained catalog's custom-client entry point. Runtime
checks and real-provider qualification remain the root agent's next step.

This is a blind static review of the uncommitted `Cargo.toml`, `src/storage.rs`,
`src/storage/remote.rs`, and `examples/r2_catalog_probe.rs` changes on 2026-10-10.
I read the repository guidance, accepted plan, relevant qualification requirements,
Rust guidance, and semi-formal review skill. I did not read the author's report,
run builds/tests, call the provider, or modify implementation files. Source
references below describe the reviewed tree; upstream paths are under the local
Cargo registry's `index.crates.io-1949cf8c6b5b557f/` directory.

## Findings

### F1 — Closed blocker: enable TLS for the REST client's Reqwest version

**Category:** correctness. **Initial location:** `Cargo.toml:47-48` and
the `RestCatalogBuilder` construction in `src/storage/remote.rs`.

`iceberg-catalog-rest` 0.10.1 declares Reqwest 0.12 with `default-features = false`
and only the `json` feature (`iceberg-catalog-rest-0.10.1/Cargo.toml:56-60`).
OpenDAL's TLS uses a separate Reqwest 0.13 dependency
(`opendal-core-0.57.0/Cargo.toml:118-121`). Features do not unify across those
versions. The reviewed lockfile confirms that Reqwest 0.12.28 has no TLS backend
(`Cargo.lock:4291-4319`), while 0.13.5 has Rustls (`Cargo.lock:4323-4356`).
Consequently, the catalog client cannot establish the HTTPS connection required
by the selected Basin endpoint even though R2 object I/O has TLS.

**Fix:** add optional Reqwest 0.12 with an explicit Rustls feature to `remote`,
and construct the catalog client with that dependency. Keep TLS certificate
validation enabled. Resolve the lockfile and qualify the exact resulting stack.

**Repair inspected:** `Cargo.toml:33,49-50` adds optional `reqwest-rest` 0.12.28
with `rustls-tls`; `src/storage/remote.rs:56-62` constructs that client and injects
it through `with_client`. No certificate-validation override is present.

### F2 — Closed warning: a stalled catalog request can hold the probe indefinitely

**Category:** correctness / operations. **Location:**
the initial client construction in `src/storage/remote.rs` and catalog calls in
`examples/r2_catalog_probe.rs`.

`remote_catalog` uses `RestCatalogBuilder::default()` without `with_client`.
The maintained `HttpClient` therefore uses `Client::default()`
(`iceberg-catalog-rest-0.10.1/src/client.rs:60-70`). Reqwest 0.12's defaults have
no connect, read, or total timeout
(`reqwest-0.12.28/src/async_impl/client.rs:299,313-314`). Every catalog operation
in the probe awaits that client without an enclosing deadline. The small Arrow
and metadata budgets do not limit elapsed network time. A peer that accepts the
connection and stalls the response can prevent completion or recovery reporting.

**Fix:** use the same optional Reqwest client required by F1 with finite connect
and whole-request deadlines, through `RestCatalogBuilder::with_client`
(`iceberg-catalog-rest-0.10.1/src/catalog.rs:143-147`). Treat a commit timeout as
an unknown outcome and reconcile retained state; do not add blind retries.

**Repair inspected:** `src/storage/remote.rs:17-20,56-62` sets a 15-second
connection deadline and 60-second whole-request deadline on the maintained
client. The patch does not add retries. These are request bounds, not a promise
that an entire multi-request operation completes within 60 seconds.

## Patch and function trace

| Function / seam | Reference | Inputs → outputs | Verified behavior |
| --- | --- | --- | --- |
| Feature registration | `Cargo.toml:32-33,47-50,61-63`; `src/storage.rs:7-8` | Optional feature → module/dependencies/example | Remote dependencies are opt-in; OpenDAL enables S3 only. |
| `remote_catalog` | `src/storage/remote.rs:34-69` | Properties + environment variable names → `Arc<dyn Catalog>` | Checks URI/warehouse, resolves nonempty secrets, forces header redaction and disables ambient S3 config/EC2 discovery, delegates construction to maintained builders. Only production caller found is the example's `connect`. |
| `RemoteCatalog::fmt` | `src/storage/remote.rs:78-81` | Wrapper → debug text | Prints no inner catalog or properties. |
| `RemoteCatalog` catalog methods | `src/storage/remote.rs:85-141` | Native catalog operations → native results | Direct forwarding; no additional retry, serialization, namespace rewrite, or deletion policy. |
| `main`, `env`, `connect` | `examples/r2_catalog_probe.rs:28-60` | Explicit provider environment → configured catalog | Probe errors print only a stable DocSpec error code; secret values are never passed as arguments or printed. |
| `run` admission | `examples/r2_catalog_probe.rs:62-119` | Write opt-in + canonical qualification UUID → new lifecycle table | Requires the qualification prefix, checks namespace absence, emits its exact reference before writing, uses small explicit budgets. |
| `run` conflict/readback | `examples/r2_catalog_probe.rs:121-150` | Seeded state → winner plus refused stale candidate | Requires a real REST conflict classification and checks the winner's revision/record and absence of the rejected record. |
| `run` response-loss/readback | `examples/r2_catalog_probe.rs:152-193` | Successful-but-discarded reply → fresh client/view | Reopens native state and checks exact current records and revision. Output limits the claim to the catalog/lifecycle seam. |
| `ProbeCatalog::update_table` | `examples/r2_catalog_probe.rs:213-252` | Native commit + one-shot fault → native outcome / injected error | Conflict fault commits a competing row before forwarding the original commit; response-loss fault forwards successfully, then returns a nonretryable error. |
| `Lifecycle::open` | `src/catalog.rs:147-181` | Dataset → native lifecycle table/view | Creates only its derived namespace/table when absent; no global settings. |
| `Lifecycle::commit` | `src/catalog.rs:293-436` | Exact view + bounded records → guarded new view | Writes records and commits once; publishes the warm cache only after successful commit. |
| `GuardedCatalog` | `src/catalog.rs:568-588` | Expected metadata + native commit → guarded result | Final load check precedes the probe wrapper; native update errors become nonretryable. |
| Native append / REST update | `src/storage.rs:397-411`; upstream `iceberg-0.10.1/src/transaction/snapshot.rs:518-527`; `iceberg-catalog-rest-0.10.1/src/catalog.rs:1005-1036` | Expected UUID/snapshot → HTTP POST | Sends requirements to the server; HTTP 409 maps to `CatalogCommitConflicts`. |

## Data flow and invariants

- The dataset is a caller-supplied canonical UUID under the fixed qualification
  prefix. All probe writes derive from that one dataset. The initial absence check
  is not an atomic namespace reservation, but the accepted scope remains synthetic
  even if the same qualification identity is accidentally reused concurrently.
  A fresh UUID is still required for each attempt.
- The conflict is introduced after the existing guard's last metadata load and
  before the real REST POST. The competing append preserves the table UUID and
  changes the current snapshot. The native request includes both UUID and
  expected-snapshot requirements. The wrapper sets its conflict flag only after
  the real client's `CatalogCommitConflicts` response. This is a valid server
  expected-snapshot test; it is not merely the application's local stale-view
  refusal.
- The lost-response injection occurs after actual catalog success. A fresh
  catalog and fresh lifecycle cache recover the exact retained rows. This proves
  native visibility after the injected lost reply if the probe passes. It does
  not qualify full operation resume, repeated-upload avoidance, publication,
  retention, or restore; the example correctly says those remain unqualified.
- No probe path calls a drop/purge method, scans existing namespaces/tables, or
  changes existing table/catalog policies. Native table creation sets the existing
  no-retry/no-GC properties (`src/storage.rs:87-99`). Those properties alone do not
  establish the provider's maintenance behavior or protected-object retention.
- The small fixture bounds the records and payload. Existing maintained native
  writers/readers and lifecycle accounting remain in use. No throughput or memory
  improvement is established by static inspection, and this seam probe is not a
  whole-engine resource qualification.

## Coverage and residual limits

The repaired module adds
`debug_redacts_dummy_secrets_without_network_requests`
(`src/storage/remote.rs:149-165`) and
`missing_required_configuration_is_refused_before_connecting`
(`src/storage/remote.rs:168-180`). The former constructs the real TLS client,
formats the redacted wrapper around dummy credentials, and requires the secret
to be absent. The latter covers missing/empty required properties. Neither calls
a catalog method, so lazy construction does not contact the placeholder host.
These tests have not been executed by this reviewer. The opt-in example has
meaningful assertions for successful native I/O, server conflict, and injected
lost response; the root agent should retain its actual provider result.

The two Reqwest versions do not imply a Rustls provider-selection panic on these
paths. Version 0.12.28 explicitly chooses the existing default or Ring and calls
`ClientConfig::builder_with_provider`
(`reqwest-0.12.28/src/async_impl/client.rs:761-778`). Version 0.13.5 explicitly
chooses the existing default or AWS-LC and calls the same explicit-provider API
(`reqwest-0.13.5/src/async_impl/client.rs:719-730,2499-2510`). Neither relies on
Rustls inferring a single provider from unified Cargo features. No process-wide
provider installation is needed solely to fix this adapter. Runtime construction
and real HTTPS remain part of the root's targeted checks.

The wrapper's own `Debug` is redacted and the probe emits only bounded chosen
fields. Upstream native `Table`/`FileIO` debug output is **not** safe to print:
`Table` derives `Debug` (`iceberg-0.10.1/src/table.rs:198-207`), `FileIO` derives
`Debug` over `StorageConfig` (`src/io/file_io.rs:62-69`), and `StorageConfig`
derives `Debug` over raw properties (`src/io/storage/config/mod.rs:54-58`). The
probe does not print those values. This is a limitation for future diagnostics,
not an observed credential disclosure in the reviewed execution path.

**Conclusion:** approve the repaired implementation for isolated provider
qualification. F1 and F2 are resolved in the inspected source. Coverage is
adequate for this foundation's static scope; runtime qualification remains
pending. Confidence is high in the repair and the server-conflict ordering.
