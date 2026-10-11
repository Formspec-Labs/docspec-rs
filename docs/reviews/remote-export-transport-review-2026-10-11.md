# Export transport fixture review

**Verdict: APPROVE** patch `/tmp/docspec-export-transport-tests.patch` (`fd32010050740a08547443bd79242c61e866c56545415361db9c2c0c824749a7`) for the planned gate. Static only; no reviewer execution or source edits.

The existing killed-worker CLI scenario now invokes actual `read-export` with its retained pin and independently decodes resulting Arrow IPC. The existing HTTP fixture reads `/datasets/comments/exports/http-file/arrow`, checks content type and exact dataset/export headers, and independently decodes the same literal schema/values. Missing-ID Arrow lookup expects HTTP404 and `no_export`.

`assert_authorized_ipc` checks field order, literal stable IDs2/1, Utf8/Int64, nullable fields, absence of arbitrary schema metadata, complete values including duplicate key and nulls, and private-canary absence. Expectations match the independent fixture source at `tests/exports.rs:67` and explicit policy at `:105`; they are not reconstructed from the returned pin. Transport callers are `src/main.rs:889` and `src/http.rs:790`, both using Engine readers and the shared maintained IPC path. The HTTP response body has a 1MiB fixture bound.

No new production behavior, provider writes, transport framework, or application compatibility claim is introduced. This closes the specific missing adapter assertion while reusing existing small scenarios. It does not count remote requests or qualify remote transports independently. No actionable finding; runtime pending parent gate. Confidence high.

## Parent-owned execution

The combined `./scripts/check` passes in
`remote-export-plan-full-check-2026-10-11.log`. It includes the relevant library
and integration cases, strict all-feature Clippy and formatting, and the restore
and worker-initialization suites. Opt-in provider and application checks have
separate retained evidence; this gate does not claim they ran automatically.
