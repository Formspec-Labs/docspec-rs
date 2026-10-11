# Maintained S3 query runtime review

Verdict: **APPROVE** for the applied query/runtime change plus `/tmp/docspec-engine-s3-canonical-endpoint.patch`, subject to the parent-owned final gate. No material finding remains. The initial Cargo insertion error is fixed. The endpoint supplements preserve omitted HTTPS schemes, native URL canonicalization and global AWS regional mapping before virtual-host construction; the last supplement uses the existing maintained `reqwest_rest::Url`, with no new dependency. Review scope is the current applied source together with that explicit final patch, not an assertion that the patch was already applied at review time.

## Patch and function trace

| Function | Current source | Verified behavior |
|---|---|---|
| `RemoteObjects::from_resolved` | `src/storage/remote/objects.rs:45` | Builds the maintained `object_store` S3 reader from the same explicit bucket, endpoint, region, access key, secret, optional token and addressing mode as the native/conditional clients. Disables retries and bounds transport timeouts. Configuration errors remain redacted. |
| `RemoteObjects::register_query_store` / `Objects::register_query_store` | `src/storage/remote/objects.rs:163`, `src/storage/object_io.rs:18` | Registers the concrete remote reader under its bucket; Local is a no-op. This reader is bucket-wide; retained-path admission remains the authority for exact object scope. |
| `Engine::compose` | `src/engine.rs:232–259` | Registers once at startup in the existing Budget runtime, before returning the Engine. No second execution runtime or registry framework is added. |
| `Budget::runtime_env` / `context` | `src/budget.rs:100–131` | Borrows the existing runtime; every context shares its existing memory pool and scratch limit. |
| `build_query` / `process_build` | `src/engine/write.rs:70–78,142–156` | Uses maintained query-only options before execution, then preserves existing plan restrictions and streams through existing output processing. |

## Data flow and invariants

Credentials stay inside the configured clients. `object_store` 0.13.2 `aws/builder.rs:1085–1116` selects its static provider when both explicit credential fields are present; this path cannot fall back to environment, config files, or metadata discovery. The optional token follows the same configured path. Scope is not inferred from possession of bucket credentials: `validate_retained_path` remains a separate admission check, and the context stays private to Engine.

The new client adds no credential exposure through derived Engine/Budget Debug. `RemoteObjects` redacts its fields; DataFusion 53.1.0 `runtime_env.rs:88–92` formats only `RuntimeEnv`; its default object-store registry formats registered scheme keys rather than store values (`object_store.rs:186–200`). Native FileIO's existing explicitly documented debug limitation is unchanged.

Pinned DataFusion `context/mod.rs:645–654` verifies `SQLOptions` before `execute_logical_plan`. Its recursive verifier at `:2180–2220` rejects DDL, DML, COPY and statements, including subplans. DocSpec's existing `validate_spec`/`validate_plan` at `src/engine.rs:2188–2309` retain one-query, input-only, repeatability and immutable-function checks. Default table functions are `generate_series` and `range`; no URL-reading table factory is enabled by this composition.

## Tests and limits

`src/engine/remote_query_tests.rs:14–115` uses the actual private Engine composition and existing shared context with a localhost S3-compatible Parquet fixture. It checks admitted scope, successful filtered one-column SELECT with duplicate/null preservation, and no additional transport request for rejected COPY, external DDL or SET. It streams batches and accepts maintained Utf8View output through an explicit test-only Arrow cast. The shared inventory fixture now serves real byte ranges while its malformed-response branch remains available.

This test supports wiring and values, not measured column-byte savings, throughput, provider compatibility, or full remote processing. `src/engine.rs:939–943` still refuses remote processing before the unsupported native writer. No public remote Engine entry point is added. Local queries retain the same operators and resource configuration; their verification belongs to the parent-owned full gate.

The added optional AWS feature uses the same `object_store` 0.13.2 already locked by DataFusion. No custom Parquet downloader, transfer buffer, cache or SQL effect parser is introduced. Static review only: builds, fixtures and provider activity are owned by the parent.

Closed finding: at `src/storage/remote/objects.rs:115–131`, the original query endpoint translation differed from pinned OpenDAL `backend.rs:502–546` for scheme-less endpoints and canonical variants of the global AWS host. The applied first supplement restores scheme/global behavior; `/tmp/docspec-engine-s3-canonical-endpoint.patch` adds URL normalization at the same pre-template point, also covering default HTTPS ports and host capitalization. Invalid URL parsing returns the existing redacted configuration error. This is a startup-only bounded string operation; no request or payload pass is added. Noncanonical endpoint variants received static source comparison, not provider execution.

I read the retained passing `docs/reviews/remote-s3-query-tests-2026-10-11-2.log`. That execution predates the final URL-normalization supplement, so it is not evidence for the final compiled source; the parent is running the final gate.

Conclusion: **APPROVE** within the stated scope. No credential, scope, supported SELECT, resource-sharing or local-regression blocker found. Complete remote native processing, selection, publication, maintenance and provider qualification remain separate work.

## Parent execution follow-up

The full repository gate passed before the final endpoint normalization. After
applying that supplement, the focused remote suite and strict all-target,
all-feature Clippy check passed. See
[full gate](remote-admission-query-full-check-2026-10-11.log),
[final remote suite](remote-query-final-tests-2026-10-11.log) and
[final Clippy](remote-query-final-clippy-2026-10-11.log). The full gate was not
repeated for the startup-only URL normalization. No throughput claim is made.
