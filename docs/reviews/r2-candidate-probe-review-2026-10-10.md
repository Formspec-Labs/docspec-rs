# R2/Basin native candidate registration probe review

**Verdict: APPROVE the isolated probe execution.** No unsafe-write or false-pass
defect was found in the reviewed path. This is static review, not provider
qualification. The probe must finish with `complete.passed=true` before its full
stated metadata-registration scope passes. Partial events remain useful evidence.

## Findings and evidence limits

- **Observation — a provider error does not prove unsupported registration.**
  `provider` retains the phase, native error kind and retryability while dropping
  error bodies and credentials (`examples/r2_candidate_probe.rs:64`). The pinned
  REST adapter maps non-200/404/409 registration responses to `Unexpected`
  (`iceberg-catalog-rest-0.10.1/src/catalog.rs:940`); its error construction retains
  HTTP status only in error context (`src/client.rs:333` in that dependency).
  The probe does not emit that context. Report a failed `register_table` as
  inconclusive unless independent evidence establishes the cause. Do not infer
  unsupported registration from `Unexpected` or log whole errors to recover it.
- **Observation — catalog-owned metadata is an explicit partial outcome.**
  A successful registration emits returned locations and `metadata_rewritten`
  before preservation checks (`examples/r2_candidate_probe.rs:227`). A changed
  table location refuses separately; a changed metadata-file location can proceed
  only inside the exact qualification prefix (`:234`, `:261`). Therefore a Basin
  metadata file outside that prefix produces `probe_metadata_scope` after the
  fresh catalog check, not an unsupported-registration result or a full pass.
  Preserve this distinction in the receipt. Any future read-only admission of an
  exact catalog-owned object needs an explicit ownership rule; do not silently
  widen the writable prefix.
- **Observation — bounded synthetic metadata is not general memory qualification.**
  The fixture has fixed fields and serializes before its explicit size check
  (`examples/r2_candidate_probe.rs:80`, `:156`). The separate rewritten-object read
  caps stored bytes and reserves its buffer before reading (`:276`). This is
  proportional to the tiny fixed probe. Catalog HTTP response allocation and
  whole-process memory are not bounded or measured by these assertions.

## Patch and function trace

The example tests whether the actual catalog can register metadata that DocSpec
created in its owned object prefix, retaining sparse field IDs. It writes no
payload data, commits no DocSpec release, and does not qualify the remote engine.

| Function / boundary | Evidence | Verified behavior |
| --- | --- | --- |
| `main`, `env`, `connect` | `examples/r2_candidate_probe.rs:29`, `:37`, `:42` | Explicit environment references configure the same catalog/object composition; terminal errors expose only a stable code. |
| `candidate`, `object` | `examples/r2_candidate_probe.rs:80`, `:71` | Uses field IDs 7 and 42, string/long types and distinct nullability; restores IDs after provisional native creation; binds serialized length/BLAKE3. |
| Argument and scope admission | `examples/r2_candidate_probe.rs:127` | Requires explicit opt-in and a canonical qualification UUID; checks namespace absence before mutation; emits exact references before effects. |
| `RemoteObjects::create_bytes` | `examples/r2_candidate_probe.rs:202`; `src/storage/remote/objects.rs:129` | Existing strict scope, size/identity validation, owned worker admission and create-only PUT apply. There is no overwriting writer or implicit retry here. |
| `register_table` | `examples/r2_candidate_probe.rs:210`; `src/storage/remote.rs:196` | Calls the maintained register operation with the exact newly created metadata path. The maintained REST request sets `overwrite=false`. |
| Preservation and fresh catalog load | `examples/r2_candidate_probe.rs:234`, `:251` | Checks table identity/location/UUID, format, current schema including IDs/types/nullability, last allocated field ID, current snapshot and requested properties; a newly constructed client must return equal metadata and the same metadata location. |
| Rewritten metadata identity | `examples/r2_candidate_probe.rs:261` | Reads only a returned in-prefix metadata file; checks size before/after and hashes the bounded bytes. No extra write or broader scope results. |
| Catalog-free opening | `examples/r2_candidate_probe.rs:305`; `src/storage/pinned.rs:36` | The exact pin opens through native FileIO without a catalog lookup; the shared reader checks length/digest, bounds decoding and checks snapshot/schema. Full decoded metadata must equal the registered result. |

## Data flow and invariants

The qualification UUID determines both the synthetic namespace and object prefix.
The native builder determines the candidate UUID, schema and initial metadata
location. Those references are logged before their corresponding writes, so a
failure leaves discoverable retained objects. The only mutations are namespace
creation, one conditional metadata PUT and one table registration. There are no
deletions, payload uploads, bucket policy changes, lifecycle selections or cleanup
actions in this example (`examples/r2_candidate_probe.rs:184` through `:213`).

Same-location metadata keeps the original exact digest. A rewritten in-prefix
metadata file gets one bounded identity read, followed by the independent native
open. That additional read establishes a new identity; it does not repeat a
completed ordinary dataset verification. The final success event follows both
fresh catalog and catalog-free checks (`examples/r2_candidate_probe.rs:251`,
`:315`). Secrets are neither printed nor retained in pins; logged paths identify
only the synthetic objects and actual provider-returned metadata.

## Coverage and conclusion

This new example has no dedicated local branch tests; it is itself an isolated
provider probe. Existing shared-reader tests cover injected plain/gzip metadata,
stored/decoded caps and changed identity/schema refusal
(`src/storage/pinned.rs:191`, `:217`, `:250`). They do not establish Basin's
registration behavior. This reviewer ran no builds, tests or provider calls.

Invalid arguments, an existing namespace, failed creation, failed registration,
changed native meaning, inconsistent fresh reads, out-of-scope rewritten metadata
and catalog-free mismatches all return before the success event. A network error
after registration may leave a registered table; exact references remain retained,
and this probe makes no lost-response reconciliation claim.

**APPROVE.** The code achieves the intended bounded experiment and its full pass
requires the intended assertions. Coverage is adequate for executing that
experiment; provider behavior remains unqualified until root retains its actual
outcome. Confidence is high in the traced static behavior and deliberately unset
for the provider result.

## Retained execution outcome

After this static review, root executed the isolated probe. The retained
`r2-candidate-provider-2026-10-10.json` reports exit 1; its JSONL reaches
`register_start` after the 757-byte conditional object creation, and stderr
records `register_table`, `Unexpected`, `retryable=false`. There is no `registered`
or successful completion event. Registration and both readback checks therefore
remain unqualified. This output alone does not distinguish unsupported
registration, request/configuration rejection, or a provider failure. No retry,
cleanup or additional provider operation was performed by this reviewer.

## Diagnosed location requirement and narrow re-review

Root's retained `r2-candidate-registration-diagnostic-2026-10-10.json` resolves
the first failure: after confirming the exact table was absent with GET 404, one
explicit registration replay reused the existing metadata object and returned
HTTP 400, `InvalidLocation`. The provider requires registered metadata beneath
`s3://spicy-regs/__r2_data_catalog`. The original qualification prefix was outside
that storage profile. This establishes a rejected location, not unsupported
registration; it does not establish that the next location will succeed.

The revised probe at `examples/r2_candidate_probe.rs:150` uses
`s3://{bucket}/__r2_data_catalog/docspec-qualification/{owner}/candidate-probe`.
The reviewed source SHA-256 is
`a0f8d458bf1035529721e7039d563fe8c58eaf977fb3932ebf70697d2ed549a9`.
Its configured `RemoteObjects` scope remains the complete fresh UUID child
prefix, not the catalog storage-profile root (`:175`). The expected native table
location and metadata path derive from that same child (`:157`). Canonical UUID
and namespace freshness guards, exclusive object creation, sparse schema checks,
fresh-client loading, catalog-free opening, and refusal of rewritten metadata
outside the exact child remain intact (`:127`, `:175`, `:203`, `:235`, `:252`,
`:267`, `:316`). The patch adds no delete, policy change, cleanup, overwrite or
broader writer scope.

**APPROVE the revised isolated run with a fresh UUID.** No additional execution
blocker was found. Retain the failed first attempt independently; its diagnosis
supports this narrow location correction without an architectural rewrite. A
successful final completion event is still required to qualify registration and
both readback paths. This re-review performed no provider calls or builds.

## Corrected provider result

Root's fresh corrected run passed. The retained
`r2-candidate-profile-provider-2026-10-10.json` reports exit 0 and binds the same
reviewed source SHA-256 above; the executed `--features remote` build's binary
SHA-256 was
`65fae9e747e21dac8c44364dedcb2169f51a40fbcb36887b8395508acef286dc`.
The later all-features repository gate rebuilt the executable at the same target
path. This retained digest identifies the build that actually ran, not the
subsequently overwritten target artifact; unchanged recorded sources and retained
build/run evidence do not require repeating the provider run.
The matching JSONL records `metadata_rewritten=false`, then
`complete.passed=true`, `fresh_catalog_readback=true`, and
`catalog_free_native_readback=true`. Stderr is empty.

The isolated table is
`candidate_09879fc9_eb50_47b8_b4c1_903329db55b3.candidate`. Its retained metadata
is the 775-byte object at
`s3://spicy-regs/__r2_data_catalog/docspec-qualification/docspec-rs-qualification-09879fc9-eb50-47b8-b4c1-903329db55b3/candidate-probe/metadata/00000-f9e5ced5-800a-4d04-a633-e416a80c8210.metadata.json`,
with BLAKE3
`60e3b643f390d026afa0d58fc1ee5a28ad916bc1e655abbc09da77d63a211507`.
The provider preserved the intended table UUID/location, current schema and field
IDs 7 and 42, allocation high-water value, current snapshot state and requested
properties. Fresh catalog metadata and independent catalog-free metadata matched.

This qualifies the tested empty-candidate registration/readback path against the
actual provider with a valid storage-profile location. It does not qualify data
append, replacement schema history, remote lifecycle operation, publication,
recovery, cleanup or a future shared helper implementation. Both failed attempts
and their diagnosis remain retained. No alternate candidate architecture is
required by this result.

## Smallest next integration recommendation

Use a private concrete local/remote `Objects` writer in the existing internal
`register_candidate`, preserving its one serialization worker and existing
admission lifetime. Share a private admitted conditional-write core with
`RemoteObjects::create_bytes`; do not call that public method from the already
admitted worker and acquire another worker/reservation. Add only the operations
actually used by candidate creation; native readers already accept FileIO.

Expose the useful remote production call through the existing composition:
`RemoteStorage::register_candidate(&self, identifier: &TableIdent, metadata:
TableMetadata, budget: &Budget) -> Result<Table>`. It delegates to the same
internal helper used by local `Engine::register_output`, with the already
configured create-only writer. Document caller-owned namespace/protection and
metadata lifetime, plus the absence of lifecycle selection or automatic retry.
The provider example can then consume this production helper instead of its
manual serialization/write/register sequence while retaining its independent
preservation and readback assertions.

This is a native storage capability alongside the composition's existing catalog
and object primitives, not a remote Engine API. Keep remote Engine construction
out of this slice until admission, pinning, validation and protection stop relying
on local paths. Do not add unused I/O accessors, speculative routing, dead-code
suppression or another catalog/lifecycle authority.
