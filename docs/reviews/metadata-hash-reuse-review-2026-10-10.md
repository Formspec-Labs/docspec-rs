# Metadata sharing and JSON hashing: independent review

**Verdict: APPROVE.** No blocking correctness, identity, admission-lifetime, or
scope regression was found in the reviewed diff. This is a static review; the
reviewer did not run builds, tests, formatters, or performance experiments.

## Scope and findings

Compared the working tree with `e7078a7`, limited to
`src/engine/backfill/origins/checks.rs`, `src/storage/inspection.rs`, and
`src/engine.rs`. Read their relevant callers, dependencies, and test assertions.
Separate implementation work and other review reports were excluded.

No actionable findings. The changes remove a deep metadata clone and temporary
serialized-byte buffers. They do not establish a measured latency or whole-process
memory improvement; the normalized JSON trees and Iceberg table wrappers still
exist.

## Patch summary and function trace

| Function | Inputs and outputs | Verified behavior |
|---|---|---|
| `Checks::table`, `src/engine/backfill/origins/checks.rs:240` | Exact native snapshot reference and budget → read-only Iceberg table | Resolves the admitted cached `Arc<TableMetadata>`, checks snapshot/schema IDs, and moves that Arc into the maintained builder instead of cloning the metadata tree. |
| `Checks::metadata`, `src/engine/backfill/origins/checks.rs:225` | Immutable object reference → shared parsed metadata | Checks full object identity before path-cache reuse. A cache miss admits, reads, verifies, decodes, and retains the metadata once. |
| `Checks::admit`, `src/engine/backfill/origins/checks.rs:177` | Serialized size and budget → shared reservation | Retains cumulative metadata accounting and the existing decoded-expansion reservation in `Checks::memory`. The patch changes neither the bound nor ownership. |
| `schema_identity`, `src/storage/inspection.rs:27` | Native struct type → hexadecimal digest | Keeps the existing `to_value` conversion and recursive removal of documentation and read/write defaults; hashes that same normalized value through `json_digest`. |
| `fingerprint`, `src/engine.rs:2032` | Operation request → hexadecimal material-request digest | Keeps every request variant's material fields, optional schema-field inclusion rules, processor identity, tuple order, and compact serialization. Only the hash sink changes. |
| `json_digest`, `src/storage.rs:56` | Any serializable value → BLAKE3 hash or serialization error | Streams the compact serializer into the maintained hasher and returns a hash only after serialization succeeds. Existing shared helper; no new hashing implementation. |

Important callers are `Checks::correspondence` at
`src/engine/backfill/origins/checks.rs:104` and `Checks::check` at lines 481–482;
both consume the returned tables inside the operation-local checks. The outer
verification context owns these checks at `src/engine/verification.rs:11`, with
qualification, correspondence, and origins entry points at lines 45, 62, and 91.

Physical schema identities feed known-file reuse and footer comparison in
`src/storage/inspection.rs:77–81`, 114–116, and 133–139, and trusted-base schema
sets in `src/engine/verification.rs:151–158`. Request digests guard planning at
`src/engine.rs:348–356`, execution/reconciliation at line 724, and operation
preparation at line 981. These callers and their failure decisions are unchanged.

## Data flow and invariants

1. **Byte identity remains exact.** The locked `serde_json` 1.0.151 implementation
   defines `to_vec` by allocating a vector and calling `to_writer` into it
   (`serde_json/src/ser.rs:2213–2219`). The helper calls that same compact
   `to_writer` (`src/storage.rs:59–61`); upstream uses the same `Serializer::new`
   (`serde_json/src/ser.rs:2177–2183`). Locked BLAKE3 1.8.7's `Write`
   implementation feeds every supplied byte to `update` and accepts the complete
   slice (`blake3/src/lib.rs:1627–1637`). Removing the vector does not change
   field ordering, escaping, scalar encoding, or the digest input.
2. **Normalization remains local and unchanged.** `schema_identity` still removes
   only `doc`, `initial-default`, and `write-default` recursively before hashing
   (`src/storage/inspection.rs:30–50`). Request fingerprinting still constructs
   its existing material JSON and hashes `(PROCESSOR, material)`
   (`src/engine.rs:2033–2063`). No schema policy or operation-reuse rule moved into
   the shared hash helper.
3. **Failure does not yield an identity.** The helper propagates serialization
   failure before `finalize`; both changed callers retain `?`
   (`src/storage.rs:60–61`, `src/storage/inspection.rs:50`,
   `src/engine.rs:2061`). The hasher cannot introduce a partial-write error through
   its maintained `Write` implementation. Existing application conversion still
   classifies JSON errors as `invalid_json` (`src/error.rs:66`).
4. **Sharing retains the original metadata object.** Locked Iceberg 0.10.1 aliases
   `TableMetadataRef` to `Arc<TableMetadata>` (`iceberg/src/spec/table_metadata.rs:61`).
   Its builder accepts `Into<TableMetadataRef>`, stores that value, and moves it
   into `Table`; `metadata_ref` clones the Arc, not the tree
   (`iceberg/src/table.rs:74–76`, 184–187, 237–239). `Checks::table` still validates
   both native IDs before building (`src/engine/backfill/origins/checks.rs:241–254`).
5. **Admission remains owned through use.** `Checks` retains its reservations
   alongside cached metadata (`src/engine/backfill/origins/checks.rs:33–46`,
   189–191, 234–237). Production `table` callers remain internal to the borrowed
   checks operation; they do not return these tables or spawn a new detached
   consumer. Metadata parsing already clones the reservation into its blocking
   worker, including cancellation (`src/engine/inputs.rs:399–440`). The patch adds
   no new metadata escape or reservation-release path.

## Tests and concrete edge cases

Tests were inspected, not executed by this reviewer. Expected behavior below is
derived from their assertions and the source trace.

| Existing or extended test | Relevant assertion and expected behavior |
|---|---|
| `cached_originals_reuse_metadata_and_recheck_caller_association`, `src/engine/backfill/origins/checks.rs:603` | New pointer-equality assertion at 687–690 proves the returned table shares the cached metadata allocation. Existing assertions preserve one admission, reject forged defaults/association changes, and reuse cached metadata after original files are removed (693–695, 720–752, 754–807). Expected to pass with the direct Arc; the pointer assertion distinguishes the prior deep clone. |
| `json_digest_preserves_typed_request_serialization`, `src/storage.rs:749` | Pins compact typed serialization and compares the streamed hash with the former vector/hash path, including processor tuples and material metadata. Expected unchanged. |
| `json_digest_preserves_nested_maps_unicode_and_unsized_values`, `src/storage.rs:781` | Compares exact old/new hashes for nested JSON, nulls, empty values, Unicode/escaping, and content crossing BLAKE3 chunk boundaries. Expected unchanged. |
| `json_digest_preserves_serialization_errors`, `src/storage.rs:802` | Forces a serializer failure after one field and an invalid JSON map key; compares class, position, message, application error code, and source. Expected unchanged; failed partial streams return no hash. |
| `live_workspace_owner_and_conflicting_operation_refuse`, `tests/lifecycle.rs:509` | Reuses an operation ID with changed SQL and requires `operation_conflict` (518–523), exercising the changed fingerprint caller. Expected unchanged. |
| `additions_updates_defaults_and_rebuild_preserve_supplied_state_and_history`, `tests/defaults.rs:295` | Preserves same-request reuse and requires conflict when the requested addition default changes (323–329); subsequent verification covers retained default-bearing schemas (450, 508). Expected unchanged. |

Direct old/new hash-equivalence assertions live at the shared serializer boundary;
there is no new caller-specific golden digest fixture. The unchanged normalization
and upstream equivalence trace are sufficient for this small call-site change.
No new payload read, retention behavior, background task, dependency, or public
API is introduced.

## Conclusion

**VERDICT: APPROVE**

- The patch removes the identified copy and temporary buffers while preserving
  exact serialization, metadata identity checks, and admission ownership.
- Coverage of changed paths: **ADEQUATE** for this scoped static review.
- Confidence: **HIGH** for equivalence and ownership; performance remains
  unmeasured. Repository execution evidence belongs to the implementation owner.
