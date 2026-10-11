# Configured inspection and read review

Verdict: **APPROVE for initial Build qualification and selected field-group reading**, subject to parent-owned compilation and runtime gates. Static review of `/tmp/docspec-remote-inspection-read.patch`, digest `a3d6fc46df9a21146ec64504188d33239779d47f3d9f9b45056a2c030f6295ce`. File references are to proposed source under `/tmp/docspec-remote-inspection-read/new/`. No builds, provider calls or implementation edits by this reviewer.

## Finding closed

`src/storage/inspection.rs` originally cloned the complete identity map for the manifest guard, then cloned it again for footer inspection. The final patch moves the first map into the footer worker after the guarded walk (`:181–183`). This removes one O(files) copy and retained map without another abstraction. No throughput claim is implied.

## Verified trace

- `Engine::read_group` → `read_pin_with_objects` (`src/engine/read.rs:29,155`) uses the Engine's explicitly configured backend after resolving exact retained release authority and representation. It opens the selected immutable metadata and streams native batches; it does not invoke creation inspection, footer qualification or full-file hashing. Existing standalone readers retain their explicit Local behavior.
- `Objects::open_pin` (`src/storage/object_io.rs:19`) checks the selected metadata URI and every captured object against configured read scope before I/O, then supplies the constructor-owned native FileIO to the existing bounded pin reader. It does not substitute arbitrary table credentials, choose a live catalog head, or derive ownership from readable scope. The metadata read checks exact identity and snapshot/schema as before.
- `Objects::check_available` (`:39`) validates the complete requested scope before stat work, then uses the shared owned worker/inventory admission. Remote availability remains HEAD-only; local stat semantics stay unchanged. Cancellation leaves the active worker's reservations intact.
- Routine `Checks` receives the configured backend in the actual `prepare_and_select` path (`src/engine/verification.rs:33,218`; `src/engine.rs`, routine checks and readback availability call sites). Initial Build has no base/schema/backfill relationship requiring the still-local advanced traversal paths.
- `inspect_table` (`src/storage/inspection.rs:144`) opens exact metadata, validates supported schema evolution and builds the existing guarded native inventory. Scope checks precede manifest decoding. The same walk establishes current membership, rejects native delete files and enforces retained object closure before any footer/sample work.
- Sample tasks (`inspection.rs:258–290`) now use the already collected live `DataFile` descriptors rather than planning another manifest walk. They preserve current schema, sparse native field IDs, name mapping, full-file size/count and the reserved file column that forces the native ID-aware transform. Existing batch/default-expansion limits and required/null checks remain in force. Pinned Iceberg 0.10.1 `scan/task.rs:55` and `scan/context.rs:123` confirm these full-file task fields; the supported no-delete path requires no predicate or delete index. The native planner currently supplies no partition spec either. This does not expand partition/deletion support.

Known physical-file evidence still skips repeat footer checks and samples when identity, rows and schema history agree. Newly materialized files receive bounded footer and shape checks; ordinary reads only decode requested data. The native sample reader may fetch its own footer after the explicit qualification footer read; avoiding that would require a separate reader integration and is not claimed here.

## Tests and remaining scope

The proposed populated fixtures establish one GET per native metadata object during inspection, no payload GET when known physical evidence is reused, sparse IDs with nullable/default additions, exact null/value readback, HEAD-only availability, and refusal of out-of-scope metadata/object paths before those paths receive I/O. Their assertions concern actual request paths, not only returned row counts.

Those fixtures call the storage helpers; they do not themselves prove Engine `read_group` dispatch. Parent has explicitly taken ownership of an isolated private-Engine provider test to cover that connection. Related evidence reads, schema/dependency verification, backfill/compaction and standalone public readers retain Local-specific paths. Public remote Engine startup and complete hosted lifecycle qualification remain unclaimed. This reviewer has no execution evidence for the proposed patch.
