# Update receipts without rebuilding values

`evidence` applies receipt corrections to an exact selected result. It preserves
that result's source values, computed field groups, and join or aggregate
contributors. The operation creates a new immutable selection with the corrected
receipts. Original receipt input bytes and actual before/after rows remain retained.

Obtain stable occurrence keys from `read-occurrences` or the HTTP
`/datasets/{dataset}/versions/{version}/occurrences` stream. Equal source values
have different occurrence keys. Use the returned `__docspec_file` and
`__docspec_row` as receipt input columns `source_file` and `source_row`.

Replacement Parquet has exactly these columns, in order:

| Column | Meaning |
|---|---|
| `source_file` | Nonempty stable occurrence file identity, native string. |
| `source_row` | Nonnegative occurrence offset, native long. |
| `evidence` | An opaque receipt value of a supported native type, including nested values. |

An occurrence can have no receipts, one receipt, or several receipts. Duplicate
receipt values, nulls, empty values, and nested order remain distinct. A
replacement supplies the complete new receipt group for its occurrence. Optional
deletion Parquet contains only the two occurrence columns; replacement and deletion
keys must not overlap. New keys must belong to the exact captured source. Later
receipt updates preserve the established receipt schema.

`EvidenceSpec` supplies dataset, operation, base version, active definition, and
input paths with BLAKE3 digests. The `plan-evidence` and `evidence` CLI commands,
the `/receipts/plan` and `/receipts/run` HTTP endpoints, and the Rust
`Engine::plan_evidence` / `Engine::update_evidence` methods use the same lifecycle.
Retry the same operation to recover its completed work. A missing original input
does not invalidate an already retained admission or processing completion.

`read-receipts`, the HTTP version `/receipts` stream, and
`Engine::read_receipts` return the selected receipt table. Existing `read-evidence`
continues to return join or aggregate contributors. A URI inside an opaque receipt
does not claim that its attachment body or external resource was retained.

A later value replacement gives its new rows fresh occurrence identities. Receipts
for replaced and deleted occurrences are removed; receipts for surviving rows
remain associated even when their file is rewritten. An independent scalar
backfill adopts receipt updates selected while it was running without repeating
its completed value computation. An incremental related refresh whose input values
remain unchanged adopts the new input selections while retaining its value and
contributor tables.

Ordinary verification checks new table structure, native field identity, sizes,
footers, and a bounded sample. Explicit deep verification checks associations and
reconstructs applied changes and inherited receipt membership. Native file row
ranges establish initial occurrence membership without scanning source values.
After physical rewrites store origins, membership reads only those origin columns,
one captured file at a time. Reader buffers remain bounded as file count grows.
Receipt mutations use the existing bounded key replacement path and share
unaffected files. They retain the original input, changed native files, and applied
change tables; those costs belong in disk and performance measurements.

The local behavior is covered by `tests/evidence_updates.rs`. Qualification status
belongs in `docs/STATUS.md`; this interface description makes no performance or
hosted-provider claim.
