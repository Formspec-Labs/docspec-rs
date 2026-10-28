# SQL jobs

Run `docspec --limits LIMITS.json job --spec JOB.json`, or deserialize
`docspec::job::JobSpec` and call `run(limits)`. Both use the same bounded service.
The CLI requires explicit host limits and opens no lifecycle workspace.

A job declares `scratch`, `inputs`, `steps`, `outputs` and optional `queries`.
Inputs are named Parquet paths, exact Iceberg pins, or plain/gzip JSON-lines files
with an explicit Arrow schema. Every JSON field must be nullable. The reader
adds `__source_key`, one-based `__source_line` and the unchanged `__raw_line`.
A complete source line must fit `max_batch_bytes` and the signed 32-bit Arrow
string limit. The reader admits raw-buffer growth before reading, holds the
native decoder's
returned arrays at their measured allocation, and releases completed raw
workspace before the next yield. Native Arrow owns decoding and its temporary
workspace. No structural or declared-width multiplier estimates decoded size.
Reservation failures retain their resource cause and identify the source key
and line. Decoded batches remain subject to the same byte allowance.

A `native_table` input selects `name`, immutable `metadata_location`,
`table_uuid`, `snapshot` and `schema_id` from a completed table operation.
All selection fields must match that metadata; a null snapshot means exactly an
empty table. It performs no current-head lookup or lifecycle inventory. Stored
and decoded metadata bytes are bounded, and decoded admission stays with every
provider, plan and stream. The caller retains the selected immutable objects.

Results report `native_inputs` for exact native inputs, using their registered
names. `data_file_count` comes from the selected snapshot's native summary;
missing or invalid optional counts remain null. An exactly empty selection has
zero data files. `snapshot_count` counts retained snapshots in that selected
metadata file. `manifest_count` and `planning_seconds` remain null until a
maintained native planning hook exposes them. These observations perform no
additional head lookup, planning pass, manifest walk or data read, and do not
independently verify the files. Embedded callers can use
`Job::native_input_observation(name)` on a registered native input.

Steps run in order. A step's `materialize` flag selects a retained result,
reserved in memory or stored in managed scratch; otherwise it is a view.
SQL must use immutable functions. Each output declares its SQL, local path and
`ExportSettings`. Ordering keys must be unique. `columns` can omit internal
ordering fields. Encoding selectors name exact Parquet leaf paths; profiles
choose whole-file, ranged or key-lookup layout. Local output and scratch must
share a filesystem for create-only linking.

The result contains file hashes, sizes and row counts. Named `queries` return
engine-serialized JSON arrays within the metadata allowance, suitable for
manifest counts. Their retained memory remains admitted until the result drops.

On failure the library returns `JobFailure.completed`. The CLI prints that
structured failure and exits unsuccessfully. Keep completed file identities,
correct the failed work, and retry with only unfinished output declarations,
using the same pinned inputs and unchanged definitions for reused files.
Existing outputs are never overwritten. `--remote CONFIG.json` supplies the
native catalog and object connections to this same executor; embedded callers
use `run_with_remote`. No Engine or lifecycle store is constructed. Configured
remote pin metadata uses one bounded GET; its Content-Range carries the full
object size, checked alongside the digest, snapshot and schema. It sends no
HEAD for this immutable metadata. Local or directly injected FileIO checks
observed file size before and after reading in every build. These observations
do not replace the caller's immutable-storage and retention obligations.

`docspec --limits LIMITS.json --remote OBJECTS.json create-file --spec FILE.json`
uses only the configured object connection. `FileUpload` supplies a local source,
destination, SHA-256, MD5, size, content type and cache control. The engine reserves
the complete transfer allowance before opening the file, checks both hashes in
one local read, and sends one conditional PUT with native Content-MD5. It performs
no HEAD, GET, multipart transfer or automatic retry. A 412 returns `already_exists`
without claiming readback verification; successful creation returns `created`.
Cancellation retains admission until the worker finishes. Whole-file memory is
linear in file size and may refuse a file that does not fit the explicit budget.

A step may declare `release_after: ["finished_name", ...]`. Names are checked
before input I/O, and removed only after that successor succeeds. Unknown,
forward, duplicate and repeated releases refuse. Existing frames and views keep
their dependencies alive; memory and scratch are freed at the final owner's
release. Declare dependent views too when their inputs have reached last use.

A component step declares `name`, `components` and optional `release_after`:
`{"name":"groups","components":{"sql":"SELECT left_key,right_key FROM edges",
"left":"left_key","right":"right_key","node_limit":2000000}}`.
It materializes `node` and `component`, both strings. The label is the smallest
node by UTF-8 bytes in the undirected connected component. Empty strings,
duplicate edges and self edges are valid; null or non-string endpoints refuse.
SQL-view options cannot be mixed into this step shape. Node limits are explicit;
container growth, retained key bytes and output batches share the job allowance.
Expected work is linear in endpoint bytes plus union-find operations
`O(E α(N))`, with `O(N)` node state. It never retains the edge population.

Jobs also register `url_resolve(reference, base)`,
`uri_normalize_encoding(value)`, `percent_decode_utf8(value)`,
`url_filename(value)`, `disposition_filename(value)`, `json_array(...)` and
the maintained `datafusion-functions-json` getters. Invalid UTF-8 escapes remain
unchanged; decoding preserves literal `+`. Download names prefer a valid
`filename*` over `filename`; duplicate filename parameters refuse. These
primitives impose no host, scheme or application identity policy.

`json_canonical(text)` parses JSON and emits recursively UTF-8-byte-sorted
object keys in compact JSON. Duplicate keys take the last value, matching
Python's JSON reader. Jiter preserves arbitrary integers and parses finite
binary64 floats; Serde handles JSON escaping and structure, with Python's
float spelling thresholds and exponent padding. No Python runtime is linked.
Invalid, nonfinite or excessively nested input refuses. Parsing is admitted
before allocation, one tree at a time; output bytes are bounded. Object ordering
costs `O(Σ K log K)` for each object's `K` keys. Selection of records and hashing
the resulting bytes remain caller policy.

`json_get_many(raw, paths_json)` extracts prefix-disjoint raw JSON fragments in
one Jiter traversal per input row. The second argument is a JSON array of paths,
supplied as a literal or a string column, whose components are string keys or
zero-based integer indices; duplicates keep their positions. Negative or null components give null
slots. Proper ancestor/descendant selections refuse; `[]` selects nothing and
`[[]]` selects the root. Each result is a valid list of nullable JSON strings,
with the same lexical bytes, first-duplicate-key and lazy malformed-tail behavior
as independent `json_get_json` calls. Paths compile into the immutable expression
once for literal definitions. Column definitions compile once per row and drop
after that row; no growing definition cache or input-sized value tree is built.
A null or invalid definition refuses. A null input produces null slots for its
valid definition. Parser workspace and actual output growth are admitted before
allocation. The executor fits multi-row output to its byte allowance; a single
row that cannot fit refuses. Values are never truncated. Work is linear in
scanned input, path state and copied output; duplicate selections can multiply output bytes.
Common-expression reuse depends on the native plan: inspect it before assuming
reuse across branches, and retain narrowed relations rather than raw bundles.

`http_date(text)` returns a nullable UTC microsecond timestamp using the maintained
`httpdate` parser. It accepts the three HTTP date forms in the supported
1970–9999 range; invalid or unsupported values return null. Obsolete two-digit
years use a fixed 1970–2069 pivot, with no current-clock dependency. Fixed-width
output is admitted before allocation. Header meaning and retry policy remain SQL.

`json_array` uses Arrow's JSON encoder with explicit nulls and compact output.
It accepts strings, booleans, integers and nested lists or structs containing
those types. Other types refuse, including floating point values whose spelling
would not establish Python identity equivalence. Output expansion is admitted
before allocation. Work is linear in the input and resulting bytes.

URL resolution uses locked `url` 2.5.8. The pinned 819-case upstream resolver
corpus has exactly 36 known upstream `file:` URL deviations, retained by name
in `tests/fixtures/url/expected_failures.txt`; all other cases match. No URL
setter behavior is claimed. See `tests/fixtures/url/NOTICE.md` for source and license.

`job::tables::append_with_retry` runs caller-supplied work preparation and the
native writer against a loaded table. After a catalog conflict it reloads and
prepares again, with at most three retries. A work identity covers selected rows
and the writer definition. Completed files are reused only while that identity,
table UUID, schema ID and partition-spec ID match. Empty work writes and commits
nothing; unrelated errors propagate. File layout and table creation properties
remain caller choices, using `storage::write_batches_with_layout` and native
`Catalog::create_table` rather than a second creation implementation.

A job can declare `catalog_root` for an explicit local native catalog and
`table_operations` after its ordinary inputs and steps. Embedded callers can use
`run_with_catalog(limits, catalog)` instead; local and injected authority cannot
be combined. No lifecycle workspace is opened. Operations run before exports
and metadata queries, and completed operation reports survive later failure.

Operations use strict tagged shapes: `create_namespace` supplies `name`,
`namespace` and `properties`; `create` supplies `name`, `table`, `location`, native
Iceberg `schema`, `format_version`, `properties`, and optional native
`partition_spec`/`sort_order`. Creation refuses existing names. `purge` supplies
`name`, `table`, `table_uuid` and exact `metadata_location`.

An `append` supplies `name`, `table`, distinct `current_table` and `work_table`
binding names, `sql`, `identity_sql` and `layout`. Each conflict reloads native
authority, rebinds the current table and materializes the work SQL once. Actual
empty work performs no commit. Nonempty work requires exactly one nonempty
string cell from identity SQL over the materialized work. That identity must
cover the selected immutable rows and writer definition. The native writer
streams the same retained work using the job's shared budget. The temporary
bindings are released after success or refusal. Preparation repeats at most
four times; identity ordering may cost `O(W log W)` for `W` selected rows.

Table identifiers use native JSON, for example
`{"namespace":["archive"],"name":"captures"}`. Layout is
`{"file_bytes":2097152,"row_group_rows":2048,"row_group_bytes":2097152}`;
the caller chooses these values within its limits. Results retain UUID,
metadata location, snapshot/schema IDs and the newly written file paths, row
counts and sizes without reopening completed data files.

`job::tables::purge_if_current` checks the selected UUID and metadata location
against the catalog, then makes one purge attempt. The REST adapter never blindly
retries purge. After any uncertain result, the caller must reload authority and
recheck it before another attempt. This check does not provide atomic server-side
purge conditions: callers must exclude concurrent replacement during maintenance.
