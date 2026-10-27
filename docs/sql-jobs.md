# SQL jobs

Run `docspec --limits LIMITS.json job --spec JOB.json`, or deserialize
`docspec::job::JobSpec` and call `run(limits)`. Both use the same bounded service.
The CLI requires explicit host limits and opens no lifecycle workspace.

A job declares `scratch`, `inputs`, `steps`, `outputs` and optional `queries`.
Inputs are named Parquet paths, exact Iceberg pins, or gzip JSON-lines files
with an explicit Arrow schema. Every JSON field must be nullable. The reader
adds `__source_key`, one-based `__source_line` and the unchanged `__raw_line`.

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
Existing outputs are never overwritten. Remote CLI configuration is not yet
bound to this command; `--remote` refuses explicitly until the C5 adapter lands.

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

`job::tables::purge_if_current` checks the selected UUID and metadata location
against the catalog, then makes one purge attempt. The REST adapter never blindly
retries purge. After any uncertain result, the caller must reload authority and
recheck it before another attempt. This check does not provide atomic server-side
purge conditions: callers must exclude concurrent replacement during maintenance.
