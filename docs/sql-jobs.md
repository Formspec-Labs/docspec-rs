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
