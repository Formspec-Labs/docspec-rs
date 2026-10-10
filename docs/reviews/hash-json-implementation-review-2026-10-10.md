# Hash-only JSON: independent implementation review

## Findings and verdict

**VERDICT: APPROVE.** No correctness, identity, error-handling, or admission
regression was found in the scoped patch. Repository-gate execution remains the
implementation task's responsibility and was not complete at this review.

Scope: the specified hash-only JSON changes against
`0011f7588ab58171c0f3d71e829b4902215ab70b`, reviewed 2026-10-10 under repository
instructions and the Rust and semi-formal
code-review skills. Only this report was changed. The reviewer ran no tests,
builds, formatting, benchmarks, corpus work, or commits, and read no other
review report or author design notes. The unrelated change-chain review was
left untouched.

**OBSERVATION — the demonstrated improvement is narrower than throughput.**
`json_digest` removes the complete temporary JSON byte vector from hash-only
sites and reuses maintained serialization and hashing. It does not remove
request admission's separate size pass, every caller allocation, or persisted
JSON serialization. No timing or process-memory measurement was supplied; no
speedup or whole-operation memory claim follows from this static review.

## Patch summary and function trace

The new crate-private helper sends compact Serde JSON directly into BLAKE3's
existing `std::io::Write` implementation. Callers still choose exactly the same
values, tuple order, key prefixes, and hexadecimal formatting. No persisted
schema, request type, dependency version, or validity check changes.

| Function | Inputs and important callers | Verified output and behavior |
| --- | --- | --- |
| `storage::json_digest` (`src/storage.rs:56`) | Any borrowed `Serialize + ?Sized` value; migrated callers below | One serialization feeds a local regular BLAKE3 hasher; returns its `Hash` only after successful serialization. |
| `corrections::records::scope_key` (`src/engine/corrections/records.rs:21`) | `(backfill::PROCESSOR, definition)`; requirements lookup and correction commit at lines 120 and 95 | Same `correction-requirements/` key; independent of adoption IDs and epochs as before. |
| `corrections::qualification_key` (`src/engine/corrections.rs:31`) | Validated proof; correction reuse at line 156 and retained-origin/inventory checks | Validation still precedes hashing; same `dependency-qualification/` key. |
| `backfill::file_key` (`src/engine/backfill.rs:1132`) | `(source, expressions.dependencies)` plus eligibility in prefix; planning at line 422 and assembly at `backfill/preparation.rs:34,150` | Same per-file completion address and eligibility boundary. |
| `Expressions::compile` (`src/engine/backfill/expressions.rs:29,140`) | `(PROCESSOR, spec, input_fields, dependencies)` after field interpretation | Same dependency digest; binding and supported-expression checks are unchanged. |
| `Engine::prepare_group` (`src/engine/backfill/preparation.rs:67,89`) | `(PROCESSOR, source.logical_metadata(), definition, expressions.input_fields)` | Same prepared-completion address and lookup before new computation. |
| `Engine::export_admitted` (`src/engine/export.rs:141`) | `ExportSpec`, after `export_request` admission at line 74 | Same request digest passed into export admission and persisted completion checks. |
| `validate_operation_record` (`src/engine/export.rs:574`) | Stored export request; retention inventory at `retention/inventory.rs:1186,1198` | Recomputes the same digest; `check_operation` still rejects different request content or digest. |
| `refresh::phase_key` (`src/engine/refresh.rs:420`) | Shared engine phase-key dispatch at `src/engine.rs:1883` | Incremental branch hashes processor, mode, required base manifest, transform, definition; full admission hashes processor and inputs; other full phases hash processor, transform, definition. Prefix, eligibility and phase suffix remain unchanged. |
| `owners::value_digest` (`src/engine/retention/owners.rs:193`) | Grant tuple through `protection_key` at line 197; binding value and parsed claim through lines 168 and 257 | Same owner-protection keys and reference-binding digests; raw path/key hashes at lines 180 and 188 remain unchanged. |

## Data flow and invariants

1. **Exact compact JSON bytes.** Pinned `serde_json` 1.0.151 implements
   [`to_vec`](/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/serde_json-1.0.151/src/ser.rs:2213)
   by allocating a vector and calling the same
   [`to_writer`](/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/serde_json-1.0.151/src/ser.rs:2177).
   The new helper changes the destination, not the formatter or supplied value.
   It does not convert typed requests through `serde_json::Value`, reorder maps,
   pretty-print, append a newline, or introduce a new canonicalization rule.

2. **Identical hash mode and complete writes.** Pinned BLAKE3 1.8.7
   [`Hasher::new`](/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/blake3-1.8.7/src/lib.rs:1089)
   and [`hash`](/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/blake3-1.8.7/src/lib.rs:920)
   use the regular hash with the same IV and flags. Its maintained
   [`Write` implementation](/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/blake3-1.8.7/src/lib.rs:1627)
   updates with every supplied byte, returns the full slice length, and has an
   infallible no-op flush. Serializer write boundaries do not change the digest.

3. **No success after a partial serialization failure.** The hasher is local
   to `json_digest`; `to_writer(...)?` precedes `finalize`. A serializer error
   therefore discards partial state and returns the same `serde_json::Error`.
   Callers still use the same conversion to application code `invalid_json`
   with its original cause (`src/error.rs:54–66`). No filesystem write or
   catalog mutation occurs in the helper.

4. **Admission and ownership remain unchanged.** This diff adds no resource
   limit or bypass. `request_memory` still performs bounded size admission
   before cloning (`src/engine/worker.rs:91–98`); correction and owner-binding
   paths retain their metadata checks (`corrections/records.rs:65–66,119`;
   `retention/owners.rs:161,247–250`). Existing repeated size/serialization work
   at those boundaries remains; this patch does not claim to eliminate it.
   The full hash-input vector is removed, while the hasher retains bounded
   internal state and callers still allocate required key strings. Persisted
   JSON and the backfill completion-size calculation are unchanged.

## Tests and evidence

The new tests compare the helper with the previous `to_vec` followed by `hash`
path, rather than comparing two calls to the new helper:

| Test | Assertions reviewed |
| --- | --- |
| `json_digest_preserves_typed_request_serialization` (`src/storage.rs:749`) | Frozen compact bytes for typed `GroupDefinition` and `ExportSpec`, including struct order and Unicode; old/new digests for these values and representative tuples. |
| `json_digest_preserves_nested_maps_unicode_and_unsized_values` (`src/storage.rs:781`) | Nested maps, null, booleans, integers, floating-point values, escaped text, empty/nonempty slices, unsized strings, and text crossing BLAKE3 chunk boundaries. |
| `json_digest_preserves_serialization_errors` (`src/storage.rs:802`) | A custom serializer fails after writing a field; an invalid map key also fails. Both paths retain error category, position, message, application code, and source message. |

The root-owned
[targeted library log](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/reviews/hash-json-targeted-lib-2026-10-10.log)
records these tests passing. The earlier non-library-filtered run was interrupted
and retained separately; it is not successful execution evidence. The full
repository gate remains pending.

Reviewed existing assertions also cover the surrounding identity consumers:
backfill recovery retains completion records across reopen/input loss
(`tests/backfill.rs:173`); bound field interpretation preserves or changes
dependency identities appropriately (`backfill/expressions/tests.rs:46,108`);
correction qualification reuses original outputs without overwriting retained
readback (`tests/dependency_corrections.rs:298`); refresh resumes only unfinished
phases (`tests/related_refresh.rs:601`); exports reuse retained materialization
and selection (`tests/exports.rs:719`); and owner grants/bindings preserve native
commit identities (`retention/owners/tests.rs:68,532`). Their assertions remain
applicable, but this review does not claim execution of those suites for this
patch.

There is no separate old-binary/new-binary recovery fixture or golden digest for
every migrated caller tuple. Compatibility here is supported by the exact
caller-argument diff, the pinned common serialization path, and the old-path
digest oracle tests. That coverage is proportionate to this narrow substitution.

## Conclusion

VERDICT: APPROVE. The patch removes unnecessary byte-vector materialization
through a small maintained-component adapter while preserving hash inputs,
persisted identities, errors, and existing admission. Changed-path coverage is
adequate for static approval; full-gate execution is pending. Confidence: HIGH
in correctness and identity preservation. Throughput and process-memory effects
remain unmeasured.

## Supplemental test-only review: unordered SQL result

The later gate correction at `src/query/selection_tests.rs:194–201` is approved.
It is separate from the hash-helper implementation review above. The SQL at
line 187 has no `ORDER BY`; the pinned DataFusion coalescing operator explicitly
makes no result-order guarantee (`datafusion-physical-plan-53.1.0/src/coalesce_partitions.rs:43`).
The fixture allows parallel execution. Its direct provider ordering assertions
at lines 168–170 remain unchanged.

The revised assertion zips the two columns before sorting, so it compares the
exact row multiset `[(Some(200), None), (Some(300), Some(3))]`. It preserves nulls,
row multiplicity, and column association: independently shuffled or mismatched
column values still fail. `values` preserves batch and row iteration order
without dropping nulls (`selection_tests.rs:78–89`); both columns have the same
row count under Arrow's `RecordBatch` invariant, so `zip` cannot silently omit
a valid column tail (`arrow-array-58.4.0/src/record_batch.rs:350–362`). Alias and
other selection/count assertions remain intact. No provider code or native
validity guard changed in this correction.

The retained first-gate log reports reversed first-column output and a failure
of the previous ordered assertion. It does not print the paired second column,
so that log alone is not evidence of correct association. The revised assertion
tests that association directly. The second repository gate is still running;
this static note does not claim its result. The reviewer ran no execution and
changed only this report.
