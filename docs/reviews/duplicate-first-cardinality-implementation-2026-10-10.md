# Duplicate-first join cardinality proof

The join planner now checks the constrained input for duplicate matchable keys
before running the existing matched-key proof. A unique constrained input avoids
executing the other input during cardinality validation. The main result remains
lazy and uses the same join query, fields and occurrence identities.

`duplicate_keys_query` uses DataFusion grouping over the complete composite key.
`NullMatching::Never` excludes keys with any null component; those rows cannot
match under ordinary equality. `NullMatching::Equal` groups nulls normally and
retains the existing null-safe semi-join proof. Duplicates alone do not cause
refusal: the existing proof must also find a matching partner. One-to-one joins
check both roles independently, including when both roles share a source.

The implementation preserves existing optimizer guards and immutable input
inventory reuse. It adds no cache, index, configuration or ordinary reporting.
The duplicate branch adds a constrained-input grouping pass before the existing
proof; this is a deliberate tradeoff for the unique-key branch.

## Verification prepared

- The unit fixture `cardinality_scans` uses the maintained `StreamingTable` and
  `PartitionStream` interfaces to count actual partition execution, without
  production instrumentation. It expects no scans for many-to-many, only the
  constrained input for unique many-to-one, each constrained input once for
  unique one-to-one, and the existing two-input proof for matched or unmatched
  duplicates. It leaves the returned main frame unexecuted to isolate validation.
- `related_refresh::cardinality` compares legal full-join results with the existing
  direct Rust oracle, covering unmatched duplicate groups on both sides, both
  null policies, partial/all-null composite keys, both constrained roles and
  self-joins. Matching duplicates must refuse before a result can be returned.

No Cargo command, benchmark, corpus read or provider call was run by the
implementation agent. The root agent owns formatting, builds and tests and will
record their results. These fixtures establish semantic and scan-execution
behavior once passing; they do not establish elapsed-time or resource improvement
on representative data.

## Root verification

The [focused tests](duplicate-first-cardinality-tests-2026-10-10.log) pass,
including the physical execution counts and exact value/evidence cases. The
[independent review](duplicate-first-cardinality-review-2026-10-10.md) approves.
The combined join/metadata [final gate](join-metadata-full-check-2026-10-10-2.log)
also passes; [STATUS](../STATUS.md#avoid-redundant-join-and-metadata-reads)
records the checkpoint and its measurement limits.
