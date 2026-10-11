# Stable-schema fixture follow-up review

Static approval; no production changes or weakened original comparison/recovery assertions found. No builds were run.

Reviewed artifacts:
- comparison: SHA256 cfc57c5a15dc18ebbcd93e2863ae4815d94c8799067b345d31601a6e2925f38a
- rebuild completion: SHA256 055137742c75da62e29099ba02fbca5b5eb17a91b29a27cbcfa2707b3d199705

`tests/comparison.rs` keeps the real coincident-ID qualification: both independent builds allocate IDs 1/2, and both directions, Values/Occurrences, automatic/forced-full still refuse unqualified changed field meaning. The added based attempt must fail `invalid_schema` and preserve latest. Explicit unbased authoring is the supported fresh-meaning path; it does not bypass the based schema rule (`src/engine/write.rs::process_build`).

`tests/rebuild_completion.rs` establishes actual hidden origin fields through public Update before backfill and paused Build capture. The later update replaces the same key 9000, so it changes values rather than adding the first origin schema history. The three-row completed replacement Build source remains unchanged. Native-commit pause, newer authority boundary, old processing retention, unavailable external input, exact TablePin reuse, parent manifest, deep verification and unchanged phase row assertions all remain. This isolates the intended native commit/rebase recovery case; separate first-origin history invalidation coverage remains in `tests/rebuild.rs`.

Runtime qualification remains parent-owned.

## Runtime correction

The comparison fixture's new refusal assertion initially expected `invalid_schema`;
the existing name-normalization boundary returns `schema_mismatch`. The test now
asserts that precise code. The failed invocation is retained in
`schema-policy-progress-focused-2026-10-11.log`; its native-commit/rebase, actual
CLI reuse and same-consumer policy cases pass. No production refusal changed.

`comparison-schema-refusal-fixed-2026-10-11.log` passes the precise refusal and
all original coincident-identity comparison cases.
