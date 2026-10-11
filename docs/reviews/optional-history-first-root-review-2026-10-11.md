# Optional-history fixture correction

APPROVE fixture-only patch `/tmp/docspec-optional-history-first-root.patch`, SHA256 fa07af81c9e29d9e0e9860d5bc49ff9d22d102027d1edbfc93abd955145cabfd. Apply-check passed; no Cargo/test execution.

The old suffix assertion at tests/updates/optional_history.rs:121 assumes processing wins first-blocker selection. Inventory only fills an absent blocker (retention/inventory.rs:771–777), and the new phase/checked/<validator-material-hash>/validation key can sort ahead of processing. Validation copies the exact processing PhaseResult and changes at engine.rs:1689–1693. Inventory expands phase.changes through the same required closure at inventory.rs:1465–1467. A validation blocker is consequently a real independent promise, not evidence of missing processing protection.

The replacement resolves the actual reported blocker key in the current dataset's retained records, decodes PhaseResult, and requires the exact current update operation, processing or validation phase, and exact ChangePin equality against the original qualified source change. It does not accept arbitrary phase, authority, unrelated operation or different changes. Existing every-object-protected checks, zero unlink while protected, all-current-operation-phase retirement, exact physical removal and cold complete-state/consumer reads remain intact. One inspection is reused for both the assertion and the original phase retirement key collection.

This changes the claimed invariant to the supported one: current processing/validation still retain the exact optional applied rows after descriptor-preparation retirement. Root owns focused runtime confirmation.
