---
id: def-9ise
status: open
deps: []
links: []
created: 2026-10-29T11:30:20Z
type: bug
priority: 2
assignee: core
tags: [engine-first, C3, C7, metadata]
---
# Preserve Parquet field metadata through native SQL handoffs

The isolated CT three-phase model reproduces all public rows, but reading native Parquet handoffs with JobInput::Parquet loses inherited PARQUET:field_id metadata. sources.parquet changes from 5311 to 5127 bytes; its 4512-byte data region is identical, while footer metadata for link_text (24) and doc_type (28) disappears. Two private evidence files also lose field IDs. This blocks byte-identical phased execution, which remains parked; the one-job release path is unchanged. Lead 20261029T110636 and revised 111712 request a native capability ticket, not a release-path workaround.

## Design

Reuse the native Parquet reader and maintained schema reconciliation. Current src/job.rs register_parquet uses ParquetReadOptions::default; DataFusion53 defaults skip_metadata=true, and JobInput::Parquet exposes only name/path. Inspect scratch materialization reads too. Preserve valid field identity without inventing IDs across conflicting branches. No application casts, metadata injection, custom writer, relaxed byte assertions or fallback release. Evidence: S/codex-impl/cx_model_phases/cx_metadata_boundary_blocker.json and final-report.md; S is the engine-first coordination scratchpad. Cross-budget hash invariance is currently unproved, not an existing promise: def-tvun currently requires byte equality on pinned limits.

## Acceptance Criteria

Fixture-first native regression reads real Parquet field metadata through a handoff and retains valid nested/scalar field IDs, exact rows/types/nullability, including conflicting-ID and missing-ID cases under existing reconciliation. The retained CT 50-body fixture must keep its original release ID and all nine public plus sixteen private byte-hash assertions across the phased boundary. Qualify the same immutable inputs/SQL/writer settings with memory-resident and spilled materializations; determine and document whether limits alone may change public file hashes/release identity, and enforce the chosen boundary before publication. Run focused native/project gates and one independent code review before merging; no network writes, credentials or real-data mutation.


## Notes

**2026-10-29T12:18:01Z**

The isolated CT prototype is now GREEN and independently APPROVED at product8e0a64eb, using the original native reading fields instead of injecting metadata into flat handoffs. The retained50-body release ID and all9 public/16 private file hashes match;71 focused/default cases pass. It pays for two additional immutable lake-fact scans, and leaves the actual native Parquet metadata-loss capability unresolved. It is unmerged and lacks phase-mode controller/E4/full native-table qualification, so no corpus-fit or release-path promise follows. Preserve the original RED receipts. The native capability and cross-budget identity question remain open as a separate lower-priority follow-up.

**2026-10-29T17:42:02Z**

October 29 reconciliation — 30: Rust retained-memory and last-reader lifetime.

Keep open; field metadata and cross-budget identity remain separately unresolved.

The preserved sibling remains the owner of native Parquet field metadata and the cross-budget byte-identity question. The October 29 CT prototype success in the preceding note does not close this native capability. Keep the original RED evidence, required field-ID/conflict cases and unchanged release/hash assertions. The retained-memory candidate in dec-eog7 addresses a separate boundary.

Evidence: [rust/manifest.json](/Users/mikewolfd/Work/spicy-stack-review-work/reconciliation-20261029/rust/manifest.json).

Full item-to-owner mapping and preserved pre-update records: [ticket update report](/Users/mikewolfd/Work/spicy-stack-review-work/reconciliation-20261029/ticket-updates/REPORT.md).
