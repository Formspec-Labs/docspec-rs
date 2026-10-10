//! Ordinary integration suites share one linked test executable.
// Rust guideline compliant 2026-02-21

#[path = "support/enriched.rs"]
mod enriched;
#[path = "support/scalar.rs"]
mod scalar;
#[cfg(feature = "test-util")]
mod support;

mod abandonment;
mod admission_formats;
mod availability_retirement;
mod backfill;
mod backfill_dependencies;
mod change_origins;
mod change_transport;
mod compaction;
mod compaction_retirement;
mod compaction_retirement_transport;
mod comparison;
mod completed_backfill_plan;
mod continuous_backfill;
mod cutover_admissions;
mod defaults;
mod dependency_correction_lifecycle;
mod dependency_correction_transport;
mod dependency_corrections;
mod evidence_updates;
mod exports;
mod grant_retirement_transport;
mod http_optional_changes;
mod http_read;
mod input_generations;
mod lifecycle;
mod operation_retirement;
mod operation_retirement_transport;
mod phase_dependencies;
mod phase_retirement;
mod phase_retirement_transport;
mod physical_removal_process;
mod physical_removal_transport;
mod qualification_roots;
mod rebuild;
mod rebuild_completion;
mod refresh;
mod related_refresh;
mod restore_catalog;
mod retention_inspection;
mod scalar_bindings;
mod schema;
mod succession;
mod updates;
