# Optional applied-change history: independent implementation review

Review date: October 10, 2026. Base: `b055102`; subject: the uncommitted patch and corrections inspected during review. **APPROVE for the inspected semantic scope.** No actionable semantic blocker remains. The original findings below were corrected and reread. Runtime qualification remains with the implementation owner; this report does not assert that compilation or tests passed.

This review applied the repository and parent instructions, the Rust skill and the semi-formal code-review skill. It inspected affected implementations, callers, callees and actual test assertions. It ran no builds, tests, formatters, benchmarks or data workloads and modified only this report. It did not read the unrelated change-chain implementation review. Scope covers independent optional-history lifetime and preservation of complete-state reads, verification, admission, recovery and restore. Accelerated public comparison is outside this patch.

## Findings and correction evidence

The severities below describe the original defects. Each entry records its corrected status.

### F1 — Complete-state Update validation lost its source transition checks

**Resolved. Original severity: BLOCKER / P1; category: correctness; confidence: high.**

Removing `verify_changes` from `verify_local_release` initially left a receipt-free Update without source schema-continuity or value-reconstruction checks. A caller could supply an immutable, internally valid source table with wrong values while retaining the correct request, inputs, definition and parent. The remaining Update branch checked metadata and computed groups; an Update without computed groups had no source transition proof. Hashing valid output did not establish the requested transformation.

The corrected trace is `verify_release_at` → verification walker → `verify_local_release` → `backfill::verification::verify_groups` → `write::verify_update` (`src/engine.rs:2300`, `:2386`; `src/engine/backfill/verification.rs:109`; `src/engine/write/verification.rs:12`). The new verifier preserves native schema/field identity at Routine level and at Deep level reconstructs from retained replacement/deletion inputs and the exact parent. It reuses normalization and null-safe key joins. Its grouped signed comparison preserves occurrence addresses outside the changed-key scope (`write/verification.rs:23`, `:46`, `:67`, `:76`, `:112`). It does not assume origin markers prove supplied values.

The physical-removal fixture substitutes valid wrong values, a wrong native schema, and equal values with a rewritten untouched origin, each in a newly serialized, digest-consistent manifest. It expects Deep or Routine refusal as applicable (`tests/updates/optional_history.rs:220`, `:309`, `:348`; helper `:449`). These assert semantic errors independently of corrupted bytes. Static correction is closed.

### F2 — Legacy complete-state JSON failed strict retention decoding

**Resolved. Original severity: BLOCKER / P1; category: compatibility and recovery; confidence: high.**

HEAD state JSON contains `DatasetPin.changes`, including null, and can contain `EvidenceResult.changes`. Ordinary decoding accepted those old members, but inventory's `Strict<T>` round trip rejected keys omitted by new serialization. Inventory reads selected Authority/ReleasePin records and retained parent manifests, so even a new descendant could encounter the error. Retention inspection and restore capture could refuse a previously usable workspace with `unsupported retention metadata fields`.

`Strict<T>` now calls `normalize_legacy_state_fields::<T>` before its existing known-field comparison (`src/engine/retention/inventory.rs:1642`, `:1664`). The exception is limited by actual decoded type and exact paths: DatasetPin, ReleasePin.dataset, Authority.selected and Authority.release.dataset. It applies only to supported formats. Non-null former changes must still decode as `Strict<ChangePin>`; malformed or unknown dependency fields refuse. PhaseResult changes remain required recovery data (`inventory.rs:1692`, `:1717`). Old state members gain neither roots nor certificate authority.

Tests cover supported formats with null/typed legacy members through all supported typed paths, malformed changes, unrelated nested fields, unsupported formats, misplaced members and required phase changes (`src/engine/retention/inventory/legacy_fields_tests.rs:85`, `:119`, `:158`, `:171`, `:190`). The strict-decoder defect is closed. These tests exercise the shared boundary rather than a complete old-workspace restore fixture.

### F3 — The explicit audit did not fully bind supplied endpoints

**Resolved. Original severity: BLOCKER / P1; category: correctness; confidence: high.**

The initial public `verify_applied_changes` could consume an altered in-memory after DatasetPin without resolving its immutable manifest. Nested ChangePin validation also initially allowed a before-manifest alias under the same version ID. A genuine VersionRef did not prove that the supplied table was its selected state.

The corrected audit shares `certificates::check_bindings`: supported rules/format, distinct operation/base, owner binding, nonempty evidence, and each nested pin's rules, base version and exact base manifest (`src/engine/changes/certificates.rs:117`). It resolves both complete ReleasePins against their manifests, performs incremental qualification when needed, and Deep-checks the after source before native reconstruction and receipt verification (`src/engine/changes.rs:27`, `:45`, `:55`, `:64`). Success creates no certificate authority or retention promise.

Migrated negative tests check absent/wrong source evidence, key IDs, base/counts, foreign removals, unsupported incremental provenance and the before alias (`tests/updates.rs:689`; `tests/related_refresh/incremental.rs:567`). The audit fixture also replaces the in-memory after source while retaining its selected manifest and expects `release_mismatch` (`tests/updates.rs:702`). No remaining endpoint-binding defect was found.

### F4 — Descriptor files lacked typed owner creation evidence

**Resolved. Original severity: WARNING / P2; category: retention and maintainability; confidence: high.**

`prepare_changes` creates `changes.json`, but its initial implementation did not expose the exact ObjectRef through owner creation enumeration. Independent owner admission and supported removal both use `confirm_creation`; a reserved directory proves ownership, not a completed object's identity. Delta files had phase creation evidence, but the descriptor could not be reclaimed after its roots ended.

`owners::visit_creation` now validates prepared/certificate records, checks their dataset's lifecycle table and observes the descriptor (`src/engine/retention/owners.rs:970`). Historical records retain creation facts after logical retirement without retaining live roots.

The source and receipt fixtures include the descriptor alongside exclusive delta Parquet files. Surviving preparation/phase/operation roots must block deletion; after retirement they require actual Removed outcomes and absent files (`tests/updates/optional_history.rs:39`, `:78`, `:103`, `:167`; `tests/evidence_updates/optional_history.rs:35`). The creation-evidence and reclamation defect is closed. Separate consumer protection uses the same inspected creation qualification path.

### F5 — Changed-path coverage and a routine expectation were incomplete

**Resolved for this scope. Original severity: WARNING / P2; category: test coverage; confidence: high.**

The routine test initially expected optional added/removed files to be checked. It now expects required source files (`tests/updates.rs:460`); explicit applied verification retains optional checks.

The source fixture covers independent roots, retirement retry, descriptor/delta removal, reopen, reads, verification, exact replay, succession, full comparison, fresh aggregate admission, restore capture and actual local restore after history removal (`tests/updates/optional_history.rs:13`, `:200`, `:239`, `:261`, `:327`). Required-file corruption still refuses. The receipt fixture covers receipt-only changes, duplicates/nulls, unchanged source addresses, direct OperationRecovery retirement of prepared changes, descriptor/delta removal, reopen, exact old/new receipts, verification and replay (`tests/evidence_updates/optional_history.rs:35`, `:170`, `:226`).

The HTTP test saves/reads a captured plan, checks exact certificate endpoints/revision, retires it, confirms the selected result survives, reconciles retry and refuses replanning retired history (`tests/http_optional_changes.rs:28`). Exact-generation/freshness checks delegate to existing guarded retirement; no independent concurrent OptionalChanges race execution is claimed. CLI wrappers are thin delegations without a newly inspected CLI subprocess test.

### F6 — Complete receipt verification lost native schema continuity

**Resolved. Original severity: WARNING / P2; category: correctness; confidence: high.**

Separating deltas initially preserved receipt values/association but removed the prior native schema/field-ID comparison. A valid table with changed native field identity and identical values could pass. First attachment also needed an expected/current schema check before a union could coerce payload types.

Complete receipt verification now calls `preserves_schema` against prior receipt state whenever the table changes (`src/engine/evidence.rs:620`). `verify_receipt_values` checks complete expected Arrow fields against the current native table before signed grouped comparison (`evidence.rs:855`). This protects prior field identity and first-attachment types/nullability independently of optional deltas. Existing nested receipt and duplicates/nulls tests exercise positive reconstruction.

The new adversarial fixture creates a valid native table with the same occurrence key and null receipt value but an integer payload instead of a string. It substitutes that table into digest-consistent first-attachment and replacement manifests, then requires the distinct expected type/schema refusals and verifies the original releases still pass (`tests/evidence_updates/optional_history.rs:259`). A coercible null value therefore cannot hide the changed payload type. The code defect and targeted static test coverage are closed.

## Function trace and authority boundaries

| Boundary | Code | Inspected behavior |
| --- | --- | --- |
| Complete versus optional state | `src/model.rs:141`, `:1097`, `:1109`, `:1123`; `src/evidence.rs:43` | Selected state omits embedded changes. AppliedChangeSet holds optional pins; ChangeCertificate binds endpoints and descriptor. Lookup creates no lasting availability promise. |
| Production/preparation | `src/engine.rs:1354`; `src/engine/changes/certificates.rs:142` | Actual producers stamp WRITE_RULES. Old unqualified phases remain usable without gaining new authority. Reserves memory before clones and reserves an owned attempt before writing. |
| Preparation reuse | `src/engine/changes/certificates.rs:175`, `:214` | Active reuse compares canonical JSON digest against newly assembled metadata without rereading the descriptor. Concurrent completion compares digest plus bytes. Selection independently reads/validates the descriptor once. |
| Atomic selection | `src/engine/history.rs:232`; `src/engine/changes/certificates.rs:298` | Active preparation matches exact parent/base and selected source/receipt metadata. Certificate, result and succession enter the same commit. Missing/retired preparation yields no certificate. |
| Local lookup | `src/engine/changes/certificates.rs:72`, `:241` | Dataset lock protects availability and selection checks. Certificate/result revisions and identities must match. Reads bounded digest-bound metadata; missing/retired history returns None. |
| Independent audit | `src/engine/changes.rs:21`, `:157` | Resolves endpoints; checks integrity, schema, removal membership and reconstruction. Writes no authority. Rules strings and origins alone do not certify arbitrary content. |
| Complete verification | `src/engine.rs:2386`; `src/engine/backfill/verification.rs:34`; `src/engine/write/verification.rs:12`; `src/engine/evidence.rs:620`, `:748`, `:813`, `:855` | Required source/computed/receipt checks remain. Reconstruction uses retained requests, inputs and predecessor state without optional history. |
| Optional inventory | `src/engine/retention/inventory.rs:813`, `:993`, `:1002`, `:1054`, `:1527` | Deduplicates descriptor expansion and binds it to preparation identity. Roots applied tables and endpoint interpretation metadata without opening entire endpoint snapshots. State/recovery/owner roots remain independent. |
| Retirement | `src/engine/changes/retirement.rs:22`, `:89`; `src/engine/retention/retirement.rs:44`, `:184`, `:513` | Distinct scopes for certificate and preparation/recovery. Shared code validates snapshot ancestry, exact generation and retry before markers. No physical deletion or result unavailability follows automatically. |
| Terminal recovery | `src/engine/retention/recovery.rs:24`, `:192`, `:249` | Includes active prepared changes or validates their prior independent retirement against the captured decision. Processing phases and reservations retain separate promises. |
| Removal | `src/engine/retention/owners.rs:970`; existing remover | Exact creation remains provable after retirement. Every independent root must end before the supported remover admits deletion. |
| Public comparison | `src/engine/comparison.rs:304` | Remains unchanged/full only; HistoryNotQualified fallback remains. Certificates do not enable accelerated public chain comparison. |

## Data flow and invariants

Actual writes create complete state and optional applied rows. Recovery owns outputs before selection. A later sampled validation cannot upgrade an older phase that lacks producing-rule qualification. Preparation serializes exact optional pins under a bounded, digest-bound reference; retries compare content identity rather than names or row counts.

Selection writes result/certificate/succession atomically. Lookup requires equal result/certificate revision and exact identity. An explicit audit of supplied evidence creates no local history. Descriptor inventory tracks both immutable object identity and preparation binding, so the same path under another operation cannot borrow an earlier expansion.

Logical retirement preserves historical creation records. Ending optional history cannot delete files still required by state, phase, preparation, owner or restore roots. After every relevant optional/recovery claim ends, complete-state objects remain protected. Deep state verification reconstructs source values and receipts from retained evidence when descriptor/delta files are physically absent. Routine verification intentionally checks required state without scanning optional history.

## Additional tests inspected

Migrated tests preserve independent expected values for updates, composite nullable/quoted keys, nested field identity, overlap refusal, empty replacements and interrupted recovery (`tests/updates.rs:188`, `:353`, `:604`, `:733`, `:923`). Receipt tests retain duplicates/nulls, associations, list ordering and null-versus-empty distinctions, plus process-loss recovery (`tests/evidence_updates.rs:229`, `:303`, `:437`, `:671`, `:897`). Their optional assertions now use certificates separately.

Origin/survivor, compaction, aggregate/join oracle, succession and rebuild tests retain state assertions (`tests/change_origins.rs:86`, `:301`; `tests/compaction.rs:179`, `:288`; `tests/related_refresh/aggregation.rs:136`, `:243`; `tests/succession.rs:73`; `tests/rebuild.rs:121`). They do not treat an origin marker as proof of values.

Inventory tests use nonexistent endpoint manifests to prove optional expansion does not read full endpoints and check duplicate-path binding conflicts/mismatched descriptors (`src/engine/retention/inventory/optional_changes_tests.rs:81`, `:114`, `:133`). The certificate fixture refuses wrong revision, malformed metadata and rebound endpoints while complete verification/replay remain independent (`tests/updates/optional_history.rs:396`). Shared retirement scope tests distinguish certificate, phase and operation keys (`src/engine/retention/retirement/tests.rs`).

## Resource use and simplicity

Removing optional table inventories from immutable results and reusing guarded retirement, owner protection, native decoding, normalization and DataFusion comparisons is a concrete structural simplification. The patch adds a descriptor and preparation/selection path without another database or row-processing engine.

Preparation reserves metadata allowance before cloning, bounds serialization with max_metadata_bytes and uses bounded descriptor reads (`src/engine/changes/certificates.rs:163`, `:192`, `:280`, `:319`). Inventory charges and deduplicates descriptor bindings (`src/engine/retention/inventory.rs:813`). These are metadata bounds, not measured whole-process memory or population-scale guarantees. Deep applied/source/receipt checks scan retained state/inputs and use grouped execution under the existing budget. No speed, memory or storage-amplification benefit is claimed without measurement.

## Review certificate

- **Verdict: APPROVE for the inspected semantic scope.** Original actionable findings are statically closed.
- **Changed-path coverage: sufficient for this static review.** Focused lifecycle, semantic-negative, recovery and HTTP assertions are present, including altered-after audit and coercible-null receipt-schema regressions.
- **Execution: not performed by this reviewer.** The implementation owner's focused tests and repository gate establish runtime qualification. Temporary compiler/test-registration work was not treated as a semantic defect.
- **Confidence: high** in traced corrections and authority/lifetime separation; **medium** in unexecuted recovery, concurrency and resource behavior.
