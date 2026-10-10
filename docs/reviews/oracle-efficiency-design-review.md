# Exact oracle efficiency: design review

**Verdict: APPROVE the thread-only diagnostic; DEFER dictionary encoding.** The first arm changes one maintained execution setting while retaining the exact checker. No blocking design finding remains. Success would justify trying that setting in a newly captured acceptance series; it would not repair the historical failed run or establish a speedup.

Static review on 2026-10-10 at `97d0e75101aa3d2b5c4763f246f82cf6c00639e3`, using the semi-formal architecture-review skill. Scope: `experiments/comments-e2e/oracle-efficiency-plan.md`, its parent protocol, current Python oracle/monitor, and retained observation metadata. No tests, builds, benchmarks, corpus payload scans, or implementation changes were performed. The root agent owns execution.

## Findings

**F1 — OBSERVATION; scope; KEEP the diagnostic interpretation.** The retained failure occurred in `exact native join`, after earlier full source/evidence checks, and the series explicitly failed (`acceptance-export-layout-20261010.json:917-935`). The new plan correctly preserves that result and labels the new 600-second observation as retained-output readback (`oracle-efficiency-plan.md:20-47`). A historical failure and one later run under different cache/load conditions cannot isolate a causal speedup. Failure at the same scratch bound rejects the sufficiency of this four-thread setting, not every concurrency explanation.

**F2 — OBSERVATION; ownership; KEEP the selection-check boundary explicit.** `oracle.py` checks pinned manifests, admitted bytes, exact native snapshots and exported objects. Current-catalog native/export selection is checked separately by `run.py:280-285`. Describe the diagnostic as the complete existing oracle over retained pins; catalog-selection success remains evidence from the original engine run. A workspace symlink preserves the oracle's resolved admitted-input containment check (`oracle.py:134-144`), but it neither freezes the target nor creates a new catalog-selection observation. Keep the retained target stable during collection and preserve absolute paths in copied metadata.

**F3 — OBSERVATION; resource accounting; KEEP the separate storage baseline.** The monitor uses `os.walk` without following directory symlinks (`comments-input/run.py:41-54`). The diagnostic's new scratch is therefore visible, while retained output behind `workspace` is excluded from its inventory. The plan already records that output separately (`oracle-efficiency-plan.md:33-39`). Register the new scratch prefix relative to the monitored directory, include the diagnostic runner and descendants in RSS, and retain current free-space preflight plus stop reasons (`comments-input/run.py:104-117`, `:134-167`, `:202-228`). Report sampled peaks as observations; the prior sample missed the higher scratch usage reported by DuckDB (`docs/STATUS.md:99-105`).

**F4 — CONCERN for the fallback only; unstated assumption; DEFER until separately frozen.** A file-inventory dictionary is sufficient only when every compared contributor path belongs to that exact domain. The reader can instead use stored `__docspec_origin_file` values, which need not equal current physical file paths (`oracle.py:200-225`). Before adopting an ENUM fallback, establish the frozen workload's physical-origin case or provide the validated logical-origin domain. Use one shared exact mapping for both relations, preserve SQL nulls, retain the original schema checks, and make unknown non-null paths fail through strict conversion. Do not use `TRY_CAST`, a dropping join, independent integer dictionaries, normalization, or a null sentinel. Preserve full rows and signed multiplicities. These are proof obligations for a later change, not reasons to delay the first diagnostic.

## Artifact summary and lineage

This is proof infrastructure serving the operator's decision: can the complete independent checker fit the existing budget without changing its equality algorithm? Exact retained releases enter; PyIceberg and PyArrow validate native metadata, then DuckDB reconstructs and compares values; new check receipts and resource observations come out (`oracle.py:124-129`, `:153-226`, `:242-346`).

| Prior artifact | Relationship and constraint |
| --- | --- |
| `docs/PLAN.md:44-75`, `:134-165` | Reuse completed work; preserve full exact qualification; acceptance still requires three fresh complete runs within 1,800 seconds each. |
| `docs/requirements/07-operations-qualification.md:23-37`, `:149-157` | Separate engine/process limits, retain failed attempts, keep independent verification visible, and distinguish a component result from full qualification. |
| `experiments/comments-e2e/protocol.md:74-108`, `:135-179` | Exact multisets, schemas, evidence and frozen execution sources remain load-bearing; scope or resource failures cannot pass acceptance. |
| `docs/STATUS.md:90-109` and `acceptance-export-layout-20261010.json:782-817`, `:917-935` | Engine/export completion and oracle scratch failure motivate this diagnostic; neither establishes the remaining readback checks. |

| Named boundary | Owner | Dependency and consumer |
| --- | --- | --- |
| Retained release/input pins → oracle views | Oracle | Serialized releases, admitted input identities and native snapshot membership → independent expected relations (`oracle.py:134-240`). |
| Views → exact equality | DuckDB through oracle | Typed values and signed occurrence counts → completed-check receipts (`oracle.py:97-106`, `:228-231`). |
| Child execution → resource result | Existing Python monitor | Process tree, scratch inventory, live free space and deadline → retained metrics/failure (`comments-input/run.py:104-228`). |
| Diagnostic → acceptance decision | Root experiment runner | All diagnostic checks within bounds → candidate setting for fresh acceptance, never retrospective pass (`oracle-efficiency-plan.md:41-47`). |

## Invariants and hypothesis trace

| Commitment / hypothesis | Status | Evidence and failure prevented |
| --- | --- | --- |
| H1: lowering only `threads` preserves the existing comparison | CONFIRMED statically; PRESERVED | The setting is independent of `exact_equal`'s signed `UNION ALL`, all-column grouping and zero multiplicity difference (`oracle.py:97-122`). Schema, null, nested-value and evidence checks remain in `run_checks`. |
| H2: four threads will fit the budget | UNRESOLVED | The failure identifies exhausted spill, not its allocation mechanism. Runtime evidence must decide; retain 18 GiB engine memory, 24 GiB observed aggregate RSS and 32 GiB scratch (`oracle-efficiency-plan.md:15-23`). |
| All existing checks execute, including earlier successful ones | PRESERVED by proposed scope | `oracle.py:242-346`; no checkpoint/reuse branch is introduced. Repetition is justified by checking the whole modified process without building another recovery mechanism. |
| Failed and completed original evidence survives | RELIED-UPON | Separate metadata/scratch/output directory, unchanged retained target, and captured source difference (`oracle-efficiency-plan.md:20-31`). Copy the original captured oracle and its siblings, then retain the exact one-line change. |
| Acceptance remains a complete fresh-workspace measurement | PRESERVED | Diagnostic labels and decision rule keep the separate 600-second bound from replacing the parent's three-run gate (`protocol.md:17-23`, `:135-140`). |

## User value, simplicity and counterfactuals

The setting-only arm adds no comparison algorithm, persisted dictionary, data copy, or production configuration surface. It tests the smallest available intervention against a concrete failed checker. DuckDB's [workload guidance](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads) and [performance checklist](https://duckdb.org/docs/current/guides/performance/my_workload_is_slow) support thread-count tuning generally; those current documents do not establish the retained DuckDB version's behavior on this workload.

The query still groups full-width rows from both relations. Thread reduction changes execution concurrency, not the number of values that require comparison (`oracle.py:97-106`, `:283-300`). A later path dictionary could narrow repeated contributor keys, but would still compare all ordinary values and occurrences; its conversion/build cost and full-oracle result must be measured. Neither intervention has demonstrated a resource improvement yet.

The kill criterion is any mismatch, missing check, timeout, or exceeded diagnostic bound. Keeping twelve threads leaves the measured failure unresolved; raising limits changes the decision being tested; deleting independent checks loses the required evidence. Removing this diagnostic only removes a bounded decision aid—the existing full acceptance harness remains the authoritative consumer. That harness already supplies execution capture and monitoring, so a new benchmark framework is unnecessary. A future reviewer should reject any attempt to promote this isolated retained-output result into fresh-run acceptance.

**Final assessment:** intent and shape match; user value is supported by the retained failure; commitments are honored; conceptual debt is neutral; existing monitoring partially subsumes the supporting machinery. Confidence is high in the static scope/correctness assessment and unmeasured in runtime sufficiency. Reassess an ENUM design only if the first diagnostic fails and its precise encoding/refusal behavior is supplied.

## Implementation follow-up after the diagnostic

**Verdict: APPROVE; no blockers.** This bounded static follow-up uses the semi-formal code-review skill. It reviews only the adopted oracle setting, its environment record, protocol appendix, diagnostic receipt and status interpretation. The earlier unmeasured-runtime conclusion is superseded for this retained-output diagnostic only. The reviewer ran no tests, builds, benchmarks or payload scans.

| Changed or critical function | Trace and verified behavior |
| --- | --- |
| `Oracle.__init__` (`oracle.py:109-122`) | `main` constructs it before `run_checks` (`:349-354`). The only diff is `threads=12` → `threads=4`; memory, spill limit, scratch location and all comparison/check code remain unchanged. A file comparison found the adopted oracle identical to the successful diagnostic's captured oracle. |
| `environment` (`run.py:216-237`) | Adds `oracle_threads: 4` beside the existing memory setting. `execute` writes this returned record before collection (`:337-346`); it reports configuration without modifying execution. |
| `freeze_execution` / `execute` / `run_one` | Capture retains the revised oracle and protocol (`run.py:187-194`); fresh runs execute that captured oracle (`:348-350`, `:286-289`). Engine `LIMITS` remains twelve threads (`workload.py:38-39`). The configured value and recorded value currently agree. |

The retained receipt reports all existing checks complete, including the formerly failing join, both contributor relations and both flat outputs (`oracle-threads4-20261010.json:61-169`). It records exit zero, no stop reason, 164.191 seconds, sampled RSS 23,345,152,000 bytes and sampled scratch 32,681,856,400 bytes (`:276-311`), consistent with the documented 21.74 GiB and 30.44 GiB. The receipt and `protocol.md:209-224` keep acceptance false and the original failed observation intact. `docs/STATUS.md:113-121` correctly distinguishes retained-pin equality from historical catalog selection and pending fresh acceptance. No isolated speedup or continuous resource-bound claim is made.

Static test inspection found constructor/custody coverage in `test_workload.py:70-117`, equality refusal cases for duplicate, null/empty and nested-order changes in `:137-161`, and combined scratch accounting in `:163-172`. These paths retain their assertions under the setting-only patch. **OBSERVATION; test coverage:** no synthetic test directly asserts `environment.oracle_threads`; direct source comparison establishes agreement for this bounded constant change, and the full diagnostic exercises the actual constructor. No new test is required merely to repeat the literal. Root-owned synthetic checks and repository gate are still pending in this review; their results are not claimed here.

The planned 12 GiB retained allowance exceeds the prior measured 10,586,909,684-byte run (`oracle-threads4-20261010.json:35-45`) and remains an estimate. Existing preflight includes every planned run, scratch and reserve (`run.py:341-346`); it must use current free space. Reusing the captured Rust executable with its matching build receipt is consistent with this Python-only correction; the capture path already preserves both (`run.py:187-197`). Full acceptance still requires all three fresh runs and checks within the original complete-path bounds.

Confidence is **HIGH** for static scope and interpretation; changed-path coverage is **ADEQUATE** for this setting-only adoption. Resource repeatability and complete acceptance remain for root execution.
