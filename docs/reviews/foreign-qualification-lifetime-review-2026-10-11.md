# Foreign qualification witness lifetime — static review

**Verdict: APPROVE after the exact-refusal assertion is tightened.** No production behavior changes; no other actionable finding found. Root accepted the assertion correction and owns runtime verification. Reviewed patch SHA256 `8766d40b57dc1431571ff3290a94ae6463ad7aca1fbee2762dd2e5efcdc8c499`, proposed source `/tmp/docspec-foreign-qualification-lifetime/new/tests/qualification_roots.rs`.

## Finding

**F1 — test coverage, minor:** proposed `tests/qualification_roots.rs:571–572` accepts every error from live owner-grant retirement. Require `unwrap_err().code() == "grant_in_use"`. That exact refusal is emitted for a still-active consumer binding at `src/engine/retention/grants.rs:125–163`; malformed records, unsupported bindings and metadata capacity have other errors and must not count as proving consumer preservation. Root agreed; final assertion verification remains pending.

## Short certificate

| Trace | Evidence | Conclusion |
| --- | --- | --- |
| Existing real qualification setup | proposed test:301–430 | Uses public Build, interrupted backfill, dependency correction and retained readback. Exact candidate closure comes from actual origins/completions; no synthetic lifecycle replacement in this new scenario. |
| Foreign admission | proposed test:432–454; `admission.rs:283–305,330–335` | The witness retains its original owner and identity. Normal managed-input qualification/protection creates the observed owner grant. Actual null/8 values are checked. |
| End owner recovery promises | proposed test:457–567 | Abandons unselected derived operations, selects a genuinely new independent Build, then separately retires old availability, execution and operation recovery. Qualification or the exact foreign witness grant remains a blocker. Qualification retirement preserves its original record for reconciliation. |
| Preserve live consumer | proposed test:568–577; `grants.rs:103–166` | Owner proof retirement alone cannot retire a grant with active consumer bindings. Exact file existence, protection, current consumer values and independent deep verification remain checked. F1 must identify that intended refusal. |
| End final foreign promise | proposed test:578–616 | New independent consumer has no parent/computed ancestry or old witness. Old consumer availability, its exact operation's phases and terminal recovery retire independently. Grant remains a blocker until explicit exact grant retirement. |
| Reclaim and reopen | proposed test:618–698 | Ownership-qualified removal must report zero protected bytes, unlink every captured exact object, reconcile saved results after reopen, and preserve both newly selected datasets and literal values. Cold consumer deep verification checks it no longer relies on removed witness bytes. |

The existing no-foreign fixture still calls the same helper with the added branch disabled. The ordinary integration module remains registered at `tests/integration.rs:45`. This scope is one foreign payload witness originally created by qualification-associated scalar work. It does not establish arbitrary foreign qualification-metadata adoption, ending a selected computed descendant's mandatory history, provider deletion, concurrency, or performance.

Static analysis only: no Cargo, tests, formatting, provider calls, or shared-source edits by this reviewer. Confidence medium until the parent runs the final assertion version.

## Runtime follow-up and final correction

**Final static verdict: APPROVE** for `/tmp/docspec-foreign-qualification-blocker-fix.patch`, SHA256 `2b77fe7bc67acb263eba914a3017313e63182d3f2e762c15e7d130fdb1ff6ade`. No actionable finding remains. Root owns execution of this corrected fixture.

The initial runtime failed at the asserted blocker label, not at loss of protection: `docs/reviews/foreign-qualification-lifetime-focused-2026-10-11.log` reports the failed foreign fixture while the existing qualification cases passed. The retained diagnostic `docs/reviews/foreign-qualification-blocker-diagnostic-2026-10-11.log:38–140` shows all candidate bytes protected, with only the exact foreign witness attributed to `d_foreign_consumer_7d5e8553e5f8.lifecycle/authority` revision 8; the other objects carry the exact qualification key. The original static certificate missed this valid first-witness ordering. It did not establish a production defect.

The production trace supports that diagnostic: `inventory.rs:192–196` sorts and pops namespaces, `:771–778` assigns only the first blocker, `:1507–1514` validates authority against its immutable selected result before expanding it, and `:1073–1075` directly retains exact dataset input objects. A live consumer authority can therefore precede the owner-side grant without changing grant eligibility or ownership.

The correction at `tests/qualification_roots.rs:560–576` permits the exact fixed consumer table and `authority` only for the exact witness already asserted in that consumer's inputs. Other candidates must still be retained by the qualification key. `:598–613` now requires the intended `grant_in_use` refusal, closing F1. The later exact grant-only blocker, explicit grant retirement, ownership-qualified removal, literal values and cold readback checks remain unchanged. The fixed run log was still in progress when reviewed; no runtime-pass claim is made here.

## Parent runtime closure

The corrected focused qualification-roots suite passes in `foreign-qualification-lifetime-fixed-2026-10-11.log`. Initial failure and the exact blocker diagnostic remain in the adjacent focused/diagnostic logs and resource receipts. The diagnostic identifies the selected foreign authority as the witness root; it did not show a production retention defect.
