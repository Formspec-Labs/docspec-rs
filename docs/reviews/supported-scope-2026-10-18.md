# Supported scope and adoption evidence

The supported v1 implementation is complete as scoped in [STATUS](../STATUS.md).
This reconciliation corrects stale README limitations; it adds no runtime,
provider, performance or deployment qualification. Historical refusals and
checkpoint limits remain retained with their later resolution.

## Revision pins and late amendments

The reviewed DocSpec baseline is `91eb44f520221ec91897403e930d23f491b5ffd0`.
Its `src/`, `tests/`, `Cargo.toml` and `Cargo.lock` are unchanged from the final
implementation repair, `9d01704983a1400ef80c40ae3341ee60a61af291`. The later
benchmark and cache-pruning commits introduce no amendments in those paths.

| Amendment | Current code and retained proof | Limit |
| --- | --- | --- |
| `087e23cf36f40e0b9a76e129e9ea9d704904bf9e`: exact remote export ownership | [Owner lookup](../../src/engine/retention/owners.rs) accepts the exact completed dual-path reservation; [focused result](remote-export-owner-proof-2026-10-11.log) covers retirement, unrelated-path refusal and lookup after transport closes. | No arbitrary prefix ownership or payload reread. |
| `8316d77d12f3a79c4a3f4710b61256f65815f89f`: absent configured generation | [Namespace adapter](../../src/storage/remote/namespaces.rs) confirms absence only after exceptional root LIST; [focused result](remote-missing-generation-root-2026-10-11.log) covers exact absence, existing-root/error propagation and request scope. | Generic permission failures are not absence. |
| `9d01704983a1400ef80c40ae3341ee60a61af291`: portable namespace properties and restore safety | [Restore implementation](../../src/engine/restore/remote.rs), [namespace/intent/epoch results](remote-portable-namespace-properties-2026-10-11.log), and [format/Clippy/build result](remote-portable-properties-gate-2026-10-11.log). | Only the assigned namespace ID and one trailing location slash are normalized; intent and other properties remain exact. |
| Final provider continuation after those repairs | [Connected review](remote-maintenance-connected-review-2026-10-11.md), [closeout](remote-maintenance-closeout-2026-10-11.md) and [receipt](remote-maintenance-restore-provider-fixed-2026-10-11/receipt.json) retain exact source/export reads, interrupted restore and stale native-commit refusal. Restore target: `f1994a3b-321f-43a6-8ce3-5e102c5b5189`. | Quiescent independent-source catalog recovery with retained objects; no object-loss backup, online failover or remote computed/related processing. |

This is a static commit-to-evidence reconciliation. The connected review records
frozen patch digests, while the linked provider and consumer receipts do not
uniformly record source Git heads. They must not be relabeled as full-suite runs
at the reviewed baseline. No uncovered DocSpec implementation amendment was
identified in this bounded trace; the missing uniform run-to-commit binding
remains an evidence limitation, not a reason to repeat the Comments workload.

## Established consumer route

The smallest established consumer is the existing opt-in **SpicyRegs MCP local
reader of canonical primitive-column exports**. Its adapter was introduced at
SpicyRegs `5d3ea092b444a812c3e6c4451a2eabcd7bb585a8`.
`src/spicy_regs/docspec_exports.py` and `tests/test_docspec_exports.py` are unchanged
at the inspected consumer head `625a3cc830e8f4da0c7281c25e39472b83a59de7`.
That comparison does not qualify later application changes as a whole.

The current merged SpicyRegs head is
`ad996130b16123f5250ae13f0296183f4ca8f9cf`. Since the original adapter, it adds
request cancellation/deadline checks and lazy admission cache tests. The existing
`tests/test_docspec_exports.py` covers cancellation killing and reaping the hash
process, separate discovery/query admission, unchanged cache reuse, and refusal
of changed bytes without another metadata lookup or hash. These are application
boundary changes; the historical native consumer receipt does not certify the
current application as a whole.

The existing application boundary module passed its focused tests on that merged
head during this reconciliation. This checks the changed Python boundary; it
does not rerun or replace the historical native producer scenarios.

The [admission review](mcp-admission-review-2026-10-11.md) and
[native consumer result](mcp-admission-native-2026-10-11.log) cover exact values and
BIGINT/text schemas, duplicate/null/empty values, correction, deletion,
evidence-only releases and an old reader retaining its original export. The
current [fixture](../../tests/exports/mcp.rs) remains registered through
[`tests/integration.rs`](../../tests/integration.rs) and `exports.rs`; it is
explicitly ignored unless the sibling application and Python runtime are supplied.
Canonical metadata and one native hash bind the export to its exact source
release. MCP exposes that release reference; it does not claim row-level receipt
mapping or replace SpicyRegs production lifecycle ownership.

The separate [resumed-transfer receipt](r2-export-mcp-2026-10-11/receipt.json)
pins source version `update-two` and manifest digest
`7942b583c780e2b9e307d63e14d7f0b688a620ed97a2793eef749b4c78893492`.
It covers one retained 824-byte export with one HEAD, two conditional GETs and one
admission hash. Its timing excludes the prior update/export and cannot establish
remote throughput.

## Remaining adoption decision and minimum follow-up

The retained evidence establishes this reader route, not a selected production
dataset, packaged release/version, deployment destination or operating owner.
Choose those explicitly before adoption. A production lifecycle replacement also
needs an explicit decision about who owns writes, receipt association and final
selection; the MCP read proof alone cannot resolve that decision.

No runtime check is needed for this documentation correction. If a chosen
consumer profile exposes an uncovered behavior, run only its relevant registered
case and the existing
`exports::mcp::actual_spicy_regs_mcp_admits_exact_exports_and_release_evidence`
case against exact producer/consumer revisions and the configured existing runtime.
That case already performs read/correction/delete/evidence-only transitions and
preserves an old pin. Extend it only for a concrete missing boundary; do not
restart full Comments or generic provider qualification for handoff. Operational
holds in the active work ticket still apply.
