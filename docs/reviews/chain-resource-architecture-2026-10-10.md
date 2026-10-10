# Forward changes and shared discovery: resource architecture review

**Decision: reuse the existing comparison normalization, and add one bounded local change resolver shared by comparison and refresh. Do not enable a chain made from an unrestricted union of per-step readers, and do not derive affected keys by comparing only key values.** The existing implementation is a sound foundation; its normal public path is still unchanged/full comparison. These are design constraints for the next increment, not claims that a disabled chain already has these defects.

Static review of `2a5ae513cb0aa7c40f771a534474956ca92364c8`, on 2026-10-10. Sources: repository instructions, `docs/PLAN.md`, R4/R5/R7, current implementation, and locally installed pinned crate source. No code edits, builds, tests, formatting, benchmarks, cleanup, or other reviewers' reports were used. The only authored file is this review. Current source and retained test assertions establish shape, not newly measured performance.

## 1. Artifact summary

The decision is the next R4 local certificate/forward-chain comparison path and its use by R5 affected-key discovery. R4 requires exact before/after members without repeated full comparisons; R5 requires complete affected joins and summaries while inputs advance. Both require shared exact-input admission and maintained decoding, and both permit a conservative affected scope. This is a product capability supported by local proof infrastructure, not a new event system (`docs/requirements/04-change-tracking.md`, “Design”; `docs/requirements/05-dependent-refresh.md`, “Design”).

Current shape:

- `comparison_plan` resolves endpoint admission, schemas, identities and metadata effects, then provides a signed endpoint relation. Counts and streams consume that relation (`src/engine/comparison.rs:174-366`).
- `change_members` cancels exact occurrences first, then optionally cancels equal values with maintained aggregation and ranking (`src/engine/comparison.rs:440-504`).
- Certificates bind a qualified local result and descriptor at the same native commit revision; their optional retention is separate from complete endpoint state (`src/engine/changes/certificates.rs:72-138`; `src/engine/retention/inventory.rs:993-1008`).
- Joins and summaries currently derive affected keys from old/new physical-file differences, then recompute complete affected groups (`src/engine/incremental.rs:135-228,318-457`; `src/engine/aggregation.rs:201-252`).

The intended change should replace the source of signed members or conservative affected keys. It should not replace the full comparison oracle, native reader, lifecycle authority, or relational execution engine.

## 2. Lineage and relationships

| Prior artifact | Relation | Consequence |
| --- | --- | --- |
| `docs/PLAN.md`, “Architecture”, “Performance and user-value rules” | Parent decision | One embedded engine; Iceberg/Arrow/DataFusion; retain useful work; no separate SQL/event authority. |
| R4, “Design” and T03–T06 | Governing requirement | Exact local trust, forward continuity, correct fallback, independent optional-history lifetime, bounded composition. |
| R5, “Design”, “Shared promotion and recovery” | Consumer requirement | Union old/new affected keys, preserve evidence effects and complete groups, share exact-input admission. |
| R7, Q7 and “Performance acceptance” | Qualification requirement | Independently vary file/history/evidence growth; measure complete cost, cold/warm behavior, spill and retained storage. |
| `docs/STATUS.md:22-111,113-169` | Implemented boundary | Normalization and local certificates exist; enabled composition and population-scale resource qualification remain open. |

| Component / seam | Owner | Reuse and boundary |
| --- | --- | --- |
| Exact comparison plan | Engine comparison | Owns endpoints, visible fields, identity, metadata effects, selected path and fallback reason. |
| Local certificate and succession resolution | Engine changes/history + lifecycle | Supplies trusted immutable references from one captured catalog state; ordering evidence is not a change payload. |
| Selected native reads | Storage/query adapters | Owns native schema interpretation, occurrence addresses, selected columns and reader buffer admission. |
| Signed normalization / key grouping | DataFusion | Owns aggregation, sort, ranking and their maintained spill behavior. |
| R5 processing | Refresh/incremental/aggregation | Owns dependency interpretation and complete affected-group recomputation. |
| Active reader / retained consumer protection | Worker/retention | A live reader uses existing admission; a durable consumer needs its own retained promise. |

These seams are named in R4/R5 and the plan. The missing implementation is a small admitted change-source helper, not an unnamed new subsystem.

## 3. Invariants and commitments

| Invariant | Status and evidence | Failure if violated |
| --- | --- | --- |
| A returned address belongs to the requested endpoint under the selected identity. | RELIED-UPON. `ChangeDescriptor` states this at `src/model.rs:745-753`; occurrence cancellation implements it at `src/engine/comparison.rs:450-463`. | An intermediate birth can appear as a final changed member. |
| Selected local production establishes certificate trust; exported shape alone does not. | PRESERVED by current checks, REQUIRED for chain admission. Certificate/result revisions and exact manifests match at `src/engine/changes/certificates.rs:72-114`. | Fabricated or rebound deltas inherit local authority. |
| Ordering, optional history, and endpoint availability have independent meanings. | PRESERVED by `src/engine/history.rs:95-129` and `src/engine/retention/inventory.rs:993-999`. | History retirement breaks normal reads, or endpoint retirement unnecessarily destroys useful acceleration. |
| A small change cannot omit affected groups. | REQUIRED. R5 “Candidate discovery”; old/new scope is used at `src/engine/incremental.rs:186-228`. | A same-key content correction silently leaves a join or summary stale. |
| Metadata and reader costs remain admitted until their last use. | REQUIRED. Existing stream custody: `src/engine/worker.rs:175-197`; metadata and output lifetime: `src/engine/comparison.rs:205-206,409-436`. | Chains retain metadata/readers after their allowances have been released, or accumulate one buffer set per step. |
| Missing optional evidence permits slower correct work; malformed present evidence refuses. | REQUIRED by R4 “Design” and R7's corruption case. Current certificate decoding already distinguishes missing/retired from invalid (`src/engine/changes/certificates.rs:269-288`). | Corruption is presented as a harmless empty result or unsupported path. |

## 4. User-value and cost analysis

The useful outcome is fewer decoded rows and fewer spurious affected groups for a small accepted update. The smaller design is to reuse local certificates and native delta files, normalize only where the consumer needs exact net differences, and retain the existing full path. A new global key index, event store, generic change framework, persistent composed-delta table, or configurable planner is not justified by the evidence inspected.

Let `H` be retained lifecycle history, `L` traversed links, `D` descriptor/pin bytes, `F` referenced native files, `C` applied rows across the chain, and `N` endpoint rows. These dimensions matter independently:

| Work | Verified current cost shape / implication |
| --- | --- |
| Cold catalog access | `Lifecycle::rows` streams the entire lifecycle table to construct current records. The cache stores one immutable snapshot, not one per dataset (`src/catalog.rs:33,203-280`). Cold comparison is not simply O(changes). |
| Warm catalog access | An exact metadata-location match returns the retained `Arc<SnapshotRows>`. Keep that Arc for traversal; changing datasets can replace the global cache (`src/catalog.rs:203-225`; `src/engine/history.rs:56-58`). |
| Succession | Existing proof traverses from the observed head to the endpoints, including lag after the requested target. It bounds serialized bytes and steps, and decodes identity-only result fields (`src/engine/history.rs:65-138,249-364`). Account for this separately from the between-endpoint chain. |
| Chain metadata | At least the selected certificates, descriptors, schema/pin details and plan nodes must fit a cumulative allowance. A per-object allowance does not bound `L × D`. `inputs::read_manifest` already accepts cumulative byte accounting (`src/engine/inputs.rs:399-407`). |
| Native reading | `OccurrenceScan` captures task/file details with an explicit limit; each active occurrence stream reserves two batch allowances (`src/storage/occurrences.rs:139-170,296-338,383-399`). Native inventory also visits retained snapshot metadata, not just current row payload (`src/storage.rs:519-569`). |
| Full fallback | Reads both endpoints and aggregates them. Value streams additionally rank surviving occurrence differences (`src/engine/comparison.rs:334-359,440-504`). A failed late chain attempt followed by full fallback adds chain work to endpoint work. |
| R5 fallback | Captures old/new inventories, identifies changed files, then currently reads complete batches before extracting keys (`src/engine/incremental.rs:365-449`). Narrow selected-column reading is already available. |
| Disk | Added delta files are shared with the new source; removed rows are separately retained (`src/engine/write.rs:598-660`). Rewriting all deltas into another wide native table adds output, metadata, digest work and recovery/cleanup cost. Scratch limits bound DataFusion temporary files; ordinary retained native tables are a separate cost (`src/budget.rs:85-95`). |

No speedup, peak-memory result, or storage-reduction measurement follows from this static analysis. The falsifier is straightforward: at a fixed small update size, the new path adds catalog/descriptor/staging work but fails to reduce complete comparison or refresh cost under the supported cold and warm conditions.

## 5. Counterfactual analysis

**Keep full comparison only.** Correctness and optional-history retirement remain simpler, but each changed endpoint comparison still reads O(N) rows. R5 retains its conservative file-based scope. This is the valid baseline and fallback, not a failed state.

**Compose wide changes into a new retained table first.** Execution becomes easy to feed to DataFusion, but every request writes and reopens C rows, adds catalog/native metadata, and must define the new table's lifetime. This loses the main reuse benefit unless a measured repeated consumer or recovery boundary requires that materialization.

**Use only projected key equality.** It is cheaper but wrong: replacing `(docket=A, amount=1)` with `(docket=A, amount=2)` yields no value difference after projection to `docket`, although summary A must change. Receipt/contributor identity may also require work when visible values match.

**Remove exact-occurrence cancellation before value ranking.** Consider `old@initial → new@zz-intermediate → new@a-final`, alongside a kept equal `new` member. Ranking raw signed rows can retain `zz-intermediate`. The existing independent fixture explicitly exercises this case (`src/engine/comparison.rs:612-645`). Keep the occurrence-first stage.

**Six-month critic:** a warm short-chain demo would conceal cold lifecycle replay, unbounded union readers, hot duplicate spill and high-churn disk costs. Retaining separate measurements and an explicit path/reason prevents that misleading claim without building another reporting framework.

## 6. Findings and recommendations

### F1 — Bound active chain readers independently from compute threads

**Severity: BLOCKER for an unrestricted union design. Category: unstated-assumption. Recommendation: RESHAPE source execution.**

In pinned DataFusion 53.1.0, `UnionExec` sums its children's partition counts; `CoalescePartitionsExec` creates a task for every input partition. A configured target partition count is not a global cap on source readers. Each DocSpec reader retains its own native metadata and batch allowance. A long chain can therefore exhaust admission with tiny deltas before its relational work needs significant memory.

Primary evidence: `$CARGO_HOME/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-53.1.0/src/union.rs:168-188,284-313`; `.../coalesce_partitions.rs:183-223`; `src/budget.rs:110-112`; `src/storage/occurrences.rs:139-170`.

Feed a bounded number of lazily opened delta streams into the existing relational normalization. A sequential source is the smallest starting point and matches existing `query::files` and replacement execution (`src/query.rs:365-380`; `src/engine/write.rs:569-572`). Keep DataFusion responsible for relational logic. Do not add concurrency tuning without evidence that the bounded source is the bottleneck. Test the resulting physical plan and peak live reader count at the declared chain bound.

Maintained alternatives considered: use `futures::stream::iter(...).then(open).try_flatten()` to open the next native relation only after the prior stream finishes, and expose that source through the existing `PartitionStream`/`DataSourceExec` adapter (`src/query.rs:157-174,312-400`). `query::files` already handles sequential files within one captured table; it supplies the local pattern, while a chain needs the small extra source-list and signed-column adaptation. DataFusion `union` remains appropriate for the bounded endpoint pair, but it supplies relational union rather than a global reader cap. A new execution engine, custom aggregation/window implementation, eager Arrow `MemTable`, or persistent combined-delta table adds no demonstrated value here.

### F2 — Share trusted discovery, while preserving R5's conservative key semantics

**Severity: BLOCKER if key-only value comparison is used. Category: commitment-violation. Recommendation: RESHAPE the shared interface.**

R4 needs normalized net endpoint members. R5 needs a complete candidate scope and may accept a superset. Sharing the admitted relation and native decoding satisfies the requirement; forcing both through key-only net value comparison does not.

For the first supported chain discovery, select old/new key columns directly from qualified source deltas and deduplicate their union. A reversion may cause extra work, which R5 explicitly permits. If exact cancellation becomes worthwhile, cancel with the needed occurrence/dependency identity before discarding those columns. Keep metadata/evidence-only effects separate; an empty value relation does not prove unchanged policy or coverage (`src/engine/comparison.rs:521-561`; R5 “Design”). A receipt-only hop with no source delta is empty source work only when its source identity continuity is established.

For existing changed-file fallback, pass its already resolved key indices into `read_projected_file` instead of reading full batches and selecting afterward (`src/engine/incremental.rs:329-332,435-445`; `src/storage/occurrences.rs:362-385`). This reuses a maintained reader path and removes unnecessary wide-column decoding without a new index.

### F3 — Traverse one captured catalog state and keep cumulative metadata admission

**Severity: CONCERN. Category: debt-accretion. Recommendation: RESHAPE helper reuse.**

Do not call public `change_certificate()` for each hop. That method starts another admitted operation, reopens the catalog, checks result availability and allocates descriptor admission; it returns inspection metadata with no lasting promise (`src/engine/changes/certificates.rs:233-295`). Likewise, do not prove succession separately for each edge or construct a full map through `Lifecycle::records`.

Use an internal resolver over the already admitted `View` and retained `Arc<SnapshotRows>`. Reuse the certificate binding checks and byte-checked descriptor reader. Count cumulative descriptors, result identities, retained table pins and plan allocation before retaining them. Keep the allowance with the resulting plan/stream; drop optional chain state before allocating a full fallback. This follows the existing history traversal, which already retains the exact rows across yields (`src/engine/history.rs:54-58`).

Intermediate complete result availability is not a prerequisite for a retained delta: optional history retains interpretation metadata and its own rows while endpoint snapshots/manifests have independent promises (`src/engine/retention/inventory.rs:993-999`). Continue requiring both requested endpoints to be readable; validate intermediate certificate/result receipts from catalog state without opening every intermediate complete release.

### F4 — Select fallback before opening payloads; preserve errors after execution starts

**Severity: CONCERN. Category: intent-vs-shape. Recommendation: KEEP explicit path selection and strengthen its boundary.**

Resolve unsupported group/schema/order, missing or retired optional history, and the declared chain metadata bound before reading delta payloads. Missing optional history can select full comparison or R5's qualified file discovery. Invalid active certificates, changed descriptor bytes and failed active delta reads must refuse, not quietly become fallback or empty changes. Once a stream starts, an error remains an error; completeness requires successful EOF (`src/model.rs:745-753`; `src/engine/comparison.rs:140-150`).

A local read stream can reuse current maintenance admission until EOF/drop (`src/engine/worker.rs:175-197`). A persisted refresh phase that depends on optional evidence must acquire the existing durable consumer protection before it becomes a recoverable dependency; inspection alone does not do that (`src/model.rs:1117-1126`; R4-T03/T05). Do not introduce a separate lease system.

For high churn, the sum of existing delta row/byte metadata can be compared with endpoint metadata to avoid obviously larger work without payload probes. An elaborate cost model is deferred until measurements show a need. Resource refusal and unsupported acceleration must remain distinguishable from corrupt evidence.

### F5 — Qualify a bounded path before enablement, and retain wider performance work as unfinished

**Severity: CONCERN. Category: user-value. Recommendation: KEEP existing oracles; add focused qualification.**

The normalization fixture proves algebra over supplied signed rows. It does not prove certificate traversal, retained protection, native decoding or cold resource behavior (`src/engine/comparison.rs:612-645`; `docs/STATUS.md:62-68`). Before enabling the local source-chain path, the minimum meaningful evidence is:

1. Compare native certified chains with forced-full comparison and independently specified value/address bags: insert, delete, replacement, duplicates/nulls, intermediate birth, reversion and compaction. Check both identity modes and count/stream agreement. Include same-key changed-content and old-to-new-key moves through join and summary discovery, not just comparison output.
2. Exercise missing/retired history fallback, malformed/rebound active certificates, chain discontinuity, unsupported schema/group/reverse cases, retired intermediate state with retained optional deltas, and a later delta read failure. Verify the chosen path/reason and that no partial result becomes an empty success.
3. At the declared supported chain/metadata bound, include wide rows and hot duplicates; observe the active-reader bound, memory/spill refusal, cancellation/EOF/drop release and maintenance protection. Small batches alone do not establish a process-memory bound.
4. Freeze a bounded comparison against full/file discovery under cold and warm catalog conditions. Hold changed rows fixed while separately varying chain length and file count, plus a high-churn control. Count necessary descriptor/native/payload passes and measure elapsed time, whole-process memory, peak additional disk and retained bytes. Reuse existing instrumentation; do not add reporting scans. Include the retained write overhead already required to produce the certificates.

Run targeted checks and the repository gate for the implementation; do not repeat unaffected large qualification runs merely because the helper changed. These focused results permit claims only for their supported bounds. R4-T06/R7 Q7 and complete consumer performance remain open until their wider measurements pass. No new benchmark execution was performed for this review.

## 7. Verdict

**RECONSIDER the path wiring; KEEP the certificate model, existing signed normalization and native reader.** Implement one captured-state resolver and bounded delta source, let R4 normalize exact differences, and let R5 derive conservative old/new keys without key-only value cancellation. Preserve explicit full fallback, active-stream custody and optional-history independence.

- Intent versus shape: matches when the source is bounded and admission is shared; diverges with per-hop public calls, unrestricted reader unions or staged wide copies.
- User-value claim: supported as fewer reads and narrower affected scope to be measured; no current speedup claim.
- Commitments: honored by the recommended shape.
- Conceptual debt: pays down duplicated discovery/admission; introduces only a concrete shared source helper.
- Sibling subsumption: substantial reuse of comparison, history, certificate checks, native projection and worker admission.
- Confidence: high in the source-level recommendations; runtime costs and provider guarantees remain unmeasured here.
