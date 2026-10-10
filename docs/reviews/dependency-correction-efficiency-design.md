# Dependency correction: efficiency and shared-design review

**Decision: keep Q4; add scoped correction and qualification to the existing phase and selection services.** A dataset-wide invalidation is a useful conservative fallback, but it cannot by itself deliver Q4's promise that unaffected work remains reusable. Do not introduce another cache, job journal, SQL database, or dependency-graph service.

Static review on 2026-10-10 of `582ea804d0be273037092459ad72b2ab69453532`. Scope: Q4, R3, shared reuse, admission, backfill, selection, retention, and metadata cost. No Rust edits, tests, Cargo commands, builds, benchmarks, or payload scans were performed. Existing untracked acceptance/review artifacts were left alone. This is an independent design review, not implementation qualification.

## Findings first

### F1 — A broad eligibility counter cannot be the complete Q4 mechanism

**BLOCKER to claiming Q4; intent-versus-shape; RESHAPE.** `invalidate_admitted` records a reason and increments dataset-wide `Authority.eligibility` (`src/engine.rs:695–742`). That counter participates in source phase keys (`src/engine.rs:1958–2003`), scalar file keys (`src/engine/backfill.rs:1062–1067`), prepared/validation keys (`src/engine/backfill/preparation.rs:32–40,216–219`), backfill source admission (`src/engine/backfill.rs:518–525`), and producer input qualification (`src/engine/inputs.rs:260–278`). It safely makes an old admission stale but also prevents reuse of unrelated work. A reason string records neither the affected declaration nor the missing material dependency.

Keep the broad operation for explicit broad invalidation. Record a Q4 correction against an exact material declaration and phase/group scope, without advancing the broad counter for an unrelated source or group. Use the current catalog boundary to order that correction against new selection. The requirement explicitly demands both stale-adoption refusal and unaffected reuse (`docs/requirements/07-operations-qualification.md:109–115`; `docs/requirements/03-schema-backfill.md:15–19`).

### F2 — Supplemental evidence must create an association, not rewrite execution history

**BLOCKER to claiming Q4; named seam; RESHAPE.** `ComputedGroup` records the adoption request, source metadata, base manifest, and exact expression input IDs (`src/model.rs:828–839`). File completions retain source/output identity and optional source interpretation metadata, but no explicit original material-declaration record or supplemental qualification (`src/engine/backfill.rs:37–47`). Current `input_fields` is an exact SQL binding: the compiler refuses additions as well as omissions (`src/engine/backfill/expressions.rs:80–95,133–157`). Replacing that map with a corrected declaration would erase what the original run declared and can break its verifier.

Retain the original generation and declaration. A new, immutable qualification binds that generation to a target input and the corrected requirements, with the evidence and verifier that justify adoption. Do not add a guessed historical language value, turn a missing field into null, or relabel old output as a new execution. Newly computed output must use a processor/definition that actually consumes the corrected inputs; merely changing a cache digest is insufficient. These are Q4 and R3 obligations, not an additional workflow (`docs/requirements/07-operations-qualification.md:115`; `docs/requirements/03-schema-backfill.md:17,31–33`).

### F3 — Checking reuse lookup alone leaves bypasses

**BLOCKER to claiming Q4; consistency; RESHAPE.** Expensive completion acknowledgment intentionally survives a change in eligibility (`src/engine.rs:1793–1808,2040–2047`). Final selection supplies the guard: ordinary operations check fresh authority at `src/engine.rs:1745–1770`; backfills do so at `src/engine/backfill.rs:829–854`; refreshes guard inputs and ancestors at `src/engine/refresh.rs:226–329`. Cached readback and evidence-only inheritance can avoid file lookup (`src/engine/backfill.rs:599–624`; `src/engine.rs:1495–1503`). A correction check only inside `file_key` or `prepare_group` misses those paths.

Use one shared metadata evaluator before reusable work is accepted and again at the final guarded selection. It must cover every component of the assembled result, including inherited groups, and every producer dependency whose material data was consumed. An unaffected group's computation may remain reusable while selection of a combined result still waits for an affected sibling to be qualified. Reuse of useful work is not permission to select a partly invalid combination.

### F4 — A correction must not make routine operations walk more historical payload

**CONCERN; debt and resource use; RESHAPE.** The existing lifecycle cold path reads retained lifecycle rows; its warm path uses one exact-snapshot cache (`src/catalog.rs:202–252`). Each commit clones current cached values and charges appended history (`src/catalog.rs:344–396`). Scalar history verification also walks owning manifests, with a request-local bounded cache (`src/engine/backfill/verification.rs:332–478`). Adding a per-row correction ledger or repeatedly expanding a correction chain multiplies costs the current status already leaves unqualified (`docs/STATUS.md:1273`).

Store one bounded correction per decision, a directly addressable current requirement state per affected declaration, and one reusable qualification per exact generation/target/scope/checker combination. Large occurrence correspondence belongs in a native relation referenced by that qualification. Resolve immutable evidence once during qualification; use retained completion and small current-state checks on retries and selection. Measure cold history, warm lookup, commit, retention, and explanation separately under Q7 before claiming scale (`docs/requirements/07-operations-qualification.md:133–139`).

## Artifact summary and decision lineage

Q4 protects operators who discover a dependency omission after work has completed or reuse has begun. The outcome is safe new adoption, useful retained work, and an accurate explanation of what ran and what is now known. It is a product capability supported by proof metadata. Current status explicitly leaves dependency correction unfinished (`docs/STATUS.md:5–12,164–165,1250–1252`); this review does not reinterpret that omission as a previously qualified defect.

| Prior artifact | Relation | Evidence |
| --- | --- | --- |
| R3 material dependency and identity rules | Parent meaning: actual consumed pins, selective invalidation, explicit adoption | `docs/requirements/03-schema-backfill.md:13–19,31–47` |
| R1 phase completion and reuse | Shared owner; ordinary catalog results and retained protection, no second ledger | `docs/requirements/01-durable-storage.md:27–42` |
| Research UR-01/02/04/05 | Preserve completed work and share authoritative decoding | `docs/requirements/RESEARCH.md:24–33` |
| Accepted Rust plan | Single Iceberg lifecycle authority, small commit checks, no routine history revalidation | `docs/PLAN.md:11–33,44–75` |
| Original adversarial review | Q4's lineage, not a substitute for normative acceptance | `../DocSpec/docs/plans/native-dataset-lifecycle-2026-10-07/ADVERSARIAL-REVIEW.md:27–42` |
| Initial Rust foundation commit | Imported requirement baseline; file history shows no later revision of Q4/R3 at the reviewed head | `3bd6445`, `docs/requirements/07-operations-qualification.md`, `docs/requirements/03-schema-backfill.md` |
| Practitioner findings | Reuse exact selected inputs; avoid an unconditional new index or scan | `/Users/mikewolfd/Work/corpora/session-reports-2026-10-07/data-processing-interviews/FINDINGS.md:5–16,35–41` |

The Rust plan supersedes earlier storage-choice freedom with Iceberg-only v1. Nothing in Q4 requires reopening that decision (`docs/PLAN.md:3–5,11–17`). No performance conclusion is inferred from the practitioner incidents.

## Existing primitives and ownership

| Component / seam | Owner and existing behavior | Q4 use |
| --- | --- | --- |
| `Lifecycle::commit`, `View`, `SnapshotRows` | R1; native expected-state commit, bounded rows, keyed lookup and prefix access (`src/catalog.rs:47–105,284–425`) | Commit correction and its current requirement state together; refuse stale selection and reconcile exact correction IDs. |
| `PhaseResult`, `PhaseDependencies` | R1; original phase operation, immutable outputs, actual recovery inputs (`src/model.rs:475–505`) | Retain generation and qualifying work through the existing completion path. Recovery references and material adequacy remain distinct concepts. |
| `Expressions::compile` and scalar completions | R3; maintained DataFusion expression analysis, stable consumed field IDs, separate file/prepared/validation work (`src/engine/backfill/expressions.rs:28–157`; `src/engine/backfill/preparation.rs:10–277`) | Preserve existing analysis; attach an explicit material-declaration identity and correction decision without another scalar engine. |
| `PreparedAdmission` | R1; exact pins/protections captured before request commit; current boundary rechecked (`src/engine/admission.rs:11–65`; `src/engine/backfill.rs:534–589`) | Carry the observed correction state and required qualifications with admission; recheck after protection acquisition and before selection. |
| Shared selection and related-input guards | R1/R5; selection, succession, captured-input policy, transitive producer checks (`src/engine/history.rs:141–234`; `src/engine/refresh.rs:226–329`) | Add current correction eligibility in this shared decision path, preserving existing coverage and definition checks. |
| Completion protection and retention inventory | R6; immutable result plus owner binding; known roots expanded by shared collector (`src/engine.rs:96–109`; `src/engine/retention/inventory.rs:850–909`) | Protect qualification evidence using the same ownership mechanism, including foreign sources. |
| Retained-generation retirement | R6; active completion resolves exact revision and retirement (`src/engine/retention/retirement.rs:287–303`; `src/model.rs:538–544`) | Reference an exact generation; a later record under the same logical key must not silently replace it. |
| Independent verification | R3/R7; composition-only checks and explicit deep comparison (`src/engine/verification.rs:29–34,125–155`; `src/engine/backfill/verification.rs:482–645`) | Verify the new association separately from the original execution; keep payload qualification explicit and reusable. |

R1/R3 own the new material-adequacy rule together. R2/R5 consume it, R6 protects its evidence, and R7 tests it; this ownership is already assigned by `docs/requirements/RESEARCH.md:33`. The missing named seam is the distinction between an original execution declaration and a later qualification for another input. Define it in the shared model rather than hiding it in the backfill key builder.

## Smallest proposed shape

The following are design requirements, not claims that these records already exist.

1. **An immutable correction and current requirement state.** Give correction submission a stable ID, exact affected declaration identity, processor/definition and phase/group scope, missing dependency description, retained reason, and expected current requirement revision. Commit the decision and the new current state together in the owning dataset's lifecycle. An exact retry returns the existing decision; a reused ID with changed content refuses. A later correction cannot forget an earlier unsatisfied requirement. Use the existing dataset/catalog guard; no new authority epoch or external service is necessary.
2. **An immutable original execution reference.** Bind new completions to the original declaration, material processor identity, exact source interpretation/pins, output generation, and validation scope. Use existing operation, completion key/native revision, and object identities rather than creating a parallel generation counter. Original file completions can represent different executions inside one assembled group; preserve those origins rather than inventing one execution for the group. Legacy absence remains unknown. A native revision alone is insufficient unless its exact record remains resolvable; new associations should retain an immutable descriptor or the exact record content/digest needed by offline readers.
3. **A retained qualification for adoption.** Bind the original generation, target source/occurrence correspondence, current corrected requirements, evidence references, qualifying processor/checker and scope, and outcome. Successful supplemental qualification records that the output was adopted; it does not modify the original execution or declaration. A target may name this qualification from its existing group/phase binding. Keep a direct reference to the original generation so repeated adoptions do not require a growing chain of supplemental associations to find it.
4. **One eligibility evaluator used by existing services.** Given the candidate's material declarations, original generations, supplemental qualifications, and current correction state, return reusable, qualification required, recomputation required, or an explicit unknown/refusal reason. Share it between planning, phase/group reuse, fresh producer-input admission, and final selection. Do not make `active_completion` itself mean semantic approval: it currently establishes completion availability and retirement status, not material adequacy.

Keep `Authority.eligibility` for its existing broad invalidation semantics. Do not include the new correction revision in every unrelated value key. A changed current requirement can make an association stale without changing its value bytes. Preserve the common material-identity representation; R3 must not build a second cache-key scheme (`docs/requirements/03-schema-backfill.md:19`). Start with explicit scopes that supported processors can interpret; do not add arbitrary predicates, automatic dependency inference for opaque classifiers, or a plugin framework.

### What qualifies an adoption

| Situation | Required result |
| --- | --- |
| Correction does not affect the candidate's declaration/scope | Reuse the existing qualified completion and exact objects. |
| Retained evidence proves the originally consumed language and target language are equivalent under the corrected material rule, with exact occurrence correspondence | Record a supplemental qualification and adopt the existing value generation. |
| Historical input is unknown, unavailable, or cannot be tied to the actual execution | Preserve unknown. Revalidate through a sufficient supported check or recompute from qualified target inputs; otherwise refuse. |
| Known material input differs | Recompute affected values unless a separately justified equivalence or complete revalidation proves the old value valid. Equality of text, business key, row hash, sample, or row count alone is insufficient. |
| Only validation/policy adequacy changed | Retain value computation and redo the affected check with the required evidence. |
| Another correction is committed after qualification | Re-evaluate against the new requirement state before adoption; retain the now-incomplete qualification as history. |

An exact retained source can supply historical input only when it establishes the relevant execution binding. A current source value cannot fill that gap. For a native field, field ID, schema interpretation, missing/default/null semantics, and occurrence scope all matter. The existing scalar compiler already extracts referenced columns and observes `arrow_metadata`; Q4 therefore needs a fixture for a real omitted material dependency or an explicit retained legacy declaration, not a production change that permits incomplete SQL bindings (`src/engine/backfill/expressions.rs:80–157`).

### A concrete Q4 fixture through the production scalar path

Changing a physical language value is not enough to test Q4: the current `file_key` includes the entire immutable source `ObjectRef`, so that change already misses the cache (`src/engine/backfill.rs:1062–1067`). Use the following legal, same-file source evolution. This needs the correction/qualification capability described above, but no new computation plugin, general adoption operation, or relaxed SQL binding.

1. Build a source containing `text`, including a value such as `pain`. Through the existing schema service, add nullable `language` with a literal initial default of `en`. The existing files have no physical column for its newly allocated field ID. Retain the exact original schema, source file identities, and evidence establishing that absence.
2. Backfill an English-specific text classification, for example a rule that classifies `pain` as negative. Stop through `backfill_until(..., "files")`, leaving the original group **unselected** while retaining its acknowledged file completions. The expression reads only `text`, so the compiler's `input_fields` is complete. The omission is a semantic prerequisite: the English rule is applicable only when the source language is English, but that condition was not in its original material declaration. In French, the same word means bread, making the assumption consequential. Preserve the actual original expression and text-only declaration. A corrected English-only rule must later require proven English applicability; blindly rerunning the old expression on French input does not fix it.
3. Before admitting the operation that will be paused, use the public schema service to omit the old `language` field from the current selection and add a fresh `language` field with initial default `fr`. This is a new field with a new native ID, not a mutation of the old field's historical default. `schema::fields` selects existing roots by ID and independently allocates new IDs for additions (`src/engine/schema.rs:427–501`). Source Parquet files and the text field remain unchanged. Existing scalar preparation therefore has a real old file-completion hit: its key sees the same source object, text field interpretation, and group definition. The initial default changes the new source's logical language without changing those inputs.
4. Admit an ordinary `BackfillSpec` against this already selected target and its current writer definition. Pause immediately after the production resolver has chosen the old file completion. At this point, capture its original completion generation and original source interpretation. Selecting the original group before the schema change lets that change produce a later prepared completion; exercise that prepared path and retained readback in separate fixtures, not as proof of this file-cache hit.
5. Submit a scoped correction saying the English classifier's applicability also depends on source language. Resume the same operation without any additional source selection, global eligibility bump, or writer-definition change. The new correction check must refuse its proposed adoption: the original English field has been removed from the target meaning and the new field is not proven equivalent. A corrected recipe that consumes the new field can compute a new result. The refusal must identify the recorded correction, proving that a stale-source or definition fence did not accidentally make the test pass.

The fixture should first confirm that source object identities and old file-completion identity match, while an independent small read establishes the intended `en`/`fr` interpretations. Only the qualification fixture reads those small values; production can establish the supported correspondence through field IDs, retained schema/defaults, and admitted file metadata.

Run separate branches for the other Q4 outcomes:

- **Adequate supplement:** keep the original language field ID and interpretation (`en`) and make only an unrelated source-schema change. The same immutable file plus the same field interpretation proves language correspondence. Qualify English applicability using the retained literal default and evidence that this exact field is physically absent. Create a supplemental association, preserving the original output objects and original declaration. A per-file column-schema/footer check, if needed to establish absence, is bounded metadata work at qualification; it is not a value scan on every reuse.
- **Unknown historical input:** perform the original text-only classification before the source has any language field or retained language observation, then legally add `language = en` later. The old completion's retained schema proves no historical language is established. The current default must not be applied backward. Require supported revalidation or recomputation under an explicitly corrected recipe; otherwise refuse.
- **Unaffected work:** include an independent text-length group. The scoped correction does not affect its declaration, and its file completions remain reusable. A final combined selection must still account for the affected classification group.

The minimum production proof form can be deliberately conservative: exact immutable file/occurrence identity plus identical stable field-ID/type/read-default interpretation, or a retained literal/default whose physical absence is established. A changed field ID is a material correspondence failure unless an explicit qualified mapping exists; never infer that mapping from the reused name `language`. General equality across rewritten files, arbitrary evidence assertions, and row-level adoption are unnecessary for this fixture and should remain unsupported until there is a concrete need. The new semantic applicability declaration is separate from SQL's exact `input_fields`; that distinction is the small missing product capability, not an existing supported feature.

### Race and recovery sequence

Resolve a reusable candidate and capture current requirements. Retain any necessary new qualification outside the short selection lock. Before selection, obtain the existing output/source locks, open fresh lifecycle views, and evaluate all candidate components against current corrections. A changed requirement triggers re-evaluation, not a blind retry or automatic relabeling. The native guarded commit then orders selection against a correction on that same lifecycle.

For a producer correction, use the existing related-input lock set and fresh producer guards. The current implementation only qualifies local single-host execution; its status leaves remote multi-host use unqualified (`docs/STATUS.md:1269`). Do not infer that an output-table compare-and-swap atomically fences a concurrent correction in a different remote producer catalog. That remains part of provider qualification, not a reason to add a coordinator now.

Completion acknowledgment remains allowed after eligibility changes, with the original declaration and generation intact. Completed operation replay returns its historical result; a new operation or export making a new adoption must pass current eligibility. Existing historical lookup remains a pinned historical read, with explicit inspection able to report later corrections. Do not turn every ordinary read into a live dependency-graph validation (`src/engine.rs:474–500`; `docs/PLAN.md:44–64`).

### Concrete implementation boundaries

| Boundary | Required change and correctness obligation |
| --- | --- |
| Shared model and correction service | Add bounded correction/current-state and qualification types; establish immutable original execution references; stable request identity and conflict behavior. Preserve unknown legacy fields. |
| `engine.rs` phase lookup and selection | Call shared eligibility before adopting a completion and before selecting. Retain completion acknowledgment behavior. Cached readback must not skip the final check. |
| `backfill/preparation.rs`, `backfill.rs` | Apply the shared evaluator to file, prepared, and validation reuse. Reuse unaffected file outputs and qualified assembly. Final selection checks affected inherited siblings as well as the target group. |
| `backfill/verification.rs` | Verify original declaration and supplemental association as distinct facts. Reuse maintained field resolution and bounded manifest reader. Update rules that currently only recognize authored/inherited bindings; do not rewrite old manifests to fit them. |
| `inputs.rs`, `refresh.rs` | Fresh captured input and ancestor checks account for scoped corrections, preserving existing succession and coupled-source checks. Computed-input groups remain outside the currently supported related-input scope unless explicitly implemented (`docs/STATUS.md:1252`). |
| `export.rs` | A new distribution checks material eligibility at admission and final selection. Today export compares the authority captured at admission with current authority (`src/engine/export.rs:237–318`); that alone cannot detect a correction already present before admission. Preserve exact completed export replay. Scope to fields/inputs actually consumed where the supported export representation can prove that scope. |
| Retention, restore, format handling | Register new record families and their exact evidence roots in the shared inventory/owner logic. Protect evidence before retaining a qualification and propagate it into dependent results. An unresolved or unknown root must not authorize deletion. Do not treat a semantic correction as physical retirement. |
| CLI/HTTP/status | Thin routes over the same service and existing plan/inspect/resume surfaces. Show affected scope, original generation, recorded correction, reusable work, and required next action. No separate mandatory diagnostic workflow. |

## Invariants and commitments

| Invariant | Status in this proposal | Evidence and failure mode |
| --- | --- | --- |
| One catalog authority selects the exact result | Preserved | `docs/PLAN.md:23–33`; a sidecar correction store would create a second mutable decision boundary. |
| Original execution, generation, declaration remain immutable | Newly made explicit in the shared representation; required by Q4 | `docs/requirements/07-operations-qualification.md:115`; rewriting old bindings makes history claim work that did not occur. |
| Unknown historical inputs remain unknown | Relied upon | Q4 and `docs/requirements/03-schema-backfill.md:17`; a current value or absent JSON field is not historical evidence. |
| Qualified expensive work survives later refusal | Preserved | `docs/requirements/01-durable-storage.md:29–42`; selection refusal must not erase completion or force unrelated execution. |
| New adoption uses current requirements | Relied upon and extended to scoped corrections | `docs/PLAN.md:30–33`; lookup-time approval alone loses Q4's race. |
| Complete results have exact coverage and occurrence correspondence | Preserved | `docs/requirements/03-schema-backfill.md:33–45`; matching a business key can attach output to a changed occurrence. |
| Ordinary reads do not revalidate history | Preserved | `docs/PLAN.md:44–64`; historical read availability and new-adoption eligibility remain separate decisions. |
| Metadata and evidence are bounded and retained honestly | Relied upon | `docs/requirements/07-operations-qualification.md:133–139`; excessive history must refuse before acknowledgment, not produce unreadable retained qualifications. |

## Resource and user-value analysis

The beneficiary is an operator correcting a discovered omission without discarding valid source, value, transfer, or validation work. The concrete improvement is selective reuse plus one shared eligibility rule. It removes repeated decision logic across scalar, refresh, and export paths, and avoids salting unaffected file work with a new broad invalidation. No speedup or memory reduction has been measured in this review.

Preserve the existing file-level reuse granularity. A change to any physical field changes that file's `ObjectRef` and can legitimately recompute its scalar outputs. Q4 does not require a new per-row or per-column value cache to recover reuse across rewritten files. The same-file cases above establish a useful supported correspondence without scanning full values. Richer correspondence proofs remain an explicit extension, not an implicit promise of this design.

Let `C` be retained correction decisions, `Q` qualification completions, `D` material declarations in the candidate, `F` candidate files, and `H` lifecycle history bytes. Store correction/qualification metadata proportional to the actual decisions and evidence references, not source rows or every descendant result. On a warm exact-snapshot cache, current-state checks should require keyed access for the candidate's `D` declarations and exact qualifications. File-level work may still be proportional to `F`; payload reconstruction is limited to the newly affected qualification/recomputation scope.

This does **not** make the current catalog constant cost: cold opening still reads `H`, commit clones current values, and bounded metadata can be exhausted (`src/catalog.rs:228–252,344–396`). Nor does it eliminate existing scalar-history walks. Retain those limitations explicitly. Avoid copying the entire accumulated correction log into every request, file completion, result, and owner binding. Store shared immutable evidence once and reference it; a current cumulative requirements record may summarize obligations but must retain links to their decisions. Do not hide new retained bytes or compaction work in a claimed lookup gain.

Large correspondence proofs should use existing Arrow/DataFusion processing and Iceberg relations, with bounded batches and exact coverage. Only the qualification boundary may read newly necessary columns or reconstruct affected values. Routine planning should report that work is required rather than execute it to produce a nicer plan. Store the qualification result once; unchanged retry and commit consume metadata. Retain proof inputs for their promised lifetime using existing owner bindings.

The value claim is falsified if a correction to one classifier forces unaffected source/group computation, if the same historical evidence is scanned on every retry, or if the accumulated qualifications make a fixed-size update unusable at the supported history target. Measure those cases after the active full-run series, using the existing Q7 protocol rather than introducing a new infrastructure benchmark.

## Counterfactuals and qualification obligations

| Alternative or probe | Verdict |
| --- | --- |
| Use only global `invalidate` and rebuild | Keep as conservative fallback; reject as Q4 completion because unaffected work is lost. |
| Mutate the old declaration or increment its generation | Reject; it destroys the distinction between what ran and what later evidence established. |
| Add a reverse dependency index and eagerly invalidate every descendant | Defer; the existing admission/ancestor/selection paths already provide the correctness boundary. Measure a history bottleneck before adding index construction and maintenance. |
| Revalidate every old result whenever it is read | Reject; conflicts with immutable ordinary reads and shifts correction cost to every consumer. |
| Remove supplemental qualification altogether | Conservative recomputation remains possible, but Q4's adequate-evidence adoption case is not delivered. |
| Add a new generation of specialized caches | Reject; R1 already owns retained completion and reuse. |

Required falsification cases for implementation, **not executed here**:

- Pause after actual reuse lookup; commit an omission; resume against a changed language input. Final selection refuses the old decision, preserves original completion, and reports the correction.
- Repeat with adequate retained historical evidence and unchanged material language: adopt the original output generation through a new supplemental association, without recomputing values.
- Repeat with known changed language, unknown historical language, missing retained evidence, absent versus null/default, and ambiguous occurrence correspondence. No inference substitutes for proof.
- Correct one declaration while an independent group reuses completed files. Preserve those file identities; reject selection only if its full combined result includes an unresolved affected component.
- Exercise cached prepared/validation/readback, evidence-only inheritance, source rebuild/update, related-input capture and ancestor reuse, and new export admission/selection. Include a correction before admission as well as during execution.
- Lose the correction commit response and retry its exact ID; lose qualification/selection responses; introduce a second correction after qualification. Reconciliation must neither duplicate the correction nor reuse a qualification under unsatisfied requirements.
- Reopen and retire one phase while another result depends on its supplemental evidence; verify preserved owner protection, exact generation identity, and conservative handling of unknown legacy records.
- Hold payload and change size fixed while growing corrections, qualified associations, files, and retained history. Record routine payload reads, metadata bytes/requests, cold/warm costs, commit cost, and retained storage. Routine lookup/selection must not repeat qualification payload work.

**Kill criterion:** an affected adoption can bypass the shared current-requirements check, or supplemental evidence cannot be tied to the original execution and exact target. Either observation invalidates this design. The likely future maintenance criticism is duplicated semantic checks across entry points or copied evidence histories; the shared evaluator and direct references address that risk without speculative infrastructure.

## Verdict

**RECONSIDER the broad-invalidation-only implementation shape; approve Q4's stated requirement.** Add scoped, immutable correction decisions and retained adoption qualifications inside the existing lifecycle, with shared lookup and final-selection eligibility checks. Keep broad invalidation as an explicit conservative operation.

- Intent versus current shape: incomplete, as STATUS already records.
- User value: supported by Q4, R1/R3, and the retained research requirements.
- Commitments: honored by the proposed reshape; broad-only invalidation does not fulfill selective reuse.
- Conceptual debt: small necessary historical distinction, offset by sharing eligibility and existing retention/commit services.
- Existing primitive coverage: substantial; domain semantics and qualification records are missing, storage/execution infrastructure is present.
- Confidence: high in the static boundary analysis; runtime correctness, resource cost, and remote behavior remain unqualified.

## Follow-up on the concrete author design

Reviewed `docs/dependency-corrections.md` on 2026-10-10, read-only except this review. **Approve its bounded implementation direction**, with the following precise encoding and helper constraints. This approves a design, not Q4 completion, runtime behavior, or resource performance.

- **Keep the actual file-cache fixture isolated.** The original English backfill must stop after acknowledged `files` and remain unselected before the language hide/add. The author plan now states this correctly (`docs/dependency-corrections.md:109–128`); the setup above is corrected to match. `prepare_group` acknowledges files and stops before native group assembly at `src/engine/backfill/preparation.rs:150–168`. Keep prepared and readback cache branches separate. Known mismatch or unknown history must refuse old reuse and require an explicitly corrected definition/check, not automatically bless another execution of the same deficient text-only expression (`docs/dependency-corrections.md:30–34`).
- **Do not append the entire correction-reference history with each current-state update.** The literal shape in `docs/dependency-corrections.md:43` stores cumulative fields *and* every correction reference. With `C` corrections to one scope, rewriting a growing reference list retains a sum proportional to `C²`, because the catalog charges all appended row versions (`src/catalog.rs:344–354`). Keep immutable decisions under directly scoped keys. Current state needs the normalized cumulative field set and a current revision/latest decision reference; an on-demand explanation can retrieve the decisions. Union with the previous field set in the guarded correction commit so requirements cannot disappear. Qualification pins the exact normalized requirement state/checker; it does not need to copy the whole decision list. The field set's own growth remains bounded by the accepted declaration size and must stay in budget.
- **Share semantic field interpretation, not the physical-schema digest.** The plan correctly requires IDs, types, requiredness, initial defaults and material metadata (`docs/dependency-corrections.md:22–28`). `storage::schema_identity` is explicitly physical: it removes `doc`, `initial-default` and `write-default` (`src/storage/inspection.rs:27–52`). It cannot prove the required correspondence. Extract/share the small per-field normalization already used by `Expressions::compile` (`src/engine/backfill/expressions.rs:136–155`), retaining read defaults and the metadata declared material while ignoring display names and future-write policy where appropriate. Reuse `inputs::read_manifest` for bounded, hash-bound JSON decoding (`src/engine/inputs.rs:399–438`) and the existing native schema/default reader rather than creating a second decoder.
- **Keep immutable origin evidence shareable.** The original-executions descriptor (`docs/dependency-corrections.md:44–52`) is justified because a group may mix generations and offline verification cannot depend on a mutable catalog key. Create it at the retained preparation boundary; reuse its exact reference in validation, readback, supplemental associations and retries. When the origin set is unchanged, retain that descriptor instead of emitting an identical copy for each new target association. Reference existing declaration/output descriptors rather than embedding their contents again. Keep file-level origin growth explicit and bounded; do not add one standalone object per row or an eager descendant index.

The plan's shared final check in `select_with_progress`, affected-value export checks, unchanged historical reads, and exemption for source-only related consumers are the smallest appropriate boundaries (`docs/dependency-corrections.md:71–90`). The remaining cost is real but bounded metadata: an origin list per distinct assembled generation, corrections, qualifications, existing cold lifecycle reads, and retained evidence. Measure those costs later with fixed payload/change size; no new cache, catalog, comparison engine, or payload scan is justified by this design review.
