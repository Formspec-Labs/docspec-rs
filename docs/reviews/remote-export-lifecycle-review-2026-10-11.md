# Remote source export: independent code review

**Verdict: APPROVE the bounded implementation for the repository gate.** No remaining material source finding after the final fixes. This is a static review plus inspection of the retained focused-test log; it does not establish provider, full remote lifecycle, storage amplification, or performance acceptance.

Reviewed the applied/formatted worktree on 2026-10-11: lifecycle core v2, final fixes `76565f458cc7ef6d8ebc836dce95feb5b34ae35a2b9d16d2d7c9ba29cba9e446`, phase-binding fixture, and shared storage helper `1e7e88fa97773b2420a41220376e233f80b52433ef4b8d507c2fcb728827add0`. No builds, provider calls, or production edits were performed by this reviewer. Repository AGENTS, ms-rust and semi-formal-code-review guidance applied.

## Findings and disposition

| Finding | Disposition and current evidence |
|---|---|
| Remote lookup could create lifecycle state | Fixed: `src/engine/export.rs:168` uses the existing-only remote `read_view` path. |
| Retained remote output could be selected without exact stage/attempt proof | Fixed: `export.rs:286` validates configured scope and `remote::check_completed` before selection; retained pin readers and completed retries check the same binding (`export.rs:204`, `:234`). |
| Repeated source-bearing operation decode and unadmitted dependencies | Fixed: validators receive one typed operation; keyed decode reservations cover dependent records (`remote.rs:285`, `:376`, `:432`, `:503`). Inventory reuses its enclosing allowance. |
| Values alone did not prove original protection or atomic transfer completion | Fixed: attempt/protection must have the same native row revision, stage follows attempt, validation follows stage, and remote materialization/validation share a later revision (`remote.rs:309`, `:463`). |
| Transfer could start before rechecking retained protection/generation | Fixed: fresh authority/channel and current attempt/protection/stage revision checks precede transfer (`remote.rs:78-114`); authority/channel are checked again before the guarded completion commit (`:127-164`). |
| Missing remote validation could fall into a local file validator | Fixed: remote half-completion explicitly refuses (`export.rs:272`). |
| HTTP read resolved a pin twice, and canonical loading preceded stream admission | Fixed: `read_export_id` performs the canonical load inside `admit_stream`; exact-ID lookup avoids a second result decode (`export.rs:129`, `:181`). HTTP uses that method directly (`http.rs:790`). |

## Patch summary and function traces

The change connects an authorized primitive source export to the already configured object backend. It reuses the local Arrow/Parquet materializer, retained completion schemas, conditional immutable transfer, and shared Arrow IPC transport. It adds one private attempt schema, not a second publication authority.

| Function/path | Inputs and output | Verified behavior |
|---|---|---|
| `export_request` / `export_admitted`, `src/engine/export.rs:72`, `:223` | Exact export request → immutable selected pin | Admits work; rejects unsupported remote groups; checks qualified initial independent Build; resolves completed output before materialization or transfer; selection remains in lifecycle catalog. |
| `reserve_export_attempt`, `src/engine/export/remote.rs:169` | Typed operation → saved attempt | Original operation revision, exact remote destination and exclusive host staging prefix are protected together before writer effects. Lost-response adoption rechecks authority, channel, identity and host. |
| `remote_materialize_export`, `remote.rs:22` | Saved attempt/stage/check → remote materialization | Retains closed stage and its check separately; checks destination before host file access; independently verifies newly transferred bytes; co-commits remote materialization/check, then caller selects it. |
| `Objects::transfer_immutable_file`, `src/storage/object_io.rs:127` | Expected identity + local stage → exact remote identity | Reuses source-admission implementation. Resume first verifies the exact destination; authoritative absence permits one conditional upload. Conflict/uncertainty reconciles the same identity. No destination change or overwrite retry. |
| `materialize_export`, `src/engine/export.rs:519`, `:600` | Exact native source → authorized host Parquet | Configured guarded scan, existing bounded writer, hash during writing, free-space checks and closed-file identity. Unique stage children prevent interrupted writers from poisoning the final remote URI. |
| `load_export_pin` / `open_export_stream`, `export.rs:168`, `:146` | Catalog result or exact supplied pin → batch stream | Metadata-only canonical identity and configured scope checks; schema matches retained fields before pulling payload. No full digest pass or separate availability HEAD on ordinary reads. |
| `check_completed` / `check_pin_binding`, `remote.rs:432`, `:503` | Retained attempt, stage, checks and native revisions | Remote creation identity derives only from the protected destination plus closed/checked stage; host file availability is not needed after durable remote completion. |
| Inventory / creation collector, `src/engine/retention/inventory.rs:1243`, `:1267`, `src/engine/retention/owners.rs:993` | Lifecycle rows → protected dependencies/creation evidence | Stage objects enter dependency inventory. Attempt prefixes and host stages never become remote creation proof; remote materialized records require exact completion binding. |
| HTTP / CLI, `src/http.rs:790`, `:877`, `src/main.rs:889` | Immutable export ID/pin → Arrow IPC | Both call Engine readers. Shared HTTP encoder retains Engine/reader custody for the response lifetime. CLI uses existing stream writer and shutdown. |

## Data flow and invariants

The catalog retains `ExportAttempt`, then existing materialization and validation types under stage keys. Expected remote owner/digest/size derive from that immutable stage; only its path changes to the already protected exact destination. A verified transfer produces the existing remote completion pair in one guarded commit. The immutable result and channel are selected afterward. No stage path, in-scope object, or intent by itself establishes remote creation.

Completed remote result reuse precedes stage/source access. Concurrent unfinished attempts may repeat uncommitted stage/transfer effort; create-only remote writes and first retained stage preserve exact identity. Existing durable completion prevents subsequent upload/verification. This review does not infer zero network requests from returning the same pin.

Memory/cancellation reuse existing Engine-owned work and stream admission. Configured flat reads retain transport/footer/row-group/batch admission until stream drop; the helper review is `/tmp/docspec-export-storage-review.md`. This is bounded declared admission, not measured peak RSS. The new path does not duplicate a storage credential registry or introduce a lease system.

## Tests and remaining qualification

Inspected `docs/reviews/remote-export-focused-2026-10-11.log`: all focused tests passed. The exact reader fixture in `export.rs:1087` checks literal values through the ID reader and refuses a changed caller digest. The native lifecycle fixture at `remote.rs:534` checks separated completion rejection, co-commit acceptance, and later identical protection rejection. It intentionally fabricates retained metadata to test validators; it is not proof of actual remote upload or host-stage creation. Flat-reader fixtures establish exact duplicate/null/empty values, scope rejection before requests, one footer-tail/body fetch, no HEAD/full GET, and permit release.

The new CLI `read-export` and HTTP Arrow route still need the small transport checks requested from their author. The opt-in provider export recovery fixture is separately reviewed and pending execution. The current passing helper/binding tests do not replace that provider result or the full repository gate.

## Explicit limits

This increment covers primitive authorized source-group export from the first independent remote Build. Other remote mutation/maintenance paths keep their existing refusals. Unfinished staging belongs to its original exclusive host; completed remote results can be read from a fresh host. Remote stage reclamation, cross-host unfinished recovery, anonymous publication, application evidence compatibility and full Q8 remain open.

Full closed stage bytes remain protected locally after remote completion, alongside the remote copy. Partial/losing unique stage children are protected but not enumerated as completed objects. Existing per-batch free-space checks are not full retained-storage accounting or disk qualification; precise stage accounting/retirement remains a concrete follow-up. No new deletion authority is introduced.

**Coverage: adequate for this bounded gate increment; provider and transport checks remain separate. Confidence: medium-high.**

## Parent-owned execution

The combined `./scripts/check` passes in
`remote-export-plan-full-check-2026-10-11.log`. It includes the relevant library
and integration cases, strict all-feature Clippy and formatting, and the restore
and worker-initialization suites. Opt-in provider and application checks have
separate retained evidence; this gate does not claim they ran automatically.
