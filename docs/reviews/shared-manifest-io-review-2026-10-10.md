# Manifest I/O and phase availability follow-up review

Root execution follow-up: the focused remote suite and actual manifest-reader
cancellation tests passed in `shared-manifest-tests-2026-10-10.log` and
`shared-manifest-cancellation-tests-2026-10-10.log`. The no-default-feature library
Clippy check passed in `shared-object-local-feature-check-2026-10-10.log`.
The final `./scripts/check` passed in
`shared-object-manifest-full-check-2026-10-10.log`; its `.resources.json` retains
the disk observations and terminal exit code. The gate includes strict
all-target/all-feature Clippy, library/integration, restore, worker-initialization
and documentation checks. The existing external Comments plan probe is ignored.
No corpus benchmark or actual-provider run was repeated for this private I/O change.

Earlier execution evidence remains retained: the first pin test compile failed
because the native Table import used the wrong module; corrected imports passed
`shared-object-pin-tests-2026-10-10-2.log`. The interim pin-only full gate in
`shared-object-pin-full-check-2026-10-10.log` was deliberately stopped by the root
agent after review identified admission ordering and before adding this manifest
seam. Its exit `-15` is an interrupted check, not a passing gate. The final complete
gate above covers both corrected changes. The independent static review below
predates that final execution and does not claim provider qualification.

2026-10-10. **VERDICT: APPROVE**, subject to the repository gate. No actionable
blocking finding in the applied/formatted source. This approves the shared
metadata reader, stat-only phase reuse, and the current local Engine integration;
it does not qualify a remote Engine or complete remote publication.

Static review only. I inspected current source, callers, helper implementations,
and test assertions. I ran no build, tests, formatting, or provider operations,
and changed no source. The earlier pin review is at
`shared-object-io-review-2026-10-10.md` and its admission note is updated.

## Findings and resolved items

**Inventory admission correction confirmed.** `snapshot_files` reserves before
its walk (`src/storage.rs:573-574`). `pin_bound` reserves before copying shared
identities (`:715`) and transfers that exact permit to the guarded walk (`:739`).
The helper consumes the permit (`:577-582`); there is no second inventory charge.
This closes the prior resource observation. The explicit native/object binding
and pre-read manifest scope guards remain intact.

**Draft compilation issues corrected in current source.** Standalone
`verify_input_coverage` now explicitly selects Local rather than referring to a
nonexistent `self` (`src/engine/inputs.rs:299-317`). The cancellation test's
`Decoded` type derives Debug (`:591-592`). Static review is not a substitute for
the parent-owned compile/test gate.

**No unnecessary repeat payload read found.** `read_manifest` creates one admitted
buffer, calls the selected reader once, then parses that same verified buffer
(`inputs.rs:423-440`). Remote metadata uses one streamed GET and bounded writes
into that buffer (`src/storage/remote/objects.rs:364-397`). The shared wrapper
hashes those same bytes once before parsing (`src/storage/object_io.rs:57-92`).
There is no hash-first GET followed by a parse GET or temporary-file copy.

## Patch summary and function trace

| Function / caller | Location | Inputs → outputs and verified behavior |
|---|---|---|
| `Objects::metadata_read_allowance` | `src/storage/object_io.rs:49` | Local adds zero; Remote adds the existing transport allowance. |
| `Objects::read_metadata_admitted` | `object_io.rs:57` | Exact ObjectRef + caller buffer → backend read, exact size, one BLAKE3 check; no worker or admission inside. |
| Remote `read_metadata_admitted` | `src/storage/remote/objects.rs:364` | Strict scope → HEAD → one stream into bounded slices → HEAD/count checks. Empty buffers perform no GET. |
| `inputs::read_manifest` | `src/engine/inputs.rs:416` | Backend + ObjectRef + aggregate accounting → one owned worker and typed JSON parsed from verified bytes. |
| `read_relationship` | `inputs.rs:398` | Same reader, then existing format/dataset/version checks. Engine callers inject their backend; standalone coverage verification stays explicitly Local. |
| Release/backfill readback | `src/engine.rs:1635`; `src/engine/backfill.rs:849` | Injected backend reads retained manifest, then unchanged exact decoded-result equality. |
| Change evidence reads | `src/engine/changes/certificates.rs:347,387`; `changes/chain.rs:123` | Injected backend; existing selected-version and applied-change checks remain. |
| `Engine::phase` | `src/engine.rs:1795-1815` | Captured completed phase → one owned stat-only worker → reuse same completion/protections. |
| `check_available_admitted` | `object_io.rs:128`; `remote/objects.rs:344` | Local existing regular-file/length check or one remote HEAD; no hashing or GET. |

The remaining free-standing traversal helpers deliberately pass Local:
`evidence.rs:583`, `schema.rs:618`, `refresh.rs:554`,
`backfill/origins.rs:78`, `backfill/verification.rs:380`, and retention expansion
at `retention/inventory.rs:1579-1656`. I checked their diff and contexts: their
validation, accumulated byte accounting, and post-read identity checks are
unchanged. Their explicit backend is a documented scope limit, not remote support.

## Data flow and invariants

- Declared byte count is converted safely and added to the existing combined
  metadata limit before allocation/I/O (`inputs.rs:423-428`). Each byte buffer is
  sized exactly once. The remote allowance supplements that buffer; the existing
  blocking worker still retains its fixed allowance. These are admission limits,
  not a measured process/transport memory ceiling.
- The shared metadata permit, buffer/transport permit, cloned backend and expected
  ObjectRef all move into the actual worker; parsing occurs before their drop
  (`inputs.rs:426-439`). `Budget::blocking` / `blocking_admitted` retain memory and
  worker ownership through closure completion (`src/budget.rs:130-165`). Caller
  cancellation does not release that active work's reservations prematurely.
- Local read semantics are preserved: open once, check declared length,
  `read_exact`, one-byte overflow probe, digest, then JSON parse
  (`object_io.rs:73-92`). Missing/short files retain underlying I/O errors;
  changed size/digest returns structured corruption refusal.
- Remote reads validate scope before their first request, refuse unexpected
  length before GET, bounds-check every slice, and refuse changed final length
  or short/excess content (`remote/objects.rs:369-395`). The wrapper checks the
  retained digest, so the parser never sees unchecked substituted bytes.
- Phase reuse checks size and file availability only. Local reuse now shares the
  existing regular-file check and maps missing files to `unavailable_phase`
  instead of exposing the old raw filesystem error. This tightens the intended
  retained-file check; it does not revalidate payloads. Native phase selection,
  eligibility checks and protection lookup remain unchanged.
- Phase workers own the retained completion while checking it; no reference
  escapes as a successful completion before those checks finish. The new worker
  makes filesystem stats stop blocking the async executor and supplies bounded
  remote I/O ownership without nested workers.

## Tests and concrete edge cases

Inspected assertions; I did not execute them.

- `canceled_manifest_read_keeps_admission_until_its_blocking_work_exits`
  (`inputs.rs:525`) still exercises the actual Local reader queued behind a
  blocking job. Its expected reservation arithmetic remains valid because the
  Local transport increment is zero.
- `canceled_remote_manifest_read_keeps_worker_and_admission_through_decode`
  (`inputs.rs:584`) calls the actual Engine helper, waits for an actual GET to be
  held by the fixture, aborts its caller, checks retained metadata/worker
  admission, releases the response, and observes deserialization afterward. It
  finally checks reclamation and one GET/two HEAD requests. The fixture's hold is
  bounded and test-only (`remote/objects/tests.rs:268-282,365-368`); no production
  hook or wider production API was introduced.
- `metadata_reader_hashes_one_get_and_availability_reads_only_metadata`
  (`tests.rs:424`) checks decoded value plus one GET and the expected HEAD-only
  availability check. `metadata_reader_refuses_changed_bytes_lengths_and_scope`
  (`:465`) covers corrupt content, truncated stream, changed final size and
  pre-request scope rejection. `empty_metadata_and_changed_availability_use_no_payload_read`
  (`:521`) confirms empty reads and availability failure avoid GET.
- Existing local relationship, release equality, change certificate, retention
  and phase-resume paths use the same parser/checks through the selected backend.
  There is no new full-dataset reconstruction or benchmark requirement for this
  dispatch change. The normal affected tests and repository gate remain necessary.

## Conclusion and remaining boundaries

The change follows the repository's read-once guidance at the relevant boundary:
metadata is bounded, read once, hashed once, and parsed from the same buffer;
qualified phase outputs receive availability checks rather than integrity scans.
It reuses the concrete storage dispatch and existing admission mechanism without
adding another lifecycle authority, reader framework, or user workflow.

**Coverage of scoped changes: ADEQUATE. Confidence: HIGH for static behavior.**
No runtime/provider performance claim is made by this review. Host admission,
object locations, DataFusion remote input access, additional read traversals,
remote retention and restore still require their connected implementation and
qualification before any remote Engine constructor is exposed.
