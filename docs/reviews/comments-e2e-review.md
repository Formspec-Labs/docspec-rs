# Comments full local acceptance review

**Current status: APPROVE the harness for controlled collection.** F1, F2 and F3
are closed by the follow-up inspections below. The parent also supplied passing
synthetic integration/negative-test evidence, inspected in the final review.
There is no remaining blocking finding in this review scope. This is not a
10-million-row performance pass. Earlier findings are preserved as history.

## Initial findings and decision

**REQUEST CHANGES before accepting a measured run.** The harness exercises the
full declared local workload, and the exact value/contributor comparisons have a
sound structure. Two readback gaps can nevertheless accept missing retained
source evidence or an incorrect flat-export field mapping. Neither finding calls
for reducing the population, omitting a stage, or weakening the independent
comparison.

This was a blind, static review on 2026-10-09 of the new
`experiments/comments-e2e/` files and the unstaged monitor changes in
`experiments/comments-input/run.py`, against HEAD
`d57042e00a43b821b9730ad688d94ad6d2e011f4`. The working tree contains other work;
that work was read only where necessary to trace actual APIs. No benchmark,
build, test, corpus scan, or corpus hash ran. Read-only Parquet footer inspection
confirmed the selected Dockets and receipt field shapes and footer row counts;
this is not payload or source qualification. Guidance: repository `AGENTS.md`,
`docs/PLAN.md`, test-hypotheses, semi-formal-code-review, and the Rust review skill.

### F1 — BLOCKER: independently check the retained original inputs

**Category:** correctness / evidence retention. **Location:**
`experiments/comments-e2e/oracle.py:108-116,211-215`.

`Oracle.input` opens the external prepared input path. For the admitted original
input, `run_checks` compares only the `digest` and `bytes` strings/numbers in the
release. It never checks `admitted["path"]`. All subsequent full comparisons read
the external original and native result files. Therefore deleting, replacing, or
misreferencing an admitted original after processing can leave every oracle check
passing. A correct native result is not proof that its promised original bytes
remain retained.

The engine explicitly creates a separate retained input with `copy_verified`
(`src/engine.rs:903-918`), and the plan requires immutable original bytes
(`docs/PLAN.md:23-28`). The acceptance protocol also claims retained whole-input
provenance (`experiments/comments-e2e/protocol.md:48-54`). This is a gap in the
acceptance oracle, not evidence that the engine currently loses those files.

**Required fix:** check the admitted reference's owner/path/size and independently
establish that the retained file has the frozen whole-input byte identity during
the timed readback. A maintained streaming digest or exact byte comparison is
appropriate at this independent qualification boundary. Keep the external
original as the expected-value authority; merely switching all expectations to
the engine's admitted copy would permit shared corruption to agree. Add a small
negative case where the retained original is missing and another where its bytes
change while metadata remains plausible. Both must prevent acceptance.

### F2 — BLOCKER: check flat field IDs and the declared export interpretation

**Category:** correctness / schema interpretation. **Location:**
`experiments/comments-e2e/oracle.py:291-305` and `41-44`.

The flat path calls `same_schema`, which deliberately ignores all Arrow metadata,
and never reads `ExportPin.fields`. Thus an export with correct names, types,
nullability, rows and values but missing/wrong `PARQUET:field_id` metadata passes.
An incorrect field-ID mapping in the pin also passes. The catalog equality check
only establishes that the catalog returns the same pin; it does not independently
validate that pin's meaning (`run.py:230-232`).

Field identity is an actual supported export property: `ExportField` records
`id`, `name`, `data_type`, and `nullable` (`src/export.rs:43-52`), and the writer
preserves the native field ID in each authorized exported field
(`src/engine/export.rs:669-684`). The native oracle already recursively verifies
IDs (`oracle.py:47-67`). The protocol promises field-ID and schema checks
(`protocol.md:71-80`), and the plan requires exact schema interpretation
(`docs/PLAN.md:160-162`).

**Required fix:** compare each flat field's native ID and interpretation against
the exact selected native field, and validate `pin.fields` against that same
mapping. Preserve the exporter rule that arbitrary source metadata is omitted;
do not require wholesale metadata equality. Add negative fixtures with correct
values but a missing/changed exported ID and an incorrect pin field mapping.

### F3 — WARNING: the frozen executable and dirty source are not fully retained

**Category:** reproducibility. **Location:**
`experiments/comments-e2e/run.py:162-184,194,233,281-293`.

The environment records HEAD, status, and one initial binary/harness digest.
`git status --short` does not capture the content of dirty source files. Commands
continue to execute the original mutable binary path, and the oracle is imported
from its mutable source path. A rebuild/edit during the series could mix arms
while the result reports only the initial identity. Under the protocol's stated
quiescent-workstation condition this is not a demonstrated failure, but the
retained record alone cannot establish an unchanged arm afterward.

**Suggestion:** retain the relevant source diff/build receipt and run a captured
binary/harness or verify the frozen identities at explicit boundaries. Do not
require another corpus hash for this check. Record any drift as a failed series.

### F4 — WARNING: guard and integrated oracle paths lack harness tests

**Category:** test coverage. **Location:**
`experiments/comments-e2e/test_workload.py:13-61`.

The new tests cover comparator duplicate/null/order behavior, schema type and
nullability differences, scratch classification, and selected request fields.
They do not exercise `run_one`/`execute`, shared deadlines, child termination,
resource refusal, a complete CLI-to-oracle fixture, or either negative case above.
No test coverage for those new harness paths was found. Existing engine tests
are useful component evidence but do not call this Python runner or oracle.

**Suggestion:** before the expensive collection, run a bounded synthetic complete
path with the actual selected binary and packages, plus targeted negative tests
for F1/F2 and the acceptance flag. Keep that fixture result distinct from the
10-million-row performance decision.

## Patch summary

The patch defines a fixed full-width Comments workload, prepares retained Dockets
and nested receipt inputs, runs the source/shaping/seal/join/summary/export/select
sequence, and checks exact native and flat values independently. It extends the
existing monitor with explicit per-call limits, a shared deadline, runner-tree
RSS sampling, and multiple scratch roots while preserving its old defaults.

Important callers are `main → execute → run_one → monitor.command` and
`oracle.main → Oracle.run_checks`. The CLI calls map to real variants and engine
entry points (`src/main.rs:56-63,575-618`); request enums/fields match
`src/model.rs:202-238,295-365,404-417,697-711` and `src/export.rs:22-40`.

## Function trace

| Function | Location | Inputs → outputs | Verified behavior |
| --- | --- | --- | --- |
| `version`, `source`, `seal` | `workload.py:45-59` | selected inputs/releases → request dictionaries | Exact release references; Comments adds byte lengths; Dockets/receipts form a generation. |
| `refresh`, `join`, `summary`, `export` | `workload.py:62-99` | releases → derived/export specs | Full left join, nulls never match, many-to-one; exact decimal sums; every declared user field exported in order. |
| `monitor_module` | `run.py:28-36` | monitor path → imported module | Reuses the old monitor without invoking its main routine. |
| `require`, `read`, `write`, `metadata`, `dependencies` | `run.py:39-63` | metadata/paths → checks or records | New observation files use exclusive creation; metadata reads do not hash payloads. |
| `prepare` | `run.py:66-140` | frozen records/input metadata → prepared record | Reuses the Comments identity; hashes small Dockets members explicitly; keeps failed attempts. |
| `validated_inputs` | `run.py:143-159` | prepared record → selected inputs | Enforces fixed population and input metadata; timed engine admission must verify bytes. |
| `environment` | `run.py:162-184` | binary/options → environment record | Captures packages/load/resource settings and initial identities; F3 limits reproducibility. |
| `run_one`, nested `command`/`request` | `run.py:187-264` | fresh workspace/allowance → result | Runs all required stages; one deadline; gathers failed-stage records; no resume/retry substitution. |
| `execute`, `main` | `run.py:267-333` | CLI options → series result | Disk preflight; acceptance requires three successful fresh runs; pilot cannot set acceptance true; TERM unwinds into cleanup. |
| `totals`, `measured_totals` | `comments-input/run.py:57-80,98-101` | inventories/scratch prefixes → logical/allocated categories | Multiple engine/oracle scratch roots; default remains legacy `scratch`. |
| `command` | `comments-input/run.py:104-228` | argv/bounds/deadline → output/metrics or error | Samples runner descendants and live free space, stops the private child group, retains command and measurement files. |
| `q`, `literal`, `columns`, `same_schema`, `native_field` | `oracle.py:29-67` | names/schema fields → SQL/checks | Quotes identifiers/values; exact Arrow shape except metadata; native IDs checked recursively. |
| `exact_equal` | `oracle.py:70-79` | two relations/columns → equality or refusal | Signed `UNION ALL`, grouping over every selected value; unequal multiplicities fail. |
| `Oracle.__init__`, `record`, `view` | `oracle.py:83-106` | run/SQL → bounded DuckDB connection/check log | 18 GiB DuckDB setting, 12 threads, explicit spill root; append-only completed-check receipts. |
| `Oracle.input`, `native`, `release` | `oracle.py:108-191,198-205` | inputs/pins → SQL views/schema/membership | Exact snapshot planning; refuses deletes/partial files; footer/membership checks; F1 remains. |
| `Oracle.equal`, `run_checks`, `main` | `oracle.py:193-326` | captured run → independent pass or exception | Checks shaped source, receipts, joins, summaries, contributors and flats; F2 remains. |

## Data flow and invariants

- **Complete population:** `ROWS` is fixed at ten million; all original Comments
  columns remain alongside lengths (`workload.py:16,24-42`). The independent
  source and join row checks require that population (`oracle.py:225,263`). No
  `LIMIT`, deduplication, text truncation, or synthetic replacement appears in the
  measured workload.
- **Duplicate/null/order fidelity:** the signed multiset comparison groups by all
  values and counts multiplicity. Null remains distinct from empty text. Nested
  list values are compared as typed values, preserving list order. Meaningful
  schema checks precede these comparisons. There is no global row-order promise
  (`protocol.md:75-81`).
- **Independent computation:** expected byte lengths use DuckDB over the original
  input; source values are checked before they supply independently reconstructed
  joins. Expected joins include each native source occurrence's file/row identity;
  full contributor relations are compared independently (`oracle.py:218-232,
  250-289`). Summary evidence uses actual joined-file positions only after the
  joined values and input contributor pairs have passed. That is a justified
  physical-identity dependency, not reuse of the engine's contributor table.
- **Actual engine semantics:** the engine constructs the same four join-location
  fields and applies the declared equality/null/cardinality rule
  (`src/engine/refresh.rs:567-627`). Aggregation casts signed operands to
  Decimal128 before summing and writes one contributor row per input occurrence
  (`src/engine/aggregation.rs:131-176`). The Python oracle independently expresses
  these computations; it does not call those Rust helpers.
- **Deadline and failure truth:** a single per-run deadline reaches every command
  (`run.py:201-209`), is rechecked after pre-command inventory in the monitor
  (`comments-input/run.py:122-126`), and is checked after readback and in final
  accounting (`run.py:233-245`). Stage failure cannot become a successful run.
  A failed repetition stops the series; pilot cannot pass acceptance
  (`run.py:276-295`). These control paths were traced, not executed.
- **Resources and disk:** limits include runner plus descendants; engine and
  oracle scratch roots share the sampled ceiling. Disk admission reserves all
  planned retained outputs, one scratch allowance and live reserve
  (`run.py:195-209,281-287`). Monitored logical size, allocated bytes and volume
  free space remain distinct. Whole-process and disk observations are sampled;
  the explicit short-lived-peak limitation is accurate (`protocol.md:112-119`).

## Tests and concrete edge cases

The test expectations below follow static traces only; this review did not run
them.

| Test/scenario | Evidence | Expected behavior / coverage |
| --- | --- | --- |
| Lost duplicate, null→empty, reordered nested list | `test_workload.py:14-31` | Each mutation changes a grouped value or net multiplicity, raising `ValueError`. |
| Int64→Int32 or required→nullable | `test_workload.py:33-38` | Arrow schema equality rejects before SQL can coerce. |
| Combined scratch and legacy default | `test_workload.py:40-49` | Prefix components classify engine and oracle spill together; old single-root behavior preserved. |
| Full left fields and coupled receipts | `test_workload.py:51-61` | Checks the constructed request fields and generation members; does not exercise the Rust API. |
| Engine join duplicates/nulls | `tests/related_refresh.rs:49-65` | Existing fixture includes duplicate and partial/all-null keys; relevant component evidence only. |
| Exact engine aggregate type | `tests/related_refresh/aggregation.rs:54-84` | Existing reader asserts Decimal128(38,0); does not cover Python oracle integration. |
| Export schema/metadata | `tests/exports.rs:113-155`; `src/engine/export.rs:832-878` | Existing checks exercise primitive values and field metadata policy; they do not close F2 in independent acceptance. |
| Retained input missing or same-size replacement | `oracle.py:211-215` | Currently no retained-path check; F1 is untested. |
| Flat field ID/pin mapping wrong with values unchanged | `oracle.py:291-305` | Currently accepted by value/metadata-ignoring schema checks; F2 is untested. |
| Timeout/resource breach/pilot/failure in repetition | `run.py:233-245,276-301`; monitor `158-228` | Static trace is fail-closed; no harness test executes those paths. |

## Scope limits, not additional blockers

The protocol explicitly excludes remote publication, acquisition, restore,
maintenance, and separate unchanged/update/backfill/interrupted qualification
(`protocol.md:3-6`). Those remain required elsewhere by `docs/PLAN.md:152-158`;
they need not be added to this fresh-run timing. CLI selection exercises the real
application service but does not qualify HTTP or remote transport.

The frozen Comments input contains literal evidence-related JSON values and
whole-input provenance. It does not supply original Comments ETL receipts or
referenced attachment bodies. Dockets receipts are retained in full as a coupled
generation, with no invented `record_id = docket_id` join. Those limits are
explicit (`protocol.md:40-54`) and must travel with any measured result. Passing
this local gate cannot establish missing upstream-body retention, per-docket
receipt decoding, current-source delete-reader correctness, or all R1–R7 behavior.

No speedup claim follows from the signed-union comparison's avoided duplicate
scan; that is a structural improvement whose full cost remains to be measured.
The source-only pilot cannot satisfy this gate. No performance conclusion follows
from this review or from the footer metadata observations.

## Conclusion

**VERDICT: REQUEST CHANGES.** Close F1 and F2, retain targeted negative evidence,
then run the unchanged full workload under the frozen protocol. F3/F4 identify
reproducibility and coverage limits worth resolving before collection. Coverage
of the new acceptance paths is **INSUFFICIENT** at review time. Confidence is
**HIGH** in the two static oracle gaps and **MEDIUM** in end-to-end runtime
compatibility, which was not executed here.

## Follow-up review: retained inputs, field interpretation and frozen execution

This follow-up inspected the revised Python files and adversarial test source.
It ran no tests, builds, hashes, dependency installation, or corpus operations.
All line references in this section refer to the revised files at follow-up time.

### F1 closed: retained bytes now establish the independent expectation

`Oracle.input` resolves the admitted path strictly, requires it to belong to the
fresh workspace, checks its size, computes its BLAKE3 identity, and compares that
identity with the frozen prepared input before opening its Parquet footer or
registering the expected-value view (`oracle.py:135-152`). Every source release
passes through this method (`oracle.py:243-249`). Missing admitted input fails at
strict resolution even when the external original remains. A same-size byte
change fails the digest comparison.

The maintained Python `blake3` package computes the digest with bounded blocks.
`file_digests` compares device, inode, size and mtime across the open file before
and after reading and against the current path; it also verifies the total bytes
read (`integrity.py:10-31`). This refuses observed replacement/mutation during the
identity pass. Small-input preparation now computes SHA-256 and BLAKE3 in one
pass (`run.py:117-129`). Timed custody verification remains inside the monitored
oracle command (`run.py:232-234`).

Using the admitted copy to reconstruct expectations is valid **after** its bytes
match the independently frozen digest. This refines the initial F1 suggestion:
keeping the external file as a second payload-reading authority is unnecessary
once this independent byte check closes the shared-corruption gap. The selected
digest remains the external authority, and the oracle does not accept a digest
invented by the engine. The harness still assumes the prepared record is a trusted
output of its explicit preparation phase and that verified retained objects stay
immutable while readback consumes them. This is not a hostile-filesystem claim.

Adversarial coverage now directly targets the original false positive:

- `test_oracle_requires_retained_bytes_even_when_external_fixture_survives`
  (`test_workload.py:20-49`) first exercises a valid retained copy, removes it while
  preserving the external fixture, then writes an equal-length file with equal
  decoded values but changed metadata bytes. Expected failures reach path
  resolution and then the BLAKE3 equality check respectively.
- `test_oracle_reconstructs_from_retained_input_without_external_fixture`
  (`test_workload.py:51-67`) establishes that verified retained bytes are sufficient
  when the external file is absent.

These are meaningful synthetic cases, not corpus evidence. Their actual execution
result belongs to the parent task's retained check output.

### F2 closed: flat field IDs and serialized interpretation are checked

`flat_schema` first checks exact field order/type/nullability, then requires an
empty exported schema metadata map, exactly the selected native field ID on each
exported field, and the complete expected descriptor in `ExportPin.fields`
(`oracle.py:81-94`). The actual flat-output path invokes it for both exports
(`oracle.py:325-339`). It permits the intended omission of arbitrary source
metadata. A missing exported ID fails the same exact metadata comparison as a
changed ID; an incorrect pin mapping fails even when the footer is correct.

`export_field_type` deliberately supports only the frozen workload's Utf8,
Int64 and Decimal128 types (`oracle.py:71-78`). Its spelling matches the engine's
Rust debug representation used in `describe_schema`
(`src/engine/export.rs:687-709`). This is appropriate for this frozen workload,
not a general exporter validator. Existing recursive native field-ID checks
remain in place (`oracle.py:48-68,178-183`).

`test_flat_export_rejects_changed_field_ids_and_pin_mapping`
(`test_workload.py:69-85`) writes an actual Parquet fixture with altered field ID
and unchanged values/types, then alters each of the pin's ID/name/type/nullability
members independently. These assertions target the former blind spots directly.
This follow-up did not execute them.

### F3 remains an acceptance blocker: initial hashes do not freeze execution

The original warning has a concrete false-acceptance trace in the current runner:

1. `execute` records initial binary/oracle/workload/integrity hashes through
   `environment` (`run.py:179-183,279-285`).
2. Later commands still execute the original binary path and
   `HERE / "oracle.py"` (`run.py:193,206-208,232`). The oracle freshly imports
   `integrity` and `workload` (`oracle.py:20-22`).
3. No recorded identity is checked again before setting either pass flag
   (`run.py:233-237,287-292`). A rebuilt binary can therefore change the arm
   between stages/runs. More decisively, replacing the oracle script after initial
   capture with a program that returns `{"exact_readback_passed": true}` bypasses
   the full readback while satisfying the only returned-result predicate.

This does not mean substitution happened. It means the current pass flag cannot
establish the protocol's held-constant executable/harness identities. The scope
is normal shared-worktree changes and stale/mixed execution artifacts, not a
promise to resist an adversary with arbitrary write access to the whole machine.

**Minimal required fix:** capture the executable and complete Python harness in
an experiment-owned execution snapshot, record their identities, and execute that
snapshot. Ensure relocated scripts resolve the intended repository and monitor
paths explicitly. A smaller alternative is to compare every captured executable,
harness and prepared-record identity before and after each external command and
again before series acceptance, refusing observed drift. Capture the content of
dirty source changes or a build receipt as well; HEAD plus status names is not a
reconstructable dirty source identity. No extra corpus hash is needed to close
this executable-freezing gap.

A useful synthetic negative case replaces the candidate oracle/binary after
initial capture and requires either use of the unchanged captured copy or a
failed series with preserved evidence. Do not treat the ability to edit results
manually after the process exits as a separate harness security requirement.

The author's proposed experiment-owned snapshot is an adequate boundary in
principle: capture binary plus `oracle.py`, `workload.py` and `integrity.py`,
establish the captured destination identities once, and invoke only those paths.
Python resolves the oracle's sibling imports from its script directory. The
already-loaded runner/monitor functions need not be reloaded or repeatedly
hashed; preserve their source and dirty-source/build evidence for reconstruction.
A snapshot that remains immutable under the experiment's ownership rules is
sufficient for ordinary repository edits or rebuilds not to change an active
series. This paragraph approves the proposed boundary, not an implementation
that this follow-up has not yet inspected.

### Current conclusion and scope

**VERDICT: REQUEST CHANGES for F3; F1/F2 CLOSED by static review.** Targeted
adversarial source coverage for the original blockers is now adequate in design;
runtime execution remains unverified by this reviewer. The existing F4 limitation
still applies to integrated runner, deadline, termination, and resource-refusal
tests. No new population, evidence, join, summary, or exporter scope reduction is
needed to close F3.

The workload remains the full declared local flow. The upstream evidence,
representativeness, sampled resource-peak, remote/provider, and separate lifecycle
qualification limits described above still apply. Closing these code-review
findings permits controlled collection; only the actual required complete runs
can establish the performance result.

## Final follow-up: execution snapshot implemented

**VERDICT: APPROVE for controlled collection; F1/F2/F3 CLOSED.** No remaining
blocking issue was found in the scoped protocol, runner, monitor adaptation and
oracle. This final follow-up read the current implementation, tests and the
parent's existing `/tmp/docspec-comments-e2e-integration.log`. The reviewer ran
no tests, builds, hashes, installations, or corpus operations.

### Frozen execution trace

The implemented boundary matches the accepted F3 correction:

| Boundary | Current evidence | Result |
| --- | --- | --- |
| Capture exact loaded helper source | `run.py:27-50`; `integrity.py:39-43` | Workload, integrity and monitor are compiled from captured source bytes. Those bytes are retained in `LOADED_SOURCES`; the running driver does not later import changed worktree versions. The runner source is captured at startup. |
| Copy selected subprocess artifacts | `run.py:173-205`; `integrity.py:17-36,46-69` | Binary, oracle, protocol, lockfile, prepared record and build receipt are captured once. Writes are exclusive; copied bytes are hashed while writing and flushed. Observed source mutation/replacement refuses. Saved loaded source comes from the retained byte strings. Files and the final directory are read-only. |
| Keep failed capture evidence | `run.py:206-212` | A capture exception writes failure/timing metadata and prevents timed runs. Partial copies remain available for diagnosis. |
| Bind preparation and source evidence | `run.py:198-203,337-345` | The prepared snapshot must equal the already validated record. The dirty Git diff and source status/commit are retained alongside the producer build receipt and captured identities. |
| Execute captured code only | `run.py:247,266-269,288,347-350` | Every production engine command uses the captured binary; the oracle subprocess uses the captured `oracle.py`. The explicit path is supplied by `execute`, so the test-only optional oracle override is not exposed by the CLI. `-B` avoids bytecode writes to the read-only directory. |
| Use captured sibling imports | `oracle.py:20-22`; `run.py:182-194` | The captured oracle's script directory contains captured `workload.py` and `integrity.py`. The helpers used by the oracle do not read the workload module's repository-relative preparation paths. |

The snapshot is an adequate protection against ordinary shared-worktree edits
and rebuilds during the series. It is intentionally not a security boundary
against arbitrary writers. Interpreter/package identities remain recorded and
the selected environment must stay unchanged. Source quiescence during initial
startup/capture remains the protocol's operator condition.

The producer's build receipt is preserved rather than parsed through a new
project-owned attestation format (`run.py:195-197`). Before interpreting the
series, match its binary identity to `execution-capture.json`, as explicitly
required by `protocol.md:175-179`. The recorded dirty diff describes the selected
source state; it does not itself prove that state produced the executable. This
is an explicit evidence obligation, not a reason to add repeated binary or corpus
hashes to every timed stage.

### Verification evidence inspected

`test_captured_subprocesses_survive_replaced_originals`
(`test_workload.py:25-48`) captures an executable and oracle/sibling sources,
replaces every original, then invokes the captured paths and requires the original
behavior. `test_capture_refuses_observed_source_mutation`
(`test_workload.py:50-68`) changes the actual source during the final stat check,
requires refusal, and confirms the failed copy remains. These tests directly
address F3's substitution and interrupted-capture cases.

`test_integration.py:20-68` supplies tiny full-width Comments, real workload field
names for Dockets, and synthetic nested receipts. It includes repeated Comments,
null and unmatched docket keys, UTF-8 text, null/empty values and nested order.
The test calls the real runner stages and independent oracle. Its explicit
wrapper sets `oracle.ROWS = 5` only for that subprocess; production
`workload.py:16` remains `ROWS = 10_000_000`, and the production CLI still validates
the frozen input and requires the full acceptance series. The test asserts the
joined match/unmatched counts and that its per-run result has no series acceptance
flag. It exercises `run_one`, not the full `execute` preparation/capture entry.

The parent log records the complete synthetic path, the negative oracle/schema
cases and the freeze helper cases as passing, ending with `Ran 10 tests` and
`OK`. That is inspected parent-generated runtime evidence, not a test run by this
reviewer. The oracle now supplies `enable_external_access` when creating DuckDB
(`oracle.py:115`), matching the initialization correction discovered by that
execution. No corpus/performance claim follows from the tiny fixture timings.

F4 is narrowed: the complete CLI-to-oracle path and the identified false-positive
cases now have relevant synthetic runtime evidence. Injected deadline,
termination, resource-refusal and full `execute` capture/series cases remain a
coverage limit. Their current control flow is fail-closed by the static traces;
that limitation is not a new blocker to the bounded controlled collection.

### Final scope

The reviewed harness still measures full-width retained Comments through
admission, shaping, real docket join, summary, native contributor evidence,
native/flat outputs, catalog selection and exact independent readback. The fixes
add the missing independent custody/schema checks and freeze the code used; they
do not reduce that workload. All earlier upstream-evidence, sampled-resource,
representativeness, remote/provider and separate lifecycle qualifications remain
explicit limits. Three qualifying full fresh-workspace runs are still required
before the local performance acceptance flag can be treated as measured evidence.
