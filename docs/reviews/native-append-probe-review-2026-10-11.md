# Native append probe — independent static review

**Verdict: APPROVE for one fresh isolated provider run after successful compilation.** Both initial readback findings below are corrected in the inspected source; no remaining execution blocker found. Reviewed `/tmp/docspec-native-append-probe.patch` and formatted `examples/r2_candidate_probe.rs` (SHA-256 `9cef880588571ad4d06db1a1dace0ba760d4539bd2c0f21a3e4df5a22cfbe212`). The final reviewed source SHA-256 is `727c1e773e6ffb7a160ae66d9d8c87d3feea3968f8ef5bac0c341d90b32201f1`. No build, provider calls, or source edits by this reviewer. Repository/Rust/semi-formal review guidance applied.

## Original findings and disposition

- **WARNING — exact values must check integer validity.** `examples/r2_candidate_probe.rs:647–650` calls `Int64Array::value(row)` without `is_null`. Arrow value access returns the backing value independent of validity, so a null integer with the expected backing value could satisfy `exact_values: true`. Reject `counter.is_null(row)` before comparing the tuple. The nullable text column is already checked correctly. **CLOSED:** current lines 654–658 reject a null counter before value access.
- **WARNING — stop decoding extra rows before collecting them.** The same loop appends every row and checks equality only after the stream ends (`635–665`). Refuse as soon as the next batch would take the observed count above three, before allocating strings. This makes the fixture's decoded collection bound explicit. A 64 KiB compressed-object bound does not itself bound decoded rows. **CLOSED:** current lines 637–642 reject a batch exceeding the remaining three-row collection allowance before string allocation.

## Traces and scope

| Boundary | Source | Verified behavior |
|---|---|---|
| Opt-in/root | `examples/r2_candidate_probe.rs:144–203` | Requires explicit flags and canonical qualification UUID; root is beneath the approved Basin profile; an existing namespace refuses. |
| Registration | `:215–280`; `src/storage/remote.rs:54`; `src/storage/tables.rs:128` | Existing candidate registration uses the create-only metadata path and returns the explicitly configured native FileIO. Identity/schema checks and independent registration readback remain before append. |
| Tiny writer | `:419–477` | Requires empty exact table location; writes one three-row Arrow batch under explicit `{root}/data` using maintained Parquet/rolling/unpartitioned writers and a fresh filename UUID. |
| Native append | `:493–517` | One native transaction appends the returned files; actual acknowledged table UUID/schema/location and new metadata URI are checked. |
| Inventory | `:529–611` | Scopes list/manifest paths before native reads, checks tiny object sizes, compares exact live file paths/counts with writer output, then retains object identities and a pin. |
| Independent readback | `:614–665`; `src/storage/pinned.rs:36` | Fresh configured catalog must return the acknowledged metadata; catalog-free open checks pinned metadata bytes/schema/snapshot; fresh native Arrow readback compares duplicate-sensitive and null-sensitive values after the two corrections. |
| Error disclosure | `examples/r2_candidate_probe.rs:42–78` | Top-level errors print codes; wrapped provider errors print phase/kind/retryability, not configured credentials. |

The pinned `iceberg-0.10.1` source derives manifest and manifest-list paths from the exact table location (`src/transaction/snapshot.rs:243–251`, `423–447`). Its transaction retry policy reads `commit.retry.num-retries` (`src/transaction/mod.rs:175–214`), which this candidate sets to zero and verifies survives registration. The transaction does perform a native catalog refresh before committing (`mod.rs:218–226`); a changed base can be adopted automatically. This isolated fresh-namespace probe assumes no concurrent writer and is not a production eligibility-fencing qualification.

The ordinary native FileIO writer does **not** gain conditional creation merely because the initial candidate metadata used it. Fresh UUID paths and the fresh synthetic root make this narrowly scoped experiment appropriate; do not report create-only native output qualification. Server metadata placement is observed after the catalog effect and rejected if outside the owned root, exactly as the probe comments state. No cleanup or retry loop is introduced; failure retains the namespace, objects, and emitted references.

The object identity helper caps each stored object at 64 KiB before reading (`:378–409`). The input is a fixed three-row table. The four MiB reservation is an allowance, not a measured native allocator/RSS cap. Native manifest decoding and independent readback repeat small reads deliberately for this qualification; this is not the routine Engine processing path or a performance benchmark.

**Disposition:** both guards are present in the final inspected source. After successful compilation, the proposed single fresh provider run can proceed within the stated scope. It can qualify maintained native append/placement and exact fresh readback only. Remote Engine, conditional native writes, concurrent lifecycle selection, recovery, retention, and end-to-end performance remain unqualified by this probe.
