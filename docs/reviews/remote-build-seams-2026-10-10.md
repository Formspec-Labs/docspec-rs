# Next remote Build slice: retained locations and attempt protection

Preparation only, 2026-10-10; source inspected at `474e9e6`. No repository edits,
builds or provider calls. This narrows `/tmp/docspec-remote-pin-next.md` to the
next connected implementation. Public remote Engine startup remains unavailable
until Build completes guarded selection and a fresh-host readback.

## Actual seams

| Current method/type | Relevant behavior and required change |
|---|---|
| `Engine::compose`, `Inner` (`src/engine.rs`) | `root: PathBuf` currently supplies host custody, scratch and immutable object paths. Keep this root local for worker custody and `Budget::in_workspace(root/scratch)`; add a separately validated retained root selected with `Objects`. Existing `open`/`with_catalog` stay Local. |
| `reserve_attempt` (`engine.rs:1829`) | Creates UUID `root/objects/<dataset>/<attempt>`, commits `Protection` under the checked lifecycle boundary **before** mkdir, returns `PathBuf`. Return a private concrete retained location; retain this ordering, UUID and exact trailing-slash prefix. Remote reservation commits protection before any object/catalog effect and performs no remote mkdir. |
| `Protection`, `ObjectRef`, `PhaseResult`, `PhaseDependencies` (`src/model.rs`) | Paths are already strings and can retain exact URIs. Keep owner/digest/bytes and original phase dependencies unchanged. A pending destination is not a qualified `PhaseResult.outputs` or admitted input. |
| `protect_admission` (`src/engine/admission.rs:260`) | Canonicalizes managed input filesystem paths beneath `root/objects`; replace only this locator branch for remote URIs with strict workspace/owner/attempt parsing. Reuse `qualified_object`, then the existing owner-first grants and request binding. |
| `prepare_source` (`engine.rs:757`) | Reuses processing/admission before opening inputs. New inputs use filesystem size, local free-space estimate, `reserve_attempt`, `copy_verified`. Remote branch uploads a local source once with `RemoteObjects::upload_file`, using trusted digest plus source length and pre-retained destination; reuse captured managed identities without upload/hash. Remote output capacity must not be charged as host disk. |
| `check_object_owner` (`retention/owners.rs:728`) / `inventory::validate_object` (`inventory.rs:1808`) | Assume absolute normalized filesystem paths. Add strict S3 URI spelling and owner/attempt descendant checks against the configured retained root and the **exact committed** `protection/<attempt>` prefix. Preserve exact grants, lifecycle table UUID/revision, identity conflicts and retirement rules. |
| `process_build`, `register_output`, `create_output` (`src/engine/write.rs`) | Query admitted Parquet URI; metadata builders take `&Path`; candidate registration still selects Local and generates identifier inside the helper. Use retained location strings at native metadata creation, preserve local Path wrappers, and retain candidate identifier + exact `MetadataLocation` before its create/registration effects. |
| `phase`, release/readback (`engine.rs:1772`, `:1613`) | Phase reuse stats local files; release uses local `write_new`, `inputs::read_manifest`. Dispatch availability and bounded manifest create/read through configured object I/O. Hash and decode the same admitted metadata buffer once. Keep lifecycle completion/selection commits and the independent readback equality check. |
| `read_group`, evidence reads (`src/engine/read.rs`, `evidence.rs`) | Fresh-host reads must use retained native pins through the configured FileIO and selected lifecycle records, without producer files or a local authority copy. Existing free local helpers need not imply remote support. |
| `Lifecycle::open/commit/rows` (`src/catalog.rs`) | Maintained catalog owns lifecycle table placement and FileIO. Keep those catalog-owned reads/writes and guarded revisions; do not rebind lifecycle tables to candidate prefix I/O. `RemoteStorage` already resolves catalog/object configuration together. |

## Smallest coherent implementation

1. **Separate locations, retain the existing authority.** Use one private concrete
   `RetainedLocation::{Local(PathBuf), Remote(String)}` for immutable destinations
   and checked child names, not a generic filesystem/backend trait. `Inner.root`
   remains host-local. The remote retained root is a caller-owned isolated workspace
   beneath the Basin storage profile, for example
   `s3://<bucket>/__r2_data_catalog/docspec-qualification/<owner>/objects`.
   Dataset/attempt descendants become durable protection paths. URI parsing must
   reject aliases, encoded separators, dot components and cross-prefix escapes;
   never pass remote URIs through canonicalize, mkdir or unlink.

2. **Make the pending physical attempt recoverable before effects.** Current
   `Protection` stores a prefix and purpose but does not bind an operation to its
   exact upload destination/candidate identifier/metadata URI. Do not infer an
   attempt from matching purpose or scan for the latest UUID. Add one small private
   operation/phase-generation attempt-intent record in the existing lifecycle,
   committed with its `Protection`, binding request digest, eligibility, attempt ID,
   exact prefix and expected identities/destinations. Resume that exact intent;
   record a new physical attempt only through an explicit guarded replacement.
   Keep pending identities distinct from qualified admissions. No new registry,
   separate database, mutable latest object or selection authority is needed.

   Source upload identities are available from trusted input digest, local length
   and reserved URI before transfer. For candidate metadata, the existing shared
   helper currently serializes and writes inside one worker. If crash recovery
   needs the exact metadata digest before create, factor that producing stage to
   return its bounded bytes/identity with reservation intact, retain the intent,
   then create those same bytes. Never serialize/hash twice or commit lifecycle
   records from inside a worker while holding the only blocking slot. This is a
   concrete required seam, not an extra retry framework. A completed phase always
   wins before reopening the original input. An incomplete/unknown upload or
   registration uses its retained identity/identifier/URI for explicit reconciliation;
   it must not automatically retransmit or choose a fresh destination.

3. **Connect admitted input, native output and query I/O together.** Reuse
   `upload_file`'s inline digest/length check and worker lifetime. Preserve known
   managed input owners through `protect_objects`/`OwnerProtectionRef` and
   `ReferenceBinding`; reject unqualified external remote inputs before effects.
   Register a maintained S3 object store once in the shared DataFusion runtime
   from the same resolved startup config, with explicit credentials and no ambient
   discovery. `FileIO` alone does not register it. DataFusion 53.1 reexports
   `object_store`, but disables its default features; enabling the matching
   `object_store` AWS feature under `remote` is a real dependency-feature prerequisite.
   Use maintained `AmazonS3Builder` and `RuntimeEnv::register_object_store`, not
   a custom forwarding adapter or local full-payload staging copy.

   Native metadata builders need a string-location core with the current local
   Path APIs delegating to it. Configure `register_output` with retained identifier
   and metadata URI; keep native `write_batches`/`DefaultLocationGenerator`, append,
   bound pinning and inspection. Validate initial and later metadata/manifests
   against the protected attempt location **before effects**, not only after append.
   Basin may own lifecycle placement; that does not authorize candidate writes
   outside their attempt. A successful initial candidate probe does not establish
   later native append placement.

4. **Complete Build before exposing startup.** Route phase availability, release
   JSON create/read, selected-result opens, bounded manifest decoding and native
   pin reads through the same configured objects. Reuse the existing `run_admitted`
   completion/validation/readback/`select_prepared` flow and lifecycle CAS. Add
   remote startup only after that connected path passes; declare its initial
   supported Build shapes and refuse unsupported operations before admission/effects.
   Keep remote restore, expiration and removal unavailable: their current code uses
   filesystem roots/unlink, and URI support in inventory is not deletion authority.

**Read permission is not protection ownership.** `RemoteObjects` scope permits
transport reads/writes inside the isolated workspace. New objects must additionally
be descendants of an active owner attempt; cross-dataset reuse needs captured exact
owner grants and consumer bindings. Neither matching bucket/prefix nor a readable
object grants lifecycle ownership. Same-workspace captured inputs may keep their
original owner; cross-workspace admission is outside this slice.

## Concrete checks and qualification

- Preserve local input alias/symlink refusals, exact attempt-prefix ownership,
  inherited owner/grant checks, generation fences and existing resume fixtures.
- Refuse malformed/off-scope URI, missing/wrong owner reservation, conflicting
  identities and unsupported operations before object effects. A fault immediately
  after reservation leaves its exact protection and intent durable.
- Tiny local Parquet input -> remote admission -> native output -> validation ->
  release JSON -> guarded selection. Check exact values (include duplicate/null
  rows), source/receipt links, native schema IDs, inventories and owners.
- Stop/restart after admission, processing, validation and readback; remove the
  producer input once admission completes. Assert completed upload/hash/compute
  work does not repeat and same-ID changed-body refuses.
- Simulate lost upload and catalog responses, caller cancellation, changed source
  and conditional conflicts. Reconcile only the retained destination/identifier;
  one-worker budgets must not nest workers or release admission early.
- Fresh Engine host directory using the same remote catalog and isolated prefix
  must resolve selected lifecycle state, read the release and native pins, and
  reproduce the tiny output without producer-local objects. Another dataset's
  attempt must not become owned merely because transport scope includes it.
- Run focused tests and the repository gate, then one tiny isolated Basin/R2
  provider qualification after review. Retain failed/unknown prefixes and receipts;
  no implicit cleanup. Existing catalog/object/candidate probes remain separate
  evidence and do not qualify this complete Build path.
