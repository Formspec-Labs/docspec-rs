# Connected remote maintenance: independent static review

Verdict: **APPROVE the connected production changes after the final guards.**
Reviewed against the applied, formatted shared tree. No builds, tests or provider
calls performed by reviewer; this is not provider qualification.

Reviewed exact artifacts:

- Namespace routing: `70b2e1f1eb65ba9e219a6c4e4ebf70f91dec8367b5f365fd7e9908526025d1f9`.
- Restore/inventory core: `6e9f3768d1e4247712b7b5ce331cdf14534f706544f8a3abe51b519349572e9a`.
- Restore follow-up: `cad10d0f805f7a8510785b02d74dccd176348d9a20268ca52f388e30542f20da`.
- Image guard: `3e6d9f3a76297dd3d6a6a945f10be75c7eaa6551281299efa415529043fd758e`.
- Cumulative image/reservation/path guards: `01b10b2f8fdb65a53f57e9cb76be92f023ede0991503f840e9bd79d4a6e0d1aa`.
- Removal/transport/test interruption: `8e7d8004418ea8c5898a8f2b08def0c346274a50e77af343ba3e8aa8cbd3f27c`.

## Exact boundaries traced

Quiescent mode requires an explicit canonical namespace generation; it does not
set owned_catalog or enable embedded SQLite restore. Normal Build/Update/export
work refuses in maintenance mode. Logical lifecycle identities remain stable;
native Tables and commits retain physical identities. Generation-scoped catalog
enumeration feeds the existing all-root inventory. Known origins, declarations,
changes and restore roots use configured Objects, and maintained native inventory
binds native reads to that backend. Unknown lifecycle families still refuse.
Full roots continue to win over interpretation-only edges.

Removal retains existing creation/protection/current-root checks, immutable
intent and unavailable markers before detachment/effects. The preparation route
never deletes objects. Resume refuses unfinished registration detachment and
requires the exact saved intent ObjectRefs and generation, not arbitrary preview
objects. Configured identity GET checks bytes through the normal reader; DELETE
uses only the explicit temporary actor. Unknown deletion remains unresolved;
authoritative HEAD404 reconciles absence without another GET/DELETE. Transport
and auth errors cannot mean absent. Existing owned worker/admission lifetime
survives caller cancellation, and local/host-stage effects remain separate.

Restore protects one portable catalog image and immutable URI closure instead
of copying existing payloads. It records the exact target intent before old
registrations disappear, binds original image/current serving/target generations
and import mode, detaches data before lifecycle authority, and reconciles
registration response loss only against the exact saved URI/UUID/revision.
Surviving unavailable markers reject closure intersection before effects. Active
retirement wins; explicit import applies only when current authority is absent.
A completed target retry requires its exact retained intent and completion.
The point is reprotected in the restored lifecycle. Original points can be reused
from a later serving generation without changing the original artifact.

## Findings and resolution

The first restore draft checked only URI prefix equality, allowing a different
configured object endpoint to serve the same URI. Follow-up checks placement and
exact owned root before target effects, while permitting credential rotation.
It also replaces repeated full hashing of live protected payloads with stat-only
availability; explicit import without a surviving promise remains a separate
identity-verification boundary. Native expansion now reserves maximum decoded
metadata regardless of filename, matching magic-based gzip decoding. No duplicate
GET or schema-reconstruction path was added for those fixes.

The first image validator could omit lifecycle authority and let fresh.open
create an empty table after detachment. The final guard requires the point owner
namespace, each namespace's exact lifecycle registration, and metadata-owner
namespace agreement before any target intent/effect. Tiny malformed-image cases
exercise those refusals. The native intent fixture establishes exact cold intent
recovery/retarget refusal and original metadata registration; localhost removal
fixtures establish transport reconciliation. Neither alone proves the complete
provider lifecycle.

## Assurance limits

Startup quiescence is an explicit operator assertion, not distributed exclusion
or credential revocation. Expiry cannot stop an already accepted DELETE; safety
requires drained actors and permanently unavailable/nonreused exact keys. The
Engine process can still hold ordinary read/catalog credentials, and its identity
worker receives the normal reader alongside the temporary actor. Supported DELETE
methods exclusively use the temporary operator; this is not hostile-process
credential isolation. Make that last distinction explicit in operator docs.

Catalog-loss restoration assumes retained immutable objects remain available;
it is not object-loss backup, automatic online failover, a namespace sweep or
remote payload recreation. Real provider role policy, obsolete commit refusal,
response-loss/interruption and cold readback remain root-owned qualification.

Final closure: arbitrary point/catalog parents could derive an unrelated
Protection ID during explicit import. The cumulative guard requires the exact
format2 producer `restore_<canonical UUID simple>/{point,catalog}.json` directory.
`check_restore_reservation` admits the exact protection row, checks its native
revision, and compares the complete active id/consumer/purpose/path binding before
both capture publication and restored-target completion. `remote_completion`
retires that proven reservation. Missing, changed, retired or differently located
reservations refuse; no unrelated protection is overwritten. The native fixture
now starts with the real reservation and tests changed promises and nested paths.

Root's final narrow allocation changes preserve those boundaries:
`RemovalActor::Remote` shares an `Arc<RemoteRemoval>`; `RemoteRemoval.exact` shares
its immutable `Arc<BTreeMap<...>>`. Generation, expiry, exact-set comparison and
per-object equality still run before the same temporary effect, while admitted
workers retain their authority without copying the full map for each deletion.
`CandidateWriter::Remote` boxes the same configured RemoteObjects and moves it
into the same bound native/create paths; URI, compression and scope checks remain
unchanged. These are allocation/layout changes, with no new authority or effect.
No actionable static blocker remains in this connected slice.

Runtime fixture correction (root-reported failure): the native SQL catalog
retains its namespace properties, including `exists=true`; the synthetic empty
namespace map therefore failed the intended exact comparison. Reviewed supplement
`7a4432cff2c919bd2b768bf589b3eb24a6873eedec94d0211b9039b74951c663`
captures `source.get_namespace(...).properties()` in the fixture, matching the
production capture path. This preserves exact property validation and changes no
production behavior. Runtime rerun remains root-owned.

## Restore rollback fencing closure

Static APPROVE of frozen epoch repair
`6535159e4d88c85c0e7f2b94613b6dc62121965387f087311ed192a52d4e17e4`
against the applied/formatted source. The earlier review missed a real rollback
hazard: an older lifecycle image could forget newer unavailable markers outside
its object closure while retaining an unfinished operation's exact destination.
A late destructive actor could then target recreated bytes. Closure intersection
checks alone were insufficient; that prior assurance is superseded here.

Each captured lifecycle registration now retains its exact Authority epoch and
content digest. Before restore completion, every captured lifecycle advances its
epoch by one through native CAS. Build/Update preparation and unfinished export
check their captured operation epoch before object effects; source, candidate and
release intents bind operation/native revision, and fresh operations allocate new
attempt destinations. Completed result/export acknowledgments remain first and
material phase keys remain unchanged. A cold retry adopts only the same table UUID
at captured revision+1 with solely the Authority row advanced, epoch+1, and all
other Authority content matching the captured digest. Other native mutations
refuse. Current removal journals must finish before detachment; explicit startup
quiescence and drained destructive credentials remain required.

The real Engine fixture captures a pending source reservation, uploads then
actually deletes its localhost object, restores the captured native registration,
loses the fence acknowledgment, and cold-reconciles the fence. Its old producer
remains available; the old request refuses, no request follows DELETE, and a new
operation obtains a distinct destination. This qualifies the pre-effect route
without claiming hosted catalog restoration. The default-authority native intent
fixture also checks idempotent fencing and retained value/reservation completion.

Existing provider maintenance fixture creates its point with the current capture
service and does not assert the restored epoch remains zero; its later explicit
fence derives the current epoch, so no concrete fixture incompatibility found.
Previously produced format2 images lacking lifecycle Authority proof safely
refuse the strengthened image validator; they cannot receive this new safety
assurance retroactively. No provider run or build performed by reviewer.

Root runtime closure: `remote-maintenance-epoch-regression-2026-10-11.log` passes saved target-intent/fence reconciliation and an actual pending source upload followed by deletion and old-attempt refusal with no further object I/O. `remote-maintenance-focused-checks-2026-10-11.log` completes the affected integration cases, maintained plain/gzip reader checks, restore, worker and doc targets. `remote-maintenance-final-gate-2026-10-11.log` passes formatting, strict all-target/all-feature Clippy and the local/mock remote startup/admission fixtures. Previously passing full library/integration cases were not repeated. Provider qualification is recorded separately.
