# Connected release recovery review

Verdict: **APPROVE for the scoped initial release path**, subject to parent-owned compilation, local regression tests and provider qualification. Static review of `/tmp/docspec-remote-release.patch`, final reviewed digest `e8d4f2e12871bab78aab1a22f9758b15518567820c88fd0a3835495c2e8a6350`. References below are to proposed source under `/tmp/docspec-remote-release/`. No builds, provider calls or implementation edits by this reviewer.

## Findings closed

- Release validation initially decoded operation/request/protection records while admitting only the small pending record. `src/engine/locations/release.rs:26–33` now reserves those dependencies before decoding. Creation and operation-recovery callers pass the budget; inventory uses its existing decoded-snapshot allowance. The pending record remains admitted separately.
- Factoring the common atomic publisher initially added a second parent-directory sync for every candidate. `src/storage/tables.rs:237–266` now performs publication durability once in `publish_metadata_new`; the candidate wrapper only adds exact-byte verification on collision. A resumed existing release verifies bytes and syncs its parent before acknowledgement.

## State and I/O trace

`prepare_and_select` checks completed readback before invoking `Engine::create_release` (`src/engine.rs`, replaced release branch). Existing completion therefore avoids another release serialization, PUT or independent decode. The new test's counter checks this boundary and keeps retained input available while removing only the original producer.

`reserve_release` (`locations/release.rs:107`) derives a key from operation revision and the exact compact `DatasetPin` digest. It stores only the bounded `ObjectRef` and identity/protection bindings, not another dataset or JSON history copy. It commits matching intent and prefix protection before creation; conflict or lost acknowledgement adopts only the same key after checking current authority and exact bindings. Different assembled material after explicit retirement obtains a different candidate; unchanged material preserves its URI.

`create_release` (`:54`) checks a pending destination before rebuilding bytes. Only first-open/first-HEAD NotFound means absence. A present object is read, hashed and decoded using one admitted buffer, then compared with the prepared dataset. Definitive absence permits serialization and same-destination conditional creation only after its size/digest match the retained identity. Conflict or uncertain write outcome receives one exact readback; no automatic upload loop exists. Corrupt bytes and permission/transport errors never become absence.

`Objects::create_release_metadata_admitted` selects the existing create-only remote client or shared local synced-temporary/no-clobber hard-link publisher. `inputs::read_manifest_if_present` shares the existing bounded reader rather than adding a second verifier. Owned memory travels into workers, so caller cancellation does not release active serialization/read/write admission. This is conservative admission accounting, not an RSS or latency measurement.

Retirement and operation-recovery inventory recognize `release-attempt/`. It is not added to qualified creation evidence or used to grant object ownership. Actual release completion remains the evidence boundary. The release object has no independent mutable latest pointer; catalog selection stays authoritative.

## Tests and limits

The proposed local Engine fixture interrupts after physical publication, confirms absent durable readback, refuses corrupted retained bytes without replacement, resumes the exact URI, and verifies subsequent completed retries do not decode or execute SQL again. The remote fixture proves initial HEAD404 versus HEAD403 handling with no GET or PUT. These are proposed tests, not execution evidence from this reviewer.

There is no new fault-injected Engine test for lost release-intent commit acknowledgement, concurrent conditional release creation, or cancellation of this specific release wrapper. Those paths are statically traced and reuse earlier primitives. Parent's planned private Engine provider run supplies actual remote inspection/read/release dispatch evidence; full remote lifecycle, destructive maintenance, restore and public remote startup remain outside this approval.
