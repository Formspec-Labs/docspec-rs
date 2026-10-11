# Full Refresh result lifetime review

VERDICT: APPROVE statically for `/tmp/docspec-related-result-lifetime.patch`, SHA256 `4b6bb234338c0e6c3e77a156b34dda081b4de37113c575ab5e5fa8bf991f8794`. Reviewed recorded preimage/proposed source and actual callers/callees. No Cargo, provider calls, formatting or shared edits.

## Protection and recovery

The new readback branch applies only to a based Full Refresh without receipt evidence. It creates the exact release through existing verified release creation, then computes and acquires exact result protection before taking the consumer commit lock (`engine.rs` changed readback branch). Existing `protect_releases` walks exact references with bounded metadata admission; `protect_objects` validates/deduplicates identities, acquires each foreign owner under its owner lock, and `acquire_owner` checks scope, availability and retained creation evidence with existing lost-commit-response reconciliation (`retention/owners.rs:546–724`). No ownership is inferred from a URI or caller-declared inventory alone.

Execution/admission/prepared phase bindings are not rewritten or discarded. Only the new readback's protection set changes from inherited broad execution scope to exact result closure. Owner acquisition precedes consumer acknowledgment; an interrupted or losing handoff can retain an extra grant, rather than leave a dangling acknowledged reference. The fresh consumer view still checks operation completion before commit and adopts an already-completed winner's own protections. Existing admitted worker lifetime excludes concurrent same-host retirement/removal effects. No owner lock is acquired while holding the consumer commit lock. Existing readback and completed result paths perform no new acquisition.

## Exact graph boundary

Interpretation ancestry is broadened only to based Full Refresh with actual related data and no receipt evidence, beside the existing full Build case. Current source/group tables, current related coverage (including coupled members), physical input captures, current contributor evidence, current scalar origins and inline qualifications remain full payload roots. Incremental Refresh and receipt-bearing results keep their existing full closure.

Historical related interpretations now explicitly retain old producer owning manifests, inline coupled membership manifests, captured native/logical schema metadata and old contributor-table schema metadata. Full Refresh verification checks the parent DatasetPin's version/boundary/relationship/roles and scalar definitions/history (`backfill/verification.rs:159–198,336–356,488–530`); it does not reconstruct the previous join payload. Current deep related verification reconstructs from current producer coverage/captures and current contributor relation (`refresh.rs:833–895`). Original scalar qualification edges still retain original execution payloads. Seal/result/recovery/grant stronger roots and monotonic full-strength upgrades are unchanged, so interpretation traversal cannot override an independent payload promise.

## Real fixture

The new fixture uses real public producer Builds and two Full Refresh operations. New sources have literal duplicate keys [3,3,4]; expected values [3,3,3,3,4] and five exact occurrence pairs are independently enumerated, including known native file identities. It checks actual new readback owner grants exclude the chosen old payload while preserving its old owning manifest metadata. Early old-grant retirement correctly refuses grant_in_use. Explicit old producer/consumer availability plus old/current execution recovery retirement does not itself remove the old owner-grant blocker. Only exact grant retirement allows public exact-object removal of one obsolete Parquet file.

Cold reopen checks completed Refresh acknowledgment after external replacement input removal, literal values, exact contributor pairs, deep release verification and removal reconciliation. Old producer/consumer manifests and native contributor/schema metadata remain present and protected. Existing public helper reuse supplies real retirement/creation proof; no synthetic authority records or broad deletion are introduced.

## Limits

Tiny local independent-producer Full Refresh qualification only. No remote deletion, current unused coupled-member payload reclamation, arbitrary Incremental Refresh or receipt-bearing output claim. Added protection capture costs one bounded metadata graph walk and can leave losing grants for ordinary explicit retirement; no throughput gain is asserted. Focused runtime remains parent-owned. No actionable static finding.
