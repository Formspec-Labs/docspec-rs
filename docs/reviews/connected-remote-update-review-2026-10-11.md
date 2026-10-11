# Connected remote source Update review

Verdict: APPROVE for parent-owned execution. No blocking static finding remains.

Frozen artifact: `/tmp/docspec-connected-remote-update.patch`, SHA256 `c63e8e35ad950532f19759ebc7e0c587e6ef7c4a6ca4496f94b36f77bd400d70`. Sources: `/tmp/docspec-connected-remote-update/new`. This is a static review, not a compilation, provider or performance result. No shared source edits, Cargo, formatter or provider calls were performed by this reviewer.

## Reviewed boundary and trace

The accepted recovery decision is `/tmp/docspec-remote-update-recovery-simplification.md`: existing durable PhaseResult is the reuse boundary; an unacknowledged Update may use a new protected attempt, leaving partial/losing objects retained. This review does not require a new multi-output recovery record or stable role identifiers.

- `engine.rs:303–324` admits only the selected independent source-only Build/Update shape. The same shape is checked before preparation and again after fresh authority lookup at final selection. Completed results return first, preserving exact older operation acknowledgment. `request_view` uses existing-only remote loading for Update; Local retains its existing open behavior. Plan/run/prepare/error reconciliation use that helper. The earlier absent-dataset creation finding is closed by this change and network-free plan/update assertions.
- `engine/write.rs:169–182,276` routes candidate creation, origin adaptation and key replacement through the injected Objects and private Location. Fresh attempt protection is acquired before output creation. Existing schema normalization, key IDs, overlapping replacement/deletion refusal and no-op handling remain. Unchanged files retain their ObjectRef identities. `storage/tables.rs:48,98` preserves native schema/high-water properties and constructs candidates using the same explicit remote configuration; no ambient client is introduced.
- Shared `storage::append` still uses the existing GuardedCatalog. Native refresh must match exact candidate metadata and URI; there is no blind append retry or mixed-role completion adoption. Completed processing lookup occurs before the processing input query or new native writers. Failure before phase acknowledgment leaves the attempt retained and may perform new work on a later fresh attempt, as explicitly accepted.
- `engine/write/verification.rs` and `engine/changes.rs` use configured exact native pin opens. `engine/changes/certificates.rs:288` publishes bounded applied-change metadata under the protected location with the existing immutable publisher. Public remote change-certificate/maintenance capabilities are not broadened.
- `engine/retention/inventory.rs:32,1696` uses configured immutable DatasetPin reads for parent expansion. Strict decoding and exact object identity checks remain. Already-complete TablePins satisfy native inventory; unsupported remote native-only, scalar-origin, Changes and Restore expansions refuse before Local I/O. Source Update parent lineage remains full protection rather than receiving the full-Build ancestry narrowing.

## Test review and evidence limits

The shared Local/provider scenario in `engine/remote_build_provider_tests/updates.rs` creates actual public-service states, not synthetic lifecycle replacements. Its literal oracle preserves duplicates, nulls and empty strings; field names/native IDs/types/nullability are checked independently. First Update replaces a duplicated key and deletes another, then stops after durable processing. Removing only external producer files leaves authoritative admitted inputs intact. Retry must retain the exact PhaseResult revision and all pin fields, and must not add namespace table registrations. The second Update requires a genuinely shared immutable data file and reused-file/byte metrics. Cold reads cover original/current source and an authorized reordered flat export omitting the private canary field. Completed stale-base operations are acknowledged exactly.

The ignored provider case uses explicit configuration, a canonical fresh UUID namespace/prefix, public with_remote, a fresh host for readback, bounded tiny literal rows and safe error-code reporting. Remote objects are retained. `fresh_host_initial_build_queries` measures only the existing initial-Build query counter; request counts are explicitly unmeasured. Reported retained bytes cover observed pins, not unpinned attempts/catalog history/host peaks.

This fixture does not inject unknown native commit responses or lost lifecycle acknowledgments. The processing stop is after a durable phase acknowledgment. Guarded native error behavior and lifecycle reconciliation are statically traced and existing shared tests remain required; this provider run cannot be reported as qualification of every fault boundary. Large/nested types, throughput, cross-host conflicts and remote destructive maintenance remain outside the measured slice. Parent should run Local scenario/startup refusals and full repository gates before the optional provider scenario, with an external deadline and no automatic fresh-prefix retry after unknown outcomes.

## Findings closed during review

1. Remote Update on an absent dataset could create lifecycle state before refusing. Fixed with request_view and absent namespace tests.
2. Provider output called the initial-Build-only counter `fresh_host_queries`. Renamed to `fresh_host_initial_build_queries` without changing the measurement.

No additional production or test changes requested. Runtime approval remains conditional on parent-owned focused/full checks and any separately authorized provider execution.
