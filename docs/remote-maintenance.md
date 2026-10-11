# Remote maintenance

Use this procedure for an explicitly owned R2/Basin namespace generation. It
supports independent source datasets and primitive source exports. Local
processing capabilities and [qualification limits](STATUS.md) remain separate.

Configure `docspec.namespace-generation` in `remote.json` as a canonical UUID
when creating the dataset. Keep this generation stable during normal operation.
Before maintenance, stop and drain every client using that generation and its
readers. Start the maintenance client with the same configuration plus
`docspec.maintenance-mode: quiescent`. This is an operator declaration, not a
distributed lock. The maintenance client refuses new processing and exports.
Keep provider expiration and compaction disabled.

## Remove an obsolete object

Retire the exact recovery or availability promise first. Selected versions,
active exports, readers and live restore points keep their independent protection.
Use `plan-removal` with exact known object references; inspect the returned plan.
`prepare-remote-removal --plan removal-plan.json` rechecks eligibility, reserves
those keys as permanently unavailable and detaches required native registrations
without deleting objects. Finish any unresolved registration outcomes before
issuing deletion credentials.

A trusted issuer creates a temporary S3 session permitting only `HeadObject` and
`DeleteObject` on the reserved keys, with an expiry within fifteen minutes. Pass
its three values through named environment variables in `RemoteRemovalConfig`;
do not place credentials in the authority JSON. The description binds the exact
objects, generation, expiry, owned prefix and S3 connection properties. Then run
`resume-remote-removal --dataset DATASET --removal ID --authority authority.json`.
The HTTP equivalents are `POST /retention/removal/remote/prepare` and
`POST /retention/removal/remote/{dataset}/{removal}/resume`.

The existing removal record is the recovery authority. If the response is lost,
resume that same removal. An absent key completes the outcome without another
payload read or deletion. Never recreate deleted keys. Expiry prevents new
requests; it does not cancel effects the provider already accepted. The deletion
adapter uses only that temporary session. The Engine controller
retains separate catalog and identity-read credentials in the same process; this
is not isolation against hostile code. An independently running deletion client
must receive only the temporary session and cannot be allowed to mint new ones.

## Restore catalog registrations

Create a restore point while the generation is quiescent and retain its exact
`RestorePointRef` outside that catalog. The point records native registrations
and protects the immutable objects they need. It copies no dataset payload and
provides catalog-loss recovery; it is not a backup against loss of those objects.
Unfinished removal decisions must be reconciled before capture or restore. Unfinished work
that depends on a host-local stage refuses portable capture.

Restore uses the same object store and retained prefix with a fresh namespace
generation. Stop old clients and drain or revoke outstanding destructive
authority first. The engine records the intended point in the fresh namespace
before detaching old registrations, then registers the saved metadata without
copying payloads. A crash resumes from that exact point and target generation.
Use `--remote source.json restore-remote point.json target.json`; both configs
must declare quiescent maintenance. The HTTP `POST /restore-remote` body contains
`point`, `target_owned_prefix`, `target_properties`, `target_secret_env` and
`allow_unregistered_import`. Set the last field only for explicit catalog-loss
recovery when current point authority is absent; it never overrides a known
retirement.

Restore advances the operation epoch in every captured dataset. Unfinished old
operations refuse before new writes, so a restored attempt cannot recreate a key
deleted after the point was captured. Completed acknowledgments remain usable;
new operations can reuse retained qualified work. A lost response reconciles the
exact saved target and its epoch changes.

Only after success should normal clients start with the returned generation;
remove the maintenance-mode setting for normal work. Old physical table names
are never reused. Recovery after the last completed point is not promised.

`fence-operations DATASET EXPECTED_EPOCH` separately prevents operations admitted
under an older epoch from publishing, while preserving selected versions and
completed work. Retry the same expected epoch after a lost response. This is a
publication fence; it does not establish quiescence or revoke credentials.
