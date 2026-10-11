# Independent detach/register capability review

**Static verdict: approve** `/tmp/docspec-detach-register-probe.py`, SHA256 `fed633f9080e8b7d3c1fb99422d61584220b009c45a0e32ca9417370425427c7`, with its frozen protocol. No actionable finding. No execution or provider request.

Traced against installed maintained PyIceberg0.12.0 source: REST drop_table forwards purgeRequested=False; register_table posts the exact saved metadata URI with overwrite=False; commit_table uses the original Table.name() and the supplied AssertTableUUID/property update. Public Tenacity retry_with(stop_after_attempt(1)) applies to the decorated operation methods and preserves native serialization/auth. The stale commit needs actual HTTP404 from the maintained exception's chained HTTP response; a generic exception/conflict/auth refusal does not pass.

Fresh canonical UUID and definitive namespace absence prevent touching existing tables. One empty native candidate uses the owned catalog-storage-profile prefix, nonpurging/deletion-disabled properties and exact schema/URI/UUID/native parsed-metadata digest. After detachment, old registration must404; new registration and fresh-client readback must preserve the exact metadata identity after the rejected stale commit. This answers the duplicateUUID-registration prerequisite without changing the retained original evidence. Config/token/provider bodies never enter the receipt; only exception class/status and exact synthetic identities are recorded. Step records precede effects; unexpected or ambiguous outcomes terminate without another mutation/identity.25s step/240s total deadlines plus root supervision bound the probe; construction/config/auth remain maintained behavior, not a claim of exactly one physical HTTP request for every constructor.

Scope remains fresh empty-table nonpurging detach/register and stale old-identifier commit rejection. Successful current readback demonstrates metadata survival for this object; it is not a provider-wide purge guarantee, populated Engine restore, obsolete-credential fencing, old-identifier reuse, or deletion qualification. Synthetic registration/metadata remain retained; no automatic cleanup is introduced. Runtime receipt belongs to root.

## Actual provider result and reconciliation

The [frozen probe](detach-register-provider-2026-10-11/receipt.json) failed its
specific404 assertion: Basin returned403 to the old-identifier commit. Exact
nonpurging detachment and registration at the new name had already succeeded.
A separate [read-only reconciliation](detach-register-reconciled-2026-10-11/receipt.json)
confirmed the old registration remains absent and the new registration retains
the exact UUID, URI and native metadata hash. Current credentials can read the
restored registration; the cause of the commit-specific403 is not established.
No mutation was repeated and no object was deleted. This proves the necessary
registration transition for this empty candidate, not full Engine restore.
