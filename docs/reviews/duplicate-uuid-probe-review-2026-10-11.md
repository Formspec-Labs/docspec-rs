# Duplicate native UUID capability probe — independent review

Static verdict: APPROVE for the bounded diagnostic. Reviewed `/tmp/docspec-duplicate-uuid-probe.py`, SHA4784a34b71b98375ad3579402f5be488ef58c915038afb3915eccc0892a93d65; no execution or author notes. No actionable finding.

The only mutation destinations are a fresh canonical-UUID-derived namespace (authoritative GET404 first), its namespace creation, and exactly one table register POST. Original source table receives GETs only; no update/drop, retry, object upload/delete or prefix sweep exists. Fixed source URI/UUID/location/empty snapshot/field IDs7/42 must match before mutation, and complete canonical native metadata hash plus location/UUID must remain equal in original and destination readback. A definitive duplicate rejection is distinguished from an inconclusive response; unknown transport exception retains attempted-stage evidence and stops without retry. The source registration stays present.

Explicit HTTPS endpoint/warehouse/token come from configured environment; redirects refuse. Requests have15-second socket timeout,4MiB JSON ceiling and150-second process alarm. Receipt is saved before requests, records status/sanitized error type/code only, and never retains raw upstream errors/token/settings. New evidence directory and final receipt remain; no cleanup occurs. The probe's environment is caller-trusted; urllib's normal proxy/certificate environment applies, unlike production's explicit object clients.

Scope: whether this configured catalog can register the exact same retained empty synthetic metadata/UUID under another namespace while original registration remains. No append, table-update/CAS, full restore, remote deletion authority or bulk/performance conclusion. Client performs no object I/O; the trusted catalog may read retained metadata as part of native registration. If source disappeared/changed or destination already exists, the probe refuses before registration. Actual result remains root-owned.

## Actual provider result

The initial urllib configuration request returned403 before mutation. Maintained
PyIceberg and httpx could read that configuration with the same credentials.
The bounded probe therefore used httpx with redirects/retries disabled, the
original response bound and same fresh namespace (no earlier mutation). Its
[retained result](duplicate-uuid-provider-httpx-2026-10-11/receipt.json) records
409 EntityAlreadyExists for the single registration request. The destination
remained absent and original metadata identity unchanged. Simultaneous native
UUID registration is unsupported in this actual combination; access failure is
not the blocker. Both attempts and comparison evidence remain retained.
