---
id: dec-pa4o
status: closed
deps: []
links: []
created: 2026-10-28T12:37:14Z
type: bug
priority: 1
assignee: mikewolfd
---
# Account JSON-lines buffers by actual decoded allocation

The direct owner amendment replaces structural and declared-width estimates with measured post-decode Arrow allocation and one held raw buffer. Preserve source bytes/types, reader lifetime, contextual resource causes and existing line/batch/signed-offset ceilings. Native Arrow owns temporary decoding workspace.

## Acceptance Criteria

Exact D1 schema plus the retained House and YouTube first lines pass together under a5GiB native pool;513MiB remains refused; resource errors retain file key/line and primary cause; focused checks, independent review, repository gate and a clean ordinary executable pin pass. Reader qualification does not claim full capture-map or whole-corpus acceptance.

## Notes

**2026-10-28T14:26:06Z**

Direct-user real-schema amendment implemented c9d518b; contracts289aca3/fcbe9f6. Both real lines pass together in6.79s, RSS1132560384B/physical908789104B.16SQLjobs/3processchecks/lifetime/Clippy/CODEAPPROVE; c9scripts/checkEXIT0(438lib/434integration/isolated/31Dagster,17existingignored). OrdinaryremoteSHA ccabc93f42142125765ce17cc3c4bde497e91587050c6b6c2c5a8215a83832da handedengine/raw; engine normallymerged/snapshotverified. Actual200receipt sourceprocessing also completed363.904s/21204sourcecount/innerphysical13112950696B<16GiB; applicationunknown-outcome guard correctly stops export, so no D1 proof/fullgeneration claim.
