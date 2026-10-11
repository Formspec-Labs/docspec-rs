# Retained R2 export to trusted local MCP qualification

Frozen before any new provider/consumer collection. One exact immutable export
`download`, dataset `docspec-rs-qualification-0e38b494-34a4-47e4-a96e-f8ae27822f00`,
source `update-two`; 824 bytes, digest
`05074782b79ff9b8097c81e34d2ea6b7da7011fffc7b4e4da44b6b72539253b2`.
Existing passing Update/export evidence and inspected retained records supply the
pin, never a new export or selected-pointer guess. Known evidence records expose
only this remote object; they name no surviving local complete copy. If root has
an exact existing retained copy, stop to document it before collecting: do not
silently redownload/discard it. No broad corpus or temp-directory sweep.

Hypothesis: a durable first half survives transfer-worker death; one conditional
tail request completes those same bytes; trusted local MCP admission performs
one native hash/canonical identity check, then repeated TTL0 queries reuse it.
Failure of exact range/length/ETag, literal values/schema/pin, or once-only observed
admission counters fails this qualification. ETag conditions bind transport
version; the canonical native BLAKE3 digest establishes completed file identity.

Sequence: explicit-credential maintained boto3 client with no retries; HEAD824
and ETag; GET bytes0–411 with IfMatch; fsync412-byte part and atomic marker; kill
only that recorded Python child of the owned time wrapper; retain marker/part;
new process GET bytes412–823 with same IfMatch, validate206/ContentRange/ETag and
exact412 bytes; fsync, atomic no-overwrite local publication; fresh configured
Rust HTTP host resolves canonical retained export and historical source release;
new trusted SpicyRegs MCP TestClient reads actual Parquet three times, with real
native standalone hash and canonical metadata boundaries counted. Provider
objects remain untouched. No cleanup of remote prefixes/old application data.

Literal oracle: counter/text columns in that order, BIGINT/VARCHAR, five rows
`(1,null),(1,last),(1,last),(3,empty),(4,keep)` including duplicates/null/empty;
canonical pin declares field IDs2/1 and required counter/nullable text. Full
ExportPin/release-manifest reference must match. Reply cannot disclose private
canary name/value, and must say row_mapping=false. R2 source-only phase provenance
is sufficient here; no remote evidence Update or row-receipt mapping is claimed.
Existing real Local initial/corrected/deleted/evidence-only trusted MCP fixture
retains its separate scope. One provider release is sufficient for this added
transfer boundary; no redundant provider Build/Update or second export.

Bounds: tiny exact824-byte object; each response≤413 payload bytes, S3 connect/read
15s, no transport retries; first marker45s, prefix wrapper reap15s, tail60s,
MCP child120s/reap135s; whole parent300s plus bounded owned-process cleanup. Rust
uses supplied existing qualified limits; MCP DuckDB256MB. Free disk floor2GiB.
Parent and child timeouts retain partial logs/receipts; unknown outcome never
triggers fresh download/prefix. Kill/reap only owned descendants/processes. Native
hash helper retains its own existing120s bound. Configured credentials remain in
provider/Rust processes, removed from the consumer child; no raw provider error
text or token/config is emitted. Root supplies known explicit remote config and
binary, and executes only when other provider/Cargo work is idle.

Cost: count actual maintained SDK before-send HTTP attempts (method only; no credentials/URLs). Require prefixHEAD1/GET1 and tailGET1, so hidden transport retries cannot masquerade as do-once transfer. Retain `/usr/bin/time -l` for prefix (including forced termination), tail
and MCP children; exact HEAD/GET ranges/body bytes and native metadata/hash
counts, per-transfer wall and whole interrupted+resume+admission wall. Complete
path includes the interrupted first half, termination/reaping, resumed tail,
canonical admission/native hash and literal queries; resume-only timing is not
complete transfer cost. Raw maximum RSS is child-resource evidence, not a
configured process-tree memory acceptance. Rust host lifecycle-read cost is
included in total wall, not separately sampled RSS. No throughput/product-SLO,
whole-provider-byte, general resumable download framework, power-loss or remote
restore qualification follows from824 bytes. Keep failed evidence and complete
local output; terminal folder cannot be reused.

Conditional read semantics use maintained APIs documented by
[Amazon GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
and [R2 S3 compatibility](https://developers.cloudflare.com/r2/api/s3/api/).
Actual configured-provider behavior still requires root execution.

Invocation (opt-in, no build):

```sh
python3 experiments/r2-export-mcp.py --output /absolute/fresh/evidence --binary /absolute/current/docspec --remote /absolute/existing/remote.json --limits /absolute/existing/limits.json --spicy-regs /absolute/spicy-regs
```

Requires the already explicit DOCSPEC provider environment for the retained
qualification. No data collection occurred while preparing this protocol.
