# Native metadata admission review

Verdict: APPROVE statically. Artifact `/tmp/docspec-native-decode-admission.patch`, SHA256 `aeda0f45602286e658b9fdda89e75a7bd675ef3cde2f2e892a291af6ed977f64`. No actionable finding. No execution or source edits by reviewer.

The patch replaces maximum decoded-size reservation for every native inventory object with actual stored-size allowance, retaining conservative maximum decoded admission only when the verified buffer begins with gzip magic. It preserves the existing shared byte budget and does not change limits or codec acceptance.

## Traces and checked properties

- `engine/retention/inventory.rs:1748–1772`: immutable descriptor size is checked before allocation/read; new caller admission is `stored_bytes*8+4096`. The local Arc remains alive through the match arm. Plain JSON therefore retains its complete actual-size decoded allowance without reserving the configured maximum for a small file.
- `engine/inputs.rs:462–498` plus patch: byte accounting/cap remains before buffer reservation. Worker owns caller memory Arc, transport/buffer permit, Budget clone and blocking job. It reads/hashes through `Objects::read_metadata_admitted` once; gzip detection occurs on that same admitted verified buffer. `storage/object_io.rs:386–440` confirms exact stored length/read and hash; there is no magic probe or second GET.
- Gzip admits `(limit-stored_size)*8` before expansion. Combined with caller `stored_size*8+4096`, total decoded allowance stays the previous conservative `limit*8+4096`. The returned extra permit is bound as `_decoded` in the native inventory arm and survives moving metadata into Table and the awaited configured snapshot walk (`inventory.rs:1865–1900`). Thus cancellation during decoding cannot release reservations before the worker exits, and successful gzip decoding retains admission through its consuming traversal.
- `storage/pinned.rs:92–118`: maintained existing decoder detects the exact same `[0x1f,0x8b]` magic, caps gzip JSON to `limit+1`, checks complete JSON/CRC/trailer and trailing-member bytes. Neither extension nor URI changes admission or codec choice. Malformed magic reserves conservatively and then refuses; worker error drops all permits.
- `budget.rs:135–188`: reserve is nonblocking/fails closed; blocking closure retains job+fixed worker admission through execution. A failed extra gzip reservation returns capacity before decoder expansion. Ordinary non-native read helpers return the tuple's value with no additional permit because their native flag is false.
- Whole-tree caller search finds one `read_native_manifest` consumer, updated in the patch. Existing public/pin decoding is unchanged.

## Evidence limits

Existing `storage::pinned` tests cover plain/gzip metadata, stored/decoded caps, truncated trailer, extra members and invalid JSON. The patch adds no dedicated inventory gzip-permit/cancellation test; static trace establishes those lifetimes, while parent owns focused capacity regressions and full checks. Do not promote this review to a measured RSS bound or runtime result.

Root runtime follow-up: the affected qualification-root, retained-ancestry and compacted-producer integration cases pass with unchanged resource limits. The maintained plain/gzip native reader tests also pass. See `remote-maintenance-focused-checks-2026-10-11.log`; this does not claim a new gzip inventory concurrency benchmark.
