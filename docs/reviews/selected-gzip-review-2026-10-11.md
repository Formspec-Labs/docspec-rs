# Selected gzip metadata review

Verdict: **APPROVE for the scoped compatibility fix**, subject to the parent-owned test gate. Static review only; I did not build, test, or contact the provider.

No actionable finding remains. `src/storage/object_io.rs:277–293` removes only the invalid equality between a selected metadata file's compression and the table's future-write property. It preserves configured read scope, table containment, canonical native metadata syntax, exact selected URI, and explicit FileIO binding. Candidate creation still enforces its own plain-JSON write policy in `src/storage/tables.rs`.

The observed provider receipt `docs/reviews/r2-native-append-codec-2026-10-11.json` records gzip bytes at a canonical `.gz.metadata.json` URI while `write.metadata.compression-codec` is absent. Pinned Iceberg 0.10.1 `spec/table_metadata.rs:449–480` reads by gzip magic; `:482–526` enforces property/location equality only when writing. Treating that writer requirement as a reader requirement incorrectly rejected this valid selected state.

The new populated fixture at `src/storage/remote/objects/tests/pinning.rs:319–452` covers the exact absent-property case, compressed-byte digest and size, sparse native schema/snapshot inventory, inherited identity/owner reuse without another metadata or payload GET, and catalog-free bounded reopening. Existing `storage/pinned.rs` bounded stored-byte read, gzip decode limits and identity checks remain unchanged. The fixture qualifies metadata/inventory behavior; it does not claim Parquet data decoding or a complete remote Engine lifecycle.

Function trace: `Objects::bind_table` (`src/storage/object_io.rs:88`) calls `PinTable::remote` (`:270`), which validates location/scope and constructs a native Table with the explicitly configured reader. `pin_bound` then obtains or reuses the stored-byte identity; `open_pin_with_io` independently reopens the immutable pin through the existing bounded gzip-aware reader. Compression is decoded at that reader boundary, not guessed from a future writer property. The constructor changes no credential, owner, lifecycle or retry state.
