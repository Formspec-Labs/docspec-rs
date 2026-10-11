# Q1 shared admission overlap review

Verdict: APPROVE the revised test-only artifact for execution. Static review only; no build, test or provider calls were made.

Artifact: `/tmp/docspec-q1-overlap.patch`, SHA-256 `777aa1e6cfe0b48d945a2f888d6f27a981a5f3f770ab596d5ee94816e54a4dcb`.

The original artifact had one blocking fixture error: it treated the full explicit permit pool as free after worker cancellation, although `Lifecycle` deliberately retains admitted `SnapshotRows` in its cache (`src/catalog.rs:213-236` and the commit cache replacement). The revision measures available admission around each actual lifetime. It leaves the cache and limits unchanged and checks the native writer's exact admission delta, held upload admission after caller cancellation, upload recovery after the HTTP response, and writer recovery after cancellation. This closes that finding.

Trace: the fixture constructs a real Arrow slice with a shared multi-megabyte backing buffer; runs the actual native writer under an admitted protected attempt; sends a real compressed source file through `RemoteObjects::upload_file`; performs an actual receipt update while both are held; and exercises oversized receipt refusal without changing selected source values or retained completed phases. `storage::write_batches` holds its fixed admitted writer allowance across the stream; the upload's owned blocking worker retains its admission after its caller is dropped. The final literal source comparison preserves duplicates, nulls and UTF-8. The bounded reserve/drop search allocates no payload and makes no full-free-pool assumption.

No remaining actionable static finding. The held response window is injected output delay. The test does not establish allocator page return, RSS limits, total process memory or throughput: its fixture/oracle buffers are deliberately outside the explicit permit pool. Actual execution plus the declared process-memory sampler are still required for Q1 resource evidence. This is a local HTTP transport fixture, not provider qualification.
