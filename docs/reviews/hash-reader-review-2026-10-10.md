# Hash reader: independent static review

## Findings

No material correctness or maintainability findings. Reviewed the uncommitted
`src/storage.rs` diff against `49f05943649c8b563bc451b65c8d45e5ecdf704e`;
reviewed file blob: `087d64718007e3e56ba02fedd441e016623d89d7`.
This review used the Rust and semi-formal code-review guidance. No builds,
tests, formatting, benchmarks, corpus reads, production edits, or commits were
performed. Execution belongs to the implementation task.

- **Digest and count are preserved.**
  [digest](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage.rs:406)
  opens the file and passes it to a private reader adapter. The adapter creates
  a fresh unkeyed hasher, waits for successful `update_reader`, then returns the
  same hexadecimal digest format and `count()`. The
  [lockfile](/Users/mikewolfd/Work/spicy-stack/docspec-rs/Cargo.lock:614)
  pins BLAKE3 1.8.7. Its
  [reader loop](/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/blake3-1.8.7/src/io.rs:10)
  hashes exactly each successful read and stops at EOF;
  [count](/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/blake3-1.8.7/src/lib.rs:1421)
  counts bytes hashed. `finalize(&self)` does not change that count.

- **Errors cannot produce a successful partial identity.** The maintained loop
  retries `Interrupted`, as intended, and returns other I/O errors immediately.
  The adapter's `?` prevents finalization/output on failure. The unchanged
  [error conversion](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/error.rs:56)
  retains code `io` and the original cause.
  [object](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage.rs:416),
  [verify_object](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage.rs:430),
  and the [hash command](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/main.rs:274)
  propagate failure before constructing an identity, returning verification
  success, or printing a successful result.

- **Copy-and-hash boundaries are unchanged.**
  [copy_verified](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage.rs:454)
  still hashes while copying, synchronizes output, checks the expected digest,
  and then returns its reference. Its
  [admission caller](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/engine.rs:996)
  and [restore caller](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/engine/restore.rs:900)
  retain their existing error propagation and publication order. This patch
  introduces no extra payload pass.

- **Focused assertions cover the changed behavior.** Static traces predict
  passing assertions for
  [empty and multiple-buffer files](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage.rs:732),
  [short reads and repeated interruptions](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage.rs:747),
  [failure after partial input](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage.rs:777),
  and [file-open failure](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage.rs:801).
  Successful results compare both digest and exact length against complete
  input; failure assertions inspect the retained I/O cause. These are reviewed
  assertions, not execution results from this reviewer.

- **Resource observation:** the custom one-MiB heap buffer and manual read/count
  loop are removed. The pinned maintained reader uses a 65,536-byte stack array;
  that is a source-level allocation change, not a measured process-memory or
  speed improvement. The unchanged
  [blocking-worker reservation](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/budget.rs:130)
  remains conservative for this buffer and still covers the unchanged copy path.

## Verdict

**APPROVE.** The patch simplifies hash-only work through an existing maintained
API while preserving digest/count identity, failure propagation, and transfer
boundaries. Coverage of changed paths: **ADEQUATE**. Static confidence: **HIGH**.
Repository-gate execution and performance claims are outside this review.

## Root execution receipt

After the static review, the root task ran `./scripts/check` against the same
Rust patch. It exited successfully; formatting, strict Clippy, all-feature unit
and integration tests, and doc tests passed. The digest fixtures exercised
empty/multiple-buffer files, interrupted/short reads, partial failures and file
open failures. See [the retained log](hash-reader-check-2026-10-10.log).
No comparative performance experiment ran.
