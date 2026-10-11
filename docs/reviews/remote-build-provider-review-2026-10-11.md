# Provider fixture static review

Verdict: APPROVE for explicitly authorized opt-in qualification. No blocking findings. This review covers src/engine/remote_build_provider_tests.rs and its configuration, lifecycle, and native Build callees; it does not certify provider runtime success.

The fixture requires a canonical fresh qualification UUID and refuses an existing lifecycle namespace before effects (lines 69-104). It resolves explicit catalog/S3 credentials through the maintained RemoteStorage path. RemoteCatalog redacts Debug; the fixture prints only Error::code on failure (lines 62-65), not upstream configuration or error messages.

The Engine uses bounded memory, metadata, batches, scratch and one compute worker (lines 41-56). The maintained remote clients impose finite connect/request timeouts and zero DataFusion object-store retries. Root's external runner will impose a whole-run 180-second timeout, retain partial logs and references, and will not automatically retry with a fresh prefix after an unknown outcome.

The test writes a tiny synthetic Parquet producer and uses the real native writer. It leaves an acknowledged native append without lifecycle processing completion, deletes only the producer, then resumes through actual Engine readback and verifies SQL executed once (lines 166-217). A fresh host performs selection and native read_group, checks exact duplicate/null values, and verifies no SQL execution (lines 231 onward). The read collector refuses more than four rows. No remote cleanup API is called; remote objects remain retained.

Scope limits: the controlled acknowledgement gap is not a process-kill test. The fixture does not assert every candidate/release transport request count, arbitrary remote maintenance, or non-initial Build behavior. Its successful execution would qualify this isolated configured lifecycle/initial Build/read path, not general public remote startup. No provider calls, Cargo builds/tests, shared-file edits, or runtime measurements were performed by this reviewer.
