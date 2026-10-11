# Foreign certificate fixture: static review

APPROVE `/tmp/docspec-cross-workspace-certificate.patch`, SHA256 `a352a7ef1ea72d20defebc0d469bb43daf9117d75b277f6ff67b78ed8b429487`. Test-only, no production weakening. No runtime execution or shared edits.

The fixture creates two real native workspaces with matching logical dataset/operation IDs and nullable/duplicate values, but distinct immutable manifests and actual local certificates. The producer's serialization roundtrip and independent applied-change audit establish portable descriptive evidence; they do not write consumer approval or retention records. Existing independent bag/occurrence helper also proves the producer and consumer each retain a legitimate ForwardChanges path.

`comparison_plan` checks both exact locally retained release pins before native resolution or certificate discovery (`src/engine/comparison.rs:216–225`); `check_read_release` resolves this host's canonical result/readback and refuses unequal pins (`src/engine/read.rs:57–98`). Both Values/Occurrences and fast/force-full paths therefore correctly refuse foreign-after and mixed endpoints with release_mismatch. Certificate lookup independently resolves exact change selection and binds native certificate to it (`src/engine/changes/certificates.rs:324–344,160–181`), so invalid_changes for a foreign VersionRef colliding with a real local operation is meaningful.

Unchanged consumer rows and exact surviving local certificate plus successful local oracle prevent this being a missing-namespace-only refusal test. It qualifies explicit refusal of foreign endpoint authority and separation of audit from admission. It does not qualify importing foreign certificates, every mixed-endpoint permutation, or distributed retention. Notes preserve that limit; no additional harness needed. Root owns focused runtime.

Parent runtime: the exact focused fixture passes in `cross-workspace-certificate-focused-2026-10-11.log`; no production change was required.
