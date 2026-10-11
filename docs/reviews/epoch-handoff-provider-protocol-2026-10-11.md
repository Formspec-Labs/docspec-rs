# Retained-provider process epoch handoff

Frozen artifact: `/tmp/docspec-epoch-handoff.patch`, SHA256
`007571d230f53d184e884e2228859861e9994c2ad88de127066bb1ff692a0b34`.
Private sources: `/tmp/docspec-epoch-handoff/new/`.
No builds, formatting or provider operations performed by author.

Use only the existing publication-race dataset
`docspec-rs-qualification-2ab6f450-a0ae-4b5e-a70d-8757375d5c79` and its configured
`.../__r2_data_catalog/docspec-qualification/<dataset>/publication-race` prefix.
The existing `public` export must bind the current four-row `updated` source;
authority must remain epoch0. Explicitly provide a fresh canonical UUID in
`DOCSPEC_EPOCH_HANDOFF_ID`; the derived new channel and A/B operation IDs must
have no prior retained records. Existing catalog/S3 configuration is inherited
by actual child processes using existing secret-environment references. Do not
print values. No namespace creation, Build, Update, remote deletion, credential
change or provider cleanup is requested.

Root commands (after independent review):

```sh
cargo test --locked --all-features --lib engine::remote_build_provider_tests::publication_race::epoch_handoff::independent_process_epoch_handoff_preserves_export_work_and_recovery -- --exact --ignored --nocapture --test-threads=1
```

Supervise the entire command with a finite outer deadline (for example600s) and
retain complete parent output, the printed control directory and both child
logs/requests/receipts. The fixture keeps its control/host directories even on
failure; RAII kills/reaps active children. Each file wait/child exit is bounded
at120s. Do not rerun the same UUID or replace the expected epoch after failure;
inspect exact durable records first. A completed run deliberately advances the
existing dataset epoch0→1 and adds only two tiny immutable export candidates and
one fresh channel. Existing public/source pins stay unchanged.

A prepares durable materialization+validation once, then an independent OS child
holds its original final maintained TableCommit before forwarding. Controller
fences0→1 with the channel still absent and every other retained record unchanged.
The released exact A commit must receive the maintained provider
CatalogCommitConflicts response, not a fabricated refusal. A retry must then
stale_export without changing original completion revisions/objects or the head.

B is newly admitted after the fence and prepares its own four-row output once.
There is no cross-operation export material alias API, so no speculative shortcut
is added. Its independent child forwards final selection; after the provider
accepts, the wrapper records the returned native metadata and withholds that
actual response. Parent observes the exact selected result and SIGKILLs the child
before Engine receives acknowledgment. A fresh host retries the same B request,
retaining materialization/validation identities and native revisions, unchanged
full retained JSON and unchanged lifecycle head. It independently streams the
literal duplicate/null/empty values and exact primitive fields. Original public
export and current source remain exact. Their unchanged pin identities are checked;
this fixture does not independently rescan those older payloads.

This qualifies epoch handoff/publication through supported services, actual
native CAS rejection and unknown selection-response recovery. It does not prove
credential revocation, global quiescence, deletion/restore safety, machine power
loss or physical request/transfer counts. No raw provider error/config bodies are
recorded; native response receipt records only success/conflict booleans.
