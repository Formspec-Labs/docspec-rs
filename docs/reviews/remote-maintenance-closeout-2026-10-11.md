# Remote maintenance closeout

The remaining provider maintenance gate passes for the declared quiescent,
independent-source profile. The qualification continued from retained handoffs
after adapter refusals; it did not repeat Build, export, deletion or credential
tests that had already completed. No production dataset or deployment changed.

## Actual provider evidence

The isolated dataset is `docspec-rs-qualification-d141f808-cd28-4bcb-8c75-17164ba64f69`.
Its saved restore target is generation `f1994a3b-321f-43a6-8ce3-5e102c5b5189`.

- [Deletion and credential evidence](remote-maintenance-provider-recovered-2026-10-11.stdout.log)
  reserves only the obsolete 810-byte export while protecting the selected source,
  selected export, held reader and live point. The actual DELETE succeeds before
  an injected lost acknowledgment. A fresh host reconciles 810 absent bytes with
  zero newly confirmed unlink bytes. The temporary credential receives 404 for
  its absent allowed key, 403 for an outside key, payload GET and new-key PUT, and
  403 after expiry. All issued destructive sessions expire before restore.
- [Restore evidence](remote-maintenance-restore-provider-fixed-2026-10-11/receipt.json)
  completes the saved target after interruption following source-registration
  detachment. A fresh host retains the exact source/export pins and independently
  reads duplicate rows, null and empty text; the public export excludes the private
  field. The actual provider refuses an unchanged native commit held by another
  OS process against the old generation. The restored epoch advances again to 2
  without changing the selected source. The point protects 319,309 retained bytes;
  restore copies no dataset payload.
- [Exact handoffs and child evidence](remote-maintenance-restore-provider-fixed-2026-10-11/handoffs/)
  retain the original setup, removal and restore decisions plus both failed and
  successful stale-actor controls. The final continuation passes in 48.61 seconds
  inside the test; [outer supervision](remote-maintenance-restore-provider-fixed-2026-10-11.supervision.json)
  records 52.21 seconds and sampled free space remaining above 4.8 GB.

These times establish bounded completion, not remote throughput. The initial
Build/export and deletion runs remain separately recorded. One deletion-recovery
attempt overlapped the final seconds of a successful local compile check; no
performance comparison uses that attempt. Physical I/O totals, process RSS and
unsampled disk peaks are not claimed by this provider test.

## Repairs and review

The first [provider attempt](remote-maintenance-provider-2026-10-11.stderr.log)
stopped before deletion because export ownership assumed one protected directory.
The fix reuses the exact typed completed-export proof for its host and retained
paths. Its [regression](remote-export-owner-proof-2026-10-11.log) closes the object
transport before checking ownership and retirement, proving no payload reread.

The subsequent restore refusals exposed actual catalog behavior: LIST of an absent
generation returns a permission-style error, and new namespaces receive a new
`namespace_id` plus a trailing slash on the same directory location. The adapter
confirms absence through the maintained existence API only after exceptional LIST;
normal success adds no request. Portable namespace comparison omits the assigned
ID and normalizes one trailing slash while preserving all other properties and
the exact restore intent. The saved point and target remain unchanged.

[Independent review](remote-maintenance-connected-review-2026-10-11.md) closes those
repairs and the earlier restore-epoch issue: unfinished captured operations cannot
recreate a destination deleted after capture. Completed acknowledgments and
qualified material work remain reusable. The
[epoch regression](remote-maintenance-epoch-regression-2026-10-11.log) exercises
actual upload/deletion followed by old-attempt refusal with no further object I/O.
The [namespace tests](remote-portable-namespace-properties-2026-10-11.log),
[REST adapter test](remote-missing-generation-root-2026-10-11.log), and
[final formatting/Clippy/build gate](remote-portable-properties-gate-2026-10-11.log)
pass. Earlier full suites and their focused completion remain in the connected
review; passing expensive workloads were not rerun.

This is catalog-loss recovery with retained immutable objects, explicit operator
quiescence and disabled automatic expiration. It is not object-loss backup,
online failover, remote computed/related processing, hostile-process credential
isolation or machine power-loss qualification. See the
[operator procedure](../remote-maintenance.md).
