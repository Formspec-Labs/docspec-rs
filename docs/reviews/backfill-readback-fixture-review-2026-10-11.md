# Killed-backfill fixture closure

Static APPROVE for the applied recount-corrected fixture change. Parent's raw gate log `docs/reviews/lifecycle-validator-parent-full-check-2026-10-11.log` confirms the failure at tests/backfill.rs319 was exact raw JSON comparison between private readback row (now scalar_validator) and public ReleasePin serialization. This is an intentional private proof member, not a changed public pin.

The correction decodes and compares the full typed public ReleasePin, separately asserts exact current scalar-validator identity, and preserves prior full raw readback JSON before/after equality plus exact resumed work-phase lists. It therefore retains public pin identity, immutable private proof preservation and no-rerun behavior. No production decoder or version proof is weakened. The initial /tmp patch had wrong hunk counts; parent applied with --recount. No builds/shared edits in this review; corrected runtime remains parent-owned.
