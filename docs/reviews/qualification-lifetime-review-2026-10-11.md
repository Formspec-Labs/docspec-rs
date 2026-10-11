# Qualification evidence lifetime review

**Verdict: approve the bounded test for execution.** Reviewed `/tmp/docspec-qualification-lifetime.patch`, SHA `67dafe10bd3293b503187864f9df5c078688c24dd12e6c719bddb0d0e92125ee`, against current production callers on 2026-10-11. No source changes, builds or provider calls by reviewer.

The new case in `tests/qualification_roots.rs` uses `qualified()` to create real scalar results, a real dependency correction and the resulting qualification through public Engine services. It does not use that file's existing synthetic `retain_only` catalog helper. A materially changed independent unbased Build selects a result without computed groups or a parent, so no selected descendant still requires the old qualification closure.

It captures exact old completion keys before creating the replacement. Availability retirement covers both ordinary and backfill readbacks; the phase list excludes the qualification itself; terminal-operation retirement ends old request and attempt protection. These match `retention/retirement.rs:19–66` and `retention/recovery.rs:218–311`. The fixture then requires the precise qualification key to be the blocker for every proposed old object, rather than assuming other protection has ended.

Inventory expands a qualification's origins and exact target snapshot (`retention/inventory.rs:937–944,1132–1143`) and skips only exact retired root/binding generations (`:304–359`). The final explicit PhaseRecovery retirement therefore ends an independent evidence promise without erasing correction authority or its immutable record. The existing removal service checks actual ownership, current protection and native registration detachment; the fixture asserts its exact confirmed effects, cold retirement/removal reconciliation and unchanged replacement values/completed retry. Runtime is still needed to establish those claims.

One test finding was fixed: final ID comparison now preserves Arrow nullness instead of unconditionally wrapping the value buffer in `Some`. Sorting keeps the assertion independent of unspecified row ordering. Fixed tiny inputs and collected results are bounded fixture work; no production overhead or new retention mechanism is introduced.

Scope is one same-workspace local qualification-root transition and its explicitly listed old origins/original creations/target metadata. It does not qualify all target files, foreign consumer owner-grant transitions, arbitrary descendant detachment, remote maintenance, crash interruption or wider R6 completion. Existing synthetic owner-acquisition tests remain separate evidence.


## Runtime-discovered scope correction

The first parent-owned fixture run correctly rejected replacing selected computed descendants with an unbased Build (`dependent_groups`, `engine.rs:2167`). The initial review missed this rebuild guard; its selected-descendant transition claim above is superseded, not qualified. No production guard was weakened.

Reviewed correction `/tmp/docspec-qualification-lifetime-correction.patch`, SHA `f5268b2dcbdc4daac4ca9871ab668f89bafbb296db2fb21acd9f3745907fd657`. **Approve this narrower withdrawn/unselected qualification case for execution.** `backfill_until(files)` acknowledges the original file completion before stopping (`backfill/preparation.rs:256–284`); the corrected operation retains qualification before its readback (`corrections.rs:142–202`, `backfill.rs:780–902`). The fixture asserts that selection remains the original source with no computed groups. It abandons both unselected derived operations, then creates genuinely new independent source material.

Availability retirement explicitly supports an abandoned operation's unselected readback (`retention/availability.rs:168–183`); terminal recovery supports that same exact abandonment. Separate old phase/result/recovery retirements then leave the exact qualification as the asserted sole remaining root. Final qualification retirement, exact listed removal and cold replay remain unchanged checks. Runtime must establish their success. This does not close selected-computed descendant retention or any foreign-owner lifetime.

## Execution result

The corrected withdrawn/unselected case passes in
`qualification-lifetime-withdrawn-2026-10-11-2.log`: the exact qualification is
the last protection, its explicit retirement permits exact removal, and cold
reconciliation plus replacement reads succeed. The preceding run misread
`unmatched_bytes`; inventory counts bytes without active protection there.
`plan_removal` separately checks ownership before returning the report. The final
assertion therefore requires zero protected bytes and all candidate bytes
unmatched. The reviewer checked this correction; no production behavior changed.
