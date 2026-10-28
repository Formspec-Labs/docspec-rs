---
id: dec-4kpa
status: closed
deps: []
links: []
created: 2026-10-28T15:23:14Z
type: bug
priority: 0
assignee: mikewolfd
---
# Measure shared admission in retained captures append


## Notes

**2026-10-28T15:55:18Z**

Fixed and reviewed in 0cbad87574a4414af56461f442a481a4cfa93079 after regression commits 435ac22 and d758e3d. Exact Senate-caption append overcharged 3,635,214,888 bytes (8x454,401,797+512) for two same-unit timestamp timezone tag casts that reuse Arrow payload/validity. Preserve all real-copy/schema/byte guards and report requested/available shared bytes plus separate native-pool usage/caller. Full 910,574-row accepted-caption regression RED before fix, then PASS; original 10GiB native append writes all rows in 12.0824s with 1,360,447,408B peak physical and 1,689,403,392B RSS. Independent review APPROVE. Full scripts/check EXIT0: 442 library,435 integration,15 restore,3 process,2 worker,11 probe,31 adapter tests;17 existing ignored unchanged. Corrected full-40-character runtime descriptor passed NativeDatasets.from_config/verify_runtime; original short descriptor explicitly withdrawn/invalid. Source line accounting for 0cb: budget.rs636->646; storage/batch.rs578->582. No source or limit changes after qualification. Comparator metadata helper is separately scoped to schema shape and cannot prove physical memory fit from empty intermediate providers.
