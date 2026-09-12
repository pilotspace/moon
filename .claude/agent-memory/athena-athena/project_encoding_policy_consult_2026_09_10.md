---
name: encoding-policy-consult-2026-09-10
description: 2026-09-10 counsel on listpack/intset policy (#896 unit bug, demotion, config thresholds, quicklist, 1.67x residual) — corrections to the brief and the sequenced plan
metadata:
  type: project
---

Advice delivered in `tmp/perf-wave3/ADVICE-ENCODING.md` (2026-09-10, on `f7c83769`).
Recommended: fix #896 by changing `listpack_batch_fits` to `(shape, items)` with a
separate `LISTPACK_ENTRY_CEILING`; NO general demotion; `EncodingLimits` Copy struct
on `Database` as the one authority; per-type redis-named knobs, BYTE budget for lists
only; do NOT build quicklist for `lrange_100`; one alloc lever (`encode_entry` stack
buffer) then stop at 1.67x.

**Why:** the brief undercounted the promotion leaks. Corrections found by reading:
(1) `update_header` already SATURATES (not `wrapping_add`) since #866;
(2) `bee90e89` is only the fuzz tip of a 4-commit #799 branch — the borrowed decoder
is already on main (#801); (3) the cold tier (`kv_serde::deserialize_collection`)
decodes WITHOUT `compact_after_decode` — a third #863-shaped flattening path, unfiled;
(4) ~22 secondary write commands (HINCRBY, SREM, ZREM, LPOP, LSET, ZINTERSTORE dest…)
eagerly promote via `get_or_create_*` — bigger memory lever than #896.
Redis verified: zset batch gate is in ITEMS (`zsetTypeMaybeConvert`), ZREM never
demotes, only lists shrink (half-limit), rdb.c re-derives against LOCAL thresholds.

**How to apply:** if asked to follow up, sequence is A(#896 alone) → merge 863 →
rebase+merge 799 → cold-tier one-liner → authority refactor (constants unchanged) →
config plumbing → default flip WITH Linux sweep → per-type write arms → list memory
measurement → quicklist decision last. Never bundle the default flip with plumbing.
Open measurement gaps: LRANGE N-sweep (intercept vs slope), HSET 8..512 sweep vs three
controls, whether cold ENCODE side also flattens.
