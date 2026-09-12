---
name: 888-warm-segment-orphans-root-cause
description: moon#888 (warm vector segments orphaned from keymaps) — measured root cause is file_id reuse + duplicate ShardManifest entries + boot self-retire, NOT only keymap skew; orphan keys are 100% live; issue numbers inflated ~3x by duplicate entries
metadata:
  type: project
---

Design delivered 2026-09-10 at `tmp/perf-wave3/DESIGN-888.md` (base f7c83769). Key facts a future
session must not re-derive:

- `next_file_id` is seeded ONLY from `data/heap-*.mpf` (`src/shard/event_loop.rs:787-790`);
  warm segments draw ids from the same counter (`src/vector/store.rs:1490`) but never feed the
  seed, so with no new spill files every boot reuses the same vector ids. `ShardManifest::add_file`
  is a plain push (`src/persistence/manifest.rs:646`) -> duplicate Active entries (up to 6x).
- `register_warm_segments` processes each duplicate entry separately; the second copy trips
  `already_covered` and `remove_dir_all`s the dir it just attached (`store.rs:2813-2835`).
  "21 retired as duplicates" were self-duplicates; 287-245 = 2x21.
- The issue's 111 orphans / 77,616 keys / 0.18 GB (48.9%) are per-WARN-line counts over duplicate
  entries. On disk: 39 distinct dirs, 27,887 key_hashes, 0.070 GB (16.1%). Re-encode share was
  19.3% (60,661/313,825), not 27.7%.
- 27,882 of 27,887 orphan key_hashes are LIVE keys (SCAN + xxh64 seed 0). Orphans must be
  adopted-or-superseded, never deleted.
- The "forever" loop: unregistered segment -> rescan re-encodes into mutable -> tail < 1000
  (DEFAULT_COMPACT_THRESHOLD) never compacts -> no snapshot job -> keymap never learns the keys.
- Nothing drains `global_snapshot_pool()` at shutdown (`event_loop.rs:1873-1886`), so the
  keymap skew is reachable on CLEAN shutdown, not just kill -9.
- Parsing recipes that worked: keymap = "MKM1"|count u64|xxh64|entries(kh u64, gid u32, cks u64,
  len u16, key); mvcc.mpf = 4 KB MoonPages, payload_bytes at +20, flags at +6 (0x02 = LZ4
  size-prepended), 8 B sub-header, 32 B rows (iid, gid, key_hash, insert_lsn, delete_lsn).

**Why:** the issue's framing pointed at atomic co-persistence; the measured chain shows a P0
(recovery deleting live segment dirs) that any "fix the skew" design would have left in place.

**How to apply:** when #888/#869/file_id/warm-segment work resurfaces, start from DESIGN-888.md
R0 (seed + dedupe + no self-retire) before anything about keymaps; verify counts on disk with
distinct ids, never from WARN-line tallies. Related: [[cold-index-persistence-design]].
