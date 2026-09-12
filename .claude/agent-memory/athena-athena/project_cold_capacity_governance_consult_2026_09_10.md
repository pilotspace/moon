---
name: cold-capacity-governance-consult-2026-09-10
description: 2026-09-10 counsel on bounding moon's cold tier (--maxdisk, FIFO-by-file_id drop, TTL, vector disk quota, disk-pressure cascade) — with the corrections to the brief that a future session must not re-derive
metadata:
  type: project
---

Advice delivered 2026-09-10 on `f7c83769`: add `--maxdisk` (KV cold heap bytes, per-instance /
num_shards, auto = 80% of volume total, mirrors maxmemory auto-80%), O(1) per-shard counter
fed by `SpillCompletion.file_entry.byte_size` and unlink; increment 1 = at quota the spill
sink becomes unavailable (reuse the existing `manifest is None` plain-drop / OOM branch in
`storage::eviction`), increment 2 = FIFO drop of whole `heap-*.mpf` files in file_id order
(spill order == LRU on the cold plane because any read promotes out), DEL-before-unlink via
`record_reason_del`. Vectors: separate `--vec-maxdisk`, refuse-not-drop via a `StallSources`
bit (`segment_stall.rs` precedent). Never: TTL-ordered on-disk structure, shared KV+vector
budget, per-db disk quota, noeviction-spills semantic change, vector segment drop.

**Why (corrections to the brief, verified):**
- "Cold TTL leaks" is stale: `ColdIndex::sweep_expired` (cold_index.rs ~530) runs every
  sweep tick from `timers::run_cold_orphan_sweep` (timers.rs:454), judging from the in-RAM
  `ColdLocation::ttl_ms`. The kv_ops.rs:223 comment is outdated. Real limits: O(n) walk per
  60s tick, cap `MAX_EXPIRED_SWEEP_BATCH`=4096/tick, and disk frees only when a whole file's
  refcount drains (file-granular).
- `noeviction` NEVER spills — spill is an eviction sink (`select_victim` returns None →
  OOM). The cold KV tier is populated only under evicting policies AND with persistence on
  (`disk_offload_spill_inert`). "noeviction × cold quota full" is reachable only after a
  runtime policy switch.
- No dead-space rewrite exists anywhere in `src/storage/tiered/` — a heap file with 1/256
  live keys pins its full size. Promote-then-respill churn is a growth driver.
- The spill writer never consults `DiskMonitor` (only `is_dir_lost`) — it keeps writing
  heap files while `diskfull` refuses client writes.
- #869 (open): unmanifested heap files invisible to every sweep (7.6 GB / 5,079 files on a
  live box); any quota counter fed from the manifest is fiction until that reconcile lands.
- The vector knob is `--vec-warm-mmap-budget` (2gb, instance-wide / shards), not
  `--warm-segment-budget`; demotion is WARM → `UnloadedSegment` stub, files stay on disk.
- `publish_cold_stats` already computes per-shard `disk_bytes` from manifest Active KvLeaf
  entries (timers.rs ~520) — the quota can hook there; `cold_index.resident_bytes()` is
  already charged to shard memory (persistence_tick.rs:550).

**How to apply:** when capacity/quota/#869/FIFO-drop work resurfaces, start from these facts;
verify `byte_size` against `stat` in a test before trusting the counter; sequence
measure-only → suppress-spill-at-quota → FIFO drop → dead-space rewrite (only after live-ratio
is measured). Related: [[cold-index-persistence-design]] (per-file KvIndex footer gives the
per-file key list FIFO drop needs), [[888-warm-segment-orphans-root-cause]].
