---
name: cold-index-persistence-design
description: 2026-09-08 consult on eliminating the O(spilled bytes) ColdIndex rebuild at startup (24.8s / 1.25M entries measured); recommended per-file KvIndex footer, with non-obvious code facts that shaped it
metadata:
  type: project
---

Recommendation given 2026-09-08: persist the cold index as a **per-file KvIndex footer** inside each
`heap-NNNNNN.mpf` (option A), legacy files fall back to scan; whole-index checkpoint snapshot (B) is
the runner-up; manifest-carried keys (C) and lazy/Bloom (D-lazy) rejected.

**Why:** heap files are write-once (`write_kv_spill_batch`: tmp -> fsync -> rename -> dir fsync,
never appended), so a footer is atomic with its pages and needs no separate validity stamp; the
manifest stays the single authority for the file set. `PageType::KvIndex = 0x12` was reserved in
`.planning/MOONSTORE-V2-COMPREHENSIVE-DESIGN.md` §6.4 as `index-NNNNNN.mpf` and never built; the
"no serialized form, no format to version" rationale in `ColdLocation::ttl_ms`'s doc comment is a
later rationalisation of that gap.

**How to apply:** if this work lands, verify against these non-obvious facts before trusting them:
- `read_cold_entry` (cold_read.rs) never compares the slot's key to the looked-up key — any
  persisted index should add that check as defense in depth.
- Constraint: index must be attached BEFORE Phase 4 WAL replay (recovery.rs ~375) or replayed
  DEL/FLUSH are silent no-ops on the cold plane; this is what kills lazy/Bloom designs, plus
  SCAN/DBSIZE/KEYS/sweep_expired/demote_replayed_cold_shadows all need the complete map at start.
- `KvLeafPage::get` copies key AND value (and LZ4-decompresses) per entry — the scan pays value
  bytes even though the index only needs keys.
- Manifest `page_count`/`byte_size` for KvLeaf files are INFO-only (timers.rs); `max_key_hash`
  is always 0 for KvLeaf (free u64 for a per-file key_count cross-check).
- Checkpoint Finalize (persistence_tick.rs ~1510) runs on the shard thread — a whole-index
  snapshot written there would stall the loop (task #59 spirit).
