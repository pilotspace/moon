# Next round — maintainer decisions (2026-09-27 interview)

Recorded after PR pilotspace/moon#1292 (this round) was marked ready.

## This round's delivery
- One PR (#1292). It closes #1281, #1279, #1272, #1276, #1280, #1265 and #1269. The in-place-mutation half of #1269 is split into #1295.
- Hosted CI: the maintainer is re-granting the GitHub app Actions write, so the orchestrator can dispatch `ci.yml` (Windows) and `crash-matrix.yml` and re-run noisy jobs itself.

## Next round: scope and order
The maintainer picked all four areas, run as **durability and perf workstreams in parallel** in wave 1 (about 3 concurrent workstreams on the 4-vCPU box, per TEAM-RULES).

| area | issues | decision |
|---|---|---|
| Durability | #1285 / #1185 (TXN.ABORT), #1293 (boot window), #1291 (grave residuals), #1290 (plain drops with disk-offload) | #1185 → **option (b), all engines**: KV compensating records with move-captured pre-images (DEL / RESTORE … REPLACE ABSTTL), plus vector tombstone logging and a graph/MQ audit. Any new WAL payload needs a fuzz target in both matrices. |
| Perf | #1295 (in-place COW copy), #1288 (adaptive expiry duty), #1294 (eviction DEL stall), #1287 (HSCAN/SSCAN O(N log N)) | none recorded yet |
| Decision briefs | #1283 | **(a), then (d)**: ship the `MOON.TS` stamp (shard clock carried on `AofMessage`, format-compatible both ways) this round; schedule (d), redis-parity clock-free replay, as its own epic |
| | #1266 | **Option 3 now, then measure 1A**: everysec fsync to an agent thread, flush the tokio tail every batch, shorter post-idle poll. Then an A/B of 1A (one `write(2)` per loop iteration on the shard thread), adopted only if it passes the perf gate. Do not pursue 1B. Document redis's 2 s postpone caveat. |
| Redis parity | #1286 (`expired_keys`), #1289 (held-file release trigger), #1296 (ACL rule order), consistency rows that need a redis 7.2+/8.x oracle | a 7.2+ oracle must be installed on the box first |

## Carry-over rules from this round
- Every MAJOR+ fix gets a red→green test. Timing-based tests state their bound and margin.
- Copy binaries out in the same command that built them, and verify each with a `strings` marker. Two worktrees produced byte-identical "different" builds this round.
- The consistency suite needs `pip install redis`. Compare against the base binary test by test, not by absolute counts.

## Additions (2026-09-27, after wave 1 integration)
- **moon#1290 cost decision:** keep the no-loss durable tiering and raise the no-AOF batch to 1024 entries / 1 MiB floor (`94296bb`). Measured gain +25–55% rps; the cost is more disk held by dead slots.
- **Wave 2 adds:**
  - **moon#1297, no-AOF block reclaim:** compact mostly-dead spill files without an AOF, adopting `F'` after the next successful snapshot. This removes the disk cost of the bigger batches. It is durability-sensitive, so it needs crash tests at every kill point.
  - **moon#1298, expiry time-bucket wheel (evaluate):** memory per TTL key vs the sorted `ExpiryPair` index, plus drain and `SET … EX` throughput. Ship only if it wins on memory with no throughput loss.

## Wave-1 review follow-ups: maintainer decisions (2026-09-28)
- F1 (SSCAN rewrite regression) and F2 (routed-script plain drop) were fixed in wave 1 by WS30, together with the MINOR/NIT items.
- **F5 → moon#1299: block other writers.** A key written inside an open TXN is locked until COMMIT/ABORT. Other clients' writes, and FLUSHDB/FLUSHALL/SWAPDB on a db with open intents, are refused with a distinct error. There must be zero cost when no TXN is open.
- **F3 + F4 → moon#1300: fix both in wave 2.**
  - F4: TXN begin/commit markers in the AOF, and recovery rolls back an unterminated TXN (brief option c2, AOF only). Needs a fuzz target.
  - F3: the snapshot serializes the pre-TXN image for keys with open intents.
- Wave 2 scope: moon#1299, moon#1300, moon#1295, moon#1283 (a), moon#1266 (option 3, then measure 1A), moon#1297, moon#1298 (evaluate), plus parity moon#1286, moon#1289 and moon#1296.
