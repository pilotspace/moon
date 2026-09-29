# WS28-durability-residuals SUMMARY

Wave 1 of the next round (base main `ce65400`, branch `perf/ws28`, integrated into
`claude/gifted-mendel-e9wiz5`). Issues: moon#1293, moon#1291, moon#1290.
All results come from the **Linux container, not the merge bar**. The oracle is redis-cli 7.0.15.

## Per-issue verdict
| issue | verdict | commits (perf/ws28) | evidence |
|---|---|---|---|
| moon#1293 | FIXED | 495f13a | `tests/crash_aof_init_generation_1293.rs` with the crash hook `MOON_TEST_AOF_INIT_CRASH=after_commit\|after_head:<shard>`. **Hook-only build of ce65400:** monoio s4 149/149 and 144/187 deleted probes back, s1 88/88; tokio s4 167/167 and 137/179. **Fixed:** 0 back at boots B and C, monoio and tokio × s1 and s4. Tokio s1 has no manifest, so the window does not exist there. |
| moon#1291 F8 | FIXED | c692894 | `tests/cold_graves_reduced_databases_1291.rs`. **Base:** 129–173 deleted probes of db 1 back in each quadrant. **Fixed:** 0 back, and db 1 DBSIZE is unchanged (no live key hidden). |
| moon#1291 F9 | FIXED | c692894 | Unit test `storage::eviction::commit_failure_tests`, red with only the fix reverted. |
| moon#1290 (a) | FIXED | 11a8d42, ef5f874 | Instrumentation on ce65400 found the drops at the monoio write gate and the SPSC gate. `tests/tiering_no_aof_write_gate_1290.rs` **on base:** 3309–5432 of 16.2K keys plain-dropped. **Fixed:** 0 dropped, DBSIZE 16200, 200/200 probes, and the same after BGSAVE + kill -9. |
| moon#1290 (b) | FIXED | 11a8d42 | Unit test `aof_routing_tests::config_set_appendonly_yes_without_a_writer_still_spills_durably`, red with only the routing line reverted. |
| Follow-up (orchestrator, maintainer decision) | DONE | integrated `94296bb` | The no-AOF durable batch is now 1024 entries with a 1 MiB floor (was 256 entries, 256 KiB). |

## Mechanisms
- **#1293:**
  - The `AofManifest::prepare*` constructors return an `UncommittedGeneration`.
  - `seed_generation_head` fsyncs every head plus the incr's directory entry.
  - Only `commit()` writes the manifest.
  - `main.rs` has one `open_fresh_generation` helper.
  - A crash before the commit leaves no manifest, so the next boot redoes the whole initialization.
- **#1291 F8:**
  - The snapshot loader skips a db selector past `--databases`; it used to abort before the graves trailer on ANY smaller count.
  - An unattached db's graves move into db 0's index. The trailer is keyed by file id, so this is safe.
- **#1291 F9:** a failed commit retires the batch's entry (`remove_file`) and graves every slot (`ColdIndex::note_unpublished_slot`).
- **#1290 (a):**
  - The event loop's manifest lives in an `Rc<RefCell>` registered in `shard::manifest_cell`.
  - The connection gates and the Lua bridge borrow it with `try_borrow_mut`, falling back to `None`.
  - The SPSC and cross-db COPY gates get it passed down.
  - `durable_batch_bytes` sets a minimum batch size.
- **#1290 (b):** the async-spill sink routes on `aof_backstop()`, which is true when the AOF writer pool exists.

## Measurements: the #1290 cost
Setup: `redis-benchmark -t set -r 50000 -d 600 -n 100000 -c 16 -P 16`, 8 MB `allkeys-lru`, disk offload on, no AOF, interleaved ×3, quiet box.

| config | base (drops keys) | fix, 256-entry batch | 1024-entry / 1 MiB batch |
|---|---|---|---|
| monoio s4 | 315–559K rps; keeps 10.7–18.6K of 43K keys | 122–125K rps; keeps every key | 151–164K rps |
| monoio s1 | 312–556K rps | 70–86K rps | 109–137K rps |
| tokio s4 | 298–308K rps | 107–117K rps | — |

- AOF-backed tiering at s4 runs at 169–196K rps, unchanged by the fix.
- The figures in the `11a8d42` commit message were taken on a loaded box and are superseded by this table.
- **Maintainer decision (2026-09-27):** keep the no-loss fix and ship the 1024-entry / 1 MiB batch. The cost is more disk held by dead slots without an AOF.

## Residual risks
1. **Tokio `--shards 1` legacy `appendonly.aof`:** a crash in the middle of writing the head leaves a non-empty file that is not re-seeded. Boots are still covered, because the snapshot carries the graves. A later BGSAVE under AOF drops the trailer.
2. **F8 analog under AOF:** with a smaller `--databases`, an unattached db's key ledger (and its hot keys after a fold) is still lost. This predates WS28 (the "reduced --databases" problem).
3. **F8 warning noise:** one warning per skipped db per shard.
4. **F9:** the failed batch's heap file stays on disk until tombstone GC.
5. **#1290:** a script run from inside the SPSC drain finds the manifest held and falls back to a plain drop. It was not seen in the repro. The code never panics there.
6. In lib unit tests, `aof_backstop` follows the config string. An in-process test that means "AOF-backed" must call `dead_slots::enable_ledger()`.
7. Pre-existing: `--appendonly yes --shards 1 --maxmemory 8mb allkeys-lru` answers OOM under this flood.
8. Pre-existing: `perf_ws21_eviction_bgsave` (monoio single-shard case) fails identically on base.
9. **Shared-target hazard:** another worktree's newer artifacts made cargo treat an edited lib as fresh. Touch `src/lib.rs` before each build and verify every binary with a marker.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.91 · Practicality 0.91 · Optimization 0.90 · Edge cases 0.90 · Self-evaluation 0.90
