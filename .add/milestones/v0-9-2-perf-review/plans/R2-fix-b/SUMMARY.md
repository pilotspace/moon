# R2-fix-b SUMMARY

Wave 2a, R2 review of lane B (AOF). Branch `w2/r2-fix-b`, base `057598f`, 7 commits, tree clean. Linux container, not the merge bar.

**Binaries:** `/home/user/wt/bin/r2fixb-v1-{monoio,tokio}`, release-fast, built at `9b8d391`. The later commits change only tests and comments. The marker string `another binary appended after this binary` is in both binaries and not in `r1b-057598f-*`.

**Result in short:**
- The mtime heuristic is replaced by a positional rule: a clean-close marker plus a stamp on every reopened file's first write.
- The R2 residual (keys lost on any restart before the forced rewrite) is red on r1b and green now.
- NEW-A (a touched or copied AOF resurrecting keys) is red on r1b and green now.
- NEW-B and NEW-C are gone; NEW-D is documented.
- The loom model now covers the fsync agent's unwind path and passes.
- One hole in the design as briefed is closed as part of this (below). No hole was found in the design itself.

## Design note: one gap closed on top of the brief

The brief judges a foreign segment (records an older binary appended) by "the next stamp's value". The next stamp is written lazily, at the new binary's first **write** after its boot, which can be hours later.

**Counter-example:**
1. The new binary stops cleanly. The old binary writes `SET k 10 PX 60000; INCR k` and stops.
2. The new binary boots at T. It correctly judges k as `11` with its TTL.
3. It idles past k's deadline, and active expiry has not reaped k yet (sampling on a large keyspace).
4. At T+40 s it writes some unrelated key, which is its first stamp (T+40 s). Then it is killed with kill -9.
5. Every later boot judges the old binary's records at T+40 s: `SET` creates an already-expired k, and `INCR` rebuilds it as a persistent `k=1`. The key is resurrected.

**Fix:** when a boot's replay finds a foreign segment running to the end of a file, it hands its judgment, `max(close, mtime pin)`, to the writer that reopens that file (`clock::take_open_foreign_segment`). The writer writes it as its first stamp, before anything else it appends (the `CLOSE` included). Every later boot then judges the segment exactly as the first boot did.

## Per-issue verdict

| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| R1 #1 residual (MAJOR, moon#1283): positional rule replaces the mtime heuristic | FIXED | `9b8d391` code, `85736a8` doc comment, `f48890a` test fix | See "Evidence" below | Unclean-stop residual, documented (see Risks) |
| NEW-A: a moved mtime re-judged the last clock tick | FIXED | `9b8d391`, `f1655e4` | `touchforward_never_rejudges_the_last_tick_{s1,s4}`: red on r1b (1/8 and 4/8 keys come back as a persistent `1`), green now. `q5`: r1b gives `k=1`, pttl -1 on both runtimes; now `k` is absent. Unit test `a_moved_mtime_never_rejudges_the_last_clock_tick` | — |
| NEW-B: last-stamp match by value | GONE | `9b8d391` | The backward scan (`log_tail.rs`) and the value match are deleted | — |
| NEW-C: misleading forced-rewrite log line | GONE | `9b8d391` | The forced rewrite is removed; `src/main.rs` is identical to `f766fc2` again, so the moon#914 log line is accurate | — |
| Docs: residual procedure | DONE | `13d391c` | STORAGE-FORMAT §3.3 and the production guide | — |
| NEW-D: `aof_pending_bio_fsync` counts writers | DOCUMENTED | `95a49c3` | Production guide ("0..N at `--shards N`, alert on `> 0`"), plus the `fsync_agent.rs` docs | — |
| R1 #10 NIT: loom model of the unwind guard | DONE | `c9600f4` | See "Loom" below | Loom is still not run by any workflow |

**Evidence for R1 #1:**
- `downgrade_then_reupgrade_{s1,s4}` and `_then_kill9_{s1,s4}` (one SET right after the re-upgrade boot, then a graceful or kill -9 restart, then a third boot):
  - red on `r1b-057598f-monoio`: 351/351 (s1) and 86/347 (s4) keys lost after a graceful restart; 338/338 and 86/332 after kill -9;
  - green on both runtimes, with no AOF rewrite anywhere in the test.
- Reviewer's `r8c` shape (SIGTERM restart, `MODE=race`), r1b vs new:

  | config | r1b keys missing | new keys missing |
  |---|---|---|
  | monoio s1 | 325/325 | 0 |
  | monoio s4 | 73 | 0 |
  | tokio s1 | 312/312 | 0 |
  | tokio s4 | 76 | 0 |

- `pure_lifecycle_{s1,s4}` (the `q1` shape): every `CLOSE` is followed by a stamp or the end of the file, there are ≥ 4 markers per log, and the rule's log line never appears.
- The `touchback` / `touchforward` / `mixed` probes still pass.

**Loom:**
- New model: the agent dies on its first job, storing `dead` (Release) and then calling `finish(false)`. The writer does its CAS Acquire on IDLE, then `dead.load(Acquire)`.
- It checks that the writer never dispatches to a dead agent, no job is left in the dying agent's queue, the hand-off never sticks IN_FLIGHT, and the failure is seen.
- A negative control (`dead` stored after `finish`) is caught by loom with `(6) a job was sent to the dying agent and is lost in its queue`.
- Loom run: 5 passed. The std smoke run: 4 passed.

### Mechanism (`9b8d391`)
- **Grammar:** `MOON.TS <ms> CLOSE` classifies as `Pseudo::Close`, routed as a Marker. `TsRecord::close()` encodes it.
- **Writer:**
  - All four writer loops (monoio/tokio × TopLevel/PerShard) append the close records on the Shutdown message, on a closed channel and on tokio cancel, before the final sync. They skip it when the write-error latch is set.
  - The logic is in the new file `writer_task/close.rs`; `writer_task.rs` grew by 18 lines (2132 → 2150).
  - `RecordCtx::appending(path)` owes the reopened file a session stamp. The first append is always stamped, even for a record with no producer clock (the writer's own clock is used). Before this change that was only incidental: clock-0 records emitted no stamp.
  - The owed stamp is preceded by the handed-over segment judgment when there is one. `reset()` (a new generation) owes nothing.
- **Replay:**
  - The three readers (flat, multi-part RESP incr, framed per-shard) report each record's end offset (`clock::at_record_end`) and pin with a `LogFormat`.
  - At a `CLOSE`, `replay/log_segment.rs` parses forward from the marker (a parse, not a byte search; only the segment's bytes; an absurd framed length reads as a torn tail) to the next stamp.
  - The segment is judged by that stamp, or at end of file by `max(close, pin)`, which is also reported to the reopening writer.
- **Removed:** `FOREIGN_TAIL_TOLERANCE_MS`, `log_tail.rs`, `foreign_tail_replayed`, and the forced rewrite in `main.rs`.
- **Fuzz:** `aof_incr_replay` gets a CLOSE prefix (bit 5) and file-backed readers (modes 2 and 3; bit 6 selects framed). It drains the handover registry after each run. It is the same target, already in both `fuzz.yml` matrices.

### Compatibility (checked by replaying hand-made files, s1 and s4, both runtimes)
- `lane-b-base-*` (pre-MOON.TS) skips `CLOSE` as an unknown command and boots with all data.
- `r1b-057598f-*` logs one "malformed MOON.TS" WARN per file, skips it and boots with all data.
- The AOF logs no MULTI/EXEC wrapper records, and the marker is written only after the writer has drained its queue, so a marker can never sit inside a transaction.
- An older binary's own rewrite drops the markers, which is fine.
- `migrate_aof` already copies every `MOON.TS` record to all shards in stream order.

### Adversarial checks
- **Paths that reopen or switch an incr:**
  - boot reopen, all four loops: a session stamp is owed;
  - rewrite drains before a generation commits write to the old file, through the same `prefix` call;
  - `reset()` runs on every commit (`rewrite.rs` ×4 and the tokio flat writer).
- **A CLOSE at the end of a file a later generation supersedes:** that file is never replayed again, so it has no effect.
- **Two graceful restarts with no writes:** `CLOSE CLOSE` is an empty segment (unit test).
- **Several CLOSE markers in one file, a CLOSE directly followed by a stamp, a segment in the middle vs at end of file:** unit tests over all three readers.

## Measurements
None needed: the change adds one ~55-byte record per clean stop and one stamp per reopened session, both on the writer thread, and one thread-local store per replayed record at boot. There is no hot-path code.

## Gates (Linux container)
- **fmt / lint / fuzz:**
  - `cargo fmt --check`: 0.
  - clippy `--all-targets -D warnings`: 0 on monoio, 0 on tokio (re-run on the final tree).
  - `cargo check --manifest-path fuzz/Cargo.toml --all-targets`: 0.
- **Lib tests:**
  - `cargo test --release --lib persistence`: monoio 1052 passed.
  - tokio 1050 passed, after `f48890a`. That commit fixes my own regression: the byte-exact test `tokio_per_shard_writer_latches_after_torn_write` now sees the session stamp.
- **Loom:**
  - The prescribed `RUSTFLAGS="--cfg loom" cargo test …` does not build: `cfg(loom)` on every crate removes `tokio::net`, and hyper-util fails to compile. The test file already documents this.
  - I ran the documented form instead: `cargo rustc --release --test loom_aof_fsync_agent -- --cfg loom` in `/home/user/wt/target-loom`, then the built test binary. 5 passed.
  - `target-loom` is deleted.
  - After that run I only changed the negative control's `should_panic` to require the observed message, and a doc line.
- **Integration**, `--include-ignored`, `MOON_BIN` pinned to `r2fixb-v1-*`, tokio suites compiled with tokio features. Every suite exits 0 on both runtimes except the one pre-existing failure below:
  - aof_replay_clock_1283 19/19 (with `MOON_DOWNGRADE_BIN=lane-b-base-*`), aof_select_after_restart_r1, aof_fsync_stall_r1, aof_everysec_kill9_1266 10/10
  - aof_multidb_kill9, crash_matrix_per_shard_aof, legacy_aof_rewrite_on_boot_914 (monoio runs 0 tests), aof_append_status_heals_on_rewrite
  - single_handler_aof_order_1099 (monoio runs 0), wal_kv_db_context_1039, cold_tier_aof_double_apply_902 (monoio, 3/3)
  - extra suites that touch the AOF head: cold_cut_single_shard_914, crash_aof_init_generation_1293, aof_auto_rewrite
- **Pre-existing failure, not mine:** `crash_recovery_cold_del_rewrite::held_spill_files_are_released_after_the_next_rewrite` (monoio). It fails identically on `r1b-057598f-monoio` ("33–36 at the fold, 0 now").
- **Not run:** the libFuzzer target (no nightly toolchain; the stable smoke test replays its mutations through the file readers too), and the hosted CI matrix.

## Cross-ownership edits
- `src/main.rs`: the R1 forced-rewrite hook is removed; the file is identical to `f766fc2` again.
- `src/persistence/aof/pool.rs`: one test's expected frames.
- `src/persistence/aof/mod.rs`: 4 lines, the flat reader's record offset.
- Docs: `docs/STORAGE-FORMAT-V1.md` §3.3 and `docs/production-guide.md`. The guide's "Key expiry during AOF replay" section was also stale since WS37: it still called the time record a future format change.

## Risks / things to re-check at integration
- **Residual, documented, not fixable positionally:** a downgrade after an **unclean** stop of the new binary (kill -9, OOM, crash, power loss), or a writer that is abandoned at stop or torn, leaves no `CLOSE`. The older binary's records are then judged by the stale last stamp.
  - Procedure (in STORAGE-FORMAT §3.3 and the production guide): stop the new binary cleanly and check its log for "AOF writers drained and synced"; after a crash, start it once and stop it cleanly first. Otherwise, as the older binary's last action, run `BGREWRITEAOF` and stop it once the rewrite is done.
- **Every clean stop appends about 55 bytes per incr**, and every reopened session adds one stamp. Byte-exact tests of a reopened or closed incr need these frames (`f48890a`).
- **Pre-existing, unchanged:** the stamp-less prefix of a file an older binary started (a plain upgrade from before moon#1283) is still judged by the file's latest mtime. The same handover could pin it (the upgrade boot's judgment written as the first stamp). That is a possible follow-up, not done here.
- **The segment-judgment handover is a process-global registry** keyed by canonicalized path: written once per replayed file, taken once per writer session. An entry nobody takes (a `DEBUG RELOAD`-style re-replay) is a few bytes.
- **Loom is still not wired into any workflow**, and it needs the `cargo rustc … -- --cfg loom` form, not `RUSTFLAGS`.

## CHANGELOG sentence (replaces the R1 "Adjustment to moon#1283" bullet)
- **Adjustment to moon#1283:** a clean stop (SHUTDOWN, SIGTERM) now ends each AOF incr with a clean-close marker `MOON.TS <ms> CLOSE`, and a restarted writer stamps its first write. So records an older binary appends after a downgrade are recognised by their position and judged the way the older binary judged them, on the re-upgrade boot and on every boot after it, whatever kind of restart comes next and with no AOF rewrite. The mtime-based guess and the forced boot rewrite are gone, so a `touch` or `cp` of the AOF no longer re-judges its last writes. Not protected: a downgrade after an unclean stop of the newer binary. Stop it cleanly first, or run `BGREWRITEAOF` as the older binary's last action (STORAGE-FORMAT §3.3). `INFO aof_pending_bio_fsync` counts writers (0..N at `--shards N`).

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.9 · Practicality 0.92 · Optimization 0.9 · Edge cases 0.91 · Self-evaluation 0.9

The stated limits are the documented unclean-stop residual (by construction) and the libFuzzer target not being run here.
