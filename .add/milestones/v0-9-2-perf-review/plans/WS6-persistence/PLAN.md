# WS6-persistence — PLAN (wave 1)
personas: `.add/personas/storage-durability-engineer.md` (lead — an acknowledged write is a promise) · `.add/personas/performance-engineer.md`
Read first: the storage-durability persona's ORIENT section, `src/main.rs` manifest-initialisation branches, the #455 fold-epoch / #452 barrier comments in `src/persistence/aof/`.

## Issues (durability first: never trade a crash guarantee for speed)
1. **moon#1187** AOF append path.
   - Must: `rewrite_expire_for_propagation` pre-scans before allocating (no Vec/clone when nothing rewrites); literals via `Bytes::from_static`; `itoa` numbers; record buffer pre-sized from the exact RESP length; writer batch buffer does not reallocate every batch (hysteresis or `write_vectored`); tokio `BufWriter::with_capacity(AOF_GROUP_COMMIT_MAX_BYTES)`.
   - Should: per-shard staging buffer (one hand-off per event-loop iteration / batch instead of one flume message per record) IF it can be done inside owned files + the send call sites without weakening #455/#452 — otherwise DEFERRED with a design note (wave 2 WS7 owns the connection handler call sites).
   - Tests: byte-identical AOF output vs HEAD for a command corpus (incl. SET EX/PX/EXAT/PXAT/KEEPTTL/GET, SETEX, PSETEX, EXPIRE family, multi-db SELECT injection); allocation-count test for the no-rewrite path.
2. **moon#1188** WAL segment rotation fsyncs inline on the shard thread.
   - Must: rotation handed to the off-loop sync agent while preserving "old segment durable before the next segment exists" (`replay.rs:340-346` mid-chain-tear policy); inline fallback only when the agent queue is full; update `tests/loom_wal_sync_agent.rs` (new atomic state ⇒ loom model); crash/replay tests stay green.
3. **moon#1185** AOF rewrite deep-copies the keyspace on the shard thread + whole RDB image in one Vec.
   - Must (interim, minimum): no per-entry `(key.clone(), entry.clone())` deep copy — serialize directly (e.g. `rdb::write_entry` into a buffer) and stream to the writer in bounded chunks; writer appends via `BufWriter` + streaming CRC; `advance_shard` takes a writer.
   - Should: segment-incremental fold with COW pre-image capture (reuse `SnapshotState`/`snapshot_cow`) so the shard thread only does bounded work per tick.
   - Tests: rewrite output loads to the identical dataset; #455 exactly-once fold tests + crash-matrix-style tests by name stay green; peak-RSS/latency evidence from a release-fast run (preload ~1–2M keys; watch `redis-cli --latency` during BGREWRITEAOF).
   - Coordinate: open moon#1158 (rewrite never commits under sustained writes) is NOT in scope — don't regress it further; note any interaction.
4. **moon#1186** BGSAVE: whole-file buffer + write+fsync on the event loop; COW overflow rescanned/deep-cloned per tick.
   - Must: stream segment blocks to the temp file off the shard thread (helper-thread pattern of `data_file_sync`) with streaming CRC; finalize (EOF, CRC, fsync, rename, dir fsync) off-thread and polled; overflow as `HashMap<(db,seg), Vec<…>>` moved out (no clone); `PENDING_KEYS` held for the whole epoch; `capture_key` skips already-serialized segments.
   - Tests: snapshot content identical to HEAD for the same dataset; writes during BGSAVE captured correctly (existing COW tests by name).
5. **moon#1181** CDC.READ rescans from segment 1 with ~9 syscalls/record on the shard thread.
   - Must: one open `File` per segment, chunked `pread`, stat once per call, seek to `from_lsn` via per-segment first-LSN (header or sidecar), scan budget counting skipped records, and the handler off the shard thread (blocking pool / spill thread) on BOTH runtimes. Tests: identical envelopes vs HEAD for a fixture WAL; syscall-count or wall-time bound independent of retained history.

## Owned files
`src/persistence/**`, `src/replication/expire_rewrite.rs`, `src/replication/effect_rewrite.rs`, `src/command/cdc/**`, `src/cdc/**`, `src/shard/persistence_tick.rs` + `src/shard/persistence_tick/**`, `src/shard/spsc_handler.rs` ONLY the `AofFold` arm, `src/shard/event_loop.rs` ONLY the snapshot-finalize / CDC-tick regions, `tests/loom_wal_sync_agent.rs`, tests `tests/perf_ws6_*.rs`.
Cross-ownership (own commit + note): the `send_append_group` call sites in `src/server/conn/**` if the staging buffer lands; `dispatch.rs` CDC.READ routing.

## Not yours
`src/storage/**` (WS1–3), `src/protocol/**` (WS4), the rest of `spsc_handler.rs` / `coordinator.rs`, `replication/state.rs` + `aof/pool.rs::issue_append_lsn` (WS7 moon#1176 in wave 2 — do not touch `issue_append_lsn`).
