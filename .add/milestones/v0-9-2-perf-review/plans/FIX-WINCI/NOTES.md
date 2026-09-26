# FIX-WINCI: working notes (ADD discipline)

## Context loaded

- Brief: `/home/user/wt/prompts/FIX-WINCI.txt`. Persona: `ci-test-integrity-engineer`.
- Issues read (read-only): moon#1065 (body + 4 comments, the 2026-09-26 one included), moon#1272, moon#1273.
- Base: `d49a2a98` (main `4a96cd5f` + part-5 plan + WS21). Branch `fix/winci-harness`. Test-only changes.
- Target dir `/home/user/wt/target-gate` (shared). Binaries pinned with `MOON_BIN`, never the cargo fallback:
  - `/home/user/wt/bin/winci-debug`: monoio debug, built from this tree's unchanged `src` (cmp-verified copy).
  - `/home/user/wt/bin/winci-debug-tokio`: the same, `runtime-tokio,jemalloc`.
  - Evidence-only binaries, `src` edited locally, restored with `git checkout -- <file>` right after the build, and never committed:
    - `moon-monoio-debug-ws16revert`: review 6's inline reclaim put back into `Database::clear` (the WS16 Charge-redirect fix reverted);
    - `moon-tokio-debug-1113revert`: the moon#1067 high-water retain rule removed from `gc_tombstones`.
- No redis oracle is involved in any of these tests.
- Commits: `d490920f` test(cold-tier) … (moon#1065); `b86bbefd` test(acl,snapshot) … (moon#1273); this file last.
- SUMMARY.md: the harness refused the write (a subagent may not write report files), so its content is in the final report.

## Inducing the stall (evidence only, never committed)

- `scratchpad/winci/freeze.py <pid> <target> <ms>`:
  - `target=proc`: SIGSTOP the whole server, SIGCONT after `<ms>` (a runner-wide stall).
  - `target=thread:<p1,p2>`: freeze just the threads whose `/proc/<pid>/task/*/comm` starts with a prefix, using `PTRACE_SEIZE` + `PTRACE_INTERRUPT`, and detach after `<ms>`. Every other thread keeps running, which models a stalled disk.
  - A plain SIGSTOP of one tid stops the whole thread group, so ptrace is the only way to stop a single thread.
- `scratchpad/winci/stallhook.rs`: `stallhook::point(name, pid)` fires once per process when `MOON_STALL_AT=name`. It runs the freezer and blocks until the stall is in force.
- `hookpatch.py` adds `#[path=...] mod stallhook;` and ONE `point(...)` call at a fixed anchor, in the old file and in the new one alike. `run_ev.sh` patches, runs one test, and restores the file byte for byte. The old tests are otherwise unmodified.
- Thread names used: `aof-writer` (`aof-writer-N` per shard), `spill-0`, `manifest-sync-0`.
- Reproduction check: with `aof-writer` frozen and the 10,000-slot channel full, one 50-`SET` pipeline took 18.7 s and answered 9 refusals, 2.0 s apiece (`--aof-fsync-timeout-ms` default 2000). Below a full channel, a frozen writer delays nothing.

## Shared helper: `tests/common/slow_host.rs` (additive)

- `write_all(conn, cmds, depth, stats, deadline, dir)`:
  - pipelines the commands and splits the replies with `framed_len`;
  - re-sends every command answered with the AOF backpressure refusal text, after a backoff of 50 ms doubling to 1 s;
  - counts `-OOM` and does not re-send it (what the suites did before);
  - panics on any other error;
  - reads each pipeline within `pipeline_budget(n) = n x 2 s + 60 s`: the server's own per-write bound under backpressure, plus a stall margin.
  - Only for writes that are safe to apply twice (`SET` of a fixed value, `DEL`): a refused write may already have been enqueued (the cancel-safety note on `send_append_backpressure`).
- `wait_until(what, within, port, dir, cond)`: polls every 50 ms. On timeout it panics with the `diagnostics` described below.
- `wait_aof_holds(parts)` / `wait_aof_holds_last(stats)`: waits until an `*.aof` file under the dir, in either layout, holds the exact RESP record. This replaces "sleep so everysec covers the last write before the kill". A record the writer has written is in the kernel, so a SIGKILL cannot lose it. Records are verbatim client encodings; checked on both runtimes.
- `wait_spill_idle`: waits until the spill thread's heartbeat (`spill_last_heartbeat_ms`) advances 300 ms while `spill_batches_flushed` holds still. A frozen spill thread does not advance its heartbeat, so a stall cannot pass for idleness.
- `diagnostics(port, dir)`: the `INFO all` lines for memory against the cap, eviction, spill, `aof_*` (including `aof_backpressure_dropped`), cold tier and rdb, plus the last 40 lines of `server.err`. It never panics.
- Constants:
  - `AOF_WRITE_BOUND` = 2 s;
  - `STALL_MARGIN` = 60 s;
  - `CONDITION_DEADLINE` = 120 s. The measured stall lasted 179 s and slowed operations 2-17x, so a condition that normally takes up to 7 s needs up to 119 s.
  - A deadline is reached only on failure; a green run returns as soon as the condition holds.

## Per test: mechanism, design, risks, evidence

In the evidence sections, "L" is the line in the committed old file. With the hook added, the line numbers shift by 2 or 3.

### 1. cold_cut_single_shard_914 `post_rewrite_write_respilled_survives_kill9_exactly_once` (moon#1065)

- **Mechanism.** `drive_filler` discarded its replies. A `sleep(1 s)` followed, then `count_cold_files > 0` at L500. A spill writer slower than 1 s gives "nothing spilled before the rewrite". Two more sleeps were stall-sensitive:
  - `sleep(1 s)` before the SIGKILL. It covers the respill AND the everysec writer; if the writer is late, the audit "loses" acknowledged writes.
  - The kill-cycle legs had the same `sleep(1 s)` plus a cold-file assert.
- **Design.**
  - The filler goes through `write_all` at depth 1, the same single-send shape as before.
  - `wait_until(count_cold_files > 0)` before the rewrite and before the first kill-cycle SIGKILL.
  - For the respill: wait until a cold file exists that did not exist before the respill filler. This is the leg's non-vacuity; before, it was only ever true by accident.
  - `wait_aof_holds(last acknowledged write)` before every SIGKILL.
  - The marker wait of 2c and `bgrewriteaof_and_wait` moved to `wait_until` with diagnostics.
  - Every assertion is kept.
- **Risks.**
  - "A new cold file after the respill" assumes every spill batch gets a new `heap-<id>.mpf`. That holds today: each batch takes a fresh id.
  - `aof_holds` rereads the AOF files on each poll. They are small here.
- **Evidence** (monoio; `spill-0` frozen 5 s just before the pre-rewrite filler):
  - OLD: red in 5.10 s, "cold-cut-914-rewrite-respill: nothing spilled before the rewrite". This is the Windows message.
  - NEW: green in 6.11 s (final code; 5.95 s on the first version). Filler 0: 200/200 applied.
  - Unstalled: 0.94 s (old 2.66 s).
  - Whole file: 5/5 on monoio and on tokio.

### 2. cold_index_duplicate_resolution_983 `respilled_key_recovers_to_its_newest_copy_after_sigkill_1_shard` (moon#1065)

- **Mechanism, two failures.**
  1. A 1000-round cap on the filler. With the spill thread stalled, the 4096-slot request queue fills and eviction answers `-OOM`, so 1000 rounds go by in about 3 s (L308).
  2. `accepted()` asserted that a filler error contained "OOM" or "backpressure". The refusal text is "AOF fsync failed; write not durable" (moon#1272), so it failed at L220.
  - Also found: a stalled writer makes the spill's `MOON.SPILLED` append fail, and the completion is withdrawn. Measured: `spill_completion_marker_withdrawn:70`, `spill_failed_reinserted:15768`. That leaves unregistered heap files holding the key's bytes. The old raw byte search counted those orphans as copies, which the restart's rebuild never reads.
  - `settle()` was a fixed 1 s wait for completions to reach the manifest.
- **Design.**
  - `fill_until_copies` is bounded by `CONDITION_DEADLINE`, not a round count. Past 1000 rounds, or after a round the server could absorb none of, it trickles (100 ms per round) instead of flooding a stalled disk.
  - A copy is a heap file holding the key that its manifest lists as Active (`registered_copies`, via `ShardManifest::open`, a read-only snapshot). This replaces both the raw count and `settle()`.
  - `SET v1`/`SET v2` go through `write_all`, and `-OOM` still fails them.
  - `wait_aof_holds_last` before the crash.
  - The data dir is removed on a pass. It used to leak; it is still kept on failure.
- **Risks.**
  - `ShardManifest::open` on a live file can read a torn page. That returns Err, which counts as "not yet".
  - The count is stricter than before (orphans no longer count), which is correct, but it is a semantic tightening.
- **Evidence** (monoio):
  - `spill-0` frozen 8 s after `SET v1`:
    - OLD: red in 8.10 s, "phase 1: k983:respilled:key did not reach 1 on-disk copies after 1000 filler rounds".
    - NEW: green in 10.74 s (final code; 12.40 s on the first version). Phase 1: 189 rounds, 8.08 s; filler: sent 18900, applied 11372, -OOM 7528.
  - `aof-writer` frozen 8 s:
    - OLD: red in 8.22 s, "filler SET answered an unexpected error: \"-ERR AOF fsync failed; write not durable\"".
    - NEW: green in 12.99 s (final code), with 3 refusals re-sent after 2 backoffs.
    - An intermediate NEW version, which required every byte-holding file to be registered, timed out: 4 files held the key and 2 were withdrawn orphans. That led to `registered_copies`.
  - Unstalled: 2.99 s (old 4.84 s).
  - Whole file: 4/4 on monoio and on tokio.

### 3. cold_file_id_seed_997_893 `torn_manifest_create_does_not_block_startup_1_shard` (moon#1065)

- **Mechanism.** `write_filler` asserted "OOM" or "backpressure" (L322), with the same round cap.
  - A frozen `aof-writer` alone (8 s, even 20 s) does NOT reproduce it: the loop ends at the first registered spill, before the channel reaches 10,000.
  - A disk stall does: `aof-writer` + `spill-0` + `manifest-sync-0` frozen together, so registration is late and the filler keeps writing until the channel is full.
- **Design.**
  - `write_filler` goes through `write_all` (depth 100).
  - A shared `fill_until(what, done)` with a deadline instead of the round cap, the old 300 ms pause every tenth round, the same trickle, and diagnostics. It is used by the torn-manifest loop and by the vector-segment loop, which had the same cap.
  - `unreadable_cold_dir`'s loop (reached only on the fail-open path) keeps its round bound.
  - `settle()` stays and is documented as not load-bearing: `cold` is read from the manifest after it.
  - The data dir is removed on a pass.
- **Evidence** (monoio; the three threads frozen 8 s at filler start):
  - OLD: red in 18.16 s, "filler SET answered an unexpected error: \"-ERR AOF fsync failed; write not durable\"". This is the Windows message.
  - NEW: green in 22.23 s (final code). 101 rounds, 2 refusals re-sent.
  - Unstalled: 15.24 s (old 12.89 s; the pause every tenth round is new for this loop).
  - Whole file: 10/10 on monoio and on tokio.

### 4. cold_file_id_reuse_1067 `pruned_tombstones_do_not_reissue_a_file_id_across_a_kill9` (moon#1065)

- **Mechanism.** `fill` and `delete` pipelines of 50 were read under the flat 20 s `read_replies` budget, with the replies discarded. Any server stall over 20 s mid-pipeline gives "timed out waiting for 50 replies; got 0 bytes". Two things can cause it:
  - backpressure: 50 writes x 2 s = 100 s is legitimate server behaviour;
  - a runner freeze.
  - Also stall-sensitive:
    - `sleep(1 s)` then one `heap_ids` sample;
    - `sleep(4 s)` for "tombstoned, pruned, persisted";
    - `sleep(1.5 s)` before each SIGKILL;
    - 30 s condition deadlines.
- **Design.**
  - `fill`/`delete` go through `write_all` with `pipeline_budget(50)` = 160 s.
  - `wait_until` with `CONDITION_DEADLINE` and diagnostics everywhere.
  - Heap ids are accumulated across every poll instead of sampled once, which is strictly stronger for the "re-issued" check.
  - Before the DEL wave, `wait_spill_idle` (see below).
  - The 4 s sleep became `manifest_settled`: in the prune arm, every entry of boot 1's ids is gone except the manifest's highest id, which `gc_tombstones` keeps as the high-water mark (#1113). In the control arm, none is Active.
  - The 1.5 s pre-kill sleep became `wait_aof_holds_last`.
- **Two design corrections the evidence forced** (recorded because they matter at review):
  1. Deleting as soon as the first heap file appears (dropping the 1 s sleep outright) left 2 dead cold files that were never reclaimed. Tokio, INFO: `cold_files:2 cold_files_dead:2 cold_files_pending_unlink:0`, `spill_completion_superseded:336`, 120 s. The 1 s sleep was load-bearing: it kept the DELs from racing in-flight spills. `wait_spill_idle` expresses that condition exactly. The dead-file state itself is a product finding (F2 below).
  2. "No boot-1 id left in the manifest" never held: the fixed server keeps the highest id's tombstone. Hence the high-water clause.
- **Evidence** (tokio, the Windows runtime; monoio is red for a pre-existing reason, F1):
  - Whole server SIGSTOPped 25 s at the start of boot 1's fill:
    - OLD: red in 20.77 s, "timed out waiting for 50 replies; got 0 bytes: \"\"". This is the Windows message.
    - NEW: green in 29.88 s (final code).
  - Guard check: NEW against `moon-tokio-debug-1113revert` is red: "LRANGE X read \"*2 a a\" … re-issued: [1, 257, 513, 769, 1025, 1281]". The new waits did not make the guard vacuous.
  - Unstalled: 4.36 s (old 10.66 s).
  - Why 25 s and not 3-5 s: the old failure needs more than 20 s by construction; 25 s is the smallest round stall that reproduces it. The new bound is derived (n x 2 s + 60 s), not tuned to the stall.

### 5. acl_user_revocation `client_kill_by_user_finds_authenticated_sessions_multi_shard` (moon#1273)

- **Mechanism.** `Resp::cmd` returned whatever arrived within a fixed 300 ms, complete or not. It also made every command cost 300 ms.
- **Design.**
  - Read exactly one complete RESP reply (`common::framed_len`) within 20 s (`REPLY_BUDGET`, the same as `Conn`). On timeout, panic with the bytes received and the elapsed time.
  - On EOF or reset, return the partial bytes, so the self-DELUSER guard still reads `""` and fails with its own message.
  - Unread bytes are kept for the next reply.
  - Other callers in the file:
    - `is_closed` is a condition poll; its bound goes from 5 s to `REPLY_BUDGET`, because a cooperative (Windows) teardown needs the server to process the poke.
    - `spawn_moon` used to return `None` (skip) when the server never answered PING. That silently passed the test; it now panics.
- **Evidence** (monoio; whole server SIGSTOPped 2 s at the first command):
  - OLD: red in 0.51 s, "[shards=4] ACL SETUSER alice failed: \"\"". This is the Windows message.
  - NEW: green in 2.16 s (final code).
  - Unstalled: 0.06 s (old 1.96 s).
  - Whole file: 6/6 on monoio and on tokio.

### 6. perf_ws16_bgsave_capture `flushdb_during_a_save_does_not_free_an_unlinked_hash_inline` (moon#1273)

- **Mechanism.** A 50 ms absolute budget on one FLUSHDB (L593). Two things break it without an inline free:
  1. **A stall of the host.** The Windows run measured 85 ms with the fix in place.
  2. **Tick catch-up bursts, found here.** The lazy-free drain runs in 250 µs slices on the shard's 1 ms tick. An interval that missed ticks fires them back to back: tokio's default `MissedTickBehavior::Burst`, a busy or descheduled shard thread, the catch-up after SIGCONT. The queued hash is then freed in one piece AHEAD of the FLUSHDB, which waits for it.
  - Measured: the OLD test run with the whole file on tokio is red 3 of 3 here, "FLUSHDB took 224-241ms". Run alone it is green (0.85-2.0 ms). Monoio with the whole file is green.
  - The same burst empties the other queues after a stall. That made my first timing-only design (the fastest of 3 FLUSHDBs, as a ratio of `DEL`) a FALSE GREEN on the reverted build under the 3 s stall: 3.10 s, 306 µs, 62 µs.
- **Calibration** (debug monoio; bug = the Charge-redirect fix reverted):
  - `DEL` (inline free) of an equal hash: 113-138 ms at 1M fields.
  - FLUSHDB with the queued hash: 0.1-1.1 ms fixed, 56-381 ms bug.
  - FLUSHDB/DEL ratio: at most 0.0013 fixed, about 0.91 bug.
  - `current_cow_size` rises by 0.99-1.00 of one hash within 0-4 ms of a fixed FLUSHDB (review 7 moves the queued charge into the frozen table's bill). With the bug it rises by 134 B.
  - `MEMORY STATS` answers the selected database's ledger synchronously (`db.estimated_memory()`); an UNLINKed value stays charged there until drained.
  - `INFO used_memory` is published on the 100 ms tick and is useless for this. `CONFIG SET slowlog-log-slower-than` is unsupported, and FLUSHDB is not in the SLOWLOG.
- **Design (v5; v1-v4 are in the evidence below).**
  - Build 1M fields. Control: the fastest of 2 synchronous `DEL`s of a `COPY`. A PING floor asserts the control really is an inline free. One hash's charge is weighed from the settled `used_memory` around the first `COPY`.
  - Per round:
    - `COPY` into db 1 and db 2;
    - let the COPY's missed ticks fire while nothing is queued;
    - ONE write of `SELECT/SET small/UNLINK` x 2, then `SELECT 0` and the held `BGSAVE`;
    - wait until armed;
    - per database, ONE write of `SELECT db; MEMORY STATS; FLUSHDB`.
  - Each sample:
    - under half a hash still queued at `MEMORY STATS`: **not judged**;
    - otherwise **SLOW** when `took x 8 >= DEL`;
    - otherwise it passes only if `current_cow_size` rose by at least half a hash (**billed**).
  - The first queued, fast, billed sample passes. `SAMPLES + 1` = 3 queued SLOW samples fail as "freed inline"; a stall's burst can spoil one whole round, and on tokio it even runs between the commands of one write. 3 rounds with neither fail as unjudgeable.
  - Memory: about 384 MB `used_memory` (the old test: 2M fields, about 256 MB). One round when green.
- **Rejected:** `UNLINK` in the same write as the FLUSHDB (v4). An UNLINK during a held save takes 132 ms even for a key created after the epoch (the pre-image capture), so every sample was SLOW on the fixed build.
- **Evidence** (debug, pinned binaries):
  - OLD, monoio, whole server SIGSTOPped 3 s at the FLUSHDB: red in 10.79 s, "FLUSHDB took 4.02s (budget 50ms)".
  - OLD, tokio, whole file, no stall: red 3/3, 224-241 ms.
  - NEW, fixed monoio:
    - no stall: green, round 1 (0.57 ms and 0.30 ms, billed +127 MB);
    - 3 s stall: green in round 2. Round 1: db1 SLOW 3.45 s; db2 "134 B queued: not judged" (the catch-up burst).
  - NEW, fixed tokio, 3 s stall: green in round 2. Round 1: 3.02 s, plus 48 ms with 120 MB queued, both SLOW.
  - NEW, whole file, no stall: tokio 3/3 green (FLUSHDB 13-23 ms under load, billed), monoio 3/3 green.
  - NEW with the Charge-redirect fix REVERTED:
    - red, "4 FLUSHDBs of a database whose hash was still queued took at least 1/8 of the 107ms a synchronous DEL of an equal hash takes: the FLUSHDB freed the UNLINKed 1000000-field hash inline" (94-98 ms and 68-69 ms);
    - with the 3 s stall: red too (4 SLOW over 3 rounds, 2 samples not judged).
  - Unstalled: 5.0 s (old 5.4 s).

## Findings outside scope (reported, not fixed)

- **F1, pre-existing, monoio only.** All 3 `cold_file_id_reuse_1067` tests are red on monoio at `d49a2a98` AND on main `4a96cd5f` (`/home/user/wt/bin/ws20-red-4a96cd5f-debug-monoio`, and `main-4a96cd5f-rel`), deterministically, with io_uring on and with `MOON_NO_URING=1`. The failure is "timed out waiting for boot 1's cold files to be reclaimed".
  - The live server shows `cold_files_pending_unlink:7 cold_dead_slots:1313`: dead files are pending unlink and never unlinked.
  - Tokio passes. Per-PR CI runs tokio only, so only the monoio main-push leg can see it. It needs a product issue.
  - The new harness prints exactly those counters on timeout.
- **F2, product, tokio.** A DEL wave racing in-flight spills leaves dead cold files that are neither pending unlink nor reclaimed for at least 120 s (`cold_files_dead:2`, `pending_unlink:0`). This is the likely cause of the Windows flake "timed out waiting for boot 1's cold files to be reclaimed" in moon#1065 comment 2. The harness now waits for spill idleness, which is the state the test means, so the test no longer depends on it.
- **F3, product.** Under AOF backpressure a spill completion is withdrawn (marker refused) and its file becomes an orphan until the sweep. That is correct, but invisible to anything counting heap files on disk.
- **F5, product, both runtimes (tokio worst).** Shard-tick catch-up bursts turn the sliced lazy-free drain into one synchronous free ahead of the next command. tokio's `interval` defaults to `MissedTickBehavior::Burst`; monoio catches up after a stall too.
  - Evidence: the OLD ws16 test run with the whole file on tokio is red 3/3 at 224-241 ms, on an already-drained queue.
  - This is the likely real cause of the Windows "FLUSHDB took 85ms". It is the latency moon#1190 exists to avoid, reintroduced by the tick.
  - Suggest `MissedTickBehavior::Skip` (or Delay) for the shard tick, or a cap on lazy-free work per wake.
- **F4, moon#1272.** The refusal text is shared with a real fsync failure. The harness retries it until its deadline, so a genuine fsync failure now surfaces after 120 s with diagnostics instead of at once. Narrow `slow_host::AOF_REFUSAL` when moon#1272 gives the refusal its own error.

## Gates (final tree, exit codes captured directly)

- `cargo fmt --check`: 0.
- `bash scripts/audit-test-tempdirs.sh`: 0 ("PASS: no fixed-name temp paths under src/").
- `cargo clippy --all-targets -- -D warnings` (default features, monoio): 0, no warnings.
- `cargo check --all-targets --no-default-features --features runtime-tokio,jemalloc`: 0.
- Whole files, pinned debug binaries (`winci-debug`, `winci-debug-tokio`):

| suite | monoio | tokio |
|---|---|---|
| acl_user_revocation | 6/6 | 6/6 |
| perf_ws16_bgsave_capture | 8/8 (4 of 4 runs of v5) | 8/8 (4 of 4 runs of v5) |
| cold_cut_single_shard_914 | 5/5 | 5/5 |
| cold_index_duplicate_resolution_983 | 4/4 | 4/4 |
| cold_file_id_seed_997_893 | 10/10 | 10/10 |
| cold_file_id_reuse_1067 | 0/3, pre-existing F1 (the old file is also 0/3 here and at main) | 3/3 |

- `tests/common` changed additively (a new `slow_host` submodule; no existing helper touched). Clippy and the tokio check compile every suite that includes it.
- Not run: Windows, MSRV, the monoio self-hosted leg. The hosted matrix needs `gh workflow run ci.yml --ref fix/winci-harness`; GitHub is read-only for this workstream.

## Self-evaluation (0-1: Completeness, Clarity, Practicality, Optimization, Edge cases, Self-evaluation)

| item | C | Cl | P | O | E | S | note |
|---|---|---|---|---|---|---|---|
| 914 | 0.95 | 0.9 | 0.95 | 0.9 | 0.9 | 0.9 | respill non-vacuity added; kill-cycle legs share the fix |
| 983 | 0.95 | 0.9 | 0.9 | 0.9 | 0.95 | 0.9 | orphan copies were a latent vacuity hole, now closed |
| 997 torn | 0.95 | 0.9 | 0.95 | 0.9 | 0.9 | 0.9 | needed a disk-wide stall to reproduce; `settle` kept, documented |
| 1067 | 0.9 | 0.9 | 0.9 | 0.9 | 0.95 | 0.9 | green on tokio; monoio red is pre-existing (F1); guard proven against the #1113 revert |
| acl | 1.0 | 0.95 | 1.0 | 0.95 | 0.95 | 0.95 | the silent skip removed too |
| ws16 | 0.95 | 0.9 | 0.9 | 0.9 | 0.95 | 0.9 | four quadrants proven on monoio plus the stall on tokio; three designs rejected on evidence |

Refinements made when an item scored below 0.9:
- ws16:
  - v1 (timing only) was false-green under stall plus revert;
  - v2 set UNLINK too early, so every sample was drained;
  - v3 flaked on tokio with the whole file (tick bursts);
  - v4 (UNLINK in the FLUSHDB write) is slow even when fixed.
  - v5 (the per-sample `MEMORY STATS` validity check) is the one committed.
- 983: the manifest wait, then registered copies only.
- 1067: spill idle, and the high-water clause.
