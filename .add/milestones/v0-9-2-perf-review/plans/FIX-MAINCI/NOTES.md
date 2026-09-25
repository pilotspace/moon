# FIX-MAINCI: working notes (ADD discipline, not a shared artifact)

## Context loaded

- Brief: `/home/user/wt/prompts/FIX-MAINCI.txt`, plus the orchestrator's mid-task additions: two PR #1242 flakes, and hrandfield as the top item-4 case.
- Personas: `ci-test-integrity-engineer` (lead) and `storage-durability-engineer`.
- Base: `07d9850`. Ports 7580-7599. Target dir: `/home/user/wt/target`, shared.
- Artifact aliasing: the shared target gives every worktree's moon lib-test binary the SAME filename. Another agent's build overwrote mine once (my strings probe found no FIX-MAINCI path). From then on, every measured binary was built with `--config 'profile.dev.package.moon.codegen-units=255'`. That setting is moon-only and changes only moon's unit hash. The binary gets a private name, deps stay shared, and each run is checked for a marker of this tree.
- Hosted logs read (read-only), run 36050539990 at `ae21476`:
  - macOS 2/3, job 107804689523;
  - macOS 1/3, job 107804689104;
  - Windows 1/3, job 107804689361;
  - Windows 3/3, job 107804689382;
  - PR #1242 Check, job 107965504992.

## Item 1: `metadata_checksum_matches_the_pre_streaming_formula`

### Mechanism, verified

- The hosted failure is the first case, `9 100 TurboQuant4 Exact`, at `collection.rs:946`. macOS gave `12998692082669032190` and Windows `2952564279481642555`, against 6002226874277776562. The loop panics at the first mismatch, so the other cases were never reached.
- `compute_checksum` streams `qjl::for_each_qjl_chunk`: LCG uniforms, then Box-Muller with `f32::ln`, `cos` and `sin`. Those lower to the host libm's `logf`/`cosf`/`sinf`. The `-O` build merges sin+cos into `sincosf`; the `-O0` build calls `sinf`/`cosf`, checked with `nm -D` on a probe.
- Scratch probe (`scratchpad/libm/qjl.rs`): the verbatim generator over the three EXACT golden cases, 5,978,240 values and 2,989,120 `logf` calls.
  - glibc 2.39 `logf` differs from the correctly rounded result (f64 `ln` rounded to f32) on 21,593 inputs (0.72%). `r*cos` differs on 44,337 values, `r*sin` on 44,572. glibc's float functions are faithful (< 1 ulp), not correctly rounded, so any other faithful libm (Apple, the MSVC UCRT, musl, a CORE-MATH glibc) is expected to differ on some of the ~6M values. The digest then differs, which is what the hosted legs show.
  - `GLIBC_TUNABLES=glibc.cpu.hwcaps=-AVX2,-FMA,-FMA4` takes effect: ld.so diagnostics show the FMA bit (leaf1 ECX b12) and the AVX2 bit (leaf7 EBX b5) cleared. The probe then gives bit-identical streams (same FNV per case). glibc's SSE2 and FMA ifunc variants agree on every value. `-O` (sincosf) and `-O0` (sinf/cosf) also agree.
- In-tree emulation (temporary, reverted): `MOON_TMP_LIBM_CR` switched the generator to a correctly rounded libm.
  - The three EXACT digests changed: `18283839132677992913`, `1606151285718504467`, `15717723196014128133`.
  - LIGHT and SQ8 were unchanged. Those two cases hash only integer-LCG sign flips and a codebook of `RAW * (1/sqrt(padded))`, which is IEEE-exact and portable.
  - The new test, with the pin disabled (`MOON_TMP_UNPIN`), still failed on `qjl_stream_chunks_concatenate_to_the_reference_generator` (dim=64). That failure is correct: the emulation changes the stream but not the reference.

### Design

- Oracle `one_shot_checksum`: f32546c's formula rewritten in the test. Same fields and order, then the materialized `qjl_matrices()` as LE f32, then one-shot `xxh64(.., 0)`. The test asserts `compute_checksum == oracle` for every case on every platform.
- The oracle and the streamed checksum share the generator, so the generator's fidelity to history is pinned separately, on every platform, by `qjl::tests::qjl_stream_chunks_concatenate_to_the_reference_generator`. That test holds a verbatim copy of the pre-moon#1213 generator; the doc comment says so.
- Golden pins:
  - EXACT goldens only under `cfg!(all(target_os="linux", target_arch="x86_64", target_env="gnu"))`, where they were computed. `target_env="gnu"` refines the brief's cfg: a musl or any non-glibc Linux x86_64 has a different libm.
  - LIGHT and SQ8 goldens on every platform: no libm, so portable.
  - The test also asserts that "hashes QJL" is exactly `Exact && TurboQuant`, so the split cannot silently drift.

### Evidence (same private debug build)

- Normal run: all five digests equal the goldens, and `qjl_stream…` passes.
- Mutation, dropping each streamed chunk's last value (`MOON_TMP_STREAM_BUG`): red, `9 100 TurboQuant4 Exact: the streamed checksum differs from the one-shot formula` (7741246797399781692 vs 6002226874277776562). The oracle catches a streaming bug with the pin out of the picture.
- Correctly rounded libm emulation: the linux-gnu pin goes red (`checksum drifted`). That is the portability signal the pin exists for. With the pin disabled, the oracle assertion stays green for all five cases, which models what macOS and Windows will now do.

### Product consequence (for the issue)

See SUMMARY.md, "Item 1 — product consequence".

### Risks

- macOS and Windows cannot be run here. The oracle assertion is libm-independent by construction: same host, same generator, both sides. The LIGHT/SQ8 goldens are claimed portable from IEEE semantics (sqrt, mul, div correctly rounded; no transcendental). That claim is not verified on hardware; re-check the hosted legs.
- The Linux aarch64 pin is deliberately NOT enabled. glibc aarch64 builds the same generic C, and the x86_64 SSE2/FMA equivalence suggests identical bits, but that is unmeasured. No aarch64 machine or qemu is available here.

### Score

| Completeness | Clarity | Practicality | Optimization | Edge cases | Self-eval |
|---|---|---|---|---|---|
| 0.95 | 0.92 | 0.95 | 0.9 | 0.9 | 0.9 |

Edge cases stay at 0.9 because aarch64 is unmeasured, and that cannot be closed from here.

## Item 2: `append_growth_is_amortized_linear` (Windows)

### Mechanism, verified where possible

- Hosted Windows 3/3 at `ae21476`: `APPEND onto 4 MB cost 20.6x (93581900 vs 4547900 ns)`, then 19.6x and 19.5x. Lib tests use the system allocator (`main.rs` alone sets `#[global_allocator]`; the shipped Windows binary uses mimalloc, since jemalloc does not build on MSVC).
- `string_grow_with` grows by `reserve_exact` (realloc), so its amortization is the allocator's. glibc grows the 4 MB block in place: 1.0x here in every configuration.
- Windows arithmetic: t0 = 4.5 ms for 2000 calls, ≈ 2.3 µs per call. t1 − t0 ≈ 89 ms for 200 KB of growth. A move per 4 KiB page crossed (~49 moves) costs ≈ 1.8 ms each, plausible for a 4 MB VirtualAlloc + copy + page faults. A move per call would be ≥ 0.8 s (8 GB copied), and a move per 64 KiB (3 moves) would need ≈ 30 ms per 4 MB copy. The per-page model is therefore an inference from timings, not a measurement of HeapReAlloc.
- Linux emulations (temporary `MOON_TMP_GROW` in `string_grow_with`, and `append_head`, HEAD's body verbatim):

| Variant | start / chunk / n | ratio |
|---|---|---|
| shipped | all 5 sweep rows | 1.0x |
| "win": move on each 4 KiB crossing above 1 MiB | 10K / 100 B / 2000 | 24.7-26.1x (reproduces the hosted ~20x) |
| "win" | 10K / 8-16 B | 1.5-2.1x |
| "quad": 2 copies per call inside grow | 100 B | 161x |
| "quad" | 8-16 B | 174-230x |
| HEAD body `append_head` | 100 B | 128-134x (in-sweep) |
| HEAD body `append_head` | 8 B | 148x (in-sweep); 265-276x in the final test run alone |

- Final test at 8 B / bound 8 (5 runs each): shipped passes 5/5; "win" passes 5/5 (as designed, see below); "quad" fails 5/5 (339-398x); HEAD body fails 5/5 (265-276x).

### Design

- Option (a) is not feasible within the constraints. Genuine amortization under any allocator needs a capacity the value can find again. The 16-byte `CompactValue` has no room for one. A capacity derived from the length (size-class rounding) would make the heap string's allocation size differ from the `Box<[u8]>` length every consumer reconstructs (`take_heap_string` → `Box`, `into_redis_value` → `Bytes::from(Box)`). Dealloc with the wrong layout is UB, so fixing it means changing the unsafe heap-string module: new unsafe, and outside scope.
- Option (b), refined:
  - Chunks of 8 B, not 100 B. The ratio then measures work per CALL, which is what the regression adds. HEAD's ratio rises from ~130x to ~270x, and the allocator's per-byte behaviour mostly drops out.
  - Bound 8x off Windows, 64x on Windows (the brief's figure). 64x has a wide margin over the predicted ~2.7x, and HEAD on Windows pays two fresh 4 MB VirtualAllocs per call, so it is far above 64x.
  - What the 8 B chunk gives up: on Linux it no longer flags an allocator that moves per page. That is an allocator property, not moon code, and it was never the test's target. The "win" emulation passing at 8 B is exactly this trade.
- `string_grow_with` doc: the amortization argument is the allocator's. It holds for jemalloc and mimalloc (the shipped allocators) and does not hold for the Windows system heap, which only lib tests use.

### Risks

- The Windows ratio at 8 B chunks is a prediction (~2.7x). The 64x bound covers an error of more than 20x. Re-check the hosted Windows leg.
- The mimalloc claim (`mi_realloc` keeps a block while `newsize <= usable && newsize >= usable/2`) comes from mimalloc's source as I know it. I did not measure it in this task.

### Score

| Completeness | Clarity | Practicality | Optimization | Edge cases | Self-eval |
|---|---|---|---|---|---|
| 0.9 | 0.92 | 0.95 | 0.9 | 0.9 | 0.9 |

Windows is unmeasured here; that is the ceiling.

## Item 3: `test_1221_r2_poison_is_checked_before_the_watermark` (macOS)

### Mechanism, verified

- The hosted panic is at `rotation_tests.rs:519:27`, the SECOND `request_sync().unwrap()`, 3/3. That is after `gate.fail = true`, not before it: the WAL was poisoned before the test's own failing request.
- `wait_durable(10)` found LSN 10 `Pending`, because the agent's fsync from `request_sync` was still in flight (F_FULLFSYNC on macOS takes ms). It then calls `request_sync()`, which queues a SECOND sync request (upto 10): `request_sync_current` cannot tell whether the in-flight request covers the LSN. `wait_watermark(10)` returns on the first publish.
- The test then sets `gate.fail`. The agent dequeues the duplicate, reads `fail == true`, the injected failure fires, and the WAL is poisoned. The test's `request_sync()` then sees `durability_poisoned()` and returns `poisoned_error()`, which is the exact message logged.
- On Linux the agent usually reads `fail` before the test thread wakes, so the race is won; on the macOS runner it lost 3/3.
- Forced ordering on Linux (temporary `MOON_TMP_GATE_DELAY`: 30 ms on the first fsync, like F_FULLFSYNC; 30 ms before the second call's `fail` read, like a preempted agent; the test pausing 100 ms after arming):
  - the old body failed 5/5 at the second `request_sync().unwrap()` with the hosted message;
  - the fixed body passed 5/5;
  - the fixed body with the same pause passed 5/5.
- The code is right. A redundant fsync is harmless, and an injected failure of a real fsync poisons the WAL correctly. There is no spurious poison, no F_FULLFSYNC failure (that would show "sync agent poisoned" from `wait_watermark`, not this message) and no process-global state (the Watermark is per agent). The test's assumption, "no fsync is in flight after `wait_durable` returns", is what was wrong.

### Design

- Arm the failure only when the agent is provably idle:
  1. `request_sync()`;
  2. `wait_until(durable_lsn() >= 10)`, the raw watermark, which queues nothing;
  3. `wait_durable(10)`, now answered by the watermark fast path;
  4. assert `gate.calls() == 1`, so exactly one fsync ran and it succeeded;
  5. arm;
  6. `request_sync()`;
  7. wait for the poison;
  8. assert `gate.calls() == 2`, so the failure is the requested fsync;
  9. assert watermark ≥ 10, so LSN 5 is under the watermark;
  10. then `wait_durable(5)` must fail, and `request_sync` must fail.
- Stronger than before. The test now proves that the refused LSN IS under the watermark, which the old body only implied.
- Mutation `MOON_TMP_WM_FIRST` (the PR-head fast path: watermark before poison in `wait_durable`): red, "a poisoned WAL must fail every durability wait, below the watermark too".
- Follow-up, not in scope: `wait_durable` queues a redundant fsync whenever the covering one is still in flight, which costs an extra F_FULLFSYNC on macOS. It could track the highest requested LSN. Reported, not changed.

### Score

| Completeness | Clarity | Practicality | Optimization | Edge cases | Self-eval |
|---|---|---|---|---|---|
| 0.95 | 0.93 | 0.95 | 0.92 | 0.93 | 0.92 |

## Item 4a: hrandfield_1171 (orchestrator's top item-4 case; blocked #1242 CI)

### Mechanism, verified

- The failure was 4.8x against a common 5x bound. The COUNT form already makes ONE sorted walk (`resolve_picks`).
- That walk reaches the largest of k distinct uniform picks, n·k/(k+1) = 5n/6. Its ratio against `entries()` is therefore 1.2 × clone/step. Hosted: 144 ns per clone, 36 ns per step, so 4.8x, exactly the failure.
- A std HashMap has no random access, so a sub-linear draw is out of scope.

### Design

- **Code.** Cache the next wanted position in the walk closure. It used to re-read `picks[order[next]]` per pair.
  - Same-build A/B: 4.4 → 3.4 ns per step in release-fast, 20 → 17 ns in debug.
  - A Map-only direct loop was tried and dropped: it was only faster in debug.
- **Test.** The bound is per form, derived in the doc comment. The single field keeps 5x; COUNT 5 gets 2x.

| Build | Single field | COUNT 5 |
|---|---|---|
| Debug | 12-21x | 5.8-7.7x |
| Release-fast | 80-138x | 20-23x |

- **Mutation.** The count path materializing through `entries()` first gives 0.87-0.90x, red.

### Score

| Completeness | Clarity | Practicality | Optimization | Edge cases | Self-eval |
|---|---|---|---|---|---|
| 0.93 | 0.92 | 0.95 | 0.9 | 0.9 | 0.9 |

## Item 4b: perf_ws15_spanning_cold_del → product bug moon#1253 (P0-class resurrection)

### Mechanism, verified

- Windows at `c111e6b`: "3 of 100 DELeted db-3 probes came back". It is general, not Windows-specific.
- Reproduced on Linux by removing the 8 s settle:
  - tokio: 4/4 red, 6-9 probes back;
  - 200 ms settle: 2/3;
  - 1 s and 8 s settles: 0/5.
- Forensics on a failing image (shard 1, probe:4):
  - the probe's slot is in heap-001281;
  - the new generation opens with COLDCUT 1512 and has no DEL probe:4;
  - `MOON.SPILLED 1281` appears in the NEW generation, 229 keys, probe:4 not among them.
- So request 1281 was in flight at the DEL and still at the fold. Its completion was applied after the fold, noting probe:4 as a ghost too late for the head.
- On restart, file 1281 is authorized wholesale, first by COLDCUT (1281 < 1512) and again by its marker. `ReplayColdGate.authorized` is per file.
- FLUSH (`clear()` dropping every in-flight record) had the same hole.

### Design (approved by the orchestrator in three file regions)

- `Database::spill_superseded: HashMap<(Bytes, req_id), ttl>` holds every request that `spill_inflight_forget` or `spill_inflight_mark` retired while it was in flight. `mark` does this when it replaces an older request; `forget` covers DEL, overwrite and promotion. `clear()` calls `spill_inflight_supersede_all`.
- The fold (`for_each_cold_delete_chunk`, `fold_cold_deletes`) chains those keys after the ledger keys. Each gets a head DEL if it is not alive at the fold instant.
- An alive key (overwritten, promoted, or with a newer request in flight) gets no DEL: the base or the newer copy shadows the old slot. A superseded key is never re-imaged from its payload, because its record is gone.
- Region 3 (`persistence_tick.rs`) settles the entry on every completion outcome:
  - publish/ghost/withdrawn entries;
  - failed write;
  - `rehydrate_unpublished_spill`, which also covers id-rejected.
- Accounting (after the #1242 hosted catch, 1fc77c3): the handles are counted in their own `spill_superseded_bytes`, NOT in `pending_spill_bytes`. moon#466's FLUSH-drops-the-charge contract and `used_memory`=0 after FLUSHALL both hold.

### Evidence

- **Lib.** `superseded_spill_tests`, 7 cases on the production fold and recovery. The three dead cases are red when the fold part is removed.
- **Settle.** `superseded_settle_tests` goes red when the publish-path settle is removed.
- **Real server, both runtimes:**
  - ws15 with no settle: tokio s4 11/11, s1 5/5; monoio s4 4/4, s1 3/3. With the fold part off: tokio s4 3/3 red, s1 2/3 red; monoio s4 2/2 red, s1 2/2 red.
  - New `crash_recovery_cold_del_inflight_1253`, #[ignore]d and in the nightly: green on both runtimes and both layouts. With the fold part off, DEL and FLUSHALL go red (1-12 probes back); overwrite stays green, as designed.

### Risks

- The remaining window is between the completion's apply and its ledger note. Both run in the same synchronous section, so there is none.
- The WAL-v3 (no AOF) recovery path is not touched: the ledger only feeds AOF folds.
- WS19 (moon#1240) moves reclaim writes onto the completion path; the settle calls must stay on every outcome after that merge.

### Score

| Completeness | Clarity | Practicality | Optimization | Edge cases | Self-eval |
|---|---|---|---|---|---|
| 0.93 | 0.9 | 0.93 | 0.92 | 0.9 | 0.9 |

Edge cases stay at 0.9: WS19's pending refactor of the completion path must keep the settle-on-every-outcome invariant, which the leak test guards.

## Item 4c: LFU OBJECT FREQ (Windows)

- The failure is pure Morris-counter randomness. There is no clock or platform input: every GET records, and the clock is pinned.
- Exact Markov computation: P(f<8 | 200 GETs) = 1.28e-4, and P(f<8 | 1000 GETs) = 1.4e-21.
- Fix: 1000 GETs with the same floor. It still fails HEAD's constant 5. 300/300 green.

## Item 4d: perf_ws12 aborted BGSAVE fixture (macOS)

- The fixed 10 ms sleep let FLUSHALL run before the shard's tick armed the epoch.
- Fix: poll for `shard-0.rrdshard.tmp`, which is created in the shard-thread stretch that arms the epoch. Tokio 3/3.
- Not reproducible from outside the server (the tick cannot be delayed), so the old failure was not forced.

## Item 4e: perf_ws8 spanning MSET capture (PR #1242 hosted flake)

- An MSET during a save costs 7-19 ms in local debug and ~100 ms hosted. Racing the epoch is inherently timing-dependent: pipelining alone still missed 156 ms and 341 ms epochs.
- Fix: the test-only `MOON_TEST_SNAPSHOT_HOLD_FILE` hook (5bf5107) holds the armed epoch's segment advance while a file exists.
  - The test pipelines 16 (MSET, INFO) pairs. Each INFO must see the save in progress.
  - It asserts the hold holds, releases it, and judges the restore.
- Tokio 3/3, ~10 s each. Red 2/2 with the pre-moon#1228 capture-free local leg restored, "135 of 400000 keys" both times: deterministic.

## Item 4f: pre-existing flakes (cause only, not fixed)

- `cold_file_id_reuse_1067` (Windows): a reclaimed file is still delete-pending when its id is reused, or the reclaim tick has not run by the probe. Timing, not id reuse.
- `tracking_expiry_invalidation_1013`: the test pumps replies for a fixed 150 ms, and a loaded runner delivers the invalidation later.
- `hash_field_expiry_invalidates_tracking_clients`: the active `expire_cycle` has a wall-clock budget, so on a slow runner the field expires on a later cycle than the test waits for.
- `cold_cut_single_shard_914`: a fixed 1 s wait for a spill to land.
- Seen in this batch's integration runs, unrelated:
  - `cold_index_rebuild_silent_drops_875` fails only as root (chmod 000 is still readable);
  - `cold_shadow_single_shard_tokio` misses its spill precondition under box load (2/3 alone);
  - the vector `bg_compact` parallelism test is CPU-timing and fails only under load (3/3 alone).

## Item 4g: moon#466 regression (the #1242 hosted catch, 1fc77c3)

- **Mechanism.** The first #1253 cut billed superseded handles into `spill_inflight_bytes`. FLUSHALL then left `pending_spill_bytes() > 0`, and `eviction_accounting::flush_retires_in_flight_spills_and_their_byte_charge` went red: moon#466 says FLUSH drops the charge, and `used_memory` must be 0 afterwards, as in redis.
- **Fix.** A separate counter, `spill_superseded_bytes`. The pending charge is untouched, and the superseded bytes are still accounted, just apart.
- **Evidence.** eviction_accounting 5/5 green. `superseded_entries_are_counted_apart_and_settle_to_zero` pins both counters, including FLUSH.

## Review of #1253 (items R1-R6)

### R1: SWAPDB strands a superseded request (889a884)

- **Mechanism.** `spill_superseded` lives in `Database`, so `SWAPDB` (whole-`Database` swap) moves it. A completion carries the db index the request was made in and settled only there, so it stayed in the swapped db until restart. It was counted in `spill_superseded_bytes` and scanned by every fold of that db.
- **Design.** `spill_superseded_settle` returns whether it hit.
  - On the not-newest path of the three completion sites, a miss falls back to `settle_superseded_elsewhere`, which tries the shard's other dbs. The call sits outside the `with_shard_db` closure, because re-entrancy is forbidden.
  - Request ids are unique per shard, so the first hit is the record.
  - The set stays per database, not per shard: a per-shard set would need a db index per entry for the fold's head DEL, and the swap makes that stale in the same way.
- **Risks.**
  - A newest record moved by the same swap is still ghosted by its completion and stays in the swapped db until the key is next written. That is moon#1237 territory (SWAPDB with cold data), pre-existing, and not changed here.
  - After such a record is later forgotten, its superseded entry can never settle. It is bounded per event, and its key is alive-filtered at the fold, so it produces no wrong DEL.
- **Evidence.**
  - The reviewer's test, extended to all 3 sites: red before (left 3), green after on both runtimes.
  - Removing the fallback at any single site gives red (left 1).
  - Full lib: monoio 6613/0, tokio 5675/0.
  - Real server on the final binaries: in-flight crash suite, ws15 and moon#1215 crash suite all green on both runtimes and both layouts.

| Completeness | Clarity | Practicality | Optimization | Edge cases | Self-eval |
|---|---|---|---|---|---|
| 0.9 | 0.92 | 0.95 | 0.95 | 0.88 | 0.9 |

Edge cases stay below 0.9 because the stranded NEWEST record (above) is left to moon#1237. Fixing it means deciding which db a swapped in-flight spill belongs to, which is that issue's design question.

### R2: bound the superseded set (DEFERRED, orchestrator: no edit to `spill_thread.rs`, WS19's area)

The design, exactly as sent:

**Design, a watermark.**
- The spill thread handles requests FIFO and flushes its whole buffer at a time.
- After each flush's `send_completions` returns, it stores `done_through = max request id in that flush` into an `AtomicU64` in `SpillThread` (Release). By then every request ≤ that id has had its completion sent, or dropped at shutdown.
- In `apply_spill_completions`, the shard loads `done_through` (Acquire) BEFORE draining the channel. Once the drain is applied, every request ≤ that value has been applied or is gone for good, so superseded entries with `req_id ≤ it` can be pruned.
- A pruned entry never reopens the fold window: its completion was already applied (settled), or its file was never listed, and the orphan sweep reclaims it.
- A dead spill thread (`join_handle.is_finished()` while not stopping) clears every superseded entry: no completion will ever arrive, and none of their files get listed.
- Cost: one atomic load per drain. The per-db prune runs only when a db's set is non-empty.

**Edits.**
- `spill_thread.rs`: the AtomicU64 plus its store in `run()` after each flush, plus `done_through()` and `is_dead()` accessors. About 20 lines.
- `persistence_tick.rs::apply_spill_completions`: about 6 lines.
- `storage/db/mod.rs`: `spill_superseded_prune_through(req_id)` and `spill_superseded_clear()`.

What stays unbounded until then: an entry whose completion never arrives (a dropped completion, or a dead spill thread). The reviewer's `q6_a_delete_whose_completion_never_arrives_strands_a_charge` shows it. Each such entry is one key handle plus `SPILL_SUPERSEDED_OVERHEAD`, counted in `spill_superseded_bytes`.

### R3: crash suite -OOM (8599cc1)

- **Mechanism.** After the filler, the 8 MB allkeys-lru server can answer `-OOM` while its spill channel is full. The overwrite case asserted `+OK`. The reviewer saw 1 failure in 4 runs, release-fast, `--shards 1`, CPU load.
- **Design.**
  - Retry `-OOM` up to 200 times, 5 ms apart.
  - A SET still refused never happened, so its probe is judged against its original value.
  - Any other reply fails, and a round with every mutation refused fails.
- **Evidence.** Green on both runtimes and both layouts, including 4 concurrent runs per runtime and a 1 MB maxmemory variant.
- **Not verified.** No run here ever answered `-OOM`, so the retry path holds by construction only.

| Completeness | Clarity | Practicality | Optimization | Edge cases | Self-eval |
|---|---|---|---|---|---|
| 0.9 | 0.93 | 0.95 | 0.9 | 0.9 | 0.85 |

Self-eval 0.85: the retry path could not be driven live on this host.

### R4: crash-matrix.yml comment (aac5887)

- The comment was wrong about which layouts run. The nightly binary is monoio: s4 is the per-shard fold and s1 is TopLevel. The tokio flat-file fold has no real-server leg.
- The fix is comment only. Adding a tokio leg is left to the owners of the nightly budget.

### R5: prove the window was hit (9e531bb)

- **Design.** Each round reads INFO `spill_completion_superseded` before BGREWRITEAOF and after the rewrite. Each test needs a rise in at least one round.
- **Measured.** 33 of 36 rounds rose; zero-rise rounds were isolated.
- **Vacuity probe.** An 8 s settle after the filler reads [0,0,0] and fails on both runtimes, while every probe reads right.
- **Limit.** This is a black-box lower bound: a completion between the INFO read and the fold counts, but it was not in the window.

| Completeness | Clarity | Practicality | Optimization | Edge cases | Self-eval |
|---|---|---|---|---|---|
| 0.9 | 0.92 | 0.93 | 0.95 | 0.88 | 0.9 |

Edge cases 0.88: on a much slower runner all 3 rounds can read 0, and the test then fails loudly. That is intended, but it is a new way for the nightly to go red.

### R6: moon#1255, an expired in-flight record is retired when its key is re-created (a5b9627)

- **Mechanism.** `promote_inflight_if_present` returned early on an EXPIRED record without retiring it. A collection write (RPUSH, HSET, and so on) re-created the key hot. The request's completion then found its record still newest and published the stale slot as the key's cold entry behind the live value, logging its `MOON.SPILLED` marker after the write. Replay then dropped the write.
- **Fix.** The expired record is retired through `spill_inflight_forget`, so it is superseded, and the completion ghosts the slot.
- **Evidence.** Mutation red on 3 tests (`inflight_expiry_1255_tests` ×3 + the completion-half settle test). Green on both runtimes.
- **Not changed.** The replay-side half: a log the PRE-fix server wrote still replays the demote. Fixing that is a replay-semantics change, the orchestrator's call.

| Completeness | Clarity | Practicality | Optimization | Edge cases | Self-eval |
|---|---|---|---|---|---|
| 0.9 | 0.9 | 0.95 | 0.95 | 0.9 | 0.9 |

