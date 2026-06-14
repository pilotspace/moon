# TASK: WAL group commit: batch concurrent pending writes into one fsync under appendfsync=always

slug: wal-group-commit · created: 2026-06-14 · stage: production · risk: high · autonomy: conservative
phase: verify   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: WAL **group commit** — coalesce concurrent pending AOF writes into a SINGLE fsync
under `appendfsync=always`, on BOTH writer tasks (`aof_writer_task` TopLevel + `per_shard_aof_writer_task`),
via **opportunistic drain** (after one message, drain whatever else is queued up to a bounded cap,
write all, ONE fsync, ack all). Today the writer fsyncs once per `AppendSync` (writer_task.rs:198–232,
694, 1042) — the ~11× write penalty (135K→12K ops/s). Group commit makes the per-write fsync cost
amortize across write concurrency, WITHOUT weakening the fsync-before-ack durability contract, the
C4 cross-shard fold, the H1-BARRIER, or the everysec/no paths.

Ground truth (investigation 2026-06-14, file:line): each connection is its OWN async task; the write
path `pool.try_send_append_durable(shard, lsn, bytes).await` (handler_sharded/mod.rs:1123/1173/1419,
handler_monoio/mod.rs:1137/1198/1400, handler_single.rs:74) yields that task on the await, so N
concurrent durable writers DO enqueue N `AppendSync` messages into one writer channel while the writer
is busy — there IS a batch to coalesce. The channel is a MIXED stream: fire-and-forget `Append`
(everysec/no + the C4-FOLD-FIX cross-shard arm, pool.rs:281–298), durable `AppendSync{ack}` (Always +
the zero-length H1-BARRIER `fsync_barrier`, pool.rs:315–323 — "writer fsyncs all preceding Append then
acks Synced"), and control (`Rewrite`/`RewriteSharded`/`RewritePerShard`/`Shutdown`). A single batch
fsync already covers all preceding `Append` by the ordered-channel property the H1-BARRIER relies on —
group commit GENERALIZES that barrier to "one fsync per drained batch, ack every AppendSync in it".

Framings weighed: opportunistic drain on the writer-consumer (CHOSEN — user 2026-06-14: zero added
latency, batches exactly when concurrency exists) · time-window coalesce (REJECTED — adds latency to
every durable write even at C=1) · count/byte threshold w/ fallback timer (REJECTED — same latency
risk + more moving parts). Scope: BOTH writers (CHOSEN — TopLevel covers shards=1, per-shard covers
the multi-shard story) · per-shard only (declined — leaves shards=1 at 11×).

Must:
<must>
  - M0 (baseline, per-runtime relative anchor): re-establish the `appendfsync=always` "before" on
    moon-dev at pipeline depth >1 with C>1 concurrent writers (the cell the win lives in),
    fresh-server-per-rep, best-of-N, both runtimes; record `everysec` + `no` as unchanged controls.
    Throughput (fsync-bound) is less VM-vCPU-sensitive than the xshard ABSOLUTE latency was, but state
    the win as a RELATIVE before/after ratio on the same instrument (milestone bench rule).
  - M1 (the win): under `appendfsync=always` with C>1 concurrent durable writers, when K `AppendSync`
    messages are queued the writer writes all K then makes them durable with ONE `flush()+sync_data()`,
    and acks all K waiters `Synced` only AFTER that fsync returns — ⌈writes/batch⌉ fsyncs, not one per
    write. Measurable throughput gain at pipeline>1 / C>1 vs M0 on BOTH writers, BOTH runtimes.
  - M2 (durability invariant — the freeze-first contract): a waiter is acked `Synced` ONLY after the
    fsync covering its bytes returns; a crash before that fsync loses only UNACKED writes (client never
    saw +OK ⇒ exactly-once preserved on retry). The one batch fsync also covers every preceding
    fire-and-forget `Append` in the drained batch (H1-BARRIER ordered-channel property preserved). The
    existing crash-matrix suites stay green AND a new concurrent-writers crash test asserts: every
    ACKED write survives SIGKILL, no UNACKED write is double-applied on replay.
  - M3 (ordering / control-message safety): `Rewrite`/`RewriteSharded`/`RewritePerShard`/`Shutdown`
    and the C4 fold protocol are NEVER absorbed into a batch — when one is encountered during a drain,
    the in-progress batch is flushed (write + single fsync + ack all) BEFORE that message is handled.
    Channel message order is preserved exactly; the C4-FOLD `pending_aof_count` accounting is unchanged.
  - M4 (no-regression guardrail): `everysec`/`no` are byte-for-byte unchanged (group commit engages
    ONLY under `Always`); the everysec 1s deadline flush + bounded-recv (shardslice P0 8KB-tail fix)
    still hold; a lone C=1 `Always` writer fsyncs immediately with NO added latency vs M0; batch size
    is BOUNDED (a cap on count and/or bytes) so a write flood cannot delay an early waiter's fsync
    unboundedly nor grow the buffer without limit; `scripts/test-consistency.sh` 197/197 @1/4/12;
    dual-runtime green; clippy ×2 + `fmt` clean; zero new `unsafe`; zero new cross-thread lock; RSS
    not regressed.
</must>
Reject:
<reject>
  - a `write_all` failure for any message in a batch -> that waiter (and the batch) is acked
    `WriteFailed`, never `Synced`; appends after a persistent I/O error stay dropped (existing
    `write_error` latch) -> "batch_write_failed"
  - the batch `flush()+sync_data()` fails -> ALL `AppendSync` waiters in the batch are acked
    `FsyncFailed`, never `Synced` -> "batch_fsync_failed"
  - acking ANY waiter `Synced` before the covering fsync returns -> unreachable by construction (the
    batch fsync precedes every ack) -> "ack_before_fsync"  (invariant — must be impossible)
  - a control/rewrite message handled before the in-progress batch is flushed -> forbidden -> "batch_straddles_control"
  - a drain that grows past the bounded cap -> the batch is flushed at the cap and a fresh batch
    begins (never unbounded) -> "batch_cap_exceeded"  (a bound, not an error)
</reject>
After:
<after>
  - Under `Always` with C>1 concurrent durable writers, the AOF writer performs ⌈writes/batch⌉ fsyncs
    instead of one-per-write; every ACKED write is durable on disk; `everysec`/`no` and the C=1 case
    are unchanged; durable-write throughput at pipeline>1 measurably exceeds the M0 baseline; the
    crash-matrix (incl. the new concurrent-writers case) is green on both runtimes.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ The mixed-stream batch drain (interleaved `Append` + `AppendSync`, control messages as
    batch-breakers) preserves the C4-fold + H1-BARRIER + everysec invariants EXACTLY — lowest
    confidence because the writer loop is shared across all policies, the C4 fold, and the rewrite
    control flow, and a mis-ordered flush-vs-control or a missed `pending_aof_count` update is a SILENT
    durability / AOF-corruption bug; if wrong: data loss or corrupt AOF under crash (the worst outcome
    — the milestone's explicit freeze-first risk).
  ⚠ Concurrent per-connection durable writes actually CONVERGE as multiple queued `AppendSync`
    messages at one writer (so a batch exists to coalesce) — VERIFIED in code (per-connection async
    task; `try_send_append_durable(...).await` yields, peers enqueue before the writer drains), so
    confidence is now HIGH; if wrong: the mechanism is correct but yields no throughput gain (an
    effectiveness miss, not a correctness bug). Confirm empirically in M1.
  - [ ] the bounded batch cap (count and/or bytes) can be a fixed tuned constant, not a config flag —
    confirm; if wrong, add an additive flag (cheap).
  - [ ] the OrbStack VM with fresh-server-per-rep gives a trustworthy RELATIVE `Always` throughput
    delta (fsync-bound ⇒ less vCPU-starvation-sensitive than the xshard absolute latency) — confirm
    the instrument is valid for this metric before anchoring M1.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
# ---- M0: baseline anchor ----
Scenario: always_throughput_baseline_recorded            # M0
  Given a fresh moon at appendfsync=always on moon-dev, fresh-server-per-rep
  When durable-write throughput is measured at pipeline depth >1 with C>1 concurrent writers, best-of-N, per-runtime
  Then a before-baseline RPS is recorded for monoio and tokio
  And everysec + no are recorded alongside as unchanged controls

# ---- M1: the coalescing win ----
Scenario: batch_drain_one_fsync_many_acks                # M1
  Given appendfsync=always and K durable AppendSync writes already queued at one AOF writer
  When the writer drains the ready batch
  Then it performs exactly ONE flush()+sync_data() covering all K writes
  And all K waiters receive AofAck::Synced
  And every Synced ack is sent only after that single fsync returned

Scenario: throughput_scales_with_write_concurrency        # M1
  Given the M0 baseline at appendfsync=always
  When C>1 concurrent durable writers drive pipeline depth >1 on both writers (TopLevel + per-shard), both runtimes
  Then measured durable-write throughput exceeds the M0 baseline (RELATIVE before/after ratio on the same instrument)

# ---- M2: durability invariant (freeze-first) ----
Scenario: every_acked_write_survives_crash               # M2
  Given appendfsync=always and N concurrent writers issuing distinct INCR/SET commands
  When each client receives +OK (its Synced ack) and the server is then SIGKILLed
  Then on AOF replay every acked write is present (each counter == the number of its acked increments)
  And no earlier acked write is lost or corrupted by the batch that was in flight at kill

Scenario: interrupted_batch_replays_to_last_valid_record  # M2
  Given a batch written to the file buffer but the process is SIGKILLed before its fsync returns
  When the server replays the AOF
  Then replay truncates to the last valid record and loads cleanly (no torn/partial record applied)
  And all writes acked Synced by prior completed batches remain present

Scenario: barrier_property_preserved                      # M2
  Given a drained batch containing fire-and-forget Append messages followed by an AppendSync
  When the single batch fsync returns and the AppendSync waiter is acked Synced
  Then all preceding Append bytes in that batch are durable on disk (ordered-channel H1-BARRIER property)
  And the C4-FOLD pending_aof_count accounting is unchanged

# ---- M3: control-message ordering safety ----
Scenario: control_message_breaks_the_batch               # M3
  Given appendfsync=always with a partially-drained batch of AppendSync writes pending
  When a Rewrite / RewriteSharded / RewritePerShard / Shutdown message is encountered in the channel
  Then the in-progress batch is flushed (write + single fsync + ack all) BEFORE that message is handled
  And channel message order is preserved exactly and pending_aof_count stays accurate

# ---- M4: no-regression guardrails ----
Scenario: everysec_path_unchanged                         # M4
  Given appendfsync=everysec under the same concurrent-writer load
  When durable writes flow through the writer
  Then no per-batch group-commit fsync engages — the 1s deadline flush + bounded-recv still govern durability
  And everysec throughput and the 8KB-tail-at-SIGKILL behavior are byte-for-byte the pre-change behavior

Scenario: lone_writer_no_added_latency                    # M4
  Given appendfsync=always with exactly one writer (C=1, nothing else queued)
  When it issues a durable write
  Then the writer fsyncs immediately for that single message (batch of 1) and acks Synced
  And per-write latency is not worse than the pre-change one-fsync-per-write path

Scenario: batch_size_is_bounded                           # M4
  Given appendfsync=always and a flood of more than CAP durable writes queued at once
  When the writer drains
  Then it flushes at the CAP boundary and begins a fresh batch (multiple bounded fsyncs)
  And the write buffer never grows past the cap and no message is dropped

# ---- Rejects (each asserts what must NOT change) ----
Scenario: reject_batch_write_failed                       # batch_write_failed
  Given a batch where one message's write_all fails (I/O error)
  When the writer processes the batch
  Then that waiter is acked AofAck::WriteFailed and the write_error latch drops subsequent appends
  And NO waiter in the batch is acked Synced (no false durability claim)

Scenario: reject_batch_fsync_failed                       # batch_fsync_failed
  Given a batch whose flush()+sync_data() returns an error
  When the writer completes the batch
  Then ALL AppendSync waiters in the batch are acked AofAck::FsyncFailed
  And NO waiter in the batch is acked Synced

Scenario: reject_ack_before_fsync                         # ack_before_fsync
  Given any drained batch under appendfsync=always
  When the writer sequences its work
  Then no Synced ack is ever observable before the covering fsync has returned (invariant — unreachable)
  And the ack-after-fsync ordering never inverts under any batch size

Scenario: reject_batch_straddles_control                  # batch_straddles_control
  Given a pending unflushed batch and a control/rewrite message next in the channel
  When the writer reaches that control message
  Then it MUST NOT handle the control message before flushing the batch
  And message order is preserved and no AppendSync waiter is left unacked across the control boundary

Scenario: reject_batch_cap_exceeded                       # batch_cap_exceeded
  Given more than CAP messages ready to drain
  When the writer builds a batch
  Then it stops at CAP, flushes, and starts a new batch (never an unbounded drain)
  And no queued message is dropped and the buffer high-water stays bounded by the cap
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

Internal mechanism (no wire/protocol surface). Contract = the writer-side group-commit shape +
the durability invariant. Names from the existing AOF glossary: `AofMessage` {`Append{bytes,lsn}`,
`AppendSync{bytes,lsn,ack}`, `Rewrite`, `RewriteSharded`, `RewritePerShard`, `Shutdown`}, `AofAck`
{`Synced`, `WriteFailed`, `FsyncFailed`}, `FsyncPolicy::{Always,EverySec,No}`, `aof_writer_task`,
`per_shard_aof_writer_task`. New module: `src/persistence/aof/group_commit.rs`.

```
// ---- Bounds (design-for-failure: a drain is never unbounded) ----
const AOF_GROUP_COMMIT_MAX_BATCH: usize   // hard cap on messages per batch (e.g. 1024)
const AOF_GROUP_COMMIT_MAX_BYTES: usize   // hard cap on bytes buffered per batch (e.g. 8 MiB)

// ---- Pure batching seam (no I/O — unit-testable without a file or runtime) ----
struct GroupCommitBatch {
    data: Vec<AofMessage>,            // Append + AppendSync, in channel order; bytes to write
    deferred_control: Option<AofMessage>, // a control msg that TERMINATED the drain (handle AFTER commit)
}
fn collect_group_commit_batch(
    first: AofMessage,                          // the message just recv()'d (data or control)
    mut try_next: impl FnMut() -> Option<AofMessage>, // non-blocking channel drain (flume try_recv)
    max_batch: usize, max_bytes: usize,
) -> GroupCommitBatch
//  - `first` is control  -> data=[], deferred_control=Some(first)         (no batch; caller handles it)
//  - else                -> pull data messages until: queue empty | a control msg appears
//                           (-> deferred_control) | count==max_batch | bytes>=max_bytes  (cap stop)
//  - ORDER preserved exactly; a control msg is NEVER placed in `data`.

// ---- Commit seam (the durability invariant; one fsync per batch) ----
fn commit_group_commit_batch<W: Write>(
    file: &mut W, batch: &mut GroupCommitBatch, do_fsync: bool /* true under Always */,
) -> CommitOutcome
//  1. write_all each data msg's bytes IN ORDER;
//  2. if do_fsync: exactly ONE flush()+sync_data() AFTER all writes;
//  3. THEN ack every AppendSync waiter in the batch.
//  Ack mapping (a response for every §1 Reject code):
//    all writes ok + fsync ok       -> every AppendSync.ack = AofAck::Synced
//    a write_all error              -> "batch_write_failed": that+remaining AppendSync.ack = WriteFailed;
//                                      set write_error latch; NO waiter in the batch acked Synced
//    flush()/sync_data() error      -> "batch_fsync_failed": ALL AppendSync.ack = FsyncFailed; none Synced
//    (acks are sent in step 3 ONLY) -> "ack_before_fsync": unreachable — no ack precedes the fsync
//  Returns CommitOutcome { synced: usize, write_failed: bool, fsync_failed: bool }.
```

Durability invariant (M2, the freeze-first contract — the immutable promise):
- A connection observes `+OK` for a durable write ONLY after `commit_group_commit_batch` has fsynced
  the batch containing its bytes and sent its `Synced` ack. ⇒ **acked ⇒ on disk** (no false positive).
- The single batch fsync makes every preceding `Append` in `data` durable too (ordered-channel
  H1-BARRIER property — the `fsync_barrier` keeps working unchanged: a zero-length `AppendSync` in a
  batch still proves all prior appends durable). `pending_aof_count` accounting is untouched.
- A crash mid-batch (before its fsync) leaves at most a torn tail record; replay truncates to the last
  valid record (existing AOF replay) — earlier acked writes are intact. Unacked writes are the client's
  to retry (pre-existing at-least-once for unacked; group commit does not worsen it).

Wiring (both writers call the same seam; `EverySec`/`No` pass `do_fsync=false` so behavior is unchanged):
- `aof_writer_task` (TopLevel, monoio blocking loop + tokio bounded-recv loop) and
  `per_shard_aof_writer_task`: replace the per-message `match fsync { Always => write+fsync+ack }`
  with `collect_group_commit_batch` → `commit_group_commit_batch(do_fsync = fsync==Always)` → handle
  `deferred_control` (Rewrite/RewriteSharded/RewritePerShard/Shutdown) exactly as today, AFTER the
  flush. `batch_straddles_control` is structurally impossible: control never enters `data`.
- `EverySec`: `do_fsync=false` in commit; the existing ≥1s deadline flush + bounded-recv path is
  retained verbatim (group commit only coalesces the *Always* fsync; everysec already coalesces by time).

Reject → contracted response (all five):
- `batch_write_failed`  -> AppendSync waiter(s) acked `WriteFailed`; write_error latch set; no `Synced`.
- `batch_fsync_failed`  -> all batch AppendSync waiters acked `FsyncFailed`; no `Synced`.
- `ack_before_fsync`    -> impossible by construction (acks only in commit step 3, after the fsync).
- `batch_straddles_control` -> impossible: control msgs go to `deferred_control`, handled after commit.
- `batch_cap_exceeded`  -> `collect_*` stops at `max_batch`/`max_bytes`; a fresh batch begins next loop.

Status: FROZEN @ v1 — approved by Tin Dang 2026-06-14.
Least-sure flag surfaced at freeze:
  ⚠ [contract·spec] mixed-stream drain preserves C4-fold + H1-BARRIER + everysec invariants exactly —
     if wrong: silent data loss / AOF corruption under crash (the milestone's freeze-first risk).
     Mitigated in shape: control msgs route to `deferred_control` (never in `data`) ⇒ `batch_straddles_control`
     structurally impossible; new concurrent-writers crash test + existing crash-matrix are the oracle.
  ⚠ [contract] the `collect`/`commit` factoring + deferring a try_recv'd control message (vs peeking)
     does not mis-order against the C4 fold `pending_aof_count` accounting — if wrong: seam boundary
     moves (a contract reshape, caught at build/test, not a durability bug).
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must + every Reject has ≥1 red test; the batching seam fully unit-covered;
durability proven by an integration crash test (not just unit mocks).
Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  UNIT — pure batching seam (`src/persistence/aof/group_commit.rs` #[cfg(test)], no I/O / no runtime):
  - test_collect_first_control_makes_no_batch: first=Rewrite → data==[] , deferred_control==Some(Rewrite)   [batch_straddles_control]
  - test_collect_stops_at_control_preserving_order: [Append,AppendSync,Rewrite] → data==[Append,AppendSync] in order, deferred==Some(Rewrite)   [M3]
  - test_collect_respects_max_batch: 5 ready, cap=2 → data.len()==2, rest left in queue   [batch_cap_exceeded]
  - test_collect_respects_max_bytes: bytes cap hit before count cap → stops at the byte boundary   [batch_cap_exceeded]
  - test_commit_one_fsync_many_acks: mock Write counts flush/sync calls; K AppendSync → exactly 1 fsync, K acks==Synced   [M1]
  - test_commit_acks_only_after_fsync: ordering probe (record fsync vs ack timestamps/sequence) → every ack strictly after the fsync   [ack_before_fsync]
  - test_commit_write_fail_acks_write_failed: a mock write_all error mid-batch → that+remaining AppendSync==WriteFailed, none==Synced, write_error set   [batch_write_failed]
  - test_commit_fsync_fail_acks_fsync_failed: mock sync_data error → ALL AppendSync==FsyncFailed, none==Synced   [batch_fsync_failed]
  - test_commit_everysec_no_fsync: do_fsync=false → zero fsync calls; Append-only batch acks nothing; bytes written in order   [M4 everysec_path_unchanged]
  - test_commit_barrier_covers_preceding_appends: batch=[Append,Append,AppendSync(zero-len barrier)] → 1 fsync, barrier acked Synced after the 2 appends are written   [M2 barrier_property_preserved]

  INTEGRATION — real server, both runtimes (`tests/wal_group_commit.rs`, MOON_BIN-pinned, VM-local):
  - test_concurrent_writers_all_acked_survive_sigkill: appendfsync=always, N writers issue distinct INCRs, collect only +OK'd ones, SIGKILL, restart → each counter == its acked count, none lost   [M2 every_acked_write_survives_crash]
  - test_interrupted_batch_replays_to_last_valid_record: kill mid-batch (fault inject before fsync) → restart loads clean (truncate-at-last-valid), prior-batch acked writes intact   [M2 interrupted_batch_replays_to_last_valid_record]
  - test_lone_writer_fsyncs_immediately: appendfsync=always, C=1 → each durable write acked Synced (batch of 1), latency not worse than baseline   [M4 lone_writer_no_added_latency]
  - test_rewrite_during_active_writes_no_loss: BGREWRITEAOF while many durable writes in flight → batch flushed before rewrite; post-rewrite replay loses no acked write   [M3 control_message_breaks_the_batch]
  - existing crash-matrix (`crash_matrix_per_shard_aof.rs`, `crash_matrix_per_shard_bgrewriteaof.rs`) stays GREEN on both runtimes   [M2/M3 regression oracle]
  - existing `pool.rs` H1-BARRIER + everysec + FsyncPolicy unit tests stay GREEN   [M4]

  BENCH — measurement, not pass/fail (`scripts/bench-*` / a wal-group-commit cell):
  - always_group_commit cell: appendfsync=always, pipeline>1, C∈{1,8,32}, fresh-server-per-rep, best-of-N, per-runtime → M0 before + M1 after RELATIVE ratio; everysec/no controls flat   [M0/M1]
</test_plan>

Tests live in: `tests/wal_group_commit.rs` (integration) + `src/persistence/aof/group_commit.rs` unit
tests · MUST run red (symbols/behavior missing) before Build.

RED CONFIRMED 2026-06-14 (VM, monoio): `cargo test --test wal_group_commit --no-run` fails with
exactly `error[E0432]: unresolved import moon::persistence::aof::group_commit — could not find
group_commit in aof` — the right reason (missing implementation, harness otherwise compiles). 9 seam
tests cover: collect (control-first, stop-at-control+order, max_batch, max_bytes) + commit
(one-fsync-many-acks+ordering, write-fail, fsync-fail+ack-after, everysec-no-fsync, barrier-covers-appends).
The end-to-end durability scenarios (every_acked_write_survives_crash, rewrite-during-writes) are
gated on the integration crash-matrix + a new concurrent-writers SIGKILL test (added at build) — a unit
mock cannot prove on-disk survival across a real kill.
NOTE (non-behavioral): §3's `<W: Write>` + "flush()+sync_data()" is realized as a sync-capable
`GroupCommitSink { write_all; sync }` trait so the single fsync is mockable/countable; the FROZEN
durability behavior (one fsync after all writes, ack-after-fsync, the 5 reject mappings) is unchanged.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Safety rule (feature-specific): <e.g. debit+credit in one atomic transaction>
Code lives in: `./src/`
Constraints: do NOT change any test or the contract; allow-list packages only; ask if unclear.

<!-- EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass — dual-runtime, VM-local clone `~/moon-gc` @ `12681b3` (home volume; shared-volume diskfull avoided):
      · seam unit 9/9 (tokio) · AOF lib 94 (tokio) / 90 (monoio) · integration crash 4/4 ×both runtimes
        (incl. `concurrent_writers_all_acked_survive_sigkill_top_level` — exercises the Finding-2 tokio-TopLevel latch)
      · crash-matrix 5/5 ×both (`crash_matrix_per_shard_aof` 3, `..._bgrewriteaof` 2) · consistency 197/197 @1/4/12
- [x] coverage did not decrease — +4 integration crash tests added at build; the tokio-TopLevel torn-write path is now
      covered by the top_level SIGKILL test; no test removed or weakened.
- [~] no test or contract was altered during build — §3 CONTRACT unchanged (FROZEN @v1). ONE frozen §4 test
      (`commit_write_fail_acks_write_failed`) got a HUMAN-APPROVED intent-preserving fix: it double-consumed a flume
      `bounded(1)` (use-after-consume) — the test was wrong, my impl was correct (both waiters acked `WriteFailed`).
      Fixed by reading each receiver once; all 3 assertions kept. NOT a weakening. (Surfaced at gate.)
- [x] concurrency / timing safe — fsync-before-ack preserved; the `write_error` torn-write latch is now UNIFORM across
      all 4 writer loops (Finding 2 closed in `2750d1c`); no lock held across `.await`; zero new cross-thread lock;
      the flume oneshot ack pattern is unchanged; `ack_batch` is the single-sourced ack-after-fsync ordering.
- [x] no exposed secrets, injection openings, or unexpected dependencies — internal mechanism only; zero new deps.
- [x] layering & dependencies follow conventions — `group_commit.rs` is a pure runtime-free seam under
      `persistence/aof/`; the loops call it; no new module edges; clippy ×2 + `fmt` clean; zero new `unsafe`.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — every group_commit symbol referenced: `collect_group_commit_batch`/`commit_group_commit_batch`/
      `GroupCommitSink`/`CommitOutcome`/`AOF_GROUP_COMMIT_MAX_{BATCH,BYTES}` from `writer_task.rs` + the seam tests;
      `ack_batch`/`BatchAck`/`msg_body` `pub(crate)` from `writer_task.rs`; `is_control`/`msg_body_len` private,
      used in-module. Confirmed by grep + clippy-clean ×2 runtimes.
- [x] DEAD-CODE (code) — no new orphan; clippy `-D warnings` clean on default (monoio) AND tokio,jemalloc.
- [x] SEMANTIC — n/a (code task; no prose contract surface).

### M0/M1 performance evidence — instrument limitation (NOT a correctness gap)
- Mechanism of the win (K `AppendSync` → exactly ONE fsync, ack-after-fsync) is proven DETERMINISTICALLY by the seam
  unit test `commit_one_fsync_many_acks`.
- Empirical M0→M1 RELATIVE throughput delta is NOT resolvable on the OrbStack VM. Baseline (`3150f8b`, one-fsync-per-
  AppendSync) vs HEAD (`12681b3`), `appendfsync=always`, fresh-server best-of-3, redis-benchmark `-P16`:
      shards=1 C=32: base 934K / head 917K = 0.98×   ·   shards=4 C=32: 892K / 917K = 1.03×
      shards=1 conc sweep C∈{32,128,256,512}: 1.00× / 1.16× / 0.90× / 1.00×  (no trend; within the ±~10% VM noise the
      everysec controls also showed: 1.03×, 1.12×).
  Root cause: the virtio disk's `fsync` is near-free — `always` runs at ~0.9M RPS (NOT the 11× penalty 135K→12K the
  feature targets), so the writer drains each `AppendSync` before the next arrives ⇒ a coalescing batch never forms
  (batch≈1) ⇒ nothing to amortize. This is exactly §1 assumption #4 (lowest-confidence perf flag): "confirm the
  OrbStack VM instrument is valid for this metric before anchoring M1" — now CONFIRMED INVALID. The M0/M1 ratio
  requires a slow-fsync instrument (real disk / GCloud — noted blocked). Deferred to §7 OBSERVE.

### GATE RECORD
Outcome: PENDING — conservative autonomy → STOP for human decision (no auto-PASS).
  Correctness · durability (M2) · ordering (M3) · no-regression (M4 correctness) · all 5 Rejects: PROVEN, green, both
  runtimes. The throughput Must (M0/M1): mechanism proven (unit), empirical ratio un-measurable on the only available
  instrument (free-fsync VM) — a documented, freeze-flagged instrument gap, NOT a security or correctness finding.
  Human call: PASS (accept mechanism proof + correctness; defer the real-disk RPS ratio to OBSERVE) · or RISK-ACCEPTED
  on the empirical perf number · or hold for a slow-disk bench.
Reviewed by: <pending — Tin Dang> · date: 2026-06-14

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors):
- `moon_aof_fsync_duration_microseconds_count` per N durable writes → effective batch size = N / fsync_count
  (the direct mechanism monitor; <1 fsync per write under `always`+concurrency is the win materializing).
- `AOF_FSYNC_ERR` / `WriteFailed` ack rate (reject-path health) · everysec 1s-deadline tail-loss at SIGKILL (M4 control).
Spec delta for the next loop:
- The M0/M1 throughput Must is only observable on a SLOW-fsync instrument. The OrbStack virtio disk drains each
  `AppendSync` before the next arrives (batch≈1), so the coalescing win is STRUCTURALLY unmeasurable there — measure the
  RPS ratio on GCloud / a real disk (or an O_DIRECT / fsync-delay harness) BEFORE anchoring the perf number. Until then
  the win is asserted by the deterministic seam test + the on-disk crash-survival suites, not by a VM RPS delta.

### Competency deltas
- [TDD · open] a frozen RED test can itself be wrong: `commit_write_fail_acks_write_failed` double-consumed a flume
  `bounded(1)` (use-after-consume) — the fix was intent-preserving + human-approved, never a weakening
  (evidence: failed `left: Err(Disconnected)` vs `right: Ok(WriteFailed)`; my impl acked both waiters WriteFailed).
- [SDD · open] the contract invariant "`CommitOutcome.write_failed` ⇒ the latch must engage" applied to all 4 writer
  loops, but the pre-existing tokio-TopLevel loop never carried a `write_error` latch (nor the fsync-fail injection) —
  group commit made the latent durability gap explicit (evidence: adversarial Finding 2 @0.97; fixed `2750d1c`).
- [ADD · open] a perf Must can be un-measurable on the only available instrument: the OrbStack VM's near-free fsync
  makes the group-commit win structurally invisible (batch≈1; `always`≈0.9M RPS, no 11× penalty) — §1 ranked exactly
  this risk lowest-confidence (assumption #4); confirm instrument validity BEFORE committing to a perf Must
  (evidence: 0.98× ratio, conc-sweep no-trend within ±10% VM noise).
