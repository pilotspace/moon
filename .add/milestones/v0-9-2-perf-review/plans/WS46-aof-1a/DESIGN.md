# WS46 — moon#1266 Option 1A: the shard writes its AOF bytes before its replies

Goal: under `appendfsync everysec` (and `no`), a `kill -9` loses no acknowledged
write. redis does it by writing its AOF buffer in `beforeSleep`, before the
event loop sends that iteration's replies; only the fsync is deferred. moon
(Option 3, WS40) acknowledged a write once its record was queued to the shard's
AOF writer thread, so a writer that was descheduled or stalled past the kill
lost acked writes (9 of 240 reps).

## 1. Where replies leave a shard thread

| runtime / driver | how a reply reaches the socket | flush point for 1A |
|---|---|---|
| monoio + io_uring (default on Linux, no SQPOLL) | `stream.write_all` queues an SQE; the kernel sees it only at the next `io_uring_enter` — the driver's `submit()` (SQ full, cold-path submit) or `park()` | a **before-submit hook** in the vendored driver (`monoio::set_before_submit_hook`): one `write(2)` of the lane per runtime iteration, before any SQE of that iteration is submitted — redis's `beforeSleep` |
| monoio + legacy driver (epoll/kqueue, `--io-driver epoll`) | the write syscall runs inside `write_all`'s poll | the same hook, which the vendored legacy driver runs before every readiness poll (park, park_timeout, submit); the reply macros park the reply (`ParkUntilHook`) until it, and the hook wakes them after its write — one `write(2)` per iteration, and the driver polls without blocking when it woke replies. (A plain yield cannot coalesce: monoio re-polls a self-woken task before its queue.) Adaptive: a lone connection stops parking |
| io_uring with `MOON_URING_SQPOLL` | the kernel thread consumes SQEs without an enter | the reply macros flush first: one `write(2)` per connection batch |
| tokio (current-thread runtime per shard) | `write_all` runs the syscall in its poll | tokio's `write_all_bounded!` awaits `flush_before_reply_coalesced`: with records buffered the reply yields once, so the connections ready in the same scheduler round append too, and the first to resume writes them all — one `write(2)` per round (adaptive: a lone connection stops yielding); the experimental uring bridge's `send_serialized` flushes plainly |
| any runtime, reply to ANOTHER shard (cross-shard command, `ExecReply`, response slot) | `OneshotSender::send` / `ResponseSlot::fill` wakes a task on another thread, which may submit its socket write before this thread's next park | both flush the calling thread's lane first (sync) |
| shard background work (expiry/eviction DEL, persistence tick) | no reply | the shard event loop flushes once per iteration; io_uring's hook covers it too |

## 2. The lane (`persistence/aof/lane.rs`, protocol core `lane_protocol.rs`)

One `AofLane` per AOF writer (per incr stream), created by the pool and handed to
its writer task. Mutex-protected core: `mode` (Writer / Direct / Closed), the
writer's `RecordCtx`, a dup of the writer's fd, the writer's fold floor, the
pending byte buffer, a write-failed latch, the count of producers between a
full-channel `try_send` and their slow (parking) send.

- **Writer mode** (= Option 3): producers `try_send` to the channel under the lane
  lock; the writer frames and writes as before.
- **Direct mode**: an `Append` is framed by the producer under the lane lock with
  the SAME `RecordCtx::prefix_for` the writer uses (SELECT, `MOON.TS`, `MOON.TXN
  BEGIN/PAUSE/END/RESET`, the session stamp of a reopened file, the R1
  `UNKNOWN_DB` first-SELECT sentinel) and the same `[u64 lsn][u32 len]` framing
  (PerShard) or bare RESP (TopLevel), the same #455 fold-floor drop, into the
  lane buffer. The buffer is written with ONE `write_all` at the flush point. A
  producer on a thread that is not this lane's shard thread writes at once.
- **Who owns the fd/ctx**: Direct ⇒ the lane (any thread, under the lock); Writer
  ⇒ the writer thread. Exactly one owner at a time, so the file order is the
  lock order.
- **Writer → Direct (release)** only by the writer, at the end of a wake, when:
  1A is on, the effective policy is not `always`, no write error is latched, the
  rewrite overflow is not armed, no producer is in a slow send, and the channel
  is empty — checked under the lane lock, where every producer decides
  channel-vs-buffer, so no record can be in flight past the check. The writer
  moves its `RecordCtx` and fold floor in, and a dup of its file.
- **Direct → Writer (flip)**: by anyone, under the lock, always writing the buffer
  first: a producer sending a non-`Append` message (an `AppendSync` — `always`
  after a runtime `CONFIG SET`, a barrier — `Rewrite*`, `Shutdown`), a flush
  whose `write(2)` failed (sets the latch), and the writer when it receives any
  message, when the policy becomes `always`, and before every stop path. So
  while Direct the channel holds nothing; a message in the channel always comes
  after every buffered record, in order.
- **Writer takes back**: before it handles any received message and before any
  stop path, it flips (if needed) and takes the parked `RecordCtx` back, and a
  failed direct write latches its own `write_error` (moon#769/#1272 semantics
  unchanged: the torn stream gets no more bytes).
- **Closed**: set when the writer exits (drop guard, also on unwind) — producers
  then reach the closed channel and get the existing dead-writer errors.

## 3. What stays on the writer thread

- The everysec fsync deadline and the WS40 agent hand-off (`EverysecSync`,
  `fsync_handoff.rs` unchanged): the writer learns of direct writes from an
  `AtomicBool` it swaps on every wake (`note_written`, before `claim()`, so the
  R1 heal rule only counts writes after a failure was learned). Its wake cadence
  while Direct is the existing idle ladder pinned at 50 ms while pending, so the
  oldest unsynced byte is fsynced within ~1 s + 50 ms (1 s when idle: the first
  wake after an idle park sees the write and the deadline already due).
- `appendfsync always` group commit: never Direct; acks after the fsync.
- BGREWRITEAOF (all four loops), generation switches, overflow drains, the TXN
  re-open, `CONFIG SET appendfsync`, the clean-close marker and its counting
  (`writer_stop.rs`): the writer takes the lane back first, so every one of these
  runs in Writer mode exactly as before. The fold's `pending_aof_count` and fold
  epoch therefore see the channel as today.
- Released again only after the control path finished (dup of the NEW file
  after a generation switch).

## 4. Switch

`MOON_AOF_SHARD_WRITE` (read once). Built default-off for the A/B, then
flipped to default ON once the gate passed (SUMMARY). `0` = Option 3 byte for
byte (the pool skips the lane entirely; the monoio writer warm-polls again) —
the same-binary A/B knob and the escape hatch. The warm poll stays in the code
for that path only: removing it would make the escape hatch a regression
against Option 3 (a futex wake per record from every producer).

## 5. Residual: a kill -9 inside a BGREWRITEAOF fold

The writer holds the append position for the whole fold; records acked while
it runs are written at the fold's post-fold drain (as in Option 3). redis has
no such window because its multi-part manifest lists the new incr from the
rewrite's start, so its main thread keeps writing to a file that is already
part of the recoverable set. Closing it here needs the same manifest change
(the new incr listed before the snapshot, the old generation kept until the
fold commits) — a follow-up, not WS46.
