# R2-fix-a SUMMARY (wave-2a R2 review, area 1: moon#1299)

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| N1 MINOR: a client that has seen its QUIT reply (or protocol-error reply, or the EOF after an output-limit close) and EOF can still find its keys held | FIXED | 0d62683 | New `tests/txn_close_after_epilogue_1299.rs`, 6 tests under `--appendonly yes --appendfsync always`, 40 rounds each: QUIT s1, QUIT s4, protocol fault, SUBSCRIBE+QUIT, SUBSCRIBE+protocol fault, reply over the output-buffer limit. Red on `r1b-057598f` on both runtimes: 36–40 of 40 SETs refused in every class. Green on `r2fixa-v1` on both runtimes: 6/6, 0 refused, 0 dirty reads. The reviewer's `r28_quitread.py` with 200 iterations: base monoio 200/200 (quit) and 194/200 (fault); base tokio 200/200 and 200/200; fixed binaries 0/200 in all four cells. | CLIENT KILL residual, see Risks |
| N2 NIT: "partially executed" when nothing else changed | FIXED | 36041f2 | New test `review_r1_txn_isolation_1299::a_cross_shard_del_that_removed_nothing_else_is_not_partial` (s4). DEL and UNLINK of {missing key, held key} now get the plain `-TXNCONFLICT`, both when the refused part is the coordinator's own slice and when it is a remote leg. When another key really was removed, the reply still says "partially executed" and that key is gone. Red on 057598f on both runtimes (the first DEL said "partially executed"); green on both. | none |

### Design notes
**N1: the socket is closed only after the epilogue**
- The connection bodies no longer close the socket. They hand the stream back in a small per-runtime enum `BodyExit<S>`, declared in each `exit.rs`. It is a plain return value: no Arc or Box, no allocation.
  - `HandOff(S)`: migration, idle park or PSYNC hijack. The caller gets `Some(stream)`, exactly as before.
  - `Close(S)`: every early exit (the 29 monoio and 8 tokio `(Done, None)` returns). The wrapper drops the stream after `end_open_txn`; before, the body dropped it before the epilogue.
  - `Shutdown(S)`, monoio only: the loop's normal exit. The wrapper runs the graceful `shutdown()` (FIN, plus `close_notify` on TLS) after the epilogue. The body tail used to run it before.
- Early exits keep a plain drop, never `shutdown()`. On a peer that has stopped reading, a TLS `close_notify` would park the task forever.
- Tokio keeps its drop-only behaviour; it never issued a graceful shutdown.
- Callers still see `(result, Option<S>)`, so `conn_accept.rs` is unchanged.
- A `debug_assert` checks that the stream is handed back open exactly when the result is a hand-off.

**N2: what counts as "applied"**
- In `coordinate_multi_del_or_exists`, a DEL/UNLINK part now counts as applied only if it removed a key. That is the local slice's `:n > 0`, or a remote leg whose integer replies sum above 0. Those counts are already in the replies, so there is no extra round trip.
- MSET is unchanged: every `+OK` leg wrote something.
- The same count drives the AOF-backpressure partial wording (moon#769), which no longer over-claims either.
- The `refused_leg_error` doc now says what "applied" means per command.

**Adversarial re-read: close paths where FIN could still come before the epilogue**
- TLS `close_notify`: monoio's tail `shutdown()` now runs after the epilogue. Tokio TLS never sent `close_notify`; its drop now also comes after the epilogue.
- Subscriber loop:
  - monoio: the loop is inline in the body; QUIT, fault and `SubscribeResult::WriteError` all return `Close`.
  - tokio: `SubscriberAction::EarlyReturn` returns `Close`.
- Write error or write timeout: the stream is now dropped after the epilogue. Where the socket is already dead the order does not matter, because the client cannot observe it.
- Output-limit refusal: `Close`, after the epilogue (covered by a test).
- Blocked peer gone, server shutdown, `xshard_reply_fatal`: all now close after the epilogue.
- Task-park, migration, PSYNC: these take the `HandOff` path. They still return `Some(stream)`, and the epilogue still runs first (a no-op for park and migrate, since both require no open TXN).
- Not a close-order issue: CLIENT KILL (see Risks).

## Measurements
No performance claims. There is no new allocation and no new lock. The only per-connection cost is that the stream is moved into an enum return instead of being dropped in the body.

File growth, all from rustfmt splitting three one-line match arms plus comments:
- `handler_monoio/mod.rs`: 5074 → 5078 lines
- `handler_sharded/mod.rs`: 3856 → 3858
- `coordinator.rs`: 4490 → 4504

## Gates (Linux container, not the merge bar; shared target-c)
- `cargo fmt --check`: rc 0.
- `cargo clippy --all-targets -- -D warnings`: rc 0 on default (monoio) and on `runtime-tokio,jemalloc`.
- `cargo test --release --lib`: monoio 7034 passed, 0 failed; tokio 6088 passed, 0 failed.
- Integration, run once per binary (`r2fixa-v1-monoio`, `r2fixa-v1-tokio`, built from HEAD 36041f2 with release-fast). These all passed on both:

  | suite | passed |
  |---|---|
  | txn_close_after_epilogue_1299 | 6 |
  | review_r1_txn_isolation_1299 | 6 |
  | txn_exit_epilogue_1299 | 8 |
  | txn_isolation_1299 | 20 |
  | txn_abort_durability_1285 | 25 |
  | protocol_error_lifetime | 8 |
  | perf_ws18_proto_fault_defer | 2 |
  | subscriber_client_state | 5 |
  | pubsub_resp3_push | 21 |
  | blocking_peer_eof | 5 |
  | migration_batch_tail | 4 |

- `parked_idle_parity`, tokio: the default-feature test build fails `unauthenticated_conn_never_task_parks` by construction. It expects `parked_clients:1` because it was compiled for monoio, and a tokio server never parks. Rebuilt with `runtime-tokio,jemalloc` it passes 7/7, on both my tokio binary and the base tokio binary.
- `parked_idle_parity`, monoio: `unauthenticated_conn_never_task_parks` failed twice on my binary (6/7). The base binary has the same failure:
  - Cargo re-runs against base could not complete; each timed out waiting on lib rebuilds in the shared target.
  - A direct Python replay of the test's steps (same flags, same 4.6 s wait), 8 runs each, gave `parked_clients` of 1 on 5/8 runs on base and 7/8 on the fix. The other runs read 0, meaning the authenticated connection had not parked yet.
  - The failure is the "exactly one parked" gauge being timing-sensitive under host load. My change leaves the park path untouched.
- Not run:
  - fuzz check: no parser or `ShardSlice` code was touched.
  - Each commit was not built separately. Red is shown on 057598f and green on HEAD, and each item has its own test.
- Binary provenance: there is no `strings` marker, because the change adds no string literal that survives release. Provenance is behavioural: the new tests are red on `r1b-057598f-*` and green on `r2fixa-v1-*`.

## Cross-ownership edits
- `src/shard/coordinator.rs` (N2 only; isolated in its own commit 36041f2).

## Risks / things the orchestrator must re-check at integration
- **CLIENT KILL residual (not fixed, pre-existing, not a close-order bug).**
  - CLIENT KILL does `shutdown(SHUT_RDWR)` on the victim's fd and only wakes the victim's task. The killer's reply, and the victim's EOF, both come before the victim's rollback.
  - Under `appendfsync always`, "CLIENT KILL ID x" followed by a SET of x's key from the killer was refused 99/100 times on monoio and 100/100 on tokio, on the fixed binaries.
  - Fixing it needs CLIENT KILL to await the victim's epilogue (a kill acknowledgement, possibly cross-shard). That is a separate follow-up.
- Anything integrated later that adds a `return` to the handler bodies must return `BodyExit::Close(stream)`. The compiler enforces the type, but a new hand-off must use `HandOff`.
- Merge conflicts are likely wherever `(MonoioHandlerResult::Done, None)` / `(HandlerResult::Done, None)` or the monoio tail's `stream.shutdown()` were edited.
- Pre-existing and unchanged:
  - A body panic is still outside the wrapper.
  - On monoio, the tail `shutdown()` is also reached through `break` after a failed or timed-out push write, so a TLS peer that stopped reading can still park the task there. Only the order changed, not the risk.
- Scripts are in `scratchpad/r2fixa/`: `r_kill.py`, `park.py`, `integ2.sh`, `gates.sh`. No servers are left running.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.92 · Practicality 0.93 · Optimization 0.93 · Edge cases 0.9 · Self-evaluation 0.9

Edge cases is capped at 0.9 because CLIENT KILL keeps the same observable window through a different mechanism, and it is only documented. Tokio's `xshard_reply_fatal` and the subscriber push-write-error exits have no black-box test; they are covered because they return through the same `Close` path.
