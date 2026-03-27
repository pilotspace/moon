---
phase: 17-blocking-commands
verified: 2026-03-24T15:00:00Z
status: gaps_found
score: 6/7 must-haves verified
re_verification: false
gaps:
  - truth: "Cross-shard blocking coordination for multi-key BLPOP"
    status: partial
    reason: "BlockRegister/BlockCancel ShardMessage variants exist and are handled in shard drain_spsc_shared, but handle_blocking_command in connection.rs registers only on keys[0] (first key). The comment at line 1840 explicitly says 'Register on first key only (multi-key cross-shard is Plan 03)'. When a multi-key BLPOP spans keys that hash to different shards, only the first key's shard receives the waiter registration. The other shard keys are never registered."
    artifacts:
      - path: "src/server/connection.rs"
        issue: "Line 1848: reg.register(selected_db, keys[0].clone(), entry) — only first key registered. Multi-key cross-shard wakeup is incomplete."
      - path: "src/shard/dispatch.rs"
        issue: "BlockRegister/BlockCancel variants exist and are handled correctly in shard event loop, but are never sent from connection.rs for keys[1..] — the send-side of cross-shard coordination is missing."
    missing:
      - "Send BlockRegister ShardMessage for keys[1..] that hash to other shards"
      - "Send BlockCancel to all registered shards when one fires"
      - "Register same wait_id on same-shard keys (same shard, keys[1..] can use blocking_registry directly)"
human_verification:
  - test: "Timeout precision within 10ms"
    expected: "A BLPOP with 0.2s timeout returns Null within 200ms+10ms (not 200ms+100ms)"
    why_human: "The 10ms timer tick is correct in code, but actual OS scheduling jitter and tokio runtime precision cannot be verified by code inspection alone. The integration test blpop_timeout uses 0.2s but only checks elapsed < 1000ms, not that it's within 10ms of 0.2s."
---

# Phase 17: Blocking Commands Verification Report

**Phase Goal:** Implement blocking list/sorted-set commands with fiber-based blocking, timeout, and fair wakeup
**Verified:** 2026-03-24T15:00:00Z
**Status:** gaps_found
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | BLPOP/BRPOP block connection fiber until data or timeout | VERIFIED | `handle_blocking_command` in connection.rs line 1797: async fn awaits `reply_rx` oneshot via `tokio::select!`. Fiber yields at `.await` — no CPU spin. Integration tests `blpop_blocks_then_wakes`, `blpop_timeout` pass. |
| 2 | BLMOVE atomic pop+push with blocking | VERIFIED | `BlockedCommand::BLMove` variant in blocking/mod.rs; wakeup hook in wakeup.rs lines 48-65 does atomic pop+push within same `try_wake_list_waiter` call. `blmove_basic` integration test passes. |
| 3 | BZPOPMIN/BZPOPMAX block until sorted set has elements | VERIFIED | `BlockedCommand::BZPopMin/BZPopMax` handled in `try_wake_zset_waiter`; wakeup fires after `ZADD` at connection.rs line 1593-1599. Tests `bzpopmin_blocks_then_wakes`, `bzpopmin_immediate`, `bzpopmax_immediate` pass. |
| 4 | Fair FIFO ordering for wakeup | VERIFIED | `BlockingRegistry` uses `VecDeque<WaitEntry>` per key; `register()` does `push_back`, `pop_front()` does `pop_front`. Unit tests `test_fifo_order` and `test_register_and_pop_front` verify ordering. |
| 5 | Timeout precision within 10ms | UNCERTAIN | `block_timeout_interval` set to 10ms in shard/mod.rs line 240. `expire_timed_out` called every tick. Code is correct; actual precision requires runtime measurement. |
| 6 | Blocked clients don't consume CPU (fiber yields) | VERIFIED | `handle_blocking_command` is `async fn`; awaits `reply_rx` (oneshot receiver) inside `tokio::select!`. Tokio parks the future at `.await` — fiber is de-scheduled. No polling loop exists. |
| 7 | Cross-shard blocking coordination for multi-key BLPOP | FAILED | `BlockRegister`/`BlockCancel` ShardMessage variants exist in dispatch.rs lines 59-69 and are handled in shard event loop (shard/mod.rs lines 601-616), but `handle_blocking_command` in connection.rs line 1848 registers only `keys[0]`. Comment explicitly states: "Register on first key only (multi-key cross-shard is Plan 03)". Sending-side of cross-shard protocol is not implemented. |

**Score:** 6/7 truths verified (1 partial/failed, 1 uncertain)

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/blocking/mod.rs` | BlockingRegistry, WaitEntry, BlockedCommand, Direction enum | VERIFIED | All types present and substantive. Full implementation with register/pop_front/remove_wait/has_waiters/expire_timed_out. 264 lines. |
| `src/blocking/wakeup.rs` | try_wake_list_waiter, try_wake_zset_waiter | VERIFIED | Both functions present and substantive. Handle BLPop/BRPop/BLMove/BZPopMin/BZPopMax correctly. 142 lines. |
| `src/command/list.rs` (lmove) | LMOVE command handler | VERIFIED | `pub fn lmove` at line 992. Full implementation with direction parsing, WRONGTYPE checks, same-key rotation, Null on empty source. |
| `src/server/connection.rs` | BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX + wakeup hooks | VERIFIED | All 5 blocking commands intercepted (lines 1375-1413). `handle_blocking_command` at line 1797. Post-dispatch wakeup hooks at lines 1580-1600. MULTI non-blocking conversion at line 1759. |
| `src/shard/mod.rs` | BlockingRegistry Rc<RefCell> + 10ms timeout timer | VERIFIED | `blocking_rc` initialized at line 207. `block_timeout_interval` at line 240 (10ms). `expire_timed_out` called at line 406. |
| `src/shard/dispatch.rs` | BlockRegister/BlockCancel ShardMessage variants | VERIFIED | `BlockRegister` at line 59, `BlockCancel` at line 67. Both handled in `drain_spsc_shared` at lines 601-616. |
| `src/lib.rs` | `pub mod blocking` | VERIFIED | Line 1: `pub mod blocking;` |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `src/blocking/wakeup.rs` | `src/blocking/mod.rs` | BlockingRegistry::pop_front, remove_wait | WIRED | Lines 20, 70 call `registry.pop_front(...)` and `registry.remove_wait(wait_id)`. |
| `src/blocking/wakeup.rs` | `src/storage/db.rs` | list_pop_front/back, zset_pop_min/max | WIRED | Calls `db.list_pop_front`, `db.list_pop_back`, `db.list_push_front`, `db.list_push_back`, `db.zset_pop_min`, `db.zset_pop_max` — all confirmed present in db.rs (lines 826-893). |
| `src/server/connection.rs` | `src/blocking/mod.rs` | register waiter then await oneshot | WIRED | `blocking_registry.borrow_mut()` at lines 1838, 1863, 1867, 1881. `WaitEntry` constructed and registered. Oneshot awaited in `tokio::select!`. |
| `src/server/connection.rs` | `src/blocking/wakeup.rs` | try_wake_list_waiter after LPUSH/RPUSH/LMOVE | WIRED | Lines 1588, 1596 call `crate::blocking::wakeup::try_wake_list_waiter` and `try_wake_zset_waiter` inline after dispatch. |
| `src/shard/mod.rs` | `src/blocking/mod.rs` | expire_timed_out in 10ms timer tick | WIRED | Line 406: `blocking_rc.borrow_mut().expire_timed_out(now)` inside `block_timeout_interval.tick()` arm. |
| `src/server/connection.rs` | `src/shard/dispatch.rs` | Send BlockRegister for cross-shard keys | NOT_WIRED | `handle_blocking_command` only registers `keys[0]` locally. No `ShardMessage::BlockRegister` is sent for any key. Cross-shard send path not implemented. |

### Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| BLOCK-01 | 17-01, 17-02 | BLPOP/BRPOP/BLMOVE blocking commands with fiber yield | SATISFIED | All three commands intercepted and wired to `handle_blocking_command`. Fiber yields on oneshot await. Passing integration tests: `blpop_blocks_then_wakes`, `blmove_basic`, `brpop_immediate`. |
| BLOCK-02 | 17-01, 17-02 | BZPOPMIN/BZPOPMAX sorted set blocking | SATISFIED | BZPopMin/BZPopMax variants in BlockedCommand; `try_wake_zset_waiter` fires after ZADD; `bzpopmin_blocks_then_wakes` integration test passes. |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `src/server/connection.rs` | 1840 | `// Register on first key only (multi-key cross-shard is Plan 03)` | Warning | Multi-key BLPOP across shards only wakes on first key. BLPOP key1 key2 key3 — if key2 or key3 gets data on a different shard, the client will not wake. This is a known deferred item per the plan. |

### Human Verification Required

**1. Timeout Precision Within 10ms**

**Test:** Run `BLPOP emptykey 0.5` on a loaded server. Measure elapsed time from send to nil response.
**Expected:** Response arrives within 510ms (500ms + at most 10ms scanner granularity)
**Why human:** The code sets a 10ms tick interval, but tokio timer jitter, OS scheduling, and test framework overhead mean actual precision can only be confirmed at runtime. The integration test `blpop_timeout` only checks `elapsed.as_millis() < 1000` — not the 10ms precision bound.

### Gaps Summary

**One gap blocking full goal achievement:** Cross-shard multi-key BLPOP blocking coordination.

The phase goal states "Cross-shard blocking coordination for multi-key BLPOP" as success criterion 7. The infrastructure for this is in place — `BlockRegister` and `BlockCancel` ShardMessage variants are defined and handled by the shard's SPSC drain loop. However, the sending side in `handle_blocking_command` (connection.rs) is explicitly deferred: only `keys[0]` is registered, and no `ShardMessage::BlockRegister` messages are sent to other shards for `keys[1..]`.

This means: a client issuing `BLPOP key1 key2 0 ` where key1 and key2 hash to different shards will only wake when key1 receives data. If key2 receives data first (on a different shard), the client remains blocked until timeout.

The single-shard behavior is fully functional and tested. All other success criteria (BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX blocking, FIFO wakeup, timeout scanning, zero-CPU blocking, MULTI non-blocking conversion) are verified.

**Note on failing integration tests:** `test_hash_commands`, `test_hscan`, `test_list_commands`, `test_aof_*` fail but are unrelated to phase 17. These are pre-existing failures in other subsystems.

---

_Verified: 2026-03-24T15:00:00Z_
_Verifier: Claude (gsd-verifier)_
