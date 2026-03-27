# Phase 17: Blocking Commands - Research

**Researched:** 2026-03-24
**Domain:** Blocking list/sorted-set commands (BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX)
**Confidence:** HIGH

## Summary

Blocking commands suspend a client connection until data becomes available on a watched key or a timeout expires. In our thread-per-core shared-nothing architecture, the natural blocking mechanism is fiber/task suspension within the shard's single-threaded Tokio runtime: a blocked client `await`s a `tokio::sync::oneshot` channel, consuming zero CPU while suspended. When a producer command (LPUSH, RPUSH, ZADD) adds data to a watched key, the shard checks its local wait queue and wakes the first blocked client by sending the popped result through the oneshot channel.

The architecture maps cleanly to our existing patterns. PubSub already demonstrates the "connection yields, wakes on event" pattern with `tokio::select!` on mpsc receivers. Blocking commands follow the same shape but with per-key wait queues and FIFO ordering. The hard case is cross-shard multi-key BLPOP (keys on different shards), which requires a coordinator that registers interest on multiple shards and cancels remaining registrations when the first shard delivers data.

LMOVE does not exist yet in the codebase -- it must be implemented as a prerequisite for BLMOVE. All five blocking commands reuse existing pop logic (LPOP/RPOP/ZPOPMIN/ZPOPMAX) with a blocking wrapper.

**Primary recommendation:** Implement a per-shard `BlockingRegistry` (HashMap<(db_index, key), VecDeque<WaitEntry>>) that producer commands check after mutations. Use tokio::sync::oneshot for wakeup. Add LMOVE first, then blocking wrappers for all five commands.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Blocking mechanism: blocked connection's fiber yields to the shard scheduler
- Blocked fiber stored in per-shard wait queue: HashMap<key, VecDeque<WaitingClient>>
- When LPUSH/RPUSH/ZADD adds data to a watched key, check wait queue and wake first blocked client
- Zero CPU consumption while blocked -- fiber is suspended, not polling
- Wait queue structure: per-shard HashMap<Bytes, VecDeque<BlockedClient>> where BlockedClient contains client ID/fiber wakeup handle, command type, timeout deadline, response channel (oneshot sender)
- FIFO fairness: first client to block on a key is served first (VecDeque maintains insertion order)
- BLPOP with multiple keys: client registered in wait queues for ALL listed keys; first key with data wins
- On wakeup: remove client from all other wait queues to prevent double-service
- Timeout precision within 10ms (timer wheel or per-shard timer task)
- 0 timeout = block indefinitely (no timeout registration)
- On timeout: return nil response, remove from all wait queues
- Per-shard timer task scans for expired blocked clients periodically (every 10ms)
- Cross-shard BLPOP: coordinator on connection's shard registers interest in all target shards via SPSC channels
- Each target shard registers the client in its local wait queue for the relevant key
- First shard to have data wakes the coordinator, which cancels registration on other shards
- BLMOVE: atomic pop from source + push to destination, block until source has data
- Source and destination may be on different shards -- requires cross-shard coordination

### Claude's Discretion
- Timer implementation (tokio timer, custom timer wheel, or event loop deadline)
- Maximum number of blocked clients per key (to prevent memory unbounded growth)
- Whether WAIT command is in scope here or Phase 19

### Deferred Ideas (OUT OF SCOPE)
- WAIT command (block until replicas acknowledge) -- Phase 19 (replication)
- XREAD BLOCK (stream blocking reads) -- Phase 18 (streams)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| BLOCK-01 | User can BLPOP/BRPOP for blocking list pops with timeout | BlockingRegistry per-shard, oneshot wakeup, LPUSH/RPUSH post-mutation hooks, timeout via tokio::time::sleep_until |
| BLOCK-02 | User can BZPOPMIN/BZPOPMAX for blocking sorted set pops | Same BlockingRegistry, ZADD post-mutation hook, sorted set pop logic reuse |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio::sync::oneshot | 1.x (bundled) | Wakeup channel for blocked clients | Already used for cross-shard Execute replies; zero-alloc single-use channel |
| tokio::time | 1.x (bundled) | Timeout deadlines via sleep_until/Instant | Already used for expiry_interval and spsc_interval in shard event loop |
| std::collections::HashMap | std | Per-key wait queues | Matches existing patterns (uring_parse_bufs, etc.) |
| std::collections::VecDeque | std | FIFO ordering within per-key queue | Matches CONTEXT.md decision; O(1) push_back/pop_front |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| bytes::Bytes | 1.10 | Key storage in wait queues | Already the canonical key type throughout codebase |
| ringbuf (SPSC) | 0.4 | Cross-shard blocking registration messages | Already used for ShardMessage dispatch |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| oneshot per blocked client | mpsc with shared sender | oneshot is simpler, lighter, one-shot semantics match exactly |
| tokio::time::sleep_until | Custom timer wheel | Timer wheel better at scale (10k+ blocked clients) but overkill here; tokio timer is efficient and already proven |
| Per-key HashMap | BTreeMap | BTreeMap not needed; no ordered iteration required for wait queues |

## Architecture Patterns

### Recommended Project Structure
```
src/
  blocking/
    mod.rs           # BlockingRegistry, WaitEntry, BlockedCommand enum
    wakeup.rs        # Post-mutation wakeup hooks for LPUSH/RPUSH/ZADD
  command/
    blocking.rs      # BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX command handlers
    list.rs          # Add LMOVE (non-blocking prerequisite)
  shard/
    dispatch.rs      # New ShardMessage variants for cross-shard blocking
    coordinator.rs   # Cross-shard blocking coordination
```

### Pattern 1: Per-Shard Blocking Registry
**What:** A `BlockingRegistry` struct owned by each shard (wrapped in `Rc<RefCell<...>>` like databases and pubsub_registry). Contains `HashMap<(usize, Bytes), VecDeque<WaitEntry>>` mapping (db_index, key) to FIFO queue of waiting clients.

**When to use:** Every blocking command registers here; every producer command checks here.

```rust
pub struct WaitEntry {
    /// Unique ID for this wait (used for cross-key deduplication)
    pub wait_id: u64,
    /// Which blocking command type
    pub cmd: BlockedCommand,
    /// Oneshot sender to deliver the result (or timeout signal)
    pub reply_tx: tokio::sync::oneshot::Sender<Option<Frame>>,
    /// Absolute deadline (None = block forever)
    pub deadline: Option<tokio::time::Instant>,
}

pub enum BlockedCommand {
    BLPop,   // pop from left
    BRPop,   // pop from right
    BLMove { destination: Bytes, wherefrom: Direction, whereto: Direction },
    BZPopMin,
    BZPopMax,
}

pub struct BlockingRegistry {
    /// (db_index, key) -> FIFO queue of waiting clients
    waiters: HashMap<(usize, Bytes), VecDeque<WaitEntry>>,
    /// wait_id -> list of (db_index, key) for cross-key cleanup
    wait_keys: HashMap<u64, Vec<(usize, Bytes)>>,
    /// Next wait_id counter
    next_id: u64,
}
```

### Pattern 2: Post-Mutation Wakeup Hook
**What:** After LPUSH/RPUSH/ZADD successfully adds elements, check the blocking registry for any waiters on that key. If found, pop the first waiter, execute the pop operation, and send the result through the oneshot channel. Remove the waiter from all other keys it was watching.

**When to use:** Called inline after every successful list push or sorted set add.

```rust
/// Called after LPUSH/RPUSH adds elements to a key.
/// Returns true if a blocked client was woken (element was consumed).
pub fn try_wake_list_waiter(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
) -> bool {
    let queue_key = (db_index, key.clone());
    while let Some(entry) = registry.front(&queue_key) {
        // Pop the first waiter
        let waiter = registry.pop_front(&queue_key).unwrap();
        // Execute the pop based on command type
        let value = match waiter.cmd {
            BlockedCommand::BLPop => pop_front(db, key),
            BlockedCommand::BRPop => pop_back(db, key),
            BlockedCommand::BLMove { ref destination, wherefrom, whereto } => {
                lmove(db, key, destination, wherefrom, whereto)
            }
            _ => unreachable!(), // sorted set commands don't watch list keys
        };
        // Remove from all other watched keys
        registry.remove_wait(waiter.wait_id);
        // Send result
        let _ = waiter.reply_tx.send(Some(value));
        return true;
    }
    false
}
```

### Pattern 3: Connection-Level Blocking (in handle_connection_sharded)
**What:** Blocking commands are intercepted at the connection level (like SUBSCRIBE, MULTI, AUTH). When a BLPOP arrives: first check if data is available (non-blocking fast path). If not, register in the blocking registry and await the oneshot receiver with a timeout.

**When to use:** All five blocking commands follow this pattern.

```rust
// In handle_connection_sharded, before normal dispatch:
if cmd.eq_ignore_ascii_case(b"BLPOP") {
    // 1. Try non-blocking pop first
    // 2. If no data, register in blocking registry
    // 3. await oneshot with timeout via tokio::select!
    let response = handle_blpop(
        cmd_args, &databases, &blocking_registry,
        selected_db, shard_id, num_shards, &dispatch_tx,
    ).await;
    responses.push(response);
    continue;
}
```

**Critical:** Blocking commands MUST break out of the batch loop. A BLPOP cannot be batched with subsequent commands because it suspends the connection. When a blocking command is encountered in a batch, process it, flush all prior responses, then enter blocking wait mode.

### Pattern 4: Cross-Shard Blocking Coordination
**What:** For multi-key BLPOP where keys span multiple shards, the connection's shard acts as coordinator. It sends `ShardMessage::BlockRegister` to each target shard, each shard registers the waiter locally. When any shard wakes the waiter, it sends the result back via oneshot. The coordinator then sends `ShardMessage::BlockCancel` to all other shards.

**When to use:** Only when BLPOP/BRPOP/BZPOPMIN/BZPOPMAX has keys on multiple shards.

New ShardMessage variants needed:
```rust
pub enum ShardMessage {
    // ... existing variants ...

    /// Register a blocked client waiting for data on a key.
    BlockRegister {
        db_index: usize,
        key: Bytes,
        wait_id: u64,
        cmd: BlockedCommand,
        reply_tx: tokio::sync::oneshot::Sender<Option<Frame>>,
    },
    /// Cancel a blocked client registration (woken by another shard).
    BlockCancel {
        db_index: usize,
        key: Bytes,
        wait_id: u64,
    },
}
```

### Anti-Patterns to Avoid
- **Polling loop for timeout:** Never busy-wait or poll for timeouts. Use `tokio::time::sleep_until` combined with `tokio::select!` on the oneshot receiver.
- **Holding database borrow across await:** The `databases.borrow_mut()` MUST be dropped before any `.await` point. Execute the non-blocking check, drop the borrow, then await.
- **Batch processing blocking commands:** A blocking command in a pipeline batch must flush prior responses and break out of the batch loop before entering blocking mode.
- **Double-service on multi-key wakeup:** When a client watches multiple keys, the wakeup hook MUST atomically remove the client from ALL watched keys before serving.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Timeout tracking | Custom timer wheel | `tokio::time::sleep_until` | Tokio's timer wheel is highly optimized; sub-ms precision; integrates with select! |
| Wakeup channel | Custom channel/flag | `tokio::sync::oneshot` | Already used for Execute replies; zero-overhead single-use semantics |
| FIFO queue | Custom linked list | `VecDeque<WaitEntry>` | O(1) push_back/pop_front; std library; well-tested |
| Cross-shard messaging | New channel type | Existing SPSC ringbuf + ShardMessage enum | Already proven for Execute/MultiExecute/PubSubFanOut patterns |

**Key insight:** The entire blocking infrastructure is built from primitives already used in the codebase (oneshot, SPSC, Rc<RefCell>). The novelty is the BlockingRegistry data structure and the post-mutation wakeup hooks, not the channel/timer mechanisms.

## Common Pitfalls

### Pitfall 1: Database Borrow Across Await Points
**What goes wrong:** `databases.borrow_mut()` held while awaiting the oneshot receiver causes a RefCell runtime panic when another task borrows.
**Why it happens:** Natural to write `let db = databases.borrow_mut(); /* check data */ /* await wakeup */`
**How to avoid:** Split into two phases: (1) borrow, check data, register waiter, drop borrow; (2) await oneshot.
**Warning signs:** RefCell panic at runtime: "already borrowed: BorrowMutError"

### Pitfall 2: BLPOP Multi-Key Double-Service
**What goes wrong:** Client registered on keys A and B. Both get data simultaneously (e.g., LPUSH on both in same tick). Client gets served twice.
**Why it happens:** Wakeup hooks for A and B both find the same WaitEntry.
**How to avoid:** Use a shared `wait_id` across all registrations. On wakeup, atomically remove all entries with that wait_id. Use oneshot (can only send once) as a natural guard -- second sender gets Err.
**Warning signs:** Client receives two responses; protocol desync.

### Pitfall 3: Blocking in Batch Pipeline
**What goes wrong:** BLPOP arrives in a pipeline batch `[SET foo bar, BLPOP mylist 0, GET foo]`. If processed as a normal batch item, GET foo is never reached because BLPOP blocks.
**Why it happens:** Pipeline batching collects N frames and processes them in a loop.
**How to avoid:** When a blocking command is detected in the batch, flush all responses accumulated so far, then enter blocking mode. Remaining batch items are processed after the blocking command completes (or dropped -- Redis does not pipeline after blocking commands in practice).
**Warning signs:** Client hangs; responses arrive out of order.

### Pitfall 4: BLMOVE Cross-Shard Atomicity
**What goes wrong:** BLMOVE source and destination are on different shards. Pop from source succeeds, but push to destination fails (shard down, ring buffer full). Element is lost.
**Why it happens:** Two-phase operation across shards is not atomic.
**How to avoid:** For cross-shard BLMOVE: pop from source, push to destination via ShardMessage::Execute, only acknowledge success after destination confirms. If destination fails, push element back to source.
**Warning signs:** Data loss; elements disappear.

### Pitfall 5: Timeout Expiry Cleanup
**What goes wrong:** A blocked client times out but its WaitEntry remains in the registry. Next LPUSH finds a stale entry with a closed oneshot sender, wastes cycles.
**Why it happens:** Timeout fires on the connection task but cleanup runs on the shard's timer scan -- race between the two.
**How to avoid:** Timeout scan in the shard event loop removes expired entries every 10ms. The oneshot sender's `send()` returns Err if the receiver is already dropped (timeout fired), which the wakeup hook handles gracefully by trying the next waiter.
**Warning signs:** Memory growth in wait queues; log noise from failed oneshot sends.

### Pitfall 6: Transaction/MULTI Context
**What goes wrong:** BLPOP inside MULTI/EXEC should NOT block -- it should behave like LPOP (non-blocking).
**Why it happens:** Redis spec says blocking commands inside MULTI behave as non-blocking variants.
**How to avoid:** Check `in_multi` flag before entering blocking mode. If in transaction, delegate to non-blocking LPOP/RPOP directly.
**Warning signs:** Transaction hangs indefinitely.

## Code Examples

### BLPOP Response Format (Redis Protocol)
```
# Success: returns [key_name, popped_value]
*2\r\n$5\r\nmylist\r\n$5\r\nhello\r\n

# Timeout: returns nil
*-1\r\n   (RESP2)
_\r\n      (RESP3)
```

### BLPOP Command Signature
```
BLPOP key [key ...] timeout
```
- timeout is the LAST argument (double, in seconds)
- 0 = block forever
- Returns: [key_name, value] or nil on timeout

### BRPOP Command Signature
```
BRPOP key [key ...] timeout
```
- Identical to BLPOP but pops from tail

### BLMOVE Command Signature
```
BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
```
- Returns: the moved element, or nil on timeout
- Atomically pops from source direction, pushes to destination direction

### BZPOPMIN/BZPOPMAX Command Signature
```
BZPOPMIN key [key ...] timeout
BZPOPMAX key [key ...] timeout
```
- Returns: [key_name, member, score] or nil on timeout

### Prerequisite: LMOVE Implementation
```rust
/// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
/// Atomically pops from source and pushes to destination.
pub fn lmove(db: &mut Database, args: &[Frame]) -> Frame {
    // args: [source, destination, wherefrom, whereto]
    if args.len() != 4 {
        return err_wrong_args("LMOVE");
    }
    let source = extract_bytes(&args[0]).unwrap();
    let destination = extract_bytes(&args[1]).unwrap();
    let wherefrom = parse_direction(&args[2]); // LEFT or RIGHT
    let whereto = parse_direction(&args[3]);   // LEFT or RIGHT

    // Pop from source
    let value = match wherefrom {
        Direction::Left => pop_front(db, source),
        Direction::Right => pop_back(db, source),
    };
    let value = match value {
        Some(v) => v,
        None => return Frame::Null,
    };

    // Push to destination
    match whereto {
        Direction::Left => push_front(db, destination, &value),
        Direction::Right => push_back(db, destination, &value),
    }

    Frame::BulkString(value)
}
```

### Blocking Registry Integration with Shard Event Loop
```rust
// In Shard::run(), add a 10ms timer for timeout scanning:
let mut block_timeout_interval = tokio::time::interval(Duration::from_millis(10));

// In the tokio::select! loop:
_ = block_timeout_interval.tick() => {
    let now = tokio::time::Instant::now();
    blocking_registry.borrow_mut().expire_timed_out(now);
}
```

### Wakeup Hook Integration in LPUSH
```rust
// After successful LPUSH in the dispatch path:
// (In handle_connection_sharded or handle_shard_message_shared)
if is_list_push_command(cmd) && !matches!(response, Frame::Error(_)) {
    if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
        let mut reg = blocking_registry.borrow_mut();
        let mut dbs = databases.borrow_mut();
        try_wake_list_waiter(&mut reg, &mut dbs[db_index], db_index, &key);
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| BRPOPLPUSH (deprecated) | BLMOVE | Redis 6.2 | BLMOVE is the generalized replacement |
| Integer timeout (seconds) | Double timeout (sub-second) | Redis 6.0 | Timeout is a double, not integer; 0.1 = 100ms |
| BLPOP in MULTI blocks | BLPOP in MULTI = LPOP (non-blocking) | Always | Blocking commands in transactions are non-blocking |

**Deprecated/outdated:**
- BRPOPLPUSH: deprecated in Redis 6.2, replaced by BLMOVE with LEFT/RIGHT direction args

## Open Questions

1. **Maximum blocked clients per key**
   - What we know: Redis has no hard limit; memory is the constraint
   - What's unclear: Whether to add a configurable cap (e.g., 10000 per key)
   - Recommendation: Default unlimited, add a config option `max-blocked-clients-per-key` that returns ERR when exceeded. Not critical for v1.

2. **BLMOVE cross-shard atomicity**
   - What we know: Source pop and destination push must be atomic
   - What's unclear: Exact recovery strategy if destination shard is unreachable
   - Recommendation: For v1, require source and destination on same shard (hash tags). Log warning and return ERR for cross-shard BLMOVE. Can be enhanced later.

3. **Interaction with key expiration**
   - What we know: If a key expires while clients are blocking on it, waiters should remain (wait for new data, not wake with nil)
   - What's unclear: Redis exact behavior on key expiry during block
   - Recommendation: Key expiration does NOT wake blocked clients. The waiter remains until timeout or new data.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test + redis crate 0.27 (integration) |
| Config file | Cargo.toml [dev-dependencies] |
| Quick run command | `cargo test --lib blocking` |
| Full suite command | `cargo test` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| BLOCK-01a | BLPOP returns immediately when data exists | integration | `cargo test --test integration blpop_immediate` | No -- Wave 0 |
| BLOCK-01b | BLPOP blocks and wakes on LPUSH | integration | `cargo test --test integration blpop_blocks_then_wakes` | No -- Wave 0 |
| BLOCK-01c | BLPOP timeout returns nil | integration | `cargo test --test integration blpop_timeout` | No -- Wave 0 |
| BLOCK-01d | BRPOP pops from tail | integration | `cargo test --test integration brpop_basic` | No -- Wave 0 |
| BLOCK-01e | BLPOP FIFO fairness | integration | `cargo test --test integration blpop_fifo` | No -- Wave 0 |
| BLOCK-01f | BLPOP multi-key first-non-empty | integration | `cargo test --test integration blpop_multi_key` | No -- Wave 0 |
| BLOCK-02a | BZPOPMIN returns immediately when data exists | integration | `cargo test --test integration bzpopmin_immediate` | No -- Wave 0 |
| BLOCK-02b | BZPOPMIN blocks and wakes on ZADD | integration | `cargo test --test integration bzpopmin_blocks_wakes` | No -- Wave 0 |
| BLOCK-02c | BZPOPMAX pops highest score | integration | `cargo test --test integration bzpopmax_basic` | No -- Wave 0 |
| BLOCK-02d | BLMOVE atomic pop+push | integration | `cargo test --test integration blmove_basic` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib blocking`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `src/blocking/mod.rs` -- BlockingRegistry struct and core logic
- [ ] `src/command/blocking.rs` -- command handlers for all 5 blocking commands
- [ ] `src/command/list.rs` -- add LMOVE (non-blocking prerequisite)
- [ ] Integration tests for blocking behavior (requires multi-connection async test pattern)
- [ ] Unit tests for BlockingRegistry (register, wake, timeout, multi-key dedup)

## Sources

### Primary (HIGH confidence)
- [Redis BLPOP docs](https://redis.io/docs/latest/commands/blpop/) -- Syntax, response format, FIFO semantics, multi-key behavior
- [Redis BLMOVE docs](https://redis.io/docs/latest/commands/blmove/) -- Syntax, atomicity, since Redis 6.2
- [Redis BZPOPMIN docs](https://redis.io/docs/latest/commands/bzpopmin/) -- Syntax, timeout as double
- [Redis BZPOPMAX docs](https://redis.io/docs/latest/commands/bzpopmax/) -- Syntax, response format
- Codebase analysis: `src/shard/mod.rs`, `src/server/connection.rs`, `src/shard/dispatch.rs`, `src/shard/coordinator.rs` -- existing patterns for cross-shard dispatch, event loop, connection handling

### Secondary (MEDIUM confidence)
- [Redis BRPOPLPUSH docs](https://redis.io/docs/latest/commands/brpoplpush/) -- Deprecated predecessor to BLMOVE

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all primitives already in use (tokio oneshot, SPSC, Rc<RefCell>)
- Architecture: HIGH -- patterns directly mirror existing PubSub and cross-shard dispatch
- Pitfalls: HIGH -- identified from code analysis (RefCell borrow, batch pipeline, double-service)
- Redis semantics: HIGH -- verified against official Redis documentation

**Research date:** 2026-03-24
**Valid until:** 2026-04-24 (stable domain, unlikely to change)
