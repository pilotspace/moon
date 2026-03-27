# Phase 5: Pub/Sub, Transactions, and Eviction - Research

**Researched:** 2026-03-23
**Domain:** Channel messaging, atomic transactions, memory-bounded eviction
**Confidence:** HIGH

## Summary

Phase 5 introduces three largely independent subsystems: Pub/Sub messaging (channel-based fire-and-forget delivery), MULTI/EXEC transactions (command queuing with optimistic locking via WATCH), and memory eviction (approximated LRU/LFU with configurable policies). Additionally, CONFIG GET/SET provides runtime configuration tuning.

The codebase is well-structured for these additions. The connection handler (`connection.rs`) already demonstrates the pattern for mode-switching (auth gate, BGSAVE interception) that Pub/Sub subscriber mode and MULTI transaction mode will follow. The `glob_match` function in `key.rs` is reusable for PSUBSCRIBE patterns and CONFIG GET pattern matching. The `Entry` struct needs three new fields (`version`, `last_access`, `access_counter`) for WATCH and eviction. The `Database` struct needs eviction methods and version tracking on mutations.

**Primary recommendation:** Implement the three subsystems in order: (1) Entry metadata additions and eviction, (2) transactions (MULTI/EXEC/DISCARD/WATCH), (3) Pub/Sub, (4) CONFIG GET/SET. Eviction first because it adds fields to Entry that WATCH also needs (version). Transactions second because they are contained within the existing connection handler pattern. Pub/Sub third because it requires a new shared subsystem (PubSubRegistry) and connection loop restructuring.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Bounded mpsc channel per subscriber (capacity 256) -- prevents OOM from slow clients
- Slow subscribers that fall behind are disconnected (channel full -> drop subscriber)
- Subscriber enters special mode: only SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PING/QUIT accepted -- matches Redis behavior
- Pub/Sub handled at connection level, NOT through normal command dispatch
- Global `PubSubRegistry` struct: `HashMap<Bytes, Vec<Subscriber>>` for channel subscriptions + `Vec<(pattern, Vec<Subscriber>)>` for pattern subscriptions
- PUBLISH iterates exact channel subscribers + checks all patterns via `glob_match` (reuse from command/key.rs)
- Pub/Sub messages NOT logged to AOF -- fire-and-forget, not persisted
- SUBSCRIBE response format: `["subscribe", channel_name, subscription_count]` as Array
- Per-connection transaction state: `in_multi: bool`, `command_queue: Vec<Frame>`, `watched_keys: HashMap<Bytes, u64>`
- MULTI sets `in_multi = true` -- subsequent commands queued instead of executed
- EXEC runs all queued commands atomically under the database lock, returns Array of results
- DISCARD clears queue and exits multi mode
- Queued commands with syntax errors: queue them anyway, EXEC returns per-command error (matches Redis 2.6+)
- WATCH tracks key version numbers -- before EXEC, check if any watched key's version changed since WATCH
- If WATCH detects modification: EXEC returns Null (transaction aborted), client retries
- Key version: add `version: u64` field to Entry, increment on every mutation
- WATCH state cleared after EXEC or DISCARD (regardless of success)
- Eviction triggers before every write command when `used_memory > maxmemory`
- Approximated LRU: sample `maxmemory-samples` (default 5) random keys, evict the one with oldest `last_access` time
- LFU: Morris counter (8-bit logarithmic frequency) + decay over time, sample and evict lowest frequency
- Per-key metadata additions to Entry: `last_access: Instant` (for LRU), `access_counter: u8` (for LFU, Morris counter)
- Eviction policies: `noeviction`, `allkeys-lru`, `allkeys-lfu`, `volatile-lru`, `volatile-lfu`, `allkeys-random`, `volatile-random`, `volatile-ttl`
- `maxmemory` tracked via estimated memory usage (key size + value size approximation)
- `maxmemory = 0` means no limit (default)
- CONFIG GET/SET: runtime-configurable parameters: `maxmemory`, `maxmemory-policy`, `maxmemory-samples`, `save`, `appendonly`, `appendfsync`
- CONFIG GET returns alternating name-value pairs (Array), supports glob patterns
- CONFIG SET updates live ServerConfig -- changes take effect immediately
- Not persisting config changes to file (CONFIG REWRITE deferred to v2)
- Pub/Sub should be a separate subsystem module (src/pubsub/) -- not tangled with storage
- Transaction EXEC should hold the database lock for the entire batch -- true atomicity
- Eviction check should be a single function call at the top of every write handler
- CONFIG GET/SET should be extensible -- easy to add new parameters later

### Claude's Discretion
- PubSubRegistry internal locking strategy (separate Mutex or part of Database)
- Exact memory estimation formula for eviction threshold
- Morris counter parameters (probability factor, decay time constant)
- Whether MULTI/EXEC responses include "+QUEUED" for each queued command (they should, per Redis)
- CONFIG parameter name matching (case-insensitive per Redis)
- Transaction command queue size limit (if any)

### Deferred Ideas (OUT OF SCOPE)
- Blocking commands (BLPOP/BRPOP) -- v2
- CONFIG REWRITE -- v2
- RESP3 protocol -- v2
- Memory-efficient encodings -- v2
- CLIENT command (CLIENT LIST, CLIENT SETNAME) -- v2
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| PUB-01 | User can SUBSCRIBE to channels and receive messages | Pub/Sub architecture with bounded mpsc per subscriber, push protocol in connection loop |
| PUB-02 | User can PUBLISH messages to channels | PubSubRegistry fan-out to channel subscribers + pattern matching |
| PUB-03 | User can PSUBSCRIBE for pattern-based subscriptions | glob_match reuse from key.rs, pmessage 4-element array format |
| PUB-04 | User can UNSUBSCRIBE/PUNSUBSCRIBE from channels | Subscriber cleanup, subscription count tracking |
| TXN-01 | User can MULTI/EXEC/DISCARD for atomic command execution | Per-connection transaction state, command queue, QUEUED response, atomic EXEC under lock |
| TXN-02 | User can WATCH keys for optimistic locking (CAS) | Entry version field, version snapshot at WATCH time, version check before EXEC |
| EVIC-01 | Server supports LRU eviction policy (approximated, sampled) | Entry last_access field, sample-based eviction, maxmemory-samples config |
| EVIC-02 | Server supports LFU eviction policy with Morris counter and decay | Entry access_counter u8, probabilistic increment with lfu-log-factor, time-based decay |
| EVIC-03 | User can configure maxmemory and eviction policy via CONFIG SET | CONFIG GET/SET command, runtime ServerConfig mutation, glob pattern matching |
</phase_requirements>

## Standard Stack

### Core (already in Cargo.toml)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1.x | Async runtime, mpsc channels for Pub/Sub delivery | Already used, mpsc::channel for per-subscriber message delivery |
| bytes | 1.10 | Zero-copy byte buffers | Already used for Frame/Entry |
| rand | 0.9 | Random sampling for eviction | Already used for expiration sampling |

### No New Dependencies Required
All Phase 5 features can be implemented using existing dependencies. Pub/Sub uses `tokio::sync::mpsc` (bounded). Eviction uses `rand` for sampling. CONFIG uses existing `glob_match`. No additional crates needed.

## Architecture Patterns

### Recommended Module Structure
```
src/
├── pubsub/              # NEW: Pub/Sub subsystem
│   ├── mod.rs           # PubSubRegistry struct + public API
│   └── subscriber.rs    # Subscriber struct (wraps mpsc::Sender)
├── storage/
│   ├── entry.rs         # ADD: version, last_access, access_counter fields
│   ├── db.rs            # ADD: eviction methods, version tracking, memory estimation
│   └── eviction.rs      # NEW: eviction logic (LRU/LFU/random/ttl)
├── command/
│   ├── mod.rs           # ADD: CONFIG, extend dispatch
│   └── config.rs        # NEW: CONFIG GET/SET handlers
├── server/
│   └── connection.rs    # MODIFY: add MULTI mode, SUBSCRIBE mode, eviction check
└── config.rs            # MODIFY: add maxmemory, maxmemory-policy, maxmemory-samples
```

### Pattern 1: Pub/Sub Registry (Separate Mutex)
**What:** PubSubRegistry should use its own `Arc<Mutex<PubSubRegistry>>`, separate from the database mutex.
**When to use:** Always -- Pub/Sub is orthogonal to storage. Publishing a message should not require the database lock.
**Example:**
```rust
// Source: Design decision based on Redis architecture
pub struct PubSubRegistry {
    /// Exact channel subscriptions: channel_name -> list of subscribers
    channels: HashMap<Bytes, Vec<Subscriber>>,
    /// Pattern subscriptions: (pattern, list of subscribers)
    patterns: Vec<(Bytes, Vec<Subscriber>)>,
}

pub struct Subscriber {
    /// Bounded sender -- if full, subscriber is dropped
    tx: mpsc::Sender<Frame>,
    /// Unique ID for cleanup
    id: u64,
}

impl PubSubRegistry {
    /// Publish to a channel. Returns number of subscribers that received the message.
    pub fn publish(&mut self, channel: &Bytes, message: &Bytes) -> i64 {
        let mut count = 0i64;

        // Exact channel subscribers
        if let Some(subs) = self.channels.get_mut(channel) {
            subs.retain(|sub| {
                // try_send: non-blocking, bounded
                match sub.tx.try_send(make_message_frame(channel, message)) {
                    Ok(()) => { count += 1; true }
                    Err(mpsc::error::TrySendError::Full(_)) => false, // disconnect slow sub
                    Err(mpsc::error::TrySendError::Closed(_)) => false,
                }
            });
        }

        // Pattern subscribers
        for (pattern, subs) in &mut self.patterns {
            if glob_match(pattern, channel) {
                subs.retain(|sub| {
                    match sub.tx.try_send(make_pmessage_frame(pattern, channel, message)) {
                        Ok(()) => { count += 1; true }
                        Err(_) => false,
                    }
                });
            }
        }

        count
    }
}
```

### Pattern 2: Connection Handler Mode Switching
**What:** The connection handler loop gains two new modes: transaction mode (in_multi) and subscriber mode (subscribed).
**When to use:** Transaction mode queues commands; subscriber mode restricts to pub/sub commands and reads from message channel.
**Example:**
```rust
// Connection-level state additions
struct ConnectionState {
    selected_db: usize,
    authenticated: bool,
    // Transaction state
    in_multi: bool,
    command_queue: Vec<Frame>,
    watched_keys: HashMap<Bytes, u64>,  // key -> version at WATCH time
    // Pub/Sub state
    subscriptions: usize,  // total channel + pattern count
    pubsub_rx: Option<mpsc::Receiver<Frame>>,
    subscriber_id: u64,
}
```

### Pattern 3: Entry Metadata for WATCH + Eviction
**What:** Add version, last_access, and access_counter to Entry. Increment version on every mutation.
**Example:**
```rust
pub struct Entry {
    pub value: RedisValue,
    pub expires_at: Option<Instant>,
    pub created_at: Instant,
    pub version: u64,           // incremented on every mutation (for WATCH)
    pub last_access: Instant,   // updated on every access (for LRU)
    pub access_counter: u8,     // Morris counter (for LFU)
}
```

### Pattern 4: Eviction Before Write
**What:** Before executing any write command, check memory usage against maxmemory and evict if needed.
**Example:**
```rust
// In connection handler, before dispatch of write commands:
fn try_evict_if_needed(db: &mut Database, config: &EvictionConfig) -> Result<(), Frame> {
    if config.maxmemory == 0 { return Ok(()); }
    while db.estimated_memory() > config.maxmemory {
        if config.policy == EvictionPolicy::NoEviction {
            return Err(Frame::Error(Bytes::from_static(
                b"OOM command not allowed when used memory > 'maxmemory'"
            )));
        }
        if !db.evict_one(&config) {
            return Err(Frame::Error(Bytes::from_static(
                b"OOM command not allowed when used memory > 'maxmemory'"
            )));
        }
    }
    Ok(())
}
```

### Pattern 5: Morris Counter for LFU
**What:** 8-bit logarithmic frequency counter with probabilistic increment and time-based decay.
**Recommendation for discretion areas:**
- **lfu-log-factor:** Default 10 (matches Redis). Increment probability = 1.0 / (counter * lfu_log_factor + 1)
- **lfu-decay-time:** Default 1 minute. Decrement counter by (elapsed_minutes / lfu_decay_time)
- **Initial value:** 5 (LFU_INIT_VAL in Redis) -- new keys start at 5 so they are not immediately evicted
**Example:**
```rust
fn lfu_log_incr(counter: u8, lfu_log_factor: u8) -> u8 {
    if counter == 255 { return 255; }
    let r: f64 = rand::random();
    let base_val = (counter as f64 - 5.0).max(0.0); // LFU_INIT_VAL = 5
    let p = 1.0 / (base_val * lfu_log_factor as f64 + 1.0);
    if r < p { counter + 1 } else { counter }
}

fn lfu_decay(counter: u8, last_access: Instant, lfu_decay_time: u64) -> u8 {
    if lfu_decay_time == 0 { return counter; }
    let elapsed_min = last_access.elapsed().as_secs() / 60;
    let decay = (elapsed_min / lfu_decay_time) as u8;
    counter.saturating_sub(decay)
}
```

### Pattern 6: CONFIG GET/SET with Glob Patterns
**What:** CONFIG GET supports glob pattern matching on parameter names (case-insensitive). CONFIG SET updates live config.
**Recommendation:** Use a match table of (name, getter, setter) tuples for extensibility.
**Example:**
```rust
fn config_get(config: &ServerConfig, pattern: &[u8]) -> Frame {
    let pattern_lower = pattern.to_ascii_lowercase();
    let mut results = Vec::new();

    let params: &[(&[u8], &dyn Fn() -> String)] = &[
        (b"maxmemory", &|| config.maxmemory.to_string()),
        (b"maxmemory-policy", &|| config.maxmemory_policy.clone()),
        (b"maxmemory-samples", &|| config.maxmemory_samples.to_string()),
        // ... more params
    ];

    for (name, getter) in params {
        if glob_match(&pattern_lower, name) {
            results.push(Frame::BulkString(Bytes::copy_from_slice(name)));
            results.push(Frame::BulkString(Bytes::from(getter())));
        }
    }

    Frame::Array(results)
}
```

### Anti-Patterns to Avoid
- **Pub/Sub through dispatch:** Do NOT route Pub/Sub through the normal command dispatch function. Subscriber mode is a connection-level concern -- the subscribed client must read from its mpsc receiver, not from command dispatch.
- **Holding DB lock during PUBLISH fan-out:** PUBLISH should acquire the PubSubRegistry lock (separate from DB), iterate subscribers and try_send. Never hold the DB lock while sending to subscriber channels.
- **Unbounded command queue in MULTI:** Although Redis has no explicit limit, set a reasonable cap (e.g., 10000 commands) to prevent memory abuse. Return an error on EXEC if limit exceeded.
- **Blocking eviction:** Eviction sampling must be bounded. If no suitable key is found after maxmemory-samples, return OOM error. Do not loop indefinitely.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Bounded async channel | Custom ring buffer | `tokio::sync::mpsc::channel(256)` | Battle-tested backpressure, try_send for non-blocking publish |
| Glob pattern matching | New glob implementation | Existing `glob_match` in command/key.rs | Already tested, handles all Redis glob features |
| Random sampling | Manual iterator skip | `rand::seq::IndexedRandom::choose_multiple` | Already used in expiration.rs, correct uniform sampling |
| Atomic u64 for subscriber IDs | Custom ID generation | `AtomicU64::fetch_add` | Simple, correct, no allocation |

## Common Pitfalls

### Pitfall 1: Pub/Sub Message Ordering and Deadlock
**What goes wrong:** Publishing inside a database lock while the subscriber's connection handler also needs the database lock creates a deadlock. Or, holding the PubSubRegistry lock while calling try_send on a full channel that blocks.
**Why it happens:** Nested lock acquisition in different orders across tasks.
**How to avoid:** PubSubRegistry has its own separate Mutex. PUBLISH acquires only the PubSubRegistry lock, never the DB lock. Use `try_send` (non-blocking) -- never `send` (blocking) -- inside the registry lock. PUBLISH command acquires DB lock (for version/access tracking) first, then registry lock, always in that order.
**Warning signs:** Server hangs under concurrent pub/sub + write load.

### Pitfall 2: WATCH Version Check Race Condition
**What goes wrong:** Between checking watched key versions and executing the queued commands, another connection modifies a key. This would miss the modification.
**Why it happens:** If version check and EXEC are not atomic (not under the same lock acquisition).
**How to avoid:** EXEC must: (1) acquire the database lock, (2) check all watched key versions under that lock, (3) if all match, execute all queued commands still under the lock, (4) release lock. The version check and execution must be a single critical section.
**Warning signs:** WATCH/MULTI/EXEC tests pass individually but fail under concurrent modification.

### Pitfall 3: Entry Constructor Explosion
**What goes wrong:** Adding 3 new fields to Entry means every Entry constructor and every test that creates entries must be updated. Missing a field causes uninitialized metadata.
**Why it happens:** Rust requires all fields to be initialized. There are 6 existing constructors in Entry.
**How to avoid:** Set sensible defaults: `version: 0`, `last_access: Instant::now()`, `access_counter: 5` (LFU_INIT_VAL). Update all 6 constructors at once. Use a builder or default helper if constructors become unwieldy.
**Warning signs:** Compilation errors across the entire codebase after adding Entry fields.

### Pitfall 4: ServerConfig Mutability for CONFIG SET
**What goes wrong:** `ServerConfig` is currently `Arc<ServerConfig>` (immutable after startup). CONFIG SET needs to mutate it at runtime.
**Why it happens:** The config was designed as read-only startup configuration.
**How to avoid:** Change to `Arc<Mutex<ServerConfig>>` or, better, extract the mutable runtime parameters into a separate `Arc<RwLock<RuntimeConfig>>` struct. Only the parameters listed for CONFIG SET need to be mutable: maxmemory, maxmemory-policy, maxmemory-samples, save, appendonly, appendfsync. Keep startup-only params (bind, port, databases) immutable.
**Warning signs:** Trying to mutate an Arc without interior mutability.

### Pitfall 5: Memory Estimation Accuracy
**What goes wrong:** Over-estimating memory causes premature eviction. Under-estimating causes OOM.
**Why it happens:** Rust's allocator overhead, HashMap internal overhead, and Bytes reference-counted buffers make exact measurement difficult.
**How to avoid:** Use a simple but consistent formula: `key.len() + value_size_estimate + 128` (per-entry overhead for HashMap bucket, Entry struct, metadata). Value size: String = bytes.len(), Hash = sum of field+value lengths, List = sum of element lengths, etc. The formula does not need to be exact -- it needs to be consistent and conservative.
**Warning signs:** maxmemory eviction triggers at unexpected times.

### Pitfall 6: Subscriber Connection Loop Complexity
**What goes wrong:** The connection handler becomes deeply nested and hard to follow with three modes (normal, multi, subscribed) each with different behavior.
**Why it happens:** All three modes share the same TCP connection and frame read/write.
**How to avoid:** Use an enum for connection mode: `enum ConnectionMode { Normal, Multi { queue, watched }, Subscribed { rx, sub_count } }`. Match on mode at the top of the loop. Keep each mode's handling in separate functions.
**Warning signs:** Connection handler exceeds 300 lines with deeply nested if/else chains.

## Code Examples

### MULTI/EXEC Transaction Flow
```rust
// Source: Redis transaction semantics (redis.io/docs/latest/develop/using-commands/transactions/)
// In connection handler:
match extract_command(&frame) {
    Some((ref cmd, _)) if cmd == b"MULTI" => {
        if state.in_multi {
            send(Frame::Error(b"ERR MULTI calls can not be nested")).await;
        } else {
            state.in_multi = true;
            state.command_queue.clear();
            send(Frame::SimpleString(b"OK")).await;
        }
    }
    Some((ref cmd, _)) if cmd == b"EXEC" => {
        if !state.in_multi {
            send(Frame::Error(b"ERR EXEC without MULTI")).await;
        } else {
            state.in_multi = false;
            // Acquire DB lock, check WATCH versions, execute queue
            let result = execute_transaction(&db, &mut state);
            state.watched_keys.clear();
            send(result).await;
        }
    }
    Some((ref cmd, _)) if cmd == b"DISCARD" => {
        if !state.in_multi {
            send(Frame::Error(b"ERR DISCARD without MULTI")).await;
        } else {
            state.in_multi = false;
            state.command_queue.clear();
            state.watched_keys.clear();
            send(Frame::SimpleString(b"OK")).await;
        }
    }
    Some((ref cmd, _)) if cmd == b"WATCH" && !state.in_multi => {
        // WATCH must be called BEFORE MULTI
        let dbs = db.lock().unwrap();
        for key in watch_keys {
            let version = dbs[selected_db].get_version(&key);
            state.watched_keys.insert(key, version);
        }
        send(Frame::SimpleString(b"OK")).await;
    }
    _ if state.in_multi => {
        // Queue the command, respond with QUEUED
        state.command_queue.push(frame);
        send(Frame::SimpleString(b"QUEUED")).await;
    }
    _ => { /* normal dispatch */ }
}
```

### Pub/Sub Subscriber Mode
```rust
// Source: Redis Pub/Sub protocol (redis.io/docs/latest/develop/pubsub/)
// In connection handler, when subscribed (subscriptions > 0):
tokio::select! {
    // Read commands from client (only sub/unsub/ping/quit allowed)
    result = framed.next() => {
        match extract_command(&frame) {
            Some((cmd, args)) if cmd == b"SUBSCRIBE" => {
                for channel in args {
                    registry.lock().unwrap().subscribe(channel, subscriber.clone());
                    state.subscriptions += 1;
                    send(subscribe_response(channel, state.subscriptions)).await;
                }
            }
            Some((cmd, args)) if cmd == b"UNSUBSCRIBE" => {
                // unsubscribe from specified channels (or all if no args)
            }
            Some((cmd, _)) if cmd == b"PING" => {
                send(Frame::SimpleString(b"PONG")).await;
            }
            Some((cmd, _)) if cmd == b"QUIT" => { break; }
            _ => {
                send(Frame::Error(b"ERR Can't execute in subscriber mode")).await;
            }
        }
    }
    // Receive published messages from registry
    Some(msg) = pubsub_rx.recv() => {
        send(msg).await;
    }
    _ = shutdown.cancelled() => { break; }
}
```

### Pub/Sub Message Frames
```rust
// subscribe confirmation: ["subscribe", channel, count]
fn subscribe_response(channel: &Bytes, count: usize) -> Frame {
    Frame::Array(vec![
        Frame::BulkString(Bytes::from_static(b"subscribe")),
        Frame::BulkString(channel.clone()),
        Frame::Integer(count as i64),
    ])
}

// message delivery: ["message", channel, payload]
fn message_frame(channel: &Bytes, payload: &Bytes) -> Frame {
    Frame::Array(vec![
        Frame::BulkString(Bytes::from_static(b"message")),
        Frame::BulkString(channel.clone()),
        Frame::BulkString(payload.clone()),
    ])
}

// pattern message delivery: ["pmessage", pattern, channel, payload]
fn pmessage_frame(pattern: &Bytes, channel: &Bytes, payload: &Bytes) -> Frame {
    Frame::Array(vec![
        Frame::BulkString(Bytes::from_static(b"pmessage")),
        Frame::BulkString(pattern.clone()),
        Frame::BulkString(channel.clone()),
        Frame::BulkString(payload.clone()),
    ])
}
```

### Eviction Sample-Based Selection
```rust
// Source: Redis eviction docs (redis.io/docs/latest/develop/reference/eviction/)
fn evict_one_lru(db: &mut Database, samples: usize) -> bool {
    let keys: Vec<Bytes> = db.keys().cloned().collect();
    if keys.is_empty() { return false; }

    let mut rng = rand::rng();
    let sampled: Vec<&Bytes> = keys.choose_multiple(&mut rng, samples.min(keys.len())).collect();

    let mut oldest_key = None;
    let mut oldest_time = Instant::now();

    for key in sampled {
        if let Some(entry) = db.data().get(key) {
            if entry.last_access < oldest_time {
                oldest_time = entry.last_access;
                oldest_key = Some(key.clone());
            }
        }
    }

    if let Some(key) = oldest_key {
        db.remove(&key);
        true
    } else {
        false
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| True LRU (doubly-linked list) | Approximated LRU (random sampling) | Redis 3.0 (2015) | 5 samples approximates true LRU closely; 10 samples is virtually identical |
| No LFU | Morris counter LFU | Redis 4.0 (2017) | 8-bit counter with decay, better cache hit rates for frequency-skewed workloads |
| WATCH detects only writes | WATCH detects writes + expiry + eviction | Redis 6.0.9 (2020) | Expired/evicted keys now correctly abort WATCH transactions |
| Pub/Sub global pattern scan | Pub/Sub per-channel subscription lists | Always | Standard approach, O(subscribers) not O(all_clients) |

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (built-in) + redis crate 0.27 for integration |
| Config file | Cargo.toml `[dev-dependencies]` |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PUB-01 | SUBSCRIBE receives published messages | integration | `cargo test --test integration pubsub_subscribe -x` | Wave 0 |
| PUB-02 | PUBLISH delivers to subscribers, returns count | integration | `cargo test --test integration pubsub_publish -x` | Wave 0 |
| PUB-03 | PSUBSCRIBE pattern matching | integration | `cargo test --test integration pubsub_psubscribe -x` | Wave 0 |
| PUB-04 | UNSUBSCRIBE/PUNSUBSCRIBE cleanup | integration | `cargo test --test integration pubsub_unsubscribe -x` | Wave 0 |
| TXN-01 | MULTI/EXEC atomicity, DISCARD, QUEUED | integration + unit | `cargo test --test integration multi_exec -x` | Wave 0 |
| TXN-02 | WATCH aborts on key modification | integration | `cargo test --test integration watch -x` | Wave 0 |
| EVIC-01 | LRU eviction under maxmemory | unit + integration | `cargo test eviction_lru -x` | Wave 0 |
| EVIC-02 | LFU eviction with Morris counter | unit | `cargo test eviction_lfu -x` | Wave 0 |
| EVIC-03 | CONFIG SET maxmemory / maxmemory-policy | integration | `cargo test --test integration config -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `src/pubsub/mod.rs` -- PubSubRegistry unit tests (subscribe, publish, pattern match, slow subscriber disconnect)
- [ ] `src/storage/eviction.rs` -- eviction unit tests (LRU sampling, LFU Morris counter, volatile-only filtering)
- [ ] `tests/integration.rs` -- Pub/Sub integration tests (requires two concurrent connections)
- [ ] `tests/integration.rs` -- MULTI/EXEC/WATCH integration tests
- [ ] `tests/integration.rs` -- CONFIG GET/SET integration tests
- [ ] `tests/integration.rs` -- eviction integration tests (set maxmemory, fill, verify eviction)

## Open Questions

1. **ServerConfig mutability pattern**
   - What we know: Config is currently `Arc<ServerConfig>` (immutable). CONFIG SET needs to mutate maxmemory, policy, etc. at runtime.
   - What's unclear: Whether to use `Arc<Mutex<ServerConfig>>` for the whole config or split into `Arc<ServerConfig>` (startup) + `Arc<RwLock<RuntimeConfig>>` (mutable).
   - Recommendation: Split into a separate `RuntimeConfig` with `Arc<RwLock<>>`. This avoids locking on every config read for immutable fields (bind, port, databases). Only eviction and CONFIG commands need the mutable config. The RuntimeConfig contains: maxmemory, maxmemory_policy, maxmemory_samples, save rules, appendonly, appendfsync.

2. **Memory estimation for maxmemory**
   - What we know: Need to estimate total memory used by all databases.
   - Recommendation: Track a running `used_memory: usize` counter on Database. Increment on set, decrement on remove. Estimate per entry: `key.len() + value_estimate + 128` (overhead). String value = len, Hash = sum of field+value lens + 64*fields, List = sum of elem lens + 24*elems, Set = sum of member lens + 24*members, SortedSet = sum of member lens + 80*members. This is a rough estimate -- consistency matters more than accuracy.

3. **Pub/Sub integration testing**
   - What we know: The `redis` crate in dev-dependencies supports pub/sub. Integration tests need two concurrent connections (subscriber + publisher).
   - Recommendation: Use `redis::Client::get_async_pubsub()` for the subscriber connection and a regular connection for PUBLISH. Use tokio::spawn for the subscriber task, with a channel to receive the first message before asserting.

## Sources

### Primary (HIGH confidence)
- [Redis Pub/Sub documentation](https://redis.io/docs/latest/develop/pubsub/) -- message format, allowed commands in subscribed mode, protocol details
- [Redis Transactions documentation](https://redis.io/docs/latest/develop/using-commands/transactions/) -- QUEUED response, error handling, WATCH semantics, EXEC return values
- [Redis Key Eviction documentation](https://redis.io/docs/latest/develop/reference/eviction/) -- all policies, LRU approximation, LFU Morris counter parameters, maxmemory-samples

### Secondary (MEDIUM confidence)
- [Redis LFU Morris counter issue #7943](https://github.com/redis/redis/issues/7943) -- counter increment formula: 1/(1 + factor*counter), initial value 5
- Project research files: `.planning/research/PITFALLS.md` (Pitfall 7: Pub/Sub backpressure), `.planning/research/ARCHITECTURE.md` (eviction subsystem), `.planning/research/FEATURES.md` (feature complexity)

### Tertiary (LOW confidence)
- LRM eviction policy (`allkeys-lrm`, `volatile-lrm`) appears in Redis 8.6+ docs but is NOT in scope for this project (not in CONTEXT.md decisions)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new dependencies, all patterns established in existing codebase
- Architecture: HIGH -- Pub/Sub registry pattern, transaction state machine, and eviction sampling are well-documented Redis patterns with clear Rust idioms
- Pitfalls: HIGH -- Pub/Sub backpressure and WATCH atomicity are documented in project research; ServerConfig mutability is identified from code inspection

**Research date:** 2026-03-23
**Valid until:** 2026-04-23 (stable domain, no fast-moving dependencies)
