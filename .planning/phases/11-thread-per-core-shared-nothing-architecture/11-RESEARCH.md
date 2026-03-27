# Phase 11: Thread-per-Core Shared-Nothing Architecture - Research

**Researched:** 2026-03-24
**Domain:** Thread-per-core async runtime, shared-nothing sharding, cross-shard coordination
**Confidence:** HIGH

## Summary

This phase transforms the entire server architecture from a Tokio multi-threaded runtime with `Arc<Vec<RwLock<Database>>>` shared across tasks to a shared-nothing design where each CPU core runs an independent shard with its own event loop, DashTable, expiry index, and memory pool. This is the single highest-impact architectural change in the project.

The current codebase is well-structured for this migration: all command handlers operate on `&mut Database` (773-line `db.rs`), the dispatch system (`command/mod.rs`) cleanly separates read/write paths, and the connection handler (`connection.rs`, 841 lines) already uses pipeline batching with lock-then-release patterns. The phased migration strategy (extract Shard struct, run on Tokio current_thread, optionally swap to Glommio) is sound and minimizes risk.

The critical challenge is that Glommio is Linux-only (requires io_uring, kernel 5.8+), while development happens on macOS. The CONTEXT.md decision to use conditional compilation with Tokio `current_thread` fallback on macOS is correct. The migration must preserve all 76 existing v1 command behaviors while introducing keyspace partitioning, cross-shard SPSC dispatch, and VLL multi-key coordination.

**Primary recommendation:** Execute the three-phase migration as specified in CONTEXT.md. Start by extracting a `Shard` struct that owns `Database` + expiry + connections. Run each shard on a dedicated OS thread with Tokio `current_thread` runtime. Only then consider Glommio swap (Phase 12 territory).

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Use Glommio as primary runtime -- Seastar-heritage thread-per-core design with three io_uring rings per thread, cooperative scheduling with latency classes, NUMA-aware allocation
- Fallback: Monoio if Glommio's API surface proves too restrictive (ByteDance production-proven, GAT-based I/O traits)
- macOS development: Glommio has limited macOS support -- may need conditional compilation with Tokio fallback for dev
- One shard per CPU core, each containing: DashTable (from Phase 10), expiry index, connection list, memory pool
- Shard struct is fully owned by its thread -- no Arc, no Mutex, no shared references
- Each shard runs a single-threaded async event loop -- all operations on shard data are sequential within the shard
- `xxhash64(key) % num_shards` for key-to-shard mapping
- Listener on core 0 accepts connections, assigns to shard via `hash(client_fd) % num_shards` or round-robin
- SPSC (single-producer single-consumer) lock-free channels between each shard pair
- VLL (Very Lightweight Locking) pattern for multi-key commands with ascending shard-ID ordering
- Phased migration: (1) Extract Shard struct, (2) Run on Tokio current_thread per OS thread, (3) Swap to Glommio

### Claude's Discretion
- Exact connection-to-shard assignment strategy (fd hash vs round-robin vs least-connections)
- SPSC channel buffer sizes and backpressure strategy
- Whether to implement a connection migration protocol (move connection to key's shard) or always use cross-shard dispatch
- Cooperative yield points within command execution (how often to yield to the event loop)
- Number of shards: auto-detect CPU count or configurable via CLI flag

### Deferred Ideas (OUT OF SCOPE)
- io_uring integration -- Phase 12 (can start with epoll/kqueue within the new runtime)
- NUMA-aware memory placement -- Phase 13
- Per-shard memory arenas -- Phase 13
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1.x (current_thread) | Per-shard single-threaded async runtime | Already in project; current_thread mode gives thread-per-core semantics without io_uring dependency |
| xxhash-rust | 0.8 | Key-to-shard hash partitioning | Already in Cargo.toml; xxhash64 is fast, well-distributed |
| ringbuf | 0.4.8 | SPSC lock-free channels between shard pairs | Lock-free, zero-allocation in steady state, cache-friendly ring buffer |
| num_cpus | 1.16 | Auto-detect CPU core count for shard count | De facto standard for CPU detection in Rust |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| core_affinity | 0.8 | Pin shard threads to specific CPU cores | Production deployments on Linux for cache-line locality |
| glommio | 0.9.0 | Thread-per-core io_uring runtime (Phase 12 swap) | Linux-only; deferred to Phase 12 but architecture designed to accommodate |
| monoio | 0.2.4 | Alternative thread-per-core runtime | Fallback if Glommio proves too restrictive |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| ringbuf (SPSC) | rtrb 0.3.3 | rtrb has realtime guarantees but ringbuf has more features and active maintenance |
| ringbuf (SPSC) | crossbeam-channel | crossbeam is MPMC (heavier), we only need SPSC per shard pair |
| ringbuf (SPSC) | Glommio SharedChannel | Only works within Glommio runtime, not Tokio; deferred |
| num_cpus | std::thread::available_parallelism() | std version available since Rust 1.59, no extra dependency needed |

**Installation:**
```bash
cargo add ringbuf@0.4 num_cpus@1.16 core_affinity@0.8
```

**Version verification:** ringbuf 0.4.8 (2025-03-24), num_cpus 1.16 (stable), core_affinity 0.8 (stable). All confirmed via `cargo search`.

**Decision: Use `std::thread::available_parallelism()` instead of num_cpus.** The standard library function is sufficient and avoids an extra dependency. Only add num_cpus if the std function proves inadequate (e.g., container-aware detection).

## Architecture Patterns

### Recommended Project Structure
```
src/
├── shard/
│   ├── mod.rs           # Shard struct definition, shard event loop
│   ├── dispatch.rs      # Cross-shard dispatch (SPSC send/recv)
│   ├── coordinator.rs   # VLL multi-key coordination
│   └── mesh.rs          # SPSC channel mesh setup (N*(N-1) channels)
├── server/
│   ├── listener.rs      # Accept loop on core 0, connection assignment
│   ├── connection.rs    # Connection fiber (runs within shard)
│   ├── expiration.rs    # Per-shard cooperative expiry task
│   └── ...
├── storage/
│   ├── db.rs            # Database (unchanged, now per-shard owned)
│   └── ...
├── command/             # All command handlers (unchanged)
├── config.rs            # Add --shards CLI flag
└── main.rs              # Bootstrap N shard threads
```

### Pattern 1: Shard Struct (Fully Owned, No Arc/Mutex)
**What:** A `Shard` struct that owns all per-shard state without any shared references.
**When to use:** Every shard thread creates exactly one Shard.
**Example:**
```rust
// src/shard/mod.rs
pub struct Shard {
    pub id: usize,
    pub databases: Vec<Database>,  // 16 databases per shard (SELECT 0-15)
    pub connections: Vec<ConnectionState>,
    // Per-shard receiver ends for SPSC mesh
    pub dispatch_rx: Vec<RingBufConsumer<ShardMessage>>,
    // Per-shard sender ends (one per other shard)
    pub dispatch_tx: Vec<RingBufProducer<ShardMessage>>,
    // Config (cloned, not shared)
    pub config: ShardConfig,
}

pub enum ShardMessage {
    /// Execute a command on this shard and send result back
    Execute {
        key: Bytes,
        command: Frame,
        reply_tx: oneshot::Sender<Frame>,
    },
    /// New connection assigned to this shard
    NewConnection(TcpStream),
}
```

### Pattern 2: Accept Loop with Connection Assignment
**What:** Single listener on core 0 distributes new connections to shard threads.
**When to use:** Server startup, all incoming TCP connections.
**Example:**
```rust
// Core 0 accept loop
loop {
    let (stream, addr) = listener.accept().await?;
    let fd = stream.as_raw_fd();
    let shard_id = (fd as usize) % num_shards;
    // Send TcpStream to target shard via SPSC channel
    shard_channels[shard_id].try_push(ShardMessage::NewConnection(stream));
}
```

**Recommendation for connection assignment:** Use `fd % num_shards` (round-robin by fd). This is what Dragonfly uses and provides good distribution without overhead. Avoid hash-based assignment (unnecessary computation) and least-connections (requires cross-shard state).

### Pattern 3: Cross-Shard Dispatch for Single-Key Commands
**What:** When a connection's shard differs from the key's shard, dispatch via SPSC.
**When to use:** Any single-key command where `xxhash64(key) % num_shards != connection_shard_id`.
**Example:**
```rust
// In connection handler (runs on shard S)
let target_shard = xxhash64(key.as_ref()) % num_shards;
if target_shard == my_shard_id {
    // Fast path: execute locally, zero overhead
    let result = dispatch(&mut shard.databases[selected_db], cmd, args, ...);
    return result;
} else {
    // Slow path: dispatch to target shard
    let (reply_tx, reply_rx) = oneshot::channel();
    shard.dispatch_tx[target_shard].try_push(ShardMessage::Execute {
        key, command, reply_tx,
    });
    let result = reply_rx.await?;
    return result;
}
```

### Pattern 4: VLL Multi-Key Coordination
**What:** Multi-key commands (MGET, MSET, DEL multi, transactions) coordinate across shards.
**When to use:** Commands that touch keys on 2+ shards.
**Example:**
```rust
// VLL pattern: ascending shard-ID ordering prevents deadlocks
fn execute_multi_key(keys: &[Bytes], shard: &mut Shard) -> Frame {
    // 1. Group keys by target shard
    let mut shard_groups: BTreeMap<usize, Vec<&Bytes>> = BTreeMap::new();
    for key in keys {
        let target = xxhash64(key.as_ref()) % num_shards;
        shard_groups.entry(target).or_default().push(key);
    }

    // 2. If all keys on local shard: execute directly (common case)
    if shard_groups.len() == 1 && shard_groups.contains_key(&shard.id) {
        return execute_locally(keys, shard);
    }

    // 3. Dispatch to shards in ascending ID order (VLL deadlock prevention)
    let mut results = Vec::new();
    for (shard_id, shard_keys) in &shard_groups {
        if *shard_id == shard.id {
            results.push(execute_locally(shard_keys, shard));
        } else {
            let (tx, rx) = oneshot::channel();
            shard.dispatch_tx[*shard_id].try_push(ShardMessage::MultiExecute {
                keys: shard_keys.clone(),
                reply_tx: tx,
            });
            results.push(rx.await?);
        }
    }

    // 4. Assemble final response
    assemble_multi_response(keys, results)
}
```

### Pattern 5: Per-Shard Cooperative Expiry
**What:** Each shard runs its own expiry cycle without global coordination.
**When to use:** Background expiry task, runs as a cooperative task within each shard's event loop.
**Example:**
```rust
// Within shard event loop, interleaved with connection handling
async fn shard_event_loop(shard: &mut Shard) {
    let mut expiry_interval = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            // Handle new connections
            msg = shard.dispatch_rx.recv() => { ... }
            // Handle client commands on existing connections
            // ... (connection fibers run as tasks)
            // Cooperative expiry
            _ = expiry_interval.tick() => {
                for db in &mut shard.databases {
                    expire_cycle(db);
                }
            }
        }
    }
}
```

### Anti-Patterns to Avoid
- **Shared mutable state between shards:** NEVER use `Arc<Mutex<T>>` or `Arc<RwLock<T>>` for data that lives on a specific shard. The entire point is zero shared mutable state.
- **Direct cross-thread function calls:** Always go through SPSC channels. No `&mut Database` references may cross thread boundaries.
- **Global PubSubRegistry with Mutex:** The current `Arc<Mutex<PubSubRegistry>>` must be replaced with per-shard registries + cross-shard fan-out via SPSC channels.
- **Blocking on SPSC in the event loop:** Use `try_push`/`try_pop` and handle backpressure gracefully. Never block the shard's event loop.
- **Unordered multi-shard dispatch:** Always acquire shard "locks" (dispatch slots) in ascending shard-ID order for multi-key commands. Out-of-order dispatch causes deadlocks.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| SPSC ring buffer | Custom lock-free queue | `ringbuf` 0.4.8 | Lock-free SPSC is subtle (memory ordering, cache-line padding); ringbuf is battle-tested |
| CPU count detection | Manual `/proc/cpuinfo` parsing | `std::thread::available_parallelism()` | Handles containers, cgroups, and edge cases |
| CPU affinity | Raw `sched_setaffinity` syscall | `core_affinity` crate | Abstracts Linux/BSD differences |
| Oneshot channels | Custom single-use waker | `tokio::sync::oneshot` | Already available; correct waker integration with Tokio runtime |
| Key hashing | Custom hash function | `xxhash_rust::xxh64::xxh64()` | Already in project, fast and well-distributed |

**Key insight:** The shared-nothing architecture itself is the complex part. Channel infrastructure, CPU detection, and thread pinning are solved problems -- use libraries so implementation effort focuses on the Shard struct, dispatch logic, VLL coordinator, and PubSub fan-out.

## Common Pitfalls

### Pitfall 1: PubSub Cross-Shard Fan-Out
**What goes wrong:** The current `PubSubRegistry` is a single `Arc<Mutex<PubSubRegistry>>` shared across all connections. In a shared-nothing architecture, there is no global registry.
**Why it happens:** PUBLISH on shard 0 must reach subscribers on shards 1, 2, ..., N.
**How to avoid:** Each shard maintains its own subscriber list. PUBLISH fans out via SPSC channels to all other shards. Each shard checks its local subscribers and delivers. This means PUBLISH is O(num_shards) but pub/sub is inherently a fan-out operation.
**Warning signs:** Subscriber receives messages only from publishers on the same shard.

### Pitfall 2: SCAN/KEYS Across Shards
**What goes wrong:** `SCAN` and `KEYS` must iterate ALL keys across ALL shards. A naive implementation either misses keys or double-counts.
**Why it happens:** Keyspace is partitioned. SCAN cursor must encode shard information.
**How to avoid:** SCAN cursor includes shard ID in upper bits. When one shard's cursor exhausts, move to next shard. KEYS aggregates results from all shards. Both require cross-shard dispatch.
**Warning signs:** SCAN returns incomplete results; KEYS misses keys from non-local shards.

### Pitfall 3: SELECT (Database Switching) with Sharding
**What goes wrong:** Redis supports 16 databases (SELECT 0-15). Each shard needs 16 databases. Cross-shard dispatch must carry the selected_db index.
**Why it happens:** SELECT is connection-level state, but dispatch messages need to target the right sub-database on the remote shard.
**How to avoid:** Include `selected_db: usize` in every `ShardMessage`. The connection tracks its selected_db locally and sends it with every cross-shard dispatch.
**Warning signs:** Data written via cross-shard dispatch ends up in wrong database.

### Pitfall 4: Tokio `current_thread` Pitfalls
**What goes wrong:** Tokio `current_thread` runtime does not have a work-stealing scheduler. If a task blocks, the entire shard freezes.
**Why it happens:** Any synchronous I/O or heavy computation without yielding blocks the event loop.
**How to avoid:** Ensure all I/O is async. For heavy operations (KEYS on large datasets), insert cooperative yield points with `tokio::task::yield_now().await`. Keep command handlers non-blocking.
**Warning signs:** Single shard stops responding while others work fine; latency spikes on one core.

### Pitfall 5: BGSAVE with Per-Shard Data
**What goes wrong:** RDB snapshot requires consistent point-in-time view across all shards.
**Why it happens:** Each shard operates independently. Taking snapshots at different times gives inconsistent state.
**How to avoid:** For Phase 11, use a coordinated snapshot approach: send a "snapshot" message to all shards (in order), each shard clones its data. Alternatively, accept per-shard inconsistency for now (Dragonfly does this). Forkless persistence is Phase 14.
**Warning signs:** RDB contains data from different points in time across shards.

### Pitfall 6: Transaction (MULTI/EXEC) Spanning Shards
**What goes wrong:** MULTI/EXEC must execute atomically. If keys in the transaction span shards, atomicity is lost.
**Why it happens:** Each shard executes independently.
**How to avoid:** For Phase 11, require all keys in a transaction to be on the same shard (return error otherwise). Hash tags `{tag}` let users co-locate related keys. This matches Redis Cluster behavior. Cross-shard transactions via VLL are a stretch goal.
**Warning signs:** Transaction partially completes on one shard but fails on another.

### Pitfall 7: macOS Development with Linux-Only Features
**What goes wrong:** Glommio requires Linux io_uring. Development on macOS fails to compile.
**Why it happens:** Glommio has no macOS backend.
**How to avoid:** Use `#[cfg(target_os)]` conditional compilation. On macOS, use Tokio `current_thread` (which works identically for the shared-nothing architecture -- just without io_uring acceleration). The Shard abstraction must not leak runtime details.
**Warning signs:** CI/CD breaks on macOS; developers cannot run tests locally.

### Pitfall 8: SPSC Channel Backpressure
**What goes wrong:** If a shard falls behind, SPSC channels fill up. `try_push` fails, causing dropped commands.
**Why it happens:** Uneven key distribution or slow commands on one shard.
**How to avoid:** Size SPSC buffers generously (4096-8192 entries). On `try_push` failure, either: (a) spin-retry with yield, or (b) return a busy error to the client (like Redis BUSY). Never block the sending shard.
**Warning signs:** Intermittent command failures under load; dropped operations without errors.

## Code Examples

### Bootstrap N Shard Threads
```rust
// src/main.rs (new structure)
fn main() -> anyhow::Result<()> {
    let config = ServerConfig::parse();
    let num_shards = config.shards.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    });

    // Create SPSC channel mesh: N*(N-1) unidirectional channels
    let mesh = create_channel_mesh(num_shards, CHANNEL_BUFFER_SIZE);

    // Create new-connection channels: listener -> each shard
    let (conn_txs, conn_rxs) = create_connection_channels(num_shards);

    let mut shard_handles = Vec::new();
    for shard_id in 0..num_shards {
        let shard_config = config.clone();
        let dispatch_tx = mesh.senders_for(shard_id);
        let dispatch_rx = mesh.receivers_for(shard_id);
        let conn_rx = conn_rxs.remove(0);

        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let mut shard = Shard::new(shard_id, shard_config, dispatch_tx, dispatch_rx);
                shard.run(conn_rx).await;
            });
        });
        shard_handles.push(handle);
    }

    // Shard 0 also runs the accept loop (or dedicate a thread)
    // ...

    for handle in shard_handles {
        handle.join().unwrap();
    }
    Ok(())
}
```

### Channel Mesh Creation
```rust
// src/shard/mesh.rs
use ringbuf::{HeapRb, traits::*};

const CHANNEL_BUFFER_SIZE: usize = 4096;

pub struct ChannelMesh {
    // channels[sender_shard][receiver_shard] = (Producer, Consumer)
    producers: Vec<Vec<HeapProd<ShardMessage>>>,
    consumers: Vec<Vec<HeapCons<ShardMessage>>>,
}

pub fn create_channel_mesh(n: usize) -> ChannelMesh {
    let mut producers = vec![Vec::new(); n];
    let mut consumers = vec![Vec::new(); n];

    for sender in 0..n {
        for receiver in 0..n {
            if sender == receiver {
                continue; // No self-channel needed
            }
            let rb = HeapRb::new(CHANNEL_BUFFER_SIZE);
            let (prod, cons) = rb.split();
            producers[sender].push(prod);
            consumers[receiver].push(cons);
        }
    }

    ChannelMesh { producers, consumers }
}
```

### Keyspace Partitioning
```rust
// src/shard/dispatch.rs
use xxhash_rust::xxh64::xxh64;

const HASH_SEED: u64 = 0;

/// Determine which shard owns a key.
#[inline]
pub fn key_to_shard(key: &[u8], num_shards: usize) -> usize {
    // Extract hash tag if present: content between first { and first }
    let hash_input = extract_hash_tag(key).unwrap_or(key);
    (xxh64(hash_input, HASH_SEED) % num_shards as u64) as usize
}

/// Extract hash tag: content between first '{' and first '}' after it.
fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|&b| b == b'{')?;
    let close = key[open+1..].iter().position(|&b| b == b'}')?;
    if close == 0 { return None; } // Empty tag {} is ignored
    Some(&key[open+1..open+1+close])
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Arc<Mutex<Database>> shared across tasks | Per-shard owned Database, no shared state | Dragonfly (2022), this phase | Eliminates lock contention entirely |
| Single Tokio multi-threaded runtime | N Tokio current_thread runtimes, one per OS thread | TechEmpower 2023+ patterns | 1.5-2x throughput improvement even without io_uring |
| Global PubSub registry with Mutex | Per-shard registry + SPSC fan-out | Dragonfly pattern | Eliminates pub/sub contention bottleneck |
| fork() for RDB snapshots | Per-shard cooperative snapshots (Phase 14) | Dragonfly 2022 | Zero memory spike, no fork latency |

**Deprecated/outdated:**
- `Arc<Vec<parking_lot::RwLock<Database>>>` -- the current 16-database model. Replaced by N shards, each with 16 databases.
- Global `AtomicU64` change counter -- becomes per-shard.
- Global `Arc<Mutex<PubSubRegistry>>` -- becomes per-shard with fan-out.

## Open Questions

1. **Connection migration vs cross-shard dispatch**
   - What we know: Dragonfly always dispatches to the key's shard. The glommio-sharded-affinity-server starter shows fd-passing for connection migration.
   - What's unclear: Whether migrating the connection (fd passing) to the key's shard provides meaningfully better performance than cross-shard dispatch via SPSC.
   - Recommendation: Use cross-shard dispatch (simpler, proven pattern). Connection migration adds complexity with marginal benefit for a KV workload where commands are fast.

2. **SPSC buffer sizing**
   - What we know: 4096 entries is a common default. Under extreme load, this could fill up.
   - What's unclear: Optimal size for the redis workload profile.
   - Recommendation: Start with 4096. Make configurable. Monitor via metrics (queue depth, push failures).

3. **Per-shard AOF vs global AOF**
   - What we know: Current AOF is a single file. Per-shard AOF eliminates coordination.
   - What's unclear: Whether per-shard AOF files complicate restore semantics.
   - Recommendation: For Phase 11, keep a single AOF writer task (on shard 0) that receives AOF entries from all shards via channels. Per-shard AOF is Phase 14 territory.

4. **WATCH/EXEC across shards**
   - What we know: WATCH keys may be on different shards. Version checking must coordinate.
   - What's unclear: How to atomically check versions across shards.
   - Recommendation: For Phase 11, restrict WATCH keys to same shard (error if cross-shard). Hash tags enable user-controlled co-location.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test + integration tests via redis crate |
| Config file | Cargo.toml [dev-dependencies] |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test` |

### Phase Requirements to Test Map
| Behavior | Test Type | Automated Command | File Exists? |
|----------|-----------|-------------------|-------------|
| Each CPU core runs one shard with independent event loop | integration | `cargo test --test integration shard_per_core` | No -- Wave 0 |
| Keyspace partitioned via xxhash64(key) % num_shards | unit | `cargo test shard::dispatch::tests` | No -- Wave 0 |
| Cross-shard SPSC dispatch works for single-key commands | integration | `cargo test --test integration cross_shard_dispatch` | No -- Wave 0 |
| Multi-key commands (MGET/MSET) coordinate via VLL | integration | `cargo test --test integration multi_key_vll` | No -- Wave 0 |
| Single-key fast path has zero cross-shard overhead | unit | `cargo test shard::tests::local_fast_path` | No -- Wave 0 |
| All existing commands pass with shared-nothing backend | integration | `cargo test --test integration` | Yes (tests/integration.rs exists) |
| PubSub works across shards | integration | `cargo test --test integration pubsub_cross_shard` | No -- Wave 0 |
| Hash tags co-locate keys | unit | `cargo test shard::dispatch::tests::hash_tag` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before verify

### Wave 0 Gaps
- [ ] `src/shard/dispatch.rs` -- key_to_shard() unit tests with hash tag extraction
- [ ] `src/shard/mesh.rs` -- channel mesh creation and message passing tests
- [ ] `tests/integration.rs` -- extend with multi-shard scenarios (MGET across shards, PubSub cross-shard)
- [ ] Existing integration tests must pass unchanged (regression gate)

## Sources

### Primary (HIGH confidence)
- Project source code: `src/server/listener.rs`, `src/server/connection.rs`, `src/storage/db.rs`, `src/command/mod.rs` -- direct analysis of refactoring surface
- [Glommio docs](https://docs.rs/glommio/latest/glommio/) -- SharedChannel API, channel types, Linux-only requirement
- [Glommio GitHub](https://github.com/DataDog/glommio) -- version 0.9.0, Seastar-heritage design
- [Monoio GitHub](https://github.com/bytedance/monoio) -- version 0.2.4, ByteDance production-proven
- [ringbuf crate](https://crates.io/crates/ringbuf) -- version 0.4.8, lock-free SPSC ring buffer
- [Glommio SharedChannel docs](https://docs.rs/glommio/latest/glommio/channels/shared_channel/index.html) -- cross-executor channel API

### Secondary (MEDIUM confidence)
- [Dragonfly DeepWiki](https://deepwiki.com/dragonflydb/dragonfly) -- EngineShard architecture, VLL pattern, cross-shard dispatch
- [Glommio sharded server starter](https://github.com/utilitydelta-io/glommio-sharded-affinity-server-starter) -- practical sharding pattern with fd passing
- [VLL VLDB 2012 paper](http://www.vldb.org/pvldb/vol6/p145-ren.pdf) -- ascending shard-ID ordering for deadlock prevention
- `.planning/architect-blue-print.md` -- project architecture blueprint with Dragonfly reference

### Tertiary (LOW confidence)
- SPSC performance comparisons from web search -- p50 latency 188us for ringbuf vs 255us for crossbeam (single source, unverified benchmark)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- libraries verified via cargo search, versions confirmed
- Architecture: HIGH -- patterns directly derived from Dragonfly (proven in production) and project blueprint
- Pitfalls: HIGH -- derived from direct analysis of current codebase (PubSub, SCAN, BGSAVE, MULTI/EXEC all have concrete cross-shard challenges identified)
- Migration strategy: HIGH -- phased approach avoids big-bang risk; Tokio current_thread is well-understood

**Research date:** 2026-03-24
**Valid until:** 2026-04-24 (30 days -- stable domain, libraries mature)
