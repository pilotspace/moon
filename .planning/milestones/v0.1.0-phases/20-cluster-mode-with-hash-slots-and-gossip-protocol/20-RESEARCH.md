# Phase 20: Cluster Mode with Hash Slots and Gossip Protocol - Research

**Researched:** 2026-03-24
**Domain:** Redis Cluster protocol — hash slots, gossip bus, MOVED/ASK, slot migration, automatic failover
**Confidence:** HIGH

## Summary

Phase 20 implements Redis Cluster compatibility on top of the existing thread-per-core shared-nothing architecture from Phase 11. The core architectural insight is that this project already has the foundational pieces: the `key_to_shard` dispatch already extracts hash tags, the shard event loop already handles inter-shard SPSC messaging, and `ReplicationState` from Phase 19 already supports the replica promotion machinery needed for automatic failover.

The implementation adds three distinct layers: (1) a `ClusterState` struct tracking slot ownership, node membership, and cluster epoch — maintained in memory and propagated via gossip; (2) a cluster bus TCP listener on `port+10000` with a binary gossip protocol for PING/PONG node exchange; (3) per-command slot routing in the connection handler that returns `MOVED` or `ASK` errors when a slot is not locally owned.

The critical design decision that is already locked: within a single node, `slot_id % num_local_shards` routes a cluster slot to the appropriate local shard. This means the existing `key_to_shard` function must be augmented (not replaced): in cluster mode, compute `crc16(key) % 16384` for the cluster slot, then `slot % num_shards` for the local shard. The `xxhash64` path used in non-cluster mode remains unchanged.

**Primary recommendation:** Introduce a new `src/cluster/` module with `state.rs`, `bus.rs`, `gossip.rs`, `migration.rs`, and `command.rs`. Gate all cluster behavior behind a `ClusterState` — when `None`, the server behaves exactly as today. This zero-cost toggle ensures no regression on the non-cluster path.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- 16,384 hash slots using `CRC16(key) % 16384`
- Hash tags: `{tag}` — extract content between first `{` and first `}` as hash input for co-location
- Within a node: `slot_id % num_local_shards` maps slot to local shard
- Slot-to-node ownership tracked in cluster state (gossip-propagated)
- Separate TCP port: base_port + 10000 (e.g., 16379 for data port 6379)
- Binary gossip protocol: PING/PONG messages carrying node state, slot assignments, epoch
- Gossip frequency: each node sends PING to random node every ~100ms
- Failure detection: node marked as PFAIL after ping_timeout (default 15s), FAIL after majority agrees
- MOVED: `-MOVED <slot> <host>:<port>` — permanent slot redirect, client updates slot map cache
- ASK: `-ASK <slot> <host>:<port>` — migration redirect, client sends ASKING then retries
- ASKING flag valid for one command only
- CLUSTER SETSLOT <slot> IMPORTING <node-id> — target accepts ASK-redirected commands
- CLUSTER SETSLOT <slot> MIGRATING <node-id> — source redirects misses to target
- Migration coordinator iterates slot's keys, sends MIGRATE to target node
- MIGRATE: serialized key transfer (RDB format) with optional COPY/REPLACE flags
- After migration: CLUSTER SETSLOT <slot> NODE <target-node-id> on all nodes
- Automatic failover: replica with lowest rank (smallest replication offset lag) initiates
- Replica requests votes from other masters (FAILOVER_AUTH_REQUEST)
- Majority votes: replica promotes, takes over master's slots
- Epoch incremented on each failover
- Manual failover via CLUSTER FAILOVER (graceful, coordinated with master)
- Cluster commands: CLUSTER MEET/ADDSLOTS/DELSLOTS/INFO/NODES/SLOTS/MYID/RESET/REPLICATE/FAILOVER
- CLUSTER SETSLOT NODE/MIGRATING/IMPORTING

### Claude's Discretion
- Cluster state persistence (nodes.conf file format)
- Exact gossip message format (binary packing)
- Cluster bus TLS support (deferred or basic)
- CLUSTER KEYSLOT / CLUSTER COUNTKEYSINSLOT / CLUSTER GETKEYSINSLOT utility commands
- Whether to implement cluster proxy mode (route commands internally vs redirect)

### Deferred Ideas (OUT OF SCOPE)
- Cluster proxy mode (route internally instead of redirect) — future optimization
- Redis Cluster bus encryption — future security phase
- Cross-slot transaction support — extremely complex, out of scope
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `crc16` | 0.4.0 | CRC16-CCITT computation for hash slot calculation | Dedicated Redis-compatible CRC16/CCITT; simple, no dependencies |
| `bytes` | 1.10 | Already in Cargo.toml; gossip message serialization | Already in use throughout codebase |
| `tokio` | 1 (current) | Already in Cargo.toml; cluster bus TCP listener | Already used for all networking |
| `rand` | 0.9 | Already in Cargo.toml; node ID generation, gossip target selection | Already in use |
| `crc32fast` | 1 | Already in Cargo.toml; message integrity for gossip frames | Already in use for RDB |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `once_cell` | 1 | Already in Cargo.toml; static CRC16 lookup table (optional optimization) | If CRC16 computation is on hot path |
| `itoa` | 1 | Already in Cargo.toml; MOVED/ASK error integer formatting | Already used for response formatting |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `crc16` crate (0.4.0) | `crc` crate (3.4.0) with CRC-16/CCITT algorithm | `crc` is more general but slightly heavier API; `crc16` is purpose-built and lighter for this use |
| Binary gossip protocol | RESP3 over cluster bus | Binary is more compact; RESP3 adds parsing overhead and doesn't match Redis Cluster wire format |

**Installation (only new dependency needed):**
```bash
# Add to Cargo.toml [dependencies]:
crc16 = "0.4.0"
```

All other dependencies are already present in Cargo.toml.

**Version verification:** `crc16 = "0.4.0"` confirmed via `cargo search crc16` on 2026-03-24.

## Architecture Patterns

### Recommended Project Structure
```
src/
├── cluster/
│   ├── mod.rs           # ClusterState, ClusterNode, slot ownership bitmap
│   ├── slots.rs         # CRC16 hash slot computation, slot_for_key(), hash tag extraction
│   ├── bus.rs           # Cluster bus TCP listener on port+10000, connection handling
│   ├── gossip.rs        # PING/PONG message serialization, gossip ticker, failure detection
│   ├── migration.rs     # Slot migration coordinator, MIGRATE command, IMPORTING/MIGRATING state
│   ├── failover.rs      # Automatic failover, vote collection, replica promotion
│   └── command.rs       # CLUSTER subcommand handlers (MEET, ADDSLOTS, NODES, INFO, etc.)
```

### Pattern 1: ClusterState as Optional — Zero-Cost Non-Cluster Path
**What:** Wrap all cluster state in `Option<Arc<RwLock<ClusterState>>>` passed into shard event loops. When `None`, all cluster routing code is a single branch check that returns immediately — exactly the existing behavior.
**When to use:** Always. This is the primary design gate.
**Example:**
```rust
// In src/cluster/mod.rs
pub struct ClusterState {
    /// This node's 40-char hex node ID.
    pub node_id: String,
    /// Config epoch (increments on failover or topology change).
    pub epoch: u64,
    /// Overall cluster status: Ok, Fail.
    pub status: ClusterStatus,
    /// All known nodes (including self). Keyed by node_id.
    pub nodes: HashMap<String, ClusterNode>,
    /// Slot ownership: slot_id -> node_id. 16384 entries.
    pub slot_map: Box<[Option<Arc<ClusterNode>>; 16384]>,
    /// Slots currently being imported (slot -> source node_id).
    pub importing: HashMap<u16, String>,
    /// Slots currently being migrated (slot -> target node_id).
    pub migrating: HashMap<u16, String>,
}

// In connection handler (sharded path), before command dispatch:
if let Some(ref cs) = cluster_state {
    if let Some(key) = extract_command_key(&cmd, &args) {
        let slot = slot_for_key(&key);
        let cs_read = cs.read();
        match cs_read.route_slot(slot) {
            SlotRoute::Local => {}  // continue to dispatch
            SlotRoute::Moved { host, port } => {
                return Frame::Error(format!("MOVED {} {}:{}", slot, host, port).into());
            }
            SlotRoute::Ask { host, port } => {
                return Frame::Error(format!("ASK {} {}:{}", slot, host, port).into());
            }
            SlotRoute::CrossSlot => {
                return Frame::Error(b"CROSSSLOT Keys in request don't hash to the same slot".as_slice().into());
            }
        }
    }
}
```

### Pattern 2: CRC16 Hash Slot Computation
**What:** Redis Cluster uses CRC16-CCITT (polynomial 0x1021) modulo 16384. Hash tags extract content between first `{` and first `}`.
**When to use:** Every key lookup in cluster mode.
**Example:**
```rust
// In src/cluster/slots.rs
use crc16::{State, ARC}; // CRC16-CCITT variant used by Redis

pub fn slot_for_key(key: &[u8]) -> u16 {
    let hash_input = extract_hash_tag(key).unwrap_or(key);
    // Redis uses XMODEM polynomial (0x1021); crc16 crate's XMODEM matches
    let crc = State::<crc16::XMODEM>::calculate(hash_input);
    crc % 16384
}

// Re-export existing extract_hash_tag from dispatch.rs (already correct)
pub use crate::shard::dispatch::extract_hash_tag;

// Local shard from cluster slot:
pub fn local_shard_for_slot(slot: u16, num_shards: usize) -> usize {
    slot as usize % num_shards
}
```

**CRITICAL:** The `crc16` crate's `XMODEM` variant matches Redis's CRC16. Verify with Redis's known test vectors: `CLUSTER KEYSLOT foo` returns 12356. `CRC16(b"foo") % 16384 = 12356`.

### Pattern 3: Cluster Bus — Separate TCP Listener
**What:** A dedicated async TCP listener on `config.port + 10000` accepting cluster peer connections. Each peer connection runs a gossip protocol exchange loop.
**When to use:** On startup when cluster mode is enabled.
**Example:**
```rust
// In src/cluster/bus.rs
pub async fn run_cluster_bus(
    bind: &str,
    cluster_port: u16,
    cluster_state: Arc<RwLock<ClusterState>>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let addr = format!("{}:{}", bind, cluster_port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Cluster bus listening on {}", addr);
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer_addr) = result?;
                let cs = cluster_state.clone();
                let tok = shutdown.child_token();
                tokio::spawn(handle_cluster_peer(stream, peer_addr, cs, tok));
            }
            _ = shutdown.cancelled() => break,
        }
    }
    Ok(())
}
```

### Pattern 4: Gossip Message Format
**What:** Binary frames for PING/PONG. Each message contains: magic (4 bytes), message type (1 byte), node ID (40 bytes), epoch (8 bytes), slot bitmap (2048 bytes = 16384 bits), node count (2 bytes), then N gossip sections (each: node_id 40 + flags 2 + ip len 1 + ip + port 2 + pport 2).
**When to use:** Gossip tick (every ~100ms) and in response to received PINGs.

The slot bitmap is a 2048-byte array where bit `i` is set if this node owns slot `i`. This matches Redis Cluster's wire format for gossip header.

### Pattern 5: MOVED and ASK Error Injection
**What:** Before every key-touching command dispatch, check slot ownership. Return early with MOVED or ASK error frame.
**When to use:** In `handle_connection_sharded` before calling `dispatch`.

The `ASKING` flag is a per-connection boolean that is consumed on the next command (one-shot). When a client sends `ASKING`, set the flag. On the next command, if the slot is `IMPORTING` on this node, allow execution regardless of ownership. Then clear the flag.

### Pattern 6: Slot Migration State Machine
```
Source node:          CLUSTER SETSLOT <slot> MIGRATING <dst-id>
Target node:          CLUSTER SETSLOT <slot> IMPORTING <src-id>
Migration loop:       CLUSTER GETKEYSINSLOT <slot> <count> -> MIGRATE each key
After all keys done:  CLUSTER SETSLOT <slot> NODE <dst-id>  (broadcast to all)
```

The `MIGRATE` command serializes a key's value in RDB format and sends it to the target node over a direct TCP connection (not the cluster bus). The target node deserializes and inserts the key. Source node deletes the key after successful MIGRATE (unless COPY flag is set).

### Pattern 7: Automatic Failover
**What:** When a master is marked FAIL (majority of masters agree on PFAIL):
1. Replica with lowest replication lag (highest ack offset) initiates.
2. Sends `FAILOVER_AUTH_REQUEST` to all masters via cluster bus.
3. Masters vote if: epoch > their last-voted epoch AND requesting replica has higher offset than any other replica they know for that master.
4. On majority vote: replica broadcasts `FAILOVER_AUTH_ACK` as new master, takes over slots, increments epoch.
**Integration point:** `ReplicationState` from Phase 19 has `shard_offsets` and `replicas` with `ack_offsets` — use these directly to determine replica rank.

### Anti-Patterns to Avoid
- **Hand-rolling CRC16:** Redis uses CRC16-CCITT/XMODEM (0x1021 polynomial). Easy to get the wrong variant. Use the `crc16` crate with `XMODEM` algorithm.
- **Global lock on ClusterState for every command:** Commands are high-frequency; slot routing must use a read-only snapshot or lock-free slot array. Use `Arc<RwLock<>>` but keep lock scopes tiny (just read the slot map, not the full state).
- **Running cluster bus on a shard thread:** The cluster bus runs on a separate task on the listener runtime, not on shard threads. Gossip I/O is low-frequency and does not belong in the hot shard event loop.
- **Blocking shard threads during migration:** Migration should be async, driven by a coordinator fiber that uses SPSC to query keys from a shard without blocking it.
- **Multi-key commands without CROSSSLOT check:** MGET, MSET, SUNIONSTORE, etc. must verify all keys hash to the same slot. Return `-CROSSSLOT` if not.
- **Changing key_to_shard for non-cluster mode:** In non-cluster mode, keep `xxhash64 % num_shards`. Only in cluster mode use `CRC16(key) % 16384` then `% num_shards`.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| CRC16 computation | custom CRC16 loop | `crc16` crate with `XMODEM` variant | Easy to choose wrong polynomial; off-by-one on bit reflection |
| Node ID generation | custom 40-char hex gen | Extend `generate_repl_id()` from Phase 19 | Already exists in `state.rs`, same 40-char hex format Redis uses |
| RDB key serialization for MIGRATE | new serializer | Extend existing `rdb.rs` serialize path | Already handles all value types; MIGRATE uses same format |
| TCP connection pool for MIGRATE | new pool | Plain `tokio::net::TcpStream` per migration batch | MIGRATE is infrequent; pooling adds complexity without benefit at this stage |
| nodes.conf format | invented format | Simple line-based format: `<node-id> <ip>:<port>@<bus-port> <flags> <master-id> <ping-sent> <pong-recv> <epoch> <link-state> <slot-ranges>` | Redis clients and tools parse this exact format |

**Key insight:** The hardest parts of cluster implementation are already solved in the codebase: hash tag extraction (`extract_hash_tag` in dispatch.rs), replication state (`ReplicationState`), per-shard WAL streaming, and snapshot infrastructure for MIGRATE's RDB serialization.

## Common Pitfalls

### Pitfall 1: Wrong CRC16 Variant
**What goes wrong:** `crc16` crate has multiple algorithms (ARC, BUYPASS, XMODEM, etc.). Using the wrong one silently produces incorrect slot assignments.
**Why it happens:** "CRC16" is ambiguous — Redis uses CRC-16-CCITT with poly 0x1021, initial value 0x0000, no reflection (also called XMODEM).
**How to avoid:** Use `State::<crc16::XMODEM>::calculate(data)`. Verify with known test vector: `slot_for_key(b"foo") == 12356`.
**Warning signs:** `CLUSTER KEYSLOT foo` returns different value than Redis.

### Pitfall 2: Cluster Mode Active Check on Every Command
**What goes wrong:** Calling `cluster_state.read()` or even checking `Option::is_some()` on a hot read path (millions of ops/sec) with a poorly-scoped lock creates contention.
**Why it happens:** `Arc<RwLock<ClusterState>>` write locks during gossip updates starve readers briefly.
**How to avoid:** Use an `AtomicBool cluster_enabled` flag for the outer check. Only acquire `RwLock<ClusterState>` read lock when `cluster_enabled` is true. Keep slot map lookup in a pre-read snapshot: read the slot owner for one slot, release read lock, proceed.

### Pitfall 3: ASKING Flag Scope
**What goes wrong:** `ASKING` is valid for exactly ONE command. If the flag is not cleared after that command executes (even if it errors), subsequent commands incorrectly bypass slot routing.
**Why it happens:** Error paths skip the `asking = false` reset.
**How to avoid:** Clear `asking` flag unconditionally at the start of dispatch for any non-ASKING command, before checking slot ownership.

### Pitfall 4: Epoch Monotonicity in Gossip
**What goes wrong:** If two nodes have different epochs for the same slot assignment, the stale node may MOVED-redirect clients to the wrong target.
**Why it happens:** Gossip propagation is eventually consistent; a restarted node may start with epoch 0.
**How to avoid:** Persist epoch to `nodes.conf`. On startup, load epoch from file. During gossip, always adopt the higher epoch when receiving node state.

### Pitfall 5: MIGRATE and Partial Failures
**What goes wrong:** MIGRATE fails halfway through (network error, target full). Source has deleted some keys; target has partial keys.
**Why it happens:** Not using the RDB load's all-or-nothing semantics.
**How to avoid:** Target node applies keys atomically only after receiving the full MIGRATE payload. Use the COPY flag during migration and only delete from source after confirming target acknowledged. The `REPLACE` flag on target handles key conflicts.

### Pitfall 6: Multi-Key Commands During Migration
**What goes wrong:** A client sends `MGET key1 key2` where key1 is still on the source and key2 has moved to the target. Both slots are the same (correct) but one key has migrated and one hasn't.
**Why it happens:** The slot is in MIGRATING state; individual keys may or may not have transferred yet.
**How to avoid:** In MIGRATING state, for each key lookup: if key not found locally AND slot is MIGRATING, return ASK (not MOVED). This tells the client to try the target node.

### Pitfall 7: Cluster Bus Port Collision
**What goes wrong:** `port + 10000` wraps around for ports near 65535 or conflicts with another service.
**Why it happens:** u16 arithmetic overflow: `6379 + 10000 = 16379` is fine, but edge cases exist.
**How to avoid:** Use `u32` arithmetic for port calculation, check result fits in `u16`. Log cluster bus port at startup.

### Pitfall 8: Failover Epoch Split Brain
**What goes wrong:** Two replicas both think they got majority votes and both promote simultaneously, leading to two nodes claiming the same slots.
**Why it happens:** Network partitions or clock skew cause split vote counting.
**How to avoid:** Each master votes only once per epoch. The epoch guard (`last_vote_epoch < request_epoch`) is mandatory. On promotion, increment epoch first, then broadcast new slot ownership.

## Code Examples

Verified patterns from existing codebase and Redis Cluster specification:

### CRC16 Slot Computation
```rust
// src/cluster/slots.rs
use crc16::{State, XMODEM};
use crate::shard::dispatch::extract_hash_tag;

pub fn slot_for_key(key: &[u8]) -> u16 {
    let hash_input = extract_hash_tag(key).unwrap_or(key);
    State::<XMODEM>::calculate(hash_input) % 16384
}

// Test vector: Redis guarantees slot_for_key(b"foo") == 12356
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_foo_slot() {
        assert_eq!(slot_for_key(b"foo"), 12356);
    }
    #[test]
    fn test_hash_tag_co_location() {
        assert_eq!(slot_for_key(b"{user}.name"), slot_for_key(b"{user}.email"));
    }
}
```

### ClusterNode and Slot Bitmap
```rust
// src/cluster/mod.rs
use std::net::SocketAddr;

#[derive(Clone, Debug, PartialEq)]
pub enum NodeFlags {
    Master,
    Replica { master_id: String },
    Pfail,
    Fail,
}

#[derive(Clone, Debug)]
pub struct ClusterNode {
    pub node_id: String,           // 40-char hex
    pub addr: SocketAddr,          // data port address
    pub bus_port: u16,             // cluster bus port (addr.port() + 10000)
    pub flags: NodeFlags,
    pub epoch: u64,
    pub ping_sent_ms: u64,
    pub pong_recv_ms: u64,
    /// Slots owned by this node. Bit i set = owns slot i.
    pub slots: Box<[u8; 2048]>,    // 16384 bits
}

impl ClusterNode {
    pub fn owns_slot(&self, slot: u16) -> bool {
        let byte = slot as usize / 8;
        let bit = slot as usize % 8;
        self.slots[byte] & (1 << bit) != 0
    }
    pub fn set_slot(&mut self, slot: u16) {
        let byte = slot as usize / 8;
        let bit = slot as usize % 8;
        self.slots[byte] |= 1 << bit;
    }
}
```

### MOVED Error Response
```rust
// In connection handler, after slot lookup
fn moved_error(slot: u16, host: &str, port: u16) -> Frame {
    // Redis format: -MOVED <slot> <host>:<port>
    let msg = format!("MOVED {} {}:{}", slot, host, port);
    Frame::Error(bytes::Bytes::from(msg))
}

fn ask_error(slot: u16, host: &str, port: u16) -> Frame {
    let msg = format!("ASK {} {}:{}", slot, host, port);
    Frame::Error(bytes::Bytes::from(msg))
}
```

### SlotRoute Enum
```rust
pub enum SlotRoute {
    /// Key belongs to this node — proceed to dispatch.
    Local,
    /// Slot permanently owned by another node.
    Moved { host: String, port: u16 },
    /// Slot is migrating — try target node.
    Ask { host: String, port: u16 },
    /// Multi-key command with keys spanning different slots.
    CrossSlot,
}

impl ClusterState {
    pub fn route_slot(&self, slot: u16, asking: bool) -> SlotRoute {
        // If slot is IMPORTING and client sent ASKING, allow locally
        if asking && self.importing.contains_key(&slot) {
            return SlotRoute::Local;
        }
        // Check if we own this slot
        if self.my_node().owns_slot(slot) {
            // Check if we're migrating this slot
            if let Some(target_id) = self.migrating.get(&slot) {
                if let Some(node) = self.nodes.get(target_id) {
                    // Key not found locally -> ASK
                    // (caller checks key existence first for miss case)
                    return SlotRoute::Ask {
                        host: node.addr.ip().to_string(),
                        port: node.addr.port(),
                    };
                }
            }
            return SlotRoute::Local;
        }
        // Slot owned by another node
        if let Some(owner) = self.slot_owner(slot) {
            return SlotRoute::Moved {
                host: owner.addr.ip().to_string(),
                port: owner.addr.port(),
            };
        }
        SlotRoute::Local // unconfigured slot — treat as local for now
    }
}
```

### Gossip Ticker
```rust
// src/cluster/gossip.rs
use tokio::time::{interval, Duration};

pub async fn run_gossip_ticker(
    cluster_state: Arc<RwLock<ClusterState>>,
    shutdown: CancellationToken,
) {
    let mut tick = interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let target = {
                    let cs = cluster_state.read().unwrap();
                    cs.pick_random_peer()  // exclude self
                };
                if let Some(peer) = target {
                    let cs = cluster_state.clone();
                    tokio::spawn(send_ping(peer, cs));
                }
                // Also check PFAIL -> FAIL transitions
                check_failure_states(&cluster_state);
            }
            _ = shutdown.cancelled() => break,
        }
    }
}
```

### Gossip Binary Message Format
```
// Cluster bus message wire format (simplified, not full Redis compat):
// [magic: u32 = 0x52656469] [total_len: u32] [ver: u16] [type: u16]
// [sender_node_id: 40 bytes] [my_slots: 2048 bytes] [config_epoch: u64]
// [sender_ip: 16 bytes] [sender_port: u16] [sender_bus_port: u16]
// [num_gossip: u16]
// per gossip entry: [node_id: 40] [ip: 16] [port: u16] [bus_port: u16]
//                   [flags: u16] [epoch: u64] [ping_sent: u64] [pong_recv: u64]
```

### nodes.conf Format
```
# nodes.conf -- one line per node
# <node-id> <ip>:<port>@<cluster-port> <flags> <master-id-or-> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot-range>...
a3b4c5d6...  127.0.0.1:6379@16379  master  -  0  1711234567890  1  connected  0-5460
f1e2d3c4...  127.0.0.1:6380@16380  master  -  0  1711234567890  1  connected  5461-10922
# "myself" flag marks this node
vars currentEpoch 2 lastVoteEpoch 0
```

### CLUSTER INFO Response
```
cluster_enabled:1
cluster_state:ok
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:3
cluster_size:3
cluster_current_epoch:2
cluster_my_epoch:1
cluster_stats_messages_sent:1234
cluster_stats_messages_received:1234
```

### ShardMessage Extensions for Cluster
```rust
// Add to shard/dispatch.rs ShardMessage enum:
/// Query keys in a slot range (for CLUSTER GETKEYSINSLOT / migration enumeration).
GetKeysInSlot {
    db_index: usize,
    slot: u16,
    count: usize,
    reply_tx: tokio::sync::oneshot::Sender<Vec<bytes::Bytes>>,
},
/// Notify shard that slot ownership has changed (add or remove).
SlotOwnershipUpdate {
    add_slots: Vec<u16>,
    remove_slots: Vec<u16>,
},
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Full cluster state lock on hot path | Atomic bool gate + read-lock only for routing | Phase 20 design | ~zero overhead on non-cluster lookups |
| Single Redis replication backlog | Per-shard WAL streaming (Phase 19) | Phase 19 | Enables per-shard partial resync post-failover |
| xxhash64 for shard routing | CRC16 % 16384, then % num_shards in cluster mode | Phase 20 | Redis Cluster wire compat without changing non-cluster path |
| Global repl state | Per-shard ack offsets for replica rank | Phase 19 | Precise failover candidate selection |

**Deprecated/outdated:**
- Standalone `key_to_shard` with xxhash: in cluster mode, this is bypassed. The cluster slots computation provides both the cluster slot (for routing across nodes) and the local shard (via `% num_shards`).

## Open Questions

1. **nodes.conf write location**
   - What we know: `config.dir` is already used for RDB/WAL/snapshots
   - What's unclear: Should nodes.conf live in `config.dir` or a separate cluster-specific dir?
   - Recommendation: Use `config.dir/nodes.conf` — consistent with Redis default

2. **Cluster state in non-sharded (single-thread) mode**
   - What we know: Non-sharded listener path exists (`listener.rs::run()`) for testing/dev
   - What's unclear: Should cluster mode require sharded path?
   - Recommendation: Gate cluster mode as sharded-only; emit error if `--cluster-enabled yes` with `--shards 1` in a way that's non-sharded

3. **MIGRATE serialization for complex types**
   - What we know: RDB serializer exists in `persistence/rdb.rs` for BTreeMap-based sorted sets
   - What's unclear: Phase 15 introduced BPTree sorted sets — does RDB serializer handle them?
   - Recommendation: Audit `rdb.rs` in Wave 1 before implementing MIGRATE; may need to add BPTree serialization path

4. **Cluster bus message authentication**
   - What we know: Deferred (no TLS per decisions)
   - What's unclear: Should we add a shared secret HMAC to gossip messages even without TLS?
   - Recommendation: Add optional `cluster_auth_token` in config; prepend 16-byte HMAC-SHA256 prefix to gossip messages if configured

5. **CROSSSLOT handling for existing multi-key commands**
   - What we know: MGET, MSET, SUNIONSTORE, ZUNIONSTORE, etc. must check all keys
   - What's unclear: How many of these currently use cross-shard dispatch (Phase 11)?
   - Recommendation: Enumerate all multi-key commands in Wave 2 audit before implementing CROSSSLOT checks

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in tests + integration tests via `redis` crate |
| Config file | none (uses `#[cfg(test)]` and `tests/integration.rs`) |
| Quick run command | `cargo test --lib cluster` |
| Full suite command | `cargo test` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CLUSTER-01 | `slot_for_key(b"foo") == 12356` CRC16 test vector | unit | `cargo test --lib cluster::slots` | Wave 0 |
| CLUSTER-02 | Hash tag co-location: `{user}.a` same slot as `{user}.b` | unit | `cargo test --lib cluster::slots` | Wave 0 |
| CLUSTER-03 | `local_shard_for_slot(slot, n) == slot % n` | unit | `cargo test --lib cluster::slots` | Wave 0 |
| CLUSTER-04 | MOVED error format: `-MOVED 12356 127.0.0.1:6380` | unit | `cargo test --lib cluster` | Wave 0 |
| CLUSTER-05 | ASKING flag clears after one command | unit | `cargo test --lib cluster` | Wave 0 |
| CLUSTER-06 | `CLUSTER INFO` returns `cluster_enabled:1` | integration | `cargo test --test integration cluster_info` | Wave 0 |
| CLUSTER-07 | `CLUSTER MYID` returns 40-char hex | integration | `cargo test --test integration cluster_myid` | Wave 0 |
| CLUSTER-08 | `CLUSTER ADDSLOTS 0` and GET on that slot routes locally | integration | `cargo test --test integration cluster_addslots` | Wave 0 |
| CLUSTER-09 | GET on non-owned slot returns MOVED error | integration | `cargo test --test integration cluster_moved` | Wave 0 |
| CLUSTER-10 | Gossip PING/PONG serialization round-trip | unit | `cargo test --lib cluster::gossip` | Wave 0 |
| CLUSTER-11 | nodes.conf persists and reloads cluster state | unit | `cargo test --lib cluster` | Wave 0 |
| CLUSTER-12 | Slot migration SETSLOT MIGRATING -> ASK on miss | integration | `cargo test --test integration cluster_migration` | Wave 0 |
| CLUSTER-13 | CLUSTER NODES output format parseable | integration | `cargo test --test integration cluster_nodes` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib cluster`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `src/cluster/mod.rs` — ClusterState, ClusterNode, SlotRoute definitions
- [ ] `src/cluster/slots.rs` — slot_for_key(), test vectors for CLUSTER-01 through CLUSTER-03
- [ ] `src/cluster/gossip.rs` — PING/PONG serialization with round-trip test
- [ ] `tests/integration.rs` additions — cluster_info, cluster_myid, cluster_addslots, cluster_moved, cluster_nodes, cluster_migration test functions

## Sources

### Primary (HIGH confidence)
- Redis source code (cluster.c, cluster.h) — CRC16 polynomial, gossip format, MOVED/ASK semantics, nodes.conf format
- `crc16` crate docs — XMODEM variant confirmed for CRC-16-CCITT
- Existing codebase `src/shard/dispatch.rs` — `extract_hash_tag` already correct, re-usable
- Existing codebase `src/replication/state.rs` — `ReplicationState`, node ID generation pattern
- Existing codebase `Cargo.toml` — confirmed available dependencies

### Secondary (MEDIUM confidence)
- Redis Cluster specification https://redis.io/docs/latest/operate/ors/reference/cluster-spec/ — gossip message structure, failure detection timings, epoch rules
- Dragonfly Cluster implementation — shard-local slot assignment pattern (`slot % num_shards`)

### Tertiary (LOW confidence)
- Blog posts on Redis Cluster internals — failure detection thresholds (15s PFAIL, majority-FAIL) are well-documented but implementation-specific tuning may vary

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — codebase dependencies already cover 95%; only `crc16 = "0.4.0"` is new, version confirmed via cargo search
- Architecture: HIGH — patterns derived directly from existing codebase structure and Redis Cluster spec
- Pitfalls: HIGH — CRC16 variant, ASKING scope, epoch monotonicity are documented Redis Cluster implementation hazards with known solutions

**Research date:** 2026-03-24
**Valid until:** 2026-06-24 (Redis Cluster spec is stable; crc16 crate is stable)
