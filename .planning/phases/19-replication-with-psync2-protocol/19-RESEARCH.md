# Phase 19: Replication with PSYNC2 Protocol - Research

**Researched:** 2026-03-24
**Domain:** Distributed systems / Redis PSYNC2 replication protocol / Rust async networking
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **Replication protocol:** PSYNC2 compatible — replicas send `PSYNC <repl_id> <offset>` to master; master responds `+FULLRESYNC <repl_id> <offset>` or `+CONTINUE`; replication ID survives restarts (persisted); second replication ID (repl_id2) maintained for failover
- **Full resync:** Master sends RDB snapshot followed by buffered commands since snapshot epoch; per-shard parallel transfer; replica loads per-shard snapshots then applies per-shard command stream; stale reads on replica during full resync are configurable
- **Partial resync:** Per-shard circular replication backlog (default 1MB per shard); on reconnect, replica sends last known offset; if within backlog partial resync; per-shard granularity means only catch up affected shards; if offset too old full resync for that shard
- **WAL streaming:** Each shard's WAL (Phase 14) is the replication stream source; master streams WAL entries to replicas in real-time; WAL format is RESP wire format; backlog is a view into WAL circular buffer
- **Replica behavior:** Read-only by default; write commands rejected with READONLY; replicas execute expiration independently; INFO replication reports role/connected_replicas/offset/lag
- **Commands:** REPLICAOF host port, REPLICAOF NO ONE, REPLCONF (listening-port, capa eof, capa psync2), WAIT numreplicas timeout (uses Phase 17 blocking)

### Claude's Discretion

- Replication backlog size per shard (1MB default, configurable)
- Heartbeat interval between master and replicas (default 10s)
- Whether to support multiple replicas simultaneously (yes, must handle)
- Replica priority for failover election (sentinel compatibility)

### Deferred Ideas (OUT OF SCOPE)

- Sentinel compatibility (automatic failover)
- Active-active replication (multi-master, KeyDB-style)
- Diskless replication (stream RDB directly without writing to disk)
</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| REQ-19-01 | REPLICAOF/SLAVEOF initiates replication handshake | PSYNC2 handshake protocol section; REPLCONF command design |
| REQ-19-02 | Full resync: master sends RDB + backlog stream | SnapshotState reuse + WAL buffer design; per-shard parallel transfer pattern |
| REQ-19-03 | Partial resync: per-shard replication backlog (circular buffer, 1MB/shard) | ReplicationBacklog struct design; offset math section |
| REQ-19-04 | Per-shard replication (catch up on affected shards only) | Per-shard offset tracking design; partial resync architecture |
| REQ-19-05 | Replica serves read-only queries | ReplicaState mode; READONLY rejection in connection handler |
| REQ-19-06 | Replication offset tracked per-shard; PSYNC2 replication ID survives restarts | ReplId persistence; per-shard offset in ReplicationState |
| REQ-19-07 | INFO replication reports correct state | INFO command extension; ReplicationState fields mapped to INFO format |
</phase_requirements>

---

## Summary

Phase 19 implements PSYNC2-compatible master-replica replication leveraging the per-shard WAL streaming architecture from Phase 14. The core insight is that this project's per-shard design enables a significant improvement over Redis's single-backlog approach: replicas only catch up on shards where they fell behind, not the entire dataset. Zero new external dependencies are needed — the RESP wire format already in the WAL doubles as the replication stream, `tokio::net::TcpStream` handles replica connections, and the existing `SnapshotState`/`WalWriter` from Phase 14 provide the full-resync machinery.

The implementation splits into four concerns: (1) a `ReplicationState` struct that tracks master/replica role, replication IDs, and per-shard offsets, persisted alongside WAL data; (2) a per-shard `ReplicationBacklog` circular buffer that captures WAL bytes as they flow; (3) a handshake state machine handling the PSYNC2 negotiation sequence (PING, REPLCONF, PSYNC); and (4) integration points — REPLICAOF/REPLCONF commands at the connection level, WAL streaming in the shard event loop, and an extended INFO replication section.

The critical architectural challenge is fan-out: when the master has multiple replicas, each write command must be broadcast to all replica connections. In the sharded model, each shard's event loop maintains a list of outbound replica sender channels (bounded `mpsc::Sender<Bytes>`), appending RESP bytes after every WAL write. The WAIT command blocks the caller until N replicas acknowledge the target offset, reusing the Phase 17 blocking infrastructure pattern.

**Primary recommendation:** Model replica connections as long-lived Tokio tasks per shard (one task per replica per shard), communicating with the shard event loop via a bounded `mpsc::Sender<Bytes>` channel. The shard appends WAL bytes to each replica's channel; the task drains the channel and writes to the TCP socket. This decouples WAL write latency from network send latency and provides natural backpressure.

---

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1 (already used) | Async TCP for replica connections | Already in Cargo.toml; `TcpStream::into_split()` for independent read/write halves |
| bytes | 1.10 (already used) | Zero-copy backlog buffer slices | Already in Cargo.toml; `Bytes::copy_from_slice` for fan-out |
| ringbuf | 0.4 (already used) | Optional: circular backlog buffer | Already in Cargo.toml; `VecDeque<u8>` is simpler and sufficient for 1MB backlog |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| rand | 0.9 (already used) | Replication ID generation (40-char hex) | `rand::Rng::random::<u8>()` x 20, formatted as hex |
| tokio::sync::mpsc | (tokio internal) | Per-replica backpressure channel | Bounded channel from shard event loop to each replica sender task |

### No New Dependencies Required

The entire phase can be implemented with the existing dependency set. The replication ID needs only `rand` (already present at v0.9). Persistence of replication state uses `std::fs` (same as WAL). TCP connections use `tokio::net::TcpStream` (already used in `src/server/`).

**Version verification:** All packages verified in `/Cargo.toml`. No additions needed.

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `VecDeque<u8>` for backlog | `ringbuf::HeapRb<u8>` | ringbuf is lock-free SPSC; VecDeque is simpler with same O(1) ops for single-producer single-consumer patterns within one shard thread |
| Per-shard fan-out via `mpsc` | Direct socket writes in shard tick | Direct writes block the shard loop on network I/O; mpsc + dedicated task is the established project pattern (see PubSubFanOut) |

---

## Architecture Patterns

### Recommended Module Structure
```
src/replication/
├── mod.rs           # pub mod declarations; ReplicationRole enum; ReplicationAction enum
├── state.rs         # ReplicationState: IDs, offsets, role, replica registry
├── backlog.rs       # ReplicationBacklog: per-shard circular buffer
├── handshake.rs     # PSYNC2 handshake state machine (master side + replica side)
├── master.rs        # Master: replica registry, fan-out, WAIT ack tracking
└── replica.rs       # Replica: outbound connection task, reconnect loop
```

Add to `src/lib.rs`:
```rust
pub mod replication;
```

Add REPLICAOF and REPLCONF handlers to `src/command/connection.rs` (same pattern as AUTH/HELLO: intercepted at connection level for access to replication state Arc).

### Pattern 1: ReplicationState — Global Identity + Per-Shard Offsets

**What:** A single `ReplicationState` owns the server's replication identity and aggregates per-shard offsets.

**When to use:** Shared via `Arc<RwLock<ReplicationState>>` injected into listener and shard threads — same pattern as `Arc<RwLock<RuntimeConfig>>` in `src/server/listener.rs`.

```rust
// src/replication/state.rs
use std::sync::atomic::{AtomicU64, Ordering};

pub struct ReplicationState {
    pub role: ReplicationRole,
    /// Primary replication ID (40-char hex). Survives restarts -- persisted to disk.
    pub repl_id: String,
    /// Secondary replication ID (previous master's ID). Used in failover scenarios.
    pub repl_id2: String,
    /// Per-shard write offset (bytes appended to WAL since server start, monotonic).
    /// Length = num_shards. Incremented atomically by the shard event loop.
    pub shard_offsets: Vec<AtomicU64>,
    /// Global replication offset = sum of all shard offsets.
    pub master_repl_offset: AtomicU64,
    /// Connected replicas (master mode). Guarded by the outer RwLock.
    pub replicas: Vec<ReplicaInfo>,
}

pub enum ReplicationRole {
    Master,
    Replica { host: String, port: u16, state: ReplicaHandshakeState },
}

pub struct ReplicaInfo {
    pub id: u64,  // monotonic, for unregister
    pub addr: std::net::SocketAddr,
    /// Per-shard acknowledged offsets from this replica (updated on REPLCONF ACK).
    pub ack_offsets: Vec<AtomicU64>,
    /// Channels to per-shard sender tasks. Index = shard_id.
    pub shard_txs: Vec<tokio::sync::mpsc::Sender<bytes::Bytes>>,
    /// Last ACK timestamp for lag computation.
    pub last_ack_time: std::sync::atomic::AtomicU64, // unix seconds
}
```

**Persistence:** On startup load `{persistence_dir}/replication.state` (plain text: `repl_id\nrepl_id2\n`). On ID change, atomically write via `.tmp` + rename (same pattern as `WalWriter::truncate_after_snapshot`).

### Pattern 2: ReplicationBacklog — Per-Shard Circular Buffer

**What:** Each shard maintains a fixed-size circular buffer of recent WAL bytes. Used to service partial resync requests without re-reading from disk.

**When to use:** Instantiated in the shard event loop alongside `WalWriter`. In-memory only; WAL on disk is the durable version.

```rust
// src/replication/backlog.rs
pub struct ReplicationBacklog {
    buf: std::collections::VecDeque<u8>,
    capacity: usize,
    /// Monotonic WAL offset of buf[0]. Never resets.
    start_offset: u64,
    /// Monotonic WAL offset of buf[buf.len()-1] + 1.
    end_offset: u64,
}

impl ReplicationBacklog {
    pub fn new(capacity: usize) -> Self {
        ReplicationBacklog {
            buf: std::collections::VecDeque::with_capacity(capacity),
            capacity,
            start_offset: 0,
            end_offset: 0,
        }
    }

    pub fn append(&mut self, data: &[u8]) {
        for &b in data {
            if self.buf.len() == self.capacity {
                self.buf.pop_front();
                self.start_offset += 1;
            }
            self.buf.push_back(b);
        }
        self.end_offset += data.len() as u64;
    }

    /// Returns owned Vec of bytes from `offset` to current end, or None if offset evicted.
    pub fn bytes_from(&self, offset: u64) -> Option<Vec<u8>> {
        if offset < self.start_offset || offset > self.end_offset {
            return None;
        }
        let skip = (offset - self.start_offset) as usize;
        Some(self.buf.iter().skip(skip).copied().collect())
    }

    pub fn contains_offset(&self, offset: u64) -> bool {
        offset >= self.start_offset && offset <= self.end_offset
    }
}
```

**Critical invariant:** `start_offset` and `end_offset` are monotonically increasing counters that match the per-shard `shard_offsets[shard_id]` in `ReplicationState`. They never reset on WAL truncation.

### Pattern 3: PSYNC2 Handshake Sequence

**What:** The exact message sequence for PSYNC2 negotiation.

**Master-side (incoming replica connection):**
```
Replica -> Master:  PING
Master  -> Replica: +PONG\r\n
Replica -> Master:  REPLCONF listening-port <port>\r\n
Master  -> Replica: +OK\r\n
Replica -> Master:  REPLCONF capa eof capa psync2\r\n
Master  -> Replica: +OK\r\n
Replica -> Master:  PSYNC <repl_id> <offset>\r\n
Master  -> Replica: +FULLRESYNC <repl_id> 0\r\n    (full resync)
                OR  +CONTINUE <repl_id>\r\n          (partial resync PSYNC2)
```

After `+FULLRESYNC`: master sends per-shard RDB bulk strings, then live stream.
After `+CONTINUE`: master immediately streams backlog bytes from `offset`.

**Replica-side (outbound, connecting to master):**

The replica spawns a dedicated Tokio task (`replication::replica::run_replica_task`) that:
1. Connects to master TCP address; retries on failure with exponential backoff
2. Sends PING, waits for PONG
3. Sends REPLCONF listening-port + capa psync2
4. Sends PSYNC with stored repl_id + per-shard sum offset
5. Parses master response (FULLRESYNC / CONTINUE)
6. If FULLRESYNC: reads N per-shard RDB bulk transfers, triggers per-shard snapshot load via `ShardMessage::ReplicaLoadSnapshot`
7. If CONTINUE: seeks to offset, enters streaming mode
8. Streams incoming WAL bytes into per-shard command dispatch

**Encoding:** Use existing `serialize::serialize()` and the `RespCodec` from `src/server/codec.rs`.

### Pattern 4: Master Fan-Out — WAL Bytes to Replica Channels

**What:** After every WAL append in the shard event loop, forward the same bytes to all connected replica channels.

**Where:** In `drain_spsc_shared` / the Execute handler, immediately after `wal_writer.append(data)`.

```rust
// In shard event loop, after WAL append -- integrated with existing pattern:
fn fan_out_to_replicas(
    data: &[u8],
    repl_backlog: &mut Option<ReplicationBacklog>,
    replica_txs: &[tokio::sync::mpsc::Sender<bytes::Bytes>],
    shard_offset: &std::sync::atomic::AtomicU64,
) {
    // Update in-memory backlog first
    if let Some(ref mut backlog) = repl_backlog {
        backlog.append(data);
    }
    // Update monotonic shard offset
    shard_offset.fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
    // Fan-out to all replica sender channels (non-blocking)
    let bytes = bytes::Bytes::copy_from_slice(data);
    for tx in replica_txs {
        let _ = tx.try_send(bytes.clone());
        // try_send: if channel full, replica is lagging -- backlog will cover catchup
    }
}
```

**Per-shard replica sender task (one task per replica per shard):**
```rust
async fn replica_sender_task(
    shard_id: usize,
    replica_id: u64,
    mut write_half: tokio::net::tcp::OwnedWriteHalf,
    mut rx: tokio::sync::mpsc::Receiver<bytes::Bytes>,
) {
    use tokio::io::AsyncWriteExt;
    while let Some(data) = rx.recv().await {
        if write_half.write_all(&data).await.is_err() {
            tracing::info!("Shard {}: replica {} disconnected", shard_id, replica_id);
            break;
        }
    }
}
```

### Pattern 5: Full Resync Coordination

**What:** Master triggers per-shard snapshots in parallel, streams them, then switches to live streaming.

**Steps:**
1. Master receives `PSYNC <unknown_id_or_-1>`
2. Respond `+FULLRESYNC <repl_id> <current_offset>\r\n`
3. Record `snapshot_start_offset` = current sum of all `shard_offsets`
4. Send `ShardMessage::SnapshotBegin { epoch, snapshot_dir, reply_tx }` to all N shards simultaneously via SPSC (existing message type)
5. Await all N `oneshot` reply channels
6. For each shard snapshot file: send `$<len>\r\n<file_bytes>\r\n` to replica TCP connection
7. For each shard backlog: send bytes from `snapshot_start_offset` to `current_offset`
8. Transition replica connection to streaming mode (step 4 above)

**Key:** Step 4-5 reuse the existing `ShardMessage::SnapshotBegin` infrastructure from Phase 14. No new shard messages are needed for snapshot triggering.

### Pattern 6: Replica Read-Only Enforcement

**What:** When server role is `Replica`, write commands are rejected before dispatch.

**Where:** In `handle_connection_sharded()` in `src/server/connection.rs`, same interception point as AUTH, BGSAVE, CONFIG.

```rust
// After command parsing, before dispatch -- in the sharded connection handler:
if let ReplicationRole::Replica { .. } = repl_state.read().role {
    if aof::is_write_command(cmd_name) {
        // aof::is_write_command already exists and contains all write command names
        sink.send(Frame::Error(bytes::Bytes::from_static(
            b"READONLY You can't write against a read only replica."
        ))).await?;
        continue;
    }
}
```

**Advantage:** `aof::is_write_command` in `src/persistence/aof.rs` already has the complete list of write commands (SET, MSET, HSET, LPUSH, etc.). No need to duplicate this list.

### Pattern 7: REPLCONF ACK and WAIT Integration

**What:** Replicas send `REPLCONF ACK <offset>` periodically. Master uses this for WAIT.

**Timing:** Replica sends ACK every 1 second from its replication task. Master sends PING heartbeat every 10 seconds to detect dead replicas.

**WAIT implementation steps:**
1. `WAIT numreplicas timeout` received on master
2. Record `target_offset` = current `master_repl_offset`
3. Register a `WaitEntry` in a new `WaitRegistry` (same structure as `BlockingRegistry` from Phase 17)
4. Each incoming `REPLCONF ACK <offset>` updates `ReplicaInfo::ack_offsets[shard_id]`; if N replicas now have sum_ack >= target_offset, resolve the wait via `oneshot::Sender`
5. On timeout, return count of replicas that acknowledged before deadline

**`WaitRegistry`** can be a simplified version of `BlockingRegistry` with entries keyed by offset threshold rather than data key. Place in `src/replication/master.rs`.

### Anti-Patterns to Avoid

- **Blocking the shard event loop on network I/O to replicas:** Never perform synchronous TCP sends inside the shard tick. Always use `try_send` to bounded mpsc channels; let dedicated Tokio tasks handle socket writes.
- **Global single backlog:** Redis has one backlog per server. This project's advantage is per-shard backlogs — do not aggregate into one shared buffer.
- **Sequential per-shard full resync:** Trigger all shard snapshots concurrently via SPSC; collect all replies before beginning transfer. Sequential per-shard resync doubles reconnect time for N shards.
- **Resetting the monotonic offset on WAL truncation:** The replication offset is a monotonic counter that NEVER resets. `WalWriter::bytes_written` resets on truncation — do NOT use it as the replication offset. Maintain a separate `AtomicU64` per shard in `ReplicationState`.
- **Forgetting repl_id2:** When a replica is promoted to master (REPLICAOF NO ONE), the old master's replication ID becomes `repl_id2`. Omitting this forces all failover replicas to full resync. Copy old `repl_id` to `repl_id2` on promotion.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Circular backlog buffer | Custom ring implementation | `std::collections::VecDeque<u8>` | Correct, simple, no unsafe; ringbuf already in deps for higher-perf variant |
| RESP framing for replica connections | Custom framing | `RespCodec` from `src/server/codec.rs` | Already exists, handles all frame types; same approach as all existing connections |
| TCP connection management | Custom reconnect loop | Tokio `TcpStream` + `tokio::time::sleep` exponential backoff | TCP in Tokio is production-tested |
| Replication ID generation | UUID library (new dep) | `rand::Rng` (already in deps) with hex format | 40 hex chars = 20 random bytes; rand already at v0.9 |
| Write command check on replica | New command list | `aof::is_write_command()` from `src/persistence/aof.rs` | Already has the complete list of 40+ write commands |
| Full snapshot for resync | New serialization | `SnapshotState` + `shard_snapshot_save` from Phase 14 | All snapshot machinery already in `src/persistence/snapshot.rs` |
| Fan-out channel pattern | New IPC mechanism | `tokio::sync::mpsc` (already used for AOF, conn_tx) | Identical to how AOF messages and connection assignments are handled |

**Key insight:** The WAL is already in RESP wire format — the same bytes that go to disk can be forwarded verbatim to replicas. No re-serialization is needed. This is the most important reuse opportunity in the phase.

---

## Common Pitfalls

### Pitfall 1: Offset Tracking Misalignment — WAL bytes_written vs Replication Offset

**What goes wrong:** `WalWriter::bytes_written` resets to 0 on every `truncate_after_snapshot` call. If the replication offset tracks `bytes_written`, replicas with an older offset believe they're ahead of the master after a snapshot cycle, triggering spurious full resyncs.

**Why it happens:** Phase 14 resets `WalWriter::bytes_written` in `truncate_after_snapshot()` (line 139: `self.bytes_written = 0`). This is correct for WAL size tracking but wrong for replication.

**How to avoid:** The per-shard replication offset (`shard_offsets[shard_id]` in `ReplicationState`) is a monotonically increasing `AtomicU64` that NEVER resets. It is incremented on every WAL append — independently of WAL truncation. The backlog's `start_offset` and `end_offset` also use this monotonic counter.

**Warning signs:** Every replica reconnection triggers full resync even when it disconnected briefly.

### Pitfall 2: Full Resync Race — Writes Arriving During Snapshot Transfer

**What goes wrong:** The master starts a snapshot at epoch E. While the snapshot advances one segment per 1ms tick, new writes arrive with offsets E+1, E+2, etc. These writes go to the backlog. The master must send: snapshot bytes (epoch E data), then backlog bytes from offset E to current. If the master sends backlog bytes before the snapshot transfer completes, the replica applies writes before it has a consistent base.

**Why it happens:** Per-shard snapshot is incremental (one segment per tick, Phase 14 design). It is not instantaneous.

**How to avoid:** Record `snapshot_start_offset` at the moment `SnapshotBegin` is sent. Only after ALL shard snapshot files have been transferred does the master stream the backlog from `snapshot_start_offset` to `current_offset`. The replica does not apply the stream until it has loaded all snapshot files and explicitly signals readiness.

**Warning signs:** Duplicate keys or version conflicts on the replica.

### Pitfall 3: Replica Connection Handled by Single Shard Thread

**What goes wrong:** In the sharded architecture, `run_sharded` distributes each incoming connection to exactly one shard thread via `conn_txs[next_shard]`. A replica connection needs data from ALL shard threads. If the replica connection is handled by shard 0's thread only, shards 1-N never stream to it.

**Why it happens:** The listener's round-robin distribution assigns the incoming replica TCP connection to one shard.

**How to avoid:** When the connection is identified as a replica at the PSYNC command, register it with the global `Arc<RwLock<ReplicationState>>`. Each shard, upon receiving a `ShardMessage::RegisterReplica { shard_tx }` message, spawns its own `replica_sender_task`. The "primary" shard (the one that received the TCP connection) handles the handshake protocol and full-resync coordination; all shards handle WAL fan-out independently.

**Warning signs:** Replica dataset is incomplete — only data for keys hashing to the "primary" shard appears.

### Pitfall 4: Blocking WAIT on Shard Thread

**What goes wrong:** `WAIT numreplicas timeout` naively blocks a Tokio task pending ACK messages. In the single-threaded shard runtime, a blocked task stalls all other tasks on that shard.

**Why it happens:** Waiting for an external event (REPLCONF ACK from replica) without yielding.

**How to avoid:** Use the Phase 17 pattern: register a `WaitEntry` in `WaitRegistry` and return from the handler; resolve via `oneshot::Sender` when enough ACKs arrive in the shard's 1ms tick. Identical to how `BlockingRegistry` defers BLPOP resolution.

**Warning signs:** Shard throughput drops to zero while any WAIT command is active.

### Pitfall 5: INFO Replication With Zero/Stale Lag

**What goes wrong:** Redis clients and monitoring tools check `master_last_io_seconds_ago` and `slave_repl_offset`. Returning zeros or stale values breaks clients that use replication lag for health checks (e.g., Redis Sentinel compatibility layer, Jedis replica reads).

**How to avoid:** `ReplicaInfo` stores `last_ack_time: AtomicU64` (unix seconds) and `ack_offsets`. INFO replication computes `lag = master_offset - sum(replica_ack_offsets)` and `last_io = current_time - last_ack_time`.

### Pitfall 6: Replication ID Not Persisted Across Restarts

**What goes wrong:** Server restart generates a new replication ID. All replicas must perform full resync after every restart, defeating the purpose of PSYNC2.

**How to avoid:** Write `{persistence_dir}/replication.state` with `repl_id\nrepl_id2\n` on every ID change using the atomic write pattern (`.tmp` + rename, same as WAL). Load this file during `Shard::restore_from_persistence()` before the event loop starts.

---

## Code Examples

### Replication State Persistence

```rust
// Source: Pattern from WalWriter::truncate_after_snapshot (src/persistence/wal.rs)

pub fn save_replication_state(dir: &std::path::Path, repl_id: &str, repl_id2: &str) -> std::io::Result<()> {
    let tmp = dir.join("replication.state.tmp");
    let dst = dir.join("replication.state");
    std::fs::write(&tmp, format!("{}\n{}\n", repl_id, repl_id2))?;
    std::fs::rename(&tmp, &dst)?;
    Ok(())
}

pub fn load_replication_state(dir: &std::path::Path) -> (String, String) {
    let path = dir.join("replication.state");
    if let Ok(content) = std::fs::read_to_string(&path) {
        let mut lines = content.lines();
        let id1 = lines.next().unwrap_or("").to_string();
        let id2 = lines.next().unwrap_or(
            "0000000000000000000000000000000000000000"
        ).to_string();
        if id1.len() == 40 {
            return (id1, id2);
        }
    }
    // Generate new ID if not found or invalid
    let id1 = generate_repl_id();
    let id2 = "0000000000000000000000000000000000000000".to_string();
    let _ = save_replication_state(dir, &id1, &id2);
    (id1, id2)
}
```

### Replication ID Generation

```rust
// Source: rand crate (0.9, already in Cargo.toml)
use rand::Rng;

pub fn generate_repl_id() -> String {
    let mut rng = rand::rng();
    (0..20)
        .map(|_| format!("{:02x}", rng.random::<u8>()))
        .collect()
    // Produces 40-char hex string matching Redis replication ID format
}
```

### PSYNC2 Decision Logic

```rust
// Source: Redis replication protocol, adapted to per-shard backlog model

pub enum PsyncDecision {
    FullResync,
    PartialResync { from_offset: u64 },
}

pub fn evaluate_psync(
    client_repl_id: &str,
    client_offset: i64,
    server_repl_id: &str,
    server_repl_id2: &str,
    per_shard_backlogs: &[ReplicationBacklog],
) -> PsyncDecision {
    if client_offset < 0 {
        return PsyncDecision::FullResync;
    }
    let id_matches = client_repl_id == server_repl_id || client_repl_id == server_repl_id2;
    if !id_matches {
        return PsyncDecision::FullResync;
    }
    // Partial resync only if ALL shard backlogs cover the requested offset.
    // Per-shard granularity: a shard that has the offset can use partial resync;
    // a shard that evicted the offset needs full resync for that shard only.
    let offset = client_offset as u64;
    if per_shard_backlogs.iter().all(|b| b.contains_offset(offset)) {
        PsyncDecision::PartialResync { from_offset: offset }
    } else {
        PsyncDecision::FullResync
    }
}
```

### WAL Fan-Out Integration Point

```rust
// Source: shard event loop pattern (src/shard/mod.rs)
// This replaces/wraps the existing WAL append call in drain_spsc_shared.

fn wal_append_and_fanout(
    data: &[u8],
    wal_writer: &mut Option<WalWriter>,
    repl_backlog: &mut Option<ReplicationBacklog>,
    replica_txs: &[tokio::sync::mpsc::Sender<bytes::Bytes>],
    shard_offset: &std::sync::atomic::AtomicU64,
) {
    if let Some(ref mut w) = wal_writer {
        w.append(data);
    }
    if let Some(ref mut backlog) = repl_backlog {
        backlog.append(data);
    }
    shard_offset.fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
    if !replica_txs.is_empty() {
        let bytes = bytes::Bytes::copy_from_slice(data);
        for tx in replica_txs {
            let _ = tx.try_send(bytes.clone());
        }
    }
}
```

### INFO Replication Section Builder

```rust
// Source: Redis INFO replication format (stable since Redis 4.0)
// Extends info() function in src/command/connection.rs

pub fn build_info_replication(repl_state: &ReplicationState) -> String {
    let mut s = String::from("# Replication\r\n");
    match &repl_state.role {
        ReplicationRole::Master => {
            s.push_str("role:master\r\n");
            let num_replicas = repl_state.replicas.len();
            s.push_str(&format!("connected_slaves:{}\r\n", num_replicas));
            s.push_str(&format!("master_failover_state:no-failover\r\n"));
            s.push_str(&format!("master_replid:{}\r\n", repl_state.repl_id));
            s.push_str(&format!("master_replid2:{}\r\n", repl_state.repl_id2));
            let offset = repl_state.master_repl_offset.load(Ordering::Relaxed);
            s.push_str(&format!("master_repl_offset:{}\r\n", offset));
            s.push_str("second_repl_offset:-1\r\n");
            s.push_str("repl_backlog_active:1\r\n");
            s.push_str("repl_backlog_size:1048576\r\n");
            s.push_str(&format!("repl_backlog_first_byte_offset:{}\r\n", 1));
            s.push_str(&format!("repl_backlog_histlen:{}\r\n", offset));
            for (i, replica) in repl_state.replicas.iter().enumerate() {
                let ack_offset: u64 = replica.ack_offsets.iter()
                    .map(|a| a.load(Ordering::Relaxed)).sum();
                let lag = offset.saturating_sub(ack_offset);
                s.push_str(&format!(
                    "slave{}:ip={},port={},state=online,offset={},lag={}\r\n",
                    i, replica.addr.ip(), replica.addr.port(), ack_offset, lag
                ));
            }
        }
        ReplicationRole::Replica { host, port, .. } => {
            s.push_str("role:slave\r\n");
            s.push_str(&format!("master_host:{}\r\n", host));
            s.push_str(&format!("master_port:{}\r\n", port));
            s.push_str("master_link_status:up\r\n");
            s.push_str("master_last_io_seconds_ago:0\r\n");
            s.push_str("master_sync_in_progress:0\r\n");
            let offset = repl_state.master_repl_offset.load(Ordering::Relaxed);
            s.push_str(&format!("slave_repl_offset:{}\r\n", offset));
            s.push_str(&format!("slave_priority:{}\r\n", 100));
            s.push_str("slave_read_only:1\r\n");
            s.push_str("replica_announced:1\r\n");
            s.push_str("connected_slaves:0\r\n");
            s.push_str(&format!("master_replid:{}\r\n", repl_state.repl_id));
            s.push_str(&format!("master_replid2:{}\r\n", repl_state.repl_id2));
            s.push_str(&format!("master_repl_offset:{}\r\n", offset));
            s.push_str("second_repl_offset:-1\r\n");
            s.push_str("repl_backlog_active:0\r\n");
        }
    }
    s.push_str("\r\n");
    s
}
```

---

## Integration Points Map

### Where REPLICAOF/REPLCONF/PSYNC are parsed

**Location:** `src/server/connection.rs` → `handle_connection_sharded()`.

The function currently intercepts at lines ~95-400: AUTH, QUIT, CONFIG, BGSAVE, DBSIZE, KEYS, SCAN, MULTI/EXEC/DISCARD, WAIT, blocking commands, INFO, OBJECT, HELLO. REPLICAOF and REPLCONF join this list with the same `eq_ignore_ascii_case` dispatch pattern.

When PSYNC arrives on a connection, that connection transitions to "replica streaming mode" — it stops reading commands and starts receiving WAL stream bytes. This is analogous to how a connection transitions to PubSub subscriber mode.

### Where WAL bytes are appended (fan-out integration)

**Location:** `src/shard/mod.rs` → `drain_spsc_shared()` and the SPSC Execute handler.

Current WAL append (from `drain_spsc_shared` — the pattern where write commands are detected):
```rust
if let Some(ref mut w) = wal_writer {
    if aof::is_write_command(cmd_name) {
        w.append(&aof::serialize_command(&frame));
    }
}
```
The fan-out call inserts immediately after `w.append(...)`.

### Where ShardMessage needs new variants

Two new variants for `ShardMessage` in `src/shard/dispatch.rs`:

```rust
/// Register a new replica's per-shard sender channel.
/// Sent to all shards when a new replica completes PSYNC handshake.
RegisterReplica {
    replica_id: u64,
    tx: tokio::sync::mpsc::Sender<bytes::Bytes>,
},
/// Unregister a replica (disconnected).
UnregisterReplica {
    replica_id: u64,
},
```

### Where ServerConfig gets new fields

**Location:** `src/config.rs` → `ServerConfig` struct.

```rust
/// Master to replicate from, "host:port" format (empty = act as master)
#[arg(long, default_value = "")]
pub replicaof: String,

/// Per-shard replication backlog size in bytes
#[arg(long, default_value_t = 1_048_576)]
pub repl_backlog_size: usize,

/// Seconds between replica heartbeat pings
#[arg(long, default_value_t = 10)]
pub repl_timeout: u64,

/// Whether replicas serve stale reads during full resync (yes/no)
#[arg(long, default_value = "yes")]
pub replica_serve_stale_data: String,

/// Replica priority for failover election (lower = more preferred, 0 = never)
#[arg(long, default_value_t = 100)]
pub replica_priority: u16,
```

### Where replica offset tracking integrates with `Shard::run()`

The shard event loop `Shard::run()` in `src/shard/mod.rs` needs:
1. A `repl_backlog: Option<ReplicationBacklog>` local variable (alongside existing `wal_writer`)
2. A `replica_txs: Vec<mpsc::Sender<Bytes>>` for fan-out (populated via `RegisterReplica` messages)
3. The `Arc<AtomicU64>` shard offset reference passed in from the shared `ReplicationState`

### Where INFO is extended

**Location:** `src/command/connection.rs` → `info()` function (line 122).

The existing `info()` function builds sections with `sections.push_str(...)`. Add a `# Replication` section by calling `build_info_replication(&repl_state)` and appending the result. The `repl_state: Arc<RwLock<ReplicationState>>` is passed into the connection handler the same way `RuntimeConfig` is passed.

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Redis single-shard replication backlog | Per-shard replication backlog (this project) | Phase 19 design | Only catch up affected shards; better partial resync hit rate |
| Redis fork-based BGSAVE for full resync | Forkless SnapshotState (Phase 14) | Phase 14 | No fork latency; 5% memory overhead vs 200% during snapshot |
| Redis PSYNC v1 (single repl ID) | PSYNC2 (repl_id + repl_id2) | Redis 4.0 | Failover without full resync |
| Global AOF for all writes | Per-shard WAL (Phase 14) | Phase 14 | WAL IS the replication stream; no extra serialization needed |
| Redis single-threaded replication | Per-shard parallel WAL streaming | Phase 19 design | Fan-out is O(num_replicas) not O(num_shards * num_replicas) per write |

**Deprecated/outdated:**
- PSYNC v1 (single replication ID): replaced by PSYNC2 in Redis 4.0; repl_id2 is required for failover without full resync
- Redis `slave` terminology in REPLCONF/INFO: `slave` is still the wire protocol term for compatibility, but internal code should use `replica`

---

## Open Questions

1. **Full resync RDB framing: N separate bulk strings or length-prefixed array?**
   - What we know: Redis sends a single RDB bulk string `$<len>\r\n<bytes>`. Phase 14 produces N separate `.rrdshard` files.
   - What's unclear: The replica needs to know how many shard files to expect and which shard each belongs to.
   - Recommendation: Send a preamble `*<N>\r\n` (RESP Array header indicating N files), then each shard file as `$<len>\r\n<bytes>`. Prefix each file with a 1-byte shard ID header before the RRDSHARD magic. This is backward compatible (replicas negotiate `capa psync2` so both sides know this format).

2. **Per-shard vs global offset for WAIT semantics**
   - What we know: Redis WAIT uses a single global offset. This project has per-shard offsets.
   - Recommendation: WAIT waits until `sum(replica.ack_offsets)` >= `master_repl_offset` for N replicas. The global `master_repl_offset` = sum of all `shard_offsets`, so this is consistent. Replicas periodically send `REPLCONF ACK <sum_of_shard_offsets>` to maintain compatibility with Redis clients.

3. **io_uring for replica sender tasks**
   - What we know: Shard threads use io_uring for client socket I/O (Phase 12).
   - Recommendation: Use plain Tokio `OwnedWriteHalf` for replica sender tasks initially. The replica sender task is a separate Tokio task that runs within the shard's `current_thread` runtime via `tokio::task::spawn_local` — the same pattern as connection handling tasks. io_uring optimization for replica I/O is a future enhancement.

4. **Reconnect behavior when replica falls too far behind**
   - What we know: If a replica's offset falls outside the 1MB backlog, it needs full resync.
   - What's unclear: Should the master detect this proactively (monitoring backlog start_offset vs last known replica offset) and pre-emptively start full resync?
   - Recommendation: Reactive approach: the replica sends its offset on reconnect; master checks backlog coverage then; if miss, responds FULLRESYNC. Proactive detection is a future optimization.

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in test (`cargo test`) |
| Config file | none (inline `#[test]` and `#[tokio::test]`) |
| Quick run command | `cargo test replication -- --test-threads=4 2>&1` |
| Full suite command | `cargo test --lib` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| REQ-19-01 | REPLICAOF command parses host/port correctly | unit | `cargo test replication::tests::test_replicaof_command` | ❌ Wave 0 |
| REQ-19-01 | REPLICAOF NO ONE promotes to master | unit | `cargo test replication::tests::test_replicaof_no_one` | ❌ Wave 0 |
| REQ-19-01 | REPLCONF listening-port parsed | unit | `cargo test replication::handshake::tests::test_replconf_listening_port` | ❌ Wave 0 |
| REQ-19-02 | Full resync: SnapshotBegin triggered for all shards | integration | `cargo test replication::master::tests::test_full_resync_triggers_snapshot` | ❌ Wave 0 |
| REQ-19-02 | Full resync: backlog bytes streamed after snapshot | integration | `cargo test replication::master::tests::test_full_resync_backlog_stream` | ❌ Wave 0 |
| REQ-19-03 | Backlog append + bytes_from within range returns data | unit | `cargo test replication::backlog::tests::test_backlog_bytes_from` | ❌ Wave 0 |
| REQ-19-03 | Backlog eviction: offset below start_offset returns None | unit | `cargo test replication::backlog::tests::test_backlog_eviction` | ❌ Wave 0 |
| REQ-19-03 | Backlog monotonic offset never resets | unit | `cargo test replication::backlog::tests::test_backlog_monotonic` | ❌ Wave 0 |
| REQ-19-04 | evaluate_psync returns PartialResync when all backlogs cover offset | unit | `cargo test replication::handshake::tests::test_psync_partial` | ❌ Wave 0 |
| REQ-19-04 | evaluate_psync returns FullResync when any backlog misses offset | unit | `cargo test replication::handshake::tests::test_psync_full_on_miss` | ❌ Wave 0 |
| REQ-19-05 | Write command returns READONLY on replica role | unit | `cargo test replication::tests::test_replica_readonly` | ❌ Wave 0 |
| REQ-19-06 | save/load replication state round trip | unit | `cargo test replication::state::tests::test_state_persistence` | ❌ Wave 0 |
| REQ-19-06 | generate_repl_id produces 40-char hex | unit | `cargo test replication::state::tests::test_repl_id_format` | ❌ Wave 0 |
| REQ-19-07 | INFO replication master section format | unit | `cargo test replication::tests::test_info_replication_master` | ❌ Wave 0 |
| REQ-19-07 | INFO replication replica section format | unit | `cargo test replication::tests::test_info_replication_replica` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test replication 2>&1 | tail -10`
- **Per wave merge:** `cargo test --lib`
- **Phase gate:** `cargo test --lib` passes all 800+ existing tests plus all new replication tests

### Wave 0 Gaps
- [ ] `src/replication/mod.rs` — module declaration; `ReplicationRole`, `ReplicationAction` enums
- [ ] `src/replication/state.rs` — `ReplicationState`, `ReplicaInfo`; persistence helpers
- [ ] `src/replication/backlog.rs` — `ReplicationBacklog` with full test coverage
- [ ] `src/replication/handshake.rs` — `evaluate_psync`, handshake state enum
- [ ] `src/replication/master.rs` — replica registry, fan-out, `WaitRegistry` for WAIT
- [ ] `src/replication/replica.rs` — outbound connection task skeleton
- [ ] Framework install: none — `cargo test` already works (801 tests passing)

---

## Sources

### Primary (HIGH confidence)
- `src/persistence/wal.rs` — `WalWriter` structure, WAL RESP format, `append`/`flush`/`truncate` API; critical finding: `bytes_written` resets on truncation, must NOT be used as replication offset
- `src/persistence/snapshot.rs` — `SnapshotState`, `shard_snapshot_save`, `shard_snapshot_load`; full resync data source
- `src/shard/dispatch.rs` — `ShardMessage` variants; `key_to_shard`; SPSC communication pattern
- `src/shard/mod.rs` (lines 1-350) — shard event loop; `drain_spsc_shared`; WAL integration; snapshot state machine; `RegisterReplica` insertion point
- `src/server/listener.rs` — `run_sharded`; connection distribution; `Arc<RwLock<RuntimeConfig>>` injection pattern
- `src/command/connection.rs` — command interception pattern (AUTH, HELLO, INFO, BGSAVE); `info()` structure; `handle_connection_sharded` extension points
- `src/config.rs` — `ServerConfig` and `RuntimeConfig` patterns
- `src/main.rs` — shard thread startup; watch channel pattern; persistence dir wiring
- `src/blocking/mod.rs` — `BlockingRegistry`, `WaitEntry` pattern for WAIT command
- `src/persistence/aof.rs` — `is_write_command()` list (reused for replica READONLY enforcement)
- `.planning/architect-blue-print.md` §Cluster design — per-shard replication backlog rationale; "significant improvement over Redis's single-backlog design" quote

### Secondary (MEDIUM confidence)
- `.planning/STATE.md` accumulated decisions — AUTH/BGSAVE/CONFIG intercepted at connection level confirmed; `Rc<RefCell<PubSubRegistry>>` pattern for per-shard registry (same for replica sender channels)
- `Cargo.toml` — verified all required crates (bytes 1.10, tokio 1, rand 0.9, ringbuf 0.4) already present; no new dependencies needed

### Tertiary (LOW confidence)
- Redis PSYNC2 handshake sequence — inferred from Redis 4.0+ behavior, public documentation, and the CONTEXT.md locked decisions. The exact `+FULLRESYNC <id> <offset>` and `+CONTINUE <id>` wire format is stable and well-documented in the Redis community. Not directly verified against Redis source code during this research session.

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all crates verified in Cargo.toml; zero new dependencies
- Architecture patterns: HIGH — derived directly from existing code (WalWriter, SnapshotState, BlockingRegistry, ShardMessage, connection interception)
- PSYNC2 protocol wire format: MEDIUM-HIGH — stable since Redis 4.0; format consistent with locked CONTEXT.md decisions
- Pitfalls: HIGH — derived from direct code analysis of `WalWriter.bytes_written` reset, `SnapshotState` incremental advance, and `ShardMessage` SPSC routing

**Research date:** 2026-03-24
**Valid until:** 2026-06-24 (stable — Redis PSYNC2 protocol unchanged since Redis 4.0; project architecture stable through Phase 18)
