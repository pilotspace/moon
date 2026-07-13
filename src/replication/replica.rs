//! Outbound replica connection task.
//!
//! Spawned via tokio::spawn when REPLICAOF host port is executed.
//! Performs the PSYNC2 handshake with the master, then enters streaming mode
//! where it reads WAL bytes from the master and dispatches them as commands.
#![allow(unused_imports)]

use bytes::{Bytes, BytesMut};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::Duration;
#[cfg(feature = "runtime-tokio")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "runtime-tokio")]
use tokio::net::TcpStream;
use tracing::{info, warn};

use crate::replication::handshake::ReplicaHandshakeState;
use crate::replication::state::{ReplicationRole, ReplicationState, save_replication_state};
use crate::shard::shared_databases::ShardDatabases;

/// Process-global generation counter for replica tasks (attach-under-write
/// P0, found while testing R2): `REPLICAOF host port` used to spawn a fresh
/// `run_replica_task` WITHOUT stopping the previous one, and `REPLICAOF NO
/// ONE` only flipped the role state — the old task kept its master link open
/// and kept APPLYING the stream. After a NO-ONE → re-attach cycle, two (then
/// three, ...) live tasks each applied every record: replica INCR counters
/// ran ~25-35% ABOVE the master under write load (reproduced at shards=1 and
/// shards=4 — pre-existing, not an R2 defect).
///
/// Every spawn bumps the epoch and hands the task its ticket; `REPLICAOF NO
/// ONE` bumps it too. A task whose ticket no longer matches exits before its
/// next connect, before applying a snapshot, and before applying any parsed
/// chunk — a superseded task can never mutate the keyspace again (its parked
/// socket read wakes on the next master byte or link close and hits the
/// pre-apply check).
static REPLICA_TASK_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Bump the generation (new REPLICAOF target, or NO ONE) and return the new
/// ticket to hand to a freshly spawned task.
pub fn bump_replica_task_epoch() -> u64 {
    REPLICA_TASK_EPOCH.fetch_add(1, Ordering::AcqRel) + 1
}

/// True when `epoch` is no longer the live generation — the owning task must
/// stop without touching local state.
fn superseded(epoch: u64) -> bool {
    REPLICA_TASK_EPOCH.load(Ordering::Acquire) != epoch
}

/// Configuration for the replica outbound connection task.
pub struct ReplicaTaskConfig {
    pub master_host: String,
    pub master_port: u16,
    pub repl_state: Arc<RwLock<ReplicationState>>,
    pub num_shards: usize,
    pub persistence_dir: Option<String>,
    pub listening_port: u16,
    /// Gives `apply::load_snapshot` / `apply::apply_local` access to the
    /// process-global `WorkspaceRegistry` (Wave B ws-plane), which lives on
    /// `ShardDatabases` rather than the thread-local `ShardSlice`.
    pub shard_databases: Arc<ShardDatabases>,
    /// Generation ticket from [`bump_replica_task_epoch`] — the task exits
    /// as soon as a newer generation exists.
    pub epoch: u64,
    /// Logical-db context of the replication stream, preserved ACROSS
    /// reconnects (HIGH-2, task #22): a `+CONTINUE` partial resync replays
    /// backlog bytes that only contain `SELECT` at db CHANGES — if the stream
    /// was in db N when the link dropped, the resumed bytes carry no fresh
    /// SELECT and a 0-reset drain would misapply them to db 0. Seeded into
    /// `stream_commands`'s drain state on `+CONTINUE`; reset to 0 on
    /// `+FULLRESYNC` (the master resets its own stream-db at snapshot capture,
    /// so post-snapshot bytes always re-establish context). In-memory only: a
    /// replica process restart starts at offset 0 → always FULLRESYNC.
    pub stream_db: std::sync::atomic::AtomicUsize,
}

/// Entry point for the outbound replica task.
///
/// Connects to master, performs PSYNC2 handshake, streams and dispatches WAL commands.
/// Reconnects with exponential backoff on disconnect.
#[cfg(feature = "runtime-tokio")]
pub async fn run_replica_task(cfg: ReplicaTaskConfig) {
    let addr = format!("{}:{}", cfg.master_host, cfg.master_port);
    // R0 streaming replication is single-shard only. A multi-shard replica would
    // misread the master's single diskless RDB bulk and mis-route the command
    // stream (see `apply::load_snapshot`, which is thread-local and clears all
    // dbs per call), silently diverging. Refuse loudly rather than loop forever
    // on a broken sync. Multi-shard replica apply is tracked for R2.
    if cfg.num_shards != 1 {
        tracing::error!(
            "Replica: streaming replication currently supports single-shard only \
             (--shards 1); this node has {} shards. Not starting replication.",
            cfg.num_shards
        );
        return;
    }

    let mut backoff_ms = 500u64;
    const MAX_BACKOFF_MS: u64 = 30_000;

    loop {
        if superseded(cfg.epoch) {
            info!("Replica: task superseded (epoch {}), exiting", cfg.epoch);
            return;
        }
        info!("Replica: connecting to master at {}", addr);
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                backoff_ms = 500; // reset backoff on successful connect
                match run_handshake_and_stream(stream, &cfg).await {
                    Ok(()) => {
                        info!("Replica: stream ended cleanly, reconnecting...");
                    }
                    Err(e) => {
                        warn!("Replica: stream error: {}, reconnecting...", e);
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Replica: connect to {} failed: {}, retrying in {}ms",
                    addr, e, backoff_ms
                );
            }
        }

        // A superseded task must not clobber the successor's handshake state.
        if superseded(cfg.epoch) {
            info!("Replica: task superseded (epoch {}), exiting", cfg.epoch);
            return;
        }

        // Update handshake state to Disconnected in ReplicationState
        if let Ok(mut rs) = cfg.repl_state.write() {
            if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
                *state = ReplicaHandshakeState::Disconnected;
            }
        }

        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
    }
}

/// Perform the PSYNC2 handshake with master, then stream and apply replication data.
#[cfg(feature = "runtime-tokio")]
async fn run_handshake_and_stream(
    mut stream: TcpStream,
    cfg: &ReplicaTaskConfig,
) -> anyhow::Result<()> {
    use crate::protocol::Frame;
    use crate::protocol::serialize;

    let mut write_buf = BytesMut::new();

    // Helper macro: send a RESP Array command
    macro_rules! send_cmd {
        ($stream:expr, $buf:expr, $parts:expr) => {{
            let frame = Frame::Array(
                $parts
                    .iter()
                    .map(|p: &&[u8]| Frame::BulkString(Bytes::copy_from_slice(p)))
                    .collect(),
            );
            $buf.clear();
            serialize(&frame, $buf);
            $stream.write_all($buf).await
        }};
    }

    // Step 1: PING
    if let Ok(mut rs) = cfg.repl_state.write() {
        if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
            *state = ReplicaHandshakeState::PingSent;
        }
    }
    send_cmd!(&mut stream, &mut write_buf, &[b"PING" as &[u8]])?;
    let response = read_line(&mut stream).await?;
    if !response.starts_with(b"+PONG") && !response.starts_with(b"+pong") {
        anyhow::bail!("Expected PONG, got: {:?}", response);
    }

    // Step 2: REPLCONF listening-port
    let port_str = cfg.listening_port.to_string();
    send_cmd!(
        &mut stream,
        &mut write_buf,
        &[b"REPLCONF" as &[u8], b"listening-port", port_str.as_bytes()]
    )?;
    let _ = read_line(&mut stream).await?; // +OK

    // Step 3: REPLCONF capa psync2
    send_cmd!(
        &mut stream,
        &mut write_buf,
        &[b"REPLCONF" as &[u8], b"capa", b"eof", b"capa", b"psync2"]
    )?;
    let _ = read_line(&mut stream).await?; // +OK

    // Step 4: PSYNC <repl_id> <offset>
    let (repl_id, offset) = {
        let rs = cfg
            .repl_state
            .read()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        let offset = rs.master_repl_offset.load(Ordering::Relaxed);
        let id = if offset == 0 {
            "?".to_string()
        } else {
            rs.repl_id.clone()
        };
        let off_str = if offset == 0 {
            "-1".to_string()
        } else {
            offset.to_string()
        };
        (id, off_str)
    };
    if let Ok(mut rs) = cfg.repl_state.write() {
        if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
            *state = ReplicaHandshakeState::PsyncPending;
        }
    }
    send_cmd!(
        &mut stream,
        &mut write_buf,
        &[b"PSYNC" as &[u8], repl_id.as_bytes(), offset.as_bytes()]
    )?;

    // Step 5: Parse master response
    let response = read_line(&mut stream).await?;
    if response.starts_with(b"+FULLRESYNC") {
        // Parse: +FULLRESYNC <repl_id> <offset>
        let parts: Vec<&[u8]> = response[1..].splitn(3, |&b| b == b' ').collect();
        if parts.len() >= 3 {
            let master_id = String::from_utf8_lossy(parts[1]).to_string();
            let master_offset: u64 = std::str::from_utf8(parts[2])
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0);
            // Update local replication state with master's repl_id
            if let Ok(mut rs) = cfg.repl_state.write() {
                rs.repl_id = master_id;
                rs.master_repl_offset
                    .store(master_offset, Ordering::Relaxed);
                if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
                    *state = ReplicaHandshakeState::FullResyncLoading {
                        shards_remaining: cfg.num_shards,
                    };
                }
                // Persist new repl_id
                if let Some(ref dir) = cfg.persistence_dir {
                    let _ = save_replication_state(
                        std::path::Path::new(dir),
                        &rs.repl_id,
                        &rs.repl_id2,
                    );
                }
            }
        }

        // Load the full-resync RDB snapshot into the local shard's databases.
        //
        // R0 targets single-shard replication: `num_shards == 1`, so the master
        // (`handle_psync_inline_single_shard`) sends exactly one diskless RDB
        // bulk and we load it into this thread's ShardSlice. `load_snapshot`
        // clears existing state first (full resync = authoritative). Multi-shard
        // replicas (merged-RDB load) are R2.
        if superseded(cfg.epoch) {
            anyhow::bail!("replica task superseded before snapshot load");
        }
        for shard_id in 0..cfg.num_shards {
            let rdb_bytes = read_rdb_bulk(&mut stream).await?;
            match crate::replication::apply::load_snapshot(&rdb_bytes, &cfg.shard_databases) {
                Ok(keys) => info!(
                    "Replica: loaded shard {} RDB snapshot ({} bytes, {} keys)",
                    shard_id,
                    rdb_bytes.len(),
                    keys
                ),
                Err(e) => {
                    warn!(
                        "Replica: failed to load shard {} RDB snapshot: {}",
                        shard_id, e
                    );
                    return Err(e);
                }
            }
        }

        // Enter streaming mode
        if let Ok(mut rs) = cfg.repl_state.write() {
            if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
                *state = ReplicaHandshakeState::Streaming;
            }
        }
        // FULLRESYNC resets the stream's db context: the master reset its own
        // stream-db in the snapshot-capture stretch, so post-snapshot bytes
        // always re-establish it with an explicit SELECT (task #22).
        cfg.stream_db.store(0, Ordering::Relaxed);
        stream_commands(stream, cfg).await?;
    } else if response.starts_with(b"+CONTINUE") {
        // Partial resync: stream from current offset
        if let Ok(mut rs) = cfg.repl_state.write() {
            if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
                *state = ReplicaHandshakeState::Streaming;
            }
        }
        stream_commands(stream, cfg).await?;
    } else {
        anyhow::bail!("Unexpected PSYNC response: {:?}", response);
    }

    Ok(())
}

/// Stream incoming WAL bytes from master and dispatch as Execute messages to shards.
///
/// Master sends RESP-encoded commands in the same format as the WAL (RESP Array frames).
/// We parse each frame and route it to the correct shard via key_to_shard.
#[cfg(feature = "runtime-tokio")]
async fn stream_commands(stream: TcpStream, cfg: &ReplicaTaskConfig) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt as _;

    // R1 (task #19): the replica acknowledges its applied offset back to the
    // master with `REPLCONF ACK <offset>` — the input WAIT needs to resolve.
    // The socket is split so a dedicated 1s ticker task owns the write half
    // (Redis's replicationCron cadence; also serves as an idle keepalive for
    // master-side lag detection), while this loop keeps the read half.
    let (mut stream, wr) = stream.into_split();
    let ack_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ack_handle = tokio::spawn({
        let stop = ack_stop.clone();
        let repl_state = cfg.repl_state.clone();
        let mut wr = wr;
        async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let offset = match repl_state.read() {
                    Ok(rs) => rs.master_repl_offset.load(Ordering::Relaxed),
                    Err(_) => break,
                };
                if wr.write_all(&encode_replconf_ack(offset)).await.is_err() {
                    break; // socket dead — the read loop is exiting too
                }
            }
        }
    });
    let result = stream_commands_read_loop(&mut stream, cfg).await;
    ack_stop.store(true, Ordering::Relaxed);
    ack_handle.abort();
    result
}

/// The read/apply half of `stream_commands` (tokio).
#[cfg(feature = "runtime-tokio")]
async fn stream_commands_read_loop(
    stream: &mut tokio::net::tcp::OwnedReadHalf,
    cfg: &ReplicaTaskConfig,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(65536);
    // Seeded from the task-level slot (NOT 0): a +CONTINUE resume must keep
    // the db context the stream was in when the link dropped — see
    // `ReplicaTaskConfig::stream_db`.
    let mut selected_db = cfg.stream_db.load(Ordering::Relaxed);

    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(anyhow::anyhow!("Master closed connection"));
        }
        // Superseded tasks must never apply another byte — checked after the
        // parked read wakes, before any parse/apply.
        if superseded(cfg.epoch) {
            anyhow::bail!("replica task superseded — dropping stream unapplied");
        }

        // Parse every complete RESP command in the buffer and apply it to the
        // local shard. The replication offset advances by CONSUMED bytes (whole
        // frames), never the raw socket read count — a read may split a frame.
        let outcome =
            crate::replication::apply::drain_replicated_commands(&mut buf, &mut selected_db);
        for rc in &outcome.commands {
            use crate::replication::apply::ApplyOutcome;
            match crate::replication::apply::apply_local(rc, &cfg.shard_databases) {
                ApplyOutcome::Applied => {}
                // Unified poison-record policy (task #48): a malformed
                // record has already been logged + counted inside
                // `apply_local`; drop the connection so the reconnect loop
                // renegotiates PSYNC instead of continuing to apply against
                // a desynced state.
                ApplyOutcome::Poisoned => {
                    return Err(anyhow::anyhow!(
                        "replication stream: poison record — dropping connection to force resync"
                    ));
                }
                ApplyOutcome::NoShardSlice => {
                    return Err(anyhow::anyhow!(
                        "replica has no ShardSlice on this thread — cannot apply replication \
                         stream"
                    ));
                }
            }
        }
        if outcome.consumed > 0 {
            if let Ok(rs) = cfg.repl_state.read() {
                rs.master_repl_offset
                    .fetch_add(outcome.consumed as u64, Ordering::Relaxed);
            }
        }
        // Persist the drain's db context so a reconnect (+CONTINUE) resumes
        // in the same logical db (HIGH-2, task #22).
        cfg.stream_db.store(selected_db, Ordering::Relaxed);
        if outcome.fatal {
            return Err(anyhow::anyhow!(
                "replication stream parse error — dropping connection to force resync"
            ));
        }
    }
}

/// Entry point for the outbound replica task under monoio runtime.
///
/// Same reconnect loop as tokio variant but uses monoio::net::TcpStream and
/// monoio::time::sleep for backoff.
#[cfg(feature = "runtime-monoio")]
pub async fn run_replica_task(cfg: ReplicaTaskConfig) {
    let addr: std::net::SocketAddr = format!("{}:{}", cfg.master_host, cfg.master_port)
        .parse()
        .expect("invalid master address");
    // R0 streaming replication is single-shard only. A multi-shard replica would
    // misread the master's single diskless RDB bulk and mis-route the command
    // stream (see `apply::load_snapshot`, which is thread-local and clears all
    // dbs per call), silently diverging. Refuse loudly rather than loop forever
    // on a broken sync. Multi-shard replica apply is tracked for R2.
    if cfg.num_shards != 1 {
        tracing::error!(
            "Replica: streaming replication currently supports single-shard only \
             (--shards 1); this node has {} shards. Not starting replication.",
            cfg.num_shards
        );
        return;
    }

    let mut backoff_ms = 500u64;
    const MAX_BACKOFF_MS: u64 = 30_000;

    loop {
        if superseded(cfg.epoch) {
            info!("Replica: task superseded (epoch {}), exiting", cfg.epoch);
            return;
        }
        info!("Replica: connecting to master at {}", addr);
        match monoio::net::TcpStream::connect(addr).await {
            Ok(stream) => {
                backoff_ms = 500;
                match run_handshake_and_stream(stream, &cfg).await {
                    Ok(()) => {
                        info!("Replica: stream ended cleanly, reconnecting...");
                    }
                    Err(e) => {
                        warn!("Replica: stream error: {}, reconnecting...", e);
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Replica: connect to {} failed: {}, retrying in {}ms",
                    addr, e, backoff_ms
                );
            }
        }

        // A superseded task must not clobber the successor's handshake state.
        if superseded(cfg.epoch) {
            info!("Replica: task superseded (epoch {}), exiting", cfg.epoch);
            return;
        }

        if let Ok(mut rs) = cfg.repl_state.write() {
            if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
                *state = ReplicaHandshakeState::Disconnected;
            }
        }

        monoio::time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
    }
}

/// Perform the PSYNC2 handshake with master under monoio, then stream and apply replication data.
#[cfg(feature = "runtime-monoio")]
async fn run_handshake_and_stream(
    mut stream: monoio::net::TcpStream,
    cfg: &ReplicaTaskConfig,
) -> anyhow::Result<()> {
    use crate::protocol::Frame;
    use crate::protocol::serialize;
    use monoio::io::AsyncWriteRentExt;

    let mut write_buf = BytesMut::new();

    // Helper macro: send a RESP Array command using monoio ownership write
    macro_rules! send_cmd {
        ($stream:expr, $buf:expr, $parts:expr) => {{
            let frame = Frame::Array(
                $parts
                    .iter()
                    .map(|p: &&[u8]| Frame::BulkString(Bytes::copy_from_slice(p)))
                    .collect(),
            );
            $buf.clear();
            serialize(&frame, $buf);
            let data = $buf.to_vec();
            let (wr, _) = $stream.write_all(data).await;
            wr.map_err(|e| anyhow::anyhow!(e))
        }};
    }

    // Step 1: PING
    if let Ok(mut rs) = cfg.repl_state.write() {
        if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
            *state = ReplicaHandshakeState::PingSent;
        }
    }
    send_cmd!(&mut stream, &mut write_buf, &[b"PING" as &[u8]])?;
    let response = read_line(&mut stream).await?;
    if !response.starts_with(b"+PONG") && !response.starts_with(b"+pong") {
        anyhow::bail!("Expected PONG, got: {:?}", response);
    }

    // Step 2: REPLCONF listening-port
    let port_str = cfg.listening_port.to_string();
    send_cmd!(
        &mut stream,
        &mut write_buf,
        &[b"REPLCONF" as &[u8], b"listening-port", port_str.as_bytes()]
    )?;
    let _ = read_line(&mut stream).await?;

    // Step 3: REPLCONF capa psync2
    send_cmd!(
        &mut stream,
        &mut write_buf,
        &[b"REPLCONF" as &[u8], b"capa", b"eof", b"capa", b"psync2"]
    )?;
    let _ = read_line(&mut stream).await?;

    // Step 4: PSYNC <repl_id> <offset>
    let (repl_id, offset) = {
        let rs = cfg
            .repl_state
            .read()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        let offset = rs.master_repl_offset.load(Ordering::Relaxed);
        let id = if offset == 0 {
            "?".to_string()
        } else {
            rs.repl_id.clone()
        };
        let off_str = if offset == 0 {
            "-1".to_string()
        } else {
            offset.to_string()
        };
        (id, off_str)
    };
    if let Ok(mut rs) = cfg.repl_state.write() {
        if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
            *state = ReplicaHandshakeState::PsyncPending;
        }
    }
    send_cmd!(
        &mut stream,
        &mut write_buf,
        &[b"PSYNC" as &[u8], repl_id.as_bytes(), offset.as_bytes()]
    )?;

    // Step 5: Parse master response
    let response = read_line(&mut stream).await?;
    if response.starts_with(b"+FULLRESYNC") {
        let parts: Vec<&[u8]> = response[1..].splitn(3, |&b| b == b' ').collect();
        if parts.len() >= 3 {
            let master_id = String::from_utf8_lossy(parts[1]).to_string();
            let master_offset: u64 = std::str::from_utf8(parts[2])
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0);
            if let Ok(mut rs) = cfg.repl_state.write() {
                rs.repl_id = master_id;
                rs.master_repl_offset
                    .store(master_offset, Ordering::Relaxed);
                if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
                    *state = ReplicaHandshakeState::FullResyncLoading {
                        shards_remaining: cfg.num_shards,
                    };
                }
                if let Some(ref dir) = cfg.persistence_dir {
                    let _ = save_replication_state(
                        std::path::Path::new(dir),
                        &rs.repl_id,
                        &rs.repl_id2,
                    );
                }
            }
        }

        // Load the full-resync RDB snapshot into the local shard's databases.
        // R0 = single-shard: the master sends one diskless RDB bulk, loaded into
        // this thread's ShardSlice (clears existing state first — full resync is
        // authoritative). Multi-shard merged-RDB load is R2.
        if superseded(cfg.epoch) {
            anyhow::bail!("replica task superseded before snapshot load");
        }
        for shard_id in 0..cfg.num_shards {
            let rdb_bytes = read_rdb_bulk(&mut stream).await?;
            match crate::replication::apply::load_snapshot(&rdb_bytes, &cfg.shard_databases) {
                Ok(keys) => info!(
                    "Replica: loaded shard {} RDB snapshot ({} bytes, {} keys)",
                    shard_id,
                    rdb_bytes.len(),
                    keys
                ),
                Err(e) => {
                    warn!(
                        "Replica: failed to load shard {} RDB snapshot: {}",
                        shard_id, e
                    );
                    return Err(e);
                }
            }
        }

        if let Ok(mut rs) = cfg.repl_state.write() {
            if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
                *state = ReplicaHandshakeState::Streaming;
            }
        }
        // FULLRESYNC resets the stream's db context: the master reset its own
        // stream-db in the snapshot-capture stretch, so post-snapshot bytes
        // always re-establish it with an explicit SELECT (task #22).
        cfg.stream_db.store(0, Ordering::Relaxed);
        stream_commands(stream, cfg).await?;
    } else if response.starts_with(b"+CONTINUE") {
        if let Ok(mut rs) = cfg.repl_state.write() {
            if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
                *state = ReplicaHandshakeState::Streaming;
            }
        }
        stream_commands(stream, cfg).await?;
    } else {
        anyhow::bail!("Unexpected PSYNC response: {:?}", response);
    }

    Ok(())
}

/// Stream incoming WAL bytes from master under monoio using ownership read.
#[cfg(feature = "runtime-monoio")]
async fn stream_commands(
    stream: monoio::net::TcpStream,
    cfg: &ReplicaTaskConfig,
) -> anyhow::Result<()> {
    // R1 (task #19): acknowledge the applied offset back to the master with
    // `REPLCONF ACK <offset>` so WAIT resolves. Split the socket: a 1s ticker
    // task owns the write half (Redis's replicationCron cadence + idle
    // keepalive for lag detection); this loop keeps the read half. A
    // timeout-wrapped read was rejected instead: cancelling an in-flight
    // io_uring read whose CQE already completed DISCARDS those bytes —
    // silent stream corruption.
    use monoio::io::Splitable as _;
    let (mut rd, wr) = stream.into_split();
    let ack_stop = std::rc::Rc::new(std::cell::Cell::new(false));
    let ack_handle = monoio::spawn({
        let stop = ack_stop.clone();
        let repl_state = cfg.repl_state.clone();
        let mut wr = wr;
        async move {
            use monoio::io::AsyncWriteRentExt;
            loop {
                monoio::time::sleep(std::time::Duration::from_secs(1)).await;
                if stop.get() {
                    break;
                }
                let offset = match repl_state.read() {
                    Ok(rs) => rs.master_repl_offset.load(Ordering::Relaxed),
                    Err(_) => break,
                };
                let (res, _) = wr.write_all(encode_replconf_ack(offset)).await;
                if res.is_err() {
                    break; // socket dead — the read loop is exiting too
                }
            }
        }
    });
    let result = stream_commands_read_loop(&mut rd, cfg).await;
    // Stop the ticker (checked each tick; the write half drops with the task,
    // closing our side of the socket within ~1s of the read loop ending).
    ack_stop.set(true);
    drop(ack_handle);
    result
}

/// The read/apply half of `stream_commands` (monoio).
#[cfg(feature = "runtime-monoio")]
async fn stream_commands_read_loop(
    stream: &mut monoio::net::tcp::TcpOwnedReadHalf,
    cfg: &ReplicaTaskConfig,
) -> anyhow::Result<()> {
    use monoio::io::AsyncReadRent;

    let mut buf = BytesMut::with_capacity(65536);
    // Seeded from the task-level slot (NOT 0): a +CONTINUE resume must keep
    // the db context the stream was in when the link dropped — see
    // `ReplicaTaskConfig::stream_db`.
    let mut selected_db = cfg.stream_db.load(Ordering::Relaxed);

    loop {
        let tmp = vec![0u8; 65536];
        let (result, tmp) = stream.read(tmp).await;
        let n = result?;
        if n == 0 {
            return Err(anyhow::anyhow!("Master closed connection"));
        }
        buf.extend_from_slice(&tmp[..n]);

        // Superseded tasks must never apply another byte — checked after the
        // parked read wakes, before any parse/apply.
        if superseded(cfg.epoch) {
            anyhow::bail!("replica task superseded — dropping stream unapplied");
        }

        // Parse every complete RESP command in the buffer and apply it to the
        // local shard. Offset advances by CONSUMED bytes (whole frames), never
        // the raw read count — a read may split a frame across boundaries.
        let outcome =
            crate::replication::apply::drain_replicated_commands(&mut buf, &mut selected_db);
        for rc in &outcome.commands {
            use crate::replication::apply::ApplyOutcome;
            match crate::replication::apply::apply_local(rc, &cfg.shard_databases) {
                ApplyOutcome::Applied => {}
                // Unified poison-record policy (task #48): a malformed
                // record has already been logged + counted inside
                // `apply_local`; drop the connection so the reconnect loop
                // renegotiates PSYNC instead of continuing to apply against
                // a desynced state.
                ApplyOutcome::Poisoned => {
                    return Err(anyhow::anyhow!(
                        "replication stream: poison record — dropping connection to force resync"
                    ));
                }
                ApplyOutcome::NoShardSlice => {
                    return Err(anyhow::anyhow!(
                        "replica has no ShardSlice on this thread — cannot apply replication \
                         stream"
                    ));
                }
            }
        }
        if outcome.consumed > 0 {
            if let Ok(rs) = cfg.repl_state.read() {
                rs.master_repl_offset
                    .fetch_add(outcome.consumed as u64, Ordering::Relaxed);
            }
        }
        // Persist the drain's db context so a reconnect (+CONTINUE) resumes
        // in the same logical db (HIGH-2, task #22).
        cfg.stream_db.store(selected_db, Ordering::Relaxed);
        if outcome.fatal {
            return Err(anyhow::anyhow!(
                "replication stream parse error — dropping connection to force resync"
            ));
        }
    }
}

/// Read a single line (up to CRLF) from a monoio TcpStream using ownership read.
///
/// Reads one byte at a time which is acceptable for the PSYNC handshake
/// (called ~5 times total, not in the hot path).
#[cfg(feature = "runtime-monoio")]
async fn read_line(stream: &mut monoio::net::TcpStream) -> anyhow::Result<Vec<u8>> {
    use monoio::io::AsyncReadRent;

    let mut line = Vec::new();
    loop {
        let buf = vec![0u8; 1];
        let (result, buf) = stream.read(buf).await;
        let n = result?;
        if n == 0 {
            anyhow::bail!("EOF during read_line");
        }
        let byte = buf[0];
        if byte == b'\n' && line.last() == Some(&b'\r') {
            line.pop();
            return Ok(line);
        }
        line.push(byte);
        if line.len() > 4096 {
            anyhow::bail!("Line too long in replication handshake");
        }
    }
}

/// Read a diskless full-resync RDB payload (`$<len>\r\n<len bytes>`) from a
/// monoio TcpStream.
///
/// Unlike a normal RESP bulk string, the master's full-resync RDB is NOT
/// terminated with a trailing `\r\n` (diskless wire format — the bytes right
/// after the RDB are the live replication command stream). Reading `len + 2`
/// would steal the first two bytes of that stream and desync every command
/// after it, so this reads EXACTLY `len` bytes and stops.
#[cfg(feature = "runtime-monoio")]
async fn read_rdb_bulk(stream: &mut monoio::net::TcpStream) -> anyhow::Result<Vec<u8>> {
    use monoio::io::AsyncReadRent;

    let len_line = read_line(stream).await?;
    if len_line.first() != Some(&b'$') {
        anyhow::bail!("Expected bulk string, got: {:?}", len_line);
    }
    let len: usize = std::str::from_utf8(&len_line[1..])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("Invalid bulk length"))?;
    // Guard against a corrupted/hostile length header driving a huge allocation
    // (the normal RESP parser's bulk cap is bypassed here for the diskless
    // no-CRLF format). 64 GiB is far above any legitimate snapshot.
    const MAX_RDB_BYTES: usize = 64 * 1024 * 1024 * 1024;
    if len > MAX_RDB_BYTES {
        anyhow::bail!("RDB bulk length {len} exceeds sanity cap {MAX_RDB_BYTES}");
    }
    let mut data = Vec::with_capacity(len);
    while data.len() < len {
        let remaining = len - data.len();
        let tmp = vec![0u8; remaining];
        let (result, tmp) = stream.read(tmp).await;
        let n = result?;
        if n == 0 {
            anyhow::bail!("EOF during RDB read");
        }
        data.extend_from_slice(&tmp[..n]);
    }
    Ok(data)
}

/// Read a single line (up to CRLF) from the stream.
#[cfg(feature = "runtime-tokio")]
async fn read_line(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).await?;
        if byte[0] == b'\n' && line.last() == Some(&b'\r') {
            line.pop(); // remove \r
            return Ok(line);
        }
        line.push(byte[0]);
        if line.len() > 4096 {
            anyhow::bail!("Line too long in replication handshake");
        }
    }
}

/// Read a diskless full-resync RDB payload (`$<len>\r\n<len bytes>`).
///
/// The master's full-resync RDB has NO trailing `\r\n` (diskless wire format);
/// reading `len + 2` would steal the first two bytes of the live command stream
/// that follows. Reads EXACTLY `len` bytes.
#[cfg(feature = "runtime-tokio")]
async fn read_rdb_bulk(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let len_line = read_line(stream).await?;
    if len_line.first() != Some(&b'$') {
        anyhow::bail!("Expected bulk string, got: {:?}", len_line);
    }
    let len: usize = std::str::from_utf8(&len_line[1..])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("Invalid bulk length"))?;
    const MAX_RDB_BYTES: usize = 64 * 1024 * 1024 * 1024;
    if len > MAX_RDB_BYTES {
        anyhow::bail!("RDB bulk length {len} exceeds sanity cap {MAX_RDB_BYTES}");
    }
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;
    Ok(data)
}

/// RESP-serialize `REPLCONF ACK <offset>` for the replication link (R1).
fn encode_replconf_ack(offset: u64) -> Vec<u8> {
    let mut n = itoa::Buffer::new();
    let off = n.format(offset);
    let mut ln = itoa::Buffer::new();
    let mut buf = Vec::with_capacity(48);
    buf.extend_from_slice(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$");
    buf.extend_from_slice(ln.format(off.len()).as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(off.as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf
}
