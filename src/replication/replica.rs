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

/// Configuration for the replica outbound connection task.
pub struct ReplicaTaskConfig {
    pub master_host: String,
    pub master_port: u16,
    pub repl_state: Arc<RwLock<ReplicationState>>,
    pub num_shards: usize,
    pub persistence_dir: Option<String>,
    pub listening_port: u16,
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
        for shard_id in 0..cfg.num_shards {
            let rdb_bytes = read_rdb_bulk(&mut stream).await?;
            match crate::replication::apply::load_snapshot(&rdb_bytes) {
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
        stream_commands(&mut stream, cfg).await?;
    } else if response.starts_with(b"+CONTINUE") {
        // Partial resync: stream from current offset
        if let Ok(mut rs) = cfg.repl_state.write() {
            if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
                *state = ReplicaHandshakeState::Streaming;
            }
        }
        stream_commands(&mut stream, cfg).await?;
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
async fn stream_commands(stream: &mut TcpStream, cfg: &ReplicaTaskConfig) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(65536);
    let mut selected_db = 0usize;

    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(anyhow::anyhow!("Master closed connection"));
        }

        // Parse every complete RESP command in the buffer and apply it to the
        // local shard. The replication offset advances by CONSUMED bytes (whole
        // frames), never the raw socket read count — a read may split a frame.
        let outcome =
            crate::replication::apply::drain_replicated_commands(&mut buf, &mut selected_db);
        for rc in &outcome.commands {
            if !crate::replication::apply::apply_local(rc) {
                return Err(anyhow::anyhow!(
                    "replica has no ShardSlice on this thread — cannot apply replication stream"
                ));
            }
        }
        if outcome.consumed > 0 {
            if let Ok(rs) = cfg.repl_state.read() {
                rs.master_repl_offset
                    .fetch_add(outcome.consumed as u64, Ordering::Relaxed);
            }
        }
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
        for shard_id in 0..cfg.num_shards {
            let rdb_bytes = read_rdb_bulk(&mut stream).await?;
            match crate::replication::apply::load_snapshot(&rdb_bytes) {
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
        stream_commands(&mut stream, cfg).await?;
    } else if response.starts_with(b"+CONTINUE") {
        if let Ok(mut rs) = cfg.repl_state.write() {
            if let ReplicationRole::Replica { ref mut state, .. } = rs.role {
                *state = ReplicaHandshakeState::Streaming;
            }
        }
        stream_commands(&mut stream, cfg).await?;
    } else {
        anyhow::bail!("Unexpected PSYNC response: {:?}", response);
    }

    Ok(())
}

/// Stream incoming WAL bytes from master under monoio using ownership read.
#[cfg(feature = "runtime-monoio")]
async fn stream_commands(
    stream: &mut monoio::net::TcpStream,
    cfg: &ReplicaTaskConfig,
) -> anyhow::Result<()> {
    use monoio::io::AsyncReadRent;

    let mut buf = BytesMut::with_capacity(65536);
    let mut selected_db = 0usize;

    loop {
        let tmp = vec![0u8; 65536];
        let (result, tmp) = stream.read(tmp).await;
        let n = result?;
        if n == 0 {
            return Err(anyhow::anyhow!("Master closed connection"));
        }
        buf.extend_from_slice(&tmp[..n]);

        // Parse every complete RESP command in the buffer and apply it to the
        // local shard. Offset advances by CONSUMED bytes (whole frames), never
        // the raw read count — a read may split a frame across boundaries.
        let outcome =
            crate::replication::apply::drain_replicated_commands(&mut buf, &mut selected_db);
        for rc in &outcome.commands {
            if !crate::replication::apply::apply_local(rc) {
                return Err(anyhow::anyhow!(
                    "replica has no ShardSlice on this thread — cannot apply replication stream"
                ));
            }
        }
        if outcome.consumed > 0 {
            if let Ok(rs) = cfg.repl_state.read() {
                rs.master_repl_offset
                    .fetch_add(outcome.consumed as u64, Ordering::Relaxed);
            }
        }
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
