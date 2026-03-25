//! Outbound replica connection task.
//!
//! Spawned via tokio::spawn when REPLICAOF host port is executed.
//! Performs the PSYNC2 handshake with the master, then enters streaming mode
//! where it reads WAL bytes from the master and dispatches them as commands.
#![allow(unused_imports)]

use bytes::{Bytes, BytesMut};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
#[cfg(feature = "runtime-tokio")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "runtime-tokio")]
use tokio::net::TcpStream;
use std::time::Duration;
use tracing::{info, warn};

use crate::replication::handshake::ReplicaHandshakeState;
use crate::replication::state::{save_replication_state, ReplicationRole, ReplicationState};

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
    use crate::protocol::serialize;
    use crate::protocol::Frame;

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
        &[
            b"REPLCONF" as &[u8],
            b"listening-port",
            port_str.as_bytes()
        ]
    )?;
    let _ = read_line(&mut stream).await?; // +OK

    // Step 3: REPLCONF capa psync2
    send_cmd!(
        &mut stream,
        &mut write_buf,
        &[
            b"REPLCONF" as &[u8],
            b"capa",
            b"eof",
            b"capa",
            b"psync2"
        ]
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
                    let _ =
                        save_replication_state(std::path::Path::new(dir), &rs.repl_id, &rs.repl_id2);
                }
            }
        }

        // Read N per-shard RDB bulk strings ($<len>\r\n<bytes>\r\n)
        // Simplified for Phase 19: read and discard RDB bytes (snapshot load via ShardMessage in Plan 04)
        for shard_id in 0..cfg.num_shards {
            let rdb_bytes = read_bulk_string(&mut stream).await?;
            info!(
                "Replica: received shard {} RDB snapshot ({} bytes)",
                shard_id,
                rdb_bytes.len()
            );
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
async fn stream_commands(
    stream: &mut TcpStream,
    cfg: &ReplicaTaskConfig,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(65536);

    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(anyhow::anyhow!("Master closed connection"));
        }

        // Update local replication offset
        if let Ok(rs) = cfg.repl_state.read() {
            rs.master_repl_offset
                .fetch_add(n as u64, Ordering::Relaxed);
        }

        // Parse and dispatch commands from the received bytes
        // Full RESP parsing and per-key dispatch routing is added in Plan 04.
        // For now, buffer is accumulated and will be wired in Plan 04.
        buf.clear();
    }
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

/// Read a RESP bulk string ($<len>\r\n<bytes>\r\n).
#[cfg(feature = "runtime-tokio")]
async fn read_bulk_string(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let len_line = read_line(stream).await?;
    if len_line.first() != Some(&b'$') {
        anyhow::bail!("Expected bulk string, got: {:?}", len_line);
    }
    let len: usize = std::str::from_utf8(&len_line[1..])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("Invalid bulk length"))?;
    let mut data = vec![0u8; len + 2]; // +2 for trailing \r\n
    stream.read_exact(&mut data).await?;
    data.truncate(len);
    Ok(data)
}
