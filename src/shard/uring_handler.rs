//! io_uring event handlers for Linux + tokio runtime.
//!
//! Extracted from shard/mod.rs. Contains handle_uring_event, send_serialized,
//! and create_reuseport_listener.

#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use std::sync::Arc;

#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use crate::command::{DispatchResult, dispatch as cmd_dispatch};
#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use crate::io::{IoEvent, UringDriver, WritevGuard};
#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use crate::storage::entry::CachedClock;

#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use super::shared_databases::ShardDatabases;

#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use super::spsc_handler::extract_command_static;

/// In-flight send buffer variants for proper RAII lifetime management (Linux + tokio only).
///
/// Keeps buffers alive until the corresponding io_uring SendComplete CQE arrives,
/// replacing the previous std::mem::forget memory leak.
#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
#[allow(dead_code)] // Fields hold buffers alive for RAII until SendComplete CQE
pub(crate) enum InFlightSend {
    /// Serialized response buffer for non-BulkString frames (heap fallback).
    Buf(bytes::BytesMut),
    /// Scatter-gather writev guard for BulkString (zero-copy GET) responses.
    Writev(WritevGuard),
    /// Pre-registered fixed buffer index from SendBufPool.
    /// Buffer is reclaimed to pool on SendComplete (no heap alloc/free).
    Fixed(u16),
}

/// Send a serialized response buffer via io_uring.
///
/// Tries the pre-registered fixed buffer pool first (zero get_user_pages overhead).
/// If the pool is exhausted or the response is too large for a pooled buffer,
/// falls back to heap-allocated BytesMut with regular submit_send.
#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
pub(crate) fn send_serialized(
    driver: &mut UringDriver,
    conn_id: u32,
    resp_buf: bytes::BytesMut,
    inflight_sends: &mut std::collections::HashMap<u32, Vec<InFlightSend>>,
) {
    let resp_len = resp_buf.len();
    // Try pooled fixed buffer: must fit in pool buffer size
    if let Some((buf_idx, pool_buf)) = driver.alloc_send_buf() {
        if resp_len <= pool_buf.len() {
            pool_buf[..resp_len].copy_from_slice(&resp_buf);
            let _ = driver.submit_send_fixed(conn_id, buf_idx, resp_len as u32);
            inflight_sends
                .entry(conn_id)
                .or_default()
                .push(InFlightSend::Fixed(buf_idx));
            return;
        }
        // Response too large for pooled buffer -- reclaim and fall through to heap
        driver.reclaim_send_buf(buf_idx);
    }
    // Fallback: heap-allocated BytesMut with regular send
    let len = resp_len as u32;
    let ptr = resp_buf.as_ptr();
    let _ = driver.submit_send(conn_id, ptr, len);
    inflight_sends
        .entry(conn_id)
        .or_default()
        .push(InFlightSend::Buf(resp_buf));
}

/// Handles recv (parse RESP frames + execute commands + send responses),
/// disconnect, recv rearm, accept, and send completion events.
/// Command dispatch reuses the same `extract_command_static` + `cmd_dispatch`
/// path as the Tokio connection handler.
#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
pub(crate) fn handle_uring_event(
    event: IoEvent,
    driver: &mut UringDriver,
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    parse_bufs: &mut std::collections::HashMap<u32, bytes::BytesMut>,
    inflight_sends: &mut std::collections::HashMap<u32, Vec<InFlightSend>>,
    uring_listener_fd: Option<std::os::fd::RawFd>,
    cached_clock: &CachedClock,
) {
    match event {
        IoEvent::Recv { conn_id, data } => {
            let parse_buf = parse_bufs
                .entry(conn_id)
                .or_insert_with(|| bytes::BytesMut::with_capacity(4096));
            parse_buf.extend_from_slice(&data);

            // Phase A: Parse all frames into a batch (up to 1024, matching Tokio path MAX_BATCH).
            let parse_config = crate::protocol::ParseConfig::default();
            let mut batch: Vec<crate::protocol::Frame> = Vec::with_capacity(16);
            let mut parse_error = false;
            loop {
                match crate::protocol::parse(parse_buf, &parse_config) {
                    Ok(Some(frame)) => {
                        batch.push(frame);
                        if batch.len() >= 1024 {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(crate::protocol::ParseError::Incomplete) => break,
                    Err(_) => {
                        parse_error = true;
                        break;
                    }
                }
            }

            if parse_error {
                // Protocol error: close connection, reclaim pooled buffers
                if let Some(sends) = inflight_sends.remove(&conn_id) {
                    for send in sends {
                        if let InFlightSend::Fixed(idx) = send {
                            driver.reclaim_send_buf(idx);
                        }
                    }
                }
                let _ = driver.close_connection(conn_id);
                parse_bufs.remove(&conn_id);
                return;
            }

            if batch.is_empty() {
                return;
            }

            // Phase B: Dispatch all commands under a single write lock.
            let responses: Vec<crate::protocol::Frame> = {
                let mut guard = shard_databases.write_db(shard_id, 0);
                let db_count = shard_databases.db_count();
                guard.refresh_now_from_cache(cached_clock);
                let mut selected = 0usize;
                let result: Vec<_> = batch
                    .iter()
                    .map(|frame| {
                        let (cmd, args) = match extract_command_static(frame) {
                            Some(pair) => pair,
                            None => {
                                return crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                    b"ERR invalid command",
                                ));
                            }
                        };
                        let result = cmd_dispatch(&mut guard, cmd, args, &mut selected, db_count);
                        match result {
                            DispatchResult::Response(f) => f,
                            DispatchResult::Quit(f) => f,
                        }
                    })
                    .collect();
                drop(guard);
                result
            };

            // Phase C: Serialize and send all responses (outside borrow).
            for response in responses {
                match response {
                    crate::protocol::Frame::BulkString(ref value) if !value.is_empty() => {
                        // Zero-copy path: scatter-gather via writev
                        match driver.submit_writev_bulkstring(conn_id, value.clone()) {
                            Ok(guard) => {
                                inflight_sends
                                    .entry(conn_id)
                                    .or_default()
                                    .push(InFlightSend::Writev(guard));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "writev failed for conn {}: {}, falling back to send",
                                    conn_id,
                                    e
                                );
                                let mut resp_buf = bytes::BytesMut::new();
                                crate::protocol::serialize(&response, &mut resp_buf);
                                send_serialized(driver, conn_id, resp_buf, inflight_sends);
                            }
                        }
                    }
                    crate::protocol::Frame::PreSerialized(ref data) if !data.is_empty() => {
                        // Zero-copy path for PreSerialized: already RESP wire format
                        match driver.submit_send_preserialized(conn_id, data.clone()) {
                            Ok(guard) => {
                                inflight_sends
                                    .entry(conn_id)
                                    .or_default()
                                    .push(InFlightSend::Writev(guard));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "writev preserialized failed for conn {}: {}, falling back",
                                    conn_id,
                                    e
                                );
                                let mut resp_buf = bytes::BytesMut::new();
                                crate::protocol::serialize(&response, &mut resp_buf);
                                send_serialized(driver, conn_id, resp_buf, inflight_sends);
                            }
                        }
                    }
                    _ => {
                        let mut resp_buf = bytes::BytesMut::new();
                        crate::protocol::serialize(&response, &mut resp_buf);
                        send_serialized(driver, conn_id, resp_buf, inflight_sends);
                    }
                }
            }
        }
        IoEvent::Disconnect { conn_id } => {
            // Reclaim all pooled Fixed buffers before removing the connection
            if let Some(sends) = inflight_sends.remove(&conn_id) {
                for send in sends {
                    if let InFlightSend::Fixed(idx) = send {
                        driver.reclaim_send_buf(idx);
                    }
                }
            }
            // Graceful close: shutdown(SHUT_WR) sends TCP FIN to peer before close().
            // redis-benchmark 8.x requires FIN (not RST) to detect benchmark completion.
            let _ = driver.shutdown_and_close_connection(conn_id);
            parse_bufs.remove(&conn_id);
        }
        IoEvent::RecvNeedsRearm { conn_id } => {
            let _ = driver.rearm_recv(conn_id);
        }
        IoEvent::Accept { raw_fd } => {
            // Direct accept from multishot (if listener fd is registered)
            let _ = driver.register_connection(raw_fd);
        }
        IoEvent::AcceptError { .. } => {
            // Multishot accept cancelled on error -- re-submit
            if let Some(lfd) = uring_listener_fd {
                let _ = driver.submit_multishot_accept(lfd);
            }
        }
        IoEvent::SendComplete { conn_id } => {
            // Drop the oldest in-flight send buffer (FIFO order matches CQE order).
            if let Some(sends) = inflight_sends.get_mut(&conn_id) {
                if !sends.is_empty() {
                    let send = sends.remove(0);
                    if let InFlightSend::Fixed(idx) = send {
                        driver.reclaim_send_buf(idx);
                    }
                }
                if sends.is_empty() {
                    inflight_sends.remove(&conn_id);
                }
            }
        }
        IoEvent::SendError { conn_id, .. } => {
            // Reclaim all pooled Fixed buffers before cleanup
            if let Some(sends) = inflight_sends.remove(&conn_id) {
                for send in sends {
                    if let InFlightSend::Fixed(idx) = send {
                        driver.reclaim_send_buf(idx);
                    }
                }
            }
            let _ = driver.close_connection(conn_id);
            parse_bufs.remove(&conn_id);
        }
        IoEvent::Timeout | IoEvent::Wakeup => {
            // Handled by the spsc_interval tick (already draining SPSC)
        }
    }
}

/// Create an SO_REUSEPORT TCP listener socket for per-shard multishot accept.
///
/// Delegates to `conn_accept::create_reuseport_socket` (socket2-based) and
/// converts the resulting `std::net::TcpListener` into a `RawFd` for io_uring
/// multishot accept.
#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
pub(crate) fn create_reuseport_listener(addr: &str) -> std::io::Result<std::os::fd::RawFd> {
    use std::os::unix::io::IntoRawFd;
    let std_listener = super::conn_accept::create_reuseport_socket(addr)?;
    Ok(std_listener.into_raw_fd())
}
