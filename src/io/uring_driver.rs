//! Per-shard io_uring driver for Linux.
//!
//! Owns one io_uring instance per shard thread with optimal flags:
//! `SINGLE_ISSUER` + `DEFER_TASKRUN` + `COOP_TASKRUN`.
//!
//! Provides:
//! - Multishot accept (single SQE produces CQEs for each new connection)
//! - Multishot recv with provided buffer ring (kernel-selected buffers)
//! - Registered file descriptor table (Fixed FD ops)
//! - SQE batching (accumulate per iteration, single submit_and_wait)
//! - Writev scatter-gather for zero-copy GET responses
//!
//! This module is only compiled on Linux (cfg-gated in mod.rs).

use std::collections::HashMap;
use std::os::fd::RawFd;

use bytes::BytesMut;
use io_uring::cqueue;
use io_uring::opcode;
use io_uring::squeue::Flags;
use io_uring::types;
use io_uring::IoUring;

use super::buf_ring::{BufRingConfig, BufRingManager};
use super::fd_table::FdTable;
use super::static_responses;
use super::{
    decode_user_data, encode_user_data, EVENT_ACCEPT, EVENT_RECV, EVENT_SEND, EVENT_TIMEOUT,
    EVENT_WAKEUP,
};

/// Default ring size (SQ entries). CQ is 2x automatically.
const DEFAULT_RING_SIZE: u32 = 256;

/// Max connections per shard for FD table sizing.
const DEFAULT_MAX_CONNECTIONS: usize = 1024;

/// Per-connection state tracked by the UringDriver.
struct ConnState {
    /// Fixed FD index in the registered table.
    fixed_fd_idx: u32,
    /// Raw file descriptor.
    raw_fd: RawFd,
    /// Accumulation buffer for partial RESP frames spanning multiple recvs.
    read_buf: BytesMut,
    /// Whether this connection has an active multishot recv.
    recv_active: bool,
}

/// Configuration for UringDriver.
pub struct UringConfig {
    /// io_uring SQ size (CQ will be 2x). Default: 256.
    pub ring_size: u32,
    /// Maximum concurrent connections per shard. Default: 1024.
    pub max_connections: usize,
    /// Provided buffer ring configuration.
    pub buf_ring: BufRingConfig,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            ring_size: DEFAULT_RING_SIZE,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            buf_ring: BufRingConfig::default(),
        }
    }
}

/// Per-shard io_uring driver.
///
/// Owns one io_uring instance with `SINGLE_ISSUER` + `DEFER_TASKRUN` + `COOP_TASKRUN`.
/// Manages connection lifecycle via multishot accept/recv, registered FDs,
/// provided buffer ring, and batched SQE submission.
///
/// # Thread Safety
///
/// NOT `Send` or `Sync` -- must be created and used from a single shard thread
/// (enforced by `SINGLE_ISSUER`).
pub struct UringDriver {
    ring: IoUring,
    fd_table: FdTable,
    buf_ring: BufRingManager,
    connections: HashMap<u32, ConnState>,
    next_conn_id: u32,
    config: UringConfig,
    /// Number of SQEs queued in current batch (not yet submitted).
    pending_sqes: usize,
}

impl UringDriver {
    /// Create a new UringDriver for a shard thread.
    ///
    /// MUST be called from the shard thread that will own this driver
    /// (`SINGLE_ISSUER` flag requires single-thread access).
    pub fn new(config: UringConfig) -> std::io::Result<Self> {
        let ring = IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .build(config.ring_size)?;

        let fd_table = FdTable::new(config.max_connections);
        let buf_ring = BufRingManager::new(config.buf_ring.clone());

        Ok(Self {
            ring,
            fd_table,
            buf_ring,
            connections: HashMap::new(),
            next_conn_id: 0,
            config,
            pending_sqes: 0,
        })
    }

    /// Initialize: register FD table and provided buffer ring with the kernel.
    ///
    /// Must be called once after construction, before any I/O operations.
    pub fn init(&mut self) -> std::io::Result<()> {
        self.fd_table.register_with_ring(&self.ring)?;
        self.buf_ring.setup_ring(&self.ring)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // SQE submission methods
    // -----------------------------------------------------------------------

    /// Submit multishot accept on a listener socket fd.
    ///
    /// The listener fd does NOT need to be in the registered table.
    /// A single SQE continuously produces CQEs for each accepted connection.
    pub fn submit_multishot_accept(&mut self, listener_fd: RawFd) -> std::io::Result<()> {
        let entry = opcode::AcceptMulti::new(types::Fd(listener_fd))
            .build()
            .user_data(encode_user_data(EVENT_ACCEPT, 0, 0));

        unsafe {
            self.ring.submission().push(&entry).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "SQ full: cannot submit accept")
            })?;
        }
        self.pending_sqes += 1;
        Ok(())
    }

    /// Register a new connection (accepted fd) and arm multishot recv.
    ///
    /// Returns the connection ID, or `None` if the FD table is full.
    pub fn register_connection(&mut self, raw_fd: RawFd) -> std::io::Result<Option<u32>> {
        let conn_id = self.next_conn_id;
        self.next_conn_id += 1;

        // Insert into FD table and register with ring
        let fixed_idx = match self.fd_table.insert_and_register(raw_fd, &self.ring)? {
            Some(idx) => idx,
            None => {
                // FD table full -- reject connection (graceful fallback with raw fd
                // could be added later, but for now reject cleanly).
                tracing::warn!(
                    "FD table full ({} connections), rejecting new connection",
                    self.config.max_connections
                );
                unsafe {
                    libc::close(raw_fd);
                }
                return Ok(None);
            }
        };

        self.connections.insert(
            conn_id,
            ConnState {
                fixed_fd_idx: fixed_idx,
                raw_fd,
                read_buf: BytesMut::with_capacity(0), // allocated on-demand for partial frames
                recv_active: false,
            },
        );

        // Arm multishot recv with provided buffer ring
        self.submit_multishot_recv(conn_id, fixed_idx)?;

        Ok(Some(conn_id))
    }

    /// Submit multishot recv for a connection using the provided buffer ring.
    fn submit_multishot_recv(
        &mut self,
        conn_id: u32,
        fixed_fd_idx: u32,
    ) -> std::io::Result<()> {
        let entry =
            opcode::RecvMulti::new(types::Fixed(fixed_fd_idx), self.buf_ring.group_id())
                .build()
                .user_data(encode_user_data(EVENT_RECV, conn_id, 0))
                .flags(Flags::BUFFER_SELECT);

        unsafe {
            self.ring.submission().push(&entry).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "SQ full: cannot submit recv")
            })?;
        }
        self.pending_sqes += 1;

        if let Some(conn) = self.connections.get_mut(&conn_id) {
            conn.recv_active = true;
        }

        Ok(())
    }

    /// Submit writev for scatter-gather response (zero-copy GET path).
    ///
    /// The `iovecs` pointer and referenced data must live until the CQE arrives.
    /// In a single-threaded shard, this is guaranteed because CQEs are processed
    /// before the next event loop iteration.
    pub fn submit_writev(
        &mut self,
        conn_id: u32,
        iovecs: *const libc::iovec,
        iovec_count: u32,
    ) -> std::io::Result<()> {
        let conn = self.connections.get(&conn_id).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "connection not found")
        })?;

        let entry = opcode::Writev::new(
            types::Fixed(conn.fixed_fd_idx),
            iovecs,
            iovec_count,
        )
        .build()
        .user_data(encode_user_data(EVENT_SEND, conn_id, 0));

        unsafe {
            self.ring.submission().push(&entry).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "SQ full: cannot submit writev")
            })?;
        }
        self.pending_sqes += 1;
        Ok(())
    }

    /// Submit a single send with raw bytes.
    pub fn submit_send(
        &mut self,
        conn_id: u32,
        data: *const u8,
        len: u32,
    ) -> std::io::Result<()> {
        let conn = self.connections.get(&conn_id).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "connection not found")
        })?;

        let entry = opcode::Send::new(types::Fixed(conn.fixed_fd_idx), data, len)
            .build()
            .user_data(encode_user_data(EVENT_SEND, conn_id, 0));

        unsafe {
            self.ring.submission().push(&entry).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "SQ full: cannot submit send")
            })?;
        }
        self.pending_sqes += 1;
        Ok(())
    }

    /// Submit a timeout for periodic tasks (expiry cycle, SPSC drain).
    pub fn submit_timeout_ms(&mut self, ms: u64) -> std::io::Result<()> {
        let ts = types::Timespec::new()
            .nsec(((ms % 1000) * 1_000_000) as u32)
            .sec(ms / 1000);

        let entry = opcode::Timeout::new(&ts as *const _)
            .build()
            .user_data(encode_user_data(EVENT_TIMEOUT, 0, 0));

        unsafe {
            self.ring.submission().push(&entry).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "SQ full: cannot submit timeout",
                )
            })?;
        }
        self.pending_sqes += 1;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // SQE batching and CQE processing
    // -----------------------------------------------------------------------

    /// Submit all accumulated SQEs and wait for at least one CQE.
    ///
    /// This is the "flush" point -- called once per event loop iteration.
    /// All SQEs queued via `submit_*` methods are submitted in a single
    /// `io_uring_submit()` syscall.
    pub fn submit_and_wait(&mut self) -> std::io::Result<usize> {
        if self.pending_sqes == 0 {
            // Nothing to submit, but still check for CQEs
            return self.ring.submit_and_wait(1).map_err(Into::into);
        }
        let n = self.ring.submit_and_wait(1)?;
        self.pending_sqes = 0;
        Ok(n)
    }

    /// Submit pending SQEs without waiting for completions (non-blocking).
    ///
    /// Used in the hybrid Tokio+io_uring path where the shard event loop
    /// polls io_uring completions on a timer rather than blocking.
    pub fn submit_and_wait_nonblocking(&mut self) -> std::io::Result<usize> {
        if self.pending_sqes == 0 {
            return Ok(0);
        }
        let n = self.ring.submit()?;
        self.pending_sqes = 0;
        Ok(n)
    }

    /// Drain all completed CQEs, returning events for the caller to process.
    ///
    /// The caller (shard event loop) handles command dispatch based on event type.
    /// Buffer lifecycle: recv data is copied from the provided buffer before
    /// the buffer is returned to the ring (per pitfall 1 in research).
    pub fn drain_completions(&mut self) -> Vec<IoEvent> {
        let mut events = Vec::new();

        for cqe in self.ring.completion() {
            let (event_type, conn_id, _aux) = decode_user_data(cqe.user_data());
            let result = cqe.result();
            let flags = cqe.flags();

            match event_type {
                EVENT_ACCEPT => {
                    if result >= 0 {
                        events.push(IoEvent::Accept { raw_fd: result });
                    } else if result != -(libc::ECANCELED as i32) {
                        // Transient error -- caller should re-submit multishot accept
                        // (per pitfall 4: multishot accept cancelled on error)
                        tracing::warn!(
                            "Accept error: {}",
                            std::io::Error::from_raw_os_error(-result)
                        );
                        events.push(IoEvent::AcceptError { errno: -result });
                    }
                    // ECANCELED means accept was intentionally cancelled (shutdown)
                }
                EVENT_RECV => {
                    if result > 0 {
                        // Extract buffer ID from CQE flags (IORING_CQE_F_BUFFER)
                        let buf_id = cqueue::buffer_select(flags).unwrap_or(0);
                        let len = result as usize;

                        // Copy data from provided buffer BEFORE returning it
                        // (per pitfall 1: parse frame and copy data before returning buffer)
                        let data = self.buf_ring.get_buf(buf_id, len).to_vec();
                        self.buf_ring.mark_in_use(buf_id);

                        // Return buffer immediately since data is copied
                        let _ = self.buf_ring.return_buf(&self.ring, buf_id);

                        events.push(IoEvent::Recv { conn_id, data });

                        // Check if multishot recv was cancelled (MORE flag absent)
                        if flags & cqueue::more() == 0 {
                            if let Some(conn) = self.connections.get_mut(&conn_id) {
                                conn.recv_active = false;
                            }
                            events.push(IoEvent::RecvNeedsRearm { conn_id });
                        }
                    } else if result == 0 {
                        // Connection closed by peer
                        events.push(IoEvent::Disconnect { conn_id });
                    } else {
                        // Error on recv
                        let errno = -result;
                        if errno == libc::ENOBUFS as i32 {
                            // Buffer ring exhausted -- rearm after buffers are returned
                            events.push(IoEvent::RecvNeedsRearm { conn_id });
                        } else {
                            events.push(IoEvent::Disconnect { conn_id });
                        }
                    }
                }
                EVENT_SEND => {
                    if result < 0 {
                        events.push(IoEvent::SendError {
                            conn_id,
                            errno: -result,
                        });
                    }
                    events.push(IoEvent::SendComplete { conn_id });
                }
                EVENT_TIMEOUT => {
                    events.push(IoEvent::Timeout);
                }
                EVENT_WAKEUP => {
                    events.push(IoEvent::Wakeup);
                }
                _ => {
                    tracing::warn!("Unknown io_uring event type: {}", event_type);
                }
            }
        }

        events
    }

    // -----------------------------------------------------------------------
    // Connection management
    // -----------------------------------------------------------------------

    /// Close a connection: unregister FD, close socket, remove state.
    pub fn close_connection(&mut self, conn_id: u32) -> std::io::Result<()> {
        if let Some(conn) = self.connections.remove(&conn_id) {
            let raw_fd =
                self.fd_table
                    .remove_and_register(conn.fixed_fd_idx, &self.ring)?;
            unsafe {
                libc::close(raw_fd);
            }
        }
        Ok(())
    }

    /// Get mutable reference to a connection's read buffer (for partial frame accumulation).
    pub fn conn_read_buf(&mut self, conn_id: u32) -> Option<&mut BytesMut> {
        self.connections.get_mut(&conn_id).map(|c| &mut c.read_buf)
    }

    /// Re-arm multishot recv for a connection (after cancellation or ENOBUFS).
    pub fn rearm_recv(&mut self, conn_id: u32) -> std::io::Result<()> {
        if let Some(conn) = self.connections.get(&conn_id) {
            if !conn.recv_active {
                let fixed_idx = conn.fixed_fd_idx;
                self.submit_multishot_recv(conn_id, fixed_idx)?;
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Number of active connections.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Number of pending (not yet submitted) SQEs.
    pub fn pending_sqe_count(&self) -> usize {
        self.pending_sqes
    }

    /// Reference to the FD table.
    pub fn fd_table(&self) -> &FdTable {
        &self.fd_table
    }

    /// Reference to the buffer ring manager.
    pub fn buf_ring(&self) -> &BufRingManager {
        &self.buf_ring
    }
}

// ---------------------------------------------------------------------------
// IoEvent: CQE routing enum for the shard event loop
// ---------------------------------------------------------------------------

/// Events produced by `drain_completions` for the shard event loop to process.
///
/// Each variant corresponds to a CQE completion type. The shard event loop
/// matches on these to dispatch connection handling, command execution, etc.
#[derive(Debug)]
pub enum IoEvent {
    /// New connection accepted. `raw_fd` is the socket fd to register.
    Accept { raw_fd: i32 },
    /// Accept error (transient). Multishot accept may need re-submission.
    AcceptError { errno: i32 },
    /// Data received on connection. `data` is copied from provided buffer.
    Recv { conn_id: u32, data: Vec<u8> },
    /// Multishot recv cancelled or ENOBUFS. Must call `rearm_recv`.
    RecvNeedsRearm { conn_id: u32 },
    /// Connection closed by peer (recv returned 0 or fatal error).
    Disconnect { conn_id: u32 },
    /// Send completed successfully.
    SendComplete { conn_id: u32 },
    /// Send failed.
    SendError { conn_id: u32, errno: i32 },
    /// Timeout expired (periodic task trigger).
    Timeout,
    /// Wakeup event (eventfd for SPSC channel notification).
    Wakeup,
}

// ---------------------------------------------------------------------------
// Scatter-gather helpers for zero-copy GET responses
// ---------------------------------------------------------------------------

/// Build iovec array for scatter-gather GET response.
///
/// Assembles a RESP bulk string response without copying the value:
/// - `iovec[0]`: `$<len>\r\n` header (written into `header_buf`)
/// - `iovec[1]`: value bytes (direct reference from DashTable -- zero-copy)
/// - `iovec[2]`: `\r\n` trailer (static)
///
/// Returns `(iovec_array, iovec_count)`. The `header_buf` must outlive the iovecs.
pub fn build_get_response_iovecs(
    value: &[u8],
    header_buf: &mut [u8; 32],
) -> ([libc::iovec; 3], usize) {
    let header_len = static_responses::bulk_string_header(value.len(), header_buf);

    let iovecs = [
        libc::iovec {
            iov_base: header_buf.as_ptr() as *mut libc::c_void,
            iov_len: header_len,
        },
        libc::iovec {
            iov_base: value.as_ptr() as *mut libc::c_void,
            iov_len: value.len(),
        },
        libc::iovec {
            iov_base: static_responses::CRLF.as_ptr() as *mut libc::c_void,
            iov_len: 2,
        },
    ];
    (iovecs, 3)
}
