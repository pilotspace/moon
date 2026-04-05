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
use io_uring::IoUring;
use io_uring::cqueue;
use io_uring::opcode;
use io_uring::squeue::Flags;
use io_uring::types;

use super::buf_ring::{BufRingConfig, BufRingManager};
use super::fd_table::FdTable;
use super::static_responses;
use super::{
    EVENT_ACCEPT, EVENT_RECV, EVENT_SEND, EVENT_TIMEOUT, EVENT_WAKEUP, decode_user_data,
    encode_user_data,
};

/// Default ring size (SQ entries). CQ is 2x automatically.
const DEFAULT_RING_SIZE: u32 = 256;

/// Max connections per shard for FD table sizing.
const DEFAULT_MAX_CONNECTIONS: usize = 1024;

/// Per-connection state tracked by the UringDriver.
struct ConnState {
    /// Fixed FD index in the registered table.
    fixed_fd_idx: u32,
    /// Raw file descriptor (kept for diagnostic use; graceful shutdown retrieves from fd_table).
    _raw_fd: RawFd,
    /// Accumulation buffer for partial RESP frames spanning multiple recvs.
    read_buf: BytesMut,
    /// Whether this connection has an active multishot recv.
    recv_active: bool,
    /// Monotonic tick counter at last recv activity (for idle reaping).
    last_recv_tick: u64,
}

/// Default number of pre-registered send buffers per shard.
const DEFAULT_SEND_BUF_POOL_SIZE: u16 = 256;

/// Default size of each registered send buffer (8KB, enough for most RESP responses).
const DEFAULT_SEND_BUF_SIZE: usize = 8192;

/// Configuration for UringDriver.
pub struct UringConfig {
    /// io_uring SQ size (CQ will be 2x). Default: 256.
    pub ring_size: u32,
    /// Maximum concurrent connections per shard. Default: 1024.
    pub max_connections: usize,
    /// Provided buffer ring configuration.
    pub buf_ring: BufRingConfig,
    /// Number of pre-registered send buffers. Default: 256 (= 2MB per shard).
    pub send_buf_pool_size: u16,
    /// Enable SQPOLL mode with the given idle timeout in milliseconds.
    ///
    /// When set, the kernel spins a dedicated SQ poll thread that submits SQEs
    /// without requiring `io_uring_enter()` syscalls, reducing submission latency.
    /// Requires `CAP_SYS_NICE` or root; falls back gracefully on EPERM.
    pub sqpoll_idle_ms: Option<u32>,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            ring_size: DEFAULT_RING_SIZE,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            buf_ring: BufRingConfig::default(),
            send_buf_pool_size: DEFAULT_SEND_BUF_POOL_SIZE,
            sqpoll_idle_ms: None,
        }
    }
}

// ---------------------------------------------------------------------------
// SendBufPool: pre-registered send buffer pool for WRITE_FIXED
// ---------------------------------------------------------------------------

/// Pool of pre-allocated buffers registered with io_uring for WRITE_FIXED.
///
/// Eliminates per-response `get_user_pages()` kernel overhead by using
/// `IORING_REGISTER_BUFFERS` once at init. Each send uses `IORING_OP_WRITE_FIXED`
/// with a buffer index, avoiding the per-I/O page pinning cost.
///
/// LIFO free-list for cache-hot buffer reuse. When exhausted, callers fall back
/// to the heap-allocated `submit_send` path.
pub struct SendBufPool {
    /// The actual buffer storage. Each Vec<u8> is a fixed-size buffer.
    /// Indices correspond to the registered buffer indices.
    buffers: Vec<Vec<u8>>,
    /// Stack of free buffer indices (LIFO for cache locality).
    free_list: Vec<u16>,
    /// Total number of buffers in the pool.
    buf_count: u16,
    /// Per-buffer capacity in bytes.
    buf_size: usize,
}

impl SendBufPool {
    /// Create a new send buffer pool with `count` buffers of `size` bytes each.
    fn new(count: u16, size: usize) -> Self {
        let mut buffers = Vec::with_capacity(count as usize);
        let mut free_list = Vec::with_capacity(count as usize);

        for i in 0..count {
            let buf = vec![0u8; size];
            buffers.push(buf);
            free_list.push(i);
        }

        Self {
            buffers,
            free_list,
            buf_count: count,
            buf_size: size,
        }
    }

    /// Build the iovec array for `register_buffers()`.
    fn build_iovecs(&self) -> Vec<libc::iovec> {
        self.buffers
            .iter()
            .map(|buf| libc::iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            })
            .collect()
    }

    /// Allocate a buffer from the pool.
    ///
    /// Returns `Some((index, slice))` where `index` is the registered buffer index
    /// and `slice` is a mutable reference to write response data into.
    /// Returns `None` if the pool is exhausted (caller should fall back to heap).
    #[inline]
    pub fn alloc(&mut self) -> Option<(u16, &mut [u8])> {
        let idx = self.free_list.pop()?;
        Some((idx, &mut self.buffers[idx as usize][..self.buf_size]))
    }

    /// Reclaim a buffer back to the pool after SendComplete CQE.
    #[inline]
    pub fn reclaim(&mut self, idx: u16) {
        debug_assert!(
            (idx as usize) < self.buffers.len(),
            "send buf index {} out of range {}",
            idx,
            self.buffers.len()
        );
        self.free_list.push(idx);
    }

    /// Check if the pool is exhausted (no free buffers available).
    #[inline]
    pub fn is_exhausted(&self) -> bool {
        self.free_list.is_empty()
    }

    /// Number of free buffers available.
    #[inline]
    pub fn free_count(&self) -> usize {
        self.free_list.len()
    }

    /// Total number of buffers in the pool.
    #[inline]
    pub fn total_count(&self) -> u16 {
        self.buf_count
    }

    /// Per-buffer size.
    #[inline]
    pub fn buf_size(&self) -> usize {
        self.buf_size
    }

    /// Get a pointer and the buffer for a given index (for building SQEs).
    #[inline]
    fn buf_ptr(&self, idx: u16) -> *const u8 {
        self.buffers[idx as usize].as_ptr()
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
    send_buf_pool: SendBufPool,
    connections: HashMap<u32, ConnState>,
    next_conn_id: u32,
    config: UringConfig,
    /// Number of SQEs queued in current batch (not yet submitted).
    pending_sqes: usize,
    /// Monotonic tick counter (incremented each drain_completions call).
    tick: u64,
}

impl UringDriver {
    /// Create a new UringDriver for a shard thread.
    ///
    /// MUST be called from the shard thread that will own this driver
    /// (`SINGLE_ISSUER` flag requires single-thread access).
    pub fn new(config: UringConfig) -> std::io::Result<Self> {
        let ring = if let Some(ms) = config.sqpoll_idle_ms {
            // SQPOLL: kernel thread polls SQ, avoiding io_uring_enter() per submit.
            // Note: SQPOLL is incompatible with DEFER_TASKRUN (kernel thread != issuer),
            // so we only set SINGLE_ISSUER + COOP_TASKRUN + SQPOLL here.
            match IoUring::builder()
                .setup_single_issuer()
                .setup_coop_taskrun()
                .setup_sqpoll(ms)
                .build(config.ring_size)
            {
                Ok(ring) => {
                    tracing::info!("io_uring SQPOLL enabled (idle {}ms)", ms);
                    ring
                }
                Err(e) if e.raw_os_error() == Some(libc::EPERM) => {
                    // EPERM: insufficient privileges for SQPOLL. Fall back to
                    // standard mode without SQPOLL (requires CAP_SYS_NICE or root).
                    tracing::warn!(
                        "io_uring SQPOLL failed (EPERM, need CAP_SYS_NICE), falling back to standard mode"
                    );
                    IoUring::builder()
                        .setup_single_issuer()
                        .setup_defer_taskrun()
                        .setup_coop_taskrun()
                        .build(config.ring_size)?
                }
                Err(e) => return Err(e),
            }
        } else {
            IoUring::builder()
                .setup_single_issuer()
                .setup_defer_taskrun()
                .setup_coop_taskrun()
                .build(config.ring_size)?
        };

        let fd_table = FdTable::new(config.max_connections);
        let buf_ring = BufRingManager::new(config.buf_ring.clone());
        let send_buf_pool = SendBufPool::new(config.send_buf_pool_size, DEFAULT_SEND_BUF_SIZE);

        Ok(Self {
            ring,
            fd_table,
            buf_ring,
            send_buf_pool,
            connections: HashMap::new(),
            next_conn_id: 0,
            config,
            pending_sqes: 0,
            tick: 0,
        })
    }

    /// Initialize: register FD table and provided buffer ring with the kernel.
    ///
    /// Must be called once after construction, before any I/O operations.
    pub fn init(&mut self) -> std::io::Result<()> {
        self.fd_table.register_with_ring(&self.ring)?;
        self.buf_ring.setup_ring(&mut self.ring)?;

        // Register send buffers with the kernel (IORING_REGISTER_BUFFERS).
        // This pins pages once at init so WRITE_FIXED skips get_user_pages() per I/O.
        let iovecs = self.send_buf_pool.build_iovecs();
        if !iovecs.is_empty() {
            unsafe {
                self.ring.submitter().register_buffers(&iovecs)?;
            }
        }

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
                _raw_fd: raw_fd,
                read_buf: BytesMut::with_capacity(0), // allocated on-demand for partial frames
                recv_active: false,
                last_recv_tick: 0,
            },
        );

        // Arm multishot recv with provided buffer ring
        self.submit_multishot_recv(conn_id, fixed_idx)?;

        Ok(Some(conn_id))
    }

    /// Submit multishot recv for a connection using the provided buffer ring.
    fn submit_multishot_recv(&mut self, conn_id: u32, fixed_fd_idx: u32) -> std::io::Result<()> {
        let entry = opcode::RecvMulti::new(types::Fixed(fixed_fd_idx), self.buf_ring.group_id())
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

        let entry = opcode::Writev::new(types::Fixed(conn.fixed_fd_idx), iovecs, iovec_count)
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
    pub fn submit_send(&mut self, conn_id: u32, data: *const u8, len: u32) -> std::io::Result<()> {
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

    /// Submit a send using a pre-registered fixed buffer (IORING_OP_WRITE_FIXED).
    ///
    /// Skips per-I/O `get_user_pages()` because the buffer was pre-registered.
    /// The `buf_idx` identifies the registered buffer; `len` is the number of
    /// valid bytes to write. The caller must have written data into the buffer
    /// via `alloc_send_buf()` before calling this.
    ///
    /// The `buf_idx` is encoded in the user_data aux field so `SendComplete`
    /// can reclaim the buffer.
    pub fn submit_send_fixed(
        &mut self,
        conn_id: u32,
        buf_idx: u16,
        len: u32,
    ) -> std::io::Result<()> {
        let conn = self.connections.get(&conn_id).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "connection not found")
        })?;

        let ptr = self.send_buf_pool.buf_ptr(buf_idx);
        let entry = opcode::WriteFixed::new(types::Fixed(conn.fixed_fd_idx), ptr, len, buf_idx)
            .build()
            .user_data(encode_user_data(EVENT_SEND, conn_id, buf_idx as u32));

        unsafe {
            self.ring.submission().push(&entry).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "SQ full: cannot submit send_fixed",
                )
            })?;
        }
        self.pending_sqes += 1;
        Ok(())
    }

    /// Allocate a send buffer from the pre-registered pool.
    ///
    /// Returns `Some((buf_idx, buf_slice))` on success, `None` if pool exhausted.
    /// Caller writes response data into `buf_slice`, then calls `submit_send_fixed`.
    #[inline]
    pub fn alloc_send_buf(&mut self) -> Option<(u16, &mut [u8])> {
        self.send_buf_pool.alloc()
    }

    /// Reclaim a send buffer back to the pool after SendComplete CQE.
    #[inline]
    pub fn reclaim_send_buf(&mut self, buf_idx: u16) {
        self.send_buf_pool.reclaim(buf_idx);
    }

    /// Check if the send buffer pool is exhausted.
    #[inline]
    pub fn send_buf_pool_exhausted(&self) -> bool {
        self.send_buf_pool.is_exhausted()
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
                std::io::Error::new(std::io::ErrorKind::Other, "SQ full: cannot submit timeout")
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
            return self.ring.submit_and_wait(1);
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
        // Step 1: Submit any pending SQEs via the crate's submit() which properly
        // syncs the SQ ring tail before calling io_uring_enter().
        let n = if self.pending_sqes > 0 {
            self.pending_sqes = 0;
            self.ring.submit()?
        } else {
            0
        };
        // Step 2: With DEFER_TASKRUN, the kernel only processes completions when
        // io_uring_enter(GETEVENTS) is called. The crate's submit()/submit_and_wait(0)
        // skip GETEVENTS when want=0, so we must always call enter() with GETEVENTS
        // to flush deferred task work (e.g. multishot accept CQEs).
        // SAFETY: IORING_ENTER_GETEVENTS=1, no sigset arg, size=0.
        unsafe {
            self.ring.submitter().enter::<libc::sigset_t>(
                0,
                0,
                1, // IORING_ENTER_GETEVENTS
                None,
            )?;
        }
        Ok(n)
    }

    /// Drain all completed CQEs, returning events for the caller to process.
    ///
    /// The caller (shard event loop) handles command dispatch based on event type.
    /// Buffer lifecycle: recv data is copied from the provided buffer before
    /// the buffer is returned to the ring (per pitfall 1 in research).
    pub fn drain_completions(&mut self) -> Vec<IoEvent> {
        self.tick += 1;
        let current_tick = self.tick;
        let mut events = Vec::new();

        // Collect CQEs first to release the mutable borrow on self.ring,
        // allowing return_buf to access the ring's submission queue below.
        let cqes: Vec<(u8, u32, u32, i32, u32)> = self
            .ring
            .completion()
            .map(|cqe| {
                let (event_type, conn_id, aux) = decode_user_data(cqe.user_data());
                (event_type, conn_id, aux, cqe.result(), cqe.flags())
            })
            .collect();

        for (event_type, conn_id, _aux, result, flags) in cqes {
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

                        // Stamp connection activity for idle reaping
                        if let Some(conn) = self.connections.get_mut(&conn_id) {
                            conn.last_recv_tick = current_tick;
                        }

                        events.push(IoEvent::Recv { conn_id, data });

                        // Check if multishot recv ended (MORE flag absent).
                        // MORE=0 can mean: buffer ring exhaustion, kernel cancellation,
                        // OR client FIN. We cannot distinguish these reliably at CQE
                        // time when result>0 (there IS data). Rearm recv — if the
                        // client truly closed, the rearmed recv will produce result=0
                        // which triggers Disconnect via the branch below.
                        if !cqueue::more(flags) {
                            if let Some(conn) = self.connections.get_mut(&conn_id) {
                                conn.recv_active = false;
                            }
                            events.push(IoEvent::RecvNeedsRearm { conn_id });
                        }
                    } else if result == 0 {
                        // Connection closed by peer (explicit 0-byte recv)
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
            let raw_fd = self
                .fd_table
                .remove_and_register(conn.fixed_fd_idx, &self.ring)?;
            unsafe {
                libc::close(raw_fd);
            }
        }
        Ok(())
    }

    /// Gracefully close a connection: shutdown(SHUT_WR) to send FIN, then close.
    ///
    /// Called when recv returns 0 (client half-close). The shutdown(SHUT_WR)
    /// sends a TCP FIN to the peer, which redis-benchmark 8.x needs to
    /// detect completion. Without this, close() on a fd with pending state
    /// may send RST instead.
    pub fn shutdown_and_close_connection(&mut self, conn_id: u32) -> std::io::Result<()> {
        if let Some(conn) = self.connections.remove(&conn_id) {
            let raw_fd = self
                .fd_table
                .remove_and_register(conn.fixed_fd_idx, &self.ring)?;

            // Send FIN to peer via shutdown(SHUT_WR).
            // Ignore ENOTCONN -- peer may have already fully closed.
            // SAFETY: raw_fd is a valid open socket fd obtained from fd_table.remove_and_register.
            unsafe {
                let ret = libc::shutdown(raw_fd, libc::SHUT_WR);
                if ret < 0 {
                    let errno = *libc::__errno_location();
                    if errno != libc::ENOTCONN {
                        tracing::debug!(
                            "shutdown(SHUT_WR) for conn {} fd {}: {}",
                            conn_id,
                            raw_fd,
                            std::io::Error::from_raw_os_error(errno)
                        );
                    }
                }
            }

            // SAFETY: raw_fd is a valid open fd; we have exclusive ownership after removing from fd_table.
            unsafe {
                libc::close(raw_fd);
            }
        }
        Ok(())
    }

    /// Reap connections idle for more than `max_idle_ticks` drain_completions cycles.
    ///
    /// Returns conn_ids that were reaped. Called periodically from the event loop
    /// (e.g. every 5 seconds) to clean up CLOSE_WAIT connections where the client
    /// closed but the multishot recv didn't produce a 0-byte CQE.
    pub fn reap_idle_connections(&mut self, max_idle_ticks: u64) -> Vec<u32> {
        let current = self.tick;
        let idle_ids: Vec<u32> = self
            .connections
            .iter()
            .filter(|(_, c)| {
                let idle = current.saturating_sub(c.last_recv_tick);
                idle > max_idle_ticks && !c.recv_active
            })
            .map(|(&id, _)| id)
            .collect();

        for &conn_id in &idle_ids {
            let _ = self.shutdown_and_close_connection(conn_id);
        }
        idle_ids
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
// WritevGuard: RAII wrapper for writev scatter-gather lifetime management
// ---------------------------------------------------------------------------

/// Owns the iovec array and header buffer for a writev submission.
/// Must live until the corresponding SendComplete CQE.
pub struct WritevGuard {
    pub iovecs: [libc::iovec; 3],
    pub header_buf: [u8; 32],
    /// Hold a reference to the value bytes to prevent deallocation.
    /// This is a Bytes handle from DashTable (cheap clone via Arc).
    pub _value_hold: bytes::Bytes,
}

impl UringDriver {
    /// Submit a direct send for PreSerialized RESP data.
    ///
    /// PreSerialized frames contain complete wire format -- no assembly needed.
    /// Uses a single iovec writev since data is contiguous.
    /// Returns a WritevGuard that the caller must keep alive until SendComplete.
    pub fn submit_send_preserialized(
        &mut self,
        conn_id: u32,
        data: bytes::Bytes,
    ) -> std::io::Result<WritevGuard> {
        let mut guard = WritevGuard {
            iovecs: [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; 3],
            header_buf: [0u8; 32],
            _value_hold: data,
        };
        // Single iovec -- PreSerialized is already complete RESP wire format
        guard.iovecs[0] = libc::iovec {
            iov_base: guard._value_hold.as_ptr() as *mut libc::c_void,
            iov_len: guard._value_hold.len(),
        };
        self.submit_writev(conn_id, guard.iovecs.as_ptr(), 1)?;
        Ok(guard)
    }

    /// Submit writev for a BulkString response using scatter-gather.
    /// Returns a WritevGuard that the caller must keep alive until SendComplete.
    pub fn submit_writev_bulkstring(
        &mut self,
        conn_id: u32,
        value: bytes::Bytes,
    ) -> std::io::Result<WritevGuard> {
        let mut guard = WritevGuard {
            iovecs: [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; 3],
            header_buf: [0u8; 32],
            _value_hold: value,
        };
        let (iovecs, _count) =
            build_get_response_iovecs(guard._value_hold.as_ref(), &mut guard.header_buf);
        guard.iovecs = iovecs;
        self.submit_writev(conn_id, guard.iovecs.as_ptr(), 3)?;
        Ok(guard)
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
