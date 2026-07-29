//! c10k W11 — idle-connection buffer downshift.
//!
//! A connection parked in `read()` pins its full working set: an 8 KiB rent
//! buffer plus the (empty) 8 KiB read-accumulation and write buffers —
//! ~24 KiB of the measured ~43 KiB idle RSS/conn. This module shrinks that
//! to ~0.5 KiB for connections idle ≥ [`IDLE_DOWNSHIFT_MS`] without putting
//! a timer or an allocation on the hot path:
//!
//! 1. Each cancel-capable connection registers one [`IdleSlot`] in a
//!    shard-thread-local registry (one `Rc` alloc per connection, at setup).
//! 2. Stage-1 park: the handler records the park timestamp (shard cached
//!    clock — no syscall) and reads via monoio's `cancelable_read`.
//! 3. The shard event loop's EXISTING 1 s chore calls [`sweep`]: any read
//!    parked ≥ the threshold gets its `Canceller` fired. Cancel-and-await
//!    is loss-free: if the op had already completed with data, awaiting the
//!    same future delivers those bytes; otherwise it returns `ECANCELED`.
//! 4. On a cancelled (data-less) wake the handler releases its buffers and
//!    re-parks in stage 2 with a [`IDLE_PROBE_BUF`]-byte buffer and NO
//!    further chore involvement — a pure-idle connection is cancelled
//!    exactly once, then sits at the small footprint until real data.
//! 5. Real data (stage 1 or 2) restores the full working set lazily.
//!
//! Hot connections never meet the sweep: their parks are microseconds, the
//! chore only sees `parked_since_ms` ≠ 0 entries older than the threshold.
//! Per-park hot-path cost: two `Cell` stores + one `Rc` clone of the
//! `CancelHandle` (no allocation).
//!
//! TLS connections (`monoio_rustls` streams) do not implement cancelable
//! reads; they opt out via [`IdleParkRead::SUPPORTS_IDLE_PARK`] and keep
//! the pre-W11 behavior byte-for-byte.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use monoio::io::{AsyncReadRent, CancelHandle, Canceller};

/// Park duration after which the sweep cancels a stage-1 read (ms).
pub(super) const IDLE_DOWNSHIFT_MS: u64 = 1000;
/// Rent-buffer size while downshifted. A typical idle→active wake (one
/// command) fits; larger bursts spill into the next full-size read.
pub(super) const IDLE_PROBE_BUF: usize = 512;
/// Full rent-buffer size (the pre-W11 constant working size).
pub(super) const PARK_BUF_FULL: usize = 8192;

/// Per-connection shared state between the handler task and the shard chore.
pub(super) struct IdleSlot {
    /// Canceller for the connection's in-flight stage-1 read. The sweep
    /// consumes it (`Canceller::cancel(self) -> Self`) and stores the fresh
    /// replacement; the handler takes a fresh `CancelHandle` from whatever
    /// canceller is current at every park.
    canceller: RefCell<Canceller>,
    /// Shard-cached-clock ms when the connection parked its stage-1 read;
    /// 0 = not parked in stage 1 (processing, downshifted, or gone).
    parked_since_ms: Cell<u64>,
}

impl IdleSlot {
    /// Take a cancel handle for the current park attempt.
    pub(super) fn handle(&self) -> CancelHandle {
        self.canceller.borrow().handle()
    }

    /// Mark the connection parked (stage 1). `now_ms` comes from the shard
    /// cached clock; clamped to ≥1 so 0 stays the "not parked" sentinel.
    pub(super) fn mark_parked(&self, now_ms: u64) {
        self.parked_since_ms.set(now_ms.max(1));
    }

    /// Mark the connection no longer parked (woke or is processing).
    pub(super) fn mark_unparked(&self) {
        self.parked_since_ms.set(0);
    }
}

thread_local! {
    /// All cancel-capable connections on this shard thread, by client id.
    static REGISTRY: RefCell<HashMap<u64, Rc<IdleSlot>>> = RefCell::new(HashMap::new());
}

/// RAII registration: removes the slot from the thread registry on drop
/// (handler return, migration hand-off, or task cancellation).
pub(super) struct IdleParkRegistration {
    pub(super) slot: Rc<IdleSlot>,
    client_id: u64,
}

impl Drop for IdleParkRegistration {
    fn drop(&mut self) {
        REGISTRY.with(|r| r.borrow_mut().remove(&self.client_id));
    }
}

/// Register a connection for idle-park sweeping on this shard thread.
pub(super) fn register(client_id: u64) -> IdleParkRegistration {
    let slot = Rc::new(IdleSlot {
        canceller: RefCell::new(Canceller::new()),
        parked_since_ms: Cell::new(0),
    });
    REGISTRY.with(|r| r.borrow_mut().insert(client_id, slot.clone()));
    IdleParkRegistration { slot, client_id }
}

/// Cancel every stage-1 read parked ≥ [`IDLE_DOWNSHIFT_MS`]. Called from the
/// shard event loop's 1 s chore (same thread as the connections). Returns the
/// number of reads cancelled (observability + tests).
///
/// The cancelled connection wakes with `ECANCELED` (or its raced data),
/// downshifts, and re-parks in stage 2 with `parked_since_ms == 0`, so each
/// idle connection is cancelled exactly once per idle period.
pub(crate) fn sweep(now_ms: u64) -> usize {
    REGISTRY.with(|r| {
        let mut cancelled = 0usize;
        for slot in r.borrow().values() {
            let parked = slot.parked_since_ms.get();
            if parked != 0 && now_ms.saturating_sub(parked) >= IDLE_DOWNSHIFT_MS {
                // Consume-and-replace: cancel() fires every associated op and
                // returns a fresh canceller for the connection's next park.
                let old = slot.canceller.replace(Canceller::new());
                let fresh = old.cancel();
                slot.canceller.replace(fresh);
                // One-shot: the waking handler also clears this, but resetting
                // here keeps a slow-to-wake connection from being re-cancelled
                // by the next sweep.
                slot.parked_since_ms.set(0);
                cancelled += 1;
            }
        }
        cancelled
    })
}

/// Release the parked working set. The rent buffer is dropped outright (the
/// pre-park sizing reallocates the probe size next iteration); the scratch
/// buffers are only released when empty — a non-empty `read_buf` holds a
/// partial frame that must survive the re-park.
pub(super) fn downshift_idle_buffers(
    tmp_buf: &mut Vec<u8>,
    read_buf: &mut bytes::BytesMut,
    write_buf: &mut bytes::BytesMut,
) {
    *tmp_buf = Vec::new();
    if read_buf.is_empty() && read_buf.capacity() > 0 {
        *read_buf = bytes::BytesMut::new();
    }
    if write_buf.is_empty() && write_buf.capacity() > 0 {
        *write_buf = bytes::BytesMut::new();
    }
}

/// Stream capability trait for the two-stage idle park.
///
/// The default is a plain read that ignores the cancel handle and reports
/// `SUPPORTS_IDLE_PARK = false`, keeping non-cancelable streams (TLS) on the
/// exact pre-W11 path. Only streams whose `cancelable_read` is loss-free
/// under cancel-and-await may set `SUPPORTS_IDLE_PARK = true`.
pub(crate) trait IdleParkRead: AsyncReadRent {
    const SUPPORTS_IDLE_PARK: bool = false;

    fn idle_park_read(
        &mut self,
        buf: Vec<u8>,
        _c: CancelHandle,
    ) -> impl std::future::Future<Output = monoio::BufResult<usize, Vec<u8>>> {
        self.read(buf)
    }
}

impl IdleParkRead for monoio::net::TcpStream {
    const SUPPORTS_IDLE_PARK: bool = true;

    fn idle_park_read(
        &mut self,
        buf: Vec<u8>,
        c: CancelHandle,
    ) -> impl std::future::Future<Output = monoio::BufResult<usize, Vec<u8>>> {
        monoio::io::CancelableAsyncReadRent::cancelable_read(self, buf, c)
    }
}

/// TLS streams keep the pre-W11 behavior (no cancelable read in
/// monoio_rustls; dropping its read future mid-handshake-record would also
/// desync the TLS session state).
impl IdleParkRead for monoio_rustls::ServerTlsStream<monoio::net::TcpStream> {}

#[cfg(test)]
mod idle_park_tests {
    use super::*;

    #[test]
    fn sweep_cancels_only_overdue_parks() {
        let reg = register(90_001);
        // Not parked: never cancelled.
        assert_eq!(sweep(10_000), 0, "unparked slot must not be swept");
        // Parked, under threshold: untouched.
        reg.slot.mark_parked(10_000);
        assert_eq!(sweep(10_000 + IDLE_DOWNSHIFT_MS - 1), 0);
        // Over threshold: cancelled exactly once, then the one-shot reset
        // keeps later sweeps away until the next park.
        assert_eq!(sweep(10_000 + IDLE_DOWNSHIFT_MS), 1);
        assert_eq!(
            reg.slot.parked_since_ms.get(),
            0,
            "sweep must reset park mark"
        );
        assert_eq!(
            sweep(10_000 + 10 * IDLE_DOWNSHIFT_MS),
            0,
            "one-shot per park"
        );
        // Re-park arms it again with the REPLACED canceller.
        reg.slot.mark_parked(60_000);
        assert_eq!(sweep(60_000 + IDLE_DOWNSHIFT_MS), 1);
    }

    #[test]
    fn registration_drop_deregisters() {
        {
            let reg = register(90_002);
            reg.slot.mark_parked(5_000);
        }
        assert_eq!(
            sweep(1_000_000),
            0,
            "dropped registration must leave the registry"
        );
    }

    #[test]
    fn downshift_releases_only_empty_scratch() {
        let mut tmp = vec![0u8; PARK_BUF_FULL];
        let mut rb = bytes::BytesMut::with_capacity(PARK_BUF_FULL);
        let mut wb = bytes::BytesMut::with_capacity(PARK_BUF_FULL);
        rb.extend_from_slice(b"partial-frame"); // must survive
        downshift_idle_buffers(&mut tmp, &mut rb, &mut wb);
        assert_eq!(tmp.capacity(), 0, "rent buffer must be dropped");
        assert_eq!(wb.capacity(), 0, "empty write buffer must be released");
        assert_eq!(&rb[..], b"partial-frame", "partial frame must be kept");
        assert!(
            rb.capacity() >= 13,
            "non-empty read buffer keeps its storage"
        );
    }
}
