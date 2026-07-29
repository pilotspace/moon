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
//! TLS connections participate too (c10k P4b): the vendored monoio-rustls
//! implements a loss-free cancelable read, and their
//! [`IdleParkRead::on_idle_downshift`] additionally releases the wrapper's
//! two 16 KiB stream buffers (vendored monoio-io-wrapper reallocates them
//! lazily on the next I/O).

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use monoio::io::{AsyncReadRent, CancelHandle, Canceller};

/// Park duration after which the sweep cancels a stage-1 read (ms).
pub(super) const IDLE_DOWNSHIFT_MS: u64 = 1000;

/// c1M P1: stage-2 threshold — a downshifted connection parked this long has
/// its read cancelled so the handler TASK can exit (task-exit parking, see
/// `conn_accept::spawn_parked_idle_watcher`). 0 = task parking disabled.
/// The value lives in `crate::runtime` (set once from main before shards
/// spawn, same contract as `set_uring_entries`); this is a local alias.
pub(crate) use crate::runtime::conn_park_after_ms as park_after_ms;
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
    /// c1M P1: true while the park is a STAGE-2 (downshifted, task-parkable)
    /// read — the sweep then applies [`park_after_ms`] instead of
    /// [`IDLE_DOWNSHIFT_MS`], and the woken handler exits its task.
    stage2: Cell<bool>,
}

impl IdleSlot {
    /// Take a cancel handle for the current park attempt.
    pub(super) fn handle(&self) -> CancelHandle {
        self.canceller.borrow().handle()
    }

    /// Mark the connection parked (stage 1). `now_ms` comes from the shard
    /// cached clock; clamped to ≥1 so 0 stays the "not parked" sentinel.
    pub(super) fn mark_parked(&self, now_ms: u64) {
        self.stage2.set(false);
        self.parked_since_ms.set(now_ms.max(1));
    }

    /// Mark the connection parked in stage 2 (c1M P1): downshifted, session
    /// verified parkable, cancel = task exit.
    pub(super) fn mark_parked_stage2(&self, now_ms: u64) {
        self.stage2.set(true);
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
        stage2: Cell::new(false),
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
    let park_after = park_after_ms();
    REGISTRY.with(|r| {
        let mut cancelled = 0usize;
        for slot in r.borrow().values() {
            let parked = slot.parked_since_ms.get();
            // Stage-2 (task-park) entries use the operator threshold; a 0
            // threshold means task parking is off and stage-2 entries are
            // never registered, but guard anyway.
            let threshold = if slot.stage2.get() {
                if park_after == 0 {
                    continue;
                }
                park_after
            } else {
                IDLE_DOWNSHIFT_MS
            };
            if parked != 0 && now_ms.saturating_sub(parked) >= threshold {
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

    /// c1M P1: streams whose task can exit while the connection stays open
    /// (requires a race-free standalone readiness await — plain TCP only;
    /// TLS keeps W11+P4b behavior because rustls session state lives in the
    /// stream and the wrapper exposes no readiness API).
    const SUPPORTS_TASK_PARK: bool = false;

    fn idle_park_read(
        &mut self,
        buf: Vec<u8>,
        _c: CancelHandle,
    ) -> impl std::future::Future<Output = monoio::BufResult<usize, Vec<u8>>> {
        self.read(buf)
    }

    /// Stream-side hook fired when the handler downshifts after a cancelled
    /// stage-1 read. Default: nothing (plain TCP has no stream-owned
    /// buffers); TLS releases its drained wrapper buffers here.
    fn on_idle_downshift(&mut self) {}
}

impl IdleParkRead for monoio::net::TcpStream {
    const SUPPORTS_IDLE_PARK: bool = true;
    const SUPPORTS_TASK_PARK: bool = true;

    fn idle_park_read(
        &mut self,
        buf: Vec<u8>,
        c: CancelHandle,
    ) -> impl std::future::Future<Output = monoio::BufResult<usize, Vec<u8>>> {
        monoio::io::CancelableAsyncReadRent::cancelable_read(self, buf, c)
    }
}

/// TLS streams downshift too (c10k P4b): the vendored monoio-rustls
/// cancelable read forwards the cancel to the inner socket read; raced
/// bytes persist in the wrapper buffer + rustls deframer, and the cancel
/// error is deliberately NOT stashed for replay (vendored
/// `SafeRead::do_io_cancelable`), so the stage-2 probe read resumes the
/// session cleanly. The downshift hook additionally sheds the wrapper's
/// two 16 KiB stream buffers.
impl IdleParkRead for monoio_rustls::ServerTlsStream<monoio::net::TcpStream> {
    const SUPPORTS_IDLE_PARK: bool = true;

    fn idle_park_read(
        &mut self,
        buf: Vec<u8>,
        c: CancelHandle,
    ) -> impl std::future::Future<Output = monoio::BufResult<usize, Vec<u8>>> {
        monoio::io::CancelableAsyncReadRent::cancelable_read(self, buf, c)
    }

    fn on_idle_downshift(&mut self) {
        self.release_idle_buffers();
    }
}

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
    fn stage2_uses_park_after_threshold() {
        let reg = register(90_003);
        // Stage-2 park: the 1s downshift threshold must NOT fire it...
        reg.slot.mark_parked_stage2(100_000);
        assert_eq!(
            sweep(100_000 + IDLE_DOWNSHIFT_MS),
            0,
            "stage-2 park must outlive the stage-1 threshold"
        );
        // ...only the operator threshold does (default 60s).
        assert_eq!(sweep(100_000 + park_after_ms()), 1);
        // A later stage-1 park on the same slot reverts to the 1s threshold.
        reg.slot.mark_parked(300_000);
        assert_eq!(sweep(300_000 + IDLE_DOWNSHIFT_MS), 1);
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
