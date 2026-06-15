//! Runtime abstraction layer for async runtime swappability.
//!
//! Defines trait contracts for Timer, Spawn, Factory, ConnectionIo, and FileIo.
//!
//! **Default runtime:** `runtime-monoio` (Linux: io_uring, macOS: kqueue via FusionDriver)
//! **Fallback runtime:** `runtime-tokio` (Windows: IOCP, or explicit opt-in on any platform)
//!
//! To use Tokio instead: `cargo build --no-default-features --features runtime-tokio,jemalloc`

#[cfg(all(feature = "runtime-tokio", feature = "runtime-monoio"))]
compile_error!(
    "Features `runtime-tokio` and `runtime-monoio` are mutually exclusive. Enable only one."
);

#[cfg(all(feature = "runtime-monoio", target_os = "windows"))]
compile_error!(
    "Monoio does not support Windows. Use: cargo build --no-default-features --features runtime-tokio,jemalloc"
);

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-monoio")))]
compile_error!("No runtime selected. Enable either `runtime-tokio` or `runtime-monoio` feature.");

pub mod cancel;
pub mod channel;
pub mod race;
pub mod traits;

/// Cooperatively relinquish to the shard event loop, letting co-located
/// connections + the 1ms tick make progress, then resume.
///
/// Used by the FT.SEARCH local slice (`ft-search-off-eventloop`) to interleave a
/// heavy search with the rest of the loop instead of monopolizing it. The yield
/// is **runtime-specific** because a naive self-wake is effective on tokio but a
/// silent no-op on monoio (benchmark: `tmp/bench_ftsearch/RESULTS.md`).
///
/// **monoio:** the io_uring run loop only reaps the completion queue when its task
/// queue empties (it `park()`s — `monoio-0.2.4/src/runtime.rs`). A self-waking
/// task re-queues itself, so the loop spins on `submit()` and NEVER reaps the CQ
/// — co-located connections' read completions are never serviced until the search
/// fully finishes (measured: co-located p99 ≈ full search time, zero relief). A
/// zero-duration timer instead PARKS the search on the timer driver: the task
/// queue empties, the loop `park()`s, the CQ is reaped (waking co-located tasks),
/// and the already-expired timer re-wakes us next iteration (measured after the
/// fix: co-located p99 ≪ search time).
///
/// **tokio:** the scheduler polls the I/O driver on its event interval, so the
/// canonical cooperative yield already lets co-located connections progress
/// (measured: co-located p99 ~6ms under a ~63ms search).
#[cfg(feature = "runtime-monoio")]
pub async fn cooperative_yield() {
    monoio_yield::park_reap().await;
}

/// Cost-free monoio yield (`ft-yield-costfree-monoio`).
///
/// `sleep(ZERO)` relieves co-located connections but pays the timer wheel's ~ms
/// granularity per yield (spike `tmp/yield-spike`, io_uring VM: ~1.8ms/yield → a
/// ~26% wall tax on a brute-force scan, which forced the chunk up to 16384).
/// Reading one byte from an always-ready `UnixStream` socketpair forces the SAME
/// drain→park→reap cycle — the read `Op` returns Pending after submit, the task
/// queue drains, the loop `park()`s, and the kernel posts the already-ready CQE
/// immediately, reaping co-located read CQEs too — at ~µs instead of ~ms.
/// Spike-proven: co-located victim relieved (10ms vs 250ms starved), near-zero
/// overhead. `Pipe` does NOT impl `AsyncReadRent`; `UnixStream::pair()` does.
///
/// Falls back to `sleep(ZERO)` when io_uring is unavailable (`MOON_NO_URING` /
/// non-Linux), when the socketpair cannot be created (sticky for the thread), or
/// when a read is starved — never failing the search, never running synchronously.
#[cfg(feature = "runtime-monoio")]
mod monoio_yield {
    use std::cell::{Cell, RefCell};
    use std::time::Duration;

    use monoio::io::{AsyncReadRent, AsyncWriteRent};
    use monoio::net::unix::UnixStream;

    /// Bytes written per re-arm; one read consumes one byte, so the socketpair
    /// stays readable for `ARM_BATCH` yields between (rare) re-arm writes.
    const ARM_BATCH: usize = 4096;

    struct YieldPipe {
        rx: UnixStream,
        tx: UnixStream,
        avail: usize,
        rbuf: Vec<u8>,
        wbuf: Vec<u8>,
    }

    thread_local! {
        static PIPE: RefCell<Option<YieldPipe>> = const { RefCell::new(None) };
        static FAILED: Cell<bool> = const { Cell::new(false) };
    }

    #[cfg(test)]
    thread_local! {
        static FORCE_FAIL: Cell<bool> = const { Cell::new(false) };
    }
    #[cfg(test)]
    pub(crate) fn set_force_fail(on: bool) {
        FORCE_FAIL.with(|c| c.set(on));
    }

    /// io_uring is the reap path the self-pipe targets; elsewhere keep `sleep(ZERO)`.
    /// `MOON_NO_URING` does not change at runtime, so the lookup is cached once.
    fn uring_active() -> bool {
        static CACHE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *CACHE.get_or_init(|| {
            cfg!(target_os = "linux") && std::env::var_os("MOON_NO_URING").is_none()
        })
    }

    async fn timer_park() {
        // The proven-correct fallback (ft-search-off-eventloop): a ZERO timer parks
        // the loop on the timer driver, draining the queue so the CQ is reaped.
        monoio::time::sleep(Duration::ZERO).await;
    }

    fn create_pipe() -> std::io::Result<YieldPipe> {
        #[cfg(test)]
        if FORCE_FAIL.with(Cell::get) {
            return Err(std::io::Error::other(
                "forced yield-pipe init failure (test)",
            ));
        }
        let (rx, tx) = UnixStream::pair()?;
        Ok(YieldPipe {
            rx,
            tx,
            avail: 0,
            rbuf: vec![0u8; 1],
            wbuf: vec![0u8; ARM_BATCH],
        })
    }

    pub(super) async fn park_reap() {
        if !uring_active() || FAILED.with(Cell::get) {
            timer_park().await;
            return;
        }

        // Lazy per-thread init (first FT.SEARCH yield on this shard).
        if PIPE.with(|c| c.borrow().is_none()) {
            match create_pipe() {
                Ok(p) => PIPE.with(|c| *c.borrow_mut() = Some(p)),
                Err(_) => {
                    FAILED.with(|c| c.set(true));
                    timer_park().await;
                    return;
                }
            }
        }

        // Take the pipe out for the await (single-threaded; a re-entrant caller —
        // two heavy searches interleaving on one thread — falls back to the timer).
        let mut pipe = match PIPE.with(|c| c.borrow_mut().take()) {
            Some(p) => p,
            None => {
                timer_park().await;
                return;
            }
        };

        // Re-arm when the readable bytes run out.
        if pipe.avail == 0 {
            let wbuf = std::mem::take(&mut pipe.wbuf);
            let (res, wbuf) = pipe.tx.write(wbuf).await;
            pipe.wbuf = wbuf;
            match res {
                Ok(n) if n > 0 => pipe.avail = n,
                _ => {
                    // Cannot re-arm — drop the pipe, go sticky-timer for the thread.
                    FAILED.with(|c| c.set(true));
                    timer_park().await;
                    return;
                }
            }
        }

        // The cost-free park+reap: one byte off the always-ready socketpair.
        let rbuf = std::mem::take(&mut pipe.rbuf);
        let (res, rbuf) = pipe.rx.read(rbuf).await;
        pipe.rbuf = rbuf;
        match res {
            Ok(n) if n > 0 => {
                pipe.avail = pipe.avail.saturating_sub(n);
                PIPE.with(|c| *c.borrow_mut() = Some(pipe));
            }
            _ => {
                // Starved / EOF: re-arm next call, fall back to the timer this yield.
                pipe.avail = 0;
                PIPE.with(|c| *c.borrow_mut() = Some(pipe));
                timer_park().await;
            }
        }
    }
}

/// See [`cooperative_yield`] (monoio variant) for the full rationale.
#[cfg(feature = "runtime-tokio")]
pub async fn cooperative_yield() {
    tokio::task::yield_now().await;
}

#[cfg(feature = "runtime-tokio")]
pub mod tokio_impl;

// Re-export concrete types based on active runtime feature.
#[cfg(feature = "runtime-tokio")]
pub use tokio_impl::{TokioFileIo, TokioRuntimeFactory, TokioSpawner, TokioTimer};

#[cfg(feature = "runtime-monoio")]
pub mod monoio_impl;

#[cfg(feature = "runtime-monoio")]
pub use monoio_impl::{MonoioFileIo, MonoioRuntimeFactory, MonoioSpawner, MonoioTimer};

/// `ft-yield-costfree-monoio` §4 — monoio io_uring behavioral tests for the
/// cost-free yield. Linux + monoio only (the self-pipe targets the io_uring reap
/// path; macOS kqueue and the tokio CI use the timer fallback / `yield_now`).
#[cfg(all(test, feature = "runtime-monoio", target_os = "linux"))]
mod yield_costfree_tests {
    use std::hint::black_box;
    use std::time::{Duration, Instant};

    use monoio::io::{AsyncReadRent, AsyncWriteRent};
    use monoio::net::unix::UnixStream;

    fn busy(ms: u64) {
        let t = Instant::now();
        let mut acc = 0u64;
        while t.elapsed() < Duration::from_millis(ms) {
            acc = black_box(acc.wrapping_add(1));
        }
        black_box(acc);
    }

    /// A co-located victim does an io_uring read on a pre-filled socket; we return
    /// the wall-clock from hog-start to victim completion. Relief => ~one chunk;
    /// starvation => ~hog_total.
    async fn colocated_victim_latency(chunks: u32, work_ms: u64) -> Duration {
        let (mut vrx, mut vtx) = UnixStream::pair().expect("victim pair");
        vtx.write(vec![1u8]).await.0.expect("victim prime");

        let start = Instant::now();
        let victim = monoio::spawn(async move {
            let (res, _b) = vrx.read(vec![0u8; 1]).await;
            res.expect("victim read");
            start.elapsed()
        });

        for _ in 0..chunks {
            busy(work_ms);
            super::cooperative_yield().await;
        }
        victim.await
    }

    /// THE red headline: 200 yields must cost ~µs each, not the ~1.8ms/yield timer
    /// tax. RED on main (`sleep(ZERO)` ≈ 360ms); GREEN with the self-pipe.
    #[test]
    fn monoio_yield_overhead_is_microscopic() {
        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_timer()
            .build()
            .expect("io_uring runtime (needs Linux kernel io_uring)");
        rt.block_on(async {
            const N: u32 = 200;
            super::cooperative_yield().await; // warm lazy init out of the measurement
            let start = Instant::now();
            for _ in 0..N {
                super::cooperative_yield().await;
            }
            let total = start.elapsed();
            assert!(
                total < Duration::from_millis(100),
                "200 cost-free yields must take <100ms; took {total:?} \
                 (sleep(ZERO) timer-park is ~1.8ms/yield ≈ 360ms)"
            );
        });
    }

    /// GUARD (green on main and after): the yield still relieves co-located work —
    /// the #179 latency contract the cost-free swap must preserve.
    #[test]
    fn monoio_yield_relieves_colocated() {
        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_timer()
            .build()
            .expect("io_uring runtime (needs Linux kernel io_uring)");
        rt.block_on(async {
            let lat = colocated_victim_latency(20, 5).await; // 20 x 5ms = 100ms hog
            assert!(
                lat < Duration::from_millis(40),
                "co-located victim must be relieved within a few chunks; got {lat:?} \
                 (starvation would be ~100ms)"
            );
        });
    }

    /// Reject `yield_init_failed`: a forced socketpair-init failure must fall back
    /// to the timer-park — still relieving co-located work, never panicking, never
    /// running synchronously.
    #[test]
    fn monoio_yield_falls_back_on_init_failure() {
        super::monoio_yield::set_force_fail(true);
        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_timer()
            .build()
            .expect("io_uring runtime (needs Linux kernel io_uring)");
        rt.block_on(async {
            let lat = colocated_victim_latency(20, 5).await;
            assert!(
                lat < Duration::from_millis(40),
                "forced init failure must fall back to timer-park, still relieving \
                 co-located work; got {lat:?}"
            );
        });
        super::monoio_yield::set_force_fail(false);
    }
}

// --- Runtime-specific type aliases for TCP networking ---
// These allow subsystem code to use a single type name regardless of runtime.

/// TCP stream type alias: tokio::net::TcpStream under tokio, placeholder under monoio.
#[cfg(feature = "runtime-tokio")]
pub type TcpStream = tokio::net::TcpStream;

/// Monoio cross-thread TcpStream placeholder.
///
/// `monoio::net::TcpStream` is `!Send` (contains `Rc<SharedFd>`), so it cannot
/// be transferred across threads via channels. For the listener-to-shard handoff,
/// we use `std::net::TcpStream` (which IS Send), then convert to
/// `monoio::net::TcpStream` on the receiving shard thread.
///
/// This type alias ensures that channel signatures compile. Shard code must call
/// `monoio::net::TcpStream::from_std(stream)` after receiving.
#[cfg(feature = "runtime-monoio")]
pub type TcpStream = std::net::TcpStream;

/// TCP listener type alias.
#[cfg(feature = "runtime-tokio")]
pub type TcpListener = tokio::net::TcpListener;

#[cfg(feature = "runtime-monoio")]
pub type TcpListener = monoio::net::TcpListener;

// --- Runtime-specific re-exports for FileIo ---
// Provide a unified `FileIoImpl` alias so callers don't need cfg gates.

#[cfg(feature = "runtime-tokio")]
pub type FileIoImpl = TokioFileIo;

#[cfg(feature = "runtime-monoio")]
pub type FileIoImpl = MonoioFileIo;

// --- Runtime-specific re-exports for Timer ---

#[cfg(feature = "runtime-tokio")]
pub type TimerImpl = TokioTimer;

#[cfg(feature = "runtime-monoio")]
pub type TimerImpl = MonoioTimer;

// --- Runtime-specific re-exports for RuntimeFactory ---

#[cfg(feature = "runtime-tokio")]
pub type RuntimeFactoryImpl = TokioRuntimeFactory;

#[cfg(feature = "runtime-monoio")]
pub type RuntimeFactoryImpl = MonoioRuntimeFactory;
