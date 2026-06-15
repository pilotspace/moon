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
    // ZERO-duration timer: forces a driver park()/CQ-reap cycle without blocking.
    // Registers a TimerEntry and returns Pending on first poll (the search does
    // NOT re-queue itself), so monoio's task queue can drain to empty.
    monoio::time::sleep(std::time::Duration::ZERO).await;
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
