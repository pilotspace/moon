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

/// Cooperatively relinquish to the shard event loop exactly once, then resume.
///
/// Runtime-agnostic (works on both monoio and tokio): a one-shot future that
/// returns `Pending` on its first poll after re-arming its own waker, then
/// `Ready` on the next. The executor parks this task, drains other ready work
/// (the 1ms tick, co-located commands, the SPSC drain), and re-polls us. Used by
/// the FT.SEARCH local slice (`ft-search-off-eventloop`) to interleave a heavy
/// search with the rest of the loop instead of monopolizing it. No allocation,
/// no lock, no cross-thread wake (the self-wake is always local to this thread).
pub async fn cooperative_yield() {
    struct YieldOnce(bool);
    impl std::future::Future for YieldOnce {
        type Output = ();
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            if self.0 {
                std::task::Poll::Ready(())
            } else {
                self.0 = true;
                // Re-arm locally so the executor re-polls us after other ready work.
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }
    YieldOnce(false).await;
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
