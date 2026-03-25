//! Runtime abstraction layer for async runtime swappability.
//!
//! Defines trait contracts for Timer, Spawn, Factory, ConnectionIo, and FileIo.
//! The default `runtime-tokio` feature provides Tokio implementations.
//! The `runtime-monoio` feature provides Monoio implementations (thread-per-core, io_uring/kqueue).

#[cfg(all(feature = "runtime-tokio", feature = "runtime-monoio"))]
compile_error!(
    "Features `runtime-tokio` and `runtime-monoio` are mutually exclusive. Enable only one."
);

pub mod cancel;
pub mod channel;
pub mod traits;

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
