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
