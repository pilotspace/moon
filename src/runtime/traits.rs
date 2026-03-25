//! Runtime-agnostic trait definitions for async runtime swappability.
//!
//! Each trait abstracts one category of runtime-specific functionality.
//! Implementations are provided per-feature (e.g., `runtime-tokio`).

use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

/// Timer abstraction for periodic intervals and one-shot sleeps.
pub trait RuntimeTimer {
    /// Opaque interval type returned by `interval()`.
    type Interval: RuntimeInterval;

    /// Create a repeating interval that ticks every `period`.
    fn interval(period: Duration) -> Self::Interval;

    /// Sleep for the given duration.
    fn sleep(duration: Duration) -> Pin<Box<dyn Future<Output = ()>>>;
}

/// An interval stream that can be ticked.
pub trait RuntimeInterval {
    /// Wait for the next tick. Returns when the interval fires.
    fn tick(&mut self) -> Pin<Box<dyn Future<Output = ()> + '_>>;
}

/// Spawn abstraction for task creation on the current thread.
pub trait RuntimeSpawn {
    /// Spawn a !Send future on the current thread's local executor.
    /// Equivalent to tokio::task::spawn_local.
    fn spawn_local<F>(future: F)
    where
        F: Future<Output = ()> + 'static;
}

/// Factory for creating per-shard single-threaded runtimes.
pub trait RuntimeFactory {
    /// Build a single-threaded runtime and run the given future to completion.
    /// Equivalent to building a current_thread Tokio runtime with LocalSet.
    fn block_on_local<F: Future<Output = ()> + 'static>(name: String, f: F);
}

/// File I/O abstraction for persistence (WAL, snapshots).
/// Provides synchronous file operations (WAL writer is sync in the shard event loop).
pub trait FileIo {
    type Writer: io::Write + FileSync;

    /// Open a file for append-mode writing, creating it if it doesn't exist.
    fn open_append(path: &Path) -> io::Result<Self::Writer>;

    /// Open a file for write-mode (truncate), creating it if it doesn't exist.
    fn open_write(path: &Path) -> io::Result<Self::Writer>;
}

/// Sync (fsync/fdatasync) abstraction for file writers.
pub trait FileSync {
    /// Flush file data to durable storage (fsync or equivalent).
    fn sync_data(&self) -> io::Result<()>;
}
