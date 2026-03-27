//! Tokio implementations of runtime traits.
//!
//! Active when `runtime-tokio` feature is enabled (default).

use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

use super::traits::{
    FileIo, FileSync, RuntimeFactory, RuntimeInterval, RuntimeSpawn, RuntimeTimer,
};

/// Tokio-based timer implementation.
pub struct TokioTimer;

/// Wrapper around tokio::time::Interval implementing RuntimeInterval.
pub struct TokioInterval(tokio::time::Interval);

impl RuntimeTimer for TokioTimer {
    type Interval = TokioInterval;

    fn interval(period: Duration) -> Self::Interval {
        TokioInterval(tokio::time::interval(period))
    }

    fn sleep(duration: Duration) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

impl RuntimeInterval for TokioInterval {
    fn tick(&mut self) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(async {
            self.0.tick().await;
        })
    }
}

/// Tokio-based task spawner.
pub struct TokioSpawner;

impl RuntimeSpawn for TokioSpawner {
    fn spawn_local<F>(future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        tokio::task::spawn_local(future);
    }
}

/// Tokio-compatible runtime factory (current_thread + LocalSet).
pub struct TokioRuntimeFactory;

impl RuntimeFactory for TokioRuntimeFactory {
    fn block_on_local<F: Future<Output = ()> + 'static>(name: String, f: F) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .thread_name(&name)
            .build()
            .unwrap_or_else(|e| panic!("failed to build runtime '{}': {}", name, e));
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(f));
    }
}

/// Standard filesystem I/O (used by Tokio runtime; future DMA impl would replace this).
pub struct TokioFileIo;

impl FileIo for TokioFileIo {
    type Writer = std::fs::File;

    fn open_append(path: &Path) -> io::Result<Self::Writer> {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
    }

    fn open_write(path: &Path) -> io::Result<Self::Writer> {
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
    }
}

impl FileSync for std::fs::File {
    fn sync_data(&self) -> io::Result<()> {
        std::fs::File::sync_data(self)
    }
}
