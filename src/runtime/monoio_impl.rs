//! Monoio implementations of runtime traits.
//!
//! Active when `runtime-monoio` feature is enabled.
//! Uses FusionDriver for io_uring on Linux and kqueue on macOS.

use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

use super::traits::{
    FileIo, FileSync, RuntimeFactory, RuntimeInterval, RuntimeSpawn, RuntimeTimer,
};

/// Monoio-based timer implementation.
pub struct MonoioTimer;

/// Wrapper around monoio::time::Interval implementing RuntimeInterval.
pub struct MonoioInterval(pub(crate) monoio::time::Interval);

impl RuntimeTimer for MonoioTimer {
    type Interval = MonoioInterval;

    fn interval(period: Duration) -> Self::Interval {
        MonoioInterval(monoio::time::interval(period))
    }

    fn sleep(duration: Duration) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(monoio::time::sleep(duration))
    }
}

impl RuntimeInterval for MonoioInterval {
    fn tick(&mut self) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(async {
            self.0.tick().await;
        })
    }
}

/// Monoio-based task spawner (inherently local -- monoio::spawn is !Send).
pub struct MonoioSpawner;

impl RuntimeSpawn for MonoioSpawner {
    fn spawn_local<F>(future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        // monoio::spawn is inherently local (thread-per-core).
        // Drop the JoinHandle for fire-and-forget semantics.
        drop(monoio::spawn(future));
    }
}

/// Monoio runtime factory using FusionDriver (io_uring on Linux, kqueue on macOS).
pub struct MonoioRuntimeFactory;

impl RuntimeFactory for MonoioRuntimeFactory {
    fn block_on_local<F: Future<Output = ()> + 'static>(name: String, f: F) {
        // Honor the documented MOON_NO_URING contract (and `--io-driver
        // epoll`) for the monoio runtime too: FusionDriver silently
        // auto-picks io_uring on Linux and offers no runtime introspection,
        // so the kill-switch must force the epoll/kqueue LegacyDriver
        // explicitly. (Previously MOON_NO_URING only gated the tokio bridge
        // + uring_active(); the monoio driver choice ignored it — discovered
        // during the c4a p=1 driver A/B.)
        if crate::runtime::legacy_driver_forced() {
            let mut rt = monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
                .enable_timer()
                .build()
                .unwrap_or_else(|e| {
                    panic!("failed to build monoio legacy runtime '{}': {}", name, e)
                });
            rt.block_on(f);
            return;
        }
        // Tuned io_uring: COOP_TASKRUN + SINGLE_ISSUER + DEFER_TASKRUN slash
        // io_uring_enter cost for a thread-per-core ring (created and entered
        // only on this shard thread; moon's cross-thread signalling is fd-based
        // — flume/eventfd/UnixStream — never a ring op from another thread,
        // which DEFER_TASKRUN forbids). Measured need: on GCE (c4a/c3) the
        // stock ring's enter cost made io_uring LOSE to epoll at p=1.
        // Kernels <6.1 reject the flags -> warn once and fall back to the
        // stock FusionDriver below. MOON_URING_PLAIN=1 skips the tuning.
        #[cfg(target_os = "linux")]
        if std::env::var_os("MOON_URING_PLAIN").is_none() {
            let mut urb = io_uring_06::IoUring::builder();
            urb.setup_coop_taskrun()
                .setup_single_issuer()
                .setup_defer_taskrun();
            match monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .uring_builder(urb)
                .enable_timer()
                .build()
            {
                Ok(mut rt) => {
                    rt.block_on(f);
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        "tuned io_uring (COOP_TASKRUN|SINGLE_ISSUER|DEFER_TASKRUN) \
                         unavailable for '{}' ({}); falling back to stock driver",
                        name,
                        e
                    );
                }
            }
        }

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .unwrap_or_else(|e| panic!("failed to build monoio runtime '{}': {}", name, e));
        rt.block_on(f);
    }
}

/// Standard filesystem I/O for Monoio runtime (sync ops, same as Tokio path).
pub struct MonoioFileIo;

impl FileIo for MonoioFileIo {
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

/// FileSync for std::fs::File -- only compiled when runtime-tokio is NOT active
/// to avoid duplicate impl (tokio_impl.rs already provides this when tokio is enabled).
#[cfg(not(feature = "runtime-tokio"))]
impl FileSync for std::fs::File {
    fn sync_data(&self) -> io::Result<()> {
        std::fs::File::sync_data(self)
    }
}
