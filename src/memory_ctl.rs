//! Allocator introspection and control: the `mallctl` surface moon needs to
//! answer "where did the memory go", plus the decay lever to act on the answer.
//!
//! # Why this exists
//!
//! A real instance, macOS, 6.8 days uptime, observed 2026-08-05: `used_memory`
//! reported 2.43 GB while the OS charged the process 7.3 GB (`phys_footprint`),
//! 7.2 GB of which had been pushed to swap. `INFO memory` could not explain the
//! ~4.9 GB gap, because moon exposes no allocator counters — so the gap was
//! indistinguishable between jemalloc fragmentation and live memory that
//! `used_memory` simply does not charge for.
//!
//! `jemalloc_stats` closes that: build with `--features jemalloc-stats` and
//! `INFO memory` reports Redis's `allocator_*` fields, which name the gap
//! precisely. `active - allocated` is fragmentation; `resident - active` is
//! dirty pages jemalloc holds but has not returned; anything still missing
//! after those is memory moon holds outside the allocator.
//!
//! # A hypothesis this module does NOT support
//!
//! moon bakes `dirty_decay_ms:1000,muzzy_decay_ms:5000,background_thread:true`
//! into `_rjem_malloc_conf` (see `main.rs`), on the understanding that jemalloc
//! hands dirty pages back about a second after they go idle.
//!
//! `background_thread` is not portable. From jemalloc's own `configure.ac`:
//!
//! ```text
//! if test "x${have_pthread}" = "x1" -a "x${je_cv_os_unfair_lock}" != "xyes" -a \
//!        "x${abi}" != "xmacho" ; then
//!   AC_DEFINE([JEMALLOC_BACKGROUND_THREAD], [ ], [ ])
//! fi
//! ```
//!
//! `abi != macho` — on Apple platforms the macro is never defined and
//! `background_threads_enable` compiles down to `not_reached()`. The option is
//! still *accepted* (so `abort_conf:true` does not fire and nothing is logged),
//! it simply does nothing.
//!
//! Without that thread, jemalloc runs decay only as a side effect of allocator
//! activity in the arena being used. A process that goes quiet therefore never
//! purges — which is exactly the shape moon is built for: a long-lived cache
//! that peaks during an index build or a compaction and then idles.
//!
//! Measured on a real instance, 6.8 days uptime, macOS, jemalloc build:
//! logical dataset 2.43 GB, but 7.4 GB of dirty anonymous memory across 37,474
//! regions, physical footprint peaked at 8.5 GB and never came back down. The
//! OS had swapped 7.2 GB of it out — the process was the largest single
//! consumer of the machine's swap while `ps` showed a healthy-looking 183 MB
//! RSS, because RSS does not count what has been paged out.
//!
//! # The fix
//!
//! Re-implement the missing thread: one low-frequency housekeeping thread that
//! asks jemalloc to run decay. `arena.<i>.decay` is precisely what the
//! background thread invokes, and it *respects* `dirty_decay_ms` — it returns
//! only pages that have genuinely been idle long enough, unlike
//! `arena.<i>.purge`, which would discard the allocator's whole dirty cache and
//! force it to re-fault pages back in under load.
//!
//! This runs on every jemalloc build, not just Apple ones. On Linux the
//! background thread has usually already done the work, so the call finds
//! nothing to do and costs one `mallctl` per second — and it covers the case
//! where the background thread silently fails to start, which jemalloc also
//! tolerates quietly (`background_thread.c` falls back when
//! `dlsym(RTLD_NEXT, "pthread_create")` returns NULL).
//!
//! **It is off by default (`--memory-decay-interval-ms 0`) and it is NOT the
//! explanation for the instance above.** A 384 MiB churn-and-free on macOS was
//! measured reclaiming to a 3.7 MiB physical footprint with no decay call at
//! all: decay runs as a side effect of the frees themselves, so "an idle
//! process never purges" does not hold for that workload. The missing
//! background thread is a real gap, but it is unproven against the observed
//! symptom, and shipping it enabled would be claiming a fix that has not been
//! demonstrated. Turn it on once `INFO memory` shows that dirty-but-unreturned
//! pages (`resident - active`) are in fact where the memory went.
//! `tests/allocator_idle_decay.rs` records the measurement.

/// jemalloc's `MALLCTL_ARENAS_ALL` (`jemalloc_macros.h`): the pseudo-arena
/// index that fans an arena ctl out across every arena.
#[cfg(all(feature = "jemalloc", test))]
const MALLCTL_ARENAS_ALL: usize = 4096;

/// `arena.4096.decay`, NUL-terminated for the C API.
///
/// Written out rather than formatted so the hot call allocates nothing and the
/// constant is verifiable by eye against `MALLCTL_ARENAS_ALL` above; the unit
/// test below pins the two together.
#[cfg(feature = "jemalloc")]
const ARENA_ALL_DECAY: &[u8] = b"arena.4096.decay\0";

/// Ask jemalloc to run decay across all arenas. Returns `false` if the ctl was
/// rejected (which would mean this build's jemalloc does not have the ctl —
/// worth knowing, but never worth failing on).
#[cfg(feature = "jemalloc")]
pub fn decay_all_arenas() -> bool {
    // SAFETY: `mallctl` is jemalloc's thread-safe control entry point.
    // `ARENA_ALL_DECAY` is a 'static NUL-terminated byte string, so the name
    // pointer is valid for the whole call. `arena.<i>.decay` is declared
    // `NEITHER_READ_NOR_WRITE()` in jemalloc's `ctl.c`, which returns EPERM
    // unless `oldp`, `oldlenp` and `newp` are all NULL and `newlen` is 0 —
    // exactly what is passed here. No buffer is read or written by jemalloc on
    // this path, so there is nothing for the caller to size or own.
    let rc = unsafe {
        tikv_jemalloc_sys::mallctl(
            ARENA_ALL_DECAY.as_ptr().cast::<std::ffi::c_char>(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
    };
    rc == 0
}

/// Non-jemalloc builds have nothing to decay.
#[cfg(not(feature = "jemalloc"))]
pub fn decay_all_arenas() -> bool {
    false
}

/// Start the housekeeping thread. `interval_ms == 0` disables it.
///
/// A plain OS thread, deliberately: this must be independent of which async
/// runtime is compiled in, must not sit on a shard thread (where a purge would
/// show up as command latency), and must keep running while every shard is
/// parked — the idle case is the one that matters.
///
/// The thread is detached and runs for the lifetime of the process. It holds
/// no state and touches nothing but the allocator, so there is nothing to
/// unwind at shutdown.
pub fn spawn(interval_ms: u64) {
    if interval_ms == 0 {
        tracing::debug!("memory decay thread disabled (interval 0)");
        return;
    }
    if !cfg!(feature = "jemalloc") {
        // mimalloc has no equivalent ctl in this codebase; saying so once is
        // better than a thread that spins doing nothing.
        tracing::debug!("memory decay thread not started: non-jemalloc build");
        return;
    }
    let interval = std::time::Duration::from_millis(interval_ms);
    let spawned = std::thread::Builder::new()
        .name("moon-decay".into())
        .spawn(move || {
            // First call is deferred by one interval: at startup there is
            // nothing idle yet, and the allocator is busy warming up.
            loop {
                std::thread::sleep(interval);
                if !decay_all_arenas() {
                    tracing::warn!(
                        "jemalloc arena decay ctl rejected; stopping the decay \
                         thread (freed memory will not be returned to the OS \
                         while this process is idle)"
                    );
                    return;
                }
            }
        });
    match spawned {
        Ok(_) => tracing::info!("memory decay thread started ({}ms)", interval_ms),
        Err(e) => tracing::warn!("could not start memory decay thread: {e}"),
    }
}

/// A snapshot of jemalloc's own accounting, in bytes.
///
/// Field meanings, because the differences are the whole point:
/// * `allocated` — bytes handed out to the application and not yet freed.
/// * `active` — bytes in pages backing those allocations. `active - allocated`
///   is **fragmentation**: space lost to size-class rounding and partly-used
///   runs.
/// * `resident` — bytes in pages jemalloc has mapped and touched, including
///   dirty pages it is holding for reuse. `resident - active` is **memory
///   jemalloc could give back but has not**, which is what a decay problem
///   looks like.
/// * `retained` — address space mapped but purged; costs virtual space, not
///   physical memory. Large `retained` is normal and harmless.
///
/// Anything the OS charges the process beyond `resident` is not the
/// allocator's: mmap'd files, thread stacks, the binary image.
#[cfg(feature = "jemalloc-stats")]
#[derive(Debug, Clone, Copy)]
pub struct JemallocStats {
    pub allocated: u64,
    pub active: u64,
    pub resident: u64,
    pub retained: u64,
}

#[cfg(feature = "jemalloc-stats")]
impl JemallocStats {
    /// Bytes lost to fragmentation (`active - allocated`).
    pub fn frag_bytes(&self) -> u64 {
        self.active.saturating_sub(self.allocated)
    }

    /// Redis's `allocator_frag_ratio` — `active / allocated`.
    pub fn frag_ratio(&self) -> f64 {
        if self.allocated == 0 {
            return 0.0;
        }
        self.active as f64 / self.allocated as f64
    }

    /// Dirty pages jemalloc holds but has not returned (`resident - active`).
    pub fn unreturned_bytes(&self) -> u64 {
        self.resident.saturating_sub(self.active)
    }
}

/// Read jemalloc's counters, refreshing them at most every
/// [`STATS_REFRESH_MS`].
///
/// Every `stats.*` read is served from a snapshot that only advances when
/// `epoch` is written, so a refresh is mandatory — but it must be rate
/// limited. This codebase already carries a scar from that: the admin metrics
/// scrape path documents that advancing `epoch` once a second made jemalloc's
/// internal bookkeeping grow without bound (~1 MB / 20 s), and deliberately
/// does NOT do it. INFO is operator-driven and can be polled by a monitoring
/// agent at any rate it likes, so the throttle lives here rather than trusting
/// callers.
#[cfg(feature = "jemalloc-stats")]
pub fn jemalloc_stats() -> Option<JemallocStats> {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Minimum gap between `epoch` advances.
    const STATS_REFRESH_MS: u64 = 5_000;
    static LAST_REFRESH_MS: AtomicU64 = AtomicU64::new(0);

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let last = LAST_REFRESH_MS.load(Ordering::Relaxed);
    if now_ms.saturating_sub(last) >= STATS_REFRESH_MS
        && LAST_REFRESH_MS
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        let _ = tikv_jemalloc_ctl::epoch::advance();
    }

    Some(JemallocStats {
        allocated: tikv_jemalloc_ctl::stats::allocated::read().ok()? as u64,
        active: tikv_jemalloc_ctl::stats::active::read().ok()? as u64,
        resident: tikv_jemalloc_ctl::stats::resident::read().ok()? as u64,
        retained: tikv_jemalloc_ctl::stats::retained::read().ok()? as u64,
    })
}

/// Builds without `jemalloc-stats` have no counters to report.
#[cfg(not(feature = "jemalloc-stats"))]
pub fn jemalloc_stats() -> Option<()> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ctl name is hand-written; if `MALLCTL_ARENAS_ALL` ever changes, the
    /// string would silently start addressing a real arena index instead of
    /// "all arenas" — a bug that would look like a partial fix.
    #[cfg(feature = "jemalloc")]
    #[test]
    fn ctl_name_matches_the_arenas_all_constant() {
        let expected = format!("arena.{MALLCTL_ARENAS_ALL}.decay\0");
        assert_eq!(
            ARENA_ALL_DECAY,
            expected.as_bytes(),
            "the hardcoded ctl name drifted from MALLCTL_ARENAS_ALL"
        );
    }

    /// The ctl must actually exist and be accepted by the linked jemalloc.
    /// This is the assertion that catches a jemalloc upgrade renaming or
    /// re-typing it — without it, `decay_all_arenas` could return false
    /// forever and the only symptom would be memory growth in production.
    #[cfg(feature = "jemalloc")]
    #[test]
    fn decay_ctl_is_accepted_by_the_linked_jemalloc() {
        assert!(
            decay_all_arenas(),
            "mallctl(\"arena.4096.decay\") was rejected — the ctl this fix \
             depends on is not available in the linked jemalloc"
        );
    }

    /// Disabling must not start a thread, and must not panic.
    #[test]
    fn zero_interval_is_a_no_op() {
        spawn(0);
    }
}
