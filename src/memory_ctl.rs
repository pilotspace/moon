//! Allocator introspection: the `mallctl` surface moon needs to answer "where
//! did the memory go".
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
//! # Known limitation: idle retention on Apple platforms
//!
//! moon bakes `dirty_decay_ms:1000,muzzy_decay_ms:5000,background_thread:true`
//! into `_rjem_malloc_conf` (see `main.rs`), on the understanding that jemalloc
//! hands dirty pages back about a second after they go idle. That understanding
//! does not hold everywhere.
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
//! it simply does nothing. Without that thread, jemalloc runs decay only as a
//! side effect of allocator activity in the arena being used, so a process that
//! goes quiet may never purge — exactly the shape moon is built for: a
//! long-lived cache that peaks during an index build or a compaction and then
//! idles.
//!
//! # What was measured, including the remedy that did not work
//!
//! Whether going idle reclaims is **platform- and configuration-dependent and
//! must not be assumed either way**. Churning and freeing 384 MiB:
//!
//! * Apple Silicon dev machine, macOS 15.7: reclaimed to a 3.7 MiB physical
//!   footprint with no intervention — decay ran as a side effect of the frees.
//! * GitHub macOS CI runner, same commit: retained all 386 MiB indefinitely.
//!
//! An earlier revision of this module shipped a `--memory-decay-interval-ms`
//! lever: a housekeeping thread calling `mallctl("arena.4096.decay")`, which is
//! precisely what jemalloc's own background thread invokes. **It was removed
//! after measurement showed it does not work on the platform that needs it.**
//! On the retaining CI runner, driving that ctl in a loop for 30 seconds
//! reclaimed nothing — the footprint stayed 386 MiB above baseline across three
//! independent retries — while `mallctl` itself returned success and a
//! separately-validated footprint instrument confirmed the measurement. Shipping
//! it would have meant an `unsafe` FFI block and a documented knob that silently
//! fails wherever it is actually needed.
//!
//! So there is currently **no in-process remedy** for idle retention on the
//! affected platforms. What this module provides instead is the ability to *see*
//! it: build with `--features jemalloc-stats` and watch
//! `allocator_unreturned_bytes`. If it is large and stays large while the
//! instance is quiet, that deployment is on the retaining side, and the
//! mitigations are external — restart cadence, or `MALLOC_CONF` tuning at the
//! operator's discretion. `tests/allocator_idle_decay.rs` records the finding.
//!
//! Production deployments target Linux (see `CLAUDE.md`), where the background
//! thread is compiled in and the retention has not been observed.

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
