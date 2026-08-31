//! Memory and CPU accounting: the RSS/allocator gauges, the platform-specific
//! `get_rss_bytes` implementations, and the INFO helpers that read them.
//!
//! Split out of the former single-file `metrics_setup.rs` (moon#479, file-size
//! ceiling); all platform `cfg` arms are unchanged.

use std::sync::atomic::{AtomicUsize, Ordering};

use metrics::gauge;

use crate::admin::metrics_setup::{
    DISPATCH_CROSS_READ_SPSC_TOTAL, METRICS_INITIALIZED, PIPELINE_MULTIKEY_FANOUT_TOTAL,
    PIPELINE_REMOTE_DEFER_TOTAL, TOTAL_CONNECTIONS, total_commands_sum,
};

// ── Memory metrics ──────────────────────────────────────────────────────

/// Update RSS gauge (called periodically by shard timer).
#[inline]
pub fn update_rss_bytes(rss: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    gauge!("moon_rss_bytes").set(rss as f64);
}

/// Continuously-updated `allocator_overhead_bytes` (task #58, LOW-1):
/// `RSS - tracked_sum` (DashTable+entries + vector/text/graph/lua planes +
/// PageCache + replication backlog), sampled once per 100ms tick by shard 0
/// (`persistence_tick::run_eviction_tick`) rather than recomputed on demand.
/// INFO memory and `/metrics` both read this single published atomic so the
/// two surfaces never drift relative to each other between reads.
static ALLOCATOR_OVERHEAD_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Publish the allocator-overhead figure computed by the 100ms tick.
/// Observability only -- never consulted by eviction or budget gating.
#[inline]
pub fn update_allocator_overhead_bytes(bytes: usize) {
    ALLOCATOR_OVERHEAD_BYTES.store(bytes, Ordering::Relaxed);
    if METRICS_INITIALIZED.load(Ordering::Relaxed) {
        gauge!("moon_memory_bytes", "kind" => "allocator_overhead").set(bytes as f64);
    }
}

/// Read the last-published `allocator_overhead_bytes` (INFO memory / MEMORY
/// STATS). Lock-free; lags at most one 100ms tick.
#[inline]
pub fn get_allocator_overhead_bytes() -> usize {
    ALLOCATOR_OVERHEAD_BYTES.load(Ordering::Relaxed)
}

// ── Memory helpers ──────────────────────────────────────────────────────

/// Query jemalloc `stats.resident` via raw `mallctl` FFI.
///
/// Returns the total bytes of physical memory mapped by the allocator,
/// or 0 on failure. This is more accurate than `/proc/self/statm` which
/// can be inflated by `mmap` regions that the allocator doesn't own (e.g.
/// WAL segment files, io_uring buffers) and which reports incorrect values
/// inside certain container/VM environments (OrbStack, Docker with cgroups v2).
///
/// **Zero-allocation**: calls `mallctl` directly — no CString, no heap churn.
/// Requires `epoch` advance first so the stats snapshot is fresh.
/// Read process RSS from /proc/self/statm (Linux only).
/// Returns bytes, or 0 on failure / non-Linux.
///
/// **True zero-allocation**: uses raw `libc::open`/`read`/`close` with a
/// static path and stack buffer. Avoids `std::fs::File::open` which
/// allocates internally (`CString::new` for the syscall path).
///
/// Note: jemalloc `mallctl("stats.resident")` was tried but calling
/// `mallctl("epoch")` every second to refresh stats causes jemalloc to
/// allocate internal bookkeeping memory that grows unbounded (~1MB/20s).
#[cfg(target_os = "linux")]
pub fn get_rss_bytes() -> u64 {
    // SAFETY: c"/proc/self/statm" is a valid C string literal.
    // open() with O_RDONLY on /proc is always safe.
    let fd = unsafe { libc::open(c"/proc/self/statm".as_ptr(), libc::O_RDONLY) };
    if fd < 0 {
        return 0;
    }
    let mut buf = [0u8; 128];
    // SAFETY: buf is valid, fd is open, read() returns bytes written.
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len()) };
    // SAFETY: close() on a valid fd is always safe.
    unsafe { libc::close(fd) };
    if n <= 0 {
        return 0;
    }
    // statm format: "size resident shared text lib data dt" (pages)
    // Field 1 (resident) is what we need.
    let s = &buf[..n as usize];
    let mut fields = s.split(|&b| b == b' ');
    let _size = fields.next(); // skip field 0
    if let Some(rss_field) = fields.next() {
        // Parse ASCII digits directly — no String allocation.
        let mut pages: u64 = 0;
        for &b in rss_field {
            if b.is_ascii_digit() {
                pages = pages * 10 + (b - b'0') as u64;
            }
        }
        let page_size = page_size_cached();
        return pages * page_size;
    }
    0
}

/// Cached page size to avoid repeated syscall.
#[cfg(target_os = "linux")]
fn page_size_cached() -> u64 {
    use std::sync::atomic::AtomicU64;
    static PAGE_SIZE: AtomicU64 = AtomicU64::new(0);
    let cached = PAGE_SIZE.load(Ordering::Relaxed);
    if cached != 0 {
        return cached;
    }
    // SAFETY: sysconf(_SC_PAGESIZE) is always safe and returns a positive value.
    let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
    PAGE_SIZE.store(ps, Ordering::Relaxed);
    ps
}

/// Read process RSS on macOS via Mach `task_info` API.
/// Returns bytes, or 0 on failure.
#[cfg(target_os = "macos")]
pub fn get_rss_bytes() -> u64 {
    macos_task_memory_info().1
}

/// Returns (virtual_size, resident_size) for the current process on macOS.
///
/// Tries `MACH_TASK_BASIC_INFO` (flavor 20) first; falls back to
/// `TASK_VM_INFO` (flavor 22) which is available on macOS 10.9+ and works
/// on all tested kernel versions including 24.x (Sequoia).
#[cfg(target_os = "macos")]
pub fn macos_task_memory_info() -> (u64, u64) {
    // Mach kernel API types and functions for querying task memory info.
    // SAFETY: These are stable Mach kernel ABI functions available on all macOS versions.
    unsafe extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(target: u32, flavor: u32, info: *mut u8, count: *mut u32) -> i32;
    }

    // ── Try MACH_TASK_BASIC_INFO (flavor 20) ─────────────────────────────
    // Layout: policy(i32), pad(i32), virtual_size(u64), resident_size(u64), ...
    // Total = 10 natural_t (40 bytes).
    const MACH_TASK_BASIC_INFO: u32 = 20;
    const MACH_TASK_BASIC_INFO_COUNT: u32 = 10;

    let mut info = [0u8; 40];
    let mut count = MACH_TASK_BASIC_INFO_COUNT;
    // SAFETY: mach_task_self() returns current task port. info is 40 bytes,
    // task_info writes at most `count` natural_t values.
    let kr = unsafe {
        task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            info.as_mut_ptr(),
            &mut count,
        )
    };
    if kr == 0 {
        let vsz = u64::from_ne_bytes(info[8..16].try_into().unwrap_or([0; 8]));
        let rss = u64::from_ne_bytes(info[16..24].try_into().unwrap_or([0; 8]));
        return (vsz, rss);
    }

    // ── Fallback: TASK_VM_INFO (flavor 22) ───────────────────────────────
    // Available macOS 10.9+. Layout: virtual_size(u64) at offset 0,
    // phys_footprint(u64) at offset 16. Count = 68 natural_t (272 bytes).
    const TASK_VM_INFO: u32 = 22;
    const TASK_VM_INFO_COUNT: u32 = 68;

    let mut vm_info = [0u8; 272];
    let mut vm_count = TASK_VM_INFO_COUNT;
    // SAFETY: vm_info is 272 bytes = TASK_VM_INFO_COUNT × 4.
    let kr2 = unsafe {
        task_info(
            mach_task_self(),
            TASK_VM_INFO,
            vm_info.as_mut_ptr(),
            &mut vm_count,
        )
    };
    if kr2 == 0 {
        let vsz = u64::from_ne_bytes(vm_info[0..8].try_into().unwrap_or([0; 8]));
        let rss = u64::from_ne_bytes(vm_info[16..24].try_into().unwrap_or([0; 8]));
        return (vsz, rss);
    }

    (0, 0)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn get_rss_bytes() -> u64 {
    0
}

/// Real-footprint measurement lives in its own module; re-exported here
/// so `metrics_setup::footprint_ratio` and friends keep resolving.
pub use crate::admin::footprint::{
    capture_footprint_baseline, footprint_baseline_bytes, footprint_correction,
    footprint_correction_refreshes, footprint_ratio, footprint_sample_bytes,
    process_footprint_bytes, publish_footprint_sample, refresh_footprint_correction,
};

// ── INFO helpers ────────────────────────────────────────────────────────

/// Total commands processed since server start (for INFO Stats).
#[inline]
pub fn total_commands_processed() -> u64 {
    total_commands_sum()
}

/// Total connections received since server start (for INFO Stats).
#[inline]
pub fn total_connections_received() -> u64 {
    TOTAL_CONNECTIONS.load(Ordering::Relaxed)
}

/// Total cross-shard commands dispatched via the SPSC slow path.
/// Covers both read and write commands that bypass the fast path.
/// Always accurate — does not require Prometheus to be initialised.
#[inline]
pub fn total_dispatch_cross_spsc() -> u64 {
    DISPATCH_CROSS_READ_SPSC_TOTAL.load(Ordering::Relaxed)
}

/// Total pipeline batches cut short by the moon#507 ordering guard (moon#513).
///
/// One per extra dispatch/await boundary. Always accurate — does not require
/// Prometheus to be initialised.
#[inline]
pub fn total_pipeline_remote_defer() -> u64 {
    PIPELINE_REMOTE_DEFER_TOTAL.load(Ordering::Relaxed)
}

/// Total spanning multi-key reads fanned out into the slotted batch (moon#513
/// A2a). Always accurate — does not require Prometheus to be initialised.
#[inline]
pub fn total_pipeline_multikey_fanout() -> u64 {
    PIPELINE_MULTIKEY_FANOUT_TOTAL.load(Ordering::Relaxed)
}

/// Read process CPU usage via `getrusage(RUSAGE_SELF)`.
///
/// Returns `(used_cpu_sys, used_cpu_user)` in seconds (f64).
/// On non-Linux platforms returns `(0.0, 0.0)`.
#[cfg(unix)]
pub fn get_cpu_usage() -> (f64, f64) {
    use std::mem::MaybeUninit;
    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` writes a valid `rusage` struct to the pointer on
    // success (returns 0). RUSAGE_SELF is always valid. We only read the
    // struct after confirming success.
    let ret = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if ret == 0 {
        // SAFETY: getrusage returned 0, so the struct is fully initialized.
        let ru = unsafe { usage.assume_init() };
        let sys = ru.ru_stime.tv_sec as f64 + ru.ru_stime.tv_usec as f64 / 1_000_000.0;
        let user = ru.ru_utime.tv_sec as f64 + ru.ru_utime.tv_usec as f64 / 1_000_000.0;
        (sys, user)
    } else {
        (0.0, 0.0)
    }
}

#[cfg(not(unix))]
pub fn get_cpu_usage() -> (f64, f64) {
    (0.0, 0.0)
}
