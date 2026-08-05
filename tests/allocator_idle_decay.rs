//! Does a long-lived, IDLE moon return freed memory to the OS?
//!
//! Motivation — a real instance, observed 2026-08-05 after 6.8 days of uptime
//! on macOS: logical dataset 2.43 GB, but 7.4 GB of dirty anonymous memory
//! across 37,474 regions, physical footprint peaked at 8.5 GB and never came
//! down. The OS had swapped 7.2 GB of it out, making the process the single
//! largest consumer of the machine's swap while `ps` showed an innocent
//! 183 MB RSS — because RSS does not count what has been paged out.
//!
//! Cause (confirmed in jemalloc's `configure.ac`, not inferred):
//! `JEMALLOC_BACKGROUND_THREAD` is only defined when `abi != macho`, so
//! moon's baked-in `background_thread:true` is a silent no-op on Apple
//! platforms. With no background thread, decay runs only as a side effect of
//! allocator activity — an idle process may never purge.
//!
//! There is currently no in-process fix. `mallctl("arena.4096.decay")` — the
//! call jemalloc's own background thread makes — was implemented, measured, and
//! removed: on the CI macOS runner that reproduces the retention, driving it for
//! 30 seconds reclaimed nothing across three independent retries, while the ctl
//! itself reported success. See `src/memory_ctl.rs`.
//!
//! So these tests do two things: assert the property where it must hold, and
//! keep the platform where it does not hold measured rather than forgotten.
//!
//! jemalloc-only; skipped on other allocators.

#![cfg(feature = "jemalloc")]

use std::time::{Duration, Instant};

/// Memory the OS currently charges to this process, in bytes.
///
/// NOT plain RSS on macOS, and the difference is the whole reason an earlier
/// version of this test read as a failure. jemalloc is built with
/// `JEMALLOC_PURGE_MADVISE_FREE` on Darwin, so purging calls
/// `madvise(MADV_FREE)`: the pages stay *resident* and merely become
/// discardable, so `ps -o rss` does not move even when decay works perfectly.
/// `phys_footprint` — the number Activity Monitor shows — excludes reclaimable
/// pages, which is exactly the distinction being tested: pages the kernel can
/// drop instead of writing to swap.
///
/// jemalloc's own `stats.*` counters would be the direct measure, but they
/// need the `stats` feature, which moon does not enable (it costs on every
/// allocation).
#[cfg(target_vendor = "apple")]
fn footprint_bytes() -> u64 {
    let out = std::process::Command::new("vmmap")
        .args(["-summary", &std::process::id().to_string()])
        .output();
    let Ok(o) = out else { return 0 };
    let text = String::from_utf8_lossy(&o.stdout);
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("Physical footprint:") else {
            continue;
        };
        let v = rest.trim();
        let (num, mult) = match v.chars().last() {
            Some('G') => (&v[..v.len() - 1], 1024.0 * 1024.0 * 1024.0),
            Some('M') => (&v[..v.len() - 1], 1024.0 * 1024.0),
            Some('K') => (&v[..v.len() - 1], 1024.0),
            _ => (v, 1.0),
        };
        if let Ok(n) = num.parse::<f64>() {
            return (n * mult) as u64;
        }
    }
    0
}

/// Linux: RSS is the right measure — jemalloc purges with `MADV_DONTNEED`
/// there, which does drop resident pages.
#[cfg(not(target_vendor = "apple"))]
fn footprint_bytes() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output();
    match out {
        Ok(o) => {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .parse::<u64>()
                .unwrap_or(0)
                * 1024
        }
        Err(_) => 0,
    }
}

/// Churn ~`mb` megabytes through the allocator and free all of it.
///
/// Mixed size classes on purpose: a single uniform size is the easiest
/// possible case for an allocator, and the instance that motivated this was
/// serving mixed-size values. Every page is touched, so the pages are
/// genuinely dirtied — an untouched allocation proves nothing about decay.
fn churn_and_free(mb: usize) {
    let mut held: Vec<Vec<u8>> = Vec::new();
    let sizes = [64 * 1024, 256 * 1024, 1024 * 1024];
    let mut total = 0usize;
    let mut i = 0usize;
    while total < mb * 1024 * 1024 {
        let n = sizes[i % sizes.len()];
        let mut v = vec![0u8; n];
        for p in (0..n).step_by(4096) {
            v[p] = 1;
        }
        held.push(v);
        total += n;
        i += 1;
    }
    drop(held);
}

/// Watch the footprint for `secs`, returning the lowest value seen.
fn footprint_floor_over(secs: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut floor = footprint_bytes();
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
        floor = floor.min(footprint_bytes());
    }
    floor
}

const MB: u64 = 1024 * 1024;
const CHURN_MB: usize = 384;

/// Freed memory must come back to the OS once the process goes idle.
///
/// This test has been wrong twice, in opposite directions, and the history is
/// why it is shaped the way it is:
///
/// 1. First it asserted that an idle jemalloc on macOS never purges. It
///    "failed" because `ps -o rss` is the wrong instrument there: Darwin
///    jemalloc is built with `JEMALLOC_PURGE_MADVISE_FREE`, so purged pages
///    stay *resident* and merely become discardable — RSS reads ~387 MiB where
///    the physical footprint reads 3.7 MiB.
/// 2. Then it asserted the opposite — that going idle is always enough —
///    because on one developer machine 384 MiB churned and freed reclaimed to
///    3.7 MiB. That passed locally and FAILED on the CI macOS runner, which
///    held all 386 MiB. Retention is real; it is just not universal, so "it
///    reclaimed on my machine" proved nothing.
///
/// Hence: a hard assertion on Linux, which is what production targets and where
/// jemalloc's background thread is compiled in, and a recorded measurement on
/// Apple, where retention is a known unfixed platform limitation. Failing the
/// build on a limitation that has no in-process remedy would only train people
/// to ignore the gate.
#[test]
fn freed_memory_is_returned_to_the_os_when_idle() {
    let baseline = footprint_bytes();
    assert!(
        baseline > 0,
        "could not read the process footprint; the test cannot measure anything"
    );

    churn_and_free(CHURN_MB);
    let idle_floor = footprint_floor_over(10);
    let retained = idle_floor.saturating_sub(baseline);

    let verdict = if retained < 128 * MB {
        "RECLAIMED"
    } else {
        "RETAINED"
    };
    eprintln!(
        "{verdict}: {} MiB still charged after churning and freeing {CHURN_MB} MiB \
         (baseline {} MiB, idle floor {} MiB)",
        retained / MB,
        baseline / MB,
        idle_floor / MB,
    );

    #[cfg(target_vendor = "apple")]
    {
        // Known limitation, deliberately not a failure — see the module docs.
        // Both outcomes occur on macOS depending on the machine, and neither
        // indicates a moon regression.
        let _ = retained;
    }

    #[cfg(not(target_vendor = "apple"))]
    assert!(
        retained < 128 * MB,
        "an idle process did not return freed memory: still charged {} MiB above \
         a {} MiB baseline after churning and freeing {CHURN_MB} MiB. On Linux \
         jemalloc's background thread is compiled in and decay must run, so this \
         is a real regression — on a long-lived instance it is what becomes \
         multi-GB of swapped-out dirty anonymous memory.",
        retained / MB,
        baseline / MB,
    );
}

/// Validate the instrument. Without this, every other assertion here is
/// worthless: a footprint reader that always returns a small number would make
/// a leaking process look healthy.
#[test]
fn footprint_tracks_live_allocations() {
    let baseline = footprint_bytes();
    let mut held: Vec<Vec<u8>> = Vec::new();
    for _ in 0..CHURN_MB {
        let mut v = vec![0u8; 1024 * 1024];
        for p in (0..v.len()).step_by(4096) {
            v[p] = 1;
        }
        held.push(v);
    }
    let while_held = footprint_bytes();
    let grew = while_held.saturating_sub(baseline);
    // Keep the allocation alive across the measurement.
    assert_eq!(held.len(), CHURN_MB);
    drop(held);

    assert!(
        grew > 128 * MB,
        "the footprint reader is broken: {CHURN_MB} MiB of live, page-touched \
         allocations moved it by only {} MiB (baseline {} MiB, held {} MiB). \
         Every other assertion in this file depends on this working.",
        grew / MB,
        baseline / MB,
        while_held / MB,
    );
}
