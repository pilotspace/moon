//! Real process footprint, and the correction it feeds into `maxmemory`.
//!
//! Split out of `metrics_setup` because it is a self-contained measurement
//! concern with a platform-specific reader, its own unit tests, and one
//! consumer contract: `footprint_ratio` is the only number the eviction budget
//! is allowed to scale by.
//!
//! Re-exported from `metrics_setup` so existing
//! `crate::admin::metrics_setup::footprint_ratio` call sites keep resolving.

use std::sync::atomic::{AtomicU64, Ordering};

/// Bytes the OS charges this process, INCLUDING pages pushed to swap.
///
/// This is deliberately not `get_rss_bytes()`. On macOS that returns
/// `resident_size`, which counts only what is currently in RAM — so a process
/// whose heap has been swapped out reports a SMALL number precisely when it is
/// consuming the most memory. Measured on the live :6381 instance: 439 MB
/// resident vs 10.1 GB `phys_footprint`, 9.9 GB of it swapped. Anything that
/// makes a capacity decision must use this, not RSS.
///
/// Returns 0 when the platform cannot answer, which callers must treat as
/// "unknown" and fall back to allocator accounting — never as "zero bytes".
#[cfg(target_os = "macos")]
fn platform_footprint_bytes() -> u64 {
    // SAFETY: identical contract to `macos_task_memory_info` above — stable
    // Mach ABI, and `vm_info` is 272 bytes = TASK_VM_INFO_COUNT x 4. Queried
    // directly rather than via that function because it tries
    // MACH_TASK_BASIC_INFO first, which succeeds and therefore never reaches
    // the TASK_VM_INFO path where `phys_footprint` lives.
    unsafe extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(target: u32, flavor: u32, info: *mut u8, count: *mut u32) -> i32;
    }
    const TASK_VM_INFO: u32 = 22;
    const TASK_VM_INFO_COUNT: u32 = 68;
    let mut vm_info = [0u8; 272];
    let mut vm_count = TASK_VM_INFO_COUNT;
    // SAFETY: see above.
    let kr = unsafe {
        task_info(
            mach_task_self(),
            TASK_VM_INFO,
            vm_info.as_mut_ptr(),
            &mut vm_count,
        )
    };
    if kr == 0 && vm_count >= PHYS_FOOTPRINT_MIN_COUNT {
        return u64::from_ne_bytes(
            vm_info[PHYS_FOOTPRINT_OFFSET..PHYS_FOOTPRINT_OFFSET + 8]
                .try_into()
                .unwrap_or([0; 8]),
        );
    }
    0
}

/// Byte offset of `phys_footprint` within `task_vm_info_data_t`.
///
/// Derived from the XNU layout (`osfmk/mach/task_info.h`), all fields 8 bytes
/// except the two `integer_t`s: virtual_size 0, region_count 8, page_size 12,
/// resident_size 16, resident_size_peak 24, device 32, device_peak 40,
/// internal 48, internal_peak 56, external 64, external_peak 72, reusable 80,
/// reusable_peak 88, purgeable_volatile_pmap 96, purgeable_volatile_resident
/// 104, purgeable_volatile_virtual 112, compressed 120, compressed_peak 128,
/// compressed_lifetime 136, **phys_footprint 144**.
///
/// Verified empirically rather than by reading the header: mapping a clean
/// file-backed region moves `resident_size` (offset 16) by the mapped size and
/// leaves offset 144 flat, which is exactly how the two fields differ —
/// footprint excludes clean file pages. An earlier revision of this function
/// read offset 16 and therefore reported RSS under the name `phys_footprint`,
/// silently disabling the whole correction on a swapped-out process.
#[cfg(target_os = "macos")]
const PHYS_FOOTPRINT_OFFSET: usize = 144;

/// Minimum `natural_t` count the kernel must return for `phys_footprint` to be
/// populated: (144 + 8) / 4. A shorter reply means an older revision of the
/// struct that predates the field — read it anyway and you get uninitialised
/// stack bytes as a memory figure.
#[cfg(target_os = "macos")]
const PHYS_FOOTPRINT_MIN_COUNT: u32 = ((PHYS_FOOTPRINT_OFFSET + 8) / 4) as u32;

/// Bytes the OS charges this process. On Linux RSS is the right measure —
/// swapped pages are not charged the same way, and `VmRSS` is what the OOM
/// killer and cgroup limits act on.
#[cfg(target_os = "linux")]
fn platform_footprint_bytes() -> u64 {
    crate::admin::metrics_setup::get_rss_bytes()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn platform_footprint_bytes() -> u64 {
    0
}

/// Platform reads performed so far. Test-only: the counter exists so a test
/// can assert that the WRITE path performs none of these, which is the whole
/// content of the fix. The shipped build must not pay for its own
/// instrumentation, hence `cfg(test)` rather than a feature flag.
#[cfg(test)]
static PLATFORM_READS: AtomicU64 = AtomicU64::new(0);

/// Read the platform's footprint figure. THREE SYSCALLS on Linux
/// (`open`/`read`/`close` on `/proc/self/statm`), one `task_info` trap on
/// macOS. Call from periodic chores, from INFO, and from startup — never from
/// a command path. [`footprint_ratio`] is the per-write consumer and reads
/// [`FOOTPRINT_SAMPLE`] instead.
pub fn process_footprint_bytes() -> u64 {
    #[cfg(test)]
    PLATFORM_READS.fetch_add(1, Ordering::Relaxed);
    platform_footprint_bytes()
}

/// Most recent footprint sample, published by the 1s shard-0 chore.
///
/// `evict_to_budget` runs on the WRITE path and calls [`footprint_ratio`] on
/// every write, so that function must never touch the platform reader: on
/// Linux `process_footprint_bytes()` is `open("/proc/self/statm")` + `read` +
/// `close`, three syscalls. Measured cost of doing that per write, v0.8.5 vs
/// the tip that introduced it: SET at c=8 P=16 fell 1.34M -> 0.57M ops/s, a
/// 2.4x collapse, with reads unaffected. `timers.rs` already samples RSS once
/// a second and says why in its own comment ("to reduce /proc/self/statm
/// open/read/close churn"); this atomic is how the write path shares that
/// sample instead of re-reading it.
///
/// 0 means "never sampled", which [`footprint_ratio`] treats as unknown and
/// therefore neutral — the correction only ever SHRINKS a budget, so an
/// unknown must not shrink it.
static FOOTPRINT_SAMPLE: AtomicU64 = AtomicU64::new(0);

/// Publish a footprint sample. Called from the periodic chore that already
/// pays for the platform read, never from a command path.
pub fn publish_footprint_sample(bytes: u64) {
    if bytes > 0 {
        FOOTPRINT_SAMPLE.store(bytes, Ordering::Relaxed);
    }
}

/// The last published footprint sample, 0 when never sampled.
pub fn footprint_sample_bytes() -> u64 {
    FOOTPRINT_SAMPLE.load(Ordering::Relaxed)
}

/// How far real footprint exceeds what the allocator says is live.
///
/// Returns 1.0 when the platform cannot measure footprint, when nothing is
/// allocated yet, or when footprint is below accounted memory — the value is
/// only ever used to make a budget SMALLER, so an unknown must not shrink it.
///
/// Reads the published sample rather than measuring. This is called per write;
/// see [`FOOTPRINT_SAMPLE`] for what measuring here cost. Staleness up to the
/// 1s sample interval is the right trade: the correction scales a budget in
/// the GB range, and a budget that reacts one second late is invisible next to
/// a write path that is 2.4x slower.
pub fn footprint_ratio(used_memory: u64) -> f64 {
    // Cheap guard FIRST. This check already existed inside `ratio_from`, but
    // the expensive reader was passed as its argument — Rust evaluates
    // arguments eagerly, so the syscalls ran before the guard could skip them.
    // A guard that the caller has already paid to get past is not a guard.
    if used_memory < RATIO_NOISE_FLOOR {
        return 1.0;
    }
    ratio_from(
        FOOTPRINT_SAMPLE.load(Ordering::Relaxed),
        FOOTPRINT_BASELINE.load(Ordering::Relaxed),
        used_memory,
    )
}

/// The published budget correction, as `f64` bits. 1.0 = no correction.
///
/// `evict_to_budget` runs on the WRITE path, so the correction it applies must
/// cost one relaxed load. Computing it there instead cost, per SET: three
/// syscalls for the footprint (see [`FOOTPRINT_SAMPLE`]) AND a
/// `logical_used_memory_bytes()` that sums four atomics per shard and takes a
/// read lock on the GLOBAL replication state — a cross-core round trip on a
/// path whose whole design rule is "per-shard locks only".
static CORRECTION_BITS: AtomicU64 = AtomicU64::new(0);

/// The correction the eviction budget should divide by. Neutral (1.0) until
/// the first sample lands, which under-corrects for up to a second rather than
/// over-evicting on an unmeasured process.
#[inline]
pub fn footprint_correction() -> f64 {
    let bits = CORRECTION_BITS.load(Ordering::Relaxed);
    if bits == 0 {
        return 1.0;
    }
    f64::from_bits(bits)
}

/// Re-measure the footprint and republish the correction.
///
/// Called once a second from shard 0's chore — the only place that pays for
/// the platform read and the instance-wide accounting sum. `footprint` is
/// passed in because the caller often already has it (on Linux it is the same
/// `/proc/self/statm` read that feeds the RSS gauge).
///
/// Staleness bound is the caller's tick. That is the right trade: the
/// correction scales a budget in the GB range, so reacting a second late is
/// invisible, while measuring per write is a 2.4x throughput collapse.
///
/// A `footprint` of 0 means the platform could not answer — Windows has no
/// reader at all, and the Linux/macOS ones can fail transiently. Call it
/// anyway: the zero is refused as a sample (so a failed read cannot erase a
/// good one), the correction resolves to its neutral 1.0, and the refresh
/// counter still advances. That last part is the point — the counter answers
/// "is the chore running", which is a different question from "can this
/// platform measure", and conflating them is what made this function's first
/// version untestable on Windows.
pub fn refresh_footprint_correction(footprint: u64) {
    publish_footprint_sample(footprint);
    let used = crate::admin::metrics_setup::logical_used_memory_bytes() as u64;
    CORRECTION_BITS.store(footprint_ratio(used).to_bits(), Ordering::Relaxed);
    CORRECTION_REFRESHES.fetch_add(1, Ordering::Relaxed);
}

/// How many times the correction has been republished.
///
/// Exposed as `maxmemory_footprint_samples` in INFO. The correction reads 1.00
/// on any instance below the noise floor, so the ratio alone cannot tell an
/// operator whether the sampler is alive or wedged — a stuck sampler and a
/// healthy small instance print the same thing. A counter that stops advancing
/// says which. It is also the only end-to-end proof that the chore is wired on
/// BOTH runtimes: the shard loop has a monoio arm and a tokio arm, and a
/// correction that silently never refreshes on one of them would restore the
/// swap-death bug #478 fixed, invisibly.
///
/// Counts chore TICKS, not successful measurements. On a platform with no
/// footprint reader (Windows) it advances while the correction stays 1.00
/// forever, which is the documented inert state — pair it with
/// `maxmemory_footprint_correction` to tell inert from active.
static CORRECTION_REFRESHES: AtomicU64 = AtomicU64::new(0);

/// Number of correction refreshes so far.
pub fn footprint_correction_refreshes() -> u64 {
    CORRECTION_REFRESHES.load(Ordering::Relaxed)
}

/// Accounted memory below which the footprint correction is inert.
///
/// A process costs the OS tens of MB before it holds a single key — binary,
/// thread stacks, arena metadata. That cost is FIXED, so dividing it by a
/// small dataset produces a large but meaningless ratio. Below this floor the
/// correction would be measuring the process, not the data.
const RATIO_NOISE_FLOOR: u64 = 64 * 1024 * 1024;

/// Pure core of [`footprint_ratio`] — testable without a live process.
///
/// `baseline` is the footprint recorded before any dataset existed; the
/// correction prices only the MARGINAL cost of the data on top of it.
fn ratio_from(footprint: u64, baseline: u64, used_memory: u64) -> f64 {
    if used_memory < RATIO_NOISE_FLOOR {
        return 1.0;
    }
    let marginal = footprint.saturating_sub(baseline);
    if marginal == 0 {
        return 1.0;
    }
    let r = marginal as f64 / used_memory as f64;
    // Clamp: below 1.0 is meaningless here, and an absurd ratio (a bad read,
    // or a huge transient) must not collapse the budget to nothing.
    r.clamp(1.0, 8.0)
}

/// Footprint charged to the process before any dataset was loaded.
static FOOTPRINT_BASELINE: AtomicU64 = AtomicU64::new(0);

/// Record the pre-dataset footprint. Call once during startup, after the
/// allocator and runtime are up but BEFORE persistence recovery loads data —
/// anything resident at that moment is fixed overhead the dataset must not be
/// charged for.
///
/// Leaving it unset (0) is safe: the correction then measures whole-process
/// footprint, which over-corrects slightly rather than under-correcting.
pub fn capture_footprint_baseline() {
    let now = process_footprint_bytes();
    FOOTPRINT_BASELINE.store(now, Ordering::Relaxed);
    // Seed the sample as well, so writes arriving before the first periodic
    // chore compare against a real reading rather than the unsampled 0.
    publish_footprint_sample(now);
}

/// The recorded pre-dataset footprint, 0 when never captured.
pub fn footprint_baseline_bytes() -> u64 {
    FOOTPRINT_BASELINE.load(Ordering::Relaxed)
}

#[cfg(test)]
mod footprint_tests {
    use super::*;

    /// Serialises the tests that read [`PLATFORM_READS`] or write
    /// [`FOOTPRINT_SAMPLE`]. Both are process-global, and `cargo test` runs
    /// these threads concurrently — without this the counter assertions would
    /// be timing-dependent, which is exactly the kind of half-flaky gate that
    /// proves nothing.
    static TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    #[test]
    fn correction_is_neutral_until_first_sampled() {
        // An unmeasured process must not be corrected. 1.0 is the only safe
        // default: the correction divides the budget, so anything above 1.0
        // here would evict on the strength of a measurement nobody took.
        //
        // `CORRECTION_BITS` is process-global and
        // `correction_round_trips_a_published_ratio` writes 2.5 into it, so
        // this must hold TEST_LOCK like every other test that touches a
        // shared atomic here — without it the two race and this assertion
        // reads the other test's 2.5 (~1 run in 6, measured).
        let _guard = TEST_LOCK.lock();
        assert_eq!(f64::from_bits(0u64).to_bits(), 0);
        let saved = CORRECTION_BITS.swap(0, Ordering::Relaxed);
        assert_eq!(footprint_correction(), 1.0);
        CORRECTION_BITS.store(saved, Ordering::Relaxed);
    }

    #[test]
    fn correction_round_trips_a_published_ratio() {
        let _guard = TEST_LOCK.lock();
        let saved = CORRECTION_BITS.swap(2.5f64.to_bits(), Ordering::Relaxed);
        assert_eq!(footprint_correction(), 2.5);
        CORRECTION_BITS.store(saved, Ordering::Relaxed);
    }

    #[test]
    fn ratio_reads_the_published_sample_never_the_platform() {
        // THE regression. `evict_to_budget` calls `footprint_ratio` on every
        // write; before this fix that call reached
        // `process_footprint_bytes()`, i.e. open+read+close on
        // /proc/self/statm, three syscalls per SET. Measured cost on the
        // moon-dev VM, v0.8.5 vs the tip that introduced it: SET c=8 P=16
        // 1.36M -> 0.58M ops/s (-57%), SET c=1 P=1 159K -> 134K (-16%), GET
        // unaffected in both. The read is not skippable by a cheap guard
        // either: the `maxmemory == 0` early return never fires because Moon
        // auto-sets maxmemory to 75% of RAM.
        //
        // Two assertions, and BOTH are needed. The count proves no syscall
        // happened; the value proves the function still answers from the
        // sample rather than having been neutered into a constant.
        let _g = TEST_LOCK.lock();
        const GB: u64 = 1024 * 1024 * 1024;
        publish_footprint_sample(400 * GB);
        let before = PLATFORM_READS.load(Ordering::Relaxed);
        let mut last = 0.0;
        for _ in 0..1000 {
            last = footprint_ratio(100 * GB);
        }
        assert_eq!(
            PLATFORM_READS.load(Ordering::Relaxed),
            before,
            "footprint_ratio read the platform — that is 3 syscalls on every \
             write through evict_to_budget"
        );
        assert_eq!(
            last, 4.0,
            "the ratio must still be computed from the published sample \
             (400 GB over 100 GB accounted); a constant 1.0 here would mean \
             the correction was disabled rather than made cheap"
        );
    }

    #[test]
    fn below_the_floor_costs_nothing_at_all() {
        // The noise-floor guard already existed — inside `ratio_from`, with
        // `process_footprint_bytes()` passed as its argument. Rust evaluates
        // arguments eagerly, so the syscalls ran to completion and only then
        // did the guard decide they were unnecessary. A guard the caller has
        // already paid to get past is not a guard; it must be checked first.
        let _g = TEST_LOCK.lock();
        let before = PLATFORM_READS.load(Ordering::Relaxed);
        for _ in 0..1000 {
            assert_eq!(footprint_ratio(4 * 1024 * 1024), 1.0);
        }
        assert_eq!(
            PLATFORM_READS.load(Ordering::Relaxed),
            before,
            "an inert correction still paid for a platform read"
        );
    }

    #[test]
    fn an_unsampled_process_is_neutral_not_zero() {
        // 0 means "never sampled", and the correction only ever SHRINKS a
        // budget. Treating an unknown as a real reading of zero bytes would
        // make `marginal` 0 and the ratio 1.0 by luck; assert it explicitly so
        // a future change to `ratio_from` cannot turn unknown into "evict".
        let _g = TEST_LOCK.lock();
        const GB: u64 = 1024 * 1024 * 1024;
        FOOTPRINT_SAMPLE.store(0, Ordering::Relaxed);
        assert_eq!(footprint_ratio(100 * GB), 1.0);
        // A zero is also refused as a sample, so a failed platform read
        // cannot erase a good one.
        publish_footprint_sample(400 * GB);
        publish_footprint_sample(0);
        assert_eq!(footprint_sample_bytes(), 400 * GB);
    }

    #[test]
    fn ratio_is_one_when_nothing_allocated() {
        // Divide-by-zero guard. The ratio only ever SHRINKS a budget, so an
        // unmeasurable state must return the neutral value, never 0 or inf.
        assert_eq!(footprint_ratio(0), 1.0);
    }

    #[test]
    fn ratio_never_below_one() {
        // Footprint below accounted memory is meaningless for our purposes
        // (and happens transiently right after a large free). Shrinking the
        // budget on that basis would evict for no reason.
        let huge = u64::MAX / 2;
        assert!(
            footprint_ratio(huge) >= 1.0,
            "a below-1 ratio would INFLATE the eviction budget"
        );
    }

    #[test]
    fn ratio_is_clamped_against_absurd_readings() {
        // A bad task_info read or a huge transient must not collapse the
        // budget to near zero and evict the entire keyspace.
        assert!(
            ratio_from(u64::MAX, 0, 64 * 1024 * 1024) <= 8.0,
            "an unclamped ratio on a 1-byte denominator would drive the \
             budget to ~0 and evict everything"
        );
    }

    #[test]
    fn ratio_is_inert_below_the_noise_floor() {
        // Regression (CI run 31683128246, `test_case_e_cross_db_copy_oom`):
        // a process's footprint is never zero — binary, thread stacks, arena
        // metadata and the test harness itself cost tens of MB before a single
        // key exists. Dividing that FIXED cost by a small dataset yields a
        // meaningless multi-x ratio, which shrank a 2 MB maxmemory budget to
        // nothing and OOM-rejected writes that must succeed. Below the floor
        // the correction is inert.
        assert_eq!(
            footprint_ratio(4 * 1024 * 1024),
            1.0,
            "fixed process overhead dominates a small dataset — correcting on \
             that basis evicts a nearly-empty instance"
        );
    }

    #[test]
    fn ratio_excludes_the_startup_baseline() {
        // The correction must price only what the DATASET costs beyond
        // accounting, not the process's constant overhead. 1 GB of accounted
        // data inside a 1.5 GB process that started at 0.5 GB has a marginal
        // footprint of exactly 1 GB — a ratio of 1.0, no correction owed.
        const GB: u64 = 1024 * 1024 * 1024;
        assert_eq!(ratio_from(3 * GB / 2, GB / 2, GB), 1.0);
    }

    #[test]
    fn ratio_reproduces_the_measured_live_gap() {
        // The instance this fix exists for: 4.21 GB accounted inside a 9.7 GB
        // footprint that started around 0.1 GB. The correction must survive
        // baseline subtraction — this is the case it MUST still fire on.
        const GB: f64 = (1024 * 1024 * 1024) as f64;
        let r = ratio_from((9.7 * GB) as u64, (0.1 * GB) as u64, (4.21 * GB) as u64);
        assert!(
            (r - 2.28).abs() < 0.02,
            "expected ~2.28x, got {r} — baseline subtraction must not \
             neutralise the real gap"
        );
    }

    #[test]
    fn ratio_denominator_must_be_instance_wide_not_per_shard() {
        // Regression: the first version of the eviction budget scaling divided
        // the WHOLE-PROCESS footprint by ONE SHARD's estimated_memory. At
        // `--shards N` that inflates the ratio by ~N and evicts N times too
        // aggressively. This asserts the shape of the mistake: a small
        // denominator against a process-sized numerator saturates the clamp.
        const GB: u64 = 1024 * 1024 * 1024;
        let footprint = 8 * GB;
        let per_shard_sized = GB; // what one shard of eight might report
        let instance_sized = 8 * GB; // what the whole instance accounts for
        assert!(
            ratio_from(footprint, 0, per_shard_sized) > ratio_from(footprint, 0, instance_sized),
            "a per-shard denominator yields a LARGER ratio than an              instance-wide one — that is the bug, and the caller must pass              logical_used_memory_bytes(), not db.estimated_memory()"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn footprint_is_phys_footprint_not_resident_size() {
        // Regression: `process_footprint_bytes` read offset 16 of
        // task_vm_info_data_t, which is `resident_size` — the exact field this
        // function exists to avoid. On the swapped-out instance that motivated
        // the fix, resident was 439 MB against a 10.1 GB footprint, so the
        // eviction correction computed a ratio of ~1.0 and did nothing.
        //
        // Discriminator: a clean file-backed mapping is charged to
        // `resident_size` but NOT to `phys_footprint`. Touch 64 MiB of one and
        // the two fields must move differently.
        let _g = TEST_LOCK.lock();
        const LEN: usize = 64 << 20;
        let path = std::env::temp_dir().join("moon_footprint_probe.bin");
        if std::fs::write(&path, vec![0u8; LEN]).is_err() {
            return; // no writable tmp — nothing to assert, don't fail the run
        }
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => return,
        };
        let fd = std::os::unix::io::AsRawFd::as_raw_fd(&file);

        let rss_before = crate::admin::metrics_setup::get_rss_bytes();
        let fp_before = process_footprint_bytes();

        // SAFETY: null hint lets the kernel choose the address; LEN is the
        // file's exact size; PROT_READ/MAP_SHARED on a valid fd. The mapping
        // is unmapped below before `file` is dropped.
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                LEN,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        assert_ne!(p, libc::MAP_FAILED, "mmap of the probe file failed");
        let mut acc = 0u64;
        let mut off = 0usize;
        while off < LEN {
            // SAFETY: `off` stays strictly inside the LEN-byte mapping.
            acc += unsafe { *(p as *const u8).add(off) } as u64;
            off += 4096;
        }
        std::hint::black_box(acc);

        let rss_after = crate::admin::metrics_setup::get_rss_bytes();
        let fp_after = process_footprint_bytes();
        // SAFETY: unmapping exactly the region returned by the mmap above.
        unsafe { libc::munmap(p, LEN) };
        let _ = std::fs::remove_file(&path);

        let rss_growth = rss_after.saturating_sub(rss_before);
        let fp_growth = fp_after.saturating_sub(fp_before);
        assert!(
            rss_growth > (LEN as u64) / 2,
            "expected resident_size to absorb the file pages, grew {rss_growth} \
             — the probe did not fault the mapping in, so the assertion below \
             would pass vacuously"
        );
        assert!(
            fp_growth < (LEN as u64) / 4,
            "phys_footprint grew {fp_growth} for a CLEAN FILE-BACKED mapping — \
             this function is reading resident_size, and every capacity \
             decision built on it is wrong on a swapped-out process"
        );
    }

    #[test]
    fn footprint_is_measurable_on_this_platform() {
        let _g = TEST_LOCK.lock();
        // Guards the offset/flavor constants: a wrong TASK_VM_INFO layout
        // reads as 0 and silently disables the whole mechanism.
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(
            process_footprint_bytes() > 0,
            "footprint must be readable, or maxmemory silently falls back to \
             accounting-only and the swap-death bug returns"
        );
    }
}
