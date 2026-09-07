//! Per-run scratch paths for **lib** (`#[cfg(test)]`) tests.
//!
//! # Why this exists (moon#822)
//!
//! Seven lib tests named their scratch directory with a string literal —
//! `std::env::temp_dir().join("moon_test_datafile")` and friends. A literal is
//! a **process-global shared resource**: every concurrent `cargo test --lib`
//! process on the host picks the same path, and each one's teardown
//! (`remove_dir_all`) deletes it out from under the others.
//!
//! That is not exotic. `scripts/ci-local.sh` runs the monoio and tokio VM
//! suites concurrently by default (measured -44.5%), and any developer running
//! two legs at once reproduces it. Measured on the macOS host at
//! ae6cd003, two concurrent processes looping one test:
//!
//! ```text
//! persistence::kv_page::tests::test_datafile_roundtrip   0/40 solo, 13/80 concurrent
//! tls::tests::test_reload_tls_config_swaps_config        0/20 solo, 24/50 concurrent
//! ```
//!
//! The TLS failure is the dangerous one. It does not report a missing file —
//! it reports `TLS config: keys may not be consistent: KeyMismatch`, because
//! the run read **another run's** cert against its own key. A harness defect
//! wearing a TLS bug's clothes.
//!
//! # Why a counter and not just pid + clock
//!
//! `tests/common/mod.rs::unique_test_dir` solved this for the integration
//! suites and its doc comment records why the obvious version is not enough:
//! on macOS `SystemTime::now()` has only MICROSECOND resolution, so two
//! `#[test]` threads inside the same microsecond get the same path. The
//! process-local atomic counter is the part that cannot collide, whatever the
//! clock's resolution turns out to be; pid and the timestamp stay in the name
//! only so a leftover directory is still attributable to a run.
//!
//! Lib tests cannot use `tests/common` (it is a separate integration crate),
//! which is exactly how that fix failed to reach them. This is the same
//! construction, crate-side.
//!
//! # Use
//!
//! ```ignore
//! let dir = crate::util::test_temp::unique_test_dir("moon-tls-reload");
//! let cert = dir.join("cert.pem");         // name inside the dir may be fixed
//! // ...
//! let _ = std::fs::remove_dir_all(&dir);   // deletes only THIS run's dir
//! ```
//!
//! Never `join()` a literal onto `temp_dir()` directly — `scripts/audit-test-tempdirs.sh`
//! fails the build if you do.

use std::path::PathBuf;

/// A scratch directory unique to this process **and** to this call, created on
/// return.
///
/// Uniqueness comes from a process-local atomic counter; pid and a nanosecond
/// timestamp are appended for attributability, not for uniqueness. The
/// directory is created with `create_dir_all`, so the caller can write into it
/// immediately.
///
/// The caller owns teardown. `remove_dir_all` on the returned path is safe
/// precisely because no other process or thread can be holding it.
pub(crate) fn unique_test_dir(prefix: &str) -> PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("{prefix}-{}-{nanos}-{seq}", std::process::id()));
    // Best-effort: a caller on a read-only or full tmpfs will fail its own
    // first write with a clear error, which is more informative than a panic
    // from inside a helper.
    let _ = std::fs::create_dir_all(&dir);
    dir
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The counter — not the clock — is what makes this sound. Two calls in
    /// the same microsecond must still differ.
    #[test]
    fn successive_calls_never_collide() {
        let a = unique_test_dir("moon-test-temp-selftest");
        let b = unique_test_dir("moon-test-temp-selftest");
        assert_ne!(a, b, "two calls returned the same path");
        assert!(a.is_dir(), "returned path was not created: {a:?}");
        assert!(b.is_dir(), "returned path was not created: {b:?}");
        let _ = std::fs::remove_dir_all(&a);
        let _ = std::fs::remove_dir_all(&b);
    }

    /// The pid component is what makes it sound ACROSS processes, which is the
    /// case moon#822 actually hit. A path that does not carry this process's
    /// pid cannot be process-unique.
    #[test]
    fn path_is_scoped_to_this_process() {
        let d = unique_test_dir("moon-test-temp-selftest-pid");
        let name = d
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        assert!(
            name.contains(&format!("-{}-", std::process::id())),
            "path {name} is not scoped to pid {}",
            std::process::id()
        );
        let _ = std::fs::remove_dir_all(&d);
    }
}
