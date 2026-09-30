//! `CONFIG SET appendfsync` at runtime (R1 review, finding 8).
//!
//! The policy used to be fixed when the writers and the pool were built, so
//! `CONFIG SET appendfsync always` answered `OK` — and `CONFIG GET` showed
//! `always` — while every write was still acknowledged before its fsync. redis
//! applies the change at once, and so does moon now:
//!
//! * The override set here is what [`effective`] returns in place of the
//!   configured policy. It is read (one relaxed load) by the producers through
//!   `AofWriterPool::fsync_policy` — which decides between the acked-after-
//!   fsync `AppendSync` path and fire-and-forget — and by every writer loop
//!   once per wake.
//! * A writer that sees the policy change moves its everysec state with it
//!   (`EverysecSync::set_policy`): leaving `everysec` drops the fsync agent,
//!   which JOINS it, so an fsync still in flight finishes first (redis's
//!   `bioDrainWorker(BIO_AOF_FSYNC)` on the same switch); entering it starts
//!   an agent.
//! * A batch holding an `AppendSync` is fsynced before its acks whatever the
//!   writer's policy of the moment (`group_commit::batch_needs_fsync`), so a
//!   producer that switched to `always` a moment before its writer did never
//!   gets an ack without the fsync.
//!
//! The override is process-wide, like the other live `CONFIG SET` parameters
//! (`publish_lfu_params`): one server per process. It stays unset until a
//! `CONFIG SET appendfsync`, so nothing else observes it.

use std::sync::atomic::{AtomicU8, Ordering};

use super::FsyncPolicy;

/// No runtime override: the configured policy rules.
const UNSET: u8 = 0;

static OVERRIDE: AtomicU8 = AtomicU8::new(UNSET);

fn encode(p: FsyncPolicy) -> u8 {
    match p {
        FsyncPolicy::Always => 1,
        FsyncPolicy::EverySec => 2,
        FsyncPolicy::No => 3,
    }
}

fn resolve(raw: u8, configured: FsyncPolicy) -> FsyncPolicy {
    match raw {
        1 => FsyncPolicy::Always,
        2 => FsyncPolicy::EverySec,
        3 => FsyncPolicy::No,
        _ => configured,
    }
}

/// The policy in force: the last `CONFIG SET appendfsync`, else `configured`.
#[inline]
pub fn effective(configured: FsyncPolicy) -> FsyncPolicy {
    resolve(OVERRIDE.load(Ordering::Relaxed), configured)
}

/// `CONFIG SET appendfsync <policy>`: every producer and writer uses it from
/// its next record / wake on.
pub fn set_runtime_policy(policy: FsyncPolicy) {
    OVERRIDE.store(encode(policy), Ordering::Relaxed);
}

/// Parse a `CONFIG SET appendfsync` value (redis accepts exactly these,
/// case-insensitively).
pub fn parse(value: &str) -> Option<FsyncPolicy> {
    if value.eq_ignore_ascii_case("always") {
        Some(FsyncPolicy::Always)
    } else if value.eq_ignore_ascii_case("everysec") {
        Some(FsyncPolicy::EverySec)
    } else if value.eq_ignore_ascii_case("no") {
        Some(FsyncPolicy::No)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `set_runtime_policy` is process-wide and every pool test in this
    // binary reads it, so the unit tests exercise the pure halves only; the
    // switch itself is covered by `tests/aof_fsync_stall_r1.rs`.
    #[test]
    fn an_override_replaces_the_configured_policy() {
        for p in [FsyncPolicy::Always, FsyncPolicy::EverySec, FsyncPolicy::No] {
            assert_eq!(resolve(UNSET, p), p, "unset: the configured policy");
            for o in [FsyncPolicy::Always, FsyncPolicy::EverySec, FsyncPolicy::No] {
                assert_eq!(resolve(encode(o), p), o);
            }
        }
    }

    #[test]
    fn parses_what_redis_accepts() {
        assert_eq!(parse("always"), Some(FsyncPolicy::Always));
        assert_eq!(parse("EverySec"), Some(FsyncPolicy::EverySec));
        assert_eq!(parse("NO"), Some(FsyncPolicy::No));
        assert_eq!(parse("sometimes"), None);
        assert_eq!(parse(""), None);
    }
}
