//! Test-only panic injection for the spill thread (moon#1265).
//!
//! Driven by `MOON_TEST_SPILL_PANIC_FILE=<path>`, an environment variable no
//! production deployment sets, like `MOON_TEST_SNAPSHOT_HOLD_FILE`
//! (`shard::test_hooks`). It is read ONCE per process; with it unset every
//! injection point costs one `Option` check. With it set, each point stats
//! `<path>`; while the file exists its content names where the thread panics:
//!
//! - `start` — as the thread starts, before it reads anything;
//! - `after-write` — a spill flush wrote its file(s), and sent no completion
//!   (the batch's requests are lost with the thread, their files unlisted);
//! - `after-send` — a flush sent its completions, and died before its
//!   watermark moved;
//! - `reclaim-write` — a cold-reclaim write job wrote its outputs, and sent
//!   no answer.
//!
//! followed by an optional ` once`: the thread that removes the file panics,
//! and no other (one panic, whichever shard gets there first). Without
//! `once` every pass through the point panics while the file exists — what a
//! restart-budget test needs.
//!
//! Unit tests use [`PanicPlan::times`] instead: a plan owned by one
//! `SpillThread`, shared by all its incarnations.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};

/// Where the spill thread may be made to panic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PanicPoint {
    Start,
    AfterWrite,
    AfterSend,
    ReclaimWrite,
}

impl PanicPoint {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "start" => Some(Self::Start),
            "after-write" => Some(Self::AfterWrite),
            "after-send" => Some(Self::AfterSend),
            "reclaim-write" => Some(Self::ReclaimWrite),
            _ => None,
        }
    }
}

enum Source {
    /// `MOON_TEST_SPILL_PANIC_FILE`.
    File(PathBuf),
    /// Panic at `point` the next `left` times it is reached (unit tests).
    #[cfg_attr(not(test), allow(dead_code))]
    Times { point: PanicPoint, left: AtomicU32 },
}

/// A panic-injection plan. See the module doc.
pub(crate) struct PanicPlan {
    source: Source,
}

impl PanicPlan {
    /// The plan `MOON_TEST_SPILL_PANIC_FILE` asks for, if any. Read once.
    pub(crate) fn from_env() -> Option<Arc<Self>> {
        static PLAN: OnceLock<Option<Arc<PanicPlan>>> = OnceLock::new();
        PLAN.get_or_init(|| {
            std::env::var_os("MOON_TEST_SPILL_PANIC_FILE").map(|p| {
                tracing::warn!(
                    path = %PathBuf::from(&p).display(),
                    "MOON_TEST_SPILL_PANIC_FILE is set: spill threads panic on request (test only)"
                );
                Arc::new(PanicPlan {
                    source: Source::File(PathBuf::from(p)),
                })
            })
        })
        .clone()
    }

    /// Panic at `point` the next `n` times it is reached.
    #[cfg(test)]
    pub(crate) fn times(point: PanicPoint, n: u32) -> Arc<Self> {
        Arc::new(PanicPlan {
            source: Source::Times {
                point,
                left: AtomicU32::new(n),
            },
        })
    }

    /// Panic now if the plan says so at `point`.
    pub(crate) fn check(&self, point: PanicPoint) {
        let fire = match &self.source {
            Source::Times { point: p, left } => {
                *p == point
                    && left
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                        .is_ok()
            }
            Source::File(path) => {
                let Ok(content) = std::fs::read_to_string(path) else {
                    return;
                };
                let mut words = content.split_whitespace();
                if words.next().and_then(PanicPoint::parse) != Some(point) {
                    return;
                }
                // `once`: whoever removes the file is the one that panics.
                words.next() != Some("once") || std::fs::remove_file(path).is_ok()
            }
        };
        if fire {
            panic!("injected spill-thread panic at {point:?} (moon#1265 test hook)");
        }
    }
}

/// Check `plan` at `point` (no plan: nothing).
#[inline]
pub(crate) fn check(plan: Option<&PanicPlan>, point: PanicPoint) {
    if let Some(plan) = plan {
        plan.check(point);
    }
}
