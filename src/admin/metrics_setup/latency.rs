//! The ONE spelling of "time this command" (moon#941, moon#963).
//!
//! moon has three dispatch paths — `command::dispatch`, `command::dispatch_read`
//! and `server::conn::try_inline_dispatch` — reached from three connection
//! handlers. Before this module each site open-coded the same four lines
//! (bump the counter, maybe take an `Instant`, run the command, record), and
//! two of them drifted:
//!
//! - the monoio write path constructed its `Instant` AFTER the `with_shard`
//!   closure that ran the command had returned, so every write on the shipped
//!   runtime reported 0 µs and `SLOWLOG` could never fire for a write
//!   (moon#941);
//! - the inline `GET`/`SET` fast path had no timer at all, so the two
//!   highest-volume commands emitted no `moon_command_duration_microseconds`
//!   series whatsoever (moon#963).
//!
//! [`LatencyProbe::observe`] takes the command as a closure. The timer is
//! started before the closure and read after it *inside this function*, so a
//! call site cannot put the `Instant` on the wrong side of the work — the
//! only way to time nothing is to not call `observe`, and the inline path is
//! given a probe as a mandatory parameter for exactly that reason.
//!
//! # What is timed (moon#994)
//!
//! Two consumers, two cadences:
//!
//! - the `moon_command_duration_microseconds` HISTOGRAM tolerates sampling,
//!   so it keeps the 1-in-16 per-connection cadence;
//! - the SLOWLOG does not. Its whole job is the rare outlier, and a 1-in-16
//!   per-connection sampler missed 15 of every 16 and never saw a connection
//!   that sent fewer than 16 commands. So while the slowlog can record
//!   anything (threshold >= 0 and max-len > 0) EVERY command is timed; while
//!   it is disabled, the unsampled command reads no clock at all.
//!
//! # Which clock
//!
//! Per-command timing needs a monotonic clock with microsecond resolution at
//! the lowest possible read cost. Measured on the shipping target (GCE
//! t2a-standard-8, Neoverse-N1, Linux 7.0, one pinned core, a start/stop
//! PAIR, best of 5 x 20M):
//!
//! | clock                                    | ns / pair | resolution |
//! |------------------------------------------|-----------|------------|
//! | `std::time::Instant` (vDSO `clock_gettime`) |   67.4 | ns         |
//! | `clock_gettime(CLOCK_MONOTONIC)` direct  |      59.4 | ns         |
//! | `clock_gettime(CLOCK_MONOTONIC_COARSE)`  |      15.9 | **1 ms**   |
//! | `quanta::Clock::raw` (`cntvct_el0`)      |      21.6 | 40 ns      |
//!
//! `COARSE` is cheapest but ticks once per jiffy, so every command under a
//! millisecond reads 0 µs — unusable for a threshold that is routinely set
//! below 1000 µs. The shard-cached timestamp that per-key TTL checks use is
//! refreshed once per batch/tick and has the same problem, and it is not a
//! duration source at all: both reads of one command would see the same
//! value. `quanta`'s raw counter (`cntvct_el0` on aarch64, the invariant TSC
//! on x86_64, falling back to the OS monotonic clock where neither is
//! trustworthy) is 3.1x cheaper than `Instant` at 40 ns resolution. It is
//! already in the dependency graph (the Prometheus exporter uses it), and it
//! adds no `unsafe` to moon. Ticks are converted to µs only after the second
//! read, with one multiply and shift.
//!
//! Nothing here allocates; the slowlog copies argv only once a command has
//! crossed the threshold, in a `#[cold]` push.
//!
//! # What is NOT here any more
//!
//! `total_commands_processed` used to be flushed from this probe's drop. It
//! is counted at the client-command boundary instead (moon#775, moon#1002;
//! see `count_client_command`): a probe runs wherever a command or a PIECE
//! of one executes — a routed command on its owner shard, one coordinator
//! leg per shard — so counting here booked legs, not commands.

use std::sync::OnceLock;

use crate::admin::metrics_setup::CachedMetricsHandles;
use crate::admin::slowlog::{Slowlog, SlowlogArgv};

/// Sampling cadence for the duration histogram: every 16th command on a
/// connection.
const SAMPLE_MASK: u32 = 0xF;

/// The process-wide command clock. `quanta` calibrates its counter against
/// the OS clock on first use (bounded at 200 ms, typically 10-20 ms), so
/// startup calls [`init_command_clock`] before any shard accepts a
/// connection; a lazily-initialised first read would stall a shard thread.
static COMMAND_CLOCK: OnceLock<quanta::Clock> = OnceLock::new();

/// Calibrate the command clock now (startup), not on the first command.
pub fn init_command_clock() {
    let _ = command_clock();
}

#[inline]
fn command_clock() -> &'static quanta::Clock {
    COMMAND_CLOCK.get_or_init(quanta::Clock::new)
}

/// Per-connection 1-in-16 sampler for the duration histogram. The counter is
/// private so that no call site can re-derive "is this a sampled command"
/// and time something else.
#[derive(Debug, Default, Clone, Copy)]
pub struct CommandSampler {
    counter: u32,
}

impl CommandSampler {
    #[must_use]
    pub const fn new() -> Self {
        Self { counter: 0 }
    }

    /// Advance the cadence; `true` on a histogram-sampled tick.
    #[inline]
    fn tick(&mut self) -> bool {
        self.counter = self.counter.wrapping_add(1);
        self.counter & SAMPLE_MASK == 0
    }
}

/// One connection's view of the command telemetry sinks: the sampler, the
/// cached Prometheus handles, the slowlog and the client identity a slowlog
/// entry carries.
///
/// Built once per batch (inline loop) or once per command (generic loops).
pub struct LatencyProbe<'a> {
    sampler: &'a mut CommandSampler,
    metrics: &'a mut CachedMetricsHandles,
    slowlog: &'a Slowlog,
    client_addr: &'a [u8],
    client_name: &'a [u8],
}

impl<'a> LatencyProbe<'a> {
    /// A probe recording into the global slowlog.
    #[inline]
    #[must_use]
    pub fn new(
        sampler: &'a mut CommandSampler,
        metrics: &'a mut CachedMetricsHandles,
        client_addr: &'a [u8],
        client_name: &'a [u8],
    ) -> Self {
        Self::with_slowlog(
            sampler,
            metrics,
            crate::admin::metrics_setup::global_slowlog(),
            client_addr,
            client_name,
        )
    }

    /// A probe recording into `slowlog` — the constructor unit tests use so
    /// they never touch the process-global ring.
    #[inline]
    #[must_use]
    pub fn with_slowlog(
        sampler: &'a mut CommandSampler,
        metrics: &'a mut CachedMetricsHandles,
        slowlog: &'a Slowlog,
        client_addr: &'a [u8],
        client_name: &'a [u8],
    ) -> Self {
        Self {
            sampler,
            metrics,
            slowlog,
            client_addr,
            client_name,
        }
    }

    /// Run `work` as one execution of `cmd` and account for it.
    ///
    /// Every call bumps `moon_commands_total{cmd}`. The closure's wall time
    /// is measured when either consumer wants it — on a histogram-sampled
    /// tick, or on EVERY call while the slowlog is enabled — and is then
    /// recorded into `moon_command_duration_microseconds{cmd}` (sampled
    /// ticks only) and offered to the slowlog with `argv`. The interval
    /// measured is exactly the closure: the clock is read on the line before
    /// it runs and on the line after, and no caller can reorder that.
    #[inline]
    pub fn observe<T>(&mut self, cmd: &[u8], argv: SlowlogArgv<'_>, work: impl FnOnce() -> T) -> T {
        let sampled = self.sampler.tick();
        let threshold_us = self.slowlog.effective_threshold_us();
        if !sampled && threshold_us < 0 {
            let out = work();
            self.metrics.observe(cmd, None);
            return out;
        }
        let clock = command_clock();
        let start = clock.raw();
        let out = work();
        let end = clock.raw();
        let elapsed_us = clock.delta_as_nanos(start, end) / 1_000;
        self.metrics.observe(cmd, sampled.then_some(elapsed_us));
        self.slowlog.record_if_over(
            elapsed_us,
            threshold_us,
            argv,
            self.client_addr,
            self.client_name,
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::metrics_setup::this_thread_commands;
    use crate::protocol::Frame;
    use bytes::Bytes;
    use std::time::Duration;

    fn raw<'a>(args: &'a [&'a [u8]]) -> SlowlogArgv<'a> {
        SlowlogArgv::Raw(args)
    }

    /// moon#994: while the slowlog is enabled EVERY command is timed, so
    /// every one over the threshold is logged — not one in sixteen. And the
    /// moon#941 shape: the logged time covers the closure. Reversing the
    /// timer inside `observe` (start after `work`) records ~0 and this fails.
    #[test]
    fn every_command_over_the_threshold_is_logged_with_the_closures_time() {
        let mut sampler = CommandSampler::new();
        let mut cache = CachedMetricsHandles::new();
        let slowlog = Slowlog::new(16, 1_000); // 1 ms threshold
        let mut probe =
            LatencyProbe::with_slowlog(&mut sampler, &mut cache, &slowlog, b"127.0.0.1:1", b"");
        let argv: [&[u8]; 2] = [b"SADD", b"k"];
        for _ in 0..3 {
            probe.observe(b"SADD", raw(&argv), || {
                std::thread::sleep(Duration::from_millis(3));
            });
        }
        let entries = slowlog.get(None);
        assert_eq!(
            entries.len(),
            3,
            "three slow commands on one short-lived connection: all logged"
        );
        for e in &entries {
            assert!(
                e.duration_us >= 3_000,
                "the timing must cover the closure: {} us",
                e.duration_us
            );
        }
        assert_eq!(
            entries[0].command,
            vec![Bytes::from_static(b"SADD"), Bytes::from_static(b"k")]
        );
        assert_eq!(entries[0].client_addr, Bytes::from_static(b"127.0.0.1:1"));
    }

    /// The first command on a connection is timed too (the pre-fix sampler
    /// timed only the 16th, so a one-command connection was invisible).
    #[test]
    fn the_first_command_on_a_connection_is_timed() {
        let mut sampler = CommandSampler::new();
        let mut cache = CachedMetricsHandles::new();
        let slowlog = Slowlog::new(16, 0);
        let mut probe = LatencyProbe::with_slowlog(&mut sampler, &mut cache, &slowlog, b"", b"");
        let argv: [&[u8]; 1] = [b"PING"];
        probe.observe(b"PING", raw(&argv), || ());
        assert_eq!(slowlog.len(), 1);
    }

    /// A disabled slowlog (negative threshold, or max-len 0) records nothing
    /// and `observe` still returns the closure's value.
    #[test]
    fn a_disabled_slowlog_records_nothing() {
        for slowlog in [Slowlog::new(16, -1), Slowlog::new(0, 0)] {
            let mut sampler = CommandSampler::new();
            let mut cache = CachedMetricsHandles::new();
            let mut probe =
                LatencyProbe::with_slowlog(&mut sampler, &mut cache, &slowlog, b"", b"");
            let argv: [&[u8]; 1] = [b"PING"];
            let mut sum = 0u32;
            for i in 0..48u32 {
                sum += probe.observe(b"PING", raw(&argv), || {
                    std::thread::sleep(Duration::from_micros(10));
                    i
                });
            }
            assert_eq!(sum, (0..48).sum::<u32>(), "observe returns the closure's value");
            assert_eq!(slowlog.len(), 0);
        }
    }

    /// The histogram cadence stays exactly 1-in-16 per connection.
    #[test]
    fn histogram_cadence_is_one_in_sixteen() {
        let mut sampler = CommandSampler::new();
        let sampled = (0..48).filter(|_| sampler.tick()).count();
        assert_eq!(sampled, 3, "48 commands = 3 histogram samples");
    }

    /// A frame that is not a multibulk has no argv to log, so it is timed
    /// but never creates a slowlog entry with an empty command (which
    /// SLOWLOG GET would render as a 0-element array).
    #[test]
    fn non_array_frames_are_timed_but_not_slowlogged() {
        let mut sampler = CommandSampler::new();
        let mut cache = CachedMetricsHandles::new();
        let slowlog = Slowlog::new(128, 0);
        let mut probe = LatencyProbe::with_slowlog(&mut sampler, &mut cache, &slowlog, b"", b"");
        let frame = Frame::SimpleString(Bytes::from_static(b"not an argv"));
        for _ in 0..32 {
            probe.observe(b"PING", SlowlogArgv::from(&frame), || ());
        }
        assert_eq!(slowlog.len(), 0);
    }

    /// A multibulk frame logs its bulk strings verbatim.
    #[test]
    fn array_frames_log_their_bulk_strings() {
        let mut sampler = CommandSampler::new();
        let mut cache = CachedMetricsHandles::new();
        let slowlog = Slowlog::new(128, 0);
        let mut probe = LatencyProbe::with_slowlog(&mut sampler, &mut cache, &slowlog, b"", b"");
        let frame = Frame::Array(crate::protocol::FrameVec::from(vec![
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"k")),
            Frame::Integer(7),
        ]));
        probe.observe(b"SET", SlowlogArgv::from(&frame), || ());
        let entries = slowlog.get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].command,
            vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"k"),
                Bytes::from_static(b"?")
            ]
        );
    }

    /// moon#1002: the probe runs wherever a command or a PIECE of one
    /// executes (a routed command on its owner shard, one coordinator leg per
    /// shard), so it must not count `total_commands_processed` — the client
    /// boundary does.
    #[test]
    fn the_probe_does_not_count_total_commands() {
        let before = this_thread_commands();
        let mut sampler = CommandSampler::new();
        let mut cache = CachedMetricsHandles::new();
        let slowlog = Slowlog::new(1, 0);
        {
            let mut probe =
                LatencyProbe::with_slowlog(&mut sampler, &mut cache, &slowlog, b"", b"");
            let argv: [&[u8]; 1] = [b"GET"];
            for _ in 0..5 {
                probe.observe(b"GET", raw(&argv), || ());
            }
        }
        assert_eq!(this_thread_commands(), before);
    }

    /// The command clock resolves microseconds: a 2 ms sleep reads as about
    /// 2000 µs, not 0 (a coarse clock would) and not a jiffy multiple.
    #[test]
    fn the_command_clock_resolves_microseconds() {
        let clock = command_clock();
        let start = clock.raw();
        std::thread::sleep(Duration::from_millis(2));
        let us = clock.delta_as_nanos(start, clock.raw()) / 1_000;
        assert!((2_000..200_000).contains(&us), "2 ms sleep measured {us} us");
        let start = clock.raw();
        let us = clock.delta_as_nanos(start, clock.raw()) / 1_000;
        assert!(us < 1_000, "back-to-back reads measured {us} us");
    }
}
