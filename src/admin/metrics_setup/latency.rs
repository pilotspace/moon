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
//! Cost model (unchanged from the open-coded sites): one `u32` increment and
//! one branch per command; `Instant::now()` twice on every 16th command only.
//! Nothing here allocates on the sampled or unsampled path — the slowlog
//! copies argv only once a sample has already crossed the threshold.

use std::time::Instant;

use crate::admin::metrics_setup::{CachedMetricsHandles, bump_total_commands_by};
use crate::admin::slowlog::{Slowlog, SlowlogArgv};

/// Sampling cadence: every 16th command on a connection takes an `Instant`.
const SAMPLE_MASK: u32 = 0xF;

/// Per-connection 1-in-16 sampler. The counter is private so that no call
/// site can re-derive "is this a sampled command" and time something else.
#[derive(Debug, Default, Clone, Copy)]
pub struct CommandSampler {
    counter: u32,
}

impl CommandSampler {
    #[must_use]
    pub const fn new() -> Self {
        Self { counter: 0 }
    }

    /// Advance the cadence and, on a sampled tick, start the clock.
    #[inline]
    fn tick(&mut self) -> Option<Instant> {
        self.counter = self.counter.wrapping_add(1);
        (self.counter & SAMPLE_MASK == 0).then(Instant::now)
    }
}

/// One connection's view of the command telemetry sinks: the sampler, the
/// cached Prometheus handles, the slowlog and the client identity a slowlog
/// entry carries.
///
/// Built once per batch (inline loop) or once per command (generic loops);
/// dropping it flushes the commands it observed into this thread's
/// `total_commands_processed` slot as ONE relaxed add, which is what keeps
/// the inline loop's batched accounting (moon#660) with a single spelling.
pub struct LatencyProbe<'a> {
    sampler: &'a mut CommandSampler,
    metrics: &'a mut CachedMetricsHandles,
    slowlog: &'a Slowlog,
    client_addr: &'a [u8],
    client_name: &'a [u8],
    observed: u64,
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
            observed: 0,
        }
    }

    /// Run `work` as one execution of `cmd` and account for it.
    ///
    /// Every call counts the command (`moon_commands_total{cmd}` and, on
    /// drop, `total_commands_processed`). A sampled call additionally
    /// records the closure's wall time into
    /// `moon_command_duration_microseconds{cmd}` and offers it to the
    /// slowlog with `argv`. The interval measured is exactly the closure —
    /// the clock is started on the line before it runs and read on the line
    /// after, and no caller can reorder that.
    #[inline]
    pub fn observe<T>(&mut self, cmd: &[u8], argv: SlowlogArgv<'_>, work: impl FnOnce() -> T) -> T {
        self.observed += 1;
        let start = self.sampler.tick();
        let out = work();
        match start {
            Some(start) => {
                let elapsed_us = start.elapsed().as_micros() as u64;
                self.metrics.observe(cmd, Some(elapsed_us));
                self.slowlog.maybe_record_argv(
                    elapsed_us,
                    argv,
                    self.client_addr,
                    self.client_name,
                );
            }
            None => self.metrics.observe(cmd, None),
        }
        out
    }
}

impl Drop for LatencyProbe<'_> {
    #[inline]
    fn drop(&mut self) {
        bump_total_commands_by(self.observed);
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

    /// The moon#941 shape: a command that takes real time must report that
    /// time on the sampled tick. Reversing the timer inside `observe` (start
    /// after `work`) turns the recorded duration into ~0 and this fails.
    #[test]
    fn sampled_tick_measures_the_closure_not_its_neighbours() {
        let mut sampler = CommandSampler::new();
        let mut cache = CachedMetricsHandles::new();
        let slowlog = Slowlog::new(16, 1_000); // 1 ms threshold
        let mut probe =
            LatencyProbe::with_slowlog(&mut sampler, &mut cache, &slowlog, b"127.0.0.1:1", b"");
        let argv: [&[u8]; 2] = [b"SADD", b"k"];
        // Ticks 1..=15 are unsampled; the 16th samples.
        for _ in 0..16 {
            probe.observe(b"SADD", raw(&argv), || {
                std::thread::sleep(Duration::from_millis(3));
            });
        }
        drop(probe);
        let entries = slowlog.get(None);
        assert_eq!(entries.len(), 1, "exactly one of 16 ticks is sampled");
        assert!(
            entries[0].duration_us >= 3_000,
            "the sample must cover the closure: {} us",
            entries[0].duration_us
        );
        assert_eq!(
            entries[0].command,
            vec![Bytes::from_static(b"SADD"), Bytes::from_static(b"k")]
        );
        assert_eq!(entries[0].client_addr, Bytes::from_static(b"127.0.0.1:1"));
    }

    /// Cadence is exactly 1-in-16, counted across `observe` calls regardless
    /// of what each closure returns.
    #[test]
    fn cadence_is_one_in_sixteen() {
        let mut sampler = CommandSampler::new();
        let mut cache = CachedMetricsHandles::new();
        let slowlog = Slowlog::new(128, 0); // log every sample
        let mut probe = LatencyProbe::with_slowlog(&mut sampler, &mut cache, &slowlog, b"", b"");
        let argv: [&[u8]; 1] = [b"PING"];
        let mut sum = 0u32;
        for i in 0..48u32 {
            sum += probe.observe(b"PING", raw(&argv), || i);
        }
        drop(probe);
        assert_eq!(
            sum,
            (0..48).sum::<u32>(),
            "observe returns the closure's value"
        );
        assert_eq!(slowlog.len(), 3, "48 commands = 3 sampled ticks");
    }

    /// A frame that is not a multibulk has no argv to log, so a sampled tick
    /// still feeds the histogram but never creates a slowlog entry with an
    /// empty command (which SLOWLOG GET would render as a 0-element array).
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
        drop(probe);
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
        for _ in 0..16 {
            probe.observe(b"SET", SlowlogArgv::from(&frame), || ());
        }
        drop(probe);
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

    /// Dropping the probe is what lands the observed commands in this
    /// thread's `total_commands_processed` slot — as one add, not N.
    #[test]
    fn drop_flushes_observed_count_into_total_commands() {
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
            assert_eq!(
                this_thread_commands(),
                before,
                "nothing lands until the probe drops"
            );
        }
        assert_eq!(this_thread_commands(), before + 5);
    }
}
