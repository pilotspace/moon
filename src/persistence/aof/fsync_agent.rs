//! The everysec fsync agent: one thread per AOF writer that runs the
//! once-per-second `fdatasync` OFF the writer's loop (moon#1266 Option 3).
//!
//! Before this, the everysec fsync ran inline on the writer thread, so while
//! the disk took its time the writer did not drain its channel: acked writes
//! piled up in process memory (lost to a kill -9) and, past 10k records,
//! producers hit the moon#769/#838 backpressure. redis runs the same fsync on
//! a background thread (`BIO_AOF_FSYNC`); this is that model.
//!
//! * The writer keeps `write(2)`-ing every batch; at the everysec deadline it
//!   hands a dup of its fd to the agent ([`EverysecSync::claim`] +
//!   [`EverysecSync::dispatch`]) and goes straight back to its channel.
//! * At most one fsync is in flight per writer; a deadline that finds the
//!   previous one still running is postponed, not queued
//!   ([`super::fsync_handoff`], loom-modeled in `tests/loom_aof_fsync_agent.rs`).
//! * A stalled fsync is loud, as in redis (R1 review, findings 3 and 4):
//!   while one has been in flight for 2 s or more the writer logs
//!   "Asynchronous AOF fsync is taking too long (disk is busy?)" (at most once
//!   per 2 s for the process), INFO shows `aof_pending_bio_fsync` (writers
//!   with an fsync in flight: 0..=N at `--shards N`, unlike redis's 0/1) and `aof_fsync_in_flight_ms` (the oldest one's
//!   age), and `aof_delayed_fsync` counts, as redis's field does, once per 2 s
//!   of an fsync in flight while written data waits for the next one.
//! * The agent records the outcome exactly where the inline fsync did: the
//!   fsync-latency metric and `record_everysec_fsync_result` (INFO
//!   `aof_last_fsync_status` / `aof_fsync_failures`). A failed fsync is
//!   retried at the next deadline even when nothing new was written, but a
//!   retry alone does not clear the writer's `err` bit (fsyncgate: the
//!   failed window's pages may be gone, and the retry "succeeds" on the
//!   clean pages that remain). The bit clears only when a successful fsync
//!   covers a successful write batch the writer issued AFTER it learned of
//!   the failure ([`EverysecSync`]'s heal rule, R1 review finding 7).
//! * No agent (the OS refused the thread, or it died): the writer fsyncs
//!   inline, exactly as before. A durability request is never dropped.
//!
//! `appendfsync always` does not use the agent: its per-batch fsync stays on
//! the writer, BEFORE the batch's acks (`group_commit`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::FsyncPolicy;
use super::fsync_handoff::{Begin, FsyncHandoff};

/// INFO `aof_delayed_fsync` (all writers): counted the way redis counts its
/// field of the same name — once for every [`STALL`] (2 s) that a writer's
/// fsync has been in flight while written data waits for the next one. redis
/// counts when it stops postponing its WRITE after 2 s and writes anyway,
/// again every 2 s while the fsync stays stuck; moon never postpones the
/// write, so the count marks the same moments without the write being held.
/// An exporter or alert built for redis reads it the same way: non-zero means
/// the disk kept an fsync busy for 2 s or more.
pub static AOF_DELAYED_FSYNC: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The everysec deadline: an fsync is handed off at most once per this.
const EVERYSEC: Duration = Duration::from_secs(1);

/// An fsync in flight this long is "taking too long": redis's threshold for
/// its log line and its `aof_delayed_fsync` count.
const STALL: Duration = Duration::from_secs(2);

/// Milliseconds on a process-wide monotonic clock, never 0 (0 = "none" in
/// the in-flight slots below).
fn mono_ms() -> u64 {
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let ms = EPOCH.get_or_init(Instant::now).elapsed().as_millis();
    u64::try_from(ms).unwrap_or(u64::MAX).saturating_add(1)
}

/// Every live agent's in-flight slot ([`mono_ms`] when its current fsync was
/// handed over, 0 when none), for INFO. Locked only when an agent starts or
/// stops and when INFO reads it — never per write.
static IN_FLIGHT_SLOTS: parking_lot::Mutex<Vec<Arc<std::sync::atomic::AtomicU64>>> =
    parking_lot::Mutex::new(Vec::new());

/// INFO `aof_pending_bio_fsync` and `aof_fsync_in_flight_ms`: how many
/// writers have an everysec fsync in flight on their agent, and how long the
/// oldest of them has been running, in ms (0 when none). redis's field counts
/// pending `BIO_AOF_FSYNC` jobs, 0 or 1 in practice; this one counts WRITERS,
/// so it ranges 0..=N at `--shards N` (R2 review, NEW-D: documented in the
/// production guide; tooling should test `> 0`).
pub fn in_flight_fsyncs() -> (usize, u64) {
    let now = mono_ms();
    let slots = IN_FLIGHT_SLOTS.lock();
    slots.iter().fold((0, 0), |(n, oldest), slot| {
        match slot.load(std::sync::atomic::Ordering::Relaxed) {
            0 => (n, oldest),
            since => (n + 1, oldest.max(now.saturating_sub(since))),
        }
    })
}

/// When [`EverysecSync::due`] last logged a stalled fsync (process-wide, in
/// [`mono_ms`]): the log line is rate-limited to one per [`STALL`].
static LAST_STALL_WARN_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Test-only: `MOON_TEST_AOF_SYNC_GATE=<path>` holds every AOF data fsync —
/// the everysec agent's and `always`'s per-batch one — while `<path>` exists.
/// Lets a test hold an fsync open and watch what is acknowledged meanwhile.
/// Read once per process; unset, it costs one cached `Option` check per fsync.
pub(super) fn sync_gate_for_test() {
    static GATE: std::sync::OnceLock<Option<std::path::PathBuf>> = std::sync::OnceLock::new();
    let gate = GATE.get_or_init(|| {
        std::env::var_os("MOON_TEST_AOF_SYNC_GATE")
            .filter(|v| !v.is_empty())
            .map(std::path::PathBuf::from)
    });
    if let Some(path) = gate {
        while path.exists() {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Settles an agent's taken job as FAILED if the agent thread unwinds before
/// it reaches `finish` (disarmed with `mem::forget` on the normal path). The
/// writer then learns the failure at its next claim, finds the agent gone
/// (the send fails) and fsyncs inline — the hand-off never sticks IN_FLIGHT.
/// It drives the same `finish` transition as the normal path, so the
/// loom-modeled state machine is unchanged.
struct SettleOnUnwind<'a> {
    handoff: &'a FsyncHandoff,
    in_flight: &'a std::sync::atomic::AtomicU64,
    dead: &'a std::sync::atomic::AtomicBool,
    writer_idx: usize,
}

impl Drop for SettleOnUnwind<'_> {
    fn drop(&mut self) {
        self.in_flight
            .store(0, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(
            "AOF everysec fsync agent (writer {}) died mid-fsync; the writer fsyncs inline \
             from now on",
            self.writer_idx
        );
        super::record_everysec_fsync_result(self.writer_idx, false);
        // Before `finish` releases IDLE: a writer that claims next sees the
        // agent dead and never sends a job the dying thread's still-open
        // channel would swallow.
        self.dead.store(true, std::sync::atomic::Ordering::Release);
        self.handoff.finish(false);
    }
}

/// One writer's fsync agent thread.
struct AofFsyncAgent {
    /// The dup to fsync, and whether a success clears the writer's `err`
    /// bit (the heal rule, see [`EverysecSync::dispatch`]).
    tx: flume::Sender<(std::fs::File, bool)>,
    handoff: Arc<FsyncHandoff>,
    /// [`mono_ms`] when the fsync in flight was handed over; 0 when none.
    /// Registered in [`IN_FLIGHT_SLOTS`] for INFO.
    in_flight_since: Arc<std::sync::atomic::AtomicU64>,
    /// Set by [`SettleOnUnwind`]: the agent thread is gone.
    dead: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl AofFsyncAgent {
    fn spawn(writer_idx: usize) -> std::io::Result<Self> {
        Self::spawn_with_backend(writer_idx, |f: &std::fs::File| f.sync_data())
    }

    fn spawn_with_backend<F>(writer_idx: usize, backend: F) -> std::io::Result<Self>
    where
        F: Fn(&std::fs::File) -> std::io::Result<()> + Send + 'static,
    {
        // Depth 1 is enough: a job is sent only after a successful
        // `try_begin`, and the state returns to IDLE only once the agent has
        // taken and settled it — at most one job exists (fsync_handoff docs).
        let (tx, rx) = flume::bounded::<(std::fs::File, bool)>(1);
        let handoff = Arc::new(FsyncHandoff::new());
        let agent_handoff = Arc::clone(&handoff);
        let in_flight_since = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let agent_in_flight = Arc::clone(&in_flight_since);
        let dead = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let agent_dead = Arc::clone(&dead);
        let name = format!("aof-fsync-{writer_idx}");
        let thread = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                crate::shard::numa::pin_current_aux_thread(&name);
                while let Ok((file, heals)) = rx.recv() {
                    // R1 review NIT: an unwind between here and `finish`
                    // must not leave the hand-off IN_FLIGHT forever (every
                    // later deadline would be postponed, with no inline
                    // fallback): the guard settles it as a failure.
                    let unwind = SettleOnUnwind {
                        handoff: &agent_handoff,
                        in_flight: &agent_in_flight,
                        dead: &agent_dead,
                        writer_idx,
                    };
                    sync_gate_for_test();
                    let t = Instant::now();
                    let result = backend(&file);
                    drop(file);
                    let took = t.elapsed();
                    // Cleared before `finish`: the writer's next hand-off
                    // (which acquires the IDLE `finish` releases) sets it
                    // again, so a clear can never land on a newer fsync.
                    agent_in_flight.store(0, std::sync::atomic::Ordering::Relaxed);
                    if took >= STALL {
                        tracing::warn!(
                            "AOF everysec fsync completed after {:.1}s (writer {writer_idx}, \
                             agent): the disk was busy; writes continued meanwhile",
                            took.as_secs_f64()
                        );
                    }
                    match &result {
                        Ok(()) => {
                            crate::admin::metrics_setup::record_aof_fsync(took.as_micros() as u64);
                            if heals {
                                super::record_everysec_fsync_result(writer_idx, true);
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "AOF everysec fsync failed (writer {writer_idx}, agent): {e}"
                            );
                            super::record_everysec_fsync_result(writer_idx, false);
                        }
                    }
                    // Outcome recorded first: a writer that sees IDLE sees it.
                    std::mem::forget(unwind);
                    agent_handoff.finish(result.is_ok());
                }
            })?;
        IN_FLIGHT_SLOTS.lock().push(Arc::clone(&in_flight_since));
        Ok(Self {
            tx,
            handoff,
            in_flight_since,
            dead,
            thread: Some(thread),
        })
    }
}

impl Drop for AofFsyncAgent {
    fn drop(&mut self) {
        // Close the channel so the agent's recv() ends, then join: never
        // leave a thread mid-fsync behind a writer that has exited.
        let (dead_tx, _) = flume::bounded(0);
        self.tx = dead_tx;
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        IN_FLIGHT_SLOTS
            .lock()
            .retain(|slot| !Arc::ptr_eq(slot, &self.in_flight_since));
    }
}

/// What the writer does at its everysec deadline ([`EverysecSync::claim`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Claim {
    /// This writer owns the next fsync: dup the fd and [`EverysecSync::dispatch`] it.
    Owned,
    /// The previous fsync is still running: nothing to do until a later wake.
    Postponed,
    /// No agent: fsync inline, then [`EverysecSync::inline_done`].
    Inline,
}

/// A writer's everysec state: the deadline, whether anything was written
/// since the last hand-off, and the agent (only under `EverySec`).
pub(super) struct EverysecSync {
    writer_idx: usize,
    agent: Option<AofFsyncAgent>,
    last_handoff: Instant,
    dirty: bool,
    /// When the fsync now (or last) in flight was handed to the agent.
    dispatched_at: Instant,
    /// [`STALL`] periods of the in-flight fsync already counted in
    /// [`AOF_DELAYED_FSYNC`].
    stalls_counted: u64,
    /// The heal rule (R1 review, finding 7). This writer knows its last
    /// fsync failed (its `err` bit is set) ...
    heal_pending: bool,
    /// ... and it has written a batch since it learned that, so the next
    /// successful fsync covers post-failure data and may clear the bit.
    written_after_failure: bool,
    /// A job was handed to the agent and its outcome not yet observed.
    job_outstanding: bool,
    /// That job's success clears the bit.
    job_heals: bool,
}

impl EverysecSync {
    /// `writer_idx`: 0 for the TopLevel writer, the shard id for PerShard.
    pub(super) fn new(writer_idx: usize, policy: FsyncPolicy) -> Self {
        let agent = if policy == FsyncPolicy::EverySec {
            match AofFsyncAgent::spawn(writer_idx) {
                Ok(a) => Some(a),
                Err(e) => {
                    tracing::warn!(
                        "AOF writer {writer_idx}: could not start the everysec fsync agent \
                         ({e}); fsyncing inline on the writer thread"
                    );
                    None
                }
            }
        } else {
            None
        };
        Self {
            writer_idx,
            agent,
            last_handoff: Instant::now(),
            dirty: false,
            dispatched_at: Instant::now(),
            stalls_counted: 0,
            heal_pending: false,
            written_after_failure: false,
            job_outstanding: false,
            job_heals: false,
        }
    }

    /// The writer's policy changed at runtime (`CONFIG SET appendfsync`,
    /// [`super::runtime_fsync`]). Leaving `everysec` drops the agent, which
    /// joins it: an fsync still in flight finishes (and records its outcome)
    /// before the writer runs its first batch under the new policy — redis's
    /// `bioDrainWorker(BIO_AOF_FSYNC)` on the same switch. Entering
    /// `everysec` starts an agent (or keeps fsyncing inline without one).
    /// A no-op when the agent already matches the policy. Returns whether
    /// the everysec state changed (the caller's pending deadline is void).
    pub(super) fn set_policy(&mut self, policy: FsyncPolicy) -> bool {
        let everysec = policy == FsyncPolicy::EverySec;
        if everysec == self.agent.is_some() {
            return false;
        }
        if everysec {
            *self = Self {
                heal_pending: self.heal_pending,
                written_after_failure: self.written_after_failure,
                ..Self::new(self.writer_idx, policy)
            };
        } else {
            // The dropped agent's settled outcome is still the writer's to
            // learn: a failure keeps its `err` bit under the heal rule.
            if let Some(agent) = self.agent.take() {
                let handoff = std::sync::Arc::clone(&agent.handoff);
                drop(agent); // joins: the in-flight fsync finishes first
                self.observe_settled_job(handoff.last_failed());
            }
            self.dirty = false;
        }
        true
    }

    #[cfg(test)]
    fn with_backend<F>(writer_idx: usize, backend: F) -> Self
    where
        F: Fn(&std::fs::File) -> std::io::Result<()> + Send + 'static,
    {
        Self {
            writer_idx,
            agent: AofFsyncAgent::spawn_with_backend(writer_idx, backend).ok(),
            last_handoff: Instant::now(),
            dirty: false,
            dispatched_at: Instant::now(),
            stalls_counted: 0,
            heal_pending: false,
            written_after_failure: false,
            job_outstanding: false,
            job_heals: false,
        }
    }

    /// A batch was written (reached the kernel) and is not yet fsynced.
    #[inline]
    pub(super) fn note_written(&mut self) {
        self.dirty = true;
        // Issued after the writer learned of its failure: the next fsync
        // that succeeds covers post-failure data (the heal rule).
        self.written_after_failure |= self.heal_pending;
    }

    /// An everysec-owed fsync this writer ran outside the deadline path (the
    /// post-fold drain's) failed: record it in INFO `aof_last_fsync_status`
    /// like a deadline fsync failure, and arm a retry that fires within
    /// ~100 ms instead of waiting for the next write (R1 review, finding 6).
    pub(super) fn boundary_fsync_failed(&mut self) {
        super::record_everysec_fsync_result(self.writer_idx, false);
        self.enter_heal_pending();
        self.backdate(EVERYSEC - Duration::from_millis(100));
    }

    /// The writer learned that its fsync failed: its `err` bit is set, and
    /// only data written from here on can clear it.
    fn enter_heal_pending(&mut self) {
        self.heal_pending = true;
        self.written_after_failure = false;
    }

    /// Whether a successful fsync issued now may clear the writer's `err`
    /// bit: no failure is known, or a batch was written since it was.
    #[inline]
    fn fsync_heals(&self) -> bool {
        !self.heal_pending || self.written_after_failure
    }

    /// The job handed to the agent has settled (seen at the next `Owned`
    /// claim, which acquires its outcome): learn a failure, or that a
    /// healing job succeeded.
    fn observe_settled_job(&mut self, failed: bool) {
        if !std::mem::take(&mut self.job_outstanding) {
            return;
        }
        if failed {
            self.enter_heal_pending();
        } else if self.job_heals {
            self.heal_pending = false;
            self.written_after_failure = false;
        }
    }

    /// Make the next deadline fall `by` earlier than a full second from now
    /// (the post-rewrite drain: the backlog must be fsynced soon, not 1 s
    /// later).
    pub(super) fn backdate(&mut self, by: Duration) {
        self.dirty = true;
        self.last_handoff = Instant::now().checked_sub(by).unwrap_or(self.last_handoff);
    }

    /// The everysec deadline has come and there is something to fsync: new
    /// bytes, or a previous fsync that failed and must be retried.
    ///
    /// Every writer wake calls this under everysec, so it is also where a
    /// stalled agent fsync is reported ([`Self::warn_if_stalled`]).
    pub(super) fn due(&self) -> bool {
        self.warn_if_stalled();
        (self.dirty || self.agent.as_ref().is_some_and(|a| a.handoff.last_failed()))
            && self.last_handoff.elapsed() >= EVERYSEC
    }

    /// redis's "Asynchronous AOF fsync is taking too long (disk is busy?)":
    /// logged while this writer's fsync has been in flight for [`STALL`] or
    /// more, at most once per [`STALL`] for the whole process. One relaxed
    /// load when no fsync is in flight.
    fn warn_if_stalled(&self) {
        use std::sync::atomic::Ordering;
        let Some(agent) = self.agent.as_ref() else {
            return;
        };
        let since = agent.in_flight_since.load(Ordering::Relaxed);
        if since == 0 {
            return;
        }
        let now = mono_ms();
        let age = now.saturating_sub(since);
        let stall_ms = STALL.as_millis() as u64;
        if age < stall_ms {
            return;
        }
        let last = LAST_STALL_WARN_MS.load(Ordering::Relaxed);
        if (last != 0 && now.saturating_sub(last) < stall_ms)
            || LAST_STALL_WARN_MS
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        tracing::warn!(
            "Asynchronous AOF fsync is taking too long (disk is busy?): writer {}'s everysec \
             fsync has been running for {:.1}s. Writes are still accepted and written to the \
             kernel, but nothing written since the last completed fsync is durable against \
             an OS crash or power loss until it returns (INFO persistence: \
             aof_pending_bio_fsync, aof_fsync_in_flight_ms, aof_delayed_fsync).",
            self.writer_idx,
            age as f64 / 1000.0
        );
    }

    /// Claim the next fsync.
    pub(super) fn claim(&mut self) -> Claim {
        let Some(agent) = self.agent.as_ref() else {
            return Claim::Inline;
        };
        match agent.handoff.try_begin() {
            Begin::Owned => {
                self.stalls_counted = 0;
                let failed = agent.handoff.last_failed();
                self.observe_settled_job(failed);
                Claim::Owned
            }
            Begin::Postponed => {
                // redis's cadence: one count per full STALL the fsync has
                // been in flight while this deadline waits for it.
                let periods = self.dispatched_at.elapsed().as_millis() / STALL.as_millis();
                let periods = u64::try_from(periods).unwrap_or(u64::MAX);
                if periods > self.stalls_counted {
                    AOF_DELAYED_FSYNC.fetch_add(
                        periods - self.stalls_counted,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    self.stalls_counted = periods;
                }
                Claim::Postponed
            }
        }
    }

    /// Send the fsync this writer owns (after [`Claim::Owned`]). `false`:
    /// the dup failed or the agent is gone — the claim is released and the
    /// caller MUST fsync inline and report it through [`Self::inline_done`].
    pub(super) fn dispatch(&mut self, dup: std::io::Result<std::fs::File>) -> bool {
        let Some(agent) = self.agent.as_ref() else {
            return false;
        };
        // The agent died (its unwind guard settled the last job): release
        // the claim and drop it, so every later deadline fsyncs inline.
        if agent.dead.load(std::sync::atomic::Ordering::Acquire) {
            agent.handoff.abort();
            self.agent = None;
            return false;
        }
        // Set before the send: the agent clears it once the fsync returns,
        // which must never be overtaken by this store.
        agent
            .in_flight_since
            .store(mono_ms(), std::sync::atomic::Ordering::Relaxed);
        let sent = match dup {
            Ok(file) => agent.tx.try_send((file, self.fsync_heals())).is_ok(),
            Err(e) => {
                tracing::warn!(
                    "AOF writer {}: could not dup the fd for the fsync agent ({e}); \
                     fsyncing inline",
                    self.writer_idx
                );
                false
            }
        };
        if sent {
            self.job_outstanding = true;
            self.job_heals = self.fsync_heals();
            self.last_handoff = Instant::now();
            self.dispatched_at = self.last_handoff;
            self.dirty = false;
        } else {
            agent
                .in_flight_since
                .store(0, std::sync::atomic::Ordering::Relaxed);
            agent.handoff.abort();
        }
        sent
    }

    /// The caller's inline fallback fsync returned: records it in INFO
    /// `aof_last_fsync_status` under the heal rule (a success clears the
    /// writer's `err` bit only when it covers a batch written after the
    /// failure was learned). A failure keeps the deadline armed (retried on
    /// the next wake, as before the agent).
    pub(super) fn inline_done(&mut self, ok: bool) {
        if ok {
            if self.fsync_heals() {
                super::record_everysec_fsync_result(self.writer_idx, true);
                self.heal_pending = false;
                self.written_after_failure = false;
            }
            self.last_handoff = Instant::now();
            self.dirty = false;
        } else {
            super::record_everysec_fsync_result(self.writer_idx, false);
            self.enter_heal_pending();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::{Condvar, Mutex};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Gate {
        open: Mutex<bool>,
        cv: Condvar,
        calls: AtomicUsize,
    }

    impl Gate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                open: Mutex::new(false),
                cv: Condvar::new(),
                calls: AtomicUsize::new(0),
            })
        }
        fn release(&self) {
            *self.open.lock() = true;
            self.cv.notify_all();
        }
        fn backend(self: &Arc<Self>) -> impl Fn(&std::fs::File) -> std::io::Result<()> + use<> {
            let g = Arc::clone(self);
            move |_f| {
                g.calls.fetch_add(1, Ordering::SeqCst);
                let mut open = g.open.lock();
                while !*open {
                    g.cv.wait(&mut open);
                }
                Ok(())
            }
        }
    }

    /// Opens the gate when dropped (also while a failed test unwinds).
    struct ReleaseOnDrop(Arc<Gate>);

    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    fn wait_until(what: &str, f: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !f() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn file() -> std::io::Result<std::fs::File> {
        tempfile::tempfile()
    }

    #[test]
    fn nothing_is_due_until_something_was_written_and_a_second_passed() {
        let mut s = EverysecSync::with_backend(0, |_f: &std::fs::File| Ok(()));
        assert!(!s.due(), "clean and fresh");
        s.note_written();
        assert!(!s.due(), "dirty but inside the second");
        s.backdate(EVERYSEC);
        assert!(s.due());
    }

    /// The writer returns from the hand-off while the fsync is still held:
    /// the fsync is off its loop.
    #[test]
    fn dispatch_returns_while_the_fsync_is_still_running() {
        let gate = Gate::new();
        let mut s = EverysecSync::with_backend(0, gate.backend());
        // Declared after `s`, so it drops (opens the gate) first: a failed
        // assertion must not leave the agent held while `s` joins it.
        let _release = ReleaseOnDrop(Arc::clone(&gate));
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
        assert!(!s.due(), "a dispatched hand-off resets the deadline");
        wait_until("the agent to enter the fsync", || {
            gate.calls.load(Ordering::SeqCst) == 1
        });
        let handoff = Arc::clone(&s.agent.as_ref().expect("agent").handoff);
        assert!(handoff.in_flight());
        gate.release();
        wait_until("the fsync to settle", || handoff.settled() == 1);
        assert!(!handoff.in_flight());
    }

    /// A deadline that finds the previous fsync running is postponed, not
    /// queued; once it settles the next claim succeeds.
    #[test]
    fn a_deadline_during_a_running_fsync_is_postponed() {
        let gate = Gate::new();
        let mut s = EverysecSync::with_backend(1, gate.backend());
        // Declared after `s`, so it drops (opens the gate) first: a failed
        // assertion must not leave the agent held while `s` joins it.
        let _release = ReleaseOnDrop(Arc::clone(&gate));
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Postponed);
        assert_eq!(s.claim(), Claim::Postponed);
        assert_eq!(s.stalls_counted, 0, "not yet in flight for 2 s");
        let handoff = Arc::clone(&s.agent.as_ref().expect("agent").handoff);
        assert_eq!(handoff.delayed(), 2);
        assert!(s.due(), "still dirty: the postponed deadline stays armed");
        gate.release();
        wait_until("the first fsync to settle", || handoff.settled() == 1);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
        wait_until("the second fsync to settle", || handoff.settled() == 2);
        assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
    }

    /// A failed fsync is retried at the next deadline with nothing new
    /// written — but that retry does not clear the writer's status bit
    /// (fsyncgate: it succeeds on whatever clean pages remain). The bit
    /// clears only once a successful fsync covers a batch written after
    /// the failure was learned (R1 review, finding 7).
    #[test]
    fn a_failed_fsync_is_retried_without_new_writes() {
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let backend = {
            let fail = Arc::clone(&fail);
            move |_f: &std::fs::File| {
                if fail.load(Ordering::SeqCst) {
                    Err(std::io::Error::other("disk gone"))
                } else {
                    Ok(())
                }
            }
        };
        let err_bit = || super::super::AOF_FSYNC_ERR_WRITERS.load(Ordering::Relaxed) & (1 << 57);
        let mut s = EverysecSync::with_backend(57, backend);
        let handoff = Arc::clone(&s.agent.as_ref().expect("agent").handoff);
        s.note_written();
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
        wait_until("the fsync to settle", || handoff.settled() == 1);
        assert!(handoff.last_failed());
        assert_ne!(err_bit(), 0, "the agent set the bit");
        assert!(!s.due(), "not before the next second");
        s.last_handoff = Instant::now() - EVERYSEC;
        assert!(s.due(), "a failed fsync keeps the deadline armed");

        // The retry, with nothing new written, succeeds — the bit stays.
        fail.store(false, Ordering::SeqCst);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.heal_pending, "the writer learned of the failure");
        assert!(s.dispatch(file()));
        wait_until("the retry to settle", || handoff.settled() == 2);
        assert!(!handoff.last_failed());
        assert_ne!(err_bit(), 0, "a retry with no new write must not heal");
        s.last_handoff = Instant::now() - EVERYSEC;
        assert!(!s.due(), "nothing owed: clean, and the retry succeeded");

        // A batch written after the failure, then a successful fsync: healed.
        s.note_written();
        s.last_handoff = Instant::now() - EVERYSEC;
        assert!(s.due());
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
        wait_until("the healing fsync to settle", || handoff.settled() == 3);
        assert_eq!(err_bit(), 0, "healed by a post-failure write + fsync");
        s.note_written();
        s.last_handoff = Instant::now() - EVERYSEC;
        assert_eq!(s.claim(), Claim::Owned);
        assert!(!s.heal_pending);
        handoff.abort();
    }

    /// The inline fallback follows the same heal rule.
    #[test]
    fn an_inline_retry_heals_only_after_a_new_write() {
        let err_bit = || super::super::AOF_FSYNC_ERR_WRITERS.load(Ordering::Relaxed) & (1 << 58);
        let mut s = EverysecSync::new(58, FsyncPolicy::Always); // no agent: inline
        s.note_written();
        s.inline_done(false);
        assert_ne!(err_bit(), 0);
        s.inline_done(true);
        assert_ne!(err_bit(), 0, "a retry with no new write must not heal");
        s.note_written();
        s.inline_done(true);
        assert_eq!(err_bit(), 0);
    }

    /// R1 review, finding 6: a failed post-fold fsync is recorded and
    /// retried within ~100 ms, with no new write.
    #[test]
    fn a_failed_boundary_fsync_is_recorded_and_retried_soon() {
        let err_bit = || super::super::AOF_FSYNC_ERR_WRITERS.load(Ordering::Relaxed) & (1 << 59);
        let mut s = EverysecSync::with_backend(59, |_f: &std::fs::File| Ok(()));
        assert!(!s.due());
        s.boundary_fsync_failed();
        assert_ne!(err_bit(), 0, "recorded in aof_last_fsync_status");
        assert!(!s.due(), "not at once");
        std::thread::sleep(Duration::from_millis(150));
        assert!(s.due(), "retried within ~100 ms, no new write needed");
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
        let handoff = Arc::clone(&s.agent.as_ref().expect("agent").handoff);
        wait_until("the retry to settle", || handoff.settled() == 1);
        assert_ne!(err_bit(), 0, "the retry alone does not heal");
        super::super::record_everysec_fsync_result(59, true);
    }

    /// A dup that fails releases the claim: the caller fsyncs inline and the
    /// next claim is not blocked by a job that never existed.
    #[test]
    fn a_failed_dup_releases_the_claim_for_the_inline_fallback() {
        let mut s = EverysecSync::with_backend(0, |_f: &std::fs::File| Ok(()));
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(!s.dispatch(Err(std::io::Error::other("EMFILE"))));
        assert!(s.due(), "still owed: the caller fsyncs inline");
        s.inline_done(true);
        assert!(!s.due());
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned, "the aborted claim was released");
        assert!(s.dispatch(file()));
    }

    /// R1 review, findings 3 and 4: `aof_delayed_fsync` counts like redis's
    /// field — once per 2 s the fsync has been in flight while a deadline
    /// waits for it, not once per stall — and INFO sees the fsync in flight
    /// and its age until it returns.
    #[test]
    fn a_stalled_fsync_is_counted_every_two_seconds_and_visible_in_info() {
        let gate = Gate::new();
        let mut s = EverysecSync::with_backend(2, gate.backend());
        // Declared after `s`, so it drops (opens the gate) first: a failed
        // assertion must not leave the agent held while `s` joins it.
        let _release = ReleaseOnDrop(Arc::clone(&gate));
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
        let slot = Arc::clone(&s.agent.as_ref().expect("agent").in_flight_since);
        assert_ne!(slot.load(Ordering::Relaxed), 0, "in flight");
        assert!(in_flight_fsyncs().0 >= 1);
        // The fsync has been running 5.x s (three deadlines later).
        s.dispatched_at = Instant::now() - Duration::from_millis(5_100);
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Postponed);
        assert_eq!(s.stalls_counted, 2, "two full 2 s periods");
        assert_eq!(s.claim(), Claim::Postponed);
        assert_eq!(s.stalls_counted, 2, "counted once per period, not per wake");
        s.dispatched_at = Instant::now() - Duration::from_millis(6_000);
        assert_eq!(s.claim(), Claim::Postponed);
        assert_eq!(s.stalls_counted, 3);
        // The age INFO reports is the slot's: back-date it to the clock's
        // first tick (the process may be younger than any fixed age).
        let before = mono_ms();
        slot.store(1, std::sync::atomic::Ordering::Relaxed);
        let (n, oldest) = in_flight_fsyncs();
        assert!(n >= 1 && oldest + 1 >= before, "({n}, {oldest}, {before})");
        assert!(s.due(), "still owed; the stall warning path runs");
        gate.release();
        let handoff = Arc::clone(&s.agent.as_ref().expect("agent").handoff);
        wait_until("the fsync to settle", || handoff.settled() == 1);
        assert_eq!(slot.load(Ordering::Relaxed), 0, "cleared when it returns");
        assert_eq!(s.claim(), Claim::Owned);
        assert_eq!(s.stalls_counted, 0, "a new fsync starts a new count");
        handoff.abort();
    }

    /// An agent leaves INFO's registry when it is dropped.
    #[test]
    fn a_dropped_agent_leaves_the_in_flight_registry() {
        let s = EverysecSync::with_backend(3, |_f: &std::fs::File| Ok(()));
        let slot = Arc::clone(&s.agent.as_ref().expect("agent").in_flight_since);
        assert!(IN_FLIGHT_SLOTS.lock().iter().any(|x| Arc::ptr_eq(x, &slot)));
        drop(s);
        assert!(!IN_FLIGHT_SLOTS.lock().iter().any(|x| Arc::ptr_eq(x, &slot)));
    }

    /// R1 review, finding 8: leaving everysec at runtime joins the agent
    /// after its in-flight fsync; entering it starts a new agent.
    #[test]
    fn a_runtime_policy_switch_drains_then_restarts_the_agent() {
        let gate = Gate::new();
        let mut s = EverysecSync::with_backend(4, gate.backend());
        // Declared after `s`, so it drops (opens the gate) first: a failed
        // assertion must not leave the agent held while `s` joins it.
        let _release = ReleaseOnDrop(Arc::clone(&gate));
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
        wait_until("the agent to enter the fsync", || {
            gate.calls.load(Ordering::SeqCst) == 1
        });
        let releaser = {
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(100));
                gate.release();
            })
        };
        let t = Instant::now();
        assert!(s.set_policy(FsyncPolicy::Always));
        assert!(
            t.elapsed() >= Duration::from_millis(90),
            "the switch must wait for the in-flight fsync ({:?})",
            t.elapsed()
        );
        releaser.join().expect("releaser");
        assert!(s.agent.is_none());
        assert_eq!(s.claim(), Claim::Inline);
        assert!(!s.set_policy(FsyncPolicy::Always), "no-op");
        assert!(s.set_policy(FsyncPolicy::EverySec));
        assert!(s.agent.is_some(), "a new agent under everysec");
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
    }

    /// R1 review NIT: an agent that unwinds mid-fsync settles its job as a
    /// failure instead of leaving the hand-off IN_FLIGHT; the writer then
    /// falls back to inline fsyncs.
    #[test]
    fn an_agent_that_unwinds_mid_fsync_does_not_stick_the_handoff() {
        let mut s = EverysecSync::with_backend(60, |_f: &std::fs::File| -> std::io::Result<()> {
            panic!("injected agent panic (expected in this test)")
        });
        let handoff = Arc::clone(&s.agent.as_ref().expect("agent").handoff);
        s.note_written();
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
        wait_until("the dying agent to settle its job", || {
            handoff.settled() == 1
        });
        assert!(!handoff.in_flight(), "never stuck IN_FLIGHT");
        assert!(handoff.last_failed());
        s.last_handoff = Instant::now() - EVERYSEC;
        assert!(s.due(), "the failure keeps the deadline armed");
        assert_eq!(s.claim(), Claim::Owned, "not postponed forever");
        // The agent is gone: the claim is released, the caller fsyncs
        // inline, and every later deadline is inline too.
        assert!(!s.dispatch(file()), "a dead agent must refuse the job");
        assert!(!handoff.in_flight());
        s.inline_done(true);
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Inline);
        s.inline_done(true);
        super::super::record_everysec_fsync_result(60, true);
    }

    #[test]
    fn no_agent_means_inline() {
        let mut s = EverysecSync::new(0, FsyncPolicy::Always);
        assert_eq!(s.claim(), Claim::Inline);
        s.note_written();
        s.inline_done(false);
        assert!(s.dirty, "a failed inline fsync keeps the write owed");
    }

    /// The real backend: the dup's fdatasync covers bytes written through the
    /// writer's own handle.
    #[test]
    fn the_real_agent_fsyncs_a_dup_of_the_writers_fd() {
        use std::io::Write;
        let mut f = tempfile::tempfile().expect("tempfile");
        let mut s = EverysecSync::new(0, FsyncPolicy::EverySec);
        f.write_all(b"record").expect("write");
        s.note_written();
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(f.try_clone()));
        let handoff = Arc::clone(&s.agent.as_ref().expect("agent").handoff);
        wait_until("the fsync to settle", || handoff.settled() == 1);
        assert!(!handoff.last_failed());
    }

    /// Dropping the writer's state joins the agent, including one in the
    /// middle of an fsync (it finishes first).
    #[test]
    fn drop_joins_the_agent_after_its_fsync() {
        let gate = Gate::new();
        let mut s = EverysecSync::with_backend(0, gate.backend());
        // Declared after `s`, so it drops (opens the gate) first: a failed
        // assertion must not leave the agent held while `s` joins it.
        let _release = ReleaseOnDrop(Arc::clone(&gate));
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
        wait_until("the agent to enter the fsync", || {
            gate.calls.load(Ordering::SeqCst) == 1
        });
        let releaser = {
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(30));
                gate.release();
            })
        };
        drop(s);
        releaser.join().expect("releaser");
        assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    }
}
