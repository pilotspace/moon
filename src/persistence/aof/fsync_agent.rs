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
//!   ([`super::fsync_handoff`], loom-modeled in `tests/loom_aof_fsync_agent.rs`),
//!   and counted in INFO `aof_delayed_fsync`.
//! * The agent records the outcome exactly where the inline fsync did: the
//!   fsync-latency metric and `record_everysec_fsync_result` (INFO
//!   `aof_last_fsync_status` / `aof_fsync_failures`). A failed fsync is
//!   retried at the next deadline even when nothing new was written.
//! * No agent (the OS refused the thread, or it died): the writer fsyncs
//!   inline, exactly as before. A durability request is never dropped.
//!
//! `appendfsync always` does not use the agent: its per-batch fsync stays on
//! the writer, BEFORE the batch's acks (`group_commit`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::FsyncPolicy;
use super::fsync_handoff::{Begin, FsyncHandoff};

/// INFO `aof_delayed_fsync`: everysec deadlines (all writers) whose fsync
/// hand-off had to be postponed because the writer's previous fsync was
/// still running — counted once per deadline, however many wakes it took.
/// redis's field of the same name counts its postponed WRITES; moon never
/// postpones a write.
pub static AOF_DELAYED_FSYNC: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The everysec deadline: an fsync is handed off at most once per this.
const EVERYSEC: Duration = Duration::from_secs(1);

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

/// One writer's fsync agent thread.
struct AofFsyncAgent {
    tx: flume::Sender<std::fs::File>,
    handoff: Arc<FsyncHandoff>,
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
        let (tx, rx) = flume::bounded::<std::fs::File>(1);
        let handoff = Arc::new(FsyncHandoff::new());
        let agent_handoff = Arc::clone(&handoff);
        let name = format!("aof-fsync-{writer_idx}");
        let thread = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                crate::shard::numa::pin_current_aux_thread(&name);
                while let Ok(file) = rx.recv() {
                    sync_gate_for_test();
                    let t = Instant::now();
                    let result = backend(&file);
                    drop(file);
                    match &result {
                        Ok(()) => {
                            crate::admin::metrics_setup::record_aof_fsync(
                                t.elapsed().as_micros() as u64
                            );
                            super::record_everysec_fsync_result(writer_idx, true);
                        }
                        Err(e) => {
                            tracing::error!(
                                "AOF everysec fsync failed (writer {writer_idx}, agent): {e}"
                            );
                            super::record_everysec_fsync_result(writer_idx, false);
                        }
                    }
                    // Outcome recorded first: a writer that sees IDLE sees it.
                    agent_handoff.finish(result.is_ok());
                }
            })?;
        Ok(Self {
            tx,
            handoff,
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
    /// The current deadline was already counted as postponed.
    postponed: bool,
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
            postponed: false,
        }
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
            postponed: false,
        }
    }

    /// A batch was written (reached the kernel) and is not yet fsynced.
    #[inline]
    pub(super) fn note_written(&mut self) {
        self.dirty = true;
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
    pub(super) fn due(&self) -> bool {
        (self.dirty || self.agent.as_ref().is_some_and(|a| a.handoff.last_failed()))
            && self.last_handoff.elapsed() >= EVERYSEC
    }

    /// Claim the next fsync.
    pub(super) fn claim(&mut self) -> Claim {
        let Some(agent) = self.agent.as_ref() else {
            return Claim::Inline;
        };
        match agent.handoff.try_begin() {
            Begin::Owned => {
                self.postponed = false;
                Claim::Owned
            }
            Begin::Postponed => {
                if !self.postponed {
                    self.postponed = true;
                    AOF_DELAYED_FSYNC.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        let sent = match dup {
            Ok(file) => agent.tx.try_send(file).is_ok(),
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
            self.last_handoff = Instant::now();
            self.dirty = false;
        } else {
            agent.handoff.abort();
        }
        sent
    }

    /// The caller's inline fallback fsync returned. A failure keeps the
    /// deadline armed (retried on the next wake, as before the agent).
    pub(super) fn inline_done(&mut self, ok: bool) {
        if ok {
            self.last_handoff = Instant::now();
            self.dirty = false;
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
    /// queued, and counted; once it settles the next claim succeeds.
    #[test]
    fn a_deadline_during_a_running_fsync_is_postponed_and_counted() {
        let gate = Gate::new();
        let mut s = EverysecSync::with_backend(1, gate.backend());
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
        s.backdate(EVERYSEC);
        let before = AOF_DELAYED_FSYNC.load(Ordering::Relaxed);
        assert_eq!(s.claim(), Claim::Postponed);
        assert_eq!(s.claim(), Claim::Postponed);
        // INFO counts the deadline once; the hand-off counts every attempt.
        assert!(AOF_DELAYED_FSYNC.load(Ordering::Relaxed) > before);
        assert!(s.postponed);
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
    /// written; the writer's status bit is set by the agent.
    #[test]
    fn a_failed_fsync_is_retried_without_new_writes() {
        let mut s = EverysecSync::with_backend(57, |_f: &std::fs::File| {
            Err(std::io::Error::other("disk gone"))
        });
        s.backdate(EVERYSEC);
        assert_eq!(s.claim(), Claim::Owned);
        assert!(s.dispatch(file()));
        let handoff = Arc::clone(&s.agent.as_ref().expect("agent").handoff);
        wait_until("the fsync to settle", || handoff.settled() == 1);
        assert!(handoff.last_failed());
        assert!(!s.due(), "not before the next second");
        s.last_handoff = Instant::now() - EVERYSEC;
        assert!(s.due(), "a failed fsync keeps the deadline armed");
        assert!(super::super::AOF_FSYNC_ERR_WRITERS.load(Ordering::Relaxed) & (1 << 57) != 0);
        super::super::record_everysec_fsync_result(57, true);
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
