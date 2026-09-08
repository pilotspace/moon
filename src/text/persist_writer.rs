//! The one thread that writes `.tpost` files.
//!
//! The shard thread ENCODES (`TextIndex::encode_for_persist`, pure CPU) and
//! hands the bytes here; this thread does tmp → fsync → rename → dir fsync
//! (`atomic_write_durable`, the `kv_spill.rs` pattern) and file deletion.
//! Nothing on the shard thread ever blocks on a syscall for postings —
//! moon#59 moved the manifest fsync off-loop for the same reason, and #873
//! removed a per-index fsync from the restore path.
//!
//! Queue semantics: one pending job per path, latest wins (a burst of
//! mutations to one index costs one write); FIFO across paths; pending bytes
//! are bounded by `MAX_PENDING_BYTES` (the shard stops encoding while the
//! writer is behind — `pending_bytes` is the back-pressure signal).
//! `flush_blocking` is the shutdown barrier: it returns once every job
//! submitted before the call has been attempted, with the first error seen.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

/// Back-pressure ceiling on bytes queued for writing.
pub const MAX_PENDING_BYTES: usize = 256 << 20;

enum Job {
    Write(Vec<u8>),
    Delete,
}

struct State {
    jobs: HashMap<PathBuf, Job>,
    order: VecDeque<PathBuf>,
    pending_bytes: usize,
    /// Set while the worker runs a job (an ack must wait for it).
    in_flight: bool,
    last_error: Option<String>,
    acks: Vec<flume::Sender<io::Result<()>>>,
    thread_gone: bool,
}

/// Handle to the writer thread. `writer()` gives the process-wide one.
pub struct PersistWriter {
    state: Arc<Mutex<State>>,
    wake: Arc<Condvar>,
}

fn run(state: Arc<Mutex<State>>, wake: Arc<Condvar>) {
    loop {
        let (path, job) = {
            let mut st = state.lock();
            loop {
                if let Some(path) = st.order.pop_front() {
                    if let Some(job) = st.jobs.remove(&path) {
                        if let Job::Write(b) = &job {
                            st.pending_bytes = st.pending_bytes.saturating_sub(b.len());
                        }
                        st.in_flight = true;
                        break (path, job);
                    }
                    continue;
                }
                // Queue drained and nothing in flight: release the barriers.
                st.in_flight = false;
                if !st.acks.is_empty() {
                    let err = st.last_error.take();
                    for ack in st.acks.drain(..) {
                        let _ = ack.send(match &err {
                            Some(e) => Err(io::Error::other(e.clone())),
                            None => Ok(()),
                        });
                    }
                }
                wake.wait(&mut st);
            }
        };
        let result = match job {
            Job::Write(bytes) => crate::persistence::atomic::atomic_write_durable(&path, &bytes)
                .map_err(io::Error::from),
            Job::Delete => match std::fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e),
            },
        };
        if let Err(e) = result {
            tracing::warn!("text postings writer: {} failed: {e}", path.display());
            state.lock().last_error = Some(format!("{}: {e}", path.display()));
        }
    }
}

impl PersistWriter {
    fn spawn(name: &str) -> Self {
        let state = Arc::new(Mutex::new(State {
            jobs: HashMap::new(),
            order: VecDeque::new(),
            pending_bytes: 0,
            in_flight: false,
            last_error: None,
            acks: Vec::new(),
            thread_gone: false,
        }));
        let wake = Arc::new(Condvar::new());
        let (ts, tw) = (Arc::clone(&state), Arc::clone(&wake));
        let thread_name = name.to_owned();
        let spawned = std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                crate::shard::numa::pin_current_aux_thread(&thread_name);
                run(ts, tw);
            });
        if let Err(e) = spawned {
            // Without the thread nothing is ever written: postings persistence
            // degrades to "rebuild on every boot", loudly.
            tracing::error!("failed to spawn the text postings writer thread: {e}");
            state.lock().thread_gone = true;
        }
        Self { state, wake }
    }

    fn submit(&self, path: PathBuf, job: Job) {
        let mut st = self.state.lock();
        if st.thread_gone {
            return;
        }
        if let Job::Write(b) = &job {
            st.pending_bytes += b.len();
        }
        match st.jobs.insert(path.clone(), job) {
            Some(Job::Write(old)) => st.pending_bytes = st.pending_bytes.saturating_sub(old.len()),
            Some(Job::Delete) | None => {}
        }
        if !st.order.contains(&path) {
            st.order.push_back(path);
        }
        drop(st);
        self.wake.notify_one();
    }

    /// Queue `bytes` for an atomic durable write to `path` (latest wins).
    pub fn submit_write(&self, path: PathBuf, bytes: Vec<u8>) {
        self.submit(path, Job::Write(bytes));
    }

    /// Queue deletion of `path` (a missing file is not an error).
    pub fn submit_delete(&self, path: PathBuf) {
        self.submit(path, Job::Delete);
    }

    /// Bytes queued and not yet handed to the filesystem — the back-pressure
    /// signal for `TextStore::persist_dirty_postings`.
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.state.lock().pending_bytes
    }

    /// Block until every job submitted before this call has been attempted.
    /// Returns the first error recorded since the last barrier, if any.
    pub fn flush_blocking(&self, timeout: Duration) -> io::Result<()> {
        let (tx, rx) = flume::bounded::<io::Result<()>>(1);
        {
            let mut st = self.state.lock();
            if st.thread_gone {
                return Err(io::Error::other(
                    "text postings writer thread is not running",
                ));
            }
            if st.order.is_empty() && !st.in_flight {
                return match st.last_error.take() {
                    Some(e) => Err(io::Error::other(e)),
                    None => Ok(()),
                };
            }
            st.acks.push(tx);
        }
        self.wake.notify_one();
        rx.recv_timeout(timeout)
            .map_err(|_| io::Error::other("timed out waiting for the text postings writer"))?
    }
}

/// The process-wide writer (one thread; `.tpost` writes are rare and
/// sequential by nature).
pub fn writer() -> &'static PersistWriter {
    static GLOBAL: OnceLock<PersistWriter> = OnceLock::new();
    GLOBAL.get_or_init(|| PersistWriter::spawn("moon-text-persist"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_land_durably_and_latest_submission_wins() {
        let w = PersistWriter::spawn("moon-text-persist-test");
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("a.tpost");
        w.submit_write(path.clone(), b"first".to_vec());
        w.submit_write(path.clone(), b"second".to_vec());
        w.flush_blocking(Duration::from_secs(10)).expect("flush");
        assert_eq!(std::fs::read(&path).unwrap(), b"second");
        assert_eq!(w.pending_bytes(), 0);
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(
            leftovers.len(),
            1,
            "no temp file left behind: {leftovers:?}"
        );
    }

    #[test]
    fn delete_removes_and_missing_is_not_an_error() {
        let w = PersistWriter::spawn("moon-text-persist-test2");
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("b.tpost");
        w.submit_write(path.clone(), b"x".to_vec());
        w.flush_blocking(Duration::from_secs(10)).expect("flush");
        assert!(path.exists());
        w.submit_delete(path.clone());
        w.submit_delete(tmp.path().join("never-existed.tpost"));
        w.flush_blocking(Duration::from_secs(10)).expect("flush");
        assert!(!path.exists());
    }

    #[test]
    fn a_failed_write_is_reported_by_the_next_barrier() {
        let w = PersistWriter::spawn("moon-text-persist-test3");
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("no-such-dir").join("c.tpost");
        w.submit_write(path, b"x".to_vec());
        let err = w
            .flush_blocking(Duration::from_secs(10))
            .expect_err("must surface");
        assert!(err.to_string().contains("c.tpost"), "{err}");
        // and the error is consumed by that barrier
        w.flush_blocking(Duration::from_secs(10))
            .expect("clean after report");
    }

    #[test]
    fn flush_on_an_idle_writer_returns_immediately() {
        let w = PersistWriter::spawn("moon-text-persist-test4");
        w.flush_blocking(Duration::from_millis(50)).expect("idle");
    }
}
