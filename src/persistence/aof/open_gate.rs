//! The tokio TopLevel AOF writer's open gate, one per AOF path (R2b review
//! P1; per path since R2b round 2 N5).
//!
//! The writer opens its file with `O_APPEND` when its thread starts, BEFORE
//! recovery runs. A boot that publishes a fresh generation by rename
//! (`fresh_generation`) would leave such a writer appending to the replaced
//! inode, so the boot holds the gate for the writer's path until the
//! generation is opened, and the writer waits for it. Keyed by path, so two
//! embedded instances in one process (different `--dir`s) never wait on each
//! other's boot; an unheld path is open (every writer that no boot gates —
//! the legacy listener, tests — opens at once).

use std::path::{Path, PathBuf};

/// One gated path: how many boots hold it, and whether one refused to start
/// (its writer must exit without opening the file).
struct Held {
    path: PathBuf,
    holders: usize,
    refused: bool,
}

/// Gated paths (a path is open when absent). A `Vec`: a process holds one
/// gate per instance booting at once, and a `const` mutex needs no lazy init.
static HELD: parking_lot::Mutex<Vec<Held>> = parking_lot::Mutex::new(Vec::new());

/// Keep the writer of `path` from opening it until the guard is dropped.
pub fn hold_writer_open(path: &Path) -> OpenGateGuard {
    let mut held = HELD.lock();
    match held.iter_mut().find(|h| h.path == path) {
        Some(h) => h.holders += 1,
        None => held.push(Held {
            path: path.to_path_buf(),
            holders: 1,
            refused: false,
        }),
    }
    OpenGateGuard(path.to_path_buf())
}

/// Whether the writer of `path` may open it now.
pub fn is_open(path: &Path) -> bool {
    !HELD.lock().iter().any(|h| h.path == path && h.holders > 0)
}

/// Whether a boot holding `path`'s gate refused to start: its writer must
/// exit without opening the file.
pub fn is_refused(path: &Path) -> bool {
    HELD.lock().iter().any(|h| h.path == path && h.refused)
}

/// Re-opens the gate when dropped — on every exit path of the boot.
pub struct OpenGateGuard(PathBuf);

impl OpenGateGuard {
    /// The boot refused to start (R2b round 2 F1): the writer waiting on this
    /// gate exits without opening — nor, at its stop, appending to — the file
    /// the boot could not read. The caller joins the writer, THEN drops the
    /// guard, which removes the gate (R2b round 3 F-G: the old `mem::forget`
    /// leaked it, and a later instance on the same dir never opened its
    /// writer).
    pub fn refuse(&self) {
        if let Some(h) = HELD.lock().iter_mut().find(|h| h.path == self.0) {
            h.refused = true;
        }
    }
}

impl Drop for OpenGateGuard {
    fn drop(&mut self) {
        let mut held = HELD.lock();
        if let Some(i) = held.iter().position(|h| h.path == self.0) {
            held[i].holders -= 1;
            if held[i].holders == 0 {
                held.swap_remove(i);
            }
        }
    }
}

/// Wait until no boot holds the gate of `path`. `false` when `cancel` fired
/// first (the writer then exits without opening anything).
#[cfg(feature = "runtime-tokio")]
pub async fn wait_writer_open(
    path: &Path,
    cancel: &crate::runtime::cancel::CancellationToken,
) -> bool {
    while !is_open(path) {
        if cancel.is_cancelled() || is_refused(path) {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gate_closes_its_own_path_only_while_any_guard_lives() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("a/appendonly.aof");
        let b = tmp.path().join("b/appendonly.aof");
        assert!(is_open(&a) && is_open(&b));
        let g1 = hold_writer_open(&a);
        let g2 = hold_writer_open(&a);
        assert!(!is_open(&a));
        assert!(is_open(&b), "another instance's path is not gated (N5)");
        drop(g1);
        assert!(!is_open(&a));
        drop(g2);
        assert!(is_open(&a));
    }

    /// R2b round 3 F-G: a refused boot's gate tells its writer to exit, and
    /// dropping the guard afterwards leaves the path open for the next
    /// instance (it used to stay held for the life of the process).
    #[test]
    fn a_refused_gate_stops_its_writer_and_is_released_on_drop() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("appendonly.aof");
        let g = hold_writer_open(&a);
        assert!(!is_refused(&a));
        g.refuse();
        assert!(is_refused(&a) && !is_open(&a));
        drop(g);
        assert!(
            is_open(&a) && !is_refused(&a),
            "the next instance opens its writer"
        );
        let again = hold_writer_open(&a);
        assert!(!is_refused(&a), "a new boot starts unrefused");
        drop(again);
    }
}
