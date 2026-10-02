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

/// Holders per path (a path is open when absent). A `Vec`: a process holds
/// one gate per instance booting at once, and a `const` mutex needs no lazy
/// init.
static HELD: parking_lot::Mutex<Vec<(PathBuf, usize)>> = parking_lot::Mutex::new(Vec::new());

/// Keep the writer of `path` from opening it until the guard is dropped.
pub fn hold_writer_open(path: &Path) -> OpenGateGuard {
    let mut held = HELD.lock();
    match held.iter_mut().find(|(p, _)| p == path) {
        Some((_, n)) => *n += 1,
        None => held.push((path.to_path_buf(), 1)),
    }
    OpenGateGuard(path.to_path_buf())
}

/// Whether the writer of `path` may open it now.
pub fn is_open(path: &Path) -> bool {
    !HELD.lock().iter().any(|(p, n)| p == path && *n > 0)
}

/// Re-opens the gate when dropped — on every exit path of the boot.
pub struct OpenGateGuard(PathBuf);

impl OpenGateGuard {
    /// Never re-open the gate: the boot refused to start (R2b round 2 F1),
    /// and the writer must not open — nor, at its stop, append to — the file
    /// the boot could not read. It exits at cancellation without opening.
    pub fn keep_closed(self) {
        std::mem::forget(self);
    }
}

impl Drop for OpenGateGuard {
    fn drop(&mut self) {
        let mut held = HELD.lock();
        if let Some(i) = held.iter().position(|(p, _)| *p == self.0) {
            held[i].1 -= 1;
            if held[i].1 == 0 {
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
        if cancel.is_cancelled() {
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
        hold_writer_open(&b).keep_closed();
        assert!(!is_open(&b), "a refused boot keeps its writer out");
    }
}
