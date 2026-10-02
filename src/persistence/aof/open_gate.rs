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
//!
//! A boot that refuses to start stops ITS writer through the writer's own
//! cancellation token (which [`wait_writer_open`] watches), joins it, then
//! drops its guard. The refusal is that writer's alone (R2b round 4
//! F-G-SHARE: a flag on the shared path entry stopped every instance's
//! writer waiting on the path).

use std::path::{Path, PathBuf};

/// One gated path and how many boots hold it.
struct Held {
    path: PathBuf,
    holders: usize,
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
        }),
    }
    OpenGateGuard(path.to_path_buf())
}

/// Whether the writer of `path` may open it now.
pub fn is_open(path: &Path) -> bool {
    !HELD.lock().iter().any(|h| h.path == path && h.holders > 0)
}

/// Re-opens the gate when dropped — on every exit path of the boot.
pub struct OpenGateGuard(PathBuf);

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
/// first — the writer's shutdown, or its own boot refusing to start (the
/// writer then exits without opening anything).
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
    }

    /// R2b round 3 F-G / round 4 F-G-SHARE: a refused boot stops its own
    /// writer (its token) and nobody else's — another instance's writer
    /// waiting on the same path keeps waiting, and opens once every guard is
    /// dropped.
    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn a_refusal_stops_only_the_refused_boots_writer() {
        use crate::runtime::cancel::CancellationToken;
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("appendonly.aof");
        let refused_boot = hold_writer_open(&a);
        let other_boot = hold_writer_open(&a);
        let refused_writer = CancellationToken::new();
        let other_writer = CancellationToken::new();
        refused_writer.cancel();
        assert!(
            !wait_writer_open(&a, &refused_writer).await,
            "the refused boot's writer exits without opening"
        );
        drop(refused_boot);
        let waiting = {
            let a = a.clone();
            let other_writer = other_writer.clone();
            tokio::spawn(async move { wait_writer_open(&a, &other_writer).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert!(
            !waiting.is_finished(),
            "the other instance's writer still waits"
        );
        drop(other_boot);
        assert!(
            waiting.await.unwrap(),
            "and opens once its boot releases the gate"
        );
        assert!(is_open(&a), "nothing stays held");
    }
}
