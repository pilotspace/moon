//! The shard's `ShardManifest`, reachable from the write-path eviction gates
//! (moon#1290 N6).
//!
//! The manifest is owned by the shard event loop. Without an AOF a victim may
//! leave RAM only after the spill file that holds it is durably listed in the
//! manifest (`evict_batch_durable`), so an eviction gate that cannot reach the
//! manifest has no durable way to tier a key. The connection write gates
//! (`handler_monoio::run_write_eviction_gate`, the two `handler_sharded`
//! gates) and the Lua bridge had none: under `--appendonly no
//! --disk-offload enable` every victim they picked was PLAIN-DROPPED — 5.4K
//! of 16.2K keys in the moon#1290 repro, with 1–7 spilled — although the
//! operator asked for tiering, not eviction.
//!
//! The event loop keeps the manifest in an `Rc<RefCell<..>>` and registers
//! it here, on its own thread. Every connection task of the shard runs on
//! that thread (their `ConnectionContext` already shares `Rc` state with the
//! loop), and no borrow is held across an `.await` — the loop borrows it for
//! one call at a time, and [`with_manifest`] only for one eviction run — so
//! a gate never observes it borrowed except when it is itself running inside
//! a loop call that holds it (a script or cross-shard write drained by the
//! event loop). Such a caller is handed `None` and takes the no-manifest path
//! it always took; the SPSC gate is passed the loop's manifest directly.

use std::cell::RefCell;
use std::rc::Rc;

use crate::persistence::manifest::ShardManifest;

/// The event loop's manifest cell.
pub(crate) type SharedManifest = Rc<RefCell<Option<ShardManifest>>>;

thread_local! {
    static SHARED: RefCell<Option<SharedManifest>> = const { RefCell::new(None) };
}

/// Register this shard thread's manifest cell (the event loop, at start).
pub(crate) fn register(cell: &SharedManifest) {
    SHARED.with(|s| *s.borrow_mut() = Some(Rc::clone(cell)));
}

/// Forget this thread's manifest cell (the event loop, on exit).
pub(crate) fn unregister() {
    SHARED.with(|s| *s.borrow_mut() = None);
}

/// Run `f` with this shard's manifest: `None` off a shard thread, with no
/// manifest (disk offload off, or it failed to open), or while the event
/// loop itself holds it (see the module doc).
pub(crate) fn with_manifest<R>(f: impl FnOnce(Option<&mut ShardManifest>) -> R) -> R {
    SHARED.with(|s| {
        let registered = s.borrow();
        match registered.as_ref().map(|cell| cell.try_borrow_mut()) {
            Some(Ok(mut guard)) => f(guard.as_mut()),
            _ => f(None),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_registered_manifest_is_lent_unless_the_loop_holds_it() {
        let tmp = tempfile::tempdir().unwrap();
        let m = ShardManifest::create(&tmp.path().join("m.manifest")).unwrap();
        assert!(with_manifest(|m| m.is_none()), "unregistered: None");
        let cell: SharedManifest = Rc::new(RefCell::new(Some(m)));
        register(&cell);
        assert!(with_manifest(|m| m.is_some()), "registered: lent");
        {
            let _held = cell.borrow_mut();
            assert!(with_manifest(|m| m.is_none()), "held by the loop: None");
        }
        unregister();
        assert!(with_manifest(|m| m.is_none()), "unregistered again: None");
    }
}
