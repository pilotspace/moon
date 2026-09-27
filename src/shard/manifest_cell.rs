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
//! event loop). The SPSC gate is passed the loop's manifest directly.
//!
//! A script routed to this shard runs INSIDE that drain (moon#1290 wave-1
//! review F2): the loop holds the cell for the whole drain, and the script's
//! bridge gate cannot be handed a parameter — its `redis.call` closure was
//! built once at VM setup. The drain therefore [`lend`]s its `&mut` manifest
//! for the duration of the script: the value moves into a second
//! thread-local slot, [`with_manifest`] finds it there, and it moves back
//! when the script returns (or unwinds). Before this, the gate was handed
//! `None` and PLAIN-DROPPED every victim of a routed `EVAL` at `--shards`
//! >= 2 under `--appendonly no --disk-offload enable`.

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
/// loop itself holds it without having [`lend`]ed it (see the module doc).
pub(crate) fn with_manifest<R>(f: impl FnOnce(Option<&mut ShardManifest>) -> R) -> R {
    SHARED.with(|s| {
        let registered = s.borrow();
        match registered.as_ref().map(|cell| cell.try_borrow_mut()) {
            Some(Ok(mut guard)) => f(guard.as_mut()),
            // The loop holds the cell: the drain may have lent it.
            _ => LENT.with(|lent| match lent.try_borrow_mut() {
                Ok(mut lent) => f(lent.as_mut()),
                Err(_) => f(None),
            }),
        }
    })
}

thread_local! {
    /// The manifest a drain lent for one routed script (see [`lend`]).
    static LENT: RefCell<Option<ShardManifest>> = const { RefCell::new(None) };
}

/// Run `f` with `slot`'s manifest reachable through [`with_manifest`] —
/// for a caller that holds the event loop's manifest `&mut` and runs code
/// (a routed script) whose eviction gate can only reach it through this
/// module. The manifest MOVES into a thread-local for the duration of `f` and
/// back into `slot` afterwards, also when `f` unwinds; `slot` is `None`
/// meanwhile. No allocation: a `ShardManifest` move is a memcpy.
///
/// A nested call (something already lent) runs `f` without lending again.
pub(crate) fn lend<R>(slot: &mut Option<ShardManifest>, f: impl FnOnce() -> R) -> R {
    if slot.is_none() {
        return f();
    }
    let lent = LENT.with(|lent| match lent.try_borrow_mut() {
        Ok(mut lent) if lent.is_none() => {
            *lent = slot.take();
            true
        }
        _ => false,
    });
    if !lent {
        return f();
    }
    /// Moves the manifest back into the caller's slot, on return and on
    /// unwind alike.
    struct Restore<'a>(&'a mut Option<ShardManifest>);
    impl Drop for Restore<'_> {
        fn drop(&mut self) {
            LENT.with(|lent| {
                if let Ok(mut lent) = lent.try_borrow_mut() {
                    *self.0 = lent.take();
                }
            });
        }
    }
    let _restore = Restore(slot);
    f()
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

    /// moon#1290 wave-1 review F2: a routed script runs inside the drain,
    /// which holds the cell. Lent, the drain's manifest is reachable; after
    /// the lend (also after a panic inside it) it is back in the drain's
    /// slot and no longer reachable.
    #[test]
    fn a_drain_lends_its_held_manifest_to_a_routed_script() {
        let tmp = tempfile::tempdir().unwrap();
        let m = ShardManifest::create(&tmp.path().join("m.manifest")).unwrap();
        let cell: SharedManifest = Rc::new(RefCell::new(Some(m)));
        register(&cell);
        {
            let mut held = cell.borrow_mut();
            assert!(with_manifest(|m| m.is_none()), "held, not lent: None");
            let seen = lend(&mut held, || with_manifest(|m| m.is_some()));
            assert!(seen, "lent: the script's gate reaches the manifest");
            assert!(held.is_some(), "returned to the drain's slot");
            // Nested lend: the inner call runs, nothing is lost.
            let nested = lend(&mut held, || {
                let mut other = None;
                lend(&mut other, || with_manifest(|m| m.is_some()))
            });
            assert!(nested);
            assert!(held.is_some());
            // Unwind: the manifest still comes back.
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                lend(&mut held, || panic!("script panicked"))
            }));
            assert!(r.is_err());
            assert!(held.is_some(), "returned on unwind");
            assert!(with_manifest(|m| m.is_none()), "no longer lent");
        }
        unregister();
    }
}
