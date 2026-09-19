//! Owner-side registration of a multi-key blocking waiter (moon#989).
//!
//! A multi-key `BLPOP`/`BRPOP`/`BZPOPMIN`/`BZPOPMAX`/`BLMPOP`/`BZMPOP` whose
//! keys are owned by another shard used to reach that owner as one
//! `BlockRegister` per key. Each was handled on its own — register the key,
//! see data, serve the waiter — so the SAME waiter was served once per
//! non-empty key. The client kept the first reply and dropped the rest, and
//! every element in the dropped replies had already left the keyspace:
//!
//! ```text
//! BLMPOP 0.3 3 {t}a {t}b {t}c LEFT   ({t}a empty, {t}b=[B1 B2], {t}c=[C1 C2])
//! redis 8.6.1     -> {t}b [B1]   {t}c=[C1 C2]
//! moon --shards 4 -> {t}b [B1]   {t}c=[C2]      <-- C1 served to nobody
//! ```
//!
//! [`register_group`] receives every key this shard owns of one waiter in a
//! single message and handles them in one synchronous stretch of the owner's
//! event loop. Nothing can interleave with it, so the waiter is served at most
//! once by construction: every key is registered under one `wait_id` before
//! any wake runs, and the wake that serves the waiter runs `remove_wait`,
//! which unregisters all of its siblings before the next key is looked at.

use bytes::Bytes;

use crate::blocking::{BlockedCommand, BlockingRegistry, WaitEntry, WaitFamily};
use crate::protocol::Frame;
use crate::shard::dispatch::{BlockRegisterGroupPayload, BlockRegisterMember};
use crate::storage::Database;

/// Register — and if the data is already there, serve — every key of one
/// multi-key waiter that this shard owns.
///
/// Keys are visited in the command's argument order, which is the order Redis
/// serves them in: the first non-empty key answers, exactly once.
///
/// When the group is the WHOLE command (every key co-located here) the owner
/// is the only place that can see all of it, so it also answers the
/// `-WRONGTYPE` Redis owes for the first existing key of the wrong type, and
/// never registers in that case. A partial group cannot: the waiter's other
/// keys live on other shards and may be serving right now.
///
/// Remote registrations carry no deadline; the client's own timer times the
/// wait out and sends `BlockCancel`, which removes every member at once.
pub fn register_group(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    payload: BlockRegisterGroupPayload,
) {
    let BlockRegisterGroupPayload {
        db_index,
        wait_id,
        members,
        whole_command,
    } = payload;

    // Every member carries the same command, so they share one family.
    let family = members.first().map(|m| m.cmd.family());

    if whole_command && let Some(err) = first_type_error(db, &members) {
        // Nothing registered, so nothing to unwind; the client's `BlockCancel`
        // on the way out is a no-op. One reply is enough — the client skips
        // the members whose senders drop here.
        if let Some(first) = members.into_iter().next() {
            let _ = first.reply_tx.send(Some(err));
        }
        return;
    }

    let mut keys: smallvec::SmallVec<[Bytes; 4]> = smallvec::SmallVec::with_capacity(members.len());
    for BlockRegisterMember {
        key,
        mut cmd,
        reply_tx,
    } in members
    {
        // moon#595: bind `$` here, with no suspension point before `register`.
        // A no-op for everything that is not an `XREAD ... $`.
        cmd.bind_stream_since(db, &key);
        registry.register(
            db_index,
            key.clone(),
            WaitEntry {
                wait_id,
                cmd,
                reply_tx,
                deadline: None,
            },
        );
        keys.push(key);
    }

    // Data may already be there (it arrived before the registration, or the
    // client's shard simply could not see it). Serve in argument order and
    // stop as soon as this waiter is gone — served, or reaped as dead — so a
    // later key is never consulted on its behalf.
    //
    // Only a key holding this waiter's type is offered to the wakers. A waker
    // handed a waiter it cannot serve (a `BLPOP` waiter on a string) pops it,
    // fails, and runs the served-waiter cleanup — `remove_wait` plus a `None`
    // reply — which would silently unregister every SIBLING key on this shard
    // too, leaving the client parked on keys nobody is watching. A wrong-typed
    // key is skipped instead; for a whole-command group it cannot come before
    // the key that serves, because `first_type_error` already answered it.
    for key in &keys {
        if !registry.is_waiting(wait_id) {
            break;
        }
        if db.exists(key) && family.is_some_and(|f| family_type_error(db, key, f).is_none()) {
            crate::blocking::wakeup::try_wake_list_waiter(registry, db, db_index, key);
            crate::blocking::wakeup::try_wake_zset_waiter(registry, db, db_index, key);
            crate::blocking::wakeup::try_wake_stream_waiter(registry, db, db_index, key);
        }
    }
}

/// Redis's pre-block ladder, minus the pop: walk the keys in argument order,
/// answer `-WRONGTYPE` for the first existing key of the wrong type, and stop
/// at the first key that exists with the right one — that key serves, so
/// nothing after it is consulted (Redis does not type-check past it either).
///
/// Read-only: the list probe uses the shared-borrow accessor so that a type
/// check never flattens a compact encoding (moon#832).
fn first_type_error(db: &mut Database, members: &[BlockRegisterMember]) -> Option<Frame> {
    for m in members {
        if let Some(err) = type_error(db, &m.key, &m.cmd) {
            return Some(err);
        }
        if db.exists(&m.key) {
            return None;
        }
    }
    None
}

/// The error `cmd` owes its client for `key` before it may block: a wrong
/// type, or `XREADGROUP`'s missing key or group.
fn type_error(db: &mut Database, key: &Bytes, cmd: &BlockedCommand) -> Option<Frame> {
    match cmd.family() {
        WaitFamily::Stream => crate::blocking::wakeup::stream_register_error(db, key, cmd),
        family => family_type_error(db, key, family),
    }
}

/// `-WRONGTYPE` when `key` exists holding something `family` cannot pop from.
fn family_type_error(db: &mut Database, key: &Bytes, family: WaitFamily) -> Option<Frame> {
    match family {
        WaitFamily::List => {
            let now_ms = db.now_ms();
            db.get_list_ref_if_alive(key, now_ms).err()
        }
        WaitFamily::ZSet => db.get_sorted_set(key).err(),
        WaitFamily::Stream => db.get_stream(key).err(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blocking::Direction;
    use crate::runtime::channel::{self, OneshotReceiver};

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    /// A group for `keys`, all waiting with `cmd()`; returns the receivers in
    /// member order.
    fn group(
        reg: &mut BlockingRegistry,
        keys: &[&str],
        cmd: impl Fn() -> BlockedCommand,
        whole_command: bool,
    ) -> (
        BlockRegisterGroupPayload,
        Vec<OneshotReceiver<Option<Frame>>>,
    ) {
        let wait_id = reg.next_wait_id();
        let mut members = Vec::new();
        let mut rxs = Vec::new();
        for k in keys {
            let (tx, rx) = channel::oneshot();
            members.push(BlockRegisterMember {
                key: b(k),
                cmd: cmd(),
                reply_tx: tx,
            });
            rxs.push(rx);
        }
        (
            BlockRegisterGroupPayload {
                db_index: 0,
                wait_id,
                members,
                whole_command,
            },
            rxs,
        )
    }

    /// Every reply the waiter received, across all of its members.
    fn replies(rxs: &[OneshotReceiver<Option<Frame>>]) -> Vec<Frame> {
        rxs.iter()
            .filter_map(|rx| rx.try_recv().ok().flatten())
            .collect()
    }

    fn list(db: &mut Database, key: &str) -> Vec<Bytes> {
        let now_ms = db.now_ms();
        match db.get_list_ref_if_alive(&b(key), now_ms) {
            Ok(Some(l)) => l.iter_bytes(),
            _ => Vec::new(),
        }
    }

    fn blmpop() -> BlockedCommand {
        BlockedCommand::BLMPop {
            dir: Direction::Left,
            count: 1,
        }
    }

    /// The issue's exact shape: two non-empty keys, one waiter, ONE pop.
    #[test]
    fn two_non_empty_keys_serve_the_waiter_once_from_the_first() {
        for whole in [true, false] {
            let mut reg = BlockingRegistry::new(0);
            let mut db = Database::new();
            db.list_push_back(&b("b"), b("B1"));
            db.list_push_back(&b("b"), b("B2"));
            db.list_push_back(&b("c"), b("C1"));
            db.list_push_back(&b("c"), b("C2"));
            let (payload, rxs) = group(&mut reg, &["a", "b", "c"], blmpop, whole);
            let wait_id = payload.wait_id;
            register_group(&mut reg, &mut db, payload);

            let got = replies(&rxs);
            assert_eq!(
                got.len(),
                1,
                "whole={whole}: exactly one reply, got {got:?}"
            );
            assert_eq!(
                got[0],
                Frame::Array(crate::framevec![
                    Frame::BulkString(b("b")),
                    Frame::Array(crate::framevec![Frame::BulkString(b("B1"))]),
                ])
            );
            assert_eq!(list(&mut db, "b"), vec![b("B2")]);
            assert_eq!(
                list(&mut db, "c"),
                vec![b("C1"), b("C2")],
                "c must be untouched"
            );
            assert!(
                !reg.is_waiting(wait_id),
                "a served waiter is fully unregistered"
            );
        }
    }

    /// The same key named twice is one pop, not two.
    #[test]
    fn a_key_named_twice_is_popped_once() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        db.list_push_back(&b("b"), b("B1"));
        db.list_push_back(&b("b"), b("B2"));
        let (payload, rxs) = group(&mut reg, &["b", "b"], blmpop, true);
        register_group(&mut reg, &mut db, payload);
        assert_eq!(replies(&rxs).len(), 1);
        assert_eq!(list(&mut db, "b"), vec![b("B2")]);
    }

    /// Nothing to serve: every key is registered and a later push wakes the
    /// waiter exactly once, from the key that received the data.
    #[test]
    fn empty_keys_register_and_the_first_push_serves_once() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let (payload, rxs) = group(&mut reg, &["a", "b", "c"], blmpop, true);
        let wait_id = payload.wait_id;
        register_group(&mut reg, &mut db, payload);
        assert!(replies(&rxs).is_empty());
        assert!(reg.is_waiting(wait_id));
        for k in ["a", "b", "c"] {
            assert!(reg.has_waiters(0, &b(k)), "{k} must be registered");
        }

        db.list_push_back(&b("c"), b("C1"));
        assert!(crate::blocking::wakeup::try_wake_list_waiter(
            &mut reg,
            &mut db,
            0,
            &b("c")
        ));
        db.list_push_back(&b("b"), b("B1"));
        assert!(!crate::blocking::wakeup::try_wake_list_waiter(
            &mut reg,
            &mut db,
            0,
            &b("b")
        ));
        assert_eq!(replies(&rxs).len(), 1);
        assert_eq!(
            list(&mut db, "b"),
            vec![b("B1")],
            "b's push had nobody to serve"
        );
        assert!(!reg.is_waiting(wait_id));
    }

    /// A whole-command group answers Redis's `-WRONGTYPE` for the first
    /// existing key of the wrong type, pops nothing and registers nothing.
    #[test]
    fn whole_command_wrong_type_before_data_is_an_error_and_pops_nothing() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        db.set_string(b"s", b("x"));
        db.list_push_back(&b("b"), b("B1"));
        let (payload, rxs) = group(&mut reg, &["a", "s", "b"], blmpop, true);
        let wait_id = payload.wait_id;
        register_group(&mut reg, &mut db, payload);
        let got = replies(&rxs);
        assert_eq!(got.len(), 1);
        assert!(
            matches!(&got[0], Frame::Error(e) if e.starts_with(b"WRONGTYPE")),
            "{got:?}"
        );
        assert_eq!(list(&mut db, "b"), vec![b("B1")]);
        assert!(!reg.is_waiting(wait_id));
    }

    /// Redis stops type-checking at the key that serves: a wrong-typed key
    /// AFTER it is never consulted.
    #[test]
    fn whole_command_wrong_type_after_data_does_not_block_the_pop() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        db.list_push_back(&b("b"), b("B1"));
        db.set_string(b"s", b("x"));
        let (payload, rxs) = group(&mut reg, &["b", "s"], blmpop, true);
        register_group(&mut reg, &mut db, payload);
        let got = replies(&rxs);
        assert_eq!(got.len(), 1);
        assert!(matches!(&got[0], Frame::Array(_)), "{got:?}");
        assert!(list(&mut db, "b").is_empty());
    }

    /// A PARTIAL group must not decide the command with an error — its
    /// siblings on other shards may be serving right now — and a wrong-typed
    /// key must not tear down the waiter's OTHER keys on this shard either:
    /// a later push to one of them still serves it.
    #[test]
    fn partial_group_skips_a_wrong_type_and_keeps_its_siblings() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        db.set_string(b"s", b("x"));
        let (payload, rxs) = group(&mut reg, &["s", "a"], blmpop, false);
        let wait_id = payload.wait_id;
        register_group(&mut reg, &mut db, payload);
        assert!(replies(&rxs).is_empty(), "no error and no nil");
        assert!(reg.is_waiting(wait_id), "still parked on its keys");
        assert!(reg.has_waiters(0, &b("a")), "the sibling survives");

        db.list_push_back(&b("a"), b("A1"));
        assert!(crate::blocking::wakeup::try_wake_list_waiter(
            &mut reg,
            &mut db,
            0,
            &b("a")
        ));
        assert_eq!(replies(&rxs).len(), 1);
    }

    /// A whole-command group whose first servable key is taken by a waiter
    /// queued AHEAD of this one: the wrong-typed key after it is skipped, not
    /// used to tear this waiter down with a spurious nil.
    #[test]
    fn whole_group_never_offers_a_wrong_typed_key_to_the_wakers() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let (ahead_tx, ahead_rx) = channel::oneshot();
        let ahead_id = reg.next_wait_id();
        reg.register(
            0,
            b("b"),
            WaitEntry {
                wait_id: ahead_id,
                cmd: blmpop(),
                reply_tx: ahead_tx,
                deadline: None,
            },
        );
        db.list_push_back(&b("b"), b("B1"));
        db.set_string(b"s", b("x"));
        let (payload, rxs) = group(&mut reg, &["b", "s"], blmpop, true);
        let wait_id = payload.wait_id;
        register_group(&mut reg, &mut db, payload);
        assert!(
            ahead_rx.try_recv().ok().flatten().is_some(),
            "FIFO: the waiter ahead is served first"
        );
        assert!(replies(&rxs).is_empty(), "ours got neither B1 nor a nil");
        assert!(reg.is_waiting(wait_id), "and is still parked");
    }

    /// A2 carried over: a waiter whose client is already gone must not
    /// consume anything.
    #[test]
    fn a_dead_waiter_consumes_nothing() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        db.list_push_back(&b("b"), b("B1"));
        let (payload, rxs) = group(&mut reg, &["a", "b"], blmpop, true);
        let wait_id = payload.wait_id;
        drop(rxs);
        register_group(&mut reg, &mut db, payload);
        assert_eq!(list(&mut db, "b"), vec![b("B1")]);
        assert!(!reg.is_waiting(wait_id), "the dead waiter is reaped");
    }

    /// `BZMPOP` over co-located sorted sets: one pop, from the first.
    #[test]
    fn zset_group_serves_once() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        db.zset_restore(&b("b"), b("B1"), 1.0);
        db.zset_restore(&b("c"), b("C1"), 1.0);
        let (payload, rxs) = group(
            &mut reg,
            &["a", "b", "c"],
            || BlockedCommand::BZMPop {
                min: true,
                count: 1,
            },
            true,
        );
        register_group(&mut reg, &mut db, payload);
        assert_eq!(replies(&rxs).len(), 1);
        assert!(!db.exists(b"b"), "b served its only member");
        assert!(db.exists(b"c"), "c must be untouched");
    }
}
