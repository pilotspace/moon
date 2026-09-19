//! How a multi-key blocking waiter registers on its keys (moon#989).
//!
//! Shared by both runtimes' `handle_blocking_command*` so the two cannot
//! drift: the registration protocol IS the exactly-once argument, and a
//! second copy of it is a second place for that argument to break.
//!
//! * keys THIS shard owns are registered directly, under one `wait_id`;
//! * keys other shards own travel as ONE `BlockRegisterGroup` per owner,
//!   carrying every key that owner holds, in argument order.
//!
//! The per-owner grouping is the fix. The coordinator used to send one
//! `BlockRegister` per key, and the owner served each on arrival — so a waiter
//! whose co-located keys both held data was served once per key, and every
//! reply after the first was dropped with its element already gone. A group is
//! handled in one synchronous stretch of the owner's loop
//! (`blocking::group::register_group`), where serving the waiter unregisters
//! its siblings before the next key is looked at.
//!
//! What grouping does NOT fix: keys owned by two DIFFERENT remote shards can
//! still each serve the same waiter, because nothing orders two shards'
//! wakes. `BLMPOP`/`BZMPOP` refuse that placement up front
//! (`immediate_scan`, CROSSSLOT); the rest of the family is moon#1019.

use bytes::Bytes;
use futures::stream::FuturesUnordered;

use crate::blocking::{BlockedCommand, BlockingRegistry, WaitEntry};
use crate::protocol::Frame;
use crate::runtime::channel;
use crate::shard::dispatch::{
    BlockRegisterGroupPayload, BlockRegisterMember, ShardMessage, key_to_shard,
};

/// Everything a multi-key waiter holds once its registrations are staged.
pub(super) struct StagedWait {
    pub wait_id: u64,
    /// One receiver per key, local and remote. The first `Some(frame)` wins.
    pub receivers: FuturesUnordered<channel::OneshotReceiver<Option<Frame>>>,
    /// One `BlockRegisterGroup` per remote owner, not yet pushed — the caller
    /// pushes them after releasing its registry borrow (A4/A5).
    pub pending_remote: Vec<(usize, ShardMessage)>,
    /// The owners in `pending_remote`, for the `BlockCancel` fan-out.
    pub remote_shards: Vec<usize>,
}

/// Register `keys` for one waiter: local keys now, remote keys staged as one
/// group per owner shard.
///
/// `local_cmd` builds the `BlockedCommand` for a LOCAL key (it binds a stream
/// `$` against this shard's view); `remote_cmd` builds one for a remote key,
/// whose owner binds it on arrival.
pub(super) fn stage_multikey_wait(
    registry: &mut BlockingRegistry,
    keys: &[Bytes],
    local_cmd: &dyn Fn(&Bytes) -> BlockedCommand,
    remote_cmd: &dyn Fn() -> BlockedCommand,
    selected_db: usize,
    shard_id: usize,
    num_shards: usize,
    deadline: Option<std::time::Instant>,
) -> StagedWait {
    let wait_id = registry.next_wait_id();
    let receivers = FuturesUnordered::new();
    // (owner, members) in the order each owner first appears in argv.
    let mut groups: Vec<(usize, Vec<BlockRegisterMember>)> = Vec::new();
    let mut any_local = false;

    for key in keys {
        let target = key_to_shard(key, num_shards);
        let (tx, rx) = channel::oneshot::<Option<Frame>>();
        receivers.push(rx);
        if target == shard_id {
            any_local = true;
            registry.register(
                selected_db,
                key.clone(),
                WaitEntry {
                    wait_id,
                    cmd: local_cmd(key),
                    reply_tx: tx,
                    deadline,
                },
            );
            continue;
        }
        let member = BlockRegisterMember {
            key: key.clone(),
            cmd: remote_cmd(),
            reply_tx: tx,
        };
        match groups.iter_mut().find(|(owner, _)| *owner == target) {
            Some((_, members)) => members.push(member),
            None => groups.push((target, vec![member])),
        }
    }

    // The owner may decide the whole command — including a `-WRONGTYPE` — only
    // when it holds every key: nothing is registered here and no other shard
    // was sent anything.
    let whole_command = !any_local && groups.len() == 1;
    let remote_shards: Vec<usize> = groups.iter().map(|(owner, _)| *owner).collect();
    let pending_remote = groups
        .into_iter()
        .map(|(owner, members)| {
            (
                owner,
                ShardMessage::BlockRegisterGroup(Box::new(BlockRegisterGroupPayload {
                    db_index: selected_db,
                    wait_id,
                    members,
                    whole_command,
                })),
            )
        })
        .collect();

    StagedWait {
        wait_id,
        receivers,
        pending_remote,
        remote_shards,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd() -> BlockedCommand {
        BlockedCommand::BLPop
    }

    fn group_of(msg: &ShardMessage) -> (&[BlockRegisterMember], bool) {
        match msg {
            ShardMessage::BlockRegisterGroup(p) => (&p.members, p.whole_command),
            _ => panic!("multi-key registrations travel as BlockRegisterGroup"),
        }
    }

    /// Co-located keys owned by another shard: ONE message, every key in
    /// argument order, flagged as the whole command.
    #[test]
    fn colocated_remote_keys_travel_as_one_whole_group() {
        const N: usize = 4;
        let keys: Vec<Bytes> = ["{t}a", "{t}b", "{t}c", "{t}b"]
            .iter()
            .map(|k| Bytes::from_static(k.as_bytes()))
            .collect();
        let owner = key_to_shard(&keys[0], N);
        let me = (owner + 1) % N;
        let mut reg = BlockingRegistry::new(me);
        let staged = stage_multikey_wait(&mut reg, &keys, &|_| cmd(), &cmd, 0, me, N, None);
        assert_eq!(staged.remote_shards, vec![owner]);
        assert_eq!(staged.pending_remote.len(), 1);
        let (members, whole) = group_of(&staged.pending_remote[0].1);
        assert!(whole);
        let names: Vec<&[u8]> = members.iter().map(|m| m.key.as_ref()).collect();
        assert_eq!(names, [&b"{t}a"[..], b"{t}b", b"{t}c", b"{t}b"]);
        assert_eq!(staged.receivers.len(), 4);
        assert!(
            !reg.is_waiting(staged.wait_id),
            "nothing registered locally"
        );
    }

    /// Co-located keys owned by THIS shard never leave it.
    #[test]
    fn local_keys_register_locally_and_send_nothing() {
        const N: usize = 4;
        let keys: Vec<Bytes> = ["{t}a", "{t}b"]
            .iter()
            .map(|k| Bytes::from_static(k.as_bytes()))
            .collect();
        let me = key_to_shard(&keys[0], N);
        let mut reg = BlockingRegistry::new(me);
        let staged = stage_multikey_wait(&mut reg, &keys, &|_| cmd(), &cmd, 0, me, N, None);
        assert!(staged.pending_remote.is_empty());
        assert!(staged.remote_shards.is_empty());
        assert!(reg.is_waiting(staged.wait_id));
    }

    /// Keys on several shards: one PARTIAL group per remote owner, so no owner
    /// can mistake its share for the whole command.
    #[test]
    fn spanning_keys_make_one_partial_group_per_owner() {
        const N: usize = 4;
        let me = 0usize;
        let pick = |owner: usize, tag: &str| -> Bytes {
            (0..10_000)
                .map(|i| Bytes::from(format!("{tag}{i}")))
                .find(|k| key_to_shard(k, N) == owner)
                .expect("a key for every shard")
        };
        let keys = vec![pick(1, "x"), pick(2, "y"), pick(1, "z"), pick(me, "w")];
        let mut reg = BlockingRegistry::new(me);
        let staged = stage_multikey_wait(&mut reg, &keys, &|_| cmd(), &cmd, 0, me, N, None);
        assert_eq!(staged.remote_shards, vec![1, 2]);
        let (m1, whole1) = group_of(&staged.pending_remote[0].1);
        let (m2, whole2) = group_of(&staged.pending_remote[1].1);
        assert!(!whole1 && !whole2);
        assert_eq!(m1.len(), 2, "both of shard 1's keys in ONE message");
        assert_eq!(m1[0].key, keys[0]);
        assert_eq!(m1[1].key, keys[2]);
        assert_eq!(m2.len(), 1);
        assert!(
            reg.is_waiting(staged.wait_id),
            "the local key is registered"
        );
    }
}
