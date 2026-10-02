//! A replica's view of its master's cross-store transactions (moon#1300).
//!
//! The master streams a `TXN`'s records as it runs them, each wrapped as
//! `[MOON.TXN BEGIN id][record][MOON.TXN PAUSE id]`, and `MOON.TXN END id`
//! once it committed or logged its rollback (`server::conn::txn_log`). The
//! replica applies them as they come — the same in-place, undo-capturing
//! replay as the AOF's ([`TxnReplay`]) — so while the master runs it holds
//! exactly the master's keyspace. When the replica stops following that
//! master (`REPLICAOF NO ONE`, a new target, `CLUSTER REPLICATE`: the
//! master may have died inside the transaction) every block still open is
//! rolled back, as the master's own crash recovery would. A full resync
//! replaces the dataset and forgets the blocks.
//!
//! The master's shards issue transaction ids independently and their records
//! share this one stream, so every marker carries the transaction's LOG id
//! (`aof::txn_log_id`: origin shard and the shard's id) — two shards' blocks
//! never share an id here (R2b W1). Ids are unique within one master
//! PROCESS: a restarted master issues them from 1 again. A full resync
//! forgets every block ([`discard`]); a partial resync across a master
//! restart (persisted replication id, offset recovered from its AOF) can
//! leave the old process's block open here — rolled back at a promotion,
//! or resumed by the new process's same id (known residual, see the R2b
//! fix-A summary).
//!
//! A replica has one shard and its apply runs on that shard's thread, so
//! the state is a thread-local, like the hold table. Nothing runs while no
//! block is open: one `RefCell` borrow per applied record.
//!
//! Master side ([`stream_reopen`]): a full sync while a transaction is open
//! sends each held key's PRE-transaction image in the sync image
//! (`persistence::redis_rdb`) and, right after the image's offset, the
//! records that re-open the transaction with the live values
//! (`transaction::reopen`) — so the replica can still roll it back.

use std::cell::RefCell;

use crate::persistence::replay::pseudo::TxnMarker;
use crate::persistence::replay::txn::TxnReplay;
use crate::protocol::Frame;

thread_local! {
    static BLOCKS: RefCell<TxnReplay> = RefCell::new(TxnReplay::default());
}

/// `cmd args` is a `MOON.TXN` record: apply it and return `true`. A
/// malformed one is skipped (and logged by the classifier's caller).
pub(crate) fn try_apply_marker(cmd: &[u8], args: &[Frame]) -> bool {
    use crate::persistence::replay::pseudo::{Pseudo, classify};
    match classify(cmd, args) {
        Some(Pseudo::Txn(marker)) => {
            on_marker(marker);
            true
        }
        Some(Pseudo::MalformedTxn) => {
            tracing::warn!(
                "replica apply: malformed MOON.TXN record ({} args) skipped",
                args.len()
            );
            true
        }
        _ => false,
    }
}

fn on_marker(marker: TxnMarker) {
    let _ = crate::shard::slice::try_with_shard(|s| {
        s.databases.with_all(|dbs| {
            BLOCKS.with(|b| b.borrow_mut().on_marker(dbs, marker));
        });
    });
}

/// Before a data record is applied (inside the apply's shard borrow): see
/// [`TxnReplay::before_data`].
pub(crate) fn before_data(
    s: &mut crate::shard::slice::ShardSlice,
    db: usize,
    cmd: &[u8],
    args: &[Frame],
) {
    BLOCKS.with(|b| {
        let mut blocks = b.borrow_mut();
        if blocks.is_idle() {
            return;
        }
        s.databases
            .with_all(|dbs| blocks.before_data(dbs, db, cmd, args));
    });
}

/// The replica stops following its master: roll back every transaction
/// block still open. Called where the role changes, on the shard thread,
/// before any new replica task runs.
pub(crate) fn roll_back_open() {
    let open = BLOCKS.with(|b| !b.borrow().is_idle());
    if !open {
        return;
    }
    let rolled = crate::shard::slice::try_with_shard(|s| {
        s.databases
            .with_all(|dbs| BLOCKS.with(|b| b.borrow_mut().finish(dbs)))
    });
    if let Some(n) = rolled
        && n > 0
    {
        tracing::warn!(
            "replica: {n} transaction(s) of the former master never ended -- rolled back"
        );
        // R2b round 2 R1: the AOF still holds their open blocks; a rewrite
        // replaces them with the rolled-back keyspace.
        crate::replication::replica_aof::after_promotion_rollback(n);
    }
}

/// A full resync replaced the dataset: forget every block.
pub(crate) fn discard() {
    BLOCKS.with(|b| *b.borrow_mut() = TxnReplay::default());
}

/// Master side of a full sync (see the module doc): push, at the current end
/// of `shard_id`'s replication stream, the records that re-open every open
/// transaction on this shard. Call on the shard's thread, in the same
/// synchronous stretch as the sync image's capture and offset read. Nothing
/// is held: one thread-local load.
pub(crate) fn stream_reopen(shard_id: usize) {
    if !crate::transaction::isolation::any_held() {
        return;
    }
    let records = crate::shard::slice::try_with_shard(|s| {
        s.databases
            .with_all_read(crate::transaction::reopen::reopen_records)
    })
    .unwrap_or_default();
    for (txn, db, record) in records {
        crate::replication::state::record_local_write_db_global(
            shard_id,
            db,
            crate::server::conn::txn_log::repl_record(shard_id, txn, &record),
        );
    }
}
