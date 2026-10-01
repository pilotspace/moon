//! An AOF fold while a cross-store transaction is open (moon#1300).
//!
//! The fold's new base holds every held key's PRE-transaction image
//! (`fold_stream::stream_fold_image`). At the same instant — the shard's
//! `AofFold` arm, after the fold epoch advanced — the shard enqueues, stamped
//! with the new epoch and the transaction, the records that set each held
//! key to its live value (`transaction::reopen`). They are the first records
//! of the transaction's block in the new generation: its later records, its
//! compensation and its END follow them. A crash before the END rolls the
//! block back to the base's images.

use super::AofWriterPool;

/// Enqueue the re-opening records of every open transaction on `shard_id`
/// (see the module doc). `false` when the writer refused one: the fold must
/// fail — a base of pre-transaction images without them would lose the
/// transaction's writes so far if it commits. One thread-local load when no
/// transaction holds a key.
pub(crate) fn enqueue_reopen(pool: &AofWriterPool, shard_id: usize) -> bool {
    if !crate::transaction::isolation::any_held() {
        return true;
    }
    let records = crate::shard::slice::with_shard(|s| {
        s.databases
            .with_all_read(crate::transaction::reopen::reopen_records)
    });
    for (txn, db, record) in records {
        let stamp = pool.fold_stamp(shard_id).in_txn(shard_id, txn);
        if !pool.try_send_append_stamped(shard_id, 0, db, record, stamp) {
            tracing::error!(
                shard_id,
                txn_id = txn,
                "AOF rewrite: the record re-opening an open transaction in the new generation \
                 was refused; failing the rewrite"
            );
            return false;
        }
    }
    true
}
