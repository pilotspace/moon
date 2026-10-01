//! Re-opening a shard's open transactions in a new log (moon#1300).
//!
//! A snapshot of a shard stores the PRE-transaction image of every key an
//! open transaction holds (`isolation::with_held_keys`). When that snapshot
//! is the base of a log the transaction keeps writing to — an AOF fold's new
//! generation, a replica's full-sync image followed by the live stream — the
//! transaction's writes so far are in neither the base nor the records after
//! it. So, at the same instant as the snapshot, the shard emits for each held
//! key the record that sets it to its LIVE value, tagged with the
//! transaction: inside the transaction's block in the new log. Then:
//!
//! - commit: the block (these records, the transaction's later ones) ends
//!   with its END and replays fully — the committed state;
//! - abort: its compensation and END follow — the pre-transaction state;
//! - a crash (or, on a replica, a promotion): the block is open and rolls
//!   back to the base's pre-transaction images.
//!
//! The records are the abort's own vocabulary: `RESTORE key <abs-ms>
//! <payload> REPLACE ABSTTL` plus one `HPEXPIREAT` per hash field deadline,
//! or `DEL key` for a key the transaction deleted (or created and deleted).

use bytes::Bytes;

use crate::storage::Database;
use crate::transaction::kv_compensation::{encode_command, push_restore_records};

/// `(txn_id, db, record)` for every key an open transaction holds on this
/// shard, in `dbs` (`dbs[i]` is shard slot `i`) — see the module doc. Empty
/// (one thread-local load) when nothing is held.
pub(crate) fn reopen_records(dbs: &[&Database]) -> Vec<(u64, usize, Bytes)> {
    let mut out = Vec::new();
    crate::transaction::isolation::with_held_keys(|held| {
        let mut recs = Vec::new();
        for h in held {
            let Some(db) = dbs.get(h.db) else {
                continue;
            };
            match db.data().get(h.key.as_ref()) {
                Some(entry) => push_restore_records(h.db, h.key, entry, &mut recs),
                None => recs.push((h.db, encode_command(&[b"DEL", h.key.as_ref()]))),
            }
            out.extend(recs.drain(..).map(|(d, r)| (h.txn, d, r)));
        }
    });
    out
}
