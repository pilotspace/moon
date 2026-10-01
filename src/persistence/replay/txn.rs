//! Replaying `MOON.TXN` blocks (moon#1300): a cross-store transaction that
//! never ended is rolled back on replay, as a crash rolls it back live.
//!
//! # Why a block, and why not "skip its records"
//!
//! A `TXN` applies its KV writes to the keyspace as they run, and the write
//! path logs each one at that moment — a `TXN` body is not held back the way
//! a `MULTI` body is. A kill -9 before `TXN.COMMIT` / `TXN.ABORT` left those
//! records in the log with nothing to say they were uncommitted, and replay
//! brought them back. The writer now brackets them ([`super::pseudo`]):
//!
//! - `MOON.TXN BEGIN <id>` — the data records up to the next `MOON.TXN`
//!   record belong to transaction `<id>` (opened the first time it is named);
//! - `MOON.TXN PAUSE <id>` — the records after it belong to no transaction
//!   (another client's writes interleave freely with a transaction's: the AOF
//!   is one stream per shard);
//! - `MOON.TXN END <id>` — `<id>` is over: committed, or rolled back with its
//!   compensating records logged inside the block before this marker;
//! - `MOON.TXN RESET` — every transaction still open is dead: the writer
//!   that reopened a file whose replay ended inside a block writes it first,
//!   so a crashed transaction is rolled back where the crash happened, not
//!   at the end of a later session's records.
//!
//! A clean-close marker (`MOON.TS <ms> CLOSE`) also ends every open block:
//! the process that ran them stopped.
//!
//! redis drops an unterminated `MULTI` at the end of its AOF; it can, because
//! a `MULTI` body is logged as one contiguous block at `EXEC`. A `TXN` block
//! is interleaved with other clients' acknowledged writes, so "skip from
//! BEGIN to the end of the file" would lose those. Instead the replay does
//! what the live server does on a crash-equivalent abort: it applies the
//! block's records in place, capturing the pre-image of every key one of
//! them writes (first capture wins — the key's pre-transaction value), and
//! when the block turns out to be unterminated it restores those
//! pre-images. Under moon#1299 no other client can write a key an open
//! transaction holds, so restoring the pre-images is exactly "the block's
//! records never ran", while every record around them keeps its position —
//! a transaction's write that READS another key (`SUNIONSTORE`) replays
//! against the value it read live.
//!
//! Robustness: a record OUTSIDE a transaction's block that writes a key the
//! transaction captured proves the transaction no longer held it — its end
//! was lost (a fold dropped the `END`, the writer refused it). The key leaves
//! the transaction's undo, so a later rollback can never overwrite that
//! acknowledged write.
//!
//! Clock records (`MOON.TS`) are observations, not data (decision Q6): they
//! never enter a block's undo and apply wherever they sit.
//!
//! # Cost
//!
//! Nothing while no block is open: one `is_empty` check per data record.
//! Inside a block, the key walk of each record and one deep clone per key
//! the block writes for the first time — what the live undo capture paid.

use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use smallvec::SmallVec;

use super::pseudo::TxnMarker;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::entry::Entry;

/// One open transaction: the pre-image of every `(db, key)` its records
/// wrote (`None`: the key was absent), first capture wins.
struct OpenTxn {
    id: u64,
    undo: HashMap<(usize, Bytes), Option<Entry>>,
}

/// The `MOON.TXN` state of one replayed log file (see the module doc).
#[derive(Default)]
pub struct TxnReplay {
    /// The transaction the next data record belongs to (0: none).
    ctx: u64,
    /// Transactions named by a `BEGIN` and not yet ended.
    open: SmallVec<[OpenTxn; 2]>,
}

impl TxnReplay {
    /// No block is open and no record is attributed to one.
    #[inline]
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.ctx == 0 && self.open.is_empty()
    }

    /// The transaction the next data record belongs to (0: none).
    #[must_use]
    pub fn context(&self) -> u64 {
        self.ctx
    }

    /// Open transactions, in the order they were begun.
    #[must_use]
    pub fn open_ids(&self) -> SmallVec<[u64; 2]> {
        self.open.iter().map(|t| t.id).collect()
    }

    /// Apply one `MOON.TXN` record. `databases` is the slice the records
    /// replay into (`[Database]` on boot, `[&mut Database]` on a replica).
    pub fn on_marker<D: BorrowMut<Database>>(&mut self, databases: &mut [D], marker: TxnMarker) {
        match marker {
            TxnMarker::Begin(id) => {
                if !self.open.iter().any(|t| t.id == id) {
                    self.open.push(OpenTxn {
                        id,
                        undo: HashMap::new(),
                    });
                }
                self.ctx = id;
            }
            TxnMarker::Pause(id) => {
                if self.ctx == id {
                    self.ctx = 0;
                }
            }
            TxnMarker::End(id) => {
                self.open.retain(|t| t.id != id);
                if self.ctx == id {
                    self.ctx = 0;
                }
            }
            TxnMarker::Reset => {
                self.roll_back_all(databases);
            }
        }
    }

    /// Before a DATA record (`cmd args`, replayed in `selected_db`) is
    /// applied: inside a block, capture the pre-image of every key it may
    /// write; outside one, release those keys from every open block (see the
    /// module doc). Nothing to do while no block is open.
    pub fn before_data<D: BorrowMut<Database>>(
        &mut self,
        databases: &mut [D],
        selected_db: usize,
        cmd: &[u8],
        args: &[Frame],
    ) {
        if self.open.is_empty() {
            return;
        }
        if databases.is_empty() {
            return;
        }
        // The replay engine resets an out-of-range db to 0 before applying.
        let db = if selected_db < databases.len() {
            selected_db
        } else {
            0
        };
        if let Some(scope) = whole_db_scope(cmd, args, db, databases.len()) {
            // A whole-database write: no key can be restored over it. Live, a
            // transaction refuses these, and another client's is refused
            // while a transaction holds a key there (moon#1299), so a block
            // that meets one is stale: forget its keys in those databases.
            for t in &mut self.open {
                t.undo.retain(|(d, _), _| !scope.contains(d));
            }
            return;
        }
        let keys = written_keys(cmd, args, db, databases.len());
        let ctx = self.ctx;
        for (d, key) in keys {
            for t in &mut self.open {
                if t.id == ctx {
                    t.undo
                        .entry((d, key.clone()))
                        .or_insert_with(|| databases[d].borrow_mut().get(&key).cloned());
                } else {
                    t.undo.remove(&(d, key.clone()));
                }
            }
        }
    }

    /// The end of the replayed file: every block still open was cut by the
    /// crash and is rolled back. Returns how many were (and whether the file
    /// ended inside a block's records), for the writer that reopens the file
    /// ([`note_reset_owed`]).
    pub fn finish<D: BorrowMut<Database>>(&mut self, databases: &mut [D]) -> usize {
        let open = self.open.len() + usize::from(self.ctx != 0 && self.open.is_empty());
        self.roll_back_all(databases);
        open
    }

    fn roll_back_all<D: BorrowMut<Database>>(&mut self, databases: &mut [D]) {
        self.ctx = 0;
        for t in self.open.drain(..) {
            if !t.undo.is_empty() {
                tracing::info!(
                    txn_id = t.id,
                    keys = t.undo.len(),
                    "transaction {} never ended (its process stopped) -- rolling back its {} key(s)",
                    t.id,
                    t.undo.len()
                );
            }
            for ((d, key), pre) in t.undo {
                if let Some(db) = databases.get_mut(d) {
                    crate::transaction::kv_compensation::restore_pre_image(
                        db.borrow_mut(),
                        &key,
                        pre,
                    );
                }
            }
        }
    }
}

/// The databases a whole-database write clears or moves (`None`: not one).
fn whole_db_scope(
    cmd: &[u8],
    args: &[Frame],
    db: usize,
    db_count: usize,
) -> Option<SmallVec<[usize; 2]>> {
    if cmd.eq_ignore_ascii_case(b"FLUSHALL") {
        return Some((0..db_count).collect());
    }
    if cmd.eq_ignore_ascii_case(b"FLUSHDB") {
        return Some(SmallVec::from_slice(&[db]));
    }
    if cmd.eq_ignore_ascii_case(b"SWAPDB") {
        let idx = |f: Option<&Frame>| match f? {
            Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<usize>().ok(),
            Frame::Integer(n) => usize::try_from(*n).ok(),
            _ => None,
        };
        let mut scope = SmallVec::new();
        scope.extend(idx(args.first()));
        scope.extend(idx(args.get(1)));
        return Some(scope);
    }
    None
}

/// Every `(db, key)` `cmd args` may write, replayed in `db`: the shared key
/// walker's write positions (what the live undo capture records), plus the
/// destination of `MOVE` / `COPY … DB n`.
fn written_keys(
    cmd: &[u8],
    args: &[Frame],
    db: usize,
    db_count: usize,
) -> SmallVec<[(usize, Bytes); 4]> {
    let mut out: SmallVec<[(usize, Bytes); 4]> = SmallVec::new();
    if cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK") {
        out.extend(args.iter().filter_map(|a| match a {
            Frame::BulkString(k) => Some((db, k.clone())),
            _ => None,
        }));
        return out;
    }
    use crate::command::keyspace::move_cmd as ksmv;
    if cmd.eq_ignore_ascii_case(b"MOVE") {
        if let Ok((key, dst)) = ksmv::parse_move_args(args, db_count) {
            out.push((db, key.clone()));
            if dst < db_count {
                out.push((dst, key));
            }
        }
        return out;
    }
    if cmd.eq_ignore_ascii_case(b"COPY")
        && let Some(Ok(ca)) = ksmv::parse_copy_db_args(args, db, db_count)
    {
        if ca.dst_db < db_count {
            out.push((ca.dst_db, ca.dst_key));
        }
        return out;
    }
    out.extend(
        crate::transaction::conn_txn_capture_keys(cmd, args)
            .into_iter()
            .map(|k| (db, k)),
    );
    out
}

/// Files whose replay ended inside a `MOON.TXN` block: the writer that
/// reopens one writes `MOON.TXN RESET` before its first append
/// ([`crate::persistence::aof::record_ctx`]). Touched once per replayed file
/// that needed it and once per writer session.
static RESET_OWED: parking_lot::Mutex<Vec<PathBuf>> = parking_lot::const_mutex(Vec::new());

fn reset_key(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// The replay of `path` ended inside a block (see [`TxnReplay::finish`]).
pub fn note_reset_owed(path: &Path) {
    let key = reset_key(path);
    let mut owed = RESET_OWED.lock();
    if !owed.contains(&key) {
        owed.push(key);
    }
}

/// Whether the writer appending to `path` owes a `MOON.TXN RESET` before its
/// first record. Taken once.
pub fn take_reset_owed(path: &Path) -> bool {
    let key = reset_key(path);
    let mut owed = RESET_OWED.lock();
    match owed.iter().position(|p| *p == key) {
        Some(at) => {
            owed.swap_remove(at);
            true
        }
        None => false,
    }
}

#[cfg(test)]
#[path = "txn_tests.rs"]
mod tests;
