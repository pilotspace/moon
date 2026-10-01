//! moon#1295: one large collection serialized as its OWN segment block, a
//! budget at a time, out of walk order.
//!
//! The epoch walk writes a key inside the segment block of its hash range.
//! When a writer is about to change a large collection the walk has not
//! reached, the epoch used to deep-clone it first (`snapshot_cow::capture_key`)
//! — 1.2 s of shard thread and +836 MB RSS for one `HSET` on a 5M-field hash.
//! Instead, the collection's epoch-start bytes are written NOW, into the
//! stream, as a block of its own:
//!
//! ```text
//! [DB_SELECTOR db]? 0xFD  u32::MAX  u32 1  <one entry>  u32 crc
//! ```
//!
//! — the same segment-block encoding every reader already parses (a block
//! index is informational, the loader only logs it), so the on-disk format
//! does not change. The entry is produced across ticks ([`KeyCursor`]) while
//! the key is write-locked (`snapshot_cow::stream`), and a TOMBSTONE in the
//! epoch's overflow map makes the walk write nothing for the key when it
//! reaches its range: the key is in the file exactly once, at its epoch-start
//! value. The walk is paused while a block is open ([`SnapshotState::walk_paused`]),
//! so the block's bytes are contiguous in the file.
//!
//! The database selector: the walk used to write each database's selector
//! once. A key block for another database writes that database's selector
//! before itself, and the walk re-selects its own afterwards
//! (`last_selector`). Readers have always taken any number of selectors.

use bytes::Bytes;
use crc32fast::Hasher;

use super::{DB_SELECTOR, SEGMENT_BLOCK_MARKER, SnapshotState};
use crate::persistence::rdb;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::Entry;
use crate::storage::value_codec::{put_f64, put_len_bytes, put_u32};

/// The segment index a key block carries. Informational only (the loader
/// logs it); `u32::MAX` tells a reader's eye it is no DashTable segment.
pub(crate) const KEY_BLOCK_INDEX: u32 = u32::MAX;

/// The collection kinds a [`KeyCursor`] streams: the large-collection
/// encodings with a stable iteration order while unchanged. Listpacks and
/// intsets are small by construction; a hash with field TTLs and a stream
/// carry sidecars, and are still copied (rare as multi-million-element
/// values).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Hash,
    List,
    Set,
    ZSet,
}

impl Kind {
    fn type_tag(self) -> u8 {
        match self {
            Kind::Hash => rdb::TYPE_HASH,
            Kind::List => rdb::TYPE_LIST,
            Kind::Set => rdb::TYPE_SET,
            Kind::ZSet => rdb::TYPE_SORTED_SET,
        }
    }
}

/// `(kind, element count, payload address)` of a streamable value. The
/// address identifies the collection's heap payload — it moves with neither
/// a DashTable split nor an entry move, only with a replacement.
fn shape(entry: &Entry) -> Option<(Kind, usize, usize)> {
    fn at<T>(r: &T) -> usize {
        std::ptr::from_ref(r) as usize
    }
    match entry.value.as_redis_value() {
        RedisValueRef::Hash(m) => Some((Kind::Hash, m.len(), at(m))),
        RedisValueRef::List(l) => Some((Kind::List, l.len(), at(l))),
        RedisValueRef::Set(s) => Some((Kind::Set, s.len(), at(s))),
        RedisValueRef::SortedSet { members, .. }
        | RedisValueRef::SortedSetBPTree { members, .. } => {
            Some((Kind::ZSet, members.len(), at(members)))
        }
        _ => None,
    }
}

/// The element count of `entry` when it can be streamed: a hash, list, set
/// or sorted set in its full-size encoding, with no key TTL (an expiry could
/// remove the key under the stream without any writer). `None` otherwise.
pub(crate) fn streamable_len(entry: &Entry) -> Option<usize> {
    if entry.has_expiry() {
        return None;
    }
    shape(entry)
        .map(|(_, n, _)| n)
        .filter(|n| *n <= u32::MAX as usize)
}

/// Resumable serializer of one entry, in exactly `rdb::write_entry`'s
/// encoding (element order aside, which no reader depends on).
///
/// Position `pos` counts elements written. Hash and sorted-set members are
/// `HashMap`s, which have no positional access: a resume re-walks the first
/// `pos` buckets (`skip`, which reads control bytes only — a few ms at 5M).
/// The order is stable because the value does not change while it streams —
/// what [`Self::matches`] re-checks before every chunk.
pub(crate) struct KeyCursor {
    kind: Kind,
    total: usize,
    pos: usize,
    payload: usize,
    /// CRC32 of the block's entry bytes written so far.
    crc: Hasher,
    done: bool,
}

impl KeyCursor {
    /// Start `key` = `entry`: append the entry's head (type tag, key, TTL,
    /// element count) to `buf`. `None` when the value is not streamable.
    pub(crate) fn begin(key: &[u8], entry: &Entry, buf: &mut Vec<u8>) -> Option<Self> {
        streamable_len(entry)?;
        let (kind, total, payload) = shape(entry)?;
        let start = buf.len();
        buf.push(kind.type_tag());
        put_len_bytes(buf, key);
        buf.extend_from_slice(&0i64.to_le_bytes());
        put_u32(buf, total as u32);
        let mut crc = Hasher::new();
        crc.update(&buf[start..]);
        Some(KeyCursor {
            kind,
            total,
            pos: 0,
            payload,
            crc,
            done: false,
        })
    }

    /// Is `entry` still the value this cursor streams (same payload, kind
    /// and length)? Every chunk is gated on it: a value that changed under
    /// the stream must fail the save, never write a block that does not
    /// parse.
    pub(crate) fn matches(&self, entry: &Entry) -> bool {
        !entry.has_expiry() && shape(entry) == Some((self.kind, self.total, self.payload))
    }

    /// True once every element (and the hash trailer) is written.
    #[inline]
    pub(crate) fn done(&self) -> bool {
        self.done
    }

    /// Append the next elements of `entry` to `buf`, until about `budget`
    /// bytes are written (at least one element) or the value is done.
    /// Returns [`Self::done`]. `entry` must [`Self::matches`].
    pub(crate) fn write_some(&mut self, entry: &Entry, buf: &mut Vec<u8>, budget: usize) -> bool {
        if self.done {
            return true;
        }
        let start = buf.len();
        let full = |buf: &Vec<u8>| buf.len() - start >= budget;
        let mut wrote = 0usize;
        match entry.value.as_redis_value() {
            RedisValueRef::Hash(m) => {
                for (f, v) in m.iter().skip(self.pos) {
                    if full(buf) {
                        break;
                    }
                    put_len_bytes(buf, f);
                    put_len_bytes(buf, v);
                    wrote += 1;
                }
            }
            RedisValueRef::List(l) => {
                for e in l.range(self.pos.min(l.len())..) {
                    if full(buf) {
                        break;
                    }
                    put_len_bytes(buf, e);
                    wrote += 1;
                }
            }
            RedisValueRef::Set(s) => {
                for m in s.iter().skip(self.pos) {
                    if full(buf) {
                        break;
                    }
                    put_len_bytes(buf, m);
                    wrote += 1;
                }
            }
            RedisValueRef::SortedSet { members, .. }
            | RedisValueRef::SortedSetBPTree { members, .. } => {
                for (m, score) in members.iter().skip(self.pos) {
                    if full(buf) {
                        break;
                    }
                    put_len_bytes(buf, m);
                    put_f64(buf, *score);
                    wrote += 1;
                }
            }
            _ => {}
        }
        self.pos += wrote;
        if self.pos >= self.total {
            if self.kind == Kind::Hash {
                // The v3 hash-TTL trailer: a plain hash carries none.
                put_u32(buf, 0);
            }
            self.done = true;
        }
        self.crc.update(&buf[start..]);
        self.done
    }

    /// Append everything not written yet (a writer that cannot wait is
    /// about to change or drop the value).
    pub(crate) fn write_rest(&mut self, entry: &Entry, buf: &mut Vec<u8>) {
        self.write_some(entry, buf, usize::MAX);
    }

    /// The block's CRC32: every entry byte goes through [`Self::begin`] or
    /// [`Self::write_some`], whichever buffer it was written into.
    fn finish_crc(self) -> u32 {
        self.crc.finalize()
    }
}

impl SnapshotState {
    /// Is the walk paused this tick? While a key block is open (its bytes
    /// must stay contiguous), or while a frozen table is rebuilt (review 6).
    pub(super) fn walk_paused(&self) -> bool {
        self.key_block_open || self.current_table_rebuilding()
    }

    /// Does the epoch already hold a pre-image (entry or tombstone) of `key`
    /// in database `db`?
    pub(crate) fn has_pre_image(&self, db: usize, key: &[u8]) -> bool {
        let hash = crate::storage::dashtable::hash_key(key);
        self.overflow
            .get(db)
            .is_some_and(|m| m.contains_key(&(hash, Bytes::copy_from_slice(key))))
    }

    /// Open a key block for `key` = `entry` in epoch database `db`: the walk
    /// writes nothing for the key from here on (a tombstone pre-image), and
    /// pauses until [`Self::close_key_block`]. `None` (nothing written) when
    /// the key is not pending any more, already has a pre-image, the value is
    /// not streamable, or a block is already open.
    pub(crate) fn open_key_block(
        &mut self,
        db: usize,
        key: &Bytes,
        entry: &Entry,
    ) -> Option<KeyCursor> {
        if self.key_block_open
            || self.aborted.is_some()
            || !self.is_key_pending(db, key)
            || self.has_pre_image(db, key)
            || streamable_len(entry).is_none()
        {
            return None;
        }
        self.write_header_if_needed();
        let before = self.output_buf.len();
        if self.last_selector != Some(db) {
            self.output_buf.push(DB_SELECTOR);
            self.output_buf.push(db as u8);
            self.last_selector = Some(db);
        }
        self.output_buf.push(SEGMENT_BLOCK_MARKER);
        self.output_buf
            .extend_from_slice(&KEY_BLOCK_INDEX.to_le_bytes());
        self.output_buf.extend_from_slice(&1u32.to_le_bytes());
        let cursor = KeyCursor::begin(key, entry, &mut self.output_buf)?;
        // The walk must not write the key again: its epoch-start bytes are
        // this block. First capture wins, and there is none yet.
        self.capture_cow(db, key.clone(), None);
        self.key_block_open = true;
        self.bytes_serialized += (self.output_buf.len() - before) as u64;
        self.ship_if_ready();
        Some(cursor)
    }

    /// Append bytes of the open key block: `fill` writes into the output.
    pub(crate) fn key_block_write(&mut self, fill: impl FnOnce(&mut Vec<u8>)) {
        let before = self.output_buf.len();
        fill(&mut self.output_buf);
        self.bytes_serialized += (self.output_buf.len() - before) as u64;
        self.ship_if_ready();
    }

    /// Close the open key block with its CRC; the walk resumes.
    pub(crate) fn close_key_block(&mut self, cursor: KeyCursor) {
        debug_assert!(cursor.done(), "a key block closes once its entry is whole");
        let crc = cursor.finish_crc();
        self.output_buf.extend_from_slice(&crc.to_le_bytes());
        self.bytes_serialized += 4;
        self.segments_written += 1;
        self.entries_written += 1;
        self.key_block_open = false;
        self.ship_if_ready();
    }

    /// Test-only: is a key block open?
    #[cfg(test)]
    pub(crate) fn key_block_open_for_test(&self) -> bool {
        self.key_block_open
    }
}
