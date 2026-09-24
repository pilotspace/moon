//! In-place string mutation for SETBIT / SETRANGE / BITFIELD / APPEND
//! (moon#1168).
//!
//! # Why this exists
//!
//! The four string mutators read the whole value into a fresh `Vec`,
//! mutated it, built a new `Entry` and `set` it back: every SETBIT on a
//! 12.5 MB bitmap copied 12.5 MB (8.3 ms, 211x redis 7.0.15), and APPEND
//! copied the existing value twice per call — quadratic, 11.2 s to build a
//! 4 MB string from 40K x 100 B. Redis mutates the sds in place: SETBIT and
//! in-bounds SETRANGE are O(1), APPEND amortized O(1).
//!
//! [`Database::mutate_string`] is the in-place equivalent: ONE `get_mut`
//! probe, the closure edits the stored bytes through [`StringBuf`] (growing
//! them by an exact `realloc` — see `CompactValue::string_grow_with` for why
//! that is amortized O(1)), and the bookkeeping `Database::set` did is
//! reproduced for what actually changed.
//!
//! # The write's effects, enumerated
//!
//! Modelled on `db/incr.rs` (moon#942), whose table this follows. The old
//! path (`db.get` + rebuilt `Entry` + `db.set`) had these effects; the in-place
//! path, on a hot live key, when the closure WROTE (`StringBuf::touched`):
//!
//! | # | old path | in place |
//! |---|---|---|
//! | 1 | `record_keyspace_change()` | reproduced |
//! | 2 | `spill_inflight_forget(key)` when the plane is non-empty (#459) | reproduced |
//! | 3 | `used_memory` from `entry_overhead` old/new | reproduced as the value delta (`key.len() + 128` cancels) |
//! | 4 | WATCH version bump on `set`'s `Updated` arm (moon#926) | reproduced via `stamp_mutation` |
//! | 5 | cold-shadow removal on the `Updated` arm (task #56) | reproduced |
//! | 6 | `maybe_has_expiring_keys` when the entry has a TTL | reproduced |
//! | 7 | TTL re-supplied by the rebuilt entry, expiry index untouched | the TTL is simply never touched |
//! | 8 | `last_access = now` (rebuilt entry) | reproduced |
//! | 9 | LFU counter RESET to `LFU_INIT_VAL` (a side effect of rebuilding) | **preserved** — the key's access history survives a write, as the entry's other metadata does (moon#1168 asks for exactly this; redis's `lookupKeyWrite` bumps, never resets, the counter) |
//!
//! A closure that did NOT write (BITFIELD whose every INCRBY hit `OVERFLOW
//! FAIL` on a string already long enough) leaves the entry bit-for-bit
//! unchanged — no version bump, which is redis's "no dirty" too.
//!
//! The keyspace notification and the AOF/replication record stay with the
//! command layer, driven by the reply exactly as before.
//!
//! # The non-hot shapes
//!
//! * TTL-expired but still present: hidden as `Database::get` hides it
//!   (`note_lazy_expired`), then treated as absent — the old path's
//!   `get -> None` then `set` of a fresh value.
//! * Absent from hot RAM: [`Database::promote_cold_known_absent`] answers
//!   the in-flight spill plane and the cold tier first (a spilled value is
//!   NOT an empty string); a promoted key is mutated in place, and an
//!   indexed-but-unreadable one is [`StringMut::ColdFault`] (moon#875) —
//!   never a value built from nothing.
//! * Truly absent: the closure runs against an empty string, and the result
//!   is stored through `Database::set` (which owns every CREATE effect) only
//!   if the closure wrote.

use crate::storage::compact_value::CompactValue;
use crate::storage::db::{Database, stamp_mutation};
use crate::storage::entry::{Entry, LFU_INIT_VAL};

/// What [`Database::mutate_string`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StringMut<R> {
    /// The closure ran against the stored string (or a new empty one, stored
    /// iff the closure wrote) and returned this.
    Applied(R),
    /// The key holds a non-string. **Nothing was written.**
    WrongType,
    /// The key is indexed in the cold tier but its bytes could not be read
    /// (moon#875). **Nothing was written.** Answer with
    /// `Database::cold_fault_error()` after `take_cold_fault()`.
    ColdFault,
}

/// A string value opened for in-place editing. Every mutator marks the value
/// written; reads do not.
pub(crate) struct StringBuf<'a> {
    value: &'a mut CompactValue,
    touched: bool,
}

impl StringBuf<'_> {
    /// Current length in bytes.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.value.as_bytes().map_or(0, <[u8]>::len)
    }

    /// The bytes, read-only.
    #[inline]
    pub(crate) fn bytes(&self) -> &[u8] {
        self.value.as_bytes().unwrap_or_default()
    }

    /// The bytes, writable at a fixed length. Marks the value written.
    #[inline]
    pub(crate) fn bytes_mut(&mut self) -> &mut [u8] {
        self.touched = true;
        self.value.string_bytes_mut().unwrap_or_default()
    }

    /// Grow to at least `len` bytes, zero-filled. Marks the value written
    /// only if it actually grew (redis's `sdsgrowzero` dirty rule).
    #[inline]
    pub(crate) fn grow_zeroed(&mut self, len: usize) {
        if len > self.len() {
            self.touched = true;
            self.value.string_grow_zeroed(len);
        }
    }

    /// Append `data`. Marks the value written even when `data` is empty —
    /// APPEND is a write on redis whatever it appends.
    #[inline]
    pub(crate) fn append(&mut self, data: &[u8]) {
        self.touched = true;
        self.value.string_append(data);
    }
}

impl Database {
    /// Edit the string at `key` in place (moon#1168). See the module docs
    /// for the full contract; in short: one probe on a hot key, the stored
    /// bytes are handed to `f` through a [`StringBuf`], the expiry and LFU
    /// metadata are kept, and every bookkeeping effect `Database::set` had is
    /// reproduced iff `f` wrote.
    pub(crate) fn mutate_string<R>(
        &mut self,
        key: &[u8],
        f: impl FnOnce(&mut StringBuf<'_>) -> R,
    ) -> StringMut<R> {
        let now_ms = self.cached_now_ms;
        let now_secs = self.cached_now;

        let mut expired = false;
        if let Some(entry) = self.data.get_mut(key) {
            if entry.is_expired_at(now_ms) {
                expired = true;
            } else {
                if entry.value.as_bytes().is_none() {
                    return StringMut::WrongType;
                }
                let old_cost = entry.value.estimate_memory();
                let mut buf = StringBuf {
                    value: &mut entry.value,
                    touched: false,
                };
                let out = f(&mut buf);
                if !buf.touched {
                    return StringMut::Applied(out);
                }
                // ── The write is committed. ─────────────────────────────
                let new_cost = entry.value.estimate_memory();
                entry.set_last_access(now_secs); // (8)
                stamp_mutation(entry); // (4) — after `f`, never on a no-op
                let has_expiry = entry.has_expiry();
                // `entry`'s borrow ends here; the rest needs `&mut self`.
                self.after_in_place_string_write(key, has_expiry, old_cost, new_cost);
                return StringMut::Applied(out);
            }
        }

        if expired {
            // Physically present, logically gone: hide it exactly as
            // `Database::get` does, then write a fresh value over it.
            self.note_lazy_expired(key);
        } else {
            // Absent from hot RAM is not absent (#459 / moon#875).
            if self.promote_cold_known_absent(key, now_ms) {
                // Hot again (never expired: promotion refuses a dead entry).
                return self.mutate_string_hot_only(key, f);
            }
            if self.cold_fault_pending() {
                return StringMut::ColdFault;
            }
        }

        // Create: run `f` against an empty string; store it iff it wrote.
        // `Database::set` owns every CREATE effect (birth version, ledger
        // charge, expiry index, keyspace change, in-flight retirement), and
        // the fresh entry carries no TTL and `LFU_INIT_VAL`, like the old
        // path's `Entry::new_string`.
        let mut value = CompactValue::inline_string(b"");
        let mut buf = StringBuf {
            value: &mut value,
            touched: false,
        };
        let out = f(&mut buf);
        if buf.touched {
            let mut entry = Entry::new_string_from_slice(b"");
            entry.value = value;
            entry.set_last_access(now_secs);
            entry.set_access_counter(LFU_INIT_VAL);
            self.set(key, entry);
        }
        StringMut::Applied(out)
    }

    /// The hot-key half of [`Self::mutate_string`], for the key a cold
    /// promotion just made hot. Falls back to the full path if the key is not
    /// hot after all (it then cannot recurse again: the promotion is spent).
    fn mutate_string_hot_only<R>(
        &mut self,
        key: &[u8],
        f: impl FnOnce(&mut StringBuf<'_>) -> R,
    ) -> StringMut<R> {
        let now_ms = self.cached_now_ms;
        let live = self.data.get(key).is_some_and(|e| !e.is_expired_at(now_ms));
        if live {
            self.mutate_string(key, f)
        } else {
            let mut value = CompactValue::inline_string(b"");
            let mut buf = StringBuf {
                value: &mut value,
                touched: false,
            };
            let out = f(&mut buf);
            if buf.touched {
                let mut entry = Entry::new_string_from_slice(b"");
                entry.value = value;
                entry.set_last_access(self.cached_now);
                entry.set_access_counter(LFU_INIT_VAL);
                self.set(key, entry);
            }
            StringMut::Applied(out)
        }
    }

    /// Effects (1), (2), (3), (5), (6) of the module table, for a hot entry
    /// already mutated in place.
    fn after_in_place_string_write(
        &mut self,
        key: &[u8],
        has_expiry: bool,
        old_cost: usize,
        new_cost: usize,
    ) {
        // (1) The rdb dirty counter: `set` charged one per write.
        crate::admin::metrics_setup::record_keyspace_change();
        // (2) #459: a write makes a queued spill payload stale.
        if !self.spill_inflight_is_empty() {
            self.spill_inflight_forget(key);
        }
        // (5) Task #56: a write to a hot, cold-shadowed key proves the
        // shadow stale.
        if let Some(ci) = self.cold_index.as_mut() {
            ci.remove(key);
        }
        // (6) Parity with `set`.
        if has_expiry {
            self.maybe_has_expiring_keys = true;
        }
        // (3) The ledger.
        self.adjust_memory(old_cost, new_cost);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::Entry;
    use bytes::Bytes;

    fn value(db: &mut Database, key: &[u8]) -> Option<Vec<u8>> {
        db.get(key)
            .and_then(|e| e.value.as_bytes().map(<[u8]>::to_vec))
    }

    /// The reference: what the old rebuild path would have left behind.
    #[test]
    fn in_place_write_keeps_ttl_lfu_and_bumps_version_and_ledger() {
        let mut db = Database::new();
        let far = crate::storage::entry::current_time_ms() + 3_600_000;
        let mut e = Entry::new_string_with_expiry(Bytes::from(vec![7u8; 100]), far);
        e.set_access_counter(200);
        db.set(b"k", e);
        let v0 = db.get_version(b"k");
        let mem0 = db.estimated_memory();

        let out = db.mutate_string(b"k", |b| {
            b.bytes_mut()[3] = 9;
            b.append(&[1u8; 400]);
            b.len()
        });
        assert_eq!(out, StringMut::Applied(500));
        let e = db.get(b"k").unwrap();
        assert_eq!(e.expires_at_ms(), far, "TTL must survive an in-place write");
        assert_eq!(e.access_counter(), 200, "LFU must not be reset");
        let v = e.value.as_bytes().unwrap().to_vec();
        assert_eq!(v.len(), 500);
        assert_eq!(v[3], 9);
        assert!(db.get_version(b"k") != v0, "WATCH version must move");
        // The ledger moved by exactly the value's size-class delta.
        let want = mem0 - crate::storage::mem_size::size_class(100)
            + crate::storage::mem_size::size_class(500);
        assert_eq!(db.estimated_memory(), want);
    }

    #[test]
    fn untouched_closure_changes_nothing() {
        let mut db = Database::new();
        db.set(
            b"k",
            Entry::new_string(Bytes::from_static(b"hello world, long value")),
        );
        let v0 = db.get_version(b"k");
        let mem0 = db.estimated_memory();
        let out = db.mutate_string(b"k", |b| b.bytes().len());
        assert_eq!(out, StringMut::Applied(23));
        assert_eq!(db.get_version(b"k"), v0, "a read-only closure bumped WATCH");
        assert_eq!(db.estimated_memory(), mem0);
        // Absent key + untouched closure: no key is created.
        assert_eq!(
            db.mutate_string(b"nokey", |b| b.len()),
            StringMut::Applied(0)
        );
        assert!(value(&mut db, b"nokey").is_none());
    }

    #[test]
    fn wrongtype_absent_and_expired_shapes() {
        let mut db = Database::new();
        crate::command::set::sadd(
            &mut db,
            &[
                crate::protocol::Frame::BulkString(Bytes::from_static(b"s")),
                crate::protocol::Frame::BulkString(Bytes::from_static(b"m")),
            ],
        );
        assert_eq!(
            db.mutate_string(b"s", |b| b.append(b"x")),
            StringMut::WrongType
        );

        // Absent: created with no TTL.
        assert_eq!(
            db.mutate_string(b"new", |b| b.append(b"abc")),
            StringMut::Applied(())
        );
        assert_eq!(value(&mut db, b"new").as_deref(), Some(&b"abc"[..]));
        assert_eq!(db.get(b"new").unwrap().expires_at_ms(), 0);

        // Expired: written over as if absent, TTL gone.
        let past = crate::storage::entry::current_time_ms().saturating_sub(10_000);
        db.set(
            b"old",
            Entry::new_string_with_expiry(Bytes::from_static(b"stale-value"), past),
        );
        assert_eq!(
            db.mutate_string(b"old", |b| b.append(b"x")),
            StringMut::Applied(())
        );
        assert_eq!(value(&mut db, b"old").as_deref(), Some(&b"x"[..]));
        assert_eq!(db.get(b"old").unwrap().expires_at_ms(), 0);
    }

    /// Inline <-> heap transitions keep the bytes and the ledger exact.
    #[test]
    fn growth_across_the_inline_boundary() {
        let mut db = Database::new();
        db.set(b"k", Entry::new_string(Bytes::from_static(b"abc")));
        let mem0 = db.estimated_memory();
        db.mutate_string(b"k", |b| b.grow_zeroed(40));
        let v = value(&mut db, b"k").unwrap();
        assert_eq!(&v[..3], b"abc");
        assert!(v[3..].iter().all(|&x| x == 0) && v.len() == 40);
        assert_eq!(
            db.estimated_memory(),
            mem0 - 3 + crate::storage::mem_size::size_class(40)
        );
    }
}
