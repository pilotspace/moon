//! Active-expiry helpers for `HashWithTtl` entries (phase 197).
//!
//! This module extends `Database` with methods for reaping expired hash fields
//! during the active-expiry tick.  Read-path lazy filtering is handled by
//! `HashRef::WithTtl` in `db_read.rs` and the updated `get_hash_ref_if_alive`
//! in `db.rs`.

use bytes::Bytes;
use smallvec::SmallVec;

use super::compact_value::RedisValueRef;
use super::entry::RedisValue;
use crate::storage::Database;

/// Outcome of reaping expired fields from a single `HashWithTtl` entry.
#[derive(Debug, PartialEq, Eq)]
pub enum ReapOutcome {
    /// Some fields were removed; the hash still exists with live fields.
    FieldsRemoved,
    /// All TTLs were drained; hash downgraded to plain `Hash`.
    Downgraded,
    /// All fields expired; the hash key must be deleted by the caller.
    KeyDeleted,
    /// No expired fields found; nothing changed.
    NoOp,
}

impl Database {
    /// Reap expired fields from the `HashWithTtl` entry at `key`.
    ///
    /// Returns a [`ReapOutcome`] telling the caller what happened:
    ///
    /// - [`ReapOutcome::KeyDeleted`] — caller **must** call `db.remove(key)`
    ///   immediately; the entry is left in a logically-empty state.
    /// - [`ReapOutcome::Downgraded`] — the last TTL sidecar entry was removed
    ///   and the encoding was converted back to a plain `Hash`.
    /// - [`ReapOutcome::FieldsRemoved`] — at least one expired field was
    ///   removed, but live TTL'd fields remain.
    /// - [`ReapOutcome::NoOp`] — key missing, wrong type, or no expired fields.
    ///
    /// # Complexity
    /// O(E) where E is the number of entries in the `ttls` BTreeMap.
    /// `SmallVec` avoids heap allocation for the common case of ≤8 expired
    /// fields per tick.
    pub fn reap_expired_fields_one_hash(&mut self, key: &[u8]) -> ReapOutcome {
        let now_ms = self.now_ms();

        // --- Pass 1: immutable scan to collect expired field names ----------
        // We clone the field names so we can drop the immutable borrow before
        // the mutable pass.
        let to_remove: SmallVec<[Bytes; 8]> = match self.data().get(key) {
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::HashWithTtl { ttls, .. } => ttls
                    .iter()
                    .filter(|(_, t)| **t <= now_ms)
                    .map(|(f, _)| f.clone())
                    .collect(),
                _ => return ReapOutcome::NoOp,
            },
            None => return ReapOutcome::NoOp,
        };

        if to_remove.is_empty() {
            return ReapOutcome::NoOp;
        }

        // --- Pass 2: mutable removal -----------------------------------------
        // `get_mut` performs lazy expiry on the whole-key TTL and returns
        // `None` if the key is absent.  For HashWithTtl the whole-key TTL is
        // never set (field TTLs live in the ttls sidecar), so this is safe.
        let entry = match self.get_mut(key) {
            Some(e) => e,
            None => return ReapOutcome::NoOp,
        };
        let rv = match entry.value.as_redis_value_mut() {
            Some(v) => v,
            None => return ReapOutcome::NoOp,
        };

        // Perform removals and determine the post-removal state.
        // We do this in a scoped block so that the mutable borrows of `fields`
        // and `ttls` (via the match arm) are dropped before we potentially
        // reassign `*rv` for the downgrade case.
        enum PostReap {
            KeyDeleted,
            NeedDowngrade(std::collections::HashMap<Bytes, Bytes>),
            FieldsRemoved,
            NoOp,
        }

        let action = match rv {
            RedisValue::HashWithTtl { fields, ttls } => {
                for f in &to_remove {
                    fields.remove(f.as_ref());
                    ttls.remove(f.as_ref());
                }
                if fields.is_empty() {
                    PostReap::KeyDeleted
                } else if ttls.is_empty() {
                    PostReap::NeedDowngrade(std::mem::take(fields))
                } else {
                    PostReap::FieldsRemoved
                }
            }
            _ => PostReap::NoOp,
        };

        match action {
            PostReap::KeyDeleted => ReapOutcome::KeyDeleted,
            PostReap::NeedDowngrade(m) => {
                // `rv` borrow from the previous match is gone; reassign safely.
                *rv = RedisValue::Hash(m);
                ReapOutcome::Downgraded
            }
            PostReap::FieldsRemoved => ReapOutcome::FieldsRemoved,
            PostReap::NoOp => ReapOutcome::NoOp,
        }
    }
}
