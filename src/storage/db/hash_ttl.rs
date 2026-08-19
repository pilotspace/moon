//! HEXPIRE-family per-field TTL storage primitives (split from db/mod.rs).

use bytes::Bytes;
use std::collections::HashMap;

use crate::protocol::Frame;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::RedisValue;

use crate::storage::db::{
    Database, FieldState, HashTtlCond, WrongType, hash_field_cost, promote_to_hash_with_ttl,
};

impl Database {
    // -- HEXPIRE family (phase 195 / issue #106) ------------------------------
    //
    // Storage primitives for Valkey 9.0 HEXPIRE-family parity. Consumed by
    // phases 196 (write commands), 198 (read commands), 199 (atomic compound).
    //
    // Storage strategy: auto-promote to `RedisValue::HashWithTtl` on first
    // per-field TTL touch. Downgrade back to plain `Hash` when the last TTL
    // is removed via `hash_persist_field`. No promotion of HashListpack
    // happens unless an actual TTL is being set — pure reads stay cheap.

    /// Set per-field TTL (absolute unix-ms). Returns the per-field result code:
    /// `0` = no such field, `1` = TTL set, `2` = expired during this call,
    /// `-2` = NX/XX/GT/LT condition not met. `Err(WrongType)` if key exists and
    /// is not a hash.
    pub fn hash_set_field_ttl(
        &mut self,
        key: &[u8],
        field: &[u8],
        ts_ms: u64,
        cond: HashTtlCond,
    ) -> Result<i64, WrongType> {
        // 1. Key existence + type check.
        let Some(entry) = self.data.get_mut(key) else {
            return Ok(0);
        };
        let Some(rv) = entry.value.as_redis_value_mut() else {
            // Inline string or heap string — wrong type.
            return Err(WrongType);
        };
        match rv {
            RedisValue::Hash(_) | RedisValue::HashListpack(_) | RedisValue::HashWithTtl { .. } => {}
            _ => return Err(WrongType),
        }

        // 2. Field-existence pre-check (avoids unnecessary promotion).
        let field_exists = match rv {
            RedisValue::Hash(map) => map.contains_key(field),
            RedisValue::HashListpack(lp) => lp.iter_pairs().any(|(f, _)| f.as_bytes() == field),
            RedisValue::HashWithTtl { fields, .. } => fields.contains_key(field),
            _ => unreachable!("type-checked above"),
        };
        if !field_exists {
            return Ok(0);
        }

        // 3. Past-expiry short-circuit: delete the field, return code 2.
        if ts_ms <= self.cached_now_ms {
            // O(1) credit for the field removed below (see the WS6 accounting
            // note above `entry_overhead`); the `HashListpack` branch instead
            // diffs `estimate_memory()` before/after since it also changes
            // encoding (listpack -> Hash), an O(n) transform it already pays.
            let mut credit: usize = 0;
            match rv {
                RedisValue::Hash(map) => {
                    if let Some(v) = map.remove(field) {
                        credit = hash_field_cost(field, &v);
                    }
                }
                RedisValue::HashListpack(lp) => {
                    let before = lp.estimate_memory();
                    // Promote to Hash to delete (listpack delete-by-key is awkward).
                    let mut map = lp.to_hash_map();
                    map.remove(field);
                    *rv = RedisValue::Hash(map);
                    let after = rv.estimate_memory();
                    credit = before.saturating_sub(after);
                }
                RedisValue::HashWithTtl {
                    fields,
                    ttls,
                    min_expiry_ms,
                } => {
                    let old_ttl = ttls.remove(field);
                    if let Some(v) = fields.remove(field) {
                        credit = hash_field_cost(field, &v);
                    }
                    if ttls.is_empty() {
                        let m = std::mem::take(fields);
                        *rv = RedisValue::Hash(m);
                    } else if old_ttl == Some(*min_expiry_ms) {
                        // The removed field held the min; recompute.
                        *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
                    }
                }
                _ => unreachable!(),
            }
            if credit > 0 {
                self.credit_memory(credit);
            }
            return Ok(2);
        }

        // 4. Promote to HashWithTtl if needed.
        promote_to_hash_with_ttl(rv);
        // moon#541: arm the latch the moment the value IS HashWithTtl — the
        // NX/XX/GT/LT gate below can return early AFTER promotion (e.g. GT
        // on a non-volatile field), and a HashWithTtl existing with the
        // latch down breaks the latch's conservativeness invariant (the
        // debug_expiry_index_consistent oracle checks exactly that).
        self.hash_field_ttl_latch = true;
        let RedisValue::HashWithTtl {
            ttls,
            min_expiry_ms,
            ..
        } = rv
        else {
            unreachable!("just promoted")
        };

        // 5. Apply NX/XX/GT/LT conditional gate.
        // Valkey semantics: a non-volatile field is treated as +∞ for GT/LT.
        let current = ttls.get(field).copied();
        let pass = match cond {
            HashTtlCond::Always => true,
            HashTtlCond::Nx => current.is_none(),
            HashTtlCond::Xx => current.is_some(),
            HashTtlCond::Gt => current.is_some_and(|c| ts_ms > c),
            HashTtlCond::Lt => current.is_some_and(|c| ts_ms < c) || current.is_none(),
        };
        if !pass {
            return Ok(-2);
        }

        // 6. Set the TTL and maintain the cached minimum.
        ttls.insert(Bytes::copy_from_slice(field), ts_ms);
        if ts_ms < *min_expiry_ms {
            *min_expiry_ms = ts_ms;
        }
        self.maybe_has_expiring_keys = true;
        Ok(1)
    }

    /// Read absolute expiry ms for a field. `None` for missing field or no TTL.
    pub fn hash_get_field_ttl_ms(&self, key: &[u8], field: &[u8]) -> Option<u64> {
        let entry = self.data.get(key)?;
        match entry.value.as_redis_value() {
            RedisValueRef::HashWithTtl { ttls, .. } => ttls.get(field).copied(),
            _ => None,
        }
    }

    /// Tri-state field-existence + TTL state lookup used by HTTL / HEXPIRETIME
    /// and the HEXPIRE conditional gates.
    pub fn hash_field_state(&self, key: &[u8], field: &[u8], _now_ms: u64) -> FieldState {
        let Some(entry) = self.data.get(key) else {
            return FieldState::Missing;
        };
        match entry.value.as_redis_value() {
            RedisValueRef::Hash(map) => {
                if map.contains_key(field) {
                    FieldState::NoTtl
                } else {
                    FieldState::Missing
                }
            }
            RedisValueRef::HashListpack(lp) => {
                if lp.iter_pairs().any(|(f, _)| f.as_bytes() == field) {
                    FieldState::NoTtl
                } else {
                    FieldState::Missing
                }
            }
            RedisValueRef::HashWithTtl { fields, ttls, .. } => {
                if !fields.contains_key(field) {
                    FieldState::Missing
                } else if let Some(&ms) = ttls.get(field) {
                    FieldState::Ttl(ms)
                } else {
                    FieldState::NoTtl
                }
            }
            _ => FieldState::Missing,
        }
    }

    /// Remove the TTL from a field. Returns `true` if the field had a TTL
    /// (and it was removed); `false` if the field was missing or had no TTL.
    /// Downgrades `HashWithTtl` back to plain `Hash` when the last TTL is
    /// removed.
    pub fn hash_persist_field(&mut self, key: &[u8], field: &[u8]) -> bool {
        let Some(entry) = self.data.get_mut(key) else {
            return false;
        };
        let Some(rv) = entry.value.as_redis_value_mut() else {
            return false;
        };
        let RedisValue::HashWithTtl {
            fields,
            ttls,
            min_expiry_ms,
        } = rv
        else {
            return false;
        };
        let removed = ttls.remove(field);
        let had_ttl = removed.is_some();
        if had_ttl {
            if ttls.is_empty() {
                let m = std::mem::take(fields);
                *rv = RedisValue::Hash(m);
            } else if removed == Some(*min_expiry_ms) {
                // Removed field held the minimum; recompute from remaining entries.
                *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
            }
        }
        had_ttl
    }

    /// Clear the TTL sidecar entries for every field in `fields`. For plain
    /// `Hash` and `HashListpack` this is a no-op (they carry no TTLs).
    /// For `HashWithTtl`, removes the sidecar entry for each field; downgrades
    /// to plain `Hash` when the last TTL is removed.
    ///
    /// Called by HSET / HMSET when they overwrite a field: the new write
    /// unconditionally persists the field (Valkey semantics — HSET clears TTL).
    pub fn hash_clear_field_ttls<F>(&mut self, key: &[u8], fields: &[F])
    where
        F: AsRef<[u8]>,
    {
        let Some(entry) = self.data.get_mut(key) else {
            return;
        };
        let Some(rv) = entry.value.as_redis_value_mut() else {
            return;
        };
        let RedisValue::HashWithTtl {
            fields: _,
            ttls,
            min_expiry_ms,
        } = rv
        else {
            // Plain Hash or HashListpack carries no per-field TTLs.
            return;
        };
        // Track whether any removed TTL equaled the cached minimum.
        // If so, recompute after all removals rather than re-scanning per step.
        let mut min_invalidated = false;
        for f in fields {
            if let Some(t) = ttls.remove(f.as_ref()) {
                if t == *min_expiry_ms {
                    min_invalidated = true;
                }
            }
        }
        if ttls.is_empty() {
            // Downgrade: borrow ends, then re-borrow to swap variant.
            let Some(rv2) = entry.value.as_redis_value_mut() else {
                return;
            };
            if let RedisValue::HashWithTtl { fields: fmap, .. } = rv2 {
                let m = std::mem::take(fmap);
                *rv2 = RedisValue::Hash(m);
            }
        } else if min_invalidated {
            *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
        }
    }

    /// Remove a single field from a hash, cleaning up the TTL sidecar when
    /// present.  Returns `(removed, hash_now_empty)`:
    /// - `removed = true` if the field existed and was deleted.
    /// - `hash_now_empty = true` if the hash has no more fields after deletion.
    ///
    /// Returns `Err(wrongtype_error())` if the key is not a hash.
    /// Returns `Ok((false, false))` for a missing key.
    pub fn hash_delete_field(&mut self, key: &[u8], field: &[u8]) -> Result<(bool, bool), Frame> {
        let Some(entry) = self.data.get_mut(key) else {
            return Ok((false, false));
        };
        let Some(rv) = entry.value.as_redis_value_mut() else {
            return Err(Self::wrongtype_error());
        };
        // Accumulated O(1) credit for this call; applied to `used_memory`
        // once at the end (single field mutated per call, so this is exactly
        // the byte delta — see the WS6 accounting note above `entry_overhead`).
        let mut credit: usize = 0;
        let result = match rv {
            RedisValue::Hash(map) => {
                if let Some(v) = map.remove(field) {
                    credit = hash_field_cost(field, &v);
                    let empty = map.is_empty();
                    Ok((true, empty))
                } else {
                    Ok((false, false))
                }
            }
            RedisValue::HashListpack(lp) => {
                // Listpack cost is O(1) (capacity-based) — snapshot before/after
                // instead of tracking a per-element formula.
                let before = lp.estimate_memory();
                // Locate field among pairs, remove both field + value entries.
                let mut found_idx: Option<usize> = None;
                for (i, (f, _)) in lp.iter_pairs().enumerate() {
                    if f.as_bytes() == field {
                        found_idx = Some(i);
                        break;
                    }
                }
                if let Some(i) = found_idx {
                    // Each pair occupies two listpack slots: field at 2*i, value at 2*i+1.
                    // Remove value first (higher index) then field to keep indices stable.
                    lp.remove_at(i * 2 + 1);
                    lp.remove_at(i * 2);
                    let after = lp.estimate_memory();
                    credit = before.saturating_sub(after);
                    let empty = lp.is_empty();
                    Ok((true, empty))
                } else {
                    Ok((false, false))
                }
            }
            RedisValue::HashWithTtl {
                fields,
                ttls,
                min_expiry_ms,
            } => {
                if let Some(v) = fields.remove(field) {
                    credit = hash_field_cost(field, &v);
                    let old_ttl = ttls.remove(field);
                    if ttls.is_empty() && !fields.is_empty() {
                        // All TTLs gone but fields remain — downgrade to plain Hash.
                        let m = std::mem::take(fields);
                        *rv = RedisValue::Hash(m);
                        Ok((true, false))
                    } else if ttls.is_empty() && fields.is_empty() {
                        // Both maps empty — signal caller to delete the key.
                        // We leave the (now-empty) HashWithTtl in place; the
                        // caller will call db.remove() to drop the key.
                        let m = std::mem::take(fields);
                        *rv = RedisValue::Hash(m);
                        Ok((true, true))
                    } else {
                        // TTLs remain; recompute min if the removed field held it.
                        if old_ttl == Some(*min_expiry_ms) {
                            *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
                        }
                        let empty = fields.is_empty();
                        Ok((true, empty))
                    }
                } else {
                    Ok((false, false))
                }
            }
            _ => Err(Self::wrongtype_error()),
        };
        if credit > 0 {
            self.credit_memory(credit);
        }
        result
    }

    // -- HGETDEL / HGETEX family (phase 199 / issue #110) ----------------------
    //
    // Atomic compound get-and-mutate primitives for the two Valkey 9.1 commands.
    // Atomicity is guaranteed by the per-shard single-threaded execution model —
    // no explicit locking is required.

    /// Atomically read and remove a single field from a hash.
    ///
    /// Returns `Ok(Some(value))` when the field existed and was removed.
    /// Returns `Ok(None)` when the key is missing or the field does not exist.
    /// Returns `Err(WrongType)` when the key exists but is not a hash.
    ///
    /// For `HashWithTtl` the TTL sidecar entry is removed together with the
    /// field. When the last TTL sidecar entry is removed the encoding is
    /// downgraded back to a plain `Hash` (mirrors `hash_delete_field`).
    ///
    /// Callers **must** call [`Database::cleanup_empty_hash`] after processing
    /// all fields — this method intentionally does NOT delete the key when the
    /// hash becomes empty, so that the caller can accumulate results first.
    pub fn hash_get_and_delete_field(
        &mut self,
        key: &[u8],
        field: &[u8],
    ) -> Result<Option<Bytes>, WrongType> {
        let Some(entry) = self.data.get_mut(key) else {
            return Ok(None);
        };
        let Some(rv) = entry.value.as_redis_value_mut() else {
            return Err(WrongType);
        };
        // O(1) credit accumulator — one field removed per call (see the WS6
        // accounting note above `entry_overhead`).
        let mut credit: usize = 0;
        let result = match rv {
            RedisValue::Hash(map) => {
                let v = map.remove(field);
                if let Some(ref val) = v {
                    credit = hash_field_cost(field, val);
                }
                Ok(v)
            }
            RedisValue::HashListpack(lp) => {
                // Listpack cost is O(1) (capacity-based) — snapshot before/after.
                let before = lp.estimate_memory();
                // Locate the field-value pair by linear scan then remove both
                // listpack slots (value first so the field index stays valid).
                let mut found: Option<(usize, Bytes)> = None;
                for (i, (f, v)) in lp.iter_pairs().enumerate() {
                    if f.as_bytes() == field {
                        found = Some((i, v.to_bytes()));
                        break;
                    }
                }
                if let Some((i, v)) = found {
                    lp.remove_at(i * 2 + 1); // value first (higher index)
                    lp.remove_at(i * 2); // then field
                    let after = lp.estimate_memory();
                    credit = before.saturating_sub(after);
                    Ok(Some(v))
                } else {
                    Ok(None)
                }
            }
            RedisValue::HashWithTtl {
                fields,
                ttls,
                min_expiry_ms,
            } => {
                let v = fields.remove(field);
                if let Some(ref val) = v {
                    credit = hash_field_cost(field, val);
                    let old_ttl = ttls.remove(field);
                    if ttls.is_empty() && !fields.is_empty() {
                        // All TTLs gone, live fields remain — downgrade to Hash.
                        let m = std::mem::take(fields);
                        *rv = RedisValue::Hash(m);
                    } else if ttls.is_empty() && fields.is_empty() {
                        // Both maps empty — leave an empty Hash shell; caller
                        // must call cleanup_empty_hash to remove the key.
                        *rv = RedisValue::Hash(HashMap::new());
                    } else if old_ttl == Some(*min_expiry_ms) {
                        // Removed field held the minimum; recompute.
                        *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
                    }
                }
                Ok(v)
            }
            _ => Err(WrongType),
        };
        if credit > 0 {
            self.credit_memory(credit);
        }
        result
    }

    /// Remove the key when its hash value has become empty.
    ///
    /// No-op if the key is absent, is not a hash, or still has live fields.
    /// Intended to be called after a series of `hash_get_and_delete_field`
    /// calls to clean up the key when all fields were consumed.
    pub fn cleanup_empty_hash(&mut self, key: &[u8]) {
        let should_delete = self
            .data
            .get(key)
            .is_some_and(|e| match e.value.as_redis_value() {
                crate::storage::compact_value::RedisValueRef::Hash(m) => m.is_empty(),
                crate::storage::compact_value::RedisValueRef::HashListpack(lp) => lp.is_empty(),
                crate::storage::compact_value::RedisValueRef::HashWithTtl { fields, .. } => {
                    fields.is_empty()
                }
                _ => false,
            });
        if should_delete {
            // Through `remove_hot` (moon#541): keeps the expiry index in
            // lock-step and credits the (now empty) entry shell's memory,
            // which the raw `data.remove` here never did.
            self.remove_hot(key);
        }
    }
}
