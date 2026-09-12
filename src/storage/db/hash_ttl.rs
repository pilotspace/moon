//! HEXPIRE-family per-field TTL storage primitives (split from db/mod.rs).

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::RedisValue;

use crate::storage::db::{
    Database, FieldState, HashTtlCond, WrongType, hash_field_cost, hash_ttl_field_cost,
    hash_ttl_sidecar_box_cost, promote_to_hash_with_ttl,
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
            RedisValue::HashListpack(lp) => lp.find_pair_index(field).is_some(),
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
                    *rv = RedisValue::Hash(Box::new(map));
                    let after = rv.estimate_memory();
                    credit = before.saturating_sub(after);
                }
                RedisValue::HashWithTtl {
                    fields,
                    ttls,
                    min_expiry_ms,
                } => {
                    let old_ttl = ttls.remove(field);
                    // moon#861: the sidecar entry is billed by
                    // `estimate_memory` too — credit it with the field.
                    if old_ttl.is_some() {
                        credit += hash_ttl_field_cost(field);
                    }
                    if let Some(v) = fields.remove(field) {
                        credit += hash_field_cost(field, &v);
                    }
                    if ttls.is_empty() {
                        let m = std::mem::take(fields);
                        *rv = RedisValue::Hash(m);
                        // The sidecar BOX goes away with the variant swap.
                        credit += hash_ttl_sidecar_box_cost();
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
            // moon#926: the field was deleted — a real modification of the
            // watched key. Stamped here rather than at the top of the method
            // so the `!field_exists` and NX/XX/GT/LT-rejected paths, which
            // redis 8.6.1 leaves clean, stay clean.
            self.stamp_hash_field_mutation(key);
            return Ok(2);
        }

        // 4. Promote to HashWithTtl if needed.
        //
        // moon#861: promotion allocates — the `ttls` sidecar box always, and a
        // whole `HashMap` when coming from a listpack. `remove_hot` credits
        // `entry_overhead` recomputed from the value at DEL time, so those
        // bytes get credited whether or not they were ever charged. The delta
        // is accumulated here and settled once at the end of the call, after
        // the `&mut` borrow into `self.data` has ended.
        let mut mem_delta = promote_to_hash_with_ttl(rv);
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
            // The promotion above already happened and is already billed by
            // `estimate_memory`; the rejected TTL does not un-promote it.
            self.apply_memory_delta(mem_delta);
            return Ok(-2);
        }

        // 6. Set the TTL and maintain the cached minimum.
        // A brand-new sidecar entry costs `hash_ttl_field_cost`; overwriting an
        // existing one replaces a `u64` in place and costs nothing.
        if current.is_none() {
            mem_delta += hash_ttl_field_cost(field) as isize;
        }
        ttls.insert(Bytes::copy_from_slice(field), ts_ms);
        if ts_ms < *min_expiry_ms {
            *min_expiry_ms = ts_ms;
        }
        self.maybe_has_expiring_keys = true;
        // moon#543: this is the ONLY writer that can LOWER a hash's field
        // minimum, so it is the only one that must index. Inserting `ts_ms`
        // unconditionally (rather than the post-write minimum) keeps the
        // lower-bound invariant with no extra read: `ts_ms >= min` only when
        // an EARLIER pair already covers the hash, and `ts_ms < min` is
        // exactly the case a new pair is needed for. Disjoint field borrow —
        // `ttls`/`min_expiry_ms` alias `self.data`, this aliases
        // `self.hash_expiry_index` (same shape as the line above).
        self.hash_expiry_index
            .insert((ts_ms, crate::storage::compact_key::CompactKey::from(key)));
        self.apply_memory_delta(mem_delta);
        // moon#926: a per-field TTL was set — see the `Ok(2)` arm above.
        self.stamp_hash_field_mutation(key);
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
                if lp.find_pair_index(field).is_some() {
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
        // moon#861: both the sidecar ENTRY and, on the last removal, the
        // sidecar BOX are billed by `estimate_memory`. Credit them here or the
        // eventual DEL credits bytes this path never released.
        let mut credit: usize = 0;
        if had_ttl {
            credit += hash_ttl_field_cost(field);
            if ttls.is_empty() {
                let m = std::mem::take(fields);
                *rv = RedisValue::Hash(m);
                credit += hash_ttl_sidecar_box_cost();
            } else if removed == Some(*min_expiry_ms) {
                // Removed field held the minimum; recompute from remaining entries.
                *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
            }
        }
        if credit > 0 {
            self.credit_memory(credit);
        }
        // moon#926: only a TTL that was actually removed is a modification.
        // `HPERSIST` on a field with no TTL answers -1 and, in redis 8.6.1,
        // leaves a watcher alone.
        if had_ttl {
            self.stamp_hash_field_mutation(key);
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
        // moon#861: every sidecar entry dropped here was billed by
        // `estimate_memory`, and so is the sidecar box once the last one goes.
        let mut credit: usize = 0;
        for f in fields {
            if let Some(t) = ttls.remove(f.as_ref()) {
                credit += hash_ttl_field_cost(f.as_ref());
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
                credit += hash_ttl_sidecar_box_cost();
            }
        } else if min_invalidated {
            *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
        }
        if credit > 0 {
            self.credit_memory(credit);
        }
        // moon#926: `credit > 0` is exactly "at least one sidecar TTL was
        // removed", i.e. this call changed the hash. Every other path out of
        // this method returned early without touching anything.
        if credit > 0 {
            self.stamp_hash_field_mutation(key);
        }
    }

    /// Remove a single field from a hash, cleaning up the TTL sidecar when
    /// present.  Returns `(removed, hash_now_empty)`:
    /// - `removed = true` if the field existed and was deleted.
    /// - `hash_now_empty = true` if the hash has no more fields after deletion.
    ///
    /// Returns `Err(wrongtype_error())` if the key is not a hash.
    /// Returns `Ok((false, false))` for a missing key.
    ///
    /// The one-field spelling of [`Self::hash_delete_fields`], and a
    /// delegator to it so the two cannot drift: HDEL's batch and a
    /// single-field caller settle the ledger, the TTL sidecar and the WATCH
    /// version through the same code.
    pub fn hash_delete_field(&mut self, key: &[u8], field: &[u8]) -> Result<(bool, bool), Frame> {
        let (removed, empty) = self.hash_delete_fields(key, &[field])?;
        Ok((removed > 0, empty))
    }

    /// Remove every field in `fields` from the hash at `key` in ONE accessor,
    /// cleaning up the TTL sidecar as it goes.
    ///
    /// Returns `(removed, hash_now_empty)`:
    /// - `removed` — how many of `fields` existed and were deleted. A field
    ///   named twice counts once, because the second removal finds nothing.
    /// - `hash_now_empty` — whether the hash has no fields left **after the
    ///   whole batch**. The caller drops the key when this is true.
    ///
    /// Returns `Err(wrongtype_error())` if the key is not a hash, without
    /// removing anything. `Ok((0, false))` for a missing key — this never
    /// fabricates a container.
    ///
    /// # Why the batch is the unit (moon#942)
    ///
    /// The per-field spelling cost TWO DashTable probes for every field: its
    /// own `self.data.get_mut(key)` and, when the field really went,
    /// `stamp_hash_field_mutation`'s. `HDEL h f1 f2 f3` therefore hashed the
    /// key SIX times, where Redis pays one `dictFind` for the command and
    /// then looks each field up inside the hash. This walks the fields inside
    /// the handle the single lookup already holds, and stamps that same
    /// handle — so the whole command is ONE probe.
    ///
    /// It also fixes what the per-field loop could not express. Emptiness is
    /// a property of the hash AFTER the batch, not of the last field the
    /// caller happened to name: `hdel` tracked it in a variable reassigned on
    /// every iteration, so `HDEL h only absent` overwrote the emptiness the
    /// real removal reported and left the key behind as a hash with zero
    /// fields.
    ///
    /// moon#926: one command is one WATCH version bump, and only when
    /// something was actually removed. `HDEL k <absent>` answers 0 and, in
    /// redis 8.6.1, leaves a watcher alone.
    pub fn hash_delete_fields<F>(&mut self, key: &[u8], fields: &[F]) -> Result<(i64, bool), Frame>
    where
        F: AsRef<[u8]>,
    {
        let Some(entry) = self.data.get_mut(key) else {
            return Ok((0, false));
        };
        let Some(rv) = entry.value.as_redis_value_mut() else {
            return Err(Self::wrongtype_error());
        };
        // Accumulated O(1) credit for the whole batch; applied to
        // `used_memory` once the entry's borrow ends — see the WS6 accounting
        // note above `entry_overhead`.
        let mut credit: usize = 0;
        let mut removed: i64 = 0;
        let result: Result<bool, Frame> = match rv {
            RedisValue::Hash(map) => {
                for f in fields {
                    if let Some(v) = map.remove(f.as_ref()) {
                        credit += hash_field_cost(f.as_ref(), &v);
                        removed += 1;
                    }
                }
                Ok(map.is_empty())
            }
            RedisValue::HashListpack(lp) => {
                // Listpack cost is O(1) (capacity-based) — one snapshot pair
                // around the whole batch, not one per field. `Vec::drain`
                // leaves capacity alone, so this is the same figure the
                // per-field spelling produced, summed the same way.
                let before = lp.estimate_memory();
                for f in fields {
                    // ONE borrowed scan per field locates the pair and drains
                    // both entries — the per-field work Redis does too
                    // (moon#799).
                    if lp.remove_pair(f.as_ref()) {
                        removed += 1;
                    }
                }
                let after = lp.estimate_memory();
                credit += before.saturating_sub(after);
                Ok(lp.is_empty())
            }
            RedisValue::HashWithTtl {
                fields: fmap,
                ttls,
                min_expiry_ms,
            } => {
                // Whether any removed field held the cached minimum. Recomputed
                // once after the batch rather than per removal.
                let mut min_invalidated = false;
                for f in fields {
                    let Some(v) = fmap.remove(f.as_ref()) else {
                        continue;
                    };
                    removed += 1;
                    credit += hash_field_cost(f.as_ref(), &v);
                    // moon#861: the sidecar entry is billed by
                    // `estimate_memory` and so is credited here.
                    if let Some(old) = ttls.remove(f.as_ref()) {
                        credit += hash_ttl_field_cost(f.as_ref());
                        if old == *min_expiry_ms {
                            min_invalidated = true;
                        }
                    }
                }
                if removed == 0 {
                    Ok(fmap.is_empty())
                } else if ttls.is_empty() {
                    // Every TTL is gone: downgrade to a plain `Hash` and
                    // credit the sidecar box (moon#861). The fields map may
                    // or may not be empty — when it is, the caller drops the
                    // key, exactly as it did before.
                    let empty = fmap.is_empty();
                    let m = std::mem::take(fmap);
                    *rv = RedisValue::Hash(m);
                    credit += hash_ttl_sidecar_box_cost();
                    Ok(empty)
                } else {
                    if min_invalidated {
                        *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
                    }
                    Ok(fmap.is_empty())
                }
            }
            _ => Err(Self::wrongtype_error()),
        };
        let container_empty = result?;
        // A batch that removed nothing reports `false`, whatever the
        // container's state — the single-field spelling's contract, kept
        // exactly, so a caller never drops a key on the strength of a HDEL
        // that did nothing.
        let empty = removed > 0 && container_empty;
        // moon#926: stamp the handle this call already holds, once, and only
        // when the command really changed the hash.
        if removed > 0 {
            crate::storage::db::stamp_mutation(entry);
        }
        if credit > 0 {
            self.credit_memory(credit);
        }
        Ok((removed, empty))
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
                // ONE borrowed scan locates the pair, materializes only the
                // matched value, and drains both entries. This used to be
                // four walks: find, get, remove, remove (moon#799).
                if let Some(v) = lp.take_pair_value(field) {
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
                    // moon#861: sidecar entry + (on the last one) sidecar box.
                    if old_ttl.is_some() {
                        credit += hash_ttl_field_cost(field);
                    }
                    if ttls.is_empty() && !fields.is_empty() {
                        // All TTLs gone, live fields remain — downgrade to Hash.
                        let m = std::mem::take(fields);
                        *rv = RedisValue::Hash(m);
                        credit += hash_ttl_sidecar_box_cost();
                    } else if ttls.is_empty() && fields.is_empty() {
                        // Both maps empty — leave an empty Hash shell; caller
                        // must call cleanup_empty_hash to remove the key.
                        *rv = RedisValue::Hash(Box::default());
                        credit += hash_ttl_sidecar_box_cost();
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
        // moon#926: stamp only when a value actually came out — `HGETDEL` on
        // an absent field is a read.
        if matches!(result, Ok(Some(_))) {
            self.stamp_hash_field_mutation(key);
        }
        result
    }

    /// Advance `key`'s WATCH version after a per-field hash write that
    /// changed something (moon#926).
    ///
    /// Costs one extra hash probe, unlike every other stamp in the tree,
    /// because the field-TTL writers borrow `entry.value` for the whole body
    /// and only know at the END whether they changed anything. That is the
    /// right trade here: the alternative is stamping at the `&mut` handout,
    /// which would abort a watcher for `HDEL k <absent-field>` and
    /// `HPERSIST` on a field with no TTL — both of which redis 8.6.1 leaves
    /// clean. The HEXPIRE/HDEL family is not a hot path; the accuracy is.
    #[inline]
    fn stamp_hash_field_mutation(&mut self, key: &[u8]) {
        if let Some(entry) = self.data.get_mut(key) {
            crate::storage::db::stamp_mutation(entry);
        }
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
