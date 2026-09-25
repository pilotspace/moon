//! List helpers for the blocking serve path (BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/
//! BLMPOP served on the spot and on wake) and LMOVE's push — split from
//! `accessors.rs` so that file stops growing (moon#1212).

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::db::{Database, list_elem_cost};
use crate::storage::db_kind;
use crate::storage::db_read::ListRef;

impl Database {
    /// Pop the front element from a list. Returns None if key missing/empty/wrong type.
    /// Removes the key if the list becomes empty. A listpack stays a listpack
    /// (moon#1212, see [`Self::list_pop_listpack`]).
    ///
    /// moon#523/#539: the lookup is deliberately NON-creating. This helper
    /// backs the blocking fast path (`try_immediate_pop` → BLPOP/BLMOVE/…),
    /// so a `get_or_create_list` here materialised an empty list on every
    /// miss — a phantom key that EXISTS/TYPE/DBSIZE reported, that a later
    /// RPUSH rejected with WRONGTYPE, and that no path ever removed.
    pub fn list_pop_front(&mut self, key: &[u8]) -> Option<Bytes> {
        if let Some(popped) = self.list_pop_listpack(key, true) {
            return popped;
        }
        let list = self.get_mut_if_present::<db_kind::ListKind>(key).ok()??;
        let val = list.pop_front()?;
        let empty = list.is_empty();
        // `list`'s borrow of `self` ends above.
        //
        // moon#949: credit the element UNCONDITIONALLY. The empty branch used
        // to skip this on the theory that whole-key removal recovers the cost
        // via `entry_overhead` — it does not. `entry_overhead` is computed
        // from the CURRENT value, which by then no longer holds the element,
        // so the push-time charge was never given back and `used_memory`
        // drifted UP by one element every time a list was drained to empty.
        // Unbounded on an empty keyspace, which is `--maxmemory` and eviction
        // firing on a server holding nothing. `pop_eager` in
        // `src/command/list/` always credited unconditionally; this is the
        // same rule.
        self.credit_memory(list_elem_cost(&val));
        if empty {
            self.remove(key);
        }
        Some(val)
    }

    /// Pop the back element from a list. Returns None if key missing/empty/wrong type.
    /// Removes the key if the list becomes empty. Handles compact listpack upgrade.
    ///
    /// Non-creating on a missing key — see [`Self::list_pop_front`].
    pub fn list_pop_back(&mut self, key: &[u8]) -> Option<Bytes> {
        if let Some(popped) = self.list_pop_listpack(key, false) {
            return popped;
        }
        let list = self.get_mut_if_present::<db_kind::ListKind>(key).ok()??;
        let val = list.pop_back()?;
        let empty = list.is_empty();
        // moon#949 — see `list_pop_front`.
        self.credit_memory(list_elem_cost(&val));
        if empty {
            self.remove(key);
        }
        Some(val)
    }

    /// The listpack arm of the blocking-path pops (moon#1212): `Some(result)`
    /// when `key` holds a `ListListpack`, popped IN PLACE — the full-form
    /// accessor below them upgrades on access, so every BLPOP/BRPOP/BLMOVE
    /// served on wake turned a small list into a `linkedlist` for good.
    /// `None` hands the key to the full-form path unchanged (full, missing,
    /// wrong type). Non-creating: the `&self` probe answers "absent" without
    /// inserting, and the write accessor only runs on a present listpack.
    fn list_pop_listpack(&mut self, key: &[u8], front: bool) -> Option<Option<Bytes>> {
        let now_ms = self.cached_now_ms;
        if !matches!(
            self.peek_list_ref_if_alive(key, now_ms),
            Ok(Some(ListRef::Listpack(_)))
        ) {
            return None;
        }
        let Ok(Some(lp)) = self.get_or_create_list_listpack(key) else {
            return Some(None);
        };
        // Listpack `estimate_memory()` is O(1) (capacity-based).
        let before = lp.estimate_memory();
        let popped = lp.pop_end(front);
        let after = lp.estimate_memory();
        let empty = lp.is_empty();
        // `lp`'s borrow of `self` ends here.
        self.adjust_memory(before, after);
        if empty {
            self.remove(key);
        }
        Some(popped)
    }

    /// Push one element onto an end of `key` exactly as a one-element
    /// `LPUSH`/`RPUSH` would: onto a listpack in place (a missing key is born
    /// a listpack when the element fits the policy), promoting past the
    /// policy, and onto the full `VecDeque` otherwise (moon#1212 — this is
    /// `LMOVE`'s push and the blocking wake path's, which used to flatten).
    ///
    /// A refusal is RETURNED (moon#1225): a wrong type, or a cold copy whose
    /// bytes cannot be read.
    pub fn list_push_end(&mut self, key: &[u8], value: &Bytes, front: bool) -> Result<(), Frame> {
        let limits = self.encoding_limits();
        if limits.fits(crate::storage::db::Shape::List, 1, value.len())
            && let Some(lp) = self.get_or_create_list_listpack(key)?
        {
            let before = lp.estimate_memory();
            if front {
                lp.push_front(value);
            } else {
                lp.push_back(value);
            }
            let after = lp.estimate_memory();
            let should_upgrade = !limits.listpack_fits(crate::storage::db::Shape::List, lp);
            // `lp`'s borrow of `self` ends here.
            self.adjust_memory(before, after);
            if should_upgrade {
                self.promote_list_listpack(key, after);
            }
            return Ok(());
        }
        let cost = list_elem_cost(value);
        let list = self.get_or_create_list(key)?;
        if front {
            list.push_front(value.clone());
        } else {
            list.push_back(value.clone());
        }
        self.charge_memory(cost);
        Ok(())
    }

    /// Promote a `ListListpack` to the full `VecDeque` and settle the one-time
    /// cost-model swing, `after` being the listpack's last billed size — the
    /// block every list writer runs when a push crosses the policy.
    pub fn promote_list_listpack(&mut self, key: &[u8], after: usize) {
        let list = self.upgrade_list_listpack_to_list(key);
        let new_cost: usize = list.iter().map(|e| list_elem_cost(e)).sum();
        self.credit_memory(after);
        self.charge_memory(new_cost);
    }

    /// Push an element to the front of a list. Creates the list if it does not exist.
    ///
    /// A refusal (wrong type, or an unreadable cold copy — moon#1225) cannot
    /// be returned through this signature; the callers (the blocking serve
    /// paths) refuse both BEFORE they pop, via `get_list_ref_if_alive`. What
    /// is left — a cold destination whose file read cleanly for that probe and
    /// then failed on this promotion's second read — is logged, never silent.
    pub fn list_push_front(&mut self, key: &[u8], value: Bytes) {
        if self.list_push_end(key, &value, true).is_err() {
            log_refused_list_push(key, value.len());
        }
    }

    /// Push an element to the back of a list. Creates the list if it does not exist.
    ///
    /// See [`Self::list_push_front`] for the refusal contract.
    pub fn list_push_back(&mut self, key: &[u8], value: Bytes) {
        if self.list_push_end(key, &value, false).is_err() {
            log_refused_list_push(key, value.len());
        }
    }
}

/// moon#1225: a blocking-path list push was refused after its element had
/// already been popped. Not reachable by type (the callers probe first); a
/// transient cold-tier fault between the probe and the push is the one way
/// in, and a dropped element must leave evidence.
#[cold]
fn log_refused_list_push(key: &[u8], value_len: usize) {
    tracing::error!(
        key_len = key.len(),
        value_len,
        "list push refused after the element was popped (cold-tier fault or wrong type); \
         the element was not stored (moon#1225)"
    );
}
