//! moon#1166: lock-free "could this write invalidate anything?" for the
//! process-global tracking table.
//!
//! With ANY client tracking (`tracking_active()`), every successful write on
//! every shard used to take the table mutex just to find out that nobody
//! tracks the key — one idle `CLIENT TRACKING on` client serialized all
//! writers on one lock and one cache line (review: SET -41%, redis -9%).
//!
//! Three counters describe the GLOBAL table's contents conservatively, so a
//! write that provably cannot match skips the lock:
//!
//! * `KEY_FILTER`: a counting filter — for each bucket, how many keys in
//!   `key_clients` hash to it. A zero bucket means no tracked key hashes
//!   there; a collision only costs a lock (a false "maybe").
//! * `TRACKED_KEYS`: how many keys `key_clients` holds. The idle-tracker case
//!   is 0, answered without hashing at all.
//! * `BCAST_PREFIXES`: how many BCAST registrations exist. Any prefix may
//!   match any key, so while one exists every write takes the lock (the
//!   pre-fix behaviour, for BCAST deployments only).
//!
//! # Why a lock-free read is as good as the lock
//!
//! Every update runs UNDER the table mutex (from `TrackingTable`'s own
//! methods): incremented BEFORE a key or prefix becomes visible in the table,
//! decremented AFTER it is gone. All accesses are SeqCst, so a writer's
//! lock-free read and a registration's increment are totally ordered — just
//! as the two lock acquisitions they replace were:
//!
//! * the read precedes the increment: the write precedes the registration,
//!   and under the lock it would have found nothing either;
//! * the read follows it: the writer sees a non-zero count and takes the
//!   lock, which orders it after the registration's critical section, so it
//!   finds the key.
//!
//! A read that observes a count that is about to drop only costs a lock.

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

const KEY_FILTER_BUCKETS: usize = 4096;
#[allow(clippy::declare_interior_mutable_const)] // template for static array init only
const FILTER_ZERO: AtomicU32 = AtomicU32::new(0);
static KEY_FILTER: [AtomicU32; KEY_FILTER_BUCKETS] = [FILTER_ZERO; KEY_FILTER_BUCKETS];
static TRACKED_KEYS: AtomicUsize = AtomicUsize::new(0);
static BCAST_PREFIXES: AtomicUsize = AtomicUsize::new(0);

#[inline]
fn bucket(key: &[u8]) -> usize {
    // Its own seed, so buckets are independent of the shard-routing hash.
    (xxhash_rust::xxh64::xxh64(key, 0x5431_3166) as usize) & (KEY_FILTER_BUCKETS - 1)
}

/// A key is about to enter the global `key_clients` (call BEFORE inserting,
/// under the table lock).
#[inline]
pub(super) fn key_added(key: &[u8]) {
    KEY_FILTER[bucket(key)].fetch_add(1, Ordering::SeqCst);
    TRACKED_KEYS.fetch_add(1, Ordering::SeqCst);
}

/// A key has left the global `key_clients` (call AFTER removing, under the
/// table lock).
#[inline]
pub(super) fn key_removed(key: &[u8]) {
    TRACKED_KEYS.fetch_sub(1, Ordering::SeqCst);
    KEY_FILTER[bucket(key)].fetch_sub(1, Ordering::SeqCst);
}

/// `n` BCAST registrations are about to be added (BEFORE, under the lock).
#[inline]
pub(super) fn prefixes_added(n: usize) {
    BCAST_PREFIXES.fetch_add(n, Ordering::SeqCst);
}

/// `n` BCAST registrations were removed (AFTER, under the lock).
#[inline]
pub(super) fn prefixes_removed(n: usize) {
    if n > 0 {
        BCAST_PREFIXES.fetch_sub(n, Ordering::SeqCst);
    }
}

/// Lock-free: could a write to `key` invalidate anything in the GLOBAL
/// table? `false` is authoritative; `true` means "take the lock and look".
#[inline]
pub(crate) fn global_may_track(key: &[u8]) -> bool {
    if BCAST_PREFIXES.load(Ordering::SeqCst) > 0 {
        return true;
    }
    TRACKED_KEYS.load(Ordering::SeqCst) > 0 && KEY_FILTER[bucket(key)].load(Ordering::SeqCst) > 0
}

/// Lock-free: the global table tracks no key and no prefix, so no write can
/// invalidate anything (the idle-tracker case, answered without hashing).
#[inline]
pub(crate) fn global_table_idle() -> bool {
    BCAST_PREFIXES.load(Ordering::SeqCst) == 0 && TRACKED_KEYS.load(Ordering::SeqCst) == 0
}

/// Test hooks: the raw counters, read by the bookkeeping tests below and in
/// `tracking::tests`.
#[cfg(test)]
pub(crate) fn bucket_count(key: &[u8]) -> u32 {
    KEY_FILTER[bucket(key)].load(Ordering::SeqCst)
}

#[cfg(test)]
pub(crate) fn prefix_count() -> usize {
    BCAST_PREFIXES.load(Ordering::SeqCst)
}
