//! WS1 (2026-09 performance review) regression tests for the storage core.
//!
//! Kept out of `db/mod.rs` (already far past the 1500-line guideline) so each
//! issue's red/green pins live together:
//! - moon#1159 — `Database::set` builds no owned key on an overwrite.

use bytes::Bytes;

use crate::storage::dashtable::take_upsert_key_builds;
use crate::storage::db::Database;
use crate::storage::entry::Entry;

// ── moon#1159 ────────────────────────────────────────────────────────────

/// `Database::set` on an EXISTING key must not construct an owned key: the
/// DashTable upsert is keyed by the borrowed slice and builds a `CompactKey`
/// only on a miss. Before moon#1159's follow-up every SET overwrite of a key
/// longer than 23 bytes allocated a heap key block and dropped it again.
#[test]
fn set_overwrite_builds_no_owned_key() {
    let mut db = Database::new();
    let key: &[u8] = b"a-long-key-that-does-not-fit-inline:0001";
    let _ = take_upsert_key_builds();
    db.set(key, Entry::new_string(Bytes::from_static(b"v1")));
    assert_eq!(
        take_upsert_key_builds(),
        1,
        "a new key stores one owned key"
    );
    for _ in 0..50 {
        db.set(key, Entry::new_string(Bytes::from_static(b"v2")));
    }
    assert_eq!(
        take_upsert_key_builds(),
        0,
        "SET overwrite built an owned key per call"
    );
    assert_eq!(
        db.get(key).and_then(|e| e.value.as_bytes_owned()),
        Some(Bytes::from_static(b"v2"))
    );
    assert_eq!(db.len(), 1);
}
