//! PERF-07 RED test: prove `--initial-keyspace-hint` eliminates
//! `split_segment` cost on production keyspaces.
//!
//! Source: .planning/milestones/v0.1.12-REQUIREMENTS.md PERF-07.
//!
//! Run with:
//!   cargo test --release --test perf_v0112_pre_size_dashtable -- --nocapture

use bytes::Bytes;

use moon::storage::compact_key::CompactKey;
use moon::storage::dashtable::DashTable;
use moon::storage::db::Database;
use moon::storage::entry::Entry;

const PRE_SIZED_KEYS: usize = 1_000_000;
const DEFAULT_SIZED_KEYS: usize = 100_000;

/// Verify that DashTable-level pre-sizing produces zero splits for 1M inserts.
/// This isolates the hash table from the Database wrapper.
#[test]
fn test_pre_sized_dashtable_zero_splits_under_1m_inserts() {
    let mut table: DashTable<CompactKey, String> = DashTable::with_capacity(PRE_SIZED_KEYS);
    assert_eq!(table.split_count(), 0);

    for i in 0..PRE_SIZED_KEYS {
        table.insert(CompactKey::from(format!("pre_{:08}", i)), format!("v{}", i));
    }

    assert_eq!(
        table.len(),
        PRE_SIZED_KEYS,
        "Lost inserts during pre-sized load"
    );
    assert_eq!(
        table.split_count(),
        0,
        "Pre-sized DashTable performed {} splits; expected 0. \
         Hint sizing formula in DashTable::with_capacity is wrong, \
         OR LOAD_THRESHOLD changed without updating the formula.",
        table.split_count()
    );
}

/// Same test through Database::set to verify the full stack.
#[test]
fn test_pre_sized_database_zero_splits_under_1m_inserts() {
    let mut db = Database::with_capacity(PRE_SIZED_KEYS);
    // Sanity: pre-sized allocation must NOT pre-charge the split counter.
    assert_eq!(db.data().split_count(), 0);

    for i in 0..PRE_SIZED_KEYS {
        let key = Bytes::from(format!("pre_{:08}", i));
        let entry = Entry::new_string(Bytes::from(format!("v{}", i)));
        db.set(key, entry);
    }

    assert_eq!(
        db.data().len(),
        PRE_SIZED_KEYS,
        "Lost inserts during pre-sized load"
    );
    assert_eq!(
        db.data().split_count(),
        0,
        "Pre-sized DashTable performed {} splits; expected 0. \
         Hint sizing formula in DashTable::with_capacity is wrong, \
         OR LOAD_THRESHOLD changed without updating the formula.",
        db.data().split_count()
    );
}

#[test]
fn test_default_sized_database_does_split() {
    // Regression guard: if this test stops splitting, the pre-sized
    // test above becomes meaningless (a no-op match).
    let mut db = Database::new();
    assert_eq!(db.data().split_count(), 0);

    for i in 0..DEFAULT_SIZED_KEYS {
        let key = Bytes::from(format!("def_{:08}", i));
        let entry = Entry::new_string(Bytes::from(format!("v{}", i)));
        db.set(key, entry);
    }

    assert_eq!(db.data().len(), DEFAULT_SIZED_KEYS);
    assert!(
        db.data().split_count() > 100,
        "Default DashTable expected to split heavily under {} inserts; \
         got {} splits. If LOAD_THRESHOLD or initial size changed, \
         the pre-sized test's claim is now untestable.",
        DEFAULT_SIZED_KEYS,
        db.data().split_count()
    );
}
