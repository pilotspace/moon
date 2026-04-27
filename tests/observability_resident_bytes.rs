//! Smoke test: every subsystem reports nonzero `resident_bytes()` after a
//! representative load. This is the foundation contract for OBS-03/04/05.
//!
//! Run with:
//!   cargo test --test observability_resident_bytes -- --nocapture
//!
//! Feature combinations tested:
//!   cargo test --no-default-features --features runtime-tokio,jemalloc --test observability_resident_bytes
//!   cargo test --no-default-features --features runtime-tokio,jemalloc,graph,text-index --test observability_resident_bytes

use bytes::Bytes;

// ---- Database + DashTable ----

use moon::storage::db::Database;
use moon::storage::entry::Entry;

#[test]
fn database_resident_bytes_zero_when_empty() {
    let db = Database::new();
    assert_eq!(db.resident_bytes(), 0, "empty database must report 0 bytes");
}

#[test]
fn database_resident_bytes_grows_after_set() {
    let mut db = Database::new();
    db.set(
        Bytes::from_static(b"hello"),
        Entry::new_string(Bytes::from_static(b"world")),
    );
    assert!(
        db.resident_bytes() > 0,
        "Database::resident_bytes must grow after SET"
    );
}

#[test]
fn database_resident_bytes_consistent_with_estimated_memory() {
    let mut db = Database::new();
    db.set(
        Bytes::from_static(b"k"),
        Entry::new_string(Bytes::from_static(b"v")),
    );
    assert_eq!(
        db.resident_bytes(),
        db.estimated_memory(),
        "resident_bytes must equal estimated_memory (same underlying counter)"
    );
}

// ---- VectorStore ----

use moon::vector::store::VectorStore;

#[test]
fn vector_store_resident_bytes_zero_when_empty() {
    let vs = VectorStore::new();
    let (m, i) = vs.resident_bytes();
    assert_eq!(m, 0, "empty VectorStore mutable must be 0");
    assert_eq!(i, 0, "empty VectorStore immutable must be 0");
}

// ---- WAL Writer ----

use moon::persistence::wal_v3::record::WalRecordType;
use moon::persistence::wal_v3::segment::WalWriterV3;

#[test]
fn wal_writer_resident_bytes_after_append() {
    let dir = tempfile::tempdir().expect("failed to create tempdir");
    let mut writer =
        WalWriterV3::new(0, dir.path(), 1024 * 1024).expect("failed to create WAL writer");

    // The buffer is pre-allocated to 8192 bytes.
    let before = writer.resident_bytes();
    assert!(
        before > 0,
        "WAL writer should have pre-allocated buffer capacity, got {before}"
    );

    // Append a record and verify bytes are still tracked.
    writer.append(
        WalRecordType::Command,
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
    );
    let after = writer.resident_bytes();
    assert!(
        after >= before,
        "WAL resident_bytes must not shrink after append: before={before}, after={after}"
    );
}

// ---- Replication Backlog ----

use moon::replication::backlog::ReplicationBacklog;
use moon::replication::state::ReplicationState;

#[test]
fn replication_backlog_resident_bytes_after_append() {
    let mut backlog = ReplicationBacklog::new(4096);
    // VecDeque pre-allocates capacity.
    let initial = backlog.resident_bytes();
    assert!(
        initial > 0,
        "ReplicationBacklog should have pre-allocated capacity, got {initial}"
    );

    backlog.append(b"some replication data payload");
    let after = backlog.resident_bytes();
    assert!(
        after >= initial,
        "resident_bytes must not shrink: initial={initial}, after={after}"
    );
}

#[test]
fn replication_state_backlog_resident_bytes_lazy_init() {
    let state = ReplicationState::new(2, "replid1".into(), "replid2".into());

    // Before any replica handshake, backlogs are None -> 0 bytes.
    assert_eq!(
        state.backlog_resident_bytes(),
        0,
        "uninitialized backlogs must report 0"
    );

    // Allocate backlogs (simulates replica handshake).
    state.ensure_backlogs_allocated(1024);

    let after = state.backlog_resident_bytes();
    assert!(
        after > 0,
        "backlog_resident_bytes must be >0 after allocation, got {after}"
    );
}

// ---- GraphStore (feature-gated) ----

#[cfg(feature = "graph")]
mod graph_tests {
    use bytes::Bytes;
    use moon::graph::store::GraphStore;
    use smallvec::smallvec;

    #[test]
    fn graph_store_resident_bytes_zero_when_empty() {
        let gs = GraphStore::new();
        assert_eq!(
            gs.resident_bytes(),
            0,
            "empty GraphStore must report 0 bytes"
        );
    }

    #[test]
    fn graph_store_resident_bytes_grows_after_node_insert() {
        let mut gs = GraphStore::new();
        let lsn = gs.allocate_lsn();
        gs.create_graph(Bytes::from_static(b"test_graph"), 1000, lsn)
            .expect("create_graph should succeed");

        // Insert a node to trigger SlotMap allocation.
        let graph = gs
            .get_graph_mut(b"test_graph")
            .expect("graph must exist after create");
        let node_lsn = 1;
        graph
            .write_buf
            .add_node(smallvec![0u16], smallvec![], None, node_lsn);

        let bytes = gs.resident_bytes();
        assert!(
            bytes > 0,
            "GraphStore::resident_bytes must be >0 after inserting a node, got {bytes}"
        );
    }
}
