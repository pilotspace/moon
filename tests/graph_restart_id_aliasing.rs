//! RED-first regression test: graph `NodeKey` aliasing across restart
//! (2026-07 PR #231 pre-merge review, Fix 1 / P0, data-corruption class).
//!
//! ## The hazard
//!
//! `slotmap::SlotMap` key allocation is deterministic: a fresh `MemGraph`'s
//! first `add_node()` always mints SlotMap index 0 / generation 1. CSR
//! segments persist `NodeMeta::external_id` = the pre-crash process's raw
//! `NodeKey` FFI bits (also index-0-based for a segment built from a fresh
//! `MemGraph`, as production commonly does at the first freeze).
//!
//! Historically, `recover_graph_store` re-seeded the post-recovery mutable
//! tier with ANOTHER fresh, unoffset `MemGraph::new(..)`, and `replay.rs`'s
//! AddNode replay loop called `mg.add_node()` unconditionally for every
//! replayed command. The first brand-new node replayed after a restart
//! could then mint the EXACT SAME SlotMap key as a loaded CSR segment's
//! first frozen node. `MergedNodeView` (`src/graph/view.rs`) checks the
//! mutable tier first, so the frozen node's properties would be silently
//! and permanently shadowed by the replayed node's properties -- a
//! same-key, different-identity collision, not a mere duplicate.
//!
//! ## The fix
//!
//! 1. Monotonic id cursors + floors (`src/graph/memgraph.rs`) --
//!    `recover_graph_store` (`src/graph/recovery.rs`) restores the persisted
//!    `next_node_id`/`next_edge_id` cursors from the manifest and raises the
//!    allocation floor past every loaded segment's `external_id`
//!    (`ensure_node_id_floor`), so freshly-minted keys can never numerically
//!    alias a loaded segment's rows. Replayed AddNode records re-materialize
//!    under their ORIGINAL logged id via `add_node_with_id` (which also
//!    raises the floor), preserving client-cached handles.
//! 2. A dedup skip in the AddNode replay loop (`src/graph/replay.rs`) -- if a
//!    replayed `node_id` is already resident in a loaded CSR segment (full
//!    history replay after AOF fold / WAL truncation of pre-freeze history
//!    left the record in place), reuse the CSR-derived identity instead of
//!    minting a new key, avoiding double enumeration of the same logical node.
//!
//! ## What this test proves
//!
//! (a) A brand-new node replayed after restart never receives a key that
//!     equals a loaded segment's `external_id`.
//! (b) `MergedNodeView` reads for the frozen node's id return the frozen
//!     node's own properties -- no shadowing by the replayed node.
//! (c) Replaying the frozen node's OWN AddNode record again (simulating
//!     un-truncated pre-freeze WAL history) does not double-enumerate it:
//!     `for_each_visible_node` still reports exactly the same node set.
//!
//! ## Confirming this is RED without the fix
//!
//! Reverting the cursor restore + floor raise in `recover_graph_store`
//! (recovery.rs) and the `node_map`-dedup skip in the AddNode replay loop
//! (replay.rs) back to:
//!   - `recover_graph_store` skipping `restore_id_cursors` /
//!     `ensure_node_id_floor` (fresh allocator, no floor), and
//!   - the replay loop unconditionally calling
//!     `mg.add_node(labels.clone(), properties.clone(), embedding.clone(), 0)`
//!     for every AddNode record (no `node_map.get(node_id)` dedup check, no
//!     `add_node_with_id` identity preservation)
//! reproduces exactly this failure: `test_restart_replay_does_not_alias_frozen_node`
//! fails with
//!   `assertion failed: replayed key must not equal the frozen node's key`
//! because the freshly-minted replay key (SlotMap index 0, generation 1) is
//! bit-for-bit identical to the frozen node's `external_id`, and the
//! subsequent property read for the frozen key returns the REPLAYED node's
//! property (222) instead of the frozen node's (111) -- i.e. shadowing.
#![cfg(feature = "graph")]

use bytes::Bytes;
use moon::graph::memgraph::MemGraph;
use moon::graph::recovery::{recover_graph_store, save_graph_store};
use moon::graph::replay::GraphReplayCollector;
use moon::graph::types::{NodeKey, PropertyValue};
use moon::graph::view::MergedNodeView;
use roaring::RoaringBitmap;
use slotmap::Key;
use smallvec::smallvec;
use tempfile::TempDir;

const GRAPH: &str = "g";
/// Marker property key used to distinguish the frozen node from replayed ones.
const MARKER_PROP: u16 = 50;

/// Build and freeze a single-node CSR segment. Using a brand-new
/// `MemGraph::new(..)` means the frozen node's `external_id` is guaranteed
/// to be the lowest possible SlotMap key (index 0, generation 1) -- exactly
/// the key a second, independently-fresh `MemGraph` would mint first. This
/// is the realistic worst case: production's very first freeze starts from
/// an empty `MemGraph` the same way.
fn freeze_one_node(marker_value: i64) -> moon::graph::csr::CsrSegment {
    let mut mg = MemGraph::new(100);
    let props = smallvec![(MARKER_PROP, PropertyValue::Int(marker_value))];
    mg.add_node(smallvec![9u16], props, None, 1);
    let frozen = mg.freeze().expect("freeze ok");
    moon::graph::csr::CsrSegment::from_frozen(frozen, 100).expect("csr ok")
}

fn addnode_args(node_id: u64, label: u16, marker_value: i64) -> Vec<Vec<u8>> {
    vec![
        GRAPH.as_bytes().to_vec(),
        node_id.to_string().into_bytes(),
        b"1".to_vec(),
        label.to_string().into_bytes(),
        b"1".to_vec(),
        MARKER_PROP.to_string().into_bytes(),
        b"i".to_vec(),
        marker_value.to_string().into_bytes(),
    ]
}

fn as_slices(args: &[Vec<u8>]) -> Vec<&[u8]> {
    args.iter().map(Vec::as_slice).collect()
}

fn empty_committed() -> RoaringBitmap {
    RoaringBitmap::new()
}

/// Enumerate every visible node key (mutable + immutable tiers) via
/// `MergedNodeView`, alongside its `MARKER_PROP` value.
fn visible_marked_nodes(view: &MergedNodeView) -> Vec<(NodeKey, Option<i64>)> {
    let committed = empty_committed();
    let mut out = Vec::new();
    view.for_each_visible_node(None, 0, 0, &committed, None, |key| {
        let marker = match view.property(key, MARKER_PROP) {
            Some(PropertyValue::Int(v)) => Some(v),
            _ => None,
        };
        out.push((key, marker));
    });
    out
}

#[test]
fn test_restart_replay_does_not_alias_frozen_node() {
    let dir = TempDir::new().expect("tmpdir");
    let shard_id = 0;

    // 1. Build + persist a store with one graph holding one frozen CSR
    //    segment (one node, marker=111).
    let mut store = moon::graph::store::GraphStore::new();
    store
        .create_graph(Bytes::from_static(GRAPH.as_bytes()), 64_000, 10)
        .expect("create graph");
    let csr = freeze_one_node(111);
    let frozen_external_id = csr.node_meta[0].external_id;
    store
        .get_graph_mut(GRAPH.as_bytes())
        .expect("graph exists")
        .segments
        .add_immutable(csr);
    save_graph_store(&store, dir.path(), shard_id).expect("save ok");

    // 2. "Restart": recover fresh from persistence. This is where
    //    `recover_graph_store` seeds the post-recovery mutable tier's key
    //    allocator watermark.
    let recovered = recover_graph_store(dir.path(), shard_id)
        .expect("io ok")
        .expect("result exists");
    let mut store = recovered.store;
    assert_eq!(recovered.segments_loaded, 1);

    // 3. Replay one brand-new AddNode record (node_id=999, marker=222) --
    //    simulating a WAL record for a node created after the last freeze,
    //    before the crash. This is the replay path that must never mint a
    //    key aliasing `frozen_external_id`.
    let mut collector = GraphReplayCollector::new();
    let args = addnode_args(999, 3, 222);
    assert!(collector.collect_command(b"GRAPH.ADDNODE", &as_slices(&args)));
    let replayed = collector.replay_into(&mut store);
    assert_eq!(replayed, 1, "exactly one AddNode command must replay");

    // 4. Inspect the post-replay mutable + immutable tiers directly. The
    // live mutable tier is `NamedGraph::write_buf` (replay's
    // `take_memgraph`/`put_memgraph` operate there); `segments.load()`
    // carries the immutable CSR list. `MergedNodeView` is built exactly the
    // way the query path builds it (write_buf + segment guard).
    let graph = store.get_graph(GRAPH.as_bytes()).expect("graph exists");
    let segs = graph.segments.load();
    let view = MergedNodeView::new(&graph.write_buf, &segs.immutable);

    // (a) No replayed key equals the frozen segment's external_id.
    let replayed_keys: Vec<NodeKey> = graph.write_buf.iter_nodes().map(|(k, _)| k).collect();
    assert_eq!(
        replayed_keys.len(),
        1,
        "exactly one node in the mutable tier"
    );
    let replayed_key = replayed_keys[0];
    let frozen_key = NodeKey::from(slotmap::KeyData::from_ffi(frozen_external_id));
    assert_ne!(
        replayed_key.data().as_ffi(),
        frozen_external_id,
        "replayed key must not equal the frozen node's key (the aliasing hazard)"
    );

    // (b) The frozen node's id still resolves to the FROZEN properties --
    // not shadowed by the replayed node.
    assert_eq!(
        view.property(frozen_key, MARKER_PROP),
        Some(PropertyValue::Int(111)),
        "frozen node's properties must not be shadowed by the replayed node"
    );
    assert_eq!(
        view.property(replayed_key, MARKER_PROP),
        Some(PropertyValue::Int(222)),
        "replayed node keeps its own properties"
    );

    // Both nodes visible, distinct, correctly attributed.
    let mut marks: Vec<Option<i64>> = visible_marked_nodes(&view)
        .into_iter()
        .map(|(_, m)| m)
        .collect();
    marks.sort();
    assert_eq!(
        marks,
        vec![Some(111), Some(222)],
        "both the frozen and replayed nodes must be visible with their own markers"
    );
}

#[test]
fn test_restart_replay_of_already_frozen_node_does_not_double_enumerate() {
    // (c) Full-history replay: the WAL still contains the AddNode record
    // for a node that is ALREADY resident in a loaded CSR segment (e.g.
    // AOF fold didn't truncate pre-freeze history). Replaying it again must
    // not create a second, duplicate live-tier entry.
    let dir = TempDir::new().expect("tmpdir");
    let shard_id = 0;

    let mut store = moon::graph::store::GraphStore::new();
    store
        .create_graph(Bytes::from_static(GRAPH.as_bytes()), 64_000, 10)
        .expect("create graph");
    let csr = freeze_one_node(111);
    // The frozen node's logical id, as it would appear in the WAL's
    // original AddNode record: the raw external_id (production encodes
    // GRAPH.ADDNODE's `node_id` field as the NodeKey's FFI bits).
    let frozen_external_id = csr.node_meta[0].external_id;
    store
        .get_graph_mut(GRAPH.as_bytes())
        .expect("graph exists")
        .segments
        .add_immutable(csr);
    save_graph_store(&store, dir.path(), shard_id).expect("save ok");

    let recovered = recover_graph_store(dir.path(), shard_id)
        .expect("io ok")
        .expect("result exists");
    let mut store = recovered.store;

    // Replay the SAME node's AddNode record again (un-truncated history),
    // using its persisted external_id as the WAL `node_id`.
    let mut collector = GraphReplayCollector::new();
    let args = addnode_args(frozen_external_id, 9, 111);
    assert!(collector.collect_command(b"GRAPH.ADDNODE", &as_slices(&args)));
    let replayed = collector.replay_into(&mut store);
    assert_eq!(
        replayed, 1,
        "the dedup-skipped AddNode still counts as replayed (identity preserved)"
    );

    let graph = store.get_graph(GRAPH.as_bytes()).expect("graph exists");
    let segs = graph.segments.load();

    assert_eq!(
        graph.write_buf.node_count(),
        0,
        "dedup skip must not insert an already-frozen node into the mutable tier"
    );

    let view = MergedNodeView::new(&graph.write_buf, &segs.immutable);
    let marks = visible_marked_nodes(&view);
    assert_eq!(
        marks.len(),
        1,
        "MATCH-scan count must equal distinct nodes -- no double enumeration"
    );
    assert_eq!(marks[0].1, Some(111));
}
