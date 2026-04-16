//! FT.DROPINDEX, FT.COMPACT, FT._LIST command handlers.

use bytes::Bytes;

use crate::protocol::{Frame, FrameVec};
use crate::vector::store::VectorStore;

use super::extract_bulk;

/// FT.DROPINDEX index_name
pub fn ft_dropindex(
    store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    args: &[Frame],
) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.DROPINDEX' command",
        ));
    }
    let name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };
    let vector_dropped = store.drop_index(&name);
    let text_dropped = text_store.drop_index(&name);
    if vector_dropped {
        crate::vector::metrics::decrement_indexes();
    }
    if vector_dropped || text_dropped {
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else {
        Frame::Error(Bytes::from_static(b"Unknown Index name"))
    }
}

/// FT.COMPACT index_name
///
/// Explicitly compacts the mutable segment into an immutable HNSW segment.
/// This converts brute-force O(n) search to HNSW O(log n) search.
/// Call after bulk insert, before search workload begins.
///
/// Also builds the FST (Finite State Transducer) term dictionary for the text
/// index with the same name (if one exists). The FST enables fuzzy/prefix query
/// expansion. FST data is persisted as a `.fst` sidecar file alongside the text
/// index metadata so it survives server restart.
pub fn ft_compact(
    store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    args: &[Frame],
) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.COMPACT' command",
        ));
    }
    let name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };

    // Vector compaction (may be a no-op if no vector index with this name).
    if let Some(idx) = store.get_index_mut(&name) {
        // FT.COMPACT is explicit user intent: compact unconditionally, ignoring threshold.
        // Without this, when compact_threshold >= mutable_len, FT.COMPACT silently no-ops,
        // leaving all vectors in brute-force mutable segment (O(n) search instead of HNSW O(log n)).
        // force_compact now compacts ALL fields (default + additional).
        idx.force_compact();
    }

    // Build FST for the text index with the same name (if one exists).
    // Single-threaded per-shard: swap is atomic, no locking needed (D-14).
    #[cfg(feature = "text-index")]
    if let Some(text_idx) = text_store.get_index_mut(name.as_ref()) {
        text_idx.build_fst();
        // Persist FST sidecar to disk so it survives server restart (FUZ-02).
        text_store.save_fst_sidecar_for_index(name.as_ref());
    }

    // Return OK if either a vector index or text index exists with this name.
    if store.get_index(&name).is_some() || text_store.get_index(name.as_ref()).is_some() {
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else {
        Frame::Error(Bytes::from_static(b"Unknown Index name"))
    }
}

/// FT._LIST
///
/// Returns an array of all index names. Compatible with the Redis
/// `FT._LIST` internal command used by tools and the Moon Console.
pub fn ft_list(store: &VectorStore) -> Frame {
    let names = store.index_names();
    let elements: Vec<Frame> = names
        .into_iter()
        .map(|n| Frame::BulkString(n.clone()))
        .collect();
    Frame::Array(FrameVec::from_vec(elements))
}
