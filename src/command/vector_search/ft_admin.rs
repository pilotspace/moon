//! FT.DROPINDEX, FT.COMPACT, FT._LIST command handlers.

use bytes::Bytes;

use crate::protocol::{Frame, FrameVec};
use crate::storage::db::Database;
use crate::vector::store::VectorStore;

use super::extract_bulk;

/// FT.DROPINDEX index_name [DD]
///
/// Drops a vector or text index. With the optional DD flag, also deletes
/// all documents that were indexed by the dropped index.
///
/// # Arguments
/// - `store`: The VectorStore containing vector indexes
/// - `text_store`: The TextStore containing text indexes
/// - `db`: Optional database reference (required when DD flag is used)
/// - `args`: Command arguments: `[index_name]` or `[index_name, DD]`
///
/// # Returns
/// - `OK` on success
/// - Error if index doesn't exist or wrong number of arguments
pub fn ft_dropindex(
    store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    db: Option<&mut Database>,
    args: &[Frame],
) -> Frame {
    // Accept 1 argument (index_name) or 2 arguments (index_name DD)
    if args.is_empty() || args.len() > 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.DROPINDEX' command",
        ));
    }

    let name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };

    // Parse DD flag (case-insensitive)
    let delete_docs = args.len() == 2
        && matches!(&args[1], Frame::BulkString(b) if b.eq_ignore_ascii_case(b"DD"));

    // If DD flag provided but something other than "DD", reject as invalid
    if args.len() == 2 && !delete_docs {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.DROPINDEX' command",
        ));
    }

    // Check if index exists before attempting deletion
    let vector_exists = store.get_index(&name).is_some();
    let text_exists = text_store.get_index(name.as_ref()).is_some();

    if !vector_exists && !text_exists {
        return Frame::Error(Bytes::from_static(b"Unknown Index name"));
    }

    // If DD flag is set, collect and delete all indexed documents BEFORE dropping the index
    if delete_docs {
        let Some(db) = db else {
            return Frame::Error(Bytes::from_static(b"ERR DD flag requires database access"));
        };

        // Collect keys from vector index (key_hash_to_key maps hash -> original key bytes)
        let mut keys_to_delete: Vec<Bytes> = Vec::new();
        if let Some(idx) = store.get_index(&name) {
            keys_to_delete.extend(idx.key_hash_to_key.values().cloned());
        }

        // Collect keys from text index (doc_id_to_key maps doc_id -> original key bytes)
        // Deduplicate with vector keys to avoid double-deletion
        if let Some(idx) = text_store.get_index(name.as_ref()) {
            for key in idx.doc_id_to_key.values() {
                if !keys_to_delete.iter().any(|k| k == key) {
                    keys_to_delete.push(key.clone());
                }
            }
        }

        // Delete all collected keys from the database
        for key in &keys_to_delete {
            db.remove(key);
        }
    }

    // Drop the index metadata (existing logic)
    let vector_dropped = store.drop_index(&name);
    let text_dropped = text_store.drop_index(&name);

    if vector_dropped {
        crate::vector::metrics::decrement_indexes();
    }

    // We already verified at least one index exists, so this should always succeed
    if vector_dropped || text_dropped {
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else {
        // Shouldn't reach here given the existence check above, but handle defensively
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
