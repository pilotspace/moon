//! Session-scoped FT.SEARCH deduplication.
//!
//! Tracks which document keys have already been returned in a conversation session
//! using a standard sorted set (session key). On each search:
//! 1. Filter out results whose Redis keys are already in the session sorted set
//! 2. Record newly returned keys with timestamp scores via ZADD-equivalent
//!
//! The session key is a normal sorted set — DEL clears it, EXPIRE sets TTL.

use bytes::Bytes;
use smallvec::SmallVec;
use std::collections::HashMap;

use crate::storage::db::Database;
use crate::vector::types::SearchResult;

/// Filter search results, removing any whose Redis key already exists in the
/// session sorted set's member map.
///
/// - `results`: raw search results from the vector engine
/// - `session_members`: the member->score map from the session sorted set
/// - `key_hash_to_key`: mapping from key_hash to the original Redis key
///
/// Returns a new SmallVec containing only results not yet seen in this session.
/// Allocation of the result SmallVec is acceptable — this is end-of-command-path.
pub fn filter_session_results(
    results: &SmallVec<[SearchResult; 32]>,
    session_members: &HashMap<Bytes, f64>,
    key_hash_to_key: &HashMap<u64, Bytes>,
) -> SmallVec<[SearchResult; 32]> {
    if session_members.is_empty() {
        return results.clone();
    }
    let mut filtered = SmallVec::with_capacity(results.len());
    for r in results {
        // Resolve the key_hash to the original Redis key
        if let Some(redis_key) = key_hash_to_key.get(&r.key_hash) {
            if session_members.contains_key(redis_key) {
                continue; // already returned in this session
            }
        }
        filtered.push(r.clone());
    }
    filtered
}

/// Record newly returned results into the session sorted set.
///
/// For each result, resolves key_hash to Redis key and inserts into the session
/// sorted set with the given timestamp as score. This mimics ZADD without going
/// through command dispatch.
///
/// - `results`: the filtered results that will be returned to the client
/// - `db`: mutable Database reference for sorted set access
/// - `session_key`: the session sorted set key
/// - `key_hash_to_key`: mapping from key_hash to the original Redis key
/// - `timestamp`: f64 epoch seconds to use as the sorted set score
pub fn record_session_results(
    results: &SmallVec<[SearchResult; 32]>,
    db: &mut Database,
    session_key: &[u8],
    key_hash_to_key: &HashMap<u64, Bytes>,
    timestamp: f64,
) {
    if results.is_empty() {
        return;
    }

    // Collect keys to record first, then do the sorted set mutation
    let keys_to_record: SmallVec<[Bytes; 32]> = results
        .iter()
        .filter_map(|r| key_hash_to_key.get(&r.key_hash).cloned())
        .collect();

    if keys_to_record.is_empty() {
        return;
    }

    // get_or_create_sorted_set returns (&mut HashMap<Bytes,f64>, &mut BPTree)
    // We need to handle the case where the key exists but is wrong type
    let (members, tree) = match db.get_or_create_sorted_set(session_key) {
        Ok(pair) => pair,
        Err(_) => return, // wrong type — silently skip (don't crash on user error)
    };

    for key in keys_to_record {
        // Only insert if not already present (idempotent)
        if !members.contains_key(&key) {
            members.insert(key.clone(), timestamp);
            tree.insert(ordered_float::OrderedFloat(timestamp), key);
        }
    }
}
