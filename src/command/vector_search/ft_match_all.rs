//! `FT.SEARCH <index> "*"` on a VECTOR-only index (moon#695).
//!
//! moon#693 made a bare `*` the match-all query and routed it to the **inverted**
//! index, because that is where the document registry (`TextIndex::doc_id_to_key`)
//! lives. That covers every index with a TEXT, TAG or NUMERIC field, including
//! mixed VECTOR+TEXT schemas.
//!
//! An index built from VECTOR fields **alone** has no inverted index at all —
//! `ft_create` only constructs a `TextIndex` when the schema declares at least one
//! TEXT/TAG/NUMERIC field. So `*` fell through to the text engine, found no text
//! index, and answered `ERR no such index` for an index that `FT._LIST` happily
//! lists.
//!
//! The vector engine has its own registry: `VectorIndex::key_hash_to_key`. It is
//! **live** — inserted on index, pruned on delete/tombstone in the same function
//! body as the segment tombstone, so the two cannot disagree — and it is the exact
//! map the KNN path already uses to turn a hit back into its Redis key
//! (`build_search_response`). Enumerating it is therefore precisely as complete as
//! search's own key resolution: a document this misses is one KNN would report as
//! a synthetic `vec:<id>` rather than by name.
//!
//! ## Reply shape
//!
//! Byte-identical to the text `*` reply — `[total, key, ["__bm25_score",
//! "0.000000"], …]` — produced by the same builder. `*` means one thing, and a
//! client that handles it should not have to branch on whether the index happens
//! to carry a TEXT field. A mixed VECTOR+TEXT index already answers `*` in exactly
//! this shape, so matching it keeps the reply a function of the *query* rather than
//! of schema details the caller never asked about.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::text::store::{TextSearchResult, TextStore};
use crate::vector::store::VectorStore;

use super::ft_text_search::build_text_response_with_total;
use super::{extract_bulk, parse_limit_clause};

/// The match-all query string. Exactly `*` and nothing else: `*=>[KNN …]` is a
/// KNN query and never reaches here, because `is_text_query` rejects it on the
/// `[KNN` marker before any of this is consulted.
const MATCH_ALL: &[u8] = b"*";

/// `Some(index_name)` when these FT.SEARCH args are a match-all against an index
/// that only the VECTOR store knows about.
///
/// The routing sites need this *before* choosing between the local path and a
/// scatter, at a point where they hold both stores but have not yet run a query.
///
/// Deliberately ordered so the text store wins every tie: if a `TextIndex` exists
/// under this name the text engine owns the query and this returns `None`, leaving
/// mixed VECTOR+TEXT schemas on exactly the path they use today.
#[must_use]
pub fn vector_only_match_all_index(
    text_store: &TextStore,
    vector_store: &VectorStore,
    args: &[Frame],
    db_index: u8,
) -> Option<Bytes> {
    let index_name = extract_bulk(args.first()?)?;
    let query = extract_bulk(args.get(1)?)?;
    if query.as_ref() != MATCH_ALL {
        return None;
    }
    // `*` is also the leading token of a HYBRID or SPARSE query, whose retriever
    // clauses live in SEPARATE args and so are invisible to the query string.
    // Both are checked HERE rather than at the call sites: this predicate is
    // consulted from four routing sites, some of them ahead of the HYBRID parse,
    // and a gate that is only correct depending on where it is called from is a
    // gate that will eventually be called from the wrong place.
    if super::has_sparse_clause(args) {
        return None;
    }
    if !matches!(super::parse_hybrid_modifier(args), Ok(None)) {
        return None;
    }
    if text_store
        .get_index_for_db(index_name.as_ref(), db_index)
        .is_some()
    {
        return None;
    }
    vector_store.get_index_for_db(index_name.as_ref(), db_index)?;
    Some(index_name)
}

/// Enumerate one shard's live keys for `index_name`.
///
/// `reply[0]` is this shard's TRUE local count, independent of `offset`/`count`,
/// which is what `merge_text_results` sums to produce the global total (C4). Keys
/// are sorted so a given shard answers deterministically; across shards the merge
/// preserves shard order, since every score is equal and the merge sort is stable.
///
/// Allocation: this is a full enumeration by definition, and it sits at the
/// response-building end of the command path, where `Vec::with_capacity` is the
/// house rule rather than a hot-path violation.
#[must_use]
pub fn match_all_local(
    vector_store: &VectorStore,
    index_name: &[u8],
    db_index: u8,
    offset: usize,
    count: usize,
) -> Frame {
    let Some(idx) = vector_store.get_index_for_db(index_name, db_index) else {
        // Unreachable through the routing sites, which all gate on
        // `vector_only_match_all_index` first. Answering the same error the text
        // engine would keeps a direct caller honest instead of silently
        // reporting an empty index as a successful enumeration.
        return Frame::Error(Bytes::from_static(b"ERR no such index"));
    };

    let mut keys: Vec<Bytes> = idx.key_hash_to_key.iter().map(|(_, k)| k.clone()).collect();
    keys.sort_unstable();

    let total = keys.len();
    let results: Vec<TextSearchResult> = keys
        .into_iter()
        .enumerate()
        .map(|(i, key)| TextSearchResult {
            doc_id: u32::try_from(i).unwrap_or(u32::MAX),
            key,
            score: 0.0,
        })
        .collect();

    build_text_response_with_total(&results, total, offset, count)
}

/// Answer a vector-only `*` from this shard, or `None` to leave the query alone.
///
/// This is the whole fix at a single-shard / per-shard executor: gate, then
/// enumerate. Multi-shard coordination lives in `scatter_ft_match_all`, which
/// fans this same call out and merges the replies.
#[must_use]
pub fn try_match_all_vector_only(
    text_store: &TextStore,
    vector_store: &VectorStore,
    args: &[Frame],
    db_index: u8,
) -> Option<Frame> {
    let index_name = vector_only_match_all_index(text_store, vector_store, args, db_index)?;
    let (offset, count) = parse_limit_clause(args);
    Some(match_all_local(
        vector_store,
        index_name.as_ref(),
        db_index,
        offset,
        count,
    ))
}
