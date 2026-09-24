//! KNN filter evaluation with the BM25 plane reachable (moon#1194).
//!
//! Under the opt-in schema-aware payload policy (`MOON_VECTOR_PAYLOAD_SCHEMA=
//! declared`, see `vector::filter::payload_schema`) a declared TEXT field is
//! not copied into the vector index's payload text index — the index's BM25
//! plane already holds it. A `@field:{words}` (`TextMatch`) KNN filter on such
//! a field is answered here instead: the words go through that field's own
//! BM25 analyzer (stop words, NOSTEM, stemming), every document holding all
//! remaining terms is taken from the BM25 postings, and each document's key
//! is mapped to the vector global id the payload filter bitmaps speak.
//! Without a policy this is exactly `PayloadIndex::evaluate_bitmap`.

use roaring::RoaringBitmap;

use crate::text::store::TextStore;
use crate::vector::filter::FilterExpr;
use crate::vector::store::VectorIndex;

/// Evaluate `filter` for `idx`, routing BM25-owned `TextMatch` nodes to
/// `text_store`'s index of the same name (db-scoped).
pub(super) fn evaluate_filter(
    idx: &VectorIndex,
    filter: &FilterExpr,
    total_vectors: u32,
    text_store: Option<&TextStore>,
    db_index: u8,
) -> RoaringBitmap {
    if idx.payload_index.schema().is_none() {
        let _ = (text_store, db_index);
        return idx.payload_index.evaluate_bitmap(filter, total_vectors);
    }
    #[cfg(feature = "text-index")]
    {
        let resolve = |field: &bytes::Bytes, terms: &[bytes::Bytes]| {
            bm25_text_match(idx, text_store?, db_index, field, terms)
        };
        idx.payload_index
            .evaluate_bitmap_with(filter, total_vectors, Some(&resolve))
    }
    #[cfg(not(feature = "text-index"))]
    {
        let _ = (text_store, db_index);
        idx.payload_index.evaluate_bitmap(filter, total_vectors)
    }
}

/// Global ids of the documents whose declared TEXT `field` holds every term
/// of `terms` after that field's BM25 analysis. `None` when the index has no
/// BM25 plane or no such TEXT field.
#[cfg(feature = "text-index")]
fn bm25_text_match(
    idx: &VectorIndex,
    text_store: &TextStore,
    db_index: u8,
    field: &bytes::Bytes,
    terms: &[bytes::Bytes],
) -> Option<RoaringBitmap> {
    let text = text_store.get_index_for_db(&idx.meta.name, db_index)?;
    let fidx = text
        .text_fields
        .iter()
        .position(|f| f.field_name == *field)?;
    let mut query = String::new();
    for t in terms {
        if let Ok(s) = std::str::from_utf8(t) {
            if !query.is_empty() {
                query.push(' ');
            }
            query.push_str(s);
        }
    }
    let analyzed: Vec<String> = text.field_analyzers[fidx]
        .tokenize_with_positions(&query)
        .into_iter()
        .map(|(term, _)| term)
        .collect();
    let mut out = RoaringBitmap::new();
    if analyzed.is_empty() {
        return Some(out);
    }
    let docs = text.num_docs() as usize;
    for hit in text.search_field(fidx, &analyzed, None, None, docs.max(1)) {
        let key_hash = xxhash_rust::xxh64::xxh64(&hit.key, 0);
        if let Some(&gid) = idx.key_hash_to_global_id.get(&key_hash) {
            out.insert(gid);
        }
    }
    Some(out)
}
