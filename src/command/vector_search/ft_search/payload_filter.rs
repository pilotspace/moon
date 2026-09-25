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
///
/// moon#1228: membership only. The terms' postings are intersected rarest
/// first into a bitmap, restricted to documents that resolve to a key, and
/// each key is hashed in place. HEAD ran the BM25 scorer over every matching
/// document (`search_field` with `top_k = num_docs`) and cloned each hit's key
/// into a result vector just to hash it — the scores were thrown away.
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
    let mut out = RoaringBitmap::new();
    for doc in bm25_text_docs(text, fidx, terms).iter() {
        let Some(key) = text.doc_id_to_key.get(&doc) else {
            continue;
        };
        let key_hash = xxhash_rust::xxh64::xxh64(key, 0);
        if let Some(&gid) = idx.key_hash_to_global_id.get(&key_hash) {
            out.insert(gid);
        }
    }
    Some(out)
}

/// Text doc ids of field `fidx` holding every analysed term of `terms` (AND),
/// restricted to documents that resolve to a key — exactly the set
/// `search_field` scores. Empty when the words analyse to nothing (all stop
/// words) or any term is absent from the field.
#[cfg(feature = "text-index")]
fn bm25_text_docs(
    text: &crate::text::store::TextIndex,
    fidx: usize,
    terms: &[bytes::Bytes],
) -> RoaringBitmap {
    let mut query = String::new();
    for t in terms {
        if let Ok(s) = std::str::from_utf8(t) {
            if !query.is_empty() {
                query.push(' ');
            }
            query.push_str(s);
        }
    }
    let mut postings: smallvec::SmallVec<[&crate::text::posting::PostingList; 8]> =
        smallvec::SmallVec::new();
    for (term, _) in text.field_analyzers[fidx].tokenize_with_positions(&query) {
        let posting = text.field_term_dicts[fidx]
            .get(&term)
            .and_then(|id| text.field_postings[fidx].get_posting(id));
        match posting {
            Some(p) => postings.push(p),
            None => return RoaringBitmap::new(), // AND: one absent term matches nothing
        }
    }
    if postings.is_empty() {
        return RoaringBitmap::new();
    }
    text.restrict_to_live(crate::text::score::intersect_rarest_first(&postings))
}

#[cfg(all(test, feature = "text-index"))]
mod tests {
    use bytes::Bytes;

    use crate::protocol::Frame;
    use crate::text::store::TextIndex;
    use crate::text::types::{BM25Config, TextFieldDef};

    /// moon#1228 result identity: the bitmap resolver selects exactly the
    /// documents HEAD's scoring resolver did (`search_field` over every
    /// document), for single, multi, repeated, stop-word, absent and
    /// cross-case terms — including after deletions and reused doc ids.
    #[test]
    fn bitmap_resolver_matches_the_scoring_resolver() {
        let mut idx = TextIndex::new(
            Bytes::from_static(b"t"),
            vec![Bytes::from_static(b"d:")],
            vec![TextFieldDef::new(Bytes::from_static(b"body"))],
            BM25Config::default(),
        );
        let words = ["red", "apple", "pie", "green", "pear", "running", "the"];
        let index = |idx: &mut TextIndex, d: u32| {
            let mut body = String::new();
            for (i, w) in words.iter().enumerate() {
                if (d as usize * (i + 2) + i) % 3 != 0 {
                    body.push_str(w);
                    body.push(' ');
                }
            }
            let key = format!("d:{d}");
            idx.index_document(
                u64::from(d) + 1,
                key.as_bytes(),
                &[
                    Frame::BulkString(Bytes::from_static(b"body")),
                    Frame::BulkString(Bytes::from(body)),
                ],
            );
        };
        for d in 0..400u32 {
            index(&mut idx, d);
        }
        // Holes and a reused id.
        for d in (0..400u32).step_by(7) {
            let doc_id = idx.key_hash_to_doc_id[&(u64::from(d) + 1)];
            idx.remove_doc_by_doc_id(doc_id);
        }
        index(&mut idx, 7);
        let queries: [&[&str]; 8] = [
            &["red"],
            &["red", "apple"],
            &["APPLE", "pie", "red"],
            &["red", "red"],
            &["the", "pear"],
            &["runs"],
            &["absent", "red"],
            &["the"],
        ];
        let mut non_empty = 0;
        for q in queries {
            let terms: Vec<Bytes> = q
                .iter()
                .map(|w| Bytes::copy_from_slice(w.as_bytes()))
                .collect();
            let got = super::bm25_text_docs(&idx, 0, &terms);
            // HEAD's resolver, verbatim: analyse, then score every document.
            let joined = q.join(" ");
            let analyzed: Vec<String> = idx.field_analyzers[0]
                .tokenize_with_positions(&joined)
                .into_iter()
                .map(|(t, _)| t)
                .collect();
            let want: roaring::RoaringBitmap = if analyzed.is_empty() {
                roaring::RoaringBitmap::new()
            } else {
                idx.search_field(0, &analyzed, None, None, (idx.num_docs() as usize).max(1))
                    .into_iter()
                    .map(|r| r.doc_id)
                    .collect()
            };
            assert_eq!(got, want, "{q:?}");
            non_empty += usize::from(!want.is_empty());
        }
        assert!(non_empty >= 4, "fixture must exercise real matches");
    }
}
