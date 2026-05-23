//! FT.INVALIDATE_RANGE command handler.
//!
//! Wire shape:
//! ```text
//! FT.INVALIDATE_RANGE <index> <node_id_field> <node_id_value> <hlc_wall_field> <hlc_wall_lo> <hlc_wall_hi>
//! ```
//!
//! Returns: `(integer)` count of records deleted.
//!
//! ## Contract (per INTEGRATION-PLAN.md §3.3)
//!
//! This command is the primitive that Lunaris's `invalidate_range(node_id, hlc_lo..hlc_hi)`
//! calls when Helios's `helios-git` detects a force-push and needs to bulk-invalidate
//! stale recall after a rebase.
//!
//! ### Field-type preconditions
//! - `<node_id_field>` MUST be declared as `TAG` in the `FT.CREATE` schema.
//! - `<hlc_wall_field>` MUST be declared as `NUMERIC` in the `FT.CREATE` schema.
//! Without correct schema declarations, the bitmap intersect returns empty and 0 is returned.
//!
//! ### Deletion semantics
//! Documents matching the compound filter are hard-deleted from the text index:
//! all inverted posting entries, TAG bitmap entries, NUMERIC BTreeMap entries,
//! and per-doc metadata are removed. This is an eager delete — not a tombstone.
//!
//! ### Version token
//! `text_version_token` on the TextStore is bumped ONCE per successful invocation
//! (including zero-match invocations), using `Ordering::Release`. This ensures that
//! Lunaris's poll of `FT.INFO text_version_token` will see an advance even for
//! force-pushes that touch no indexed documents (the force-push still advanced
//! wall-clock state; consumers need to re-poll).
//! Only parse / range / index errors skip the bump.
//!
//! ### Error replies
//! - `WRONGTYPE` — index does not exist.
//! - `SYNTAX` — wrong number of arguments.
//! - `RANGE` — `hlc_wall_lo > hlc_wall_hi`.

use bytes::Bytes;

use crate::command::vector_search::extract_bulk;
use crate::protocol::Frame;
use crate::text::store::TextStore;

/// Handle `FT.INVALIDATE_RANGE <index> <node_id_field> <node_id_value> <hlc_wall_field> <hlc_wall_lo> <hlc_wall_hi>`.
///
/// Deletes all documents in `index` where:
/// - `node_id_field` TAG value equals `node_id_value`, AND
/// - `hlc_wall_field` NUMERIC value is in `[hlc_wall_lo, hlc_wall_hi]`.
///
/// Returns the count of deleted documents as a RESP integer.
///
/// See module-level documentation for the Lunaris integration contract.
#[cfg(feature = "text-index")]
pub fn ft_invalidate_range(text_store: &mut TextStore, args: &[Frame]) -> Frame {
    todo!("W2-M2 GREEN not yet implemented")
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "text-index"))]
mod tests {
    use super::*;
    use bytes::Bytes;

    use crate::command::vector_search::ft_create::ft_create;
    use crate::protocol::{Frame, FrameVec};
    use crate::text::store::TextStore;
    use crate::vector::store::VectorStore;

    fn bulk(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::from(s.to_vec()))
    }

    /// Build a TextStore with one index `idx` having schema:
    /// - `node_id` TAG
    /// - `hlc_wall` NUMERIC
    /// - `content` TEXT
    ///
    /// Then insert `n` docs: doc `i` has
    /// - `node_id = "node:1"` (or "node:2" for even i)
    /// - `hlc_wall = i` (as f64)
    /// - `content = "hello world"`
    fn make_store_with_docs(n: u64) -> TextStore {
        let mut vs = VectorStore::new();
        let mut ts = TextStore::new();

        // FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA node_id TAG hlc_wall NUMERIC content TEXT
        let create_args = vec![
            bulk(b"idx"),
            bulk(b"ON"),
            bulk(b"HASH"),
            bulk(b"PREFIX"),
            bulk(b"1"),
            bulk(b"doc:"),
            bulk(b"SCHEMA"),
            bulk(b"node_id"),
            bulk(b"TAG"),
            bulk(b"hlc_wall"),
            bulk(b"NUMERIC"),
            bulk(b"content"),
            bulk(b"TEXT"),
        ];
        ft_create(&mut vs, &mut ts, &create_args);

        // Insert docs
        for i in 0..n {
            let key = format!("doc:{i}");
            let key_bytes = Bytes::from(key.clone());
            let node_id = if i % 2 == 0 { "node:1" } else { "node:2" };
            let hlc_str = format!("{}", i);

            // Replicate auto_index_hset logic: call index_document + tag_index_document + numeric_index_document.
            use xxhash_rust::xxh64::xxh64;
            let key_hash = xxh64(key_bytes.as_ref(), 0);

            let hset_args = vec![
                bulk(b"node_id"),
                bulk(node_id.as_bytes()),
                bulk(b"hlc_wall"),
                bulk(hlc_str.as_bytes()),
                bulk(b"content"),
                bulk(b"hello world"),
            ];

            let idx = ts.get_index_mut(b"idx").expect("index exists");
            idx.index_document(key_hash, key_bytes.as_ref(), &hset_args);
            idx.tag_index_document(key_hash, key_bytes.as_ref(), &hset_args);
            idx.numeric_index_document(key_hash, key_bytes.as_ref(), &hset_args);
        }

        ts
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Happy path: 8 docs, node:1 is docs 0,2,4,6 (hlc 0,2,4,6);
    //             invalidate_range node:1 hlc [2..4] → deletes docs 2 and 4 → count=2.
    // After: FT.SEARCH node:1 sees docs 0 and 6 only.
    // ─────────────────────────────────────────────────────────────────────────
    #[test]
    fn invalidate_range_deletes_matching_docs() {
        let mut ts = make_store_with_docs(8);

        let args = vec![
            bulk(b"idx"),
            bulk(b"node_id"),
            bulk(b"node:1"),
            bulk(b"hlc_wall"),
            bulk(b"2"),
            bulk(b"4"),
        ];
        let result = ft_invalidate_range(&mut ts, &args);
        assert_eq!(
            result,
            Frame::Integer(2),
            "should delete docs with hlc_wall in [2,4] for node:1 (docs 2 and 4)"
        );

        // Verify remaining docs: node:1 docs are 0 and 6 (hlc 0 and 6).
        let idx = ts.get_index(b"idx").expect("index still exists");
        // doc with hlc 2 must be gone
        let still_has_doc2 = idx.doc_id_to_key.values().any(|k| k.as_ref() == b"doc:2");
        assert!(!still_has_doc2, "doc:2 should have been deleted");
        let still_has_doc4 = idx.doc_id_to_key.values().any(|k| k.as_ref() == b"doc:4");
        assert!(!still_has_doc4, "doc:4 should have been deleted");
        // doc:0 and doc:6 still present
        assert!(idx.doc_id_to_key.values().any(|k| k.as_ref() == b"doc:0"));
        assert!(idx.doc_id_to_key.values().any(|k| k.as_ref() == b"doc:6"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Empty range — query that matches no documents returns 0.
    // ─────────────────────────────────────────────────────────────────────────
    #[test]
    fn invalidate_range_empty_returns_zero() {
        let mut ts = make_store_with_docs(4);

        // Range [100, 200] — no documents have hlc_wall in that range.
        let args = vec![
            bulk(b"idx"),
            bulk(b"node_id"),
            bulk(b"node:1"),
            bulk(b"hlc_wall"),
            bulk(b"100"),
            bulk(b"200"),
        ];
        let result = ft_invalidate_range(&mut ts, &args);
        assert_eq!(result, Frame::Integer(0), "no docs in range → returns 0");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Version token bumps on every successful invocation, including 0-match.
    // The bump is exactly 1 regardless of deletion count.
    // ─────────────────────────────────────────────────────────────────────────
    #[test]
    fn invalidate_range_bumps_version_token() {
        let mut ts = make_store_with_docs(4);

        let before = ts.version_token();

        // Zero-match invocation still bumps.
        let args_zero = vec![
            bulk(b"idx"),
            bulk(b"node_id"),
            bulk(b"node:1"),
            bulk(b"hlc_wall"),
            bulk(b"999"),
            bulk(b"1000"),
        ];
        ft_invalidate_range(&mut ts, &args_zero);
        let after_zero = ts.version_token();
        assert_eq!(
            after_zero,
            before + 1,
            "zero-match invocation must still bump token by exactly 1"
        );

        // Non-zero-match invocation also bumps.
        let args_match = vec![
            bulk(b"idx"),
            bulk(b"node_id"),
            bulk(b"node:1"),
            bulk(b"hlc_wall"),
            bulk(b"0"),
            bulk(b"2"),
        ];
        ft_invalidate_range(&mut ts, &args_match);
        let after_match = ts.version_token();
        assert_eq!(
            after_match,
            after_zero + 1,
            "non-zero-match invocation must bump token by exactly 1"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Malformed args: wrong number of arguments.
    // ─────────────────────────────────────────────────────────────────────────
    #[test]
    fn invalidate_range_invalid_args_returns_syntax_error() {
        let mut ts = TextStore::new();

        // Too few args (only 3 instead of 6)
        let args = vec![bulk(b"idx"), bulk(b"node_id"), bulk(b"node:1")];
        let result = ft_invalidate_range(&mut ts, &args);
        match result {
            Frame::Error(e) => {
                let s = std::str::from_utf8(&e).unwrap_or("");
                assert!(
                    s.contains("SYNTAX") || s.contains("wrong number"),
                    "expected SYNTAX error, got: {s}"
                );
            }
            other => panic!("expected SYNTAX error, got {other:?}"),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // lo > hi: RANGE error.
    // ─────────────────────────────────────────────────────────────────────────
    #[test]
    fn invalidate_range_lo_gt_hi_returns_range_error() {
        let mut ts = make_store_with_docs(4);

        let args = vec![
            bulk(b"idx"),
            bulk(b"node_id"),
            bulk(b"node:1"),
            bulk(b"hlc_wall"),
            bulk(b"10"),  // lo
            bulk(b"5"),   // hi < lo
        ];
        let result = ft_invalidate_range(&mut ts, &args);
        match result {
            Frame::Error(e) => {
                let s = std::str::from_utf8(&e).unwrap_or("");
                assert!(
                    s.contains("RANGE") || s.contains("lo"),
                    "expected RANGE error, got: {s}"
                );
            }
            other => panic!("expected RANGE error, got {other:?}"),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Non-existent index: WRONGTYPE error.
    // ─────────────────────────────────────────────────────────────────────────
    #[test]
    fn invalidate_range_nonexistent_index_returns_wrongtype() {
        let mut ts = TextStore::new(); // empty — no indexes

        let args = vec![
            bulk(b"doesnotexist"),
            bulk(b"node_id"),
            bulk(b"node:1"),
            bulk(b"hlc_wall"),
            bulk(b"0"),
            bulk(b"100"),
        ];
        let result = ft_invalidate_range(&mut ts, &args);
        match result {
            Frame::Error(e) => {
                let s = std::str::from_utf8(&e).unwrap_or("");
                assert!(
                    s.contains("WRONGTYPE") || s.contains("no such index") || s.contains("Unknown"),
                    "expected WRONGTYPE error, got: {s}"
                );
            }
            other => panic!("expected WRONGTYPE error, got {other:?}"),
        }
    }
}
