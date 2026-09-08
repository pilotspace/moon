//! Boot-time reconciliation of text indexes loaded from `.tpost`.
//!
//! The text half of `vector::persistence::recover_v2::RecoveryState`: for
//! every keyspace hash that matches a LOADED index, compare the stored
//! per-doc content checksum with one computed over the live hash. Equal →
//! the doc is skipped (its postings are already right). Different or unknown
//! → the caller re-indexes it from the live hash. After the walk, `finish`
//! removes every loaded doc whose key was never observed (the deletion
//! probe — `DEL` never unindexes text, the boot rebuild used to be what
//! dropped deleted keys, and this keeps that guarantee) and recomputes the
//! per-field stats so the result equals a rebuild.
//!
//! Indexes that were NOT loaded are untouched here: the walk indexes every
//! matching key into them, as before.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use smallvec::SmallVec;

use crate::protocol::Frame;
use crate::text::store::TextStore;

#[derive(Debug, Default, Clone, Copy)]
struct Counters {
    loaded_docs: usize,
    verified_unchanged: usize,
    re_indexed: usize,
    removed: usize,
}

/// Per-shard state of the text plane's boot reconcile. `Default` = nothing
/// loaded, every method a no-op.
#[derive(Debug, Default)]
pub struct TextRecoveryState {
    loaded: HashMap<Bytes, Counters>,
    observed: HashMap<Bytes, HashSet<u64>>,
}

impl TextRecoveryState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an index whose postings came from disk.
    pub fn mark_loaded(&mut self, name: Bytes, loaded_docs: usize) {
        self.loaded.insert(
            name.clone(),
            Counters {
                loaded_docs,
                ..Counters::default()
            },
        );
        self.observed
            .insert(name, HashSet::with_capacity(loaded_docs));
    }

    #[must_use]
    pub fn loaded_count(&self) -> usize {
        self.loaded.len()
    }

    /// One keyspace hash. `args` is the full `[key, field, value, ...]`
    /// frame list the rescan builds (the key at `[0]`, exactly what
    /// `auto_index_hset` receives). Returns the loaded indexes for which the
    /// doc is verified unchanged — the caller skips text indexing for those.
    #[must_use]
    pub fn reconcile(
        &mut self,
        text_store: &TextStore,
        key: &[u8],
        args: &[Frame],
        db_index: u8,
    ) -> SmallVec<[Bytes; 4]> {
        let mut unchanged: SmallVec<[Bytes; 4]> = SmallVec::new();
        if self.loaded.is_empty() {
            return unchanged;
        }
        let matching = text_store.find_matching_index_names_for_db(key, db_index);
        if matching.is_empty() {
            return unchanged;
        }
        let key_hash = xxhash_rust::xxh64::xxh64(key, 0);
        let text_args = if args.is_empty() { args } else { &args[1..] };
        for name in matching {
            let Some(counters) = self.loaded.get_mut(&name) else {
                continue;
            };
            if let Some(seen) = self.observed.get_mut(&name) {
                seen.insert(key_hash);
            }
            let Some(idx) = text_store.get_index(&name) else {
                continue;
            };
            let stored = idx.stored_content_checksum(key_hash);
            let live = idx.content_checksum(text_args);
            if stored == Some(live) {
                counters.verified_unchanged += 1;
                unchanged.push(name);
            } else {
                counters.re_indexed += 1;
            }
        }
        unchanged
    }

    /// Deletion probe + stats recompute + one summary line per loaded index.
    pub fn finish(self, text_store: &mut TextStore) {
        for (name, mut c) in self.loaded {
            let observed = self.observed.get(&name);
            if let Some(idx) = text_store.get_index_mut(&name) {
                let stale: Vec<u32> = idx
                    .key_hash_to_doc_id
                    .iter()
                    .filter(|(kh, _)| !observed.is_some_and(|o| o.contains(kh)))
                    .map(|(_, &doc_id)| doc_id)
                    .collect();
                for doc_id in &stale {
                    idx.remove_doc_by_doc_id(*doc_id);
                }
                c.removed = stale.len();
                idx.recompute_field_stats();
            }
            tracing::info!(
                "text index {}: loaded {} doc(s) from .tpost, {} verified unchanged, {} re-indexed, {} removed",
                String::from_utf8_lossy(&name),
                c.loaded_docs,
                c.verified_unchanged,
                c.re_indexed,
                c.removed,
            );
        }
        text_store.sweep_orphan_postings_files();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text::store::TextIndex;
    use crate::text::types::{BM25Config, TextFieldDef};

    fn frames(key: &str, pairs: &[(&str, &str)]) -> Vec<Frame> {
        let mut v = vec![Frame::BulkString(Bytes::copy_from_slice(key.as_bytes()))];
        for (f, val) in pairs {
            v.push(Frame::BulkString(Bytes::copy_from_slice(f.as_bytes())));
            v.push(Frame::BulkString(Bytes::copy_from_slice(val.as_bytes())));
        }
        v
    }

    /// A store with one "loaded" index holding docs t:0 and t:1, stamped the
    /// way `auto_index_hset` stamps them.
    fn store_with_loaded_index() -> (TextStore, TextRecoveryState) {
        let mut store = TextStore::new();
        let idx = TextIndex::new(
            Bytes::from_static(b"ix"),
            vec![Bytes::from_static(b"t:")],
            vec![TextFieldDef::new(Bytes::from_static(b"body"))],
            BM25Config::default(),
        );
        store.create_index(Bytes::from_static(b"ix"), idx).unwrap();
        let idx = store.get_index_mut(b"ix").unwrap();
        for (k, body) in [("t:0", "alpha beta"), ("t:1", "gamma delta")] {
            let args = frames(k, &[("body", body)]);
            let kh = xxhash_rust::xxh64::xxh64(k.as_bytes(), 0);
            idx.index_document(kh, k.as_bytes(), &args[1..]);
            idx.record_content_checksum(kh, &args[1..]);
        }
        let mut rec = TextRecoveryState::new();
        rec.mark_loaded(Bytes::from_static(b"ix"), 2);
        (store, rec)
    }

    #[test]
    fn unchanged_doc_is_verified_and_skipped() {
        let (store, mut rec) = store_with_loaded_index();
        let args = frames("t:0", &[("body", "alpha beta"), ("extra", "ignored")]);
        let unchanged = rec.reconcile(&store, b"t:0", &args, 0);
        assert_eq!(unchanged.as_slice(), &[Bytes::from_static(b"ix")]);
        assert_eq!(rec.loaded[b"ix".as_slice()].verified_unchanged, 1);
    }

    #[test]
    fn changed_missing_or_unstamped_doc_is_re_indexed() {
        let (mut store, mut rec) = store_with_loaded_index();
        // changed value
        let args = frames("t:0", &[("body", "alpha CHANGED")]);
        assert!(rec.reconcile(&store, b"t:0", &args, 0).is_empty());
        // unknown key
        let args = frames("t:9", &[("body", "whatever")]);
        assert!(rec.reconcile(&store, b"t:9", &args, 0).is_empty());
        // known key, stamp missing (indexed by a path that never stamped it)
        let idx = store.get_index_mut(b"ix").unwrap();
        let kh = xxhash_rust::xxh64::xxh64(b"t:1", 0);
        let doc = idx.key_hash_to_doc_id[&kh];
        idx.doc_id_to_content_checksum.remove(&doc);
        let args = frames("t:1", &[("body", "gamma delta")]);
        assert!(rec.reconcile(&store, b"t:1", &args, 0).is_empty());
        assert_eq!(rec.loaded[b"ix".as_slice()].re_indexed, 3);
    }

    #[test]
    fn an_index_that_was_not_loaded_is_never_touched() {
        let (store, mut rec) = store_with_loaded_index();
        rec.loaded.clear();
        rec.observed.clear();
        let args = frames("t:0", &[("body", "alpha beta")]);
        assert!(rec.reconcile(&store, b"t:0", &args, 0).is_empty());
    }

    #[test]
    fn finish_removes_docs_whose_key_was_never_observed_and_recomputes_stats() {
        let (mut store, mut rec) = store_with_loaded_index();
        let args = frames("t:0", &[("body", "alpha beta")]);
        let _ = rec.reconcile(&store, b"t:0", &args, 0);
        // t:1 is never observed by the walk (deleted key) -> probe removes it
        rec.finish(&mut store);
        let idx = store.get_index(b"ix").unwrap();
        assert_eq!(idx.num_docs(), 1);
        assert!(
            idx.search_field(0, &["gamma".to_owned()], None, None, 10)
                .is_empty()
        );
        assert_eq!(
            idx.search_field(0, &["alpha".to_owned()], None, None, 10)
                .len(),
            1
        );
        assert_eq!(idx.field_stats[0].num_docs, 1);
        assert_eq!(idx.field_stats[0].total_field_length, 2);
    }
}
