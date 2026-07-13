/// Mutable term-to-ID dictionary.
///
/// Maps string terms to sequential integer IDs for compact posting list
/// storage. Uses a simple HashMap for the mutable phase; will be replaced
/// with FST (finite state transducer) in Phase 151 for memory-efficient
/// immutable term storage and prefix/fuzzy queries.
use std::collections::HashMap;

/// Dictionary mapping terms to unique integer IDs.
///
/// IDs are assigned sequentially starting from 0. Once assigned, a term's
/// ID is stable for the lifetime of the dictionary.
/// Fixed per-entry `HashMap` bucket-overhead constant used to approximate
/// `TermDictionary::resident_bytes` (K4 accounting spine). Not exact --
/// `hashbrown`'s SwissTable control-byte layout is an implementation detail
/// -- but monotonic, matching `Database`'s own `entry_overhead`
/// approximation (WS6) and `graph::index::PropertyIndex::resident_bytes`'s
/// convention.
const MAP_ENTRY_OVERHEAD: usize = 48;

pub struct TermDictionary {
    terms: HashMap<String, u32>,
    next_id: u32,
    /// Term count at last FST build. Terms with id >= this value were added post-compaction.
    /// Used by the dual-path expansion strategy (D-12): FST covers ids < fst_high_water_mark,
    /// HashMap brute-force covers ids >= fst_high_water_mark.
    pub fst_high_water_mark: u32,
    /// K4 (P0 fix): O(1) cached total mirroring `resident_bytes()`.
    /// Maintained incrementally in `get_or_insert`'s new-term branch --
    /// `TermDictionary` has no deletion path (ids are stable for the
    /// dictionary's lifetime), so insertion is the only mutation site.
    resident_bytes: usize,
}

impl TermDictionary {
    /// Create an empty term dictionary.
    pub fn new() -> Self {
        Self {
            terms: HashMap::new(),
            next_id: 0,
            fst_high_water_mark: 0,
            resident_bytes: 0,
        }
    }

    /// Get the ID for a term, assigning a new ID if the term is not yet known.
    ///
    /// Returns the stable ID for this term.
    pub fn get_or_insert(&mut self, term: &str) -> u32 {
        if let Some(&id) = self.terms.get(term) {
            return id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.resident_bytes += term.len() + std::mem::size_of::<u32>() + MAP_ENTRY_OVERHEAD;
        self.terms.insert(term.to_owned(), id);
        id
    }

    /// Look up a term's ID without inserting.
    pub fn get(&self, term: &str) -> Option<u32> {
        self.terms.get(term).copied()
    }

    /// Reconstruct a `TermDictionary` from a persisted `(term, id)` pair set
    /// (kernel M4 / task #50: term-dict sidecar load path).
    ///
    /// This is the id-space-preserving counterpart to repeated
    /// `get_or_insert` calls: instead of assigning ids by first-encounter
    /// order (which is NOT reproducible across a restart because the
    /// keyspace rescan iterates `DashTable` hash-iteration order, not the
    /// original insertion order), every id is taken verbatim from the
    /// sidecar. `next_id` continues from the persisted high-water mark so
    /// terms discovered fresh by the post-load rescan get NEW, non-colliding
    /// ids rather than restarting from 0.
    ///
    /// Callers MUST validate the sidecar (magic/version/checksum) and
    /// reject anything malformed BEFORE calling this -- this constructor
    /// trusts its inputs (ids may be sparse or unsorted; both are fine,
    /// duplicates are rejected by returning `None` since a single term
    /// can't legitimately hold two persisted ids and a collision means the
    /// sidecar was corrupted between writes).
    #[must_use]
    pub fn from_pairs(
        pairs: Vec<(String, u32)>,
        next_id: u32,
        fst_high_water_mark: u32,
    ) -> Option<Self> {
        let mut terms = HashMap::with_capacity(pairs.len());
        let mut seen_ids: std::collections::HashSet<u32> =
            std::collections::HashSet::with_capacity(pairs.len());
        let mut resident_bytes = 0usize;
        let mut max_id_plus_one = 0u32;
        for (term, id) in pairs {
            if id >= next_id {
                // A persisted id can never reach/exceed the persisted
                // next_id counter -- that would mean the sidecar's own
                // invariant was violated when it was written.
                return None;
            }
            if !seen_ids.insert(id) {
                return None; // duplicate id -- corrupt sidecar
            }
            resident_bytes += term.len() + std::mem::size_of::<u32>() + MAP_ENTRY_OVERHEAD;
            max_id_plus_one = max_id_plus_one.max(id + 1);
            if terms.insert(term, id).is_some() {
                return None; // duplicate term -- corrupt sidecar
            }
        }
        if max_id_plus_one > next_id || fst_high_water_mark > next_id {
            return None; // internal inconsistency -- fail closed
        }
        Some(Self {
            terms,
            next_id,
            fst_high_water_mark,
            resident_bytes,
        })
    }

    /// Number of unique terms in the dictionary.
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }

    /// Iterate all (term, id) pairs for FST construction.
    ///
    /// No ordering guarantee — callers MUST sort before building an FST.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &u32)> {
        self.terms.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Current next_id value (number of terms ever assigned).
    pub fn next_id(&self) -> u32 {
        self.next_id
    }

    /// Approximate resident bytes: term string length + the `u32` id value
    /// plus a fixed per-entry `HashMap` bucket-overhead constant (K4
    /// accounting spine).
    ///
    /// K4 (P0 fix): O(1) cached read, maintained incrementally by
    /// `get_or_insert` -- this used to be an O(vocabulary size) walk called
    /// unconditionally every 100ms from the shard eviction tick, which does
    /// not scale with corpus size. See `resident_bytes_ground_truth`
    /// (`#[cfg(test)]`) for the equivalent full-walk formula this cached
    /// value must always match.
    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    /// Ground-truth full recompute of `resident_bytes()`, using the exact
    /// same fixed-cost formula as the incremental accumulator. Test-only.
    #[cfg(test)]
    pub(crate) fn resident_bytes_ground_truth(&self) -> usize {
        self.terms
            .keys()
            .map(|term| term.len() + std::mem::size_of::<u32>() + MAP_ENTRY_OVERHEAD)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resident_bytes_zero_when_empty() {
        let dict = TermDictionary::new();
        assert_eq!(dict.resident_bytes(), 0);
    }

    #[test]
    fn resident_bytes_grows_with_inserts() {
        let mut dict = TermDictionary::new();
        let before = dict.resident_bytes();
        dict.get_or_insert("hello");
        dict.get_or_insert("world");
        assert!(dict.resident_bytes() > before);
        // Re-inserting an existing term must not double-count.
        let after_dup = dict.resident_bytes();
        dict.get_or_insert("hello");
        assert_eq!(dict.resident_bytes(), after_dup);
    }

    /// K4 (P0 fix): RED-first — the O(1) incremental accumulator must never
    /// drift from a from-scratch ground-truth recompute.
    #[test]
    fn resident_bytes_matches_ground_truth_after_inserts() {
        let mut dict = TermDictionary::new();
        assert_eq!(dict.resident_bytes(), dict.resident_bytes_ground_truth());
        for term in ["alpha", "beta", "gamma", "alpha", "delta", "beta"] {
            dict.get_or_insert(term);
            assert_eq!(dict.resident_bytes(), dict.resident_bytes_ground_truth());
        }
    }

    /// K4 (P0 fix): `resident_bytes()` must be a pure O(1) load, not a walk.
    #[test]
    fn resident_bytes_is_o1_not_a_walk() {
        let mut dict = TermDictionary::new();
        for i in 0..50_000u32 {
            dict.get_or_insert(&format!("term-{i}"));
        }
        let start = std::time::Instant::now();
        for _ in 0..100_000 {
            std::hint::black_box(dict.resident_bytes());
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(200),
            "100k reads of resident_bytes() took {elapsed:?} -- looks like a walk, not O(1)"
        );
    }

    /// Kernel M4 (task #50): `from_pairs` reconstructs a dictionary whose
    /// ids/lookups/resident_bytes are indistinguishable from one built by
    /// live `get_or_insert` calls in the same order.
    #[test]
    fn from_pairs_roundtrips_equivalent_to_get_or_insert() {
        let mut live = TermDictionary::new();
        let id_a = live.get_or_insert("alpha");
        let id_b = live.get_or_insert("beta");
        let id_c = live.get_or_insert("gamma");
        live.fst_high_water_mark = live.next_id();

        let pairs: Vec<(String, u32)> = live.iter().map(|(t, &id)| (t.to_owned(), id)).collect();
        let restored = TermDictionary::from_pairs(pairs, live.next_id(), live.fst_high_water_mark)
            .expect("valid sidecar pairs must reconstruct");

        assert_eq!(restored.get("alpha"), Some(id_a));
        assert_eq!(restored.get("beta"), Some(id_b));
        assert_eq!(restored.get("gamma"), Some(id_c));
        assert_eq!(restored.next_id(), live.next_id());
        assert_eq!(restored.fst_high_water_mark, live.fst_high_water_mark);
        assert_eq!(restored.resident_bytes(), live.resident_bytes());
        assert_eq!(
            restored.resident_bytes(),
            restored.resident_bytes_ground_truth()
        );
    }

    /// A term whose id is not less than `next_id` violates the sidecar's
    /// own invariant -- fail closed (return `None`), never silently clamp.
    #[test]
    fn from_pairs_rejects_id_at_or_above_next_id() {
        let pairs = vec![("x".to_owned(), 5u32)];
        assert!(TermDictionary::from_pairs(pairs, 5, 0).is_none());
    }

    /// Two different terms claiming the same persisted id is a corrupt
    /// sidecar -- fail closed.
    #[test]
    fn from_pairs_rejects_duplicate_ids() {
        let pairs = vec![("x".to_owned(), 0u32), ("y".to_owned(), 0u32)];
        assert!(TermDictionary::from_pairs(pairs, 2, 0).is_none());
    }

    /// A `fst_high_water_mark` above `next_id` is internally inconsistent
    /// -- fail closed.
    #[test]
    fn from_pairs_rejects_hwm_above_next_id() {
        let pairs = vec![("x".to_owned(), 0u32)];
        assert!(TermDictionary::from_pairs(pairs, 1, 5).is_none());
    }

    #[test]
    fn from_pairs_empty_is_valid() {
        let restored = TermDictionary::from_pairs(Vec::new(), 0, 0).expect("empty is valid");
        assert_eq!(restored.term_count(), 0);
        assert_eq!(restored.next_id(), 0);
    }
}
