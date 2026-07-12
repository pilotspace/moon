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
}
