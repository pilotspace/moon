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
pub struct TermDictionary {
    terms: HashMap<String, u32>,
    next_id: u32,
    /// Term count at last FST build. Terms with id >= this value were added post-compaction.
    /// Used by the dual-path expansion strategy (D-12): FST covers ids < fst_high_water_mark,
    /// HashMap brute-force covers ids >= fst_high_water_mark.
    pub fst_high_water_mark: u32,
}

impl TermDictionary {
    /// Create an empty term dictionary.
    pub fn new() -> Self {
        Self {
            terms: HashMap::new(),
            next_id: 0,
            fst_high_water_mark: 0,
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
    /// accounting spine). Not exact -- `hashbrown`'s SwissTable control-byte
    /// layout is an implementation detail -- but monotonic and consistent
    /// with `Database`'s own `entry_overhead` approximation (WS6) and
    /// `graph::index::PropertyIndex::resident_bytes`'s `serialized_size()`
    /// convention. O(vocabulary size); called from the shard's 100ms tick,
    /// the same cadence `GraphStore::resident_bytes` already accepts for its
    /// own O(segment_count) walk.
    pub fn resident_bytes(&self) -> usize {
        const MAP_ENTRY_OVERHEAD: usize = 48;
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
}
