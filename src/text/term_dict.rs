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
}

impl TermDictionary {
    /// Create an empty term dictionary.
    pub fn new() -> Self {
        Self {
            terms: HashMap::new(),
            next_id: 0,
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
}
