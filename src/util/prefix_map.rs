//! Prefix -> index-name map: O(key length) "which indexes cover this key?"
//! lookups, independent of how many indexes exist.
//!
//! Both `VectorStore` and `TextStore` answered that question by walking
//! every index and testing every one of its prefixes with `starts_with` —
//! O(indexes) per key, on the HSET/DEL hot path and, worse, once per key of
//! the whole keyspace in the boot-time index rescan. A store with 4,191 text
//! and 2,793 vector indexes paid ~7k prefix compares per key: for a 2.2M-key
//! restart that alone is ~15 billion compares before a single document is
//! re-indexed.
//!
//! A key `k` matches prefix `p` iff `p == k[..p.len()]`, so the set of
//! prefixes that match `k` is a subset of `k`'s own leading slices. Hashing
//! each leading slice `k[..i]` for `i in 0..=min(k.len(), longest prefix)`
//! against a `prefix -> names` table answers the question in O(key length)
//! hash probes, regardless of index count.
//!
//! Semantics are kept exactly as the two stores had them:
//! - a prefix list that is EMPTY never matches in the vector store (an index
//!   with no prefixes is unreachable by HSET), while the text store treats it
//!   as "match every key" — the caller picks via [`PrefixMap::insert`] vs
//!   [`PrefixMap::insert_match_all`];
//! - an empty-STRING prefix `""` matches every key in both (it is `k[..0]`);
//! - an index is reported at most once per key even when several of its
//!   prefixes match.

use std::collections::HashMap;

use bytes::Bytes;
use smallvec::SmallVec;

/// Index names sharing one prefix. Almost always exactly one.
type Names = SmallVec<[Bytes; 1]>;

/// Reverse map from key prefix to the index names declared with it.
#[derive(Debug, Default, Clone)]
pub struct PrefixMap {
    by_prefix: HashMap<Bytes, Names>,
    /// Indexes registered with [`Self::insert_match_all`]: they match every key.
    match_all: Vec<Bytes>,
    /// Longest registered prefix: bounds the probe loop so a 1 KB key does
    /// not pay 1 K hash probes when every prefix is 8 bytes long.
    max_len: usize,
}

impl PrefixMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `name` under each of `prefixes`. An empty `prefixes` slice
    /// registers nothing (the index will never match — vector-store rule).
    pub fn insert(&mut self, name: &Bytes, prefixes: &[Bytes]) {
        for p in prefixes {
            let names = self.by_prefix.entry(p.clone()).or_default();
            if !names.iter().any(|n| n == name) {
                names.push(name.clone());
            }
            self.max_len = self.max_len.max(p.len());
        }
    }

    /// Register `name` as matching every key (text-store rule for an index
    /// declared with no prefixes).
    pub fn insert_match_all(&mut self, name: &Bytes) {
        if !self.match_all.iter().any(|n| n == name) {
            self.match_all.push(name.clone());
        }
    }

    /// Unregister `name` from `prefixes` (and from the match-all list).
    /// `max_len` is left as is: it is only an upper bound on the probe loop.
    pub fn remove(&mut self, name: &[u8], prefixes: &[Bytes]) {
        for p in prefixes {
            if let Some(names) = self.by_prefix.get_mut(p) {
                names.retain(|n| n.as_ref() != name);
                if names.is_empty() {
                    self.by_prefix.remove(p);
                }
            }
        }
        self.match_all.retain(|n| n.as_ref() != name);
    }

    /// Names of every index with at least one prefix covering `key`, each
    /// reported once. Order is unspecified (the walks this replaces iterated
    /// a `HashMap`, so callers never depended on order).
    pub fn matching(&self, key: &[u8]) -> Vec<Bytes> {
        let mut out: Vec<Bytes> = Vec::new();
        self.for_each_matching(key, |n| {
            if !out.iter().any(|o| o == n) {
                out.push(n.clone());
            }
        });
        out
    }

    /// `true` when any registered index covers `key` — the allocation-free
    /// form for callers that only need the predicate.
    pub fn any_matching(&self, key: &[u8]) -> bool {
        if !self.match_all.is_empty() {
            return true;
        }
        if self.by_prefix.is_empty() {
            return false;
        }
        let upto = key.len().min(self.max_len);
        (0..=upto).any(|i| self.by_prefix.contains_key(&key[..i]))
    }

    fn for_each_matching<'a>(&'a self, key: &[u8], mut f: impl FnMut(&'a Bytes)) {
        for n in &self.match_all {
            f(n);
        }
        if self.by_prefix.is_empty() {
            return;
        }
        let upto = key.len().min(self.max_len);
        for i in 0..=upto {
            if let Some(names) = self.by_prefix.get(&key[..i]) {
                for n in names {
                    f(n);
                }
            }
        }
    }

    /// Number of distinct registered prefixes (diagnostics/tests).
    pub fn prefix_count(&self) -> usize {
        self.by_prefix.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn sorted(mut v: Vec<Bytes>) -> Vec<Bytes> {
        v.sort();
        v
    }

    /// Brute-force oracle: the exact walk both stores used to do.
    fn oracle(indexes: &[(Bytes, Vec<Bytes>, bool)], key: &[u8]) -> Vec<Bytes> {
        let mut out = Vec::new();
        for (name, prefixes, match_all_when_empty) in indexes {
            let hit = if prefixes.is_empty() {
                *match_all_when_empty
            } else {
                prefixes.iter().any(|p| key.starts_with(p))
            };
            if hit {
                out.push(name.clone());
            }
        }
        sorted(out)
    }

    #[test]
    fn matches_agree_with_brute_force_walk() {
        let indexes: Vec<(Bytes, Vec<Bytes>, bool)> = vec![
            (b("users"), vec![b("user:")], false),
            (b("user_tags"), vec![b("user:"), b("tag:")], false),
            (b("all_text"), vec![], true), // text-store: no prefix = everything
            (b("never"), vec![], false),   // vector-store: no prefix = nothing
            (b("root"), vec![b("")], false), // empty-string prefix = everything
            (b("deep"), vec![b("user:profile:")], false),
            (b("dup"), vec![b("a"), b("ab"), b("abc")], false), // several prefixes match one key
        ];
        let mut map = PrefixMap::new();
        for (name, prefixes, match_all) in &indexes {
            if prefixes.is_empty() && *match_all {
                map.insert_match_all(name);
            } else {
                map.insert(name, prefixes);
            }
        }
        for key in [
            &b"user:1"[..],
            b"user:profile:7",
            b"tag:x",
            b"abc:1",
            b"ab",
            b"a",
            b"",
            b"zzz",
            b"user",
        ] {
            assert_eq!(
                sorted(map.matching(key)),
                oracle(&indexes, key),
                "key {:?}",
                String::from_utf8_lossy(key)
            );
            assert_eq!(
                map.any_matching(key),
                !oracle(&indexes, key).is_empty(),
                "any_matching for key {:?}",
                String::from_utf8_lossy(key)
            );
        }
    }

    #[test]
    fn each_index_reported_once_even_with_overlapping_prefixes() {
        let mut map = PrefixMap::new();
        map.insert(&b("dup"), &[b("a"), b("ab"), b("abc")]);
        assert_eq!(map.matching(b"abcd"), vec![b("dup")]);
    }

    #[test]
    fn remove_unregisters_every_prefix_and_match_all() {
        let mut map = PrefixMap::new();
        map.insert(&b("x"), &[b("p:"), b("q:")]);
        map.insert(&b("y"), &[b("p:")]);
        map.insert_match_all(&b("everything"));
        map.remove(b"x", &[b("p:"), b("q:")]);
        assert_eq!(sorted(map.matching(b"p:1")), vec![b("everything"), b("y")]);
        assert!(map.matching(b"q:1") == vec![b("everything")]);
        map.remove(b"everything", &[]);
        assert!(map.matching(b"q:1").is_empty());
        assert_eq!(
            map.prefix_count(),
            1,
            "q: was dropped when its last name left"
        );
    }

    #[test]
    fn empty_map_matches_nothing_without_probing() {
        let map = PrefixMap::new();
        assert!(map.matching(b"anything").is_empty());
        assert!(!map.any_matching(b"anything"));
    }
}
