#[cfg(feature = "text-index")]
use fst::automaton::Str;
/// FST-based term dictionary for fuzzy and prefix search.
///
/// Provides three expansion functions:
/// - `build_fst_from_term_dict`: Build a sorted FST Map from a TermDictionary
/// - `expand_fuzzy`: Expand a query term to matching term IDs via Levenshtein automaton
/// - `expand_prefix`: Expand a prefix to matching term IDs via FST Str automaton
/// - `expand_fuzzy_hashmap`: Brute-force fuzzy scan for post-compaction terms
/// - `expand_prefix_hashmap`: Brute-force prefix scan for post-compaction terms
///
/// All public functions require the `text-index` feature flag.
#[cfg(feature = "text-index")]
use fst::{Automaton, IntoStreamer, Map, MapBuilder, Streamer};
#[cfg(feature = "text-index")]
use levenshtein_automata::LevenshteinAutomatonBuilder;

#[cfg(feature = "text-index")]
use crate::text::posting::PostingStore;
#[cfg(feature = "text-index")]
use crate::text::term_dict::TermDictionary;

/// Build a sorted FST Map from a TermDictionary.
///
/// Collects all (term, term_id) pairs from the dictionary, sorts them
/// lexicographically (required by FST), and builds the map in memory.
///
/// Returns `Vec<u8>` — the raw FST bytes suitable for `fst::Map::new()`.
///
/// # Errors
/// Returns `fst::Error` if the FST builder encounters an issue (e.g., out-of-order
/// keys — the `debug_assert!` catches this in debug mode).
#[cfg(feature = "text-index")]
pub fn build_fst_from_term_dict(dict: &TermDictionary) -> Result<Vec<u8>, fst::Error> {
    // Collect all (term, term_id) pairs; sort BEFORE build (FST requires lexicographic order).
    let mut pairs: Vec<(String, u64)> = dict
        .iter()
        .map(|(term, &id)| (term.to_owned(), id as u64))
        .collect();
    pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    // Debug assertion catches accidental unsorted builds.
    debug_assert!(
        pairs.windows(2).all(|w| w[0].0 <= w[1].0),
        "FST pairs must be sorted lexicographically"
    );

    let mut builder = MapBuilder::memory();
    for (term, id) in &pairs {
        builder.insert(term.as_str(), *id)?;
    }
    builder.into_inner()
}

/// Expand a query term to matching term IDs via Levenshtein automaton (FST path).
///
/// Uses `levenshtein_automata::LevenshteinAutomatonBuilder` which implements
/// `fst::Automaton` via the `fst_automaton` feature — preferred over fst's
/// built-in Levenshtein which has memory issues (D-04).
///
/// Results are sorted by document frequency descending and capped at `max_terms`
/// to prevent query explosion on common short terms (D-09: max 50).
///
/// # Arguments
/// * `fst_map` — The built FST Map for this field
/// * `term` — The (lowercased, NFKD-normalized, NOT stemmed) query term
/// * `distance` — Levenshtein edit distance (1, 2, or 3 max per D-03)
/// * `postings` — The field's PostingStore for doc frequency lookup
/// * `max_terms` — Hard cap on expanded terms (50 per D-09)
#[cfg(feature = "text-index")]
pub fn expand_fuzzy(
    fst_map: &Map<Vec<u8>>,
    term: &str,
    distance: u8,
    postings: &PostingStore,
    max_terms: usize,
) -> Vec<u32> {
    // levenshtein_automata DFA implements fst::Automaton via "fst_automaton" feature.
    // true = allow transpositions (Damerau-Levenshtein variant).
    let builder = LevenshteinAutomatonBuilder::new(distance, true);
    let dfa = builder.build_dfa(term);

    let mut expanded: Vec<(u32, u32)> = Vec::new(); // (doc_freq, term_id)
    let mut stream = fst_map.search(&dfa).into_stream();
    while let Some((_key, term_id)) = stream.next() {
        let id = term_id as u32;
        let df = postings.doc_freq(id);
        expanded.push((df, id));
    }

    // Sort by doc_freq descending, cap at max_terms (D-09).
    expanded.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    expanded.truncate(max_terms);
    expanded.into_iter().map(|(_, id)| id).collect()
}

/// Expand a prefix to matching term IDs via FST Str automaton (FST path).
///
/// Uses `fst::automaton::Str::new(prefix).starts_with()` to stream all
/// dictionary terms beginning with the given prefix.
///
/// Results are sorted by document frequency descending and capped at `max_terms`.
///
/// # Arguments
/// * `fst_map` — The built FST Map for this field
/// * `prefix` — The prefix string (lowercased, NFKD-normalized, NOT stemmed)
/// * `postings` — The field's PostingStore for doc frequency lookup
/// * `max_terms` — Hard cap on expanded terms (50 per D-09)
#[cfg(feature = "text-index")]
pub fn expand_prefix(
    fst_map: &Map<Vec<u8>>,
    prefix: &str,
    postings: &PostingStore,
    max_terms: usize,
) -> Vec<u32> {
    let aut = Str::new(prefix).starts_with();
    let mut expanded: Vec<(u32, u32)> = Vec::new();
    let mut stream = fst_map.search(aut).into_stream();
    while let Some((_key, term_id)) = stream.next() {
        let id = term_id as u32;
        let df = postings.doc_freq(id);
        expanded.push((df, id));
    }

    expanded.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    expanded.truncate(max_terms);
    expanded.into_iter().map(|(_, id)| id).collect()
}

/// Brute-force fuzzy expansion for post-compaction terms (HashMap path).
///
/// Scans only TermDictionary entries with `id >= high_water_mark` — these are
/// terms added after the last FST build and therefore NOT in the FST (D-12).
///
/// When `high_water_mark == 0` (no FST built yet), scans the entire dictionary.
///
/// Results are sorted by document frequency descending and capped at `max_terms`.
///
/// # Arguments
/// * `dict` — The TermDictionary to scan
/// * `term` — The query term to match against
/// * `distance` — Maximum Levenshtein edit distance
/// * `postings` — The field's PostingStore for doc frequency lookup
/// * `high_water_mark` — Only scan terms with id >= this value
/// * `max_terms` — Hard cap on expanded terms
#[cfg(feature = "text-index")]
pub fn expand_fuzzy_hashmap(
    dict: &TermDictionary,
    term: &str,
    distance: u8,
    postings: &PostingStore,
    high_water_mark: u32,
    max_terms: usize,
) -> Vec<u32> {
    let dist = distance as usize;
    let mut expanded: Vec<(u32, u32)> = Vec::new();

    for (candidate, &id) in dict.iter() {
        if id < high_water_mark {
            continue;
        }
        if levenshtein_distance(term, candidate) <= dist {
            let df = postings.doc_freq(id);
            expanded.push((df, id));
        }
    }

    expanded.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    expanded.truncate(max_terms);
    expanded.into_iter().map(|(_, id)| id).collect()
}

/// Brute-force prefix expansion for post-compaction terms (HashMap path).
///
/// Scans only TermDictionary entries with `id >= high_water_mark` — these are
/// terms added after the last FST build and therefore NOT in the FST (D-12).
///
/// When `high_water_mark == 0` (no FST built yet), scans the entire dictionary.
///
/// Results are sorted by document frequency descending and capped at `max_terms`.
///
/// # Arguments
/// * `dict` — The TermDictionary to scan
/// * `prefix` — The prefix string to match against
/// * `postings` — The field's PostingStore for doc frequency lookup
/// * `high_water_mark` — Only scan terms with id >= this value
/// * `max_terms` — Hard cap on expanded terms
#[cfg(feature = "text-index")]
pub fn expand_prefix_hashmap(
    dict: &TermDictionary,
    prefix: &str,
    postings: &PostingStore,
    high_water_mark: u32,
    max_terms: usize,
) -> Vec<u32> {
    let mut expanded: Vec<(u32, u32)> = Vec::new();

    for (candidate, &id) in dict.iter() {
        if id < high_water_mark {
            continue;
        }
        if candidate.starts_with(prefix) {
            let df = postings.doc_freq(id);
            expanded.push((df, id));
        }
    }

    expanded.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    expanded.truncate(max_terms);
    expanded.into_iter().map(|(_, id)| id).collect()
}

/// Compute Levenshtein edit distance between two strings.
///
/// Uses a standard O(m*n) dynamic programming algorithm.
/// Both strings are treated as sequences of Unicode scalar values (chars).
///
/// This is only used for the HashMap brute-force fallback path on
/// post-compaction terms. For FST traversal, the `levenshtein_automata`
/// DFA handles distances efficiently.
#[cfg(feature = "text-index")]
fn levenshtein_distance(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();

    // Edge cases
    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }

    // Use two rows to save memory (standard optimization).
    let mut prev: Vec<usize> = (0..=n).collect();
    let mut curr: Vec<usize> = vec![0; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a_chars[i - 1] == b_chars[j - 1] {
                0
            } else {
                1
            };
            curr[j] = (prev[j] + 1) // deletion
                .min(curr[j - 1] + 1) // insertion
                .min(prev[j - 1] + cost); // substitution
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[n]
}

#[cfg(test)]
#[cfg(feature = "text-index")]
mod tests {
    use super::*;
    use crate::text::posting::PostingStore;
    use crate::text::term_dict::TermDictionary;

    #[test]
    fn test_build_fst_roundtrip() {
        let mut dict = TermDictionary::new();
        let id_a = dict.get_or_insert("apple");
        let id_b = dict.get_or_insert("banana");
        let id_c = dict.get_or_insert("cherry");

        let bytes = build_fst_from_term_dict(&dict).expect("build FST");
        let map = fst::Map::new(bytes).expect("load FST");

        assert_eq!(map.get("apple"), Some(id_a as u64));
        assert_eq!(map.get("banana"), Some(id_b as u64));
        assert_eq!(map.get("cherry"), Some(id_c as u64));
        assert_eq!(map.get("durian"), None);
    }

    #[test]
    fn test_expand_fuzzy_distance1() {
        // FST contains "machin" (stemmed form of "machine").
        // Expand "machn" (edit distance 1 from "machin") — should find "machin".
        let mut dict = TermDictionary::new();
        let id = dict.get_or_insert("machin");
        let bytes = build_fst_from_term_dict(&dict).expect("build FST");
        let map = fst::Map::new(bytes).expect("load FST");

        let mut ps = PostingStore::new();
        ps.add_term_occurrence(id, 0, None);

        let results = expand_fuzzy(&map, "machn", 1, &ps, 50);
        assert!(
            results.contains(&id),
            "machin should be found with distance=1 from machn"
        );
    }

    #[test]
    fn test_expand_fuzzy_distance2() {
        // FST contains "machin". Expand "machne" (edit distance 2 from "machin").
        // machne -> machin: substitute 'n'->'i', insert 'n' = 2 edits
        let mut dict = TermDictionary::new();
        let id = dict.get_or_insert("machin");
        let bytes = build_fst_from_term_dict(&dict).expect("build FST");
        let map = fst::Map::new(bytes).expect("load FST");

        let mut ps = PostingStore::new();
        ps.add_term_occurrence(id, 0, None);

        let results = expand_fuzzy(&map, "machne", 2, &ps, 50);
        assert!(
            results.contains(&id),
            "machin should be found with distance=2 from machne"
        );
    }

    #[test]
    fn test_expand_prefix() {
        // FST contains "machin", "machineri", "macro".
        // Prefix "mach" should match "machin" and "machineri" but NOT "macro".
        let mut dict = TermDictionary::new();
        let id_machin = dict.get_or_insert("machin");
        let id_machineri = dict.get_or_insert("machineri");
        let id_macro = dict.get_or_insert("macro");
        let bytes = build_fst_from_term_dict(&dict).expect("build FST");
        let map = fst::Map::new(bytes).expect("load FST");

        let mut ps = PostingStore::new();
        ps.add_term_occurrence(id_machin, 0, None);
        ps.add_term_occurrence(id_machineri, 1, None);
        ps.add_term_occurrence(id_macro, 2, None);

        let results = expand_prefix(&map, "mach", &ps, 50);
        assert_eq!(results.len(), 2, "mach* should match machin and machineri");
        assert!(results.contains(&id_machin));
        assert!(results.contains(&id_machineri));
        assert!(!results.contains(&id_macro), "macro should NOT match mach*");
    }

    #[test]
    fn test_expand_cap() {
        // Insert 60 terms starting with "a", build FST, expand prefix "a" with max=50.
        // Verify exactly 50 results returned.
        let mut dict = TermDictionary::new();
        let mut ps = PostingStore::new();
        for i in 0u32..60 {
            let term = format!("a{:04}", i);
            let id = dict.get_or_insert(&term);
            // Give each term a unique doc count so sort is deterministic
            for doc in 0..i {
                ps.add_term_occurrence(id, doc, None);
            }
        }
        let bytes = build_fst_from_term_dict(&dict).expect("build FST");
        let map = fst::Map::new(bytes).expect("load FST");

        let results = expand_prefix(&map, "a", &ps, 50);
        assert_eq!(results.len(), 50, "cap at max_terms=50");
    }

    #[test]
    fn test_levenshtein_distance_helper() {
        assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
        assert_eq!(levenshtein_distance("", "abc"), 3);
        assert_eq!(levenshtein_distance("abc", "abc"), 0);
        assert_eq!(levenshtein_distance("abc", ""), 3);
        assert_eq!(levenshtein_distance("a", "b"), 1);
    }

    #[test]
    fn test_expand_fuzzy_hashmap_no_fst() {
        // When no FST built (high_water_mark=0), scan entire HashMap.
        let mut dict = TermDictionary::new();
        let id = dict.get_or_insert("machin");
        let mut ps = PostingStore::new();
        ps.add_term_occurrence(id, 0, None);

        // "machn" is distance 1 from "machin" — should be found
        let results = expand_fuzzy_hashmap(&dict, "machn", 1, &ps, 0, 50);
        assert!(results.contains(&id));
    }

    #[test]
    fn test_expand_prefix_hashmap_post_compaction() {
        // high_water_mark=1: only scan terms with id >= 1.
        let mut dict = TermDictionary::new();
        let _id_old = dict.get_or_insert("apple"); // id=0, before compaction
        let id_new = dict.get_or_insert("apricot"); // id=1, after compaction
        let mut ps = PostingStore::new();
        ps.add_term_occurrence(_id_old, 0, None);
        ps.add_term_occurrence(id_new, 1, None);

        // With high_water_mark=1, only "apricot" is scanned
        let results = expand_prefix_hashmap(&dict, "ap", &ps, 1, 50);
        assert!(
            results.contains(&id_new),
            "apricot should match 'ap' prefix"
        );
        assert!(
            !results.contains(&_id_old),
            "apple (id=0) should be skipped (below high_water_mark)"
        );
    }
}
