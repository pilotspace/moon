#[cfg(feature = "text-index")]
use fst::automaton::Str;
/// FST-based term dictionary for fuzzy and prefix search.
///
/// Provides:
/// - `build_fst_from_term_dict`: Build a sorted FST Map from a TermDictionary
/// - `expand_fuzzy`: Expand a query term to matching term IDs via Levenshtein automaton
/// - `expand_prefix`: Expand a prefix to matching term IDs via FST Str automaton
/// - `expand_fuzzy_hashmap`: Brute-force fuzzy scan for post-compaction terms
/// - `expand_prefix_hashmap`: Brute-force prefix scan for post-compaction terms
/// - [`TopTerms`]: the capped selection every expansion path feeds (moon#1218 contract)
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

/// The capped selection of a fuzzy/prefix expansion (D-09: at most `cap` terms).
///
/// Contract (moon#1218; tie-break corrected by the moon#1221 review): candidates rank by document
/// frequency DESC, then by the term's BYTES ascending. Both keys are data, so the kept terms are a
/// function of the matching terms and their document frequencies alone — the same on every
/// process, after a restart, after a rebuild and on a replica, whenever those agree. The term id
/// is NOT used: ids are handed out in first-seen order, which a rebuild (keyspace-walk order) or a
/// replica's own write history assigns differently. One selection spans every source of an
/// expansion (FST stream and post-FST dictionary scan), so the result is the top `cap` of their
/// union — no second, differently-ordered re-cap.
///
/// Bounded: at most `cap` term copies whatever the match count — an evicted candidate's buffer is
/// reused for its replacement.
#[cfg(feature = "text-index")]
pub struct TopTerms {
    cap: usize,
    heap: std::collections::BinaryHeap<Candidate>,
}

#[cfg(feature = "text-index")]
#[derive(PartialEq, Eq)]
struct Candidate {
    df: u32,
    term: Vec<u8>,
    id: u32,
}

#[cfg(feature = "text-index")]
impl Ord for Candidate {
    /// Greater = WORSE, so the max-heap's top is the one to evict: lower df, then larger term
    /// bytes (then larger id — only reachable if a corrupt FST names one term twice).
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .df
            .cmp(&self.df)
            .then_with(|| self.term.cmp(&other.term))
            .then_with(|| self.id.cmp(&other.id))
    }
}

#[cfg(feature = "text-index")]
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "text-index")]
impl TopTerms {
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            heap: std::collections::BinaryHeap::with_capacity(cap.min(64)),
        }
    }

    /// Consider one matching term. A term id already selected is ignored (the FST and the
    /// post-FST scan are disjoint by construction; this only guards a corrupt FST).
    pub fn offer(&mut self, df: u32, term: &[u8], id: u32) {
        if self.cap == 0 {
            return;
        }
        if self.heap.len() == self.cap {
            let Some(worst) = self.heap.peek() else {
                return;
            };
            let better = df
                .cmp(&worst.df)
                .then_with(|| worst.term.as_slice().cmp(term))
                .then_with(|| worst.id.cmp(&id))
                .is_gt();
            if !better {
                return;
            }
        }
        if self.heap.iter().any(|c| c.id == id) {
            return;
        }
        if self.heap.len() < self.cap {
            self.heap.push(Candidate {
                df,
                term: term.to_vec(),
                id,
            });
        } else if let Some(mut worst) = self.heap.peek_mut() {
            worst.df = df;
            worst.term.clear();
            worst.term.extend_from_slice(term);
            worst.id = id;
        }
    }

    /// The selected term ids, best first.
    #[must_use]
    pub fn into_ids(self) -> Vec<u32> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|c| c.id)
            .collect()
    }
}

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
/// Results are capped at `max_terms` by [`TopTerms`] (df DESC, term bytes ASC) to prevent query
/// explosion on common short terms (D-09: max 50).
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
    let mut top = TopTerms::new(max_terms);
    offer_fuzzy_fst(fst_map, term, distance, postings, &mut top);
    top.into_ids()
}

/// Feed every FST term within `distance` of `term` into `top`.
#[cfg(feature = "text-index")]
pub fn offer_fuzzy_fst(
    fst_map: &Map<Vec<u8>>,
    term: &str,
    distance: u8,
    postings: &PostingStore,
    top: &mut TopTerms,
) {
    // levenshtein_automata DFA implements fst::Automaton via "fst_automaton" feature.
    // true = allow transpositions (Damerau-Levenshtein variant).
    let builder = LevenshteinAutomatonBuilder::new(distance, true);
    let dfa = builder.build_dfa(term);
    let mut stream = fst_map.search(&dfa).into_stream();
    while let Some((key, term_id)) = stream.next() {
        let id = term_id as u32;
        top.offer(postings.doc_freq(id), key, id);
    }
}

/// Expand a prefix to matching term IDs via FST Str automaton (FST path).
///
/// Uses `fst::automaton::Str::new(prefix).starts_with()` to stream all
/// dictionary terms beginning with the given prefix.
///
/// Results are capped at `max_terms` by [`TopTerms`] (df DESC, term bytes ASC).
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
    let mut top = TopTerms::new(max_terms);
    offer_prefix_fst(fst_map, prefix, postings, &mut top);
    top.into_ids()
}

/// Feed every FST term starting with `prefix` into `top`.
#[cfg(feature = "text-index")]
pub fn offer_prefix_fst(
    fst_map: &Map<Vec<u8>>,
    prefix: &str,
    postings: &PostingStore,
    top: &mut TopTerms,
) {
    let aut = Str::new(prefix).starts_with();
    let mut stream = fst_map.search(aut).into_stream();
    while let Some((key, term_id)) = stream.next() {
        let id = term_id as u32;
        top.offer(postings.doc_freq(id), key, id);
    }
}

/// Brute-force fuzzy expansion for post-compaction terms (HashMap path).
///
/// Scans only TermDictionary entries with `id >= high_water_mark` — these are
/// terms added after the last FST build and therefore NOT in the FST (D-12).
///
/// When `high_water_mark == 0` (no FST built yet), scans the entire dictionary.
///
/// Results are capped at `max_terms` by [`TopTerms`] (df DESC, term bytes ASC).
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
    let mut top = TopTerms::new(max_terms);
    offer_fuzzy_dict(dict, term, distance, postings, high_water_mark, &mut top);
    top.into_ids()
}

/// Feed every dictionary term with `id >= high_water_mark` within `distance` of `term` into
/// `top`. The dictionary iterates in HashMap order; `top` makes the result independent of it.
#[cfg(feature = "text-index")]
pub fn offer_fuzzy_dict(
    dict: &TermDictionary,
    term: &str,
    distance: u8,
    postings: &PostingStore,
    high_water_mark: u32,
    top: &mut TopTerms,
) {
    let dist = distance as usize;
    for (candidate, &id) in dict.iter() {
        if id >= high_water_mark && levenshtein_distance(term, candidate) <= dist {
            top.offer(postings.doc_freq(id), candidate.as_bytes(), id);
        }
    }
}

/// Brute-force prefix expansion for post-compaction terms (HashMap path).
///
/// Scans only TermDictionary entries with `id >= high_water_mark` — these are
/// terms added after the last FST build and therefore NOT in the FST (D-12).
///
/// When `high_water_mark == 0` (no FST built yet), scans the entire dictionary.
///
/// Results are capped at `max_terms` by [`TopTerms`] (df DESC, term bytes ASC).
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
    let mut top = TopTerms::new(max_terms);
    offer_prefix_dict(dict, prefix, postings, high_water_mark, &mut top);
    top.into_ids()
}

/// Feed every dictionary term with `id >= high_water_mark` starting with `prefix` into `top`.
#[cfg(feature = "text-index")]
pub fn offer_prefix_dict(
    dict: &TermDictionary,
    prefix: &str,
    postings: &PostingStore,
    high_water_mark: u32,
    top: &mut TopTerms,
) {
    for (candidate, &id) in dict.iter() {
        if id >= high_water_mark && candidate.starts_with(prefix) {
            top.offer(postings.doc_freq(id), candidate.as_bytes(), id);
        }
    }
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

    /// Capped expansion must not depend on HashMap iteration order NOR on term-id assignment:
    /// two dictionaries with the same terms inserted in opposite orders (so every term has a
    /// different id, and each map its own random hasher) expand a >cap, all-df-tied prefix to the
    /// same terms — the 50 smallest by bytes (moon#1221 review: the moon#1218 fix broke ties by
    /// term id, which a rebuild or a replica assigns differently).
    #[test]
    fn capped_expansion_is_deterministic_on_df_ties() {
        let words: Vec<String> = (0..120).map(|i| format!("pre{i:03}")).collect();
        let build = |order: &mut dyn Iterator<Item = &String>| {
            let mut dict = TermDictionary::new();
            let mut ids = std::collections::HashMap::new();
            for w in order {
                ids.insert(w.clone(), dict.get_or_insert(w));
            }
            let mut ps = PostingStore::new();
            for (w, &id) in &ids {
                // Every term df = 2: a full tie at the cap boundary.
                let d = w[3..].parse::<u32>().unwrap_or(0);
                ps.add_term_occurrence(id, d, None);
                ps.add_term_occurrence(id, d + 1000, None);
            }
            (dict, ps, ids)
        };
        let (d1, p1, ids1) = build(&mut words.iter());
        let (d2, p2, ids2) = build(&mut words.iter().rev());
        let names = |ids: &std::collections::HashMap<String, u32>, got: &[u32]| {
            let by_id: std::collections::HashMap<u32, &String> =
                ids.iter().map(|(w, &i)| (i, w)).collect();
            got.iter()
                .map(|i| by_id[i].clone())
                .collect::<Vec<String>>()
        };
        let a = expand_prefix_hashmap(&d1, "pre", &p1, 0, 50);
        let b = expand_prefix_hashmap(&d2, "pre", &p2, 0, 50);
        // Best first: df tie -> ascending term bytes, whatever the ids.
        assert_eq!(names(&ids1, &a), words[..50].to_vec());
        assert_eq!(names(&ids2, &b), words[..50].to_vec());
        // FST path: same terms, same order.
        for (d, p, ids) in [(&d1, &p1, &ids1), (&d2, &p2, &ids2)] {
            let map = fst::Map::new(build_fst_from_term_dict(d).expect("build")).expect("load");
            assert_eq!(names(ids, &expand_prefix(&map, "pre", p, 50)), words[..50]);
            let fuzzy = expand_fuzzy(&map, "pre000", 3, p, 50);
            assert_eq!(names(ids, &fuzzy), words[..50]);
        }
        // Re-running on the same dictionary is stable too (fuzzy path).
        let f1 = expand_fuzzy_hashmap(&d1, "pre000", 3, &p1, 0, 50);
        let f2 = expand_fuzzy_hashmap(&d2, "pre000", 3, &p2, 0, 50);
        assert_eq!(names(&ids1, &f1), names(&ids2, &f2));
    }

    /// `TopTerms` keeps exactly the `cap` best of everything offered — (df DESC, term bytes ASC),
    /// best first — whatever the offer order, reusing buffers; a repeated id is kept once.
    #[test]
    fn top_terms_selects_the_best_by_df_then_term_bytes() {
        let mut state = 0x1221_u64;
        let mut rand = move |n: u64| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) % n
        };
        for trial in 0..200 {
            let n = rand(300) as usize;
            let cap = rand(60) as usize;
            // Unique terms per id (as in a dictionary), few distinct dfs -> many ties.
            let mut cands: Vec<(u32, Vec<u8>, u32)> = (0..n as u32)
                .map(|id| {
                    let term = format!("t{:x}", rand(1 << 20) * 1024 + u64::from(id));
                    (rand(4) as u32, term.into_bytes(), id)
                })
                .collect();
            // Offer in a shuffled order.
            for i in (1..cands.len()).rev() {
                cands.swap(i, rand(i as u64 + 1) as usize);
            }
            let mut top = TopTerms::new(cap);
            for (df, term, id) in &cands {
                top.offer(*df, term, *id);
            }
            // A repeated id never takes a second slot.
            if let Some((df, term, id)) = cands.first() {
                top.offer(*df, term, *id);
            }
            cands.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
            let want: Vec<u32> = cands.iter().take(cap).map(|c| c.2).collect();
            assert_eq!(top.into_ids(), want, "trial {trial} n={n} cap={cap}");
        }
    }

    fn body_only_index() -> crate::text::store::TextIndex {
        let mut body = crate::text::types::TextFieldDef::new(bytes::Bytes::from_static(b"body"));
        body.nostem = true;
        crate::text::store::TextIndex::new(
            bytes::Bytes::from_static(b"x"),
            vec![bytes::Bytes::from_static(b"x:")],
            vec![body],
            crate::text::types::BM25Config::default(),
        )
    }

    /// The four documents of the review fixture: `pre000..pre119`, the 60 odd terms in two docs
    /// (df 2), the even ones in one (df 1).
    fn review_docs() -> Vec<(&'static str, Vec<u32>)> {
        vec![
            ("x:a1", (0..60).collect()),
            ("x:b1", (0..60).filter(|i| i % 2 == 1).collect()),
            ("x:a2", (60..120).collect()),
            ("x:b2", (60..120).filter(|i| i % 2 == 1).collect()),
        ]
    }

    /// Index `docs` in order; build the FST after `fst_after` of them (`None`: never).
    fn index_docs(
        docs: &[(&str, Vec<u32>)],
        fst_after: Option<usize>,
    ) -> crate::text::store::TextIndex {
        let mut idx = body_only_index();
        for (i, (key, terms)) in docs.iter().enumerate() {
            if fst_after == Some(i) {
                idx.build_fst();
            }
            let text: Vec<String> = terms.iter().map(|t| format!("pre{t:03}")).collect();
            let kh = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
            let args = [
                crate::protocol::Frame::BulkString(bytes::Bytes::from_static(b"body")),
                crate::protocol::Frame::BulkString(bytes::Bytes::from(text.join(" "))),
            ];
            idx.index_document(kh, key.as_bytes(), &args);
        }
        if fst_after == Some(docs.len()) {
            idx.build_fst();
        }
        idx
    }

    fn expanded_terms(
        idx: &crate::text::store::TextIndex,
        text: &str,
        modifier: &crate::text::store::TermModifier,
    ) -> Vec<String> {
        let by_id: std::collections::HashMap<u32, &str> = idx.field_term_dicts[0]
            .iter()
            .map(|(t, &id)| (id, t))
            .collect();
        idx.expand_terms(0, text, modifier)
            .iter()
            .map(|id| by_id[id].to_owned())
            .collect()
    }

    /// moon#1221 review: with an FST AND post-FST terms, `expand_terms` re-capped the merged ids
    /// with a df-only unstable sort — which df-tied terms survived was a sort-implementation
    /// accident (it kept `pre101`, dropped `pre059`). Every path — FST only, dictionary only and
    /// both — must keep the 50 df-2 terms smallest by bytes, best first.
    #[test]
    fn capped_expansion_breaks_df_ties_by_term_bytes_on_every_path() {
        use crate::text::store::TermModifier;
        let want: Vec<String> = (0..120u32)
            .filter(|i| i % 2 == 1)
            .take(50)
            .map(|i| format!("pre{i:03}"))
            .collect();
        for (label, fst_after) in [
            ("dual", Some(2)),
            ("fst-only", Some(4)),
            ("dict-only", None),
        ] {
            let idx = index_docs(&review_docs(), fst_after);
            if label == "dual" {
                let hwm = idx.field_term_dicts[0].fst_high_water_mark;
                assert_eq!(hwm, 60, "dual fixture: 60 terms in the FST, 60 after it");
            }
            for modifier in [TermModifier::Prefix, TermModifier::Fuzzy(3)] {
                let probe = if modifier == TermModifier::Prefix {
                    "pre"
                } else {
                    "pre050"
                };
                assert_eq!(
                    expanded_terms(&idx, probe, &modifier),
                    want,
                    "{label} {modifier:?}"
                );
            }
        }
    }

    /// The kept terms depend on the terms and their document frequencies only: indexing the same
    /// documents in the opposite order (every term id different, and a different half of the
    /// vocabulary in the FST) expands to the same terms on every path — the property a rebuild
    /// (keyspace-walk order) and a replica need.
    #[test]
    fn capped_expansion_is_independent_of_term_id_assignment() {
        use crate::text::store::TermModifier;
        let forward = review_docs();
        let mut backward = review_docs();
        backward.reverse();
        for fst_after in [Some(2), Some(4), None] {
            let a = index_docs(&forward, fst_after);
            let b = index_docs(&backward, fst_after);
            assert_ne!(
                a.field_term_dicts[0].get("pre001"),
                b.field_term_dicts[0].get("pre001"),
                "the fixture must assign different ids"
            );
            for (probe, modifier) in [
                ("pre", TermModifier::Prefix),
                ("pre0", TermModifier::Prefix),
                ("pre050", TermModifier::Fuzzy(3)),
                ("pre05", TermModifier::Fuzzy(2)),
            ] {
                let x = expanded_terms(&a, probe, &modifier);
                assert!(!x.is_empty());
                assert_eq!(
                    x,
                    expanded_terms(&b, probe, &modifier),
                    "{probe} {modifier:?} fst_after={fst_after:?}"
                );
            }
        }
    }
}
