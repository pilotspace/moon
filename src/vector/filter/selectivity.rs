use roaring::RoaringBitmap;

/// Search strategy selected by cost-based analysis of filter selectivity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterStrategy {
    /// No filter applied -- standard unfiltered search.
    Unfiltered,
    /// <2% selectivity or <20K matches: bitmap intersect then SIMD linear scan.
    BruteForceFiltered,
    /// 2-80% selectivity: HNSW beam search with bitmap allow-list + ACORN 2-hop.
    HnswFiltered,
    /// >80% selectivity: standard HNSW with 3x K oversampling then post-filter.
    HnswPostFilter,
}

const BRUTE_FORCE_SELECTIVITY: f64 = 0.02;
const BRUTE_FORCE_MAX_MATCHES: u64 = 20_000;
const POST_FILTER_SELECTIVITY: f64 = 0.80;

/// Select optimal search strategy based on filter selectivity.
///
/// selectivity = matching_vectors / total_vectors
/// - <2% (or <20K matches): BruteForceFiltered
/// - 2%-80%: HnswFiltered (ACORN 2-hop)
/// - >80%: HnswPostFilter (3x oversampling)
pub fn select_strategy(filter_bitmap: Option<&RoaringBitmap>, total_vectors: u32) -> FilterStrategy {
    let bitmap = match filter_bitmap {
        None => return FilterStrategy::Unfiltered,
        Some(bm) => bm,
    };
    if total_vectors == 0 {
        return FilterStrategy::BruteForceFiltered;
    }
    let matching = bitmap.len();
    if matching < BRUTE_FORCE_MAX_MATCHES {
        return FilterStrategy::BruteForceFiltered;
    }
    let selectivity = matching as f64 / total_vectors as f64;
    if selectivity < BRUTE_FORCE_SELECTIVITY {
        FilterStrategy::BruteForceFiltered
    } else if selectivity > POST_FILTER_SELECTIVITY {
        FilterStrategy::HnswPostFilter
    } else {
        FilterStrategy::HnswFiltered
    }
}

// Tests will be added in Task 2
