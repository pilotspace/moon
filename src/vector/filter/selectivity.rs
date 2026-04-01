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
pub fn select_strategy(
    filter_bitmap: Option<&RoaringBitmap>,
    total_vectors: u32,
) -> FilterStrategy {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bitmap_with_n(n: u32) -> RoaringBitmap {
        let mut bm = RoaringBitmap::new();
        if n > 0 {
            bm.insert_range(0..n);
        }
        bm
    }

    #[test]
    fn test_none_filter_unfiltered() {
        assert_eq!(select_strategy(None, 1_000_000), FilterStrategy::Unfiltered);
    }

    #[test]
    fn test_total_vectors_zero() {
        let bm = bitmap_with_n(10);
        assert_eq!(
            select_strategy(Some(&bm), 0),
            FilterStrategy::BruteForceFiltered
        );
    }

    #[test]
    fn test_empty_bitmap_brute_force() {
        let bm = RoaringBitmap::new();
        assert_eq!(
            select_strategy(Some(&bm), 1_000_000),
            FilterStrategy::BruteForceFiltered
        );
    }

    #[test]
    fn test_small_match_count_brute_force() {
        // 100 matches out of 1M -> < 20K threshold
        let bm = bitmap_with_n(100);
        assert_eq!(
            select_strategy(Some(&bm), 1_000_000),
            FilterStrategy::BruteForceFiltered
        );
    }

    #[test]
    fn test_below_20k_threshold_brute_force() {
        // 15,000 matches out of 1M (1.5%) -> < 20K absolute threshold
        let bm = bitmap_with_n(15_000);
        assert_eq!(
            select_strategy(Some(&bm), 1_000_000),
            FilterStrategy::BruteForceFiltered
        );
    }

    #[test]
    fn test_mid_selectivity_hnsw_filtered() {
        // 50,000 matches out of 1M (5%) -> HnswFiltered
        let bm = bitmap_with_n(50_000);
        assert_eq!(
            select_strategy(Some(&bm), 1_000_000),
            FilterStrategy::HnswFiltered
        );
    }

    #[test]
    fn test_high_selectivity_post_filter() {
        // 900,000 matches out of 1M (90%) -> HnswPostFilter
        let bm = bitmap_with_n(900_000);
        assert_eq!(
            select_strategy(Some(&bm), 1_000_000),
            FilterStrategy::HnswPostFilter
        );
    }

    #[test]
    fn test_boundary_at_80_percent() {
        // Exactly 80% -> should be HnswFiltered (> 0.80 required for PostFilter)
        let bm = bitmap_with_n(800_000);
        assert_eq!(
            select_strategy(Some(&bm), 1_000_000),
            FilterStrategy::HnswFiltered
        );
    }

    #[test]
    fn test_just_above_20k_with_low_selectivity() {
        // 20,000 matches out of 1M (2%) -> at boundary, selectivity == 0.02
        // selectivity < 0.02 is false at exactly 0.02, so HnswFiltered
        let bm = bitmap_with_n(20_000);
        assert_eq!(
            select_strategy(Some(&bm), 1_000_000),
            FilterStrategy::HnswFiltered
        );
    }
}
