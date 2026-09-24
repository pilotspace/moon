// BM25 scoring function and per-field statistics.
//
// Implements the Okapi BM25 formula:
//   score = SUM_over_terms( IDF(t) * (tf * (k1+1)) / (tf + k1*(1-b+b*dl/avgdl)) )
// where:
//   IDF(t) = ln(1 + (N - df + 0.5) / (df + 0.5))
//   N = total documents, df = document frequency of term
//   tf = term frequency in document, dl = document field length
//   avgdl = average document field length
//   k1, b = tuning parameters (default 1.2, 0.75)

/// Per-field statistics maintained incrementally during indexing.
///
/// Used at query time to compute BM25 scores without scanning all documents.
#[derive(Debug, Clone, Default)]
pub struct FieldStats {
    /// Number of documents that have this field indexed.
    pub num_docs: u32,
    /// Sum of all document field lengths (in tokens).
    pub total_field_length: u64,
}

impl FieldStats {
    /// Create empty field statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Average document length for this field.
    ///
    /// Returns 0.0 when no documents are indexed (avoids division by zero).
    #[inline]
    pub fn avg_doc_len(&self) -> f32 {
        if self.num_docs == 0 {
            0.0
        } else {
            self.total_field_length as f32 / self.num_docs as f32
        }
    }
}

/// Compute BM25 relevance score for a single term in a single document.
///
/// # Arguments
/// * `term_freq` - Number of times the term appears in this document field
/// * `doc_freq` - Number of documents containing this term
/// * `total_docs` - Total number of documents in the index
/// * `field_length` - Number of tokens in this document's field
/// * `avg_field_length` - Average field length across all documents
/// * `k1` - Term frequency saturation (default 1.2)
/// * `b` - Length normalization (default 0.75)
///
/// Returns 0.0 for degenerate inputs (zero tf, zero docs, zero df).
#[inline]
pub fn bm25_score(
    term_freq: f32,
    doc_freq: u32,
    total_docs: u32,
    field_length: u32,
    avg_field_length: f32,
    k1: f32,
    b: f32,
) -> f32 {
    if term_freq == 0.0 || total_docs == 0 || doc_freq == 0 {
        return 0.0;
    }
    let idf = bm25_idf(doc_freq, total_docs);
    bm25_from_parts(
        term_freq,
        idf,
        bm25_len_norm(field_length, avg_field_length, k1, b),
        k1,
    )
}

// ── Hoistable parts of `bm25_score` (moon#1191) ─────────────────────────────
//
// A literal split of the expression above, so a query can compute the IDF once
// per term and the length normalisation once per (document, field) instead of
// once per (document, term). Each part is the SAME f32 operation sequence as
// the monolithic formula, so `bm25_from_parts(tf, bm25_idf(..), bm25_len_norm(..), k1)`
// is bit-identical to `bm25_score` (pinned by `bm25_parts_are_bit_identical`).
// Callers must apply the degenerate-input guard (`tf == 0 || N == 0 || df == 0
// -> 0.0`) themselves, exactly as `bm25_score` does.

/// IDF: `ln(1 + (N - df + 0.5) / (df + 0.5))`.
#[inline]
pub fn bm25_idf(doc_freq: u32, total_docs: u32) -> f32 {
    ((total_docs as f32 - doc_freq as f32 + 0.5) / (doc_freq as f32 + 0.5) + 1.0).ln()
}

/// Length normalisation term `k1 * (1 - b + b * dl / max(avgdl, 1))`.
#[inline]
pub fn bm25_len_norm(field_length: u32, avg_field_length: f32, k1: f32, b: f32) -> f32 {
    k1 * (1.0 - b + b * field_length as f32 / avg_field_length.max(1.0))
}

/// `idf * (tf * (k1 + 1)) / (tf + len_norm)`.
#[inline]
pub fn bm25_from_parts(term_freq: f32, idf: f32, len_norm: f32, k1: f32) -> f32 {
    let tf_norm = (term_freq * (k1 + 1.0)) / (term_freq + len_norm);
    idf * tf_norm
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pre-moon#1191 monolithic formula, verbatim.
    fn bm25_score_head(tf: f32, df: u32, n: u32, dl: u32, avgdl: f32, k1: f32, b: f32) -> f32 {
        if tf == 0.0 || n == 0 || df == 0 {
            return 0.0;
        }
        let idf = ((n as f32 - df as f32 + 0.5) / (df as f32 + 0.5) + 1.0).ln();
        let tf_norm = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * dl as f32 / avgdl.max(1.0)));
        idf * tf_norm
    }

    #[test]
    fn bm25_parts_are_bit_identical() {
        let mut checked = 0;
        for tf in [1u32, 2, 3, 7, 40] {
            for df in [1u32, 2, 9, 500, 12_000] {
                for n in [1u32, 3, 10, 1_000, 1_000_000] {
                    for dl in [0u32, 1, 5, 17, 300] {
                        for avgdl in [0.0f32, 0.5, 1.0, 3.7, 211.25] {
                            for (k1, b) in [(1.2f32, 0.75f32), (0.9, 0.4), (2.0, 1.0)] {
                                let want = bm25_score_head(tf as f32, df, n, dl, avgdl, k1, b);
                                let got = bm25_score(tf as f32, df, n, dl, avgdl, k1, b);
                                let parts = bm25_from_parts(
                                    tf as f32,
                                    bm25_idf(df, n),
                                    bm25_len_norm(dl, avgdl, k1, b),
                                    k1,
                                );
                                assert_eq!(got.to_bits(), want.to_bits());
                                assert_eq!(parts.to_bits(), want.to_bits());
                                checked += 1;
                            }
                        }
                    }
                }
            }
        }
        assert!(checked > 5000);
    }
}
