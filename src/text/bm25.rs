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
    // IDF: ln(1 + (N - df + 0.5) / (df + 0.5))
    let idf = ((total_docs as f32 - doc_freq as f32 + 0.5) / (doc_freq as f32 + 0.5) + 1.0).ln();
    // TF normalization with length normalization
    let tf_norm = (term_freq * (k1 + 1.0))
        / (term_freq + k1 * (1.0 - b + b * field_length as f32 / avg_field_length.max(1.0)));
    idf * tf_norm
}
