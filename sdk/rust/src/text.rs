use crate::error::{MoonError, Result};
use crate::types::{AggregateRow, Reducer, TextSearchHit, encode_vector};
use crate::util::{parse_aggregate_rows, parse_text_search_hits};

/// Full-text search sub-client (BM25, `FT.AGGREGATE`, hybrid RRF).
///
/// Obtain via [`MoonClient::text`](crate::MoonClient::text).
pub struct TextClient {
    pub(crate) conn: redis::aio::MultiplexedConnection,
}

impl TextClient {
    /// BM25 full-text search via `FT.SEARCH` with a text query string.
    ///
    /// Returns scored hits sorted by BM25 relevance.
    pub async fn search(
        &mut self,
        index: &str,
        query: &str,
        limit: usize,
        return_fields: Option<&[&str]>,
    ) -> Result<Vec<TextSearchHit>> {
        let mut cmd = redis::cmd("FT.SEARCH");
        cmd.arg(index).arg(query).arg("LIMIT").arg("0").arg(limit);
        if let Some(fields) = return_fields {
            cmd.arg("RETURN").arg(fields.len());
            for f in fields {
                cmd.arg(*f);
            }
        }
        let raw: redis::Value = cmd.query_async(&mut self.conn).await?;
        Ok(parse_text_search_hits(raw))
    }

    /// Hybrid search combining sparse BM25 and dense vector similarity via RRF.
    ///
    /// `weights` is `[bm25_weight, vector_weight]`. Both must be positive and finite.
    /// Uses `FT.SEARCH` with `HYBRID VECTOR … SPARSE … FUSION RRF`.
    pub async fn hybrid_search(
        &mut self,
        index: &str,
        text_query: &str,
        query_vec: &[f32],
        vec_field: &str,
        k: usize,
        weights: [f64; 2],
    ) -> Result<Vec<TextSearchHit>> {
        for (i, &w) in weights.iter().enumerate() {
            if !w.is_finite() || w < 0.0 {
                return Err(MoonError::invalid_arg(format!(
                    "weight[{i}] must be a non-negative finite number, got {w}"
                )));
            }
        }
        if weights.iter().all(|&w| w == 0.0) {
            return Err(MoonError::invalid_arg("all RRF weights are zero"));
        }

        let blob = encode_vector(query_vec);
        let mut cmd = redis::cmd("FT.SEARCH");
        cmd.arg(index)
            .arg(text_query)
            .arg("HYBRID")
            .arg("VECTOR")
            .arg(vec_field)
            .arg(&blob)
            .arg("SPARSE")
            .arg(text_query)
            .arg("FUSION")
            .arg("RRF")
            .arg("WEIGHTS")
            .arg(weights[0])
            .arg(weights[1])
            .arg("LIMIT")
            .arg("0")
            .arg(k);
        let raw: redis::Value = cmd.query_async(&mut self.conn).await?;
        Ok(parse_text_search_hits(raw))
    }

    /// Run an aggregation pipeline via `FT.AGGREGATE`.
    ///
    /// # Example
    /// ```no_run
    /// use moon_client::types::Reducer;
    /// let rows = client.text().aggregate(
    ///     "docs",
    ///     "*",
    ///     "category",
    ///     &[Reducer::Count, Reducer::Avg("price".into())],
    ///     Some(("count", false)),
    ///     Some(100),
    /// ).await?;
    /// ```
    pub async fn aggregate(
        &mut self,
        index: &str,
        query: &str,
        group_by: &str,
        reducers: &[Reducer],
        sort_by: Option<(&str, bool)>, // (field, ascending)
        limit: Option<usize>,
    ) -> Result<Vec<AggregateRow>> {
        let mut cmd = redis::cmd("FT.AGGREGATE");
        cmd.arg(index)
            .arg(query)
            .arg("GROUPBY")
            .arg("1")
            .arg(format!("@{group_by}"));
        for reducer in reducers {
            for arg in reducer.to_args() {
                cmd.arg(arg);
            }
        }
        if let Some((field, asc)) = sort_by {
            cmd.arg("SORTBY")
                .arg("2")
                .arg(format!("@{field}"))
                .arg(if asc { "ASC" } else { "DESC" });
        }
        if let Some(n) = limit {
            cmd.arg("LIMIT").arg("0").arg(n);
        }
        let raw: redis::Value = cmd.query_async(&mut self.conn).await?;
        Ok(parse_aggregate_rows(raw))
    }
}
