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

    /// Hybrid search combining BM25 + dense vector similarity (and optionally
    /// sparse) via RRF in a single server round trip.
    ///
    /// `weights` is `[bm25_weight, dense_weight, sparse_weight]`. All must be
    /// non-negative and finite. When `sparse_field` is `None` the wire format
    /// omits the SPARSE clause entirely so indexes without a SPARSE-typed
    /// field (e.g. plain TEXT + VECTOR schema) still complete the query —
    /// per the Moon server's two-way fusion fallback documented in
    /// `src/command/vector_search/hybrid.rs` (`HybridQuery::sparse: Option`).
    /// `weights[2]` is ignored in two-way mode.
    ///
    /// Wire format (full): `HYBRID VECTOR @vec_field $qv SPARSE @sparse_field $sq FUSION RRF WEIGHTS w1 w2 w3 PARAMS 4 ...`
    /// Wire format (no sparse): `HYBRID VECTOR @vec_field $qv FUSION RRF WEIGHTS w1 w2 w3 PARAMS 2 query_vec <blob>`
    pub async fn hybrid_search(
        &mut self,
        index: &str,
        text_query: &str,
        query_vec: &[f32],
        vec_field: &str,
        sparse_field: Option<&str>,
        k: usize,
        weights: [f64; 3],
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
            .arg(format!("@{vec_field}"))
            .arg("$query_vec");
        if let Some(sf) = sparse_field {
            cmd.arg("SPARSE").arg(format!("@{sf}")).arg("$sparse_query");
        }
        cmd.arg("FUSION")
            .arg("RRF")
            .arg("WEIGHTS")
            .arg(weights[0])
            .arg(weights[1])
            .arg(weights[2])
            .arg("LIMIT")
            .arg("0")
            .arg(k);
        if sparse_field.is_some() {
            cmd.arg("PARAMS")
                .arg(4)
                .arg("query_vec")
                .arg(&blob)
                .arg("sparse_query")
                .arg(text_query);
        } else {
            cmd.arg("PARAMS").arg(2).arg("query_vec").arg(&blob);
        }
        let raw: redis::Value = cmd.query_async(&mut self.conn).await?;
        Ok(parse_text_search_hits(raw))
    }

    /// Run an aggregation pipeline via `FT.AGGREGATE`.
    ///
    /// # Example
    /// ```no_run
    /// use moondb::types::Reducer;
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
