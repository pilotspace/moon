use crate::error::{MoonError, Result};
use crate::types::{AggregateRow, Reducer, TextSearchHit, encode_vector};
use crate::util::{parse_aggregate_rows, parse_text_search_hits};

// ─── HybridFilter (CHANGE G) ─────────────────────────────────────────────────

/// Recursive filter tree for HYBRID FT.SEARCH (SDK mirror of the server enum).
///
/// A [`HybridFilter::Tag`] whose `value` ends in `'*'` is a prefix match
/// (StartsWith); all other values are exact matches.
///
/// The encoder enforces the same limits as the server-side parser: max depth 4,
/// max 16 leaves. Exceeding either returns a [`MoonError::InvalidArgument`]
/// before sending the wire request.
///
/// `None` passed to [`TextClient::hybrid_search`] ⇒ wire unchanged (backward
/// compatible with all pre-CHANGE-G callers).
#[derive(Debug, Clone)]
pub enum HybridFilter {
    /// `TAG @<field> <value>` — exact if `value` has no trailing `'*'`;
    /// prefix (StartsWith) if `value` ends with `'*'`.
    Tag { field: String, value: String },
    /// `NUMERIC @<field> <min> <max>` — inclusive range `[min, max]`.
    Numeric { field: String, min: f64, max: f64 },
    /// `AND <n> <expr>{n}` — all children must match.
    And(Vec<HybridFilter>),
    /// `OR <n> <expr>{n}` — at least one child must match.
    Or(Vec<HybridFilter>),
}

/// State for client-side depth/leaf validation.
struct FilterValidator {
    leaf_count: usize,
}

impl FilterValidator {
    fn new() -> Self {
        Self { leaf_count: 0 }
    }

    fn validate(&mut self, filter: &HybridFilter, depth: usize) -> std::result::Result<(), String> {
        if depth > 4 {
            return Err("FILTER too complex (depth > 4)".to_string());
        }
        match filter {
            HybridFilter::Tag { .. } | HybridFilter::Numeric { .. } => {
                self.leaf_count += 1;
                if self.leaf_count > 16 {
                    return Err("FILTER too complex (> 16 leaves)".to_string());
                }
            }
            HybridFilter::And(children) | HybridFilter::Or(children) => {
                for child in children {
                    self.validate(child, depth + 1)?;
                }
            }
        }
        Ok(())
    }
}

/// Encode a `HybridFilter` into the CHANGE E wire form, appending tokens to `cmd`.
///
/// Grammar (recursive prefix, arity-counted):
/// ```text
/// FILTER TAG @<field> <value>
/// FILTER NUMERIC @<field> <min> <max>
/// FILTER AND <n> <expr>{n}
/// FILTER OR  <n> <expr>{n}
/// ```
fn encode_filter(cmd: &mut redis::Cmd, filter: &HybridFilter, top_level: bool) {
    if top_level {
        cmd.arg("FILTER");
    }
    match filter {
        HybridFilter::Tag { field, value } => {
            cmd.arg("TAG").arg(format!("@{field}")).arg(value);
        }
        HybridFilter::Numeric { field, min, max } => {
            cmd.arg("NUMERIC")
                .arg(format!("@{field}"))
                .arg(min.to_string())
                .arg(max.to_string());
        }
        HybridFilter::And(children) => {
            cmd.arg("AND").arg(children.len());
            for child in children {
                encode_filter(cmd, child, false);
            }
        }
        HybridFilter::Or(children) => {
            cmd.arg("OR").arg(children.len());
            for child in children {
                encode_filter(cmd, child, false);
            }
        }
    }
}

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
    /// `filter` is an optional pre-RRF filter (CHANGE G). When `None` the wire
    /// format is **identical** to the pre-CHANGE-G form — fully backward
    /// compatible. When `Some`, the filter tree is appended as a `FILTER <expr>`
    /// clause after `FUSION RRF WEIGHTS`. The encoder validates depth (≤ 4) and
    /// leaf count (≤ 16) client-side and returns [`MoonError::InvalidArgument`]
    /// before sending if the limits are exceeded.
    ///
    /// Wire format (no filter, no sparse):
    ///   `HYBRID VECTOR @f $qv FUSION RRF WEIGHTS w1 w2 w3 LIMIT 0 k PARAMS 2 ...`
    /// Wire format (with filter):
    ///   `HYBRID VECTOR @f $qv FUSION RRF WEIGHTS w1 w2 w3 FILTER TAG @source v LIMIT 0 k PARAMS 2 ...`
    pub async fn hybrid_search(
        &mut self,
        index: &str,
        text_query: &str,
        query_vec: &[f32],
        vec_field: &str,
        sparse_field: Option<&str>,
        k: usize,
        weights: [f64; 3],
        filter: Option<&HybridFilter>,
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

        // Client-side filter validation (same depth/leaf limits as the server parser).
        if let Some(f) = filter {
            let mut validator = FilterValidator::new();
            validator
                .validate(f, 0)
                .map_err(MoonError::invalid_arg)?;
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
            .arg(weights[2]);

        // Append FILTER clause after WEIGHTS, before LIMIT (CHANGE G).
        // When None, wire is unchanged — backward compat.
        if let Some(f) = filter {
            encode_filter(&mut cmd, f, true);
        }

        cmd.arg("LIMIT").arg("0").arg(k);
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
