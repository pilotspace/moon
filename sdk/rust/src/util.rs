use std::collections::HashMap;
use redis::Value;
use crate::types::{AggregateRow, CacheSearchResult, GraphEdge, GraphNode, IndexInfo, QueryResult, SearchResult, TextSearchHit};

// ── Value helpers ────────────────────────────────────────────────────────────

pub fn value_to_string(v: &Value) -> String {
    match v {
        Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        Value::SimpleString(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Double(f) => f.to_string(),
        Value::Boolean(b) => b.to_string(),
        _ => String::new(),
    }
}

#[allow(dead_code)]
pub fn value_to_bytes(v: &Value) -> Vec<u8> {
    match v {
        Value::BulkString(b) => b.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => vec![],
    }
}

pub fn value_to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int(n) => Some(*n),
        Value::BulkString(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        Value::SimpleString(s) => s.parse().ok(),
        _ => None,
    }
}

pub fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Double(f) => Some(*f),
        Value::Int(n) => Some(*n as f64),
        Value::BulkString(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        Value::SimpleString(s) => s.parse().ok(),
        _ => None,
    }
}

#[allow(dead_code)]
pub fn value_is_ok(v: &Value) -> bool {
    matches!(
        v,
        Value::Okay | Value::SimpleString(_) | Value::BulkString(_)
    )
}

// ── Flat key-value array → HashMap ──────────────────────────────────────────

pub fn parse_flat_kv(arr: &[Value]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut i = 0;
    while i + 1 < arr.len() {
        let k = value_to_string(&arr[i]);
        let v = value_to_string(&arr[i + 1]);
        if !k.is_empty() {
            map.insert(k, v);
        }
        i += 2;
    }
    map
}

// ── FT.SEARCH response ───────────────────────────────────────────────────────

/// Parse the RESP2 response of FT.SEARCH / FT.RECOMMEND / FT.NAVIGATE / FT.EXPAND.
///
/// Moon returns: `[total_count, key1, [field1, val1, ...], key2, [...], ...]`
pub fn parse_search_results(raw: Value) -> Vec<SearchResult> {
    let arr = match raw {
        Value::Array(a) => a,
        _ => return vec![],
    };
    if arr.is_empty() {
        return vec![];
    }

    let mut results = Vec::new();
    let mut i = 1; // skip total_count at index 0
    while i < arr.len() {
        let key = value_to_string(&arr[i]);
        i += 1;
        if i >= arr.len() {
            break;
        }

        let fields_raw = match &arr[i] {
            Value::Array(a) => a.as_slice(),
            _ => {
                i += 1;
                continue;
            }
        };
        i += 1;

        let mut fields = HashMap::new();
        let mut score = 0.0_f64;
        let mut graph_hops: Option<i64> = None;
        let mut cache_hit: Option<bool> = None;

        let mut j = 0;
        while j + 1 < fields_raw.len() {
            let fname = value_to_string(&fields_raw[j]);
            let fval = value_to_string(&fields_raw[j + 1]);
            match fname.as_str() {
                "__vec_score" | "vec_score" => {
                    score = fval.parse().unwrap_or(0.0);
                }
                "__graph_hops" => {
                    graph_hops = fval.parse().ok();
                }
                "cache_hit" => {
                    cache_hit = Some(fval.to_lowercase() == "true" || fval == "1");
                }
                _ => {
                    fields.insert(fname, fval);
                }
            }
            j += 2;
        }

        results.push(SearchResult { key, score, fields, graph_hops, cache_hit });
    }
    results
}

/// Parse FT.CACHESEARCH response → `CacheSearchResult`.
pub fn parse_cache_search_results(raw: Value) -> CacheSearchResult {
    let results = parse_search_results(raw);
    let cache_hit = results.iter().any(|r| r.cache_hit == Some(true));
    CacheSearchResult { results, cache_hit }
}

// ── FT.INFO response ─────────────────────────────────────────────────────────

pub fn parse_index_info(name: String, raw: Value) -> IndexInfo {
    let arr = match raw {
        Value::Array(a) => a,
        _ => return IndexInfo { name, ..Default::default() },
    };

    let kv = parse_flat_kv(&arr);
    let num_docs = kv.get("num_docs").and_then(|v| v.parse().ok()).unwrap_or(0);
    let dimension = kv
        .get("dimension")
        .or_else(|| kv.get("dim"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let distance_metric = kv.get("distance_metric").cloned().unwrap_or_default();
    let mut extra = HashMap::new();
    for (k, v) in &kv {
        if !matches!(k.as_str(), "num_docs" | "dimension" | "dim" | "distance_metric") {
            extra.insert(k.clone(), v.clone());
        }
    }

    IndexInfo { name, num_docs, dimension, distance_metric, fields: vec![], extra }
}

// ── GRAPH.QUERY response ─────────────────────────────────────────────────────

/// Parse `[headers, rows, stats]` response from GRAPH.QUERY.
pub fn parse_query_result(raw: Value) -> QueryResult {
    let outer = match raw {
        Value::Array(a) => a,
        _ => return QueryResult { headers: vec![], rows: vec![], stats: vec![] },
    };

    let headers = outer
        .first()
        .and_then(|v| if let Value::Array(a) = v { Some(a) } else { None })
        .map(|a| a.iter().map(value_to_string).collect())
        .unwrap_or_default();

    let rows = outer
        .get(1)
        .and_then(|v| if let Value::Array(a) = v { Some(a) } else { None })
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    if let Value::Array(cells) = row {
                        Some(cells.iter().map(value_to_string).collect())
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    let stats = outer
        .get(2)
        .map(|v| match v {
            Value::Array(a) => a.iter().map(value_to_string).collect(),
            Value::BulkString(_) | Value::SimpleString(_) => vec![value_to_string(v)],
            _ => vec![],
        })
        .unwrap_or_default();

    QueryResult { headers, rows, stats }
}

// ── GRAPH.NEIGHBORS response ─────────────────────────────────────────────────

pub fn parse_neighbors(raw: Value) -> Vec<(i64, i64, String, f64)> {
    let arr = match raw {
        Value::Array(a) => a,
        _ => return vec![],
    };
    // Server returns alternating [edge_map, node_map, ...].
    // Edge maps have "src"/"dst" keys; node maps have "label". We collect edges only.
    let mut results = Vec::new();
    for item in arr {
        let pairs: Vec<(String, &Value)> = match &item {
            Value::Map(m) => m.iter().map(|(k, v)| (value_to_string(k), v)).collect(),
            Value::Array(f) if f.len() % 2 == 0 => f
                .chunks_exact(2)
                .map(|c| (value_to_string(&c[0]), &c[1]))
                .collect(),
            _ => continue,
        };
        // Use item references via temporary; re-derive from item itself.
        let get = |key: &str| -> Option<Value> {
            match &item {
                Value::Map(m) => m.iter().find(|(k, _)| value_to_string(k) == key).map(|(_, v)| v.clone()),
                Value::Array(f) => f.chunks_exact(2).find(|c| value_to_string(&c[0]) == key).map(|c| c[1].clone()),
                _ => None,
            }
        };
        let _ = pairs; // used for detection above
        let Some(src_v) = get("src") else { continue };
        let Some(dst_v) = get("dst") else { continue };
        let Some(src) = value_to_i64(&src_v) else { continue };
        let Some(dst) = value_to_i64(&dst_v) else { continue };
        let edge_type = get("type").as_ref().map(value_to_string).unwrap_or_default();
        let weight = get("weight").as_ref().and_then(value_to_f64).unwrap_or(1.0);
        results.push((src, dst, edge_type, weight));
    }
    results
}

// ── FT.AGGREGATE response ────────────────────────────────────────────────────

pub fn parse_aggregate_rows(raw: Value) -> Vec<AggregateRow> {
    let arr = match raw {
        Value::Array(a) => a,
        _ => return vec![],
    };
    // First element is total count, skip it
    arr.into_iter()
        .skip(1)
        .filter_map(|item| {
            if let Value::Array(fields) = item {
                let map = parse_flat_kv(&fields);
                Some(AggregateRow { fields: map })
            } else {
                None
            }
        })
        .collect()
}

// ── FT.SEARCH (BM25 text mode) ───────────────────────────────────────────────

pub fn parse_text_search_hits(raw: Value) -> Vec<TextSearchHit> {
    parse_search_results(raw)
        .into_iter()
        .map(|r| TextSearchHit { key: r.key, score: r.score, fields: r.fields })
        .collect()
}

// ── GRAPH node/edge helpers ──────────────────────────────────────────────────

#[allow(dead_code)]
pub fn parse_graph_node(raw: Value) -> Option<GraphNode> {
    let arr = match raw {
        Value::Array(a) => a,
        _ => return None,
    };
    let kv = parse_flat_kv(&arr);
    let node_id: i64 = kv.get("node_id")?.parse().ok()?;
    let label = kv.get("label").cloned().unwrap_or_default();
    let mut properties = HashMap::new();
    for (k, v) in kv {
        if !matches!(k.as_str(), "node_id" | "label") {
            properties.insert(k, v);
        }
    }
    Some(GraphNode { node_id, label, properties })
}

#[allow(dead_code)]
pub fn parse_graph_edge(raw: Value) -> Option<GraphEdge> {
    let arr = match raw {
        Value::Array(a) => a,
        _ => return None,
    };
    let kv = parse_flat_kv(&arr);
    let src_id: i64 = kv.get("src_id")?.parse().ok()?;
    let dst_id: i64 = kv.get("dst_id")?.parse().ok()?;
    let edge_type = kv.get("edge_type").cloned().unwrap_or_default();
    let weight: f64 = kv.get("weight").and_then(|v| v.parse().ok()).unwrap_or(1.0);
    let mut properties = HashMap::new();
    for (k, v) in kv {
        if !matches!(k.as_str(), "src_id" | "dst_id" | "edge_type" | "weight") {
            properties.insert(k, v);
        }
    }
    Some(GraphEdge { src_id, dst_id, edge_type, weight, properties })
}
