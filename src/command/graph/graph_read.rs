//! GRAPH.* read command handlers.
//!
//! These commands read from GraphStore: NEIGHBORS, INFO, LIST, QUERY, RO_QUERY, EXPLAIN.

use bytes::Bytes;
use slotmap::Key;

use crate::graph::cypher;
use crate::graph::store::GraphStore;
use crate::graph::traversal::SegmentMergeReader;
use crate::graph::types::Direction;
use crate::protocol::Frame;

use super::graph_write::extract_bulk;

/// Parse an optional `VALID_AT <timestamp_ms>` argument from command args.
///
/// Scans the args array for a `VALID_AT` keyword followed by an i64 timestamp.
/// Returns `None` if not present or unparseable (non-temporal query).
fn parse_valid_at(args: &[Frame]) -> Option<i64> {
    for i in 0..args.len().saturating_sub(1) {
        if let Frame::BulkString(ref bs) = args[i] {
            if bs.eq_ignore_ascii_case(b"VALID_AT") {
                if let Frame::BulkString(ref val) = args[i + 1] {
                    return std::str::from_utf8(val).ok()?.trim().parse::<i64>().ok();
                }
            }
        }
    }
    None
}

/// Parse optional `--decay <lambda_per_sec>` and `--time-weight <w>` from
/// GRAPH.QUERY args into a `DecayConfig` (temporal-decay traversal scoring).
///
/// Strict validation (unlike `parse_valid_at`'s silent-None: decay is a new
/// surface, so malformed input is an error, not a silent no-op):
/// - both values must parse as finite, non-negative f64
/// - `--time-weight` without `--decay` is rejected
/// - a dangling flag with no value is rejected
///
/// Returns `Ok(None)` when neither flag is present.
fn parse_decay(args: &[Frame]) -> Result<Option<crate::graph::scoring::DecayConfig>, &'static str> {
    fn flag_value(args: &[Frame], flag: &[u8]) -> Result<Option<f64>, &'static str> {
        for i in 0..args.len() {
            if let Frame::BulkString(ref bs) = args[i] {
                if bs.as_ref() == flag {
                    let Some(Frame::BulkString(val)) = args.get(i + 1) else {
                        return Err("ERR flag requires a value");
                    };
                    let parsed = std::str::from_utf8(val)
                        .ok()
                        .and_then(|s| s.trim().parse::<f64>().ok());
                    return match parsed {
                        Some(v) if v.is_finite() && v >= 0.0 => Ok(Some(v)),
                        _ => Err("ERR value must be a finite non-negative number"),
                    };
                }
            }
        }
        Ok(None)
    }

    let lambda = flag_value(args, b"--decay")
        .map_err(|_| "ERR --decay must be a finite non-negative number (1/seconds)")?;
    let time_weight = flag_value(args, b"--time-weight")
        .map_err(|_| "ERR --time-weight must be a finite non-negative number")?;

    match (lambda, time_weight) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err("ERR --time-weight requires --decay"),
        (Some(lambda_per_sec), tw) => Ok(Some(crate::graph::scoring::DecayConfig {
            lambda_per_sec,
            time_weight: tw.unwrap_or(1.0),
            now_ms: crate::storage::entry::current_time_ms(),
        })),
    }
}

/// Parse an optional `TIMEOUT <ms>` argument (RedisGraph parity).
///
/// `TIMEOUT 0` disables the timeout for this query. Strict validation like
/// `parse_decay` (new surface ⇒ malformed input is an error, not a silent
/// no-op): a dangling keyword or non-integer value is rejected. Returns
/// `Ok(None)` when the keyword is absent.
pub(super) fn parse_timeout_ms(args: &[Frame]) -> Result<Option<u64>, &'static str> {
    for i in 0..args.len() {
        if let Frame::BulkString(ref bs) = args[i] {
            if bs.eq_ignore_ascii_case(b"TIMEOUT") {
                let Some(Frame::BulkString(val)) = args.get(i + 1) else {
                    return Err("ERR TIMEOUT requires a value in milliseconds");
                };
                let parsed = std::str::from_utf8(val)
                    .ok()
                    .and_then(|s| s.trim().parse::<u64>().ok());
                return match parsed {
                    Some(ms) => Ok(Some(ms)),
                    None => Err("ERR TIMEOUT must be a non-negative integer (milliseconds)"),
                };
            }
        }
    }
    Ok(None)
}

/// Build the traversal guard for one query: per-query `TIMEOUT <ms>` override
/// if present (0 = unlimited), else the configured process default
/// (`--graph-timeout-ms`, 30s unless overridden).
fn query_guard(
    args: &[Frame],
    snapshot_lsn: u64,
) -> Result<crate::graph::traversal_guard::TraversalGuard, &'static str> {
    use crate::graph::traversal_guard::TraversalGuard;
    Ok(match parse_timeout_ms(args)? {
        Some(0) => TraversalGuard::new(snapshot_lsn, std::time::Duration::MAX),
        Some(ms) => TraversalGuard::new(snapshot_lsn, std::time::Duration::from_millis(ms)),
        None => TraversalGuard::with_default_timeout(snapshot_lsn),
    })
}

/// Cheap pre-check for the Cypher result cache (Task #32): does this
/// GRAPH.QUERY carry a `--decay` flag? Decay queries are wall-clock
/// dependent (`TemporalDecayScorer::now` captures real time at query start,
/// not derived from graph state) and must NEVER be cached regardless of
/// `write_gen` -- two decay queries at different real times against an
/// unchanged graph legitimately score/order differently. Byte-scan only
/// (not a full `parse_decay`) so a miss on this check costs nothing on the
/// hot path; `parse_decay`'s stricter validation still runs later in
/// `run_read_query` regardless of this pre-check's answer.
fn has_decay_flag(args: &[Frame]) -> bool {
    args.iter()
        .any(|f| matches!(f, Frame::BulkString(b) if b.as_ref() == b"--decay"))
}

/// Hash the "remaining args" (everything after the graph name and Cypher
/// text -- `--params`, `VALID_AT`, `TIMEOUT`, `--decay`, `--time-weight`)
/// for the Cypher result-cache key (Task #32). Allocation-free: each arg's
/// logical byte content is hashed independently via `xxh64` and folded,
/// rather than concatenated into one buffer first. Order-sensitive (a
/// differently-ordered but semantically-identical arg list gets a different
/// key) -- a harmless false split of the key space, never a correctness
/// issue, since a miss just re-executes and re-populates.
fn hash_query_args(args: &[Frame]) -> u64 {
    let mut acc: u64 = 0;
    for frame in args {
        acc = acc.rotate_left(13) ^ hash_frame_bytes(frame);
    }
    acc
}

/// `xxh64` of a single `Frame`'s logical byte content, used only for
/// result-cache key derivation -- NOT a wire format. Numeric/boolean
/// variants hash their shortest text representation (stack-buffer
/// `itoa`/`ryu`, no allocation) rather than their binary encoding; hash
/// collisions across variants are harmless because `ResultCacheKey`
/// equality is a plain struct compare, not the hash alone -- a collision
/// only costs a wasted cache miss, never a wrong answer.
fn hash_frame_bytes(frame: &Frame) -> u64 {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => cypher::planner::hash_query(b),
        Frame::Integer(n) => {
            let mut buf = itoa::Buffer::new();
            cypher::planner::hash_query(buf.format(*n).as_bytes())
        }
        Frame::Double(f) => {
            let mut buf = ryu::Buffer::new();
            cypher::planner::hash_query(buf.format(*f).as_bytes())
        }
        Frame::Boolean(b) => {
            cypher::planner::hash_query(if *b { b"\x01true" } else { b"\x01false" })
        }
        Frame::Null => cypher::planner::hash_query(b"\x01null"),
        _ => cypher::planner::hash_query(b"\x01other"),
    }
}

/// Build the Cypher result-cache key (Task #32) for `cypher_bytes` (the raw
/// query text, `args[1]`) and `rest_args` (everything after it, `args[2..]`
/// -- `--params`/`VALID_AT`/`TIMEOUT`/`--decay`/`--time-weight`). Shared by
/// the pre-lookup in `graph_query_readonly` and the population step in
/// `run_read_query` so both sides always compute the identical key.
fn result_cache_key(
    cypher_bytes: &[u8],
    rest_args: &[Frame],
) -> cypher::result_cache::ResultCacheKey {
    cypher::result_cache::ResultCacheKey {
        query_hash: cypher::planner::hash_query(cypher_bytes),
        args_hash: hash_query_args(rest_args),
    }
}

/// Parse `--params <json_object>` from GRAPH.QUERY args into executor `Value` map.
///
/// Scans args for `--params` keyword followed by a JSON string. The JSON must be
/// an object (`{...}`); each top-level key becomes a parameter name, each value
/// is converted to the executor's `Value` enum. Returns empty map if `--params`
/// is absent or the JSON is malformed (graceful degradation — matches pre-fix
/// behavior where params were always empty).
fn parse_params(args: &[Frame]) -> std::collections::HashMap<String, cypher::executor::Value> {
    for i in 0..args.len().saturating_sub(1) {
        if let Frame::BulkString(ref bs) = args[i] {
            if bs.as_ref() == b"--params" {
                if let Frame::BulkString(ref val) = args[i + 1] {
                    if let Ok(s) = std::str::from_utf8(val) {
                        if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(s) {
                            let mut out = std::collections::HashMap::with_capacity(map.len());
                            for (k, v) in map {
                                out.insert(k, json_to_graph_value(&v));
                            }
                            return out;
                        }
                    }
                }
            }
        }
    }
    std::collections::HashMap::new()
}

fn json_to_graph_value(v: &serde_json::Value) -> cypher::executor::Value {
    match v {
        serde_json::Value::Null => cypher::executor::Value::Null,
        serde_json::Value::Bool(b) => cypher::executor::Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                cypher::executor::Value::Int(i)
            } else {
                cypher::executor::Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => {
            cypher::executor::Value::String(Bytes::copy_from_slice(s.as_bytes()))
        }
        serde_json::Value::Array(arr) => {
            cypher::executor::Value::List(arr.iter().map(json_to_graph_value).collect())
        }
        serde_json::Value::Object(map) => cypher::executor::Value::Map(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_graph_value(v)))
                .collect(),
        ),
    }
}

/// GRAPH.NEIGHBORS <graph> <node_id> [TYPE <type>] [DEPTH <n>] [DIRECTION IN|OUT|BOTH]
///
/// Returns an array of neighbor nodes/edges as RESP3 Maps.
/// Default direction: BOTH (outgoing + incoming).
/// DEPTH > 1 performs multi-hop expansion (BFS).
pub fn graph_neighbors(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.NEIGHBORS' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let node_id = match parse_u64(&args[1]) {
        Some(id) => id,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid node ID")),
    };

    let graph = match store.get_graph(graph_name) {
        Some(g) => g,
        None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
    };

    // Parse optional TYPE, DEPTH, and DIRECTION arguments.
    let mut edge_type_filter: Option<u16> = None;
    let mut depth: u32 = 1;
    let mut direction = Direction::Both;
    let mut pos = 2;

    while pos < args.len() {
        let key = match extract_bulk(&args[pos]) {
            Some(b) => b,
            None => {
                pos += 1;
                continue;
            }
        };

        if key.eq_ignore_ascii_case(b"TYPE") {
            pos += 1;
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR missing TYPE value"));
            }
            if let Some(type_name) = extract_bulk(&args[pos]) {
                edge_type_filter = Some(super::graph_write::label_to_id(type_name));
            }
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"DEPTH") {
            pos += 1;
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR missing DEPTH value"));
            }
            depth = match parse_u32(&args[pos]) {
                Some(d) if d > 0 => d,
                _ => return Frame::Error(Bytes::from_static(b"ERR invalid DEPTH value")),
            };
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"DIRECTION") {
            pos += 1;
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR missing DIRECTION value"));
            }
            direction = match extract_bulk(&args[pos]) {
                Some(d) if d.eq_ignore_ascii_case(b"OUT") => Direction::Outgoing,
                Some(d) if d.eq_ignore_ascii_case(b"IN") => Direction::Incoming,
                Some(d) if d.eq_ignore_ascii_case(b"BOTH") => Direction::Both,
                _ => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR invalid DIRECTION value (IN|OUT|BOTH)",
                    ));
                }
            };
            pos += 1;
        } else {
            pos += 1;
        }
    }

    // Cap depth to prevent explosion.
    let max_depth = 10u32;
    if depth > max_depth {
        return Frame::Error(Bytes::from_static(b"ERR DEPTH exceeds maximum (10)"));
    }

    let node_key = super::graph_write::external_id_to_node_key(node_id);

    let memgraph = &graph.write_buf;

    let lsn = u64::MAX - 1; // See all live data (MAX-1 because deleted_lsn=MAX means alive).
    let segments_guard = graph.segments.load();
    let csr_segs = &segments_guard.immutable;

    // Verify the start node exists in EITHER tier — freeze MOVES nodes into
    // CSR segments (drains the write buffer), so a memgraph-only check would
    // reject every compacted node.
    let view = crate::graph::view::MergedNodeView::new(memgraph, csr_segs);
    if !view.contains(node_key) {
        return Frame::Error(Bytes::from_static(b"ERR node not found"));
    }

    // Build a SegmentMergeReader that sees both MemGraph and immutable CSR
    // segments, honoring the requested traversal direction.
    let reader =
        SegmentMergeReader::new(Some(memgraph), csr_segs, direction, lsn, edge_type_filter);

    // TraversalGuard enforces bounded epoch hold (per-query TIMEOUT override,
    // else the configured `--graph-timeout-ms` default).
    let guard = match query_guard(args, lsn) {
        Ok(g) => g,
        Err(msg) => return Frame::Error(Bytes::from_static(msg.as_bytes())),
    };

    // BFS expansion using SegmentMergeReader for per-node neighbor lookup.
    let mut visited = std::collections::HashSet::new();
    visited.insert(node_id);
    let mut frontier = vec![node_key];
    let mut results: Vec<Frame> = Vec::with_capacity(128);
    // Cap total results.
    let max_results = 10_000usize;

    for _hop in 0..depth {
        // Check traversal timeout at each hop.
        if let Err(timeout) = guard.check_timeout() {
            return Frame::Error(Bytes::from(format!("ERR {timeout}")));
        }

        let mut next_frontier = Vec::with_capacity(frontier.len() * 4);

        for &current in &frontier {
            for merged in reader.neighbors(current) {
                let neighbor_ext_id = merged.node.data().as_ffi();

                if visited.contains(&neighbor_ext_id) {
                    continue;
                }
                visited.insert(neighbor_ext_id);

                // Add edge as RESP3 Map (from MemGraph if available, otherwise synthetic).
                if let Some(edge) = memgraph.get_edge(merged.edge) {
                    results.push(edge_to_frame(merged.edge, edge));
                } else {
                    // CSR-only edge: build a minimal edge frame from MergedNeighbor.
                    results.push(merged_edge_to_frame(&merged));
                }

                // Add neighbor node as RESP3 Map.
                if let Some(node) = memgraph.get_node(merged.node) {
                    results.push(node_to_frame(merged.node, node));
                } else {
                    // CSR-only node: minimal node frame.
                    results.push(merged_node_to_frame(&merged));
                }

                if results.len() >= max_results {
                    break;
                }

                next_frontier.push(merged.node);
            }

            if results.len() >= max_results {
                break;
            }
        }

        frontier = next_frontier;
        if frontier.is_empty() {
            break;
        }
    }

    Frame::Array(results.into())
}

/// GRAPH.INFO <graph>
///
/// Returns graph statistics as RESP3 Map.
pub fn graph_info(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.INFO' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let graph = match store.get_graph(graph_name) {
        Some(g) => g,
        None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
    };

    let memgraph = &graph.write_buf;
    let segments = graph.segments.load();
    let stats = &graph.stats;

    let node_count = memgraph.node_count() as i64;
    let edge_count = memgraph.edge_count() as i64;
    let immutable_segments = segments.immutable.len() as i64;

    // Degree distribution from GraphStats.
    let degree_stats = Frame::Map(vec![
        (
            Frame::SimpleString(Bytes::from_static(b"avg")),
            Frame::Double(stats.degree_stats.avg),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"p50")),
            Frame::Integer(stats.degree_stats.p50 as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"p99")),
            Frame::Integer(stats.degree_stats.p99 as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"max")),
            Frame::Integer(stats.degree_stats.max as i64),
        ),
    ]);

    Frame::Map(vec![
        (
            Frame::SimpleString(Bytes::from_static(b"name")),
            Frame::BulkString(graph.name.clone()),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"node_count")),
            Frame::Integer(node_count),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"edge_count")),
            Frame::Integer(edge_count),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"immutable_segments")),
            Frame::Integer(immutable_segments),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"edge_threshold")),
            Frame::Integer(graph.edge_threshold as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"created_lsn")),
            Frame::Integer(graph.created_lsn as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"degree_stats")),
            degree_stats,
        ),
        (
            // Monotonic freshness counter for this shard's GRAPH engine.
            // Starts at 0 on boot; NOT restored from WAL (hint only).
            // Bumped after every successful node/edge/property mutation and
            // GRAPH.CREATE/DROP. Callers use this to detect stale query cache.
            Frame::SimpleString(Bytes::from_static(b"version_token")),
            Frame::Integer(store.version_token() as i64),
        ),
    ])
}

/// GRAPH.LIST
///
/// Returns an array of all graph names.
pub fn graph_list(store: &GraphStore) -> Frame {
    let names = store.list_graphs();
    let frames: Vec<Frame> = names
        .into_iter()
        .map(|name| Frame::BulkString(name.clone()))
        .collect();
    Frame::Array(frames.into())
}

// ---------------------------------------------------------------------------
// GRAPH.QUERY, GRAPH.RO_QUERY, GRAPH.EXPLAIN
// ---------------------------------------------------------------------------

/// Literal-normalize the Cypher text for plan-cache keying.
///
/// Returns the effective text (normalized, or the original when nothing was
/// rewritten), its plan-cache hash, and the auto-extracted parameter values
/// to merge into the user params before execution.
fn normalize_cypher(
    cypher_bytes: &[u8],
) -> (
    std::borrow::Cow<'_, [u8]>,
    u64,
    Vec<(String, cypher::executor::Value)>,
) {
    match cypher::parameterize::parameterize(cypher_bytes) {
        Some(pq) => {
            let hash = cypher::planner::hash_query(&pq.normalized);
            (std::borrow::Cow::Owned(pq.normalized), hash, pq.auto_params)
        }
        None => (
            std::borrow::Cow::Borrowed(cypher_bytes),
            cypher::planner::hash_query(cypher_bytes),
            Vec::new(),
        ),
    }
}

/// Parse the effective (possibly literal-normalized) Cypher text.
///
/// If the normalized text fails to parse (a rewrite edge case), falls back
/// to the raw text so parse errors reference the user's own query — dropping
/// the auto-params and re-keying the plan cache on the raw hash. The rewrite
/// can therefore never turn a working query into a broken one.
fn parse_effective(
    raw: &[u8],
    effective: &std::borrow::Cow<'_, [u8]>,
    hash: u64,
    auto_params: Vec<(String, cypher::executor::Value)>,
) -> Result<
    (
        cypher::CypherQuery,
        u64,
        Vec<(String, cypher::executor::Value)>,
    ),
    String,
> {
    match cypher::parse_cypher(effective) {
        Ok(q) => Ok((q, hash, auto_params)),
        Err(e) => {
            if matches!(effective, std::borrow::Cow::Borrowed(_)) {
                return Err(format!("ERR Cypher parse error: {e}"));
            }
            match cypher::parse_cypher(raw) {
                Ok(q) => Ok((q, cypher::planner::hash_query(raw), Vec::new())),
                Err(e) => Err(format!("ERR Cypher parse error: {e}")),
            }
        }
    }
}

/// Execute a compiled read-only plan: params (user + auto-extracted),
/// valid-time, decay, executor, RESP encoding. Shared by GRAPH.QUERY (both
/// handlers) and GRAPH.RO_QUERY.
///
/// Takes a `SlotTable` explicitly (rather than calling `cypher::executor::
/// execute`, which rebuilds one every call) so plan-cache hits reuse the
/// `SlotTable` cached alongside the plan (Fix 2 -- one `String` allocation
/// per bound variable, per execution, otherwise).
///
/// `cache_protocol_version` (Task #32): `Some(v)` enables result-cache
/// POPULATION after a successful execution, encoding the reply for RESP
/// version `v` (2 or 3) into `graph.result_cache`. `None` means this call
/// site is not wired into the result cache at all (e.g. `graph_query_or_
/// write`'s read branches -- see module docs for the scope boundary): no
/// lookup happens here regardless (a hit is handled entirely by the
/// caller, BEFORE `run_read_query` is invoked at all -- this function only
/// ever runs on a miss), so `None` just skips the population step.
fn run_read_query(
    graph: &crate::graph::store::NamedGraph,
    args: &[Frame],
    plan: &cypher::PhysicalPlan,
    slots: &cypher::executor::SlotTable,
    auto_params: Vec<(String, cypher::executor::Value)>,
    cache_protocol_version: Option<u8>,
) -> Frame {
    let mut params = parse_params(args);
    for (name, value) in auto_params {
        params.insert(name, value);
    }
    let valid_at = parse_valid_at(args);
    let decay = match parse_decay(args) {
        Ok(d) => d,
        Err(msg) => return Frame::Error(Bytes::from_static(msg.as_bytes())),
    };
    let guard = match query_guard(args, 0) {
        Ok(g) => g,
        Err(msg) => return Frame::Error(Bytes::from_static(msg.as_bytes())),
    };
    let ctx = cypher::executor::ExecutionContext {
        valid_time_as_of: valid_at,
        decay,
        guard: Some(guard),
        ..Default::default()
    };
    // Race guard (Task #32): capture the write generation BEFORE execution,
    // store only if it is still unchanged AFTER -- on this shard thread
    // nothing else can mutate `graph` while this synchronous call is
    // running, but capturing before/comparing after costs one extra `u64`
    // read and keeps the invariant correct-by-construction even if this
    // function ever grows a yield point.
    let write_gen_before = graph.write_gen;
    match cypher::executor::execute_with_slots(graph, plan, slots, &params, &ctx) {
        Ok(r) => {
            if let Some(protocol_version) = cache_protocol_version {
                // Never cache decay queries (wall-clock dependent) or a
                // result computed against a graph state that mutated
                // mid-call (write_gen_before must still hold).
                if decay.is_none()
                    && graph.write_gen == write_gen_before
                    && !args.is_empty()
                    && args.len() >= 2
                {
                    if let Some(cypher_bytes) = extract_bulk(&args[1]) {
                        let key = result_cache_key(cypher_bytes, &args[2..]);
                        let frame = exec_result_to_frame(&r);
                        let mut buf = bytes::BytesMut::new();
                        if protocol_version >= 3 {
                            crate::protocol::serialize_resp3(&frame, &mut buf);
                        } else {
                            crate::protocol::serialize(&frame, &mut buf);
                        }
                        graph.result_cache.lock().put(
                            key,
                            write_gen_before,
                            protocol_version,
                            buf.freeze(),
                        );
                        return frame;
                    }
                }
            }
            exec_result_to_frame(&r)
        }
        Err(e) => {
            let msg = format!("ERR Cypher execution error: {e}");
            Frame::Error(Bytes::from(msg))
        }
    }
}

/// GRAPH.QUERY <graph> <cypher_string>
///
/// Normalizes literals into auto-parameters, then executes via the plan
/// cache: a cache hit runs with ZERO parse/compile work (the cache holds
/// read-only plans only, so a hit is safe to execute directly).
///
/// `protocol_version` (Task #32): `Some(v)` is the caller's negotiated RESP
/// version (2 or 3), threaded through to the Cypher result cache so a hit
/// replays correctly-encoded wire bytes and a miss populates the right
/// slot. `None` disables the result cache entirely for this call -- used by
/// call sites that cannot reliably determine the originating connection's
/// protocol version (e.g. the cross-shard `ShardMessage::GraphCommand` hop);
/// serving a wrong-protocol cached reply would be a real correctness bug
/// (RESP2/RESP3 wire formats differ), so those sites opt out rather than
/// guess.
pub fn graph_query(store: &GraphStore, args: &[Frame], protocol_version: Option<u8>) -> Frame {
    graph_query_readonly(store, args, false, protocol_version)
}

/// Shared read-path core for GRAPH.QUERY / GRAPH.RO_QUERY.
///
/// `reject_writes`: RO_QUERY refuses write clauses with an explicit error;
/// GRAPH.QUERY lets the read-only executor report them (it has no write lock).
fn graph_query_readonly(
    store: &GraphStore,
    args: &[Frame],
    reject_writes: bool,
    protocol_version: Option<u8>,
) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.QUERY' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let graph = match store.get_graph(graph_name) {
        Some(g) => g,
        None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
    };

    let cypher_bytes = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid Cypher query")),
    };

    // Task #32: Cypher result-cache lookup BEFORE any plan-cache/parse work
    // -- a hit skips plan lookup, param parsing, guard construction, and
    // execution entirely. Decay queries (wall-clock dependent) never
    // consult the cache regardless of `write_gen` freshness.
    if let Some(pv) = protocol_version {
        if !has_decay_flag(&args[2..]) {
            let key = result_cache_key(cypher_bytes, &args[2..]);
            if let Some(bytes) = graph.result_cache.lock().get(key, graph.write_gen, pv) {
                return Frame::PreSerialized(bytes);
            }
        }
    }

    // Raw-hash pre-lookup (Fix 2): an EXACT repeat of a query text we've
    // already compiled hits here without ever calling `parameterize()` (a
    // full lexer pass + Vec/String allocations) — the raw-hash entry
    // carries this exact text's auto_params, cached at insert time.
    let raw_hash = cypher::planner::hash_query(cypher_bytes);
    if let Some(cached) = graph.plan_cache.lock().get(raw_hash) {
        // W2-7 caches WRITE plans too — a write hit falls through to the
        // parse path, which reports it exactly like an uncached write query.
        if cached.read_only {
            let auto_params = cached.auto_params.as_ref().clone();
            return run_read_query(
                graph,
                args,
                &cached.plan,
                &cached.slots,
                auto_params,
                protocol_version,
            );
        }
    }

    let (effective, query_hash, auto_params) = normalize_cypher(cypher_bytes);

    // Normalized-hash READ-ONLY hit ⇒ no parse. `parameterize()` above
    // already ran (needed to derive THIS text's auto_params and the
    // normalized hash), but parse+compile is skipped. A cached WRITE plan
    // (W2-7) falls through to the parse path instead.
    let cached = graph.plan_cache.lock().get(query_hash);
    if let Some(cached) = cached {
        if cached.read_only {
            return run_read_query(
                graph,
                args,
                &cached.plan,
                &cached.slots,
                auto_params,
                protocol_version,
            );
        }
    }

    let (query, query_hash, auto_params) =
        match parse_effective(cypher_bytes, &effective, query_hash, auto_params) {
            Ok(t) => t,
            Err(msg) => return Frame::Error(Bytes::from(msg)),
        };

    if reject_writes && !query.is_read_only() {
        return Frame::Error(Bytes::from_static(
            b"ERR GRAPH.RO_QUERY does not allow write clauses (CREATE, DELETE, SET, MERGE)",
        ));
    }

    let plan = match cypher::planner::compile(&query) {
        Ok(p) => std::sync::Arc::new(p),
        Err(e) => {
            let msg = format!("ERR Cypher plan error: {e}");
            return Frame::Error(Bytes::from(msg));
        }
    };
    // Cache read-only plans only on this handler — write plans are cached
    // (flagged read_only=false) by the write handlers (W2-7), and every hit
    // above re-checks the flag. Insert under both the raw and normalized
    // hash so a later exact repeat of this text hits the allocation-free
    // raw-hash path above.
    let slots = if query.is_read_only() {
        graph.plan_cache.lock().insert_both(
            raw_hash,
            query_hash,
            plan.clone(),
            auto_params.clone(),
            true,
        )
    } else {
        std::sync::Arc::new(cypher::executor::SlotTable::from_plan(&plan))
    };
    run_read_query(graph, args, &plan, &slots, auto_params, protocol_version)
}

/// GRAPH.QUERY <graph> <cypher_string> — write-capable variant.
///
/// Called when the Cypher query contains write clauses (CREATE, DELETE, SET, MERGE).
/// Takes `&mut GraphStore` to allow mutable access to the named graph.
pub fn graph_query_write(store: &mut GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.QUERY' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let cypher_bytes = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid Cypher query")),
    };

    // W2-7: literal-normalize + plan-cache for the write path too. A hit on
    // a cached WRITE plan skips parse/compile entirely; per-run literal
    // values arrive through the auto-extracted parameters.
    let (effective, query_hash, auto_params) = normalize_cypher(cypher_bytes);
    let cached = store
        .get_graph(graph_name)
        .and_then(|g| g.plan_cache.lock().get(query_hash));

    let (plan, auto_params) = match cached {
        Some(cached) if !cached.read_only => (cached.plan, auto_params),
        // Read-only hit on the write handler is a dispatcher anomaly —
        // treat as a miss so classification runs as before. Same for a
        // genuine miss.
        _ => {
            let (query, query_hash, auto_params) =
                match parse_effective(cypher_bytes, &effective, query_hash, auto_params) {
                    Ok(t) => t,
                    Err(msg) => return Frame::Error(Bytes::from(msg)),
                };
            let plan = match cypher::planner::compile(&query) {
                Ok(p) => std::sync::Arc::new(p),
                Err(e) => {
                    let msg = format!("ERR Cypher plan error: {e}");
                    return Frame::Error(Bytes::from(msg));
                }
            };
            if let Some(g) = store.get_graph(graph_name) {
                g.plan_cache
                    .lock()
                    .insert(query_hash, plan.clone(), query.is_read_only());
            }
            (plan, auto_params)
        }
    };

    // Decay biases read-path traversal cost only; reject before any side
    // effect (LSN allocation, mutation) — same contract as the write branch
    // of `graph_query_or_write`.
    match parse_decay(args) {
        Ok(None) => {}
        Ok(Some(_)) => {
            return Frame::Error(Bytes::from_static(
                b"ERR --decay requires a read-only Cypher query",
            ));
        }
        Err(msg) => return Frame::Error(Bytes::from_static(msg.as_bytes())),
    }

    let lsn = store.allocate_lsn();

    // Scoped borrow: get mutable graph, execute, release borrow before WAL push.
    let result = {
        let graph = match store.get_graph_mut(graph_name) {
            Some(g) => g,
            None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
        };

        // Merge auto-extracted literal values: the plan was compiled from
        // the normalized text, so `$__pN` parameters must resolve.
        let mut params = parse_params(args);
        for (name, value) in auto_params {
            params.insert(name, value);
        }
        match cypher::executor::execute_mut(graph, &plan, &params, lsn) {
            Ok(r) => r,
            Err(e) => {
                let msg = format!("ERR Cypher execution error: {e}");
                return Frame::Error(Bytes::from(msg));
            }
        }
    };

    // Generate WAL records for mutations performed during execution.
    for mutation in &result.mutations {
        match mutation {
            cypher::executor::MutationRecord::CreateNode {
                node_id,
                labels,
                properties,
                embedding,
            } => {
                store
                    .wal_pending
                    .push(crate::graph::wal::serialize_add_node(
                        graph_name,
                        *node_id,
                        labels,
                        properties,
                        embedding.as_deref(),
                    ));
            }
            cypher::executor::MutationRecord::CreateEdge {
                edge_id,
                src_id,
                dst_id,
                edge_type,
                weight,
                properties,
            } => {
                store
                    .wal_pending
                    .push(crate::graph::wal::serialize_add_edge(
                        graph_name,
                        *edge_id,
                        *src_id,
                        *dst_id,
                        *edge_type,
                        *weight,
                        properties.as_ref(),
                    ));
            }
            // W2-2: DELETEs must be WAL-logged or the entity RESURRECTS at
            // restart (replay re-adds it from its CreateNode/AddNode record;
            // frozen-tier tombstones are pure write-buf state otherwise).
            cypher::executor::MutationRecord::DeleteNode { node_id, .. } => {
                store
                    .wal_pending
                    .push(crate::graph::wal::serialize_remove_node(
                        graph_name, *node_id,
                    ));
            }
            cypher::executor::MutationRecord::DeleteEdge { edge_id, .. } => {
                store
                    .wal_pending
                    .push(crate::graph::wal::serialize_remove_edge(
                        graph_name, *edge_id,
                    ));
            }
            // W2-9: SET must be WAL-logged or a restart replays the original
            // ADDNODE property state (the crash suite's G1/G3 caught exactly
            // this loss).
            cypher::executor::MutationRecord::SetProperty {
                entity_id,
                is_node,
                key,
                new_value,
                ..
            } => {
                store
                    .wal_pending
                    .push(crate::graph::wal::serialize_set_prop(
                        graph_name, *entity_id, *is_node, *key, new_value,
                    ));
            }
            cypher::executor::MutationRecord::SetLabel { node_id, label } => {
                store
                    .wal_pending
                    .push(crate::graph::wal::serialize_set_label(
                        graph_name, *node_id, *label,
                    ));
            }
        }
    }

    // Bump version if any mutations were executed (Cypher write query).
    // Task #32: also invalidate the graph's cached query results -- gated
    // on `!result.mutations.is_empty()` so an idempotent MERGE match-branch
    // (zero mutation records) doesn't pay an invalidation for a no-op.
    if !result.mutations.is_empty() {
        store.bump_version();
        if let Some(graph) = store.get_graph_mut(graph_name) {
            graph.touch();
        }
    }

    exec_result_to_frame(&result)
}

/// GRAPH.QUERY <graph> <cypher_string> — auto-routing handler.
///
/// Parses the Cypher query once, then dispatches to the read or write path
/// based on whether the query contains write clauses.
///
/// Returns `(Frame, Vec<GraphWriteIntent>, Vec<GraphUndoOp>)`. Read-only
/// queries and failed write queries return empty vectors. Phase 167
/// (CYP-01/02) — handlers forward intents into `CrossStoreTxn::record_graph`
/// so TXN.ABORT can roll back CREATE/MERGE-created entities via
/// [`crate::transaction::abort::abort_cross_store_txn`]. Phase 174 FIX-01
/// adds `GraphUndoOp`s for SET/DELETE/MERGE rollback.
pub fn graph_query_or_write(
    store: &mut GraphStore,
    args: &[Frame],
) -> (
    Frame,
    Vec<cypher::executor::GraphWriteIntent>,
    Vec<crate::transaction::GraphUndoOp>,
) {
    if args.len() < 2 {
        return (
            Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'GRAPH.QUERY' command",
            )),
            Vec::new(),
            Vec::new(),
        );
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => {
            return (
                Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
                Vec::new(),
                Vec::new(),
            );
        }
    };

    let cypher_bytes = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => {
            return (
                Frame::Error(Bytes::from_static(b"ERR invalid Cypher query")),
                Vec::new(),
                Vec::new(),
            );
        }
    };

    // Raw-hash pre-lookup (Fix 2): an EXACT repeat of a query text we've
    // already compiled hits here without ever calling `parameterize()`.
    // W2-7: a read-only hit routes to the read path, a WRITE hit to the
    // write path — both with zero parse/compile work.
    let raw_hash = cypher::planner::hash_query(cypher_bytes);
    let raw_cached = store
        .get_graph(graph_name)
        .and_then(|g| g.plan_cache.lock().get(raw_hash));
    if let Some(cached) = raw_cached {
        let auto_params = cached.auto_params.as_ref().clone();
        if cached.read_only {
            let Some(graph) = store.get_graph(graph_name) else {
                return (
                    Frame::Error(Bytes::from_static(b"ERR graph not found")),
                    Vec::new(),
                    Vec::new(),
                );
            };
            return (
                // Task #32: this auto-routing entry point is NOT wired into
                // the result cache (`None`) -- see module docs for the
                // scope boundary (protocol_version is not reliably
                // available at every caller of `graph_query_or_write`,
                // e.g. the cross-shard `ShardMessage::GraphCommand` hop).
                run_read_query(graph, args, &cached.plan, &cached.slots, auto_params, None),
                Vec::new(),
                Vec::new(),
            );
        }
        return execute_write_plan(store, graph_name, args, &cached.plan, auto_params);
    }

    let (effective, query_hash, auto_params) = normalize_cypher(cypher_bytes);

    // Fast path: normalized-hash plan-cache hit — a read-only plan routes to
    // the read path, a WRITE plan (W2-7) to the write path; both with ZERO
    // parse/compile work (per-run literal values arrive via the auto-params).
    let cached = store
        .get_graph(graph_name)
        .and_then(|g| g.plan_cache.lock().get(query_hash));
    if let Some(cached) = cached {
        if cached.read_only {
            let Some(graph) = store.get_graph(graph_name) else {
                return (
                    Frame::Error(Bytes::from_static(b"ERR graph not found")),
                    Vec::new(),
                    Vec::new(),
                );
            };
            return (
                // Task #32: see the raw-hash branch above -- not wired here.
                run_read_query(graph, args, &cached.plan, &cached.slots, auto_params, None),
                Vec::new(),
                Vec::new(),
            );
        }
        return execute_write_plan(store, graph_name, args, &cached.plan, auto_params);
    }

    // Slow path: parse once (normalized text, raw fallback) and classify.
    let (query, query_hash, auto_params) =
        match parse_effective(cypher_bytes, &effective, query_hash, auto_params) {
            Ok(t) => t,
            Err(msg) => return (Frame::Error(Bytes::from(msg)), Vec::new(), Vec::new()),
        };

    if query.is_read_only() {
        // Read path: compile plan, cache it, execute read-only.
        let graph = match store.get_graph(graph_name) {
            Some(g) => g,
            None => {
                return (
                    Frame::Error(Bytes::from_static(b"ERR graph not found")),
                    Vec::new(),
                    Vec::new(),
                );
            }
        };

        let plan = match cypher::planner::compile(&query) {
            Ok(p) => std::sync::Arc::new(p),
            Err(e) => {
                let msg = format!("ERR Cypher plan error: {e}");
                return (Frame::Error(Bytes::from(msg)), Vec::new(), Vec::new());
            }
        };
        // Insert under both hashes so a later exact repeat of this text
        // hits the allocation-free raw-hash path above.
        let slots = graph.plan_cache.lock().insert_both(
            raw_hash,
            query_hash,
            plan.clone(),
            auto_params.clone(),
            true,
        );

        (
            // Task #32: see the raw-hash branch above -- not wired here.
            run_read_query(graph, args, &plan, &slots, auto_params, None),
            Vec::new(),
            Vec::new(),
        )
    } else {
        let plan = match cypher::planner::compile(&query) {
            Ok(p) => std::sync::Arc::new(p),
            Err(e) => {
                let msg = format!("ERR Cypher plan error: {e}");
                return (Frame::Error(Bytes::from(msg)), Vec::new(), Vec::new());
            }
        };
        // W2-7: cache the write plan (flagged) so the next occurrence of
        // this normalized text — or an exact repeat via the raw-hash
        // pre-lookup — skips parse + compile entirely.
        if let Some(g) = store.get_graph(graph_name) {
            let _ = g.plan_cache.lock().insert_both(
                raw_hash,
                query_hash,
                plan.clone(),
                auto_params.clone(),
                false,
            );
        }
        execute_write_plan(store, graph_name, args, &plan, auto_params)
    }
}

/// Write-execution tail shared by `graph_query_or_write`'s compile path and
/// its W2-7 plan-cache hit path: decay validation (before any side effect),
/// LSN allocation, `execute_mut` with auto-params merged, and the
/// mutation → WAL / txn-intent / undo-op fan-out.
fn execute_write_plan(
    store: &mut GraphStore,
    graph_name: &[u8],
    args: &[Frame],
    plan: &cypher::PhysicalPlan,
    auto_params: Vec<(String, cypher::executor::Value)>,
) -> (
    Frame,
    Vec<cypher::executor::GraphWriteIntent>,
    Vec<crate::transaction::GraphUndoOp>,
) {
    // Decay biases read-path traversal cost only; a write query must not
    // silently accept (or skip validating) the flag. Reject before any
    // side effect (LSN allocation, mutation).
    match parse_decay(args) {
        Ok(None) => {}
        Ok(Some(_)) => {
            return (
                Frame::Error(Bytes::from_static(
                    b"ERR --decay requires a read-only Cypher query",
                )),
                Vec::new(),
                Vec::new(),
            );
        }
        Err(msg) => {
            return (
                Frame::Error(Bytes::from_static(msg.as_bytes())),
                Vec::new(),
                Vec::new(),
            );
        }
    }

    let lsn = store.allocate_lsn();

    // Phase 174 FIX-02: extract mutations regardless of Ok/Err so that
    // partial writes from before the error are visible to TXN.ABORT.
    let (result_or_err, mutations) = {
        let graph = match store.get_graph_mut(graph_name) {
            Some(g) => g,
            None => {
                return (
                    Frame::Error(Bytes::from_static(b"ERR graph not found")),
                    Vec::new(),
                    Vec::new(),
                );
            }
        };

        // Merge auto-extracted literal values: the write plan was
        // compiled from the normalized text, so `$__pN` parameters must
        // resolve or CREATE would store Nulls.
        let mut params = parse_params(args);
        for (name, value) in auto_params {
            params.insert(name, value);
        }
        match cypher::executor::execute_mut(graph, plan, &params, lsn) {
            Ok(r) => {
                let muts = r.mutations;
                (
                    Ok(cypher::executor::ExecResult {
                        columns: r.columns,
                        rows: r.rows,
                        nodes_created: r.nodes_created,
                        nodes_deleted: r.nodes_deleted,
                        properties_set: r.properties_set,
                        execution_time_us: r.execution_time_us,
                        mutations: Vec::new(), // moved out above
                    }),
                    muts,
                )
            }
            Err(e) => {
                let msg = format!("ERR Cypher execution error: {e}");
                (Err(msg), e.partial_mutations)
            }
        }
    };

    // Phase 167: collect write intents for CrossStoreTxn rollback. Every
    // CreateNode/CreateEdge mutation (from CreatePattern and the Merge
    // create-branch) becomes an intent; MERGE match-branches produce no
    // mutation and therefore no intent (idempotent rollback).
    let mut intents: Vec<cypher::executor::GraphWriteIntent> = Vec::with_capacity(mutations.len());

    // Phase 174 FIX-01: collect undo ops for SET/DELETE/MERGE rollback.
    let gname_bytes = Bytes::copy_from_slice(graph_name);
    let mut undo_ops: Vec<crate::transaction::GraphUndoOp> = Vec::new();

    // Generate WAL records for mutations + collect intents/undo ops.
    for mutation in &mutations {
        match mutation {
            cypher::executor::MutationRecord::CreateNode {
                node_id,
                labels,
                properties,
                embedding,
            } => {
                intents.push(cypher::executor::GraphWriteIntent {
                    entity_id: *node_id,
                    is_node: true,
                });
                store
                    .wal_pending
                    .push(crate::graph::wal::serialize_add_node(
                        graph_name,
                        *node_id,
                        labels,
                        properties,
                        embedding.as_deref(),
                    ));
            }
            cypher::executor::MutationRecord::CreateEdge {
                edge_id,
                src_id,
                dst_id,
                edge_type,
                weight,
                properties,
            } => {
                intents.push(cypher::executor::GraphWriteIntent {
                    entity_id: *edge_id,
                    is_node: false,
                });
                store
                    .wal_pending
                    .push(crate::graph::wal::serialize_add_edge(
                        graph_name,
                        *edge_id,
                        *src_id,
                        *dst_id,
                        *edge_type,
                        *weight,
                        properties.as_ref(),
                    ));
            }
            // Phase 174 FIX-01: new variants for SET/DELETE/MERGE rollback.
            cypher::executor::MutationRecord::SetProperty {
                entity_id,
                is_node,
                key,
                old_value,
                new_value,
            } => {
                undo_ops.push(crate::transaction::GraphUndoOp::RestoreProperty {
                    graph_name: gname_bytes.clone(),
                    entity_id: *entity_id,
                    is_node: *is_node,
                    prop_key: *key,
                    old_value: old_value.clone(),
                });
                // W2-9: WAL the SET or it is silently lost on kill -9 (replay
                // re-runs the original ADDNODE property state).
                store
                    .wal_pending
                    .push(crate::graph::wal::serialize_set_prop(
                        graph_name, *entity_id, *is_node, *key, new_value,
                    ));
            }
            // W2-9: WAL-only — label rollback was never captured (pre-existing
            // Phase 174 scope), so no undo op here.
            cypher::executor::MutationRecord::SetLabel { node_id, label } => {
                store
                    .wal_pending
                    .push(crate::graph::wal::serialize_set_label(
                        graph_name, *node_id, *label,
                    ));
            }
            cypher::executor::MutationRecord::DeleteNode { node_id, .. } => {
                undo_ops.push(crate::transaction::GraphUndoOp::UndeleteNode {
                    graph_name: gname_bytes.clone(),
                    node_id: *node_id,
                    delete_lsn: lsn,
                });
                // W2-2: WAL the delete or it resurrects at restart.
                store
                    .wal_pending
                    .push(crate::graph::wal::serialize_remove_node(
                        graph_name, *node_id,
                    ));
            }
            cypher::executor::MutationRecord::DeleteEdge { edge_id, .. } => {
                undo_ops.push(crate::transaction::GraphUndoOp::UndeleteEdge {
                    graph_name: gname_bytes.clone(),
                    edge_id: *edge_id,
                });
                store
                    .wal_pending
                    .push(crate::graph::wal::serialize_remove_edge(
                        graph_name, *edge_id,
                    ));
            }
        }
    }

    // Task #32: invalidate the graph's cached query results whenever this
    // write actually produced mutations (mirrors the `graph_query_write`
    // gate -- an idempotent MERGE match-branch must not pay an
    // invalidation for a no-op). Runs on both Ok and Err (Phase 174 FIX-02:
    // `mutations` already includes partial pre-error writes).
    if !mutations.is_empty() {
        if let Some(graph) = store.get_graph_mut(graph_name) {
            graph.touch();
        }
    }

    // Phase 174 FIX-02: return intents/undo_ops on BOTH Ok and Err paths
    // so TXN.ABORT can roll back partial writes from before the error.
    match result_or_err {
        Ok(ref result) => (exec_result_to_frame(result), intents, undo_ops),
        Err(msg) => (Frame::Error(Bytes::from(msg)), intents, undo_ops),
    }
}

/// GRAPH.RO_QUERY <graph> <cypher_string>
///
/// Like GRAPH.QUERY but rejects write clauses (CREATE, DELETE, SET, MERGE).
/// Shares the single-parse read core with GRAPH.QUERY — a plan-cache hit is
/// read-only by the cache invariant, so no classification re-parse is needed.
pub fn graph_ro_query(store: &GraphStore, args: &[Frame], protocol_version: Option<u8>) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.RO_QUERY' command",
        ));
    }
    graph_query_readonly(store, args, true, protocol_version)
}

/// GRAPH.EXPLAIN <graph> <cypher_string>
///
/// Returns the execution plan without running the query.
/// Includes cost-based strategy selection when graph stats are available.
pub fn graph_explain(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.EXPLAIN' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let cypher_bytes = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid Cypher query")),
    };

    let query = match cypher::parse_cypher(cypher_bytes) {
        Ok(q) => q,
        Err(e) => {
            let msg = format!("ERR Cypher parse error: {e}");
            return Frame::Error(Bytes::from(msg));
        }
    };

    let plan = match cypher::planner::compile(&query) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("ERR Cypher plan error: {e}");
            return Frame::Error(Bytes::from(msg));
        }
    };

    // Return plan as a formatted string.
    let mut output = String::new();
    for (i, op) in plan.operators.iter().enumerate() {
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(&format!("{i}: {op:?}"));
    }

    // Append cost-based strategy selection if graph exists.
    if let Some(graph) = store.get_graph(graph_name) {
        let stats = &graph.stats;

        // Extract traversal parameters from the plan operators.
        let hops = extract_max_hops(&plan);
        let k = 10u32; // Default k for vector search.
        let dim = 128u32; // Default dimension estimate.

        let estimate = cypher::planner::select_strategy(
            stats, 1, // start_nodes (single seed)
            hops, k, dim, None, // No specific start node degree without node ID.
        );

        output.push_str(&format!(
            "\n--- Cost Estimation ---\nStrategy: {}\nGraph-first cost: {:.1}\nVector-first cost: {:.1}\nHub detected: {}",
            estimate.strategy,
            estimate.graph_first_cost,
            estimate.vector_first_cost,
            estimate.hub_detected,
        ));
    }

    Frame::BulkString(Bytes::from(output))
}

/// GRAPH.PROFILE <graph> <cypher_string>
///
/// Execute query with per-operator timing. Returns `[results, operator_profiles]`.
pub fn graph_profile(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.PROFILE' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let graph = match store.get_graph(graph_name) {
        Some(g) => g,
        None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
    };

    let cypher_bytes = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid Cypher query")),
    };

    let query = match cypher::parse_cypher(cypher_bytes) {
        Ok(q) => q,
        Err(e) => {
            let msg = format!("ERR Cypher parse error: {e}");
            return Frame::Error(Bytes::from(msg));
        }
    };

    let plan = match cypher::planner::compile(&query) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("ERR Cypher plan error: {e}");
            return Frame::Error(Bytes::from(msg));
        }
    };

    let params = std::collections::HashMap::new();
    let valid_at = parse_valid_at(args);
    let decay = match parse_decay(args) {
        Ok(d) => d,
        Err(msg) => return Frame::Error(Bytes::from_static(msg.as_bytes())),
    };
    let guard = match query_guard(args, 0) {
        Ok(g) => g,
        Err(msg) => return Frame::Error(Bytes::from_static(msg.as_bytes())),
    };
    let ctx = cypher::executor::ExecutionContext {
        valid_time_as_of: valid_at,
        decay,
        guard: Some(guard),
        ..Default::default()
    };
    let profile = match cypher::executor::execute_profile(graph, &plan, &params, &ctx) {
        Ok(r) => r,
        Err(e) => {
            let msg = format!("ERR Cypher execution error: {e}");
            return Frame::Error(Bytes::from(msg));
        }
    };

    profile_result_to_frame(&profile)
}

/// Convert a `ProfileResult` to a RESP3 Frame.
///
/// Format: Array [
///   exec_result_frame,       // same format as GRAPH.QUERY
///   Array [                  // per-operator profiles
///     Array [name, row_count, duration_us],
///     ...
///   ]
/// ]
fn profile_result_to_frame(profile: &cypher::executor::ProfileResult) -> Frame {
    let result_frame = exec_result_to_frame(&profile.exec_result);

    let op_frames: Vec<Frame> = profile
        .operator_profiles
        .iter()
        .map(|op| {
            Frame::Array(
                vec![
                    Frame::BulkString(Bytes::from(op.name)),
                    Frame::Integer(op.row_count as i64),
                    Frame::Integer(op.duration_us as i64),
                ]
                .into(),
            )
        })
        .collect();

    Frame::Array(vec![result_frame, Frame::Array(op_frames.into())].into())
}

/// Convert an `ExecResult` to a RESP3 Frame.
///
/// Format: Array [
///   Array [column_name_1, column_name_2, ...],   // headers
///   Array [ Array [val1, val2, ...], ... ],        // rows
///   BulkString "Nodes created: N, ..."             // stats
/// ]
fn exec_result_to_frame(result: &cypher::executor::ExecResult) -> Frame {
    // 1. Headers
    let headers: Vec<Frame> = result
        .columns
        .iter()
        .map(|c| Frame::BulkString(Bytes::from(c.clone())))
        .collect();

    // 2. Rows -- each row is an array of values.
    let rows: Vec<Frame> = result
        .rows
        .iter()
        .map(|row| {
            let cells: Vec<Frame> = row.iter().map(value_to_frame).collect();
            Frame::Array(cells.into())
        })
        .collect();

    // 3. Stats — use write! to pre-allocated buffer instead of format!()
    let mut stats_buf = Vec::with_capacity(128);
    use std::io::Write as _;
    let _ = write!(
        stats_buf,
        "Nodes created: {}, Nodes deleted: {}, Properties set: {}, \
         Query internal execution time: {} us",
        result.nodes_created, result.nodes_deleted, result.properties_set, result.execution_time_us
    );

    Frame::Array(
        vec![
            Frame::Array(headers.into()),
            Frame::Array(rows.into()),
            Frame::BulkString(Bytes::from(stats_buf)),
        ]
        .into(),
    )
}

/// Convert an executor Value to a Frame.
///
/// Uses `itoa` + stack buffer for Node/Edge IDs to avoid per-row `format!()`
/// heap allocations on the query result serialization hot path.
fn value_to_frame(value: &cypher::executor::Value) -> Frame {
    use cypher::executor::Value;
    match value {
        Value::Null => Frame::Null,
        Value::Int(n) => Frame::Integer(*n),
        Value::Float(f) => Frame::Double(*f),
        // Zero-copy (W2-4): the stored property's Bytes flows straight into
        // the reply frame — a refcount bump, not an allocation.
        Value::String(s) => Frame::BulkString(s.clone()),
        Value::Bool(b) => Frame::Boolean(*b),
        Value::Node(key) => {
            // "node:" (5) + max u64 (20 digits) = 25 bytes max
            let mut buf = [0u8; 32];
            buf[..5].copy_from_slice(b"node:");
            let mut itoa_buf = itoa::Buffer::new();
            let n = itoa_buf.format(key.data().as_ffi());
            let end = 5 + n.len();
            buf[5..end].copy_from_slice(n.as_bytes());
            Frame::BulkString(Bytes::copy_from_slice(&buf[..end]))
        }
        Value::Edge(key) => {
            let mut buf = [0u8; 32];
            buf[..5].copy_from_slice(b"edge:");
            let mut itoa_buf = itoa::Buffer::new();
            let n = itoa_buf.format(key.data().as_ffi());
            let end = 5 + n.len();
            buf[5..end].copy_from_slice(n.as_bytes());
            Frame::BulkString(Bytes::copy_from_slice(&buf[..end]))
        }
        Value::List(items) => {
            let frames: Vec<Frame> = items.iter().map(value_to_frame).collect();
            Frame::Array(frames.into())
        }
        Value::Map(entries) => {
            let pairs: Vec<(Frame, Frame)> = entries
                .iter()
                .map(|(k, v)| (Frame::BulkString(Bytes::from(k.clone())), value_to_frame(v)))
                .collect();
            Frame::Map(pairs)
        }
        // v0.1.9 CYP-04/05: serialize a Path as Array[Integer] of node IDs.
        // Clients can reconstruct edge IDs via GRAPH.NEIGHBORS between
        // consecutive node IDs; that keeps the wire format forward-
        // compatible with a richer Path frame in a future release.
        Value::Path(nodes) => {
            let frames: Vec<Frame> = nodes
                .iter()
                .map(|k| Frame::Integer(k.data().as_ffi() as i64))
                .collect();
            Frame::Array(frames.into())
        }
    }
}

/// Extract the maximum hop count from Expand operators in a physical plan.
fn extract_max_hops(plan: &cypher::planner::PhysicalPlan) -> u32 {
    let mut max_hops = 1u32;
    for op in &plan.operators {
        if let cypher::planner::PhysicalOp::Expand { max_hops: mh, .. } = op {
            if *mh > max_hops {
                max_hops = *mh;
            }
        }
    }
    max_hops
}

// ---------------------------------------------------------------------------
// RESP3 entity formatting
// ---------------------------------------------------------------------------

/// Format a node as a RESP3 Map: {id, labels, properties}.
/// Format a CSR-only edge from a MergedNeighbor as a RESP3 Map.
/// Used when the edge exists only in immutable CSR segments.
fn merged_edge_to_frame(merged: &crate::graph::traversal::MergedNeighbor) -> Frame {
    Frame::Map(vec![
        (
            Frame::SimpleString(Bytes::from_static(b"id")),
            Frame::Integer(merged.edge.data().as_ffi() as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"type")),
            Frame::Integer(merged.edge_type as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"weight")),
            Frame::Double(merged.weight),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"properties")),
            Frame::Map(Vec::new()),
        ),
    ])
}

/// Format a CSR-only node from a MergedNeighbor as a RESP3 Map.
/// Used when the node exists only in immutable CSR segments.
fn merged_node_to_frame(merged: &crate::graph::traversal::MergedNeighbor) -> Frame {
    let external_id = merged.node.data().as_ffi();
    Frame::Map(vec![
        (
            Frame::SimpleString(Bytes::from_static(b"id")),
            Frame::Integer(external_id as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"labels")),
            Frame::Array(Vec::new().into()),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"properties")),
            Frame::Map(Vec::new()),
        ),
    ])
}

fn node_to_frame(
    key: crate::graph::types::NodeKey,
    node: &crate::graph::types::MutableNode,
) -> Frame {
    let external_id = key.data().as_ffi();

    let labels: Vec<Frame> = node
        .labels
        .iter()
        .map(|&l| Frame::Integer(l as i64))
        .collect();

    let props = properties_to_frame(&node.properties);

    Frame::Map(vec![
        (
            Frame::SimpleString(Bytes::from_static(b"id")),
            Frame::Integer(external_id as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"labels")),
            Frame::Array(labels.into()),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"properties")),
            props,
        ),
    ])
}

/// Format an edge as a RESP3 Map: {id, type, src, dst, properties}.
fn edge_to_frame(
    key: crate::graph::types::EdgeKey,
    edge: &crate::graph::types::MutableEdge,
) -> Frame {
    let external_id = key.data().as_ffi();

    let src_ext = edge.src.data().as_ffi();
    let dst_ext = edge.dst.data().as_ffi();

    let props = match &edge.properties {
        Some(p) => properties_to_frame(p),
        None => Frame::Map(Vec::new()),
    };

    Frame::Map(vec![
        (
            Frame::SimpleString(Bytes::from_static(b"id")),
            Frame::Integer(external_id as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"type")),
            Frame::Integer(edge.edge_type as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"src")),
            Frame::Integer(src_ext as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"dst")),
            Frame::Integer(dst_ext as i64),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"weight")),
            Frame::Double(edge.weight),
        ),
        (
            Frame::SimpleString(Bytes::from_static(b"properties")),
            props,
        ),
    ])
}

/// Convert a PropertyMap to a RESP3 Map frame.
fn properties_to_frame(props: &crate::graph::types::PropertyMap) -> Frame {
    let pairs: Vec<(Frame, Frame)> = props
        .iter()
        .map(|(key, val)| {
            let k = Frame::Integer(*key as i64);
            let v = match val {
                crate::graph::types::PropertyValue::Int(n) => Frame::Integer(*n),
                crate::graph::types::PropertyValue::Float(f) => Frame::Double(*f),
                crate::graph::types::PropertyValue::String(s) => Frame::BulkString(s.clone()),
                crate::graph::types::PropertyValue::Bool(b) => Frame::Boolean(*b),
                crate::graph::types::PropertyValue::Bytes(b) => Frame::BulkString(b.clone()),
            };
            (k, v)
        })
        .collect();
    Frame::Map(pairs)
}

// ---------------------------------------------------------------------------
// GRAPH.VSEARCH — graph-filtered vector search (HYB-01)
// ---------------------------------------------------------------------------

/// GRAPH.VSEARCH <graph> <start_node_id> <hops> <k> <vector_blob> [THRESHOLD <n>] [TYPE <edge_type>]
///
/// Traverses `hops` from `start_node_id`, collects candidate nodes, then scores
/// by cosine similarity to `vector_blob`. Returns top-K results.
pub fn graph_vsearch(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 5 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.VSEARCH' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let start_id = match parse_u64(&args[1]) {
        Some(id) => id,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid start node ID")),
    };

    let hops = match parse_u32(&args[2]) {
        Some(h) if h > 0 && h <= 10 => h,
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid hops (1-10)")),
    };

    let k = match parse_u32(&args[3]) {
        Some(k) if k > 0 => k as usize,
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid k")),
    };

    let query_vector = match extract_f32_vector(&args[4]) {
        Some(v) if !v.is_empty() => v,
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid vector blob")),
    };

    let graph = match store.get_graph(graph_name) {
        Some(g) => g,
        None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
    };

    // Parse optional args.
    let mut threshold = crate::graph::hybrid::DEFAULT_STRATEGY_THRESHOLD;
    let mut edge_type_filter: Option<u16> = None;
    let mut pos = 5;
    while pos < args.len() {
        let key = match extract_bulk(&args[pos]) {
            Some(b) => b,
            None => {
                pos += 1;
                continue;
            }
        };
        if key.eq_ignore_ascii_case(b"THRESHOLD") {
            pos += 1;
            if pos < args.len() {
                if let Some(t) = parse_u32(&args[pos]) {
                    threshold = t as usize;
                }
            }
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"TYPE") {
            pos += 1;
            if pos < args.len() {
                if let Some(type_name) = extract_bulk(&args[pos]) {
                    edge_type_filter = Some(super::graph_write::label_to_id(type_name));
                }
            }
            pos += 1;
        } else {
            pos += 1;
        }
    }

    let node_key = super::graph_write::external_id_to_node_key(start_id);
    let memgraph = &graph.write_buf;
    let segments_guard = graph.segments.load();
    let csr_segs = &segments_guard.immutable;
    let lsn = u64::MAX - 1;

    let mut search =
        crate::graph::hybrid::GraphFilteredSearch::new(node_key, hops, query_vector, k);
    search.threshold = threshold;
    search.edge_type_filter = edge_type_filter;

    match search.execute(memgraph, csr_segs, lsn) {
        Ok(results) => hybrid_results_to_frame(&results),
        Err(e) => Frame::Error(Bytes::from(format!("ERR {e}"))),
    }
}

// ---------------------------------------------------------------------------
// GRAPH.HYBRID — general hybrid query dispatcher
// ---------------------------------------------------------------------------

/// GRAPH.HYBRID <graph> <mode> <args...>
///
/// Modes:
///   FILTER <start_id> <hops> <k> <vector> — graph-filtered vector search (HYB-01)
///   EXPAND <k> <expansion_hops> <vector> — vector-to-graph expansion (HYB-02)
///   WALK <start_id> <max_depth> <beam_width> <min_sim> <vector> — vector-guided walk (HYB-03)
///   RERANK <ref_node_id> <max_hops> <alpha> <k> <vector> — graph-constrained re-ranking (HYB-04)
pub fn graph_hybrid(store: &GraphStore, args: &[Frame]) -> Frame {
    if args.len() < 3 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'GRAPH.HYBRID' command",
        ));
    }

    let graph_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid graph name")),
    };

    let mode = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid mode")),
    };

    let graph = match store.get_graph(graph_name) {
        Some(g) => g,
        None => return Frame::Error(Bytes::from_static(b"ERR graph not found")),
    };

    let memgraph = &graph.write_buf;
    let segments_guard = graph.segments.load();
    let csr_segs = &segments_guard.immutable;
    let lsn = u64::MAX - 1;

    if mode.eq_ignore_ascii_case(b"FILTER") {
        // GRAPH.HYBRID g FILTER <start_id> <hops> <k> <vector>
        if args.len() < 6 {
            return Frame::Error(Bytes::from_static(
                b"ERR FILTER requires: start_id hops k vector",
            ));
        }
        let start_id = match parse_u64(&args[2]) {
            Some(id) => id,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid start node ID")),
        };
        let hops = match parse_u32(&args[3]) {
            Some(h) if h > 0 && h <= 10 => h,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid hops")),
        };
        let k = match parse_u32(&args[4]) {
            Some(k) if k > 0 => k as usize,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid k")),
        };
        let query_vector = match extract_f32_vector(&args[5]) {
            Some(v) if !v.is_empty() => v,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid vector")),
        };

        let node_key = super::graph_write::external_id_to_node_key(start_id);
        let search =
            crate::graph::hybrid::GraphFilteredSearch::new(node_key, hops, query_vector, k);
        match search.execute(memgraph, csr_segs, lsn) {
            Ok(results) => hybrid_results_to_frame(&results),
            Err(e) => Frame::Error(Bytes::from(format!("ERR {e}"))),
        }
    } else if mode.eq_ignore_ascii_case(b"WALK") {
        // GRAPH.HYBRID g WALK <start_id> <max_depth> <beam_width> <min_sim> <vector>
        if args.len() < 7 {
            return Frame::Error(Bytes::from_static(
                b"ERR WALK requires: start_id max_depth beam_width min_sim vector",
            ));
        }
        let start_id = match parse_u64(&args[2]) {
            Some(id) => id,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid start node ID")),
        };
        let max_depth = match parse_u32(&args[3]) {
            Some(d) if d > 0 && d <= 100 => d,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid max_depth")),
        };
        let beam_width = match parse_u32(&args[4]) {
            Some(bw) if bw > 0 => bw as usize,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid beam_width")),
        };
        let min_sim = match parse_f64(&args[5]) {
            Some(s) => s,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid min_sim")),
        };
        let query_vector = match extract_f32_vector(&args[6]) {
            Some(v) if !v.is_empty() => v,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid vector")),
        };

        let node_key = super::graph_write::external_id_to_node_key(start_id);
        let mut walk =
            crate::graph::hybrid::VectorGuidedWalk::new(node_key, query_vector, max_depth);
        walk.beam_width = beam_width;
        walk.min_similarity = min_sim;

        match walk.execute(memgraph, csr_segs, lsn) {
            Ok(results) => hybrid_results_to_frame(&results),
            Err(e) => Frame::Error(Bytes::from(format!("ERR {e}"))),
        }
    } else if mode.eq_ignore_ascii_case(b"RERANK") {
        // GRAPH.HYBRID g RERANK <ref_node_id> <max_hops> <alpha> <k> <vector>
        if args.len() < 7 {
            return Frame::Error(Bytes::from_static(
                b"ERR RERANK requires: ref_node_id max_hops alpha k vector",
            ));
        }
        let ref_id = match parse_u64(&args[2]) {
            Some(id) => id,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid ref node ID")),
        };
        let max_hops = match parse_u32(&args[3]) {
            Some(h) if h > 0 && h <= 10 => h,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid max_hops (1..10)")),
        };
        let alpha = match parse_f64(&args[4]) {
            Some(a) => a.clamp(0.0, 1.0),
            None => return Frame::Error(Bytes::from_static(b"ERR invalid alpha")),
        };
        let k = match parse_u32(&args[5]) {
            Some(k) if k > 0 => k as usize,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid k")),
        };
        let query_vector = match extract_f32_vector(&args[6]) {
            Some(v) if !v.is_empty() => v,
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid vector")),
        };

        let node_key = super::graph_write::external_id_to_node_key(ref_id);
        let reranker = crate::graph::hybrid::GraphConstrainedReRanker::new(
            node_key,
            max_hops,
            alpha,
            query_vector,
            k,
        );
        match reranker.execute(memgraph, csr_segs, lsn) {
            Ok(results) => hybrid_results_to_frame(&results),
            Err(e) => Frame::Error(Bytes::from(format!("ERR {e}"))),
        }
    } else {
        Frame::Error(Bytes::from_static(
            b"ERR unknown GRAPH.HYBRID mode (supported: FILTER, WALK, RERANK)",
        ))
    }
}

// ---------------------------------------------------------------------------
// Hybrid result formatting
// ---------------------------------------------------------------------------

/// Convert hybrid results to a RESP3 Array of Maps.
fn hybrid_results_to_frame(results: &[crate::graph::hybrid::HybridResult]) -> Frame {
    let frames: Vec<Frame> = results
        .iter()
        .map(|r| {
            let ext_id = r.node.data().as_ffi();
            let mut pairs = vec![
                (
                    Frame::SimpleString(Bytes::from_static(b"id")),
                    Frame::Integer(ext_id as i64),
                ),
                (
                    Frame::SimpleString(Bytes::from_static(b"score")),
                    Frame::Double(r.score),
                ),
            ];

            if let Some(dist) = r.graph_distance {
                pairs.push((
                    Frame::SimpleString(Bytes::from_static(b"graph_distance")),
                    Frame::Integer(dist as i64),
                ));
            }

            if !r.context.is_empty() {
                let ctx: Vec<Frame> = r
                    .context
                    .iter()
                    .map(|c| {
                        let ctx_id = c.node.data().as_ffi();
                        Frame::Map(vec![
                            (
                                Frame::SimpleString(Bytes::from_static(b"id")),
                                Frame::Integer(ctx_id as i64),
                            ),
                            (
                                Frame::SimpleString(Bytes::from_static(b"edge_type")),
                                Frame::Integer(c.edge_type as i64),
                            ),
                            (
                                Frame::SimpleString(Bytes::from_static(b"hops")),
                                Frame::Integer(c.hops as i64),
                            ),
                        ])
                    })
                    .collect();
                pairs.push((
                    Frame::SimpleString(Bytes::from_static(b"context")),
                    Frame::Array(ctx.into()),
                ));
            }

            Frame::Map(pairs)
        })
        .collect();

    Frame::Array(frames.into())
}

/// Extract a float vector from a Frame (space-separated f32 values in a BulkString).
fn extract_f32_vector(frame: &Frame) -> Option<Vec<f32>> {
    let bytes = match frame {
        Frame::BulkString(b) => b.as_ref(),
        Frame::SimpleString(b) => b.as_ref(),
        _ => return None,
    };

    // Text form first ("0.1 0.2 ..."), then binary little-endian f32 array —
    // the SAME blob format GRAPH.ADDNODE's VECTOR argument accepts, so
    // vectors round-trip between ADDNODE and VSEARCH/HYBRID unchanged.
    if let Ok(text) = core::str::from_utf8(bytes) {
        let values: Result<Vec<f32>, _> =
            text.split_whitespace().map(|s| s.parse::<f32>()).collect();
        match values {
            Ok(v) if !v.is_empty() => return Some(v),
            _ => {}
        }
    }
    if bytes.is_empty() || !bytes.len().is_multiple_of(4) {
        return None;
    }
    Some(
        bytes
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect(),
    )
}

/// Parse an f64 from a Frame.
fn parse_f64(frame: &Frame) -> Option<f64> {
    match frame {
        Frame::Double(f) => Some(*f),
        Frame::Integer(n) => Some(*n as f64),
        Frame::BulkString(b) | Frame::SimpleString(b) => core::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_u64(frame: &Frame) -> Option<u64> {
    match frame {
        Frame::Integer(n) => {
            if *n >= 0 {
                Some(*n as u64)
            } else {
                None
            }
        }
        Frame::BulkString(b) | Frame::SimpleString(b) => core::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}

fn parse_u32(frame: &Frame) -> Option<u32> {
    match frame {
        Frame::Integer(n) => {
            if *n >= 0 && *n <= u32::MAX as i64 {
                Some(*n as u32)
            } else {
                None
            }
        }
        Frame::BulkString(b) | Frame::SimpleString(b) => core::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}
