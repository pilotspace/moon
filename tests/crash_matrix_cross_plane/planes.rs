//! Per-plane write/verify helpers shared by every scenario module.
//!
//! Every helper takes a `tag: &str` used as a RESP2 hash-tag (`{tag}`) baked
//! into the plane's routing key, so the mixed-workload scenarios can force
//! KV/graph/vector/MQ writes onto the SAME shard at `--shards 4` (WS is
//! always shard-0-pinned regardless of tag — see `ws_create`'s doc).

#![allow(dead_code)]

use std::collections::BTreeSet;

use crate::resp::{Conn, Resp, as_array, as_int};

// ---------------------------------------------------------------------------
// KV (incl. spilled collections)
// ---------------------------------------------------------------------------

pub fn kv_key(tag: &str, n: u64) -> String {
    format!("{{{tag}}}:kv:{n}")
}

pub fn kv_set(c: &mut Conn, key: &str, val: &str) {
    assert!(c.cmd_s(&["SET", key, val]).is_ok(), "SET {key} failed");
}

pub fn kv_get_eq(c: &mut Conn, key: &str, expected: &str) -> bool {
    matches!(c.cmd_s(&["GET", key]), Resp::Bulk(Some(b)) if b == expected.as_bytes())
}

pub fn kv_del(c: &mut Conn, key: &str) {
    c.cmd_s(&["DEL", key]);
}

/// Fill `count` filler keys under a DIFFERENT (untagged) prefix, each
/// `value_len` bytes, to push shard memory past the disk-offload spill
/// threshold — forces the tagged probe keys written earlier to evict to the
/// cold tier. Pattern: `tests/crash_recovery_cold_del_resurrection.rs`'s
/// `write_filler` — pipelined (one `pipeline_s` batch per chunk, not one
/// round trip per key) so 16,000+ filler writes complete in a couple of
/// seconds rather than minutes; caller MUST verify the spill actually
/// happened via a hard precondition (`harness::count_heap_mpf_files`) rather
/// than assuming byte-count math alone forced it — see review round 3 (task
/// #52 P0): a prior 3000×512B filler against a 4 MiB cap silently never
/// spilled at all and the cell degenerated to plain AOF-replay coverage.
pub fn kv_spill_filler(c: &mut Conn, prefix: &str, count: u64, value_len: usize) {
    let value = "f".repeat(value_len);
    const BATCH: u64 = 2000;
    let mut i = 0u64;
    while i < count {
        let end = (i + BATCH).min(count);
        let keys: Vec<String> = (i..end).map(|n| format!("{prefix}:filler:{n}")).collect();
        let cmds: Vec<Vec<&str>> = keys
            .iter()
            .map(|k| vec!["SET", k.as_str(), value.as_str()])
            .collect();
        c.pipeline_s(&cmds);
        i = end;
    }
}

// ---------------------------------------------------------------------------
// Graph (structural + GraphTemporal)
// ---------------------------------------------------------------------------

pub fn graph_name(tag: &str) -> String {
    format!("{{{tag}}}graph")
}

pub fn graph_create(c: &mut Conn, name: &str) {
    assert_eq!(
        c.cmd_s(&["GRAPH.CREATE", name]),
        Resp::Simple("OK".into()),
        "GRAPH.CREATE {name}"
    );
}

pub fn graph_addnode(c: &mut Conn, name: &str, label: &str, id: u64) -> i64 {
    let id_s = id.to_string();
    match c.cmd_s(&["GRAPH.ADDNODE", name, label, "id", &id_s]) {
        Resp::Int(h) => h,
        other => panic!("GRAPH.ADDNODE {name} {label} id={id} failed: {other:?}"),
    }
}

/// Write `marker` as a property VALUE (not a label) on a fresh node, then
/// return. Property values are preserved literally in the WAL v3 record;
/// labels are NOT (dictionary/handle-encoded on the rewritten internal
/// command form — confirmed via `xxd` on a real
/// `shard-0/wal-v3/000000000001.wal` segment). Every marker-sync in this
/// suite that targets the graph plane MUST go through this helper, never
/// `graph_addnode`'s `label` argument, or the WAL-wait below will time out
/// waiting for bytes that are never written verbatim.
pub fn graph_addnode_marker(c: &mut Conn, name: &str, marker: &str) {
    match c.cmd_s(&["GRAPH.ADDNODE", name, "Marker", "tag", marker]) {
        Resp::Int(_) => {}
        other => panic!("GRAPH.ADDNODE {name} Marker tag={marker} failed: {other:?}"),
    }
}

pub fn graph_addedge(c: &mut Conn, name: &str, src: i64, dst: i64) {
    let (s, d) = (src.to_string(), dst.to_string());
    match c.cmd_s(&["GRAPH.ADDEDGE", name, &s, &d, "E"]) {
        Resp::Int(_) => {}
        other => panic!("GRAPH.ADDEDGE {name} {src}->{dst} failed: {other:?}"),
    }
}

pub fn graph_query_ids(c: &mut Conn, name: &str, cypher: &str) -> BTreeSet<i64> {
    match c.cmd_s(&["GRAPH.QUERY", name, cypher]) {
        Resp::Array(Some(outer)) => match outer.get(1) {
            Some(Resp::Array(Some(rows))) => rows
                .iter()
                .map(|r| match r {
                    Resp::Array(Some(cells)) => match cells.first() {
                        Some(Resp::Int(i)) => *i,
                        Some(Resp::Bulk(Some(b))) => {
                            String::from_utf8_lossy(b).parse().expect("int cell")
                        }
                        other => panic!("unexpected cell: {other:?}"),
                    },
                    other => panic!("malformed row: {other:?}"),
                })
                .collect(),
            other => panic!("malformed query reply, rows slot = {other:?}"),
        },
        Resp::Error(e) => panic!("query {cypher:?} errored: {e}"),
        other => panic!("malformed query reply: {other:?}"),
    }
}

/// Same as [`graph_query_ids`] but with an explicit `VALID_AT <ms>` clause.
/// `VALID_AT` is a SEPARATE `GRAPH.QUERY` argument, not part of the Cypher
/// text itself — embedding it into the Cypher string (e.g. `"... RETURN
/// n.id VALID_AT 123"`) is a parse error (`ERR Cypher parse error: ...
/// expected clause keyword ... found Ident(...)`). Confirmed against the
/// working pattern in `tests/crash_recovery_temporal_mq.rs`.
pub fn graph_query_ids_valid_at(
    c: &mut Conn,
    name: &str,
    cypher: &str,
    valid_at_ms: &str,
) -> BTreeSet<i64> {
    match c.cmd_s(&["GRAPH.QUERY", name, cypher, "VALID_AT", valid_at_ms]) {
        Resp::Array(Some(outer)) => match outer.get(1) {
            Some(Resp::Array(Some(rows))) => rows
                .iter()
                .map(|r| match r {
                    Resp::Array(Some(cells)) => match cells.first() {
                        Some(Resp::Int(i)) => *i,
                        Some(Resp::Bulk(Some(b))) => {
                            String::from_utf8_lossy(b).parse().expect("int cell")
                        }
                        other => panic!("unexpected cell: {other:?}"),
                    },
                    other => panic!("malformed row: {other:?}"),
                })
                .collect(),
            other => panic!("malformed query reply, rows slot = {other:?}"),
        },
        Resp::Error(e) => panic!("query {cypher:?} VALID_AT {valid_at_ms} errored: {e}"),
        other => panic!("malformed query reply: {other:?}"),
    }
}

pub fn graph_write(c: &mut Conn, name: &str, cypher: &str) {
    if let Resp::Error(e) = c.cmd_s(&["GRAPH.QUERY", name, cypher]) {
        panic!("write query {cypher:?} on {name} errored: {e}");
    }
}

/// `TEMPORAL.INVALIDATE <node_id> NODE <graph>` — GraphTemporal WAL record;
/// structurally covered by the graph engine's `snapshot_lsn` floor (kernel
/// M3 brief §1.3) but currently guarded like WS/MQ by
/// `segment_holds_plane_history` pre-K2.
pub fn temporal_invalidate_node(c: &mut Conn, node_id: &str, graph: &str) {
    assert_eq!(
        c.cmd_s(&["TEMPORAL.INVALIDATE", node_id, "NODE", graph]),
        Resp::Simple("OK".into()),
        "TEMPORAL.INVALIDATE {node_id} on {graph}"
    );
}

/// `GRAPH.DELETE <graph>` — drops a named graph and all its data.
pub fn graph_delete(c: &mut Conn, name: &str) {
    assert_eq!(
        c.cmd_s(&["GRAPH.DELETE", name]),
        Resp::Simple("OK".into()),
        "GRAPH.DELETE {name}"
    );
}

/// `GRAPH.LIST` — the set of currently-registered graph names. Used as the
/// single source of truth for "does this graph still exist" (task #53
/// review round 2 / P0-1 drop-resurrection regression cell) instead of
/// `GRAPH.QUERY`'s error-string matching, which conflates "graph never
/// existed" with "graph correctly deleted".
pub fn graph_list_names(c: &mut Conn) -> BTreeSet<String> {
    match c.cmd_s(&["GRAPH.LIST"]) {
        Resp::Array(Some(items)) => items
            .iter()
            .map(|it| match it {
                Resp::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
                Resp::Simple(s) => s.clone(),
                other => panic!("GRAPH.LIST unexpected item: {other:?}"),
            })
            .collect(),
        Resp::Array(None) => BTreeSet::new(),
        other => panic!("GRAPH.LIST malformed reply: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Vector
// ---------------------------------------------------------------------------

pub const VEC_DIM: usize = 8;

pub fn vec_index_name(tag: &str) -> String {
    format!("{tag}vecidx")
}

pub fn vec_key(tag: &str, n: u64) -> String {
    format!("{{{tag}}}:vec:{n}")
}

/// Deterministic float32 blob from a seed — no external RNG dependency.
pub fn vec_blob(seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(VEC_DIM * 4);
    for i in 0..VEC_DIM {
        let v = ((seed.wrapping_mul(31).wrapping_add(i as u64)) % 1000) as f32 / 1000.0;
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

pub fn ft_create_hnsw(c: &mut Conn, idx: &str, prefix: &str) {
    let dim_s = VEC_DIM.to_string();
    let reply = c.cmd(&[
        b"FT.CREATE",
        idx.as_bytes(),
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        prefix.as_bytes(),
        b"SCHEMA",
        b"vec",
        b"VECTOR",
        b"HNSW",
        b"8",
        b"TYPE",
        b"FLOAT32",
        b"DIM",
        dim_s.as_bytes(),
        b"DISTANCE_METRIC",
        b"L2",
    ]);
    assert_eq!(reply, Resp::Simple("OK".into()), "FT.CREATE {idx} failed");
}

pub fn hset_vec(c: &mut Conn, key: &str, blob: &[u8]) {
    match c.cmd(&[b"HSET", key.as_bytes(), b"vec", blob]) {
        Resp::Int(_) => {}
        other => panic!("HSET {key} failed: {other:?}"),
    }
}

/// True if `key` is among the top-`k` KNN results against `blob` — enough
/// signal for a crash-durability check (does the vector plane know about
/// this key at all after restart), not a recall benchmark.
pub fn vec_search_contains(c: &mut Conn, idx: &str, k: u32, blob: &[u8], key: &str) -> bool {
    let query = format!("*=>[KNN {k} @vec $B]");
    let r = c.cmd(&[
        b"FT.SEARCH",
        idx.as_bytes(),
        query.as_bytes(),
        b"PARAMS",
        b"2",
        b"B",
        blob,
        b"DIALECT",
        b"2",
    ]);
    let items = as_array(&r);
    items[1..]
        .iter()
        .step_by(2)
        .any(|v| matches!(v, Resp::Bulk(Some(b)) if b == key.as_bytes()))
}

// ---------------------------------------------------------------------------
// Text (FTS) — kernel M4, task #50: term-dict + FST sidecar durability
// ---------------------------------------------------------------------------

pub fn text_index_name(tag: &str) -> String {
    format!("{tag}textidx")
}

pub fn text_key(tag: &str, n: u64) -> String {
    format!("{{{tag}}}:txt:{n}")
}

pub fn ft_create_text(c: &mut Conn, idx: &str, prefix: &str) {
    let reply = c.cmd(&[
        b"FT.CREATE",
        idx.as_bytes(),
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        prefix.as_bytes(),
        b"SCHEMA",
        b"body",
        b"TEXT",
    ]);
    assert_eq!(reply, Resp::Simple("OK".into()), "FT.CREATE {idx} failed");
}

pub fn hset_text(c: &mut Conn, key: &str, body: &str) {
    match c.cmd(&[b"HSET", key.as_bytes(), b"body", body.as_bytes()]) {
        Resp::Int(_) => {}
        other => panic!("HSET {key} failed: {other:?}"),
    }
}

/// FT.COMPACT: builds + persists the term-dict + FST sidecar
/// (`TextStore::save_term_fst_sidecar_for_index`) so it survives kill-9.
pub fn ft_compact(c: &mut Conn, idx: &str) {
    let reply = c.cmd(&[b"FT.COMPACT", idx.as_bytes()]);
    assert_eq!(reply, Resp::Simple("OK".into()), "FT.COMPACT {idx} failed");
}

/// True if `key` is among the results of a FUZZY query against `idx` --
/// FUZZY expansion exercises the FST path (`TermModifier::Fuzzy`), which is
/// exactly the path that is unsafe to serve from a stale-id-space sidecar
/// (see `TextStore::load_fst_sidecars`'s doc comment). A crash-durability
/// cell that only checked exact-term search would miss that corruption
/// class entirely.
pub fn ft_search_fuzzy_contains(c: &mut Conn, idx: &str, fuzzy_term: &str, key: &str) -> bool {
    let query = format!("@body:%{fuzzy_term}%");
    let r = c.cmd(&[
        b"FT.SEARCH",
        idx.as_bytes(),
        query.as_bytes(),
        b"DIALECT",
        b"2",
    ]);
    let items = as_array(&r);
    items[1..]
        .iter()
        .step_by(2)
        .any(|v| matches!(v, Resp::Bulk(Some(b)) if b == key.as_bytes()))
}

/// Additive `FT.INFO` `sidecar_recovered_indexes` counter (kernel M4, task
/// #50) — 1+ means at least one shard's boot took the fast sidecar-recovery
/// path instead of falling back to a full keyspace rescan.
pub fn ft_info_sidecar_recovered_indexes(c: &mut Conn, idx: &str) -> i64 {
    let r = c.cmd(&[b"FT.INFO", idx.as_bytes()]);
    let items = as_array(&r);
    let mut i = 0;
    while i + 1 < items.len() {
        if let Resp::Bulk(Some(k)) = &items[i]
            && k == b"sidecar_recovered_indexes"
        {
            return as_int(&items[i + 1]);
        }
        i += 2;
    }
    panic!("FT.INFO {idx}: sidecar_recovered_indexes key not found");
}

// ---------------------------------------------------------------------------
// WS (workspace registry — process-global, shard-0-pinned per Wave B)
// ---------------------------------------------------------------------------

pub fn ws_create(c: &mut Conn, name: &str) -> Vec<u8> {
    match c.cmd_s(&["WS", "CREATE", name]) {
        Resp::Bulk(Some(id)) => id,
        other => panic!("WS CREATE {name} failed: {other:?}"),
    }
}

pub fn ws_drop(c: &mut Conn, id: &[u8]) {
    let id_s = String::from_utf8_lossy(id).into_owned();
    match c.cmd_s(&["WS", "DROP", &id_s]) {
        Resp::Simple(_) | Resp::Int(_) => {}
        other => panic!("WS DROP {id_s} failed: {other:?}"),
    }
}

pub fn ws_list_contains(c: &mut Conn, id: &[u8]) -> bool {
    match c.cmd_s(&["WS", "LIST"]) {
        Resp::Array(Some(entries)) => entries.iter().any(|e| match e {
            Resp::Array(Some(cells)) => {
                matches!(cells.first(), Some(Resp::Bulk(Some(b))) if b == id)
            }
            _ => false,
        }),
        other => panic!("WS LIST failed: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// MQ (incl. DLQ / PEL / triggers)
// ---------------------------------------------------------------------------

pub fn mq_queue_name(tag: &str) -> String {
    format!("{{{tag}}}mq")
}

pub fn mq_create(c: &mut Conn, q: &str, max_delivery: u32) {
    let md = max_delivery.to_string();
    assert_eq!(
        c.cmd_s(&["MQ", "CREATE", q, "MAXDELIVERY", &md]),
        Resp::Simple("OK".into()),
        "MQ CREATE {q}"
    );
}

pub fn mq_push(c: &mut Conn, q: &str, field: &str, val: &str) -> Vec<u8> {
    match c.cmd_s(&["MQ", "PUSH", q, field, val]) {
        Resp::Bulk(Some(id)) => id,
        other => panic!("MQ PUSH {q} failed: {other:?}"),
    }
}

pub fn mq_pop(c: &mut Conn, q: &str, count: u32) -> Vec<Resp> {
    let cnt = count.to_string();
    as_array(&c.cmd_s(&["MQ", "POP", q, "COUNT", &cnt])).to_vec()
}

pub fn mq_ack(c: &mut Conn, q: &str, ids: &[Vec<u8>]) -> i64 {
    let id_strs: Vec<String> = ids
        .iter()
        .map(|i| String::from_utf8_lossy(i).into_owned())
        .collect();
    let mut parts: Vec<&str> = vec!["MQ", "ACK", q];
    parts.extend(id_strs.iter().map(|s| s.as_str()));
    as_int(&c.cmd_s(&parts))
}

pub fn mq_dlqlen(c: &mut Conn, q: &str) -> i64 {
    as_int(&c.cmd_s(&["MQ", "DLQLEN", q]))
}

pub fn mq_trigger(c: &mut Conn, q: &str, callback: &str, debounce_ms: u32) {
    let ms = debounce_ms.to_string();
    assert_eq!(
        c.cmd_s(&["MQ", "TRIGGER", q, callback, "DEBOUNCE", &ms]),
        Resp::Simple("OK".into()),
        "MQ TRIGGER {q}"
    );
}

pub fn xlen(c: &mut Conn, key: &str) -> i64 {
    as_int(&c.cmd_s(&["XLEN", key]))
}

// ---------------------------------------------------------------------------
// Cross-store TXN (MULTI/EXEC spanning ≥2 planes)
// ---------------------------------------------------------------------------

/// Queue a mixed KV+graph write inside MULTI/EXEC and execute it. Returns
/// the EXEC reply (caller inspects it for the golden invariant: an
/// EXEC-committed write must survive exactly like a non-MULTI write).
pub fn txn_kv_graph_write(
    c: &mut Conn,
    kv_key: &str,
    kv_val: &str,
    graph: &str,
    label: &str,
    node_id: u64,
) -> Resp {
    assert_eq!(c.cmd_s(&["MULTI"]), Resp::Simple("OK".into()));
    let id_s = node_id.to_string();
    assert_eq!(
        c.cmd_s(&["SET", kv_key, kv_val]),
        Resp::Simple("QUEUED".into())
    );
    assert_eq!(
        c.cmd_s(&["GRAPH.ADDNODE", graph, label, "id", &id_s]),
        Resp::Simple("QUEUED".into())
    );
    c.cmd_s(&["EXEC"])
}
