//! Concurrent mixed-writes-across-all-planes workload generator.
//!
//! This is the genuinely new coverage the kernel M3 brief calls out: no
//! existing suite drives KV + graph + vector + WS + MQ writes CONCURRENTLY
//! against the same shard (the closest precedent,
//! `tests/replication_planes.rs`'s Lua-writes-survive-kill-9 test, is
//! single-plane-at-a-time even though the script itself touches several
//! command families). Four threads, one per plane, each on its own
//! connection, barrier-started so they overlap on the wire; every write
//! lands on the SAME shard via a shared `{tag}` hash-tag baked into every
//! plane's routing key (WS is shard-0-pinned regardless of tag — Wave B).

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use crate::harness;
use crate::planes::*;
use crate::resp::Conn;

pub struct MixedPlan {
    pub kv_n: u64,
    pub graph_n: u64,
    pub vec_n: u64,
    pub mq_n: u64,
}

impl Default for MixedPlan {
    fn default() -> Self {
        MixedPlan {
            kv_n: 20,
            graph_n: 20,
            vec_n: 20,
            mq_n: 20,
        }
    }
}

/// Ground truth for the PREFIX of each plane's ops that was waited-for on
/// disk before the kill. Ops beyond this prefix were sent but not
/// necessarily durable — a kill can land before, during, or after their WAL
/// append, so they are deliberately not asserted either way.
pub struct MixedTruth {
    pub tag: String,
    pub kv_synced: Vec<(String, String)>,
    pub graph_name: String,
    pub graph_ids_synced: BTreeSet<i64>,
    pub vec_idx: String,
    pub vec_synced: Vec<(String, Vec<u8>)>,
    pub ws_name: String,
    pub ws_id: Vec<u8>,
    pub mq_queue: String,
    pub mq_synced: u64,
}

/// Run the four plane-writer threads concurrently to completion (schema ops
/// — GRAPH.CREATE / FT.CREATE / MQ.CREATE / WS.CREATE — happen once,
/// up-front, sequentially, since they are one-shot setup, not part of the
/// "sustained write load"). `sync_fraction` controls how much of EACH
/// plane's completed op stream is waited-for on disk (via a per-plane
/// marker on the last synced op) before this function returns — the
/// "kill at a random/periodic offset in the write stream" kill point from
/// the brief. `sync_fraction` is chosen by the caller (fixed 1.0 for a
/// full-durability check, or `harness::jitter_range` for a randomized
/// partial one).
pub fn run_mixed_workload_synced(
    dir: &Path,
    port: u16,
    num_shards: u32,
    tag: &str,
    plan: MixedPlan,
    sync_fraction: f64,
) -> MixedTruth {
    let graph_name = graph_name(tag);
    let vec_idx = vec_index_name(tag);
    let vec_prefix = format!("{{{tag}}}:vec:");
    let ws_name = format!("{tag}-ws");
    let mq_queue = mq_queue_name(tag);

    // One-shot schema setup, sequential (not part of the concurrent burst).
    let mut setup = Conn::open(port);
    graph_create(&mut setup, &graph_name);
    ft_create_hnsw(&mut setup, &vec_idx, &vec_prefix);
    mq_create(&mut setup, &mq_queue, 3);
    let ws_id = ws_create(&mut setup, &ws_name);

    let barrier = Arc::new(Barrier::new(4));

    // KV writer.
    let (kv_port, kv_tag, kv_n, kv_barrier) = (port, tag.to_string(), plan.kv_n, barrier.clone());
    let kv_handle = thread::spawn(move || -> Vec<(String, String)> {
        let mut c = Conn::open(kv_port);
        kv_barrier.wait();
        (0..kv_n)
            .map(|i| {
                let key = kv_key(&kv_tag, i);
                let val = format!("v{i}");
                kv_set(&mut c, &key, &val);
                (key, val)
            })
            .collect()
    });

    // Graph writer. `graph_addnode`'s RETURN value is the internal node
    // HANDLE, which legitimately renumbers across a restart (WAL replay can
    // reassign shard-encoded handles) — collect the "id" PROPERTY value
    // (`i`, the value actually queryable post-restart via `RETURN n.id`)
    // instead, matching how every other scenario in this suite verifies
    // graph durability.
    let (g_port, g_name, g_n, g_barrier) =
        (port, graph_name.clone(), plan.graph_n, barrier.clone());
    let graph_handle = thread::spawn(move || -> Vec<i64> {
        let mut c = Conn::open(g_port);
        g_barrier.wait();
        (0..g_n)
            .map(|i| {
                graph_addnode(&mut c, &g_name, "MixN", i);
                i as i64
            })
            .collect()
    });

    // Vector writer.
    let (v_port, v_prefix, v_n, v_barrier) =
        (port, vec_prefix.clone(), plan.vec_n, barrier.clone());
    let vec_handle = thread::spawn(move || -> Vec<(String, Vec<u8>)> {
        let mut c = Conn::open(v_port);
        v_barrier.wait();
        (0..v_n)
            .map(|i| {
                let key = format!("{v_prefix}{i}");
                let blob = vec_blob(i);
                hset_vec(&mut c, &key, &blob);
                (key, blob)
            })
            .collect()
    });

    // MQ writer.
    let (m_port, m_queue, m_n, m_barrier) = (port, mq_queue.clone(), plan.mq_n, barrier);
    let mq_handle = thread::spawn(move || -> u64 {
        let mut c = Conn::open(m_port);
        m_barrier.wait();
        for i in 0..m_n {
            mq_push(&mut c, &m_queue, "seq", &i.to_string());
        }
        m_n
    });

    let kv_all = kv_handle.join().expect("kv writer panicked");
    let graph_ids: BTreeSet<i64> = graph_handle
        .join()
        .expect("graph writer panicked")
        .into_iter()
        .collect();
    let vec_all = vec_handle.join().expect("vector writer panicked");
    let mq_pushed = mq_handle.join().expect("mq writer panicked");

    let cut = |n: u64| -> u64 { ((n as f64) * sync_fraction).floor() as u64 };
    let kv_cut = cut(kv_all.len() as u64) as usize;
    let graph_cut = cut(graph_ids.len() as u64) as usize;
    let vec_cut = cut(vec_all.len() as u64) as usize;
    let mq_cut = cut(mq_pushed);

    // Sync each plane's prefix by writing ONE extra marker op tagged with
    // the cut index and waiting for it on the WAL — proves everything
    // strictly before it (same connection, same shard, in-order append) is
    // durable too.
    let mut sync_conn = Conn::open(port);
    if kv_cut > 0 {
        // KV rides the AOF, not wal-v3 (confirmed empirically — plain SETs
        // never appear in wal-v3 at all).
        let marker = format!("XPLANE-KV-SYNC-{tag}-{kv_cut}");
        kv_set(&mut sync_conn, &format!("{{{tag}}}:kvsync"), &marker);
        harness::wait_for_aof_bytes_any_shard(dir, marker.as_bytes());
    }
    if graph_cut > 0 {
        // Marker must be a property VALUE, not a label (labels are not
        // preserved verbatim in the wal-v3 record — see
        // `planes::graph_addnode_marker`'s doc).
        let marker = format!("XPLANE-GRAPH-SYNC-{tag}-{graph_cut}");
        graph_addnode_marker(&mut sync_conn, &graph_name, &marker);
        harness::wait_for_wal_v3_bytes_any_shard(dir, num_shards, marker.as_bytes());
    }
    if vec_cut > 0 {
        // Vector-HSET durability rides the AOF too (brief §1.2), not wal-v3.
        let marker_key = format!("{vec_prefix}sync");
        let marker_blob = vec_blob(777);
        hset_vec(&mut sync_conn, &marker_key, &marker_blob);
        harness::wait_for_aof_bytes_any_shard(dir, marker_key.as_bytes());
    }
    if mq_cut > 0 {
        let marker = format!("XPLANE-MQ-SYNC-{tag}-{mq_cut}");
        mq_push(&mut sync_conn, &mq_queue, "sync", &marker);
        harness::wait_for_wal_v3_bytes_any_shard(dir, num_shards, marker.as_bytes());
    }
    // WS.CREATE above is a single fast op on its own — always wait for it
    // directly rather than deriving a cut.
    harness::wait_for_wal_v3_bytes_any_shard(dir, num_shards, ws_name.as_bytes());

    MixedTruth {
        tag: tag.to_string(),
        kv_synced: kv_all.into_iter().take(kv_cut).collect(),
        graph_name,
        graph_ids_synced: graph_ids.into_iter().take(graph_cut).collect(),
        vec_idx,
        vec_synced: vec_all.into_iter().take(vec_cut).collect(),
        ws_name,
        ws_id,
        mq_queue,
        mq_synced: mq_cut,
    }
}

pub fn assert_mixed_truth_recovered(c: &mut Conn, truth: &MixedTruth) {
    for (k, v) in &truth.kv_synced {
        assert!(
            kv_get_eq(c, k, v),
            "mixed-workload KV key {k} must survive (synced prefix)"
        );
    }
    if !truth.graph_ids_synced.is_empty() {
        let got = graph_query_ids(c, &truth.graph_name, "MATCH (n:MixN) RETURN n.id");
        assert!(
            truth.graph_ids_synced.is_subset(&got),
            "mixed-workload graph ids {:?} must be a subset of recovered {:?} \
             (graph plane {})",
            truth.graph_ids_synced,
            got,
            truth.graph_name,
        );
    }
    for (key, blob) in &truth.vec_synced {
        assert!(
            vec_search_contains(c, &truth.vec_idx, 5, blob, key),
            "mixed-workload vector key {key} must survive (synced prefix, index {})",
            truth.vec_idx,
        );
    }
    assert!(
        ws_list_contains(c, &truth.ws_id),
        "mixed-workload workspace {} must survive",
        truth.ws_name,
    );
    if truth.mq_synced > 0 {
        assert!(
            xlen(c, &truth.mq_queue) >= truth.mq_synced as i64,
            "mixed-workload MQ queue {} must have at least {} entries after restart",
            truth.mq_queue,
            truth.mq_synced,
        );
    }
}

/// RAII guard (coordinator review round 3, item 3): filters the EXPECTED
/// "server died mid-write" panic messages (`Conn::fill`'s `read from
/// server` / `connection closed mid-frame`, `write cmd`/`write pipeline`)
/// out of stderr for as long as it is held, restoring the previous hook on
/// drop. Deliberately scoped TIGHTLY around a single
/// `concurrent_burst_no_corruption` iteration (not installed process-wide,
/// not left in place for the rest of this long-lived test binary) — this
/// test binary runs 37 tests sequentially in one process, so a
/// process-wide swap would risk silently swallowing a GENUINE
/// disconnect-shaped panic in some unrelated LATER test. Any panic message
/// that doesn't match the known-benign substrings is forwarded to the
/// previous hook unchanged, so real bugs stay visible.
type PanicHook = dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send;

pub struct BenignDisconnectPanicFilter {
    prev: Option<std::sync::Arc<PanicHook>>,
}

impl BenignDisconnectPanicFilter {
    pub fn install() -> Self {
        let prev: std::sync::Arc<PanicHook> = std::panic::take_hook().into();
        let forward = prev.clone();
        std::panic::set_hook(Box::new(move |info| {
            let payload = info.payload();
            let msg = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("");
            let benign = msg.contains("read from server")
                || msg.contains("connection closed mid-frame")
                || msg.contains("write cmd")
                || msg.contains("write pipeline");
            if !benign {
                forward(info);
            }
        }));
        Self { prev: Some(prev) }
    }
}

impl Drop for BenignDisconnectPanicFilter {
    fn drop(&mut self) {
        if let Some(prev) = self.prev.take() {
            std::panic::set_hook(Box::new(move |info| prev(info)));
        }
    }
}

/// Genuinely-concurrent variant: start the four writer threads and return
/// while they are STILL mid-burst (no join, no markers, no sync wait) — the
/// purest form of "random offset in the write stream". The caller MUST
/// SIGKILL the server immediately after this returns (any delay narrows the
/// "mid-flight" window). Because nothing is synchronized, this makes NO
/// durability claim about which ops landed; callers only assert the server
/// comes back up cleanly and every plane answers well-formed queries
/// afterward (no corruption / no crash-loop / no wedged replay). Writer
/// threads are detached (not joined) — once the socket dies each iteration's
/// `.expect()`/`assert_eq!` panics and unwinds that thread only (Cargo
/// forces `panic = "unwind"` for test binaries regardless of the release
/// profile's `panic = "abort"`), which is harmless noise on stderr (silenced
/// by `BenignDisconnectPanicFilter` when the caller installs it), not a
/// test failure (nothing here ever calls `.join().unwrap()`).
pub fn start_concurrent_burst(port: u16, tag: &str) {
    let graph_name = graph_name(tag);
    let vec_idx = vec_index_name(tag);
    let vec_prefix = format!("{{{tag}}}:vec:");
    let mq_queue = mq_queue_name(tag);

    let mut setup = Conn::open(port);
    graph_create(&mut setup, &graph_name);
    ft_create_hnsw(&mut setup, &vec_idx, &vec_prefix);
    mq_create(&mut setup, &mq_queue, 3);
    drop(setup);

    let barrier = Arc::new(Barrier::new(4));

    let (kv_port, kv_tag, b) = (port, tag.to_string(), barrier.clone());
    thread::spawn(move || {
        let mut c = Conn::open(kv_port);
        b.wait();
        for i in 0..100_000u64 {
            kv_set(&mut c, &kv_key(&kv_tag, i), "v");
        }
    });

    let (g_port, g_name, b) = (port, graph_name, barrier.clone());
    thread::spawn(move || {
        let mut c = Conn::open(g_port);
        b.wait();
        for i in 0..100_000u64 {
            graph_addnode(&mut c, &g_name, "BurstN", i);
        }
    });

    let (v_port, v_prefix, b) = (port, vec_prefix, barrier.clone());
    thread::spawn(move || {
        let mut c = Conn::open(v_port);
        b.wait();
        for i in 0..100_000u64 {
            let key = format!("{v_prefix}{i}");
            hset_vec(&mut c, &key, &vec_blob(i));
        }
    });

    let (m_port, m_queue, b) = (port, mq_queue, barrier);
    thread::spawn(move || {
        let mut c = Conn::open(m_port);
        b.wait();
        for i in 0..100_000u64 {
            mq_push(&mut c, &m_queue, "seq", &i.to_string());
        }
    });

    // Let the burst run for a short, randomized window before returning —
    // genuinely "somewhere mid-stream", never a marker-synced boundary.
    let jitter_ms = harness::jitter_range(0xB0F5, 50, 400);
    thread::sleep(Duration::from_millis(jitter_ms));
}
