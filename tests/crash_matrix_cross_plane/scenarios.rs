//! Shared scenario bodies, parametrized by [`Config`]. Thin `#[test]`
//! wrappers in `tests_prod.rs` / `tests_legacy.rs` / `tests_spot.rs` /
//! `tests_seeded_red.rs` bind each body to a specific config cell and own
//! the `cross_plane_<config>_<cell>` naming + any `#[ignore = "RED: ..."]`
//! annotation. Bodies themselves always assert the FULL expected contract —
//! whether a given cell is RED or GREEN is discovered by actually running
//! it, never hard-coded here.

#![allow(dead_code)]

use std::time::Duration;

use crate::harness::{self, Config};
use crate::mixed::{self, MixedPlan};
use crate::planes::*;
use crate::resp::Conn;

// ---------------------------------------------------------------------------
// Single-plane isolated baselines
// ---------------------------------------------------------------------------

/// KV isolated: write a small batch, sync a randomly-chosen prefix boundary
/// (the "periodic markers / kill at a random offset" kill point), kill,
/// verify exactly that prefix survived. `appendonly=no` cells have no
/// durability contract — liveness only.
pub fn kv_isolated(cfg: &Config) {
    let dir = harness::unique_dir(&format!("kviso-{}", cfg.label));
    let (guard, port) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c = Conn::open(port);
    let tag = "kviso";
    const N: u64 = 12;

    if cfg.no_durability_contract() {
        for i in 0..N {
            kv_set(&mut c, &kv_key(tag, i), &format!("v{i}"));
        }
        harness::crash(guard, port);
        let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
        let mut c2 = Conn::open(port2);
        // No durability claim; just prove the server serves cleanly.
        assert!(matches!(c2.cmd_s(&["PING"]), crate::resp::Resp::Simple(_)));
        drop(guard2);
        return;
    }

    let kill_at = harness::jitter_range(1001, 1, N + 1);
    let mut written = Vec::new();
    for i in 0..kill_at {
        let key = kv_key(tag, i);
        let val = format!("v{i}");
        kv_set(&mut c, &key, &val);
        written.push((key, val));
    }
    // The AOF stores `SET key val` as two SEPARATE RESP bulk strings, never
    // the concatenated literal "key=val" — wait for both independently.
    let (last_key, last_val) = written.last().expect("kill_at >= 1");
    harness::wait_for_aof_bytes_all_any_shard(&dir, &[last_key.as_bytes(), last_val.as_bytes()]);
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c2 = Conn::open(port2);
    for (key, val) in &written {
        assert!(
            kv_get_eq(&mut c2, key, val),
            "cell {}: KV key {key} must survive up to the synced offset {kill_at}/{N}",
            cfg.label
        );
    }
    drop(guard2);
}

/// KV spilled: filler pushes the probe keys to the cold tier before they are
/// deleted-and-resynced; only meaningful when disk-offload is enabled.
///
/// Filler sizing mirrors `crash_recovery_cold_del_resurrection.rs` exactly
/// (`MAXMEMORY_BYTES` = 8 MiB, `FILLER_COUNT` × `FILLER_VALUE_LEN` =
/// 16,000×600B ≈ 9.6 MiB, `SETTLE_AFTER_FILLER` = 8s) rather than an
/// independently-invented ratio. `--maxmemory` is divided evenly across
/// shards (`RuntimeConfig::maxmemory_per_shard`, `src/config.rs`), so at
/// `shards=1` the whole 8 MiB cap lives on one shard (trivially exceeded by
/// unscoped filler) and at `shards=4` each shard's ~2 MiB cap is exceeded by
/// the filler's average ~2.4 MiB/shard scatter — the SAME total-byte ratio
/// the reference suite already validated forces a spill at its own
/// `SHARDS=4` config, so no per-shard-count filler scaling is needed on top
/// of matching the reference's absolute numbers. A prior 3000×512B filler
/// (~1.5 MiB) against a 4 MiB cap never came close to the 0.85× spill
/// threshold at either shard count — confirmed on the VM via
/// `reclamation_cold_segments:0` and zero `heap-*.mpf` files — and the cell
/// silently degenerated to ordinary AOF-replay coverage (already proven by
/// `kv_isolated`), a real false-GREEN (review round 3, task #52 P0). The
/// `count_heap_mpf_files` precondition assert below is the hard backstop:
/// any future load-parameter regression now fails LOUD instead of vacuously
/// passing.
pub fn kv_spilled_isolated(cfg: &Config) {
    assert!(
        cfg.disk_offload,
        "kv_spilled_isolated requires disk-offload enable"
    );
    const MAXMEMORY_BYTES: u64 = 8 * 1024 * 1024;
    const FILLER_COUNT: u64 = 16_000;
    const FILLER_VALUE_LEN: usize = 600;
    const SETTLE_AFTER_FILLER: u64 = 8;

    let dir = harness::unique_dir(&format!("kvspill-{}", cfg.label));
    let maxmemory_s = MAXMEMORY_BYTES.to_string();
    let extra = [
        "--maxmemory",
        maxmemory_s.as_str(),
        "--maxmemory-policy",
        "allkeys-lru",
    ];
    let (guard, port) = harness::spawn_moon_on(&dir, cfg, &extra);
    let mut c = Conn::open(port);
    let tag = "kvspill";

    const PROBES: u64 = 10;
    let mut probes = Vec::new();
    for i in 0..PROBES {
        let key = kv_key(tag, i);
        let val = format!("probe-{i}");
        kv_set(&mut c, &key, &val);
        probes.push((key, val));
    }
    // Push probes to the cold tier.
    kv_spill_filler(&mut c, "kvspillfiller", FILLER_COUNT, FILLER_VALUE_LEN);
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));

    // Hard precondition (review round 3, task #52 P0): without any
    // heap-*.mpf files on disk, filler pressure never forced a spill and
    // this scenario has no power — it would silently degenerate to
    // ordinary AOF-replay coverage, already proven by `kv_isolated`.
    let heap_files = harness::count_heap_mpf_files(&dir);
    assert!(
        heap_files > 0,
        "cell {}: precondition failed: no heap-*.mpf files — filler did not \
         force a spill",
        cfg.label
    );

    let marker = "XPLANE-KVSPILL-MARKER".to_string();
    kv_set(&mut c, &format!("{{{tag}}}:marker"), &marker);
    harness::wait_for_aof_bytes_any_shard(&dir, marker.as_bytes());
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &extra);
    let mut c2 = Conn::open(port2);
    for (key, val) in &probes {
        assert!(
            kv_get_eq(&mut c2, key, val),
            "cell {}: spilled KV probe {key} must survive kill-9 (task #41)",
            cfg.label
        );
    }
    drop(guard2);
}

/// Graph isolated: structural writes (ADDNODE/ADDEDGE + Cypher CREATE/SET/
/// DELETE) + GraphTemporal (`TEMPORAL.INVALIDATE`). SEEDED RED in legacy
/// mode (brief §1.4 / PR #288 changelog) — the body still asserts the full
/// contract; the `#[ignore]` annotation lives on the legacy-cell wrapper.
pub fn graph_isolated(cfg: &Config) {
    let dir = harness::unique_dir(&format!("graphiso-{}", cfg.label));
    let (guard, port) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c = Conn::open(port);
    let name = graph_name("graphiso");

    graph_create(&mut c, &name);
    let handles: Vec<i64> = (0..6)
        .map(|i| graph_addnode(&mut c, &name, "N", i))
        .collect();
    for i in 0..6 {
        graph_addedge(&mut c, &name, handles[i], handles[(i + 1) % 6]);
    }
    graph_write(&mut c, &name, "CREATE (n:W {id: 100})");
    graph_write(&mut c, &name, "MATCH (n:N {id: 3}) SET n.score = 42");
    graph_write(&mut c, &name, "MATCH (n:N {id: 5}) DELETE n");

    // GraphTemporal: invalidate node 0.
    let node0_id = match graph_query_ids(&mut c, &name, "MATCH (n:N {id: 0}) RETURN n.id").len() {
        1 => handles[0].to_string(),
        _ => panic!("node id=0 must exist before invalidation"),
    };
    temporal_invalidate_node(&mut c, &node0_id, &name);

    if cfg.no_durability_contract() {
        harness::crash(guard, port);
        let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
        let mut c2 = Conn::open(port2);
        assert!(matches!(c2.cmd_s(&["PING"]), crate::resp::Resp::Simple(_)));
        drop(guard2);
        return;
    }

    let marker = "XPLANE-GRAPH-MARKER".to_string();
    graph_addnode_marker(&mut c, &name, &marker);
    harness::wait_for_wal_v3_bytes_any_shard(&dir, cfg.shards, marker.as_bytes());
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c2 = Conn::open(port2);
    assert_eq!(
        graph_query_ids(&mut c2, &name, "MATCH (n:N) RETURN n.id"),
        (0..5).collect(),
        "cell {}: structural writes (ADDNODE/ADDEDGE + Cypher DELETE) must \
         survive kill-9",
        cfg.label
    );
    assert_eq!(
        graph_query_ids(&mut c2, &name, "MATCH (n:W) RETURN n.id"),
        std::collections::BTreeSet::from([100]),
        "cell {}: Cypher CREATE must survive kill-9",
        cfg.label
    );
    assert_eq!(
        graph_query_ids(&mut c2, &name, "MATCH (n:N {id: 3}) RETURN n.score"),
        std::collections::BTreeSet::from([42]),
        "cell {}: Cypher SET must survive kill-9",
        cfg.label
    );
    // VALID_AT is a separate GRAPH.QUERY argument, not part of the Cypher
    // text (see `graph_query_ids_valid_at`'s doc).
    let visible_far_future = graph_query_ids_valid_at(
        &mut c2,
        &name,
        "MATCH (n:N {id: 0}) RETURN n.id",
        "9000000000000",
    );
    assert!(
        visible_far_future.is_empty(),
        "cell {}: TEMPORAL.INVALIDATE (GraphTemporal) must survive kill-9 — \
         node 0 must NOT be visible at a far-future VALID_AT after restart, \
         got {visible_far_future:?}",
        cfg.label
    );
    // Positive control (review round 3, P2): without this, the negative
    // check above would PASS vacuously if VALID_AT plumbing itself were
    // broken and always returned empty regardless of invalidation state.
    // Node 1 was never invalidated or deleted, so it MUST still be visible
    // at the same far-future VALID_AT.
    let node1_visible_far_future = graph_query_ids_valid_at(
        &mut c2,
        &name,
        "MATCH (n:N {id: 1}) RETURN n.id",
        "9000000000000",
    );
    assert_eq!(
        node1_visible_far_future,
        std::collections::BTreeSet::from([1]),
        "cell {}: VALID_AT positive control — node 1 (never invalidated) \
         must be visible at a far-future VALID_AT",
        cfg.label
    );
    drop(guard2);
}

/// Vector isolated: FT.CREATE + HSET batch, synced prefix, kill, verify.
pub fn vector_isolated(cfg: &Config) {
    let dir = harness::unique_dir(&format!("veciso-{}", cfg.label));
    let (guard, port) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c = Conn::open(port);
    let tag = "veciso";
    let idx = vec_index_name(tag);
    let prefix = format!("{{{tag}}}:vec:");
    ft_create_hnsw(&mut c, &idx, &prefix);

    if cfg.no_durability_contract() {
        for i in 0..10u64 {
            hset_vec(&mut c, &format!("{prefix}{i}"), &vec_blob(i));
        }
        harness::crash(guard, port);
        let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
        let mut c2 = Conn::open(port2);
        assert!(matches!(c2.cmd_s(&["PING"]), crate::resp::Resp::Simple(_)));
        drop(guard2);
        return;
    }

    const N: u64 = 10;
    let kill_at = harness::jitter_range(2002, 1, N + 1);
    let mut written = Vec::new();
    for i in 0..kill_at {
        let key = format!("{prefix}{i}");
        let blob = vec_blob(i);
        hset_vec(&mut c, &key, &blob);
        written.push((key, blob));
    }
    // Vector durability rides the same path as any other HSET (brief §1.2:
    // "normal Command WAL/AOF replay of the hash that gets auto-indexed") —
    // wait on the AOF, not wal-v3.
    harness::wait_for_aof_bytes_any_shard(&dir, written.last().expect("kill_at >= 1").0.as_bytes());
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c2 = Conn::open(port2);
    for (key, blob) in &written {
        assert!(
            vec_search_contains(&mut c2, &idx, 5, blob, key),
            "cell {}: vector key {key} must survive up to the synced offset \
             {kill_at}/{N}",
            cfg.label
        );
    }
    drop(guard2);
}

/// Text (FTS) isolated: kernel M4, task #50 tripwire. FT.CREATE, index docs,
/// FT.COMPACT (builds + persists the term-dict + FST sidecar), index MORE
/// docs post-compact (so boot must both recover the sidecar AND correctly
/// continue assigning ids to genuinely-new terms), kill-9, restart, verify a
/// FUZZY query -- which exercises the FST expansion path, not just exact
/// HashMap lookup -- still finds every doc, and that `FT.INFO` reports the
/// index as sidecar-recovered (not a silent fallback to full rescan).
///
/// This is a default-GREEN cell: unlike the RED root-cause groups tracked
/// elsewhere in this matrix, task #50's fix is expected to hold here. A
/// regression here means either the `.tfst` sidecar stopped being written
/// at FT.COMPACT time, `load_term_fst_sidecars` stopped being wired into
/// shard boot before the rescan, or the id-continuation invariant
/// (`TermDictionary::from_pairs`'s `next_id` handoff) broke.
pub fn text_fts_sidecar_isolated(cfg: &Config) {
    let dir = harness::unique_dir(&format!("txtiso-{}", cfg.label));
    let (guard, port) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c = Conn::open(port);
    let tag = "txtiso";
    let idx = text_index_name(tag);
    let prefix = format!("{{{tag}}}:txt:");
    ft_create_text(&mut c, &idx, &prefix);

    if cfg.no_durability_contract() {
        hset_text(&mut c, &format!("{prefix}0"), "machine vision system");
        harness::crash(guard, port);
        let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
        let mut c2 = Conn::open(port2);
        assert!(matches!(c2.cmd_s(&["PING"]), crate::resp::Resp::Simple(_)));
        drop(guard2);
        return;
    }

    // Pre-compact corpus: every doc contains "machine", which stems to
    // "machin" -- FUZZY-reachable via edit-distance-1 probe "machn". (Note:
    // "machinery" stems to a DIFFERENT token ("machineri") that is NOT
    // within edit-distance-1 of "machn" -- an earlier draft of this fixture
    // used "deep learning machinery" and got a false-negative FUZZY miss
    // that looked like a sidecar-recovery bug but was actually a test-data
    // bug in the fixture, not the product.)
    let pre_compact: Vec<(String, &str)> = vec![
        (text_key(tag, 0), "machine vision system"),
        (text_key(tag, 1), "deep learning machine"),
        (text_key(tag, 2), "machine learning pipeline"),
    ];
    for (key, body) in &pre_compact {
        hset_text(&mut c, key, body);
    }
    ft_compact(&mut c, &idx);

    // Post-compact corpus: NEW terms discovered only by the rescan after a
    // sidecar-recovered boot -- proves `next_id` continuation doesn't
    // collide with the loaded sidecar's id-space.
    let post_compact: Vec<(String, &str)> = vec![
        (text_key(tag, 3), "machine translation service"),
        (text_key(tag, 4), "robotic machine arm"),
    ];
    for (key, body) in &post_compact {
        hset_text(&mut c, key, body);
    }

    let all_docs: Vec<(String, &str)> = pre_compact
        .into_iter()
        .chain(post_compact)
        .collect::<Vec<_>>();
    harness::wait_for_aof_bytes_any_shard(
        &dir,
        all_docs.last().expect("non-empty corpus").0.as_bytes(),
    );
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c2 = Conn::open(port2);
    for (key, _) in &all_docs {
        assert!(
            ft_search_fuzzy_contains(&mut c2, &idx, "machn", key),
            "cell {}: FUZZY 'machn' must find {key} after sidecar-recovered restart \
             (term-dict+FST sidecar durability, kernel M4 task #50)",
            cfg.label
        );
    }
    assert!(
        ft_info_sidecar_recovered_indexes(&mut c2, &idx) >= 1,
        "cell {}: FT.INFO sidecar_recovered_indexes must be >= 1 after a clean \
         restart with a valid .tfst sidecar on disk -- 0 means boot silently \
         fell back to a full keyspace rescan",
        cfg.label
    );
    drop(guard2);
}

/// WS isolated: WS CREATE, synced, kill, verify WS LIST.
pub fn ws_isolated(cfg: &Config) {
    let dir = harness::unique_dir(&format!("wsiso-{}", cfg.label));
    let (guard, port) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c = Conn::open(port);
    let ws_name = "wsiso-workspace";
    let ws_id = ws_create(&mut c, ws_name);

    if cfg.no_durability_contract() {
        harness::crash(guard, port);
        let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
        let mut c2 = Conn::open(port2);
        assert!(matches!(c2.cmd_s(&["PING"]), crate::resp::Resp::Simple(_)));
        drop(guard2);
        return;
    }

    harness::wait_for_wal_v3_bytes_any_shard(&dir, cfg.shards, ws_name.as_bytes());
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c2 = Conn::open(port2);
    assert!(
        ws_list_contains(&mut c2, &ws_id),
        "cell {}: WS CREATE must survive kill-9",
        cfg.label
    );
    drop(guard2);
}

/// MQ isolated: stream content, ACK-subset, cursor resume, DLQ routing,
/// trigger registration (pattern: `crash_recovery_mq_effects.rs`, condensed).
pub fn mq_isolated(cfg: &Config) {
    let dir = harness::unique_dir(&format!("mqiso-{}", cfg.label));
    let (guard, port) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c = Conn::open(port);
    let ordersq = "mqiso-ordersq";
    let dlqtestq = "mqiso-dlqtestq";

    mq_create(&mut c, ordersq, 0);
    for i in 1..=5u32 {
        mq_push(&mut c, ordersq, "seq", &i.to_string());
    }
    let popped = mq_pop(&mut c, ordersq, 3);
    assert_eq!(popped.len(), 3, "must claim exactly 3 messages");
    let claimed_ids: Vec<Vec<u8>> = popped
        .iter()
        .map(|entry| match entry {
            crate::resp::Resp::Array(Some(cells)) => match &cells[0] {
                crate::resp::Resp::Bulk(Some(id)) => id.clone(),
                other => panic!("unexpected id cell: {other:?}"),
            },
            other => panic!("unexpected pop entry: {other:?}"),
        })
        .collect();
    let acked = mq_ack(&mut c, ordersq, &claimed_ids[0..2]);
    assert_eq!(acked, 2, "must ack exactly 2 messages");

    mq_create(&mut c, dlqtestq, 1);
    mq_push(&mut c, dlqtestq, "f", "a");
    mq_push(&mut c, dlqtestq, "f", "b");
    let dlq_popped = mq_pop(&mut c, dlqtestq, 2);
    assert_eq!(
        dlq_popped.len(),
        0,
        "MAXDELIVERY 1 routes both straight to DLQ"
    );
    assert_eq!(mq_dlqlen(&mut c, dlqtestq), 2);

    if cfg.no_durability_contract() {
        harness::crash(guard, port);
        let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
        let mut c2 = Conn::open(port2);
        assert!(matches!(c2.cmd_s(&["PING"]), crate::resp::Resp::Simple(_)));
        drop(guard2);
        return;
    }

    let marker = "XPLANE-MQ-MARKER".to_string();
    mq_push(&mut c, ordersq, "final", &marker);
    harness::wait_for_wal_v3_bytes_any_shard(&dir, cfg.shards, marker.as_bytes());
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c2 = Conn::open(port2);
    assert_eq!(
        xlen(&mut c2, ordersq),
        6,
        "cell {}: MQ.PUSH'd messages (incl. the sync marker) must survive kill-9",
        cfg.label
    );
    let resumed = mq_pop(&mut c2, ordersq, 10);
    assert_eq!(
        resumed.len(),
        3,
        "cell {}: cursor must resume after msg 3 (msgs 4, 5, marker remain)",
        cfg.label
    );
    assert_eq!(
        mq_dlqlen(&mut c2, dlqtestq),
        2,
        "cell {}: DLQ-routed entries must survive kill-9",
        cfg.label
    );
    drop(guard2);
}

/// Cross-store TXN, split into two independently-gated functions (review
/// round 3, P1: the two halves have UNRELATED root causes and must not
/// share one `red_guard` — see each function's own doc):
///
/// - [`txn_isolated_committed`]: a committed transaction spanning
///   KV+graph must survive kill-9 exactly like a non-transactional write
///   (extends `sharded_multi_exec_durability.rs`'s single-plane proof
///   cross-plane). RED on `prod_s1`/`prod_s4` — task #52.
/// - [`txn_isolated_atomicity`]: a transaction queued but killed BEFORE it
///   commits must apply NOTHING (atomicity — the "kill mid cross-store TXN"
///   kill point). GREEN on `prod_s1`/`prod_s4` (unrelated to task #52) —
///   gating it behind the SAME `red_guard` as the committed-exec half would
///   skip a working, unrelated regression tripwire by default. Still RED on
///   `legacy_yes_s1` (same pre-existing legacy-graph-WAL-replay gap as
///   every other legacy graph cell — `GRAPH.CREATE` itself never
///   reconstructs there, so any post-restart `GRAPH.QUERY` errors "graph
///   not found" regardless of transaction semantics; empirically confirmed,
///   not assumed — see `tests_legacy.rs`).
///
/// Each half runs its OWN spawn/crash/restart cycle (task #52 review round:
/// interleaving them in one server lifetime inserted extra round-trips —
/// queuing the atomicity round's transaction — between the committed-exec
/// round's durability sync-wait and the kill, an avoidable timing confound
/// for a RED-cell repro that needs to be deterministic).
///
/// REAL RED FINDING (kernel M3, task #52 — reviewed and confirmed NOT a
/// harness artifact): on `prod_s1`/`prod_s4` (checkpoint-backed,
/// appendonly=yes), round (1)'s KV leg survives kill-9 but the graph leg
/// does NOT. PR #247's `persist_txn_aof` covers the KV leg of a cross-store
/// transaction; the graph leg's node-create — queued and committed inside
/// the SAME transaction — is not durably replayed. Ruled out as a false
/// negative from this suite's own known harness bugs: the verification
/// query (`MATCH (n:TxnN {id: 1}) RETURN n.id`) matches on the property
/// VALUE, not an internal node handle (unaffected by the mixed-workload
/// node-id-renumbering bug), does not use `VALID_AT` (unaffected by that
/// parse-error bug), and both the KV-leg AOF wait and the graph-leg wal-v3
/// wait (via `graph_addnode_marker`, issued and synced AFTER the
/// transaction commits) complete successfully before the kill — so the
/// graph leg's own WAL flush tick had run by kill time, yet the node is
/// still gone post-restart.
///
/// Determinism conditions (task #52 — the repro is now shaped to remove the
/// two plausible sources of flakiness): (a) `--checkpoint-timeout 3600`
/// rules out a periodic checkpoint Finalize "accidentally" persisting the
/// graph leg via an unrelated snapshot in the window between the commit and
/// the kill (the flag's own default is already 300s vs. this scenario's
/// sub-second runtime, so this mostly documents intent rather than changing
/// behavior — but removes the ambiguity outright); (b) the kill happens
/// IMMEDIATELY after the sync-wait completes, with NO further commands
/// issued in between (round (2)'s queued-uncommitted transaction now runs
/// in its own separate server lifetime, never sharing a process with round
/// (1)). Verified 3× consecutive reproduction at both shards=1 and
/// shards=4 (kernel M3 stage 1 hardening pass) after this restructuring.
///
/// Minimal repro:
/// ```text
/// MULTI
/// SET k v
/// GRAPH.ADDNODE g Label id 1
/// EXEC
/// <sync-wait for both legs' WAL flush: a later AOF marker + a later
///  wal-v3 graph marker, via the SAME connection as the commit>
/// kill -9 IMMEDIATELY (no further commands); restart
/// GET k               -> v                (KV leg survives)
/// GRAPH.QUERY g "MATCH (n:Label {id: 1}) RETURN n.id"  -> empty (graph leg lost)
/// ```
pub fn txn_isolated_committed(cfg: &Config) {
    let dir = harness::unique_dir(&format!("txniso-committed-{}", cfg.label));
    // Long checkpoint-timeout: see this function's own "Determinism
    // conditions" doc above — rules out a periodic checkpoint as the rescuer.
    let extra = ["--checkpoint-timeout", "3600"];
    let (guard, port) = harness::spawn_moon_on(&dir, cfg, &extra);
    let mut c = Conn::open(port);
    let graph = graph_name("txniso");
    graph_create(&mut c, &graph);

    let committed_key = "{txniso}:committed";
    let commit_reply =
        txn_kv_graph_write(&mut c, committed_key, "committed-val", &graph, "TxnN", 1);
    match commit_reply {
        crate::resp::Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2, "commit returns 2 replies")
        }
        other => panic!("commit must return an array: {other:?}"),
    }

    if cfg.no_durability_contract() {
        harness::crash(guard, port);
        let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &extra);
        let mut c2 = Conn::open(port2);
        assert!(matches!(c2.cmd_s(&["PING"]), crate::resp::Resp::Simple(_)));
        drop(guard2);
        return;
    }

    // Two separate log files, two separate writers (KV leg -> AOF, graph
    // leg -> wal-v3) — each needs its own same-family sync check; waiting
    // on only one does not prove the other's flush tick has also run. The
    // committed value proves the KV leg; a follow-up graph marker (issued
    // AFTER the commit, same connection) proves the graph leg, since
    // wal-v3 appends are strictly ordered per shard.
    harness::wait_for_aof_bytes_any_shard(&dir, b"committed-val");
    let graph_marker = "XPLANE-TXN-GRAPH-SYNC".to_string();
    graph_addnode_marker(&mut c, &graph, &graph_marker);
    harness::wait_for_wal_v3_bytes_any_shard(&dir, cfg.shards, graph_marker.as_bytes());
    // Kill IMMEDIATELY — no further commands between the sync-wait
    // completing and the kill (task #52 determinism requirement).
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &extra);
    let mut c2 = Conn::open(port2);
    assert!(
        kv_get_eq(&mut c2, committed_key, "committed-val"),
        "cell {}: committed cross-store transaction (KV leg) must survive \
         kill-9",
        cfg.label
    );
    assert_eq!(
        graph_query_ids(&mut c2, &graph, "MATCH (n:TxnN {id: 1}) RETURN n.id"),
        std::collections::BTreeSet::from([1]),
        "cell {}: committed cross-store transaction (graph leg) must \
         survive kill-9",
        cfg.label
    );
    drop(guard2);
}

/// A transaction queued but killed BEFORE it commits must apply NOTHING
/// (atomicity — the "kill mid cross-store TXN" kill point). Own
/// spawn/crash/restart cycle, fully independent of
/// [`txn_isolated_committed`] — see that function's doc for why.
///
/// Deliberately NOT gated behind the task #52 `red_guard` on
/// `prod_s1`/`prod_s4` (review round 3, P1): this is a genuinely different
/// claim from the committed half (nothing was ever queued to durably apply,
/// vs. a committed transaction losing a leg) and is GREEN there — gating it
/// behind the same reason as the committed half would skip a working,
/// unrelated regression tripwire by default. IS gated on `legacy_yes_s1`
/// via `LEGACY_GRAPH_RED_REASON` (see `tests_legacy.rs`): `graph_create`
/// itself never survives a restart in legacy mode (the same pre-existing
/// WAL-v3-replay gap every other legacy graph cell hits), so the graph-leg
/// assertion below panics on "ERR graph not found" rather than evaluating
/// the atomicity claim at all — confirmed on the VM, not assumed.
///
/// The initial `GRAPH.CREATE` MUST sync-wait before the MULTI is queued and
/// the kill fires (an earlier draft of this function skipped that wait,
/// treating "kill as close to queued as possible" as license to skip ALL
/// sync-waiting — that was wrong): without it, the graph's own existence
/// races the same WAL flush tick as everything else, so a post-restart
/// `GRAPH.QUERY` can error "ERR graph not found" for a reason that has
/// NOTHING to do with the atomicity claim under test, on every config
/// (confirmed on the VM — the earlier unwaited version failed identically
/// on `prod_s1`/`prod_s4`/`legacy_yes_s1` alike, which was the tell that it
/// was a harness bug, not a finding). Waiting for `GRAPH.CREATE` does not
/// weaken the kill point: the transaction itself is still queued and killed
/// with zero settle time.
pub fn txn_isolated_atomicity(cfg: &Config) {
    let dir = harness::unique_dir(&format!("txniso-queued-{}", cfg.label));
    let (guard, port) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c = Conn::open(port);
    let graph = graph_name("txnisoq");
    graph_create(&mut c, &graph);
    if !cfg.no_durability_contract() {
        harness::wait_for_wal_v3_bytes_any_shard(&dir, cfg.shards, graph.as_bytes());
    }

    let queued_key = "{txnisoq}:queued-uncommitted";
    c.pipeline_s(&[
        vec!["MULTI"],
        vec!["SET", queued_key, "must-not-persist"],
        vec!["GRAPH.ADDNODE", &graph, "TxnN", "id", "2"],
    ]);
    // Deliberately never committed, no wait for the transaction itself —
    // kill as close to "queued" as possible (only the PRECEDING
    // GRAPH.CREATE is sync-waited, see above).
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c2 = Conn::open(port2);

    if cfg.no_durability_contract() {
        assert!(matches!(c2.cmd_s(&["PING"]), crate::resp::Resp::Simple(_)));
        drop(guard2);
        return;
    }

    assert!(
        matches!(
            c2.cmd_s(&["GET", queued_key]),
            crate::resp::Resp::Bulk(None)
        ),
        "cell {}: a transaction queued but never committed must apply \
         NOTHING (KV leg) — atomicity violated if this key exists",
        cfg.label
    );
    assert_eq!(
        graph_query_ids(&mut c2, &graph, "MATCH (n:TxnN {id: 2}) RETURN n.id"),
        std::collections::BTreeSet::new(),
        "cell {}: a transaction queued but never committed must apply \
         NOTHING (graph leg) — atomicity violated if this node exists",
        cfg.label
    );
    drop(guard2);
}

// ---------------------------------------------------------------------------
// Concurrent mixed-workload scenarios (the genuinely new coverage)
// ---------------------------------------------------------------------------

/// Concurrent writes across KV/graph/vector/WS/MQ on ONE shard, a randomized
/// synced prefix per plane, kill, verify.
pub fn mixed_synced(cfg: &Config) {
    for iter in 0..crate::soak_iters() {
        let dir = harness::unique_dir(&format!("mixedsync-{}-{iter}", cfg.label));
        let (guard, port) = harness::spawn_moon_on(&dir, cfg, &[]);
        let tag = format!("mixedsync{iter}");
        let sync_fraction = if cfg.no_durability_contract() {
            0.0
        } else {
            harness::jitter_range(3003 + iter as u64, 3, 10) as f64 / 10.0
        };
        let truth = mixed::run_mixed_workload_synced(
            &dir,
            port,
            cfg.shards,
            &tag,
            MixedPlan::default(),
            sync_fraction,
        );
        harness::crash(guard, port);

        let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
        let mut c2 = Conn::open(port2);
        if cfg.no_durability_contract() {
            assert!(matches!(c2.cmd_s(&["PING"]), crate::resp::Resp::Simple(_)));
        } else {
            mixed::assert_mixed_truth_recovered(&mut c2, &truth);
        }
        drop(guard2);
    }
}

/// Mixed workload + kill in the probabilistic checkpoint-Finalize window
/// (see the module doc's honesty note — this is NOT per-step fault
/// injection). Only meaningful when `disk_offload` is enabled (Finalize is
/// the checkpoint-backed-mode-only code path, brief §1.1).
///
/// FIXED (kernel M3 stage 2 / K2, task #53): on both `prod_s1` and
/// `prod_s4`, some kill offsets in the 0-150ms post-`BGSAVE` window used
/// to cause TOTAL loss of the graph plane's content — not partial loss of
/// the unsynced tail, but the FULL set of already wal-v3-synced nodes.
/// Root cause: `save_graph_store` (`src/graph/recovery.rs`) wrote the
/// reference/floor (`graph_metadata.json`) BEFORE the payload it claims
/// durable (CSR segments + `manifest.json`) — an ARIES-inverted write
/// order; a kill-9 between the two writes left a fully-advanced floor
/// pointing at a payload that never reached disk. Fixed by reordering
/// `save_graph_store` to write the payload first, the floor last (atomic
/// via `atomic_write_durable`). Confirmed via `MOON_CRASH_MATRIX_RED=1
/// MOON_CRASH_MATRIX_ITERS=20` soak: 20/20 clean at both shard counts
/// (pre-fix baseline: prod_s1 hit at iteration 11/20, prod_s4 at iteration
/// 7/20). See `tests/crash_matrix_cross_plane.rs`'s module doc for the
/// full writeup.
pub fn mixed_mid_checkpoint(cfg: &Config) {
    assert!(
        cfg.disk_offload,
        "mixed_mid_checkpoint requires disk-offload enable"
    );
    for iter in 0..crate::soak_iters() {
        let dir = harness::unique_dir(&format!("mixedckpt-{}-{iter}", cfg.label));
        let (guard, port) = harness::spawn_moon_on(&dir, cfg, &[]);
        let tag = format!("mixedckpt{iter}");
        let truth = mixed::run_mixed_workload_synced(
            &dir,
            port,
            cfg.shards,
            &tag,
            MixedPlan::default(),
            1.0,
        );

        let mut c = Conn::open(port);
        let before = std::fs::read(harness::control_file_path(&dir)).ok();
        // BGSAVE replies `+Background saving started\r\n` (see
        // `command::persistence::bgsave_start_sharded`), NOT `+OK` — a
        // single call, checked once, accepting either a simple-string
        // acknowledgement or the (benign, racy) already-in-progress error.
        let bgsave_reply = c.cmd_s(&["BGSAVE"]);
        assert!(
            matches!(bgsave_reply, crate::resp::Resp::Simple(_))
                || matches!(&bgsave_reply, crate::resp::Resp::Error(e)
                    if e.to_lowercase().contains("already in progress")),
            "cell {}: BGSAVE must be accepted (or report already-in-progress), \
             got {bgsave_reply:?}",
            cfg.label
        );
        // Randomized short sleep across the window a small checkpoint
        // typically finalizes in — see module doc's honesty note.
        let jitter_ms = harness::jitter_range(4004 + iter as u64, 0, 150);
        std::thread::sleep(Duration::from_millis(jitter_ms));
        let _ = &before; // best-effort signal only, not asserted pre-kill
        harness::crash(guard, port);

        let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
        let mut c2 = Conn::open(port2);
        mixed::assert_mixed_truth_recovered(&mut c2, &truth);
        drop(guard2);
    }
}

/// Mixed workload + kill mid-Pass-C-recycle: tiny `--wal-segment-size` /
/// `--max-wal-size` force several autovacuum Pass C ticks within seconds
/// (pattern: `crash_recovery_wal_recycle_legacy.rs`). This is the kill
/// point most directly relevant to K2 (Stage 2) — it targets exactly the
/// recycle-floor invariant the brief's whole milestone is about.
pub fn mixed_mid_pass_c(cfg: &Config) {
    for iter in 0..crate::soak_iters() {
        let dir = harness::unique_dir(&format!("mixedpassc-{}-{iter}", cfg.label));
        let extra = [
            "--wal-segment-size",
            "32kb",
            "--max-wal-size",
            "96kb",
            "--autovacuum-interval-secs",
            "1",
        ];
        let (guard, port) = harness::spawn_moon_on(&dir, cfg, &extra);
        let tag = format!("mixedpassc{iter}");

        // Sync a small mixed batch first so there is real plane history in
        // the earliest (soon-to-be-recycle-eligible) segment.
        let truth = mixed::run_mixed_workload_synced(
            &dir,
            port,
            cfg.shards,
            &tag,
            MixedPlan {
                kv_n: 5,
                graph_n: 5,
                vec_n: 5,
                mq_n: 5,
            },
            1.0,
        );

        // Blow past the tiny WAL ceiling with padded GRAPH writes (Pass C /
        // `--max-wal-size` pressure is a wal-v3-only concept — plain KV SETs
        // ride the AOF and would never grow wal-v3 at all, so padding must
        // land on a plane that actually appends there; pattern:
        // `crash_recovery_wal_recycle_legacy.rs`), then give the 1s
        // autovacuum tick several chances to run before killing.
        let mut c = Conn::open(port);
        let padding = "x".repeat(2048);
        let pad_graph = graph_name(&format!("{tag}pad"));
        graph_create(&mut c, &pad_graph);
        for i in 0..200u64 {
            c.cmd_s(&[
                "GRAPH.ADDNODE",
                &pad_graph,
                "Bulk",
                "pad",
                &padding,
                "id",
                &i.to_string(),
            ]);
        }
        std::thread::sleep(Duration::from_secs(5));
        harness::crash(guard, port);

        let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &extra);
        let mut c2 = Conn::open(port2);
        if cfg.no_durability_contract() {
            assert!(matches!(c2.cmd_s(&["PING"]), crate::resp::Resp::Simple(_)));
        } else {
            mixed::assert_mixed_truth_recovered(&mut c2, &truth);
        }
        drop(guard2);
    }
}

/// `BGSAVE` + poll `INFO persistence` until `rdb_bgsave_in_progress:0` —
/// makes each checkpoint in [`graph_drop_survives_repeated_checkpoints`] an
/// awaited, deterministic step instead of a fire-and-forget async kick.
/// `SAVE_IN_PROGRESS` (`src/command/persistence.rs`) is a single
/// process-global atomic cleared only once every shard's local checkpoint
/// finishes, so polling it via any one connection is valid at every shard
/// count this suite runs, including `prod_s4`.
fn wait_for_bgsave(c: &mut Conn) {
    let reply = c.cmd_s(&["BGSAVE"]);
    assert!(
        matches!(reply, crate::resp::Resp::Simple(_))
            || matches!(&reply, crate::resp::Resp::Error(e)
                if e.to_lowercase().contains("already in progress")),
        "BGSAVE must be accepted (or report already-in-progress), got {reply:?}"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let info = c.cmd_s(&["INFO", "persistence"]).flat();
        if info.contains("rdb_bgsave_in_progress:0") {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "BGSAVE never completed within 20s (INFO persistence: {info})"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Task #53 review round 2 / P0-1 regression cell: a `GRAPH.DELETE`'d
/// graph must stay deleted across repeated checkpoints, even though the
/// delete leaves the graph map EMPTY (`graph_count() == 0`).
///
/// Root cause this cell pins down: `persist_graph_at_checkpoint`'s old
/// short-circuit (`!is_dirty() || graph_count() == 0`) skipped
/// `save_graph_store` whenever a delete emptied the graph map — even
/// though the delete itself marks the store dirty via the WAL drain — so
/// `graph_metadata.json` kept describing the PRE-delete graph while later
/// checkpoints kept advancing `control.graph_floor_lsn` past the WAL
/// segment holding the DELETE record. Once that segment is recycled, a
/// crash + restart loads the stale metadata with nothing left in the WAL
/// to replay the delete: the dropped graph resurrects. Fixed by dropping
/// `graph_count() == 0` from the short-circuit — dirty alone gates it now;
/// `save_graph_store` on an empty graph store correctly no-ops its
/// per-graph loop while still rewriting `graph_metadata.json` to describe
/// zero graphs, durably advancing the floor past the delete.
///
/// Tiny `--wal-segment-size`/`--max-wal-size` (pattern: `mixed_mid_pass_c`)
/// force real WAL segment rotation so the DELETE's record actually becomes
/// recycle-eligible within the test's lifetime; `--checkpoint-timeout
/// 3600` disables the periodic background checkpoint so every checkpoint
/// in this cell is an explicit, awaited `BGSAVE` (`wait_for_bgsave`) —
/// deterministic step count, no timer race with the kill point.
pub fn graph_drop_survives_repeated_checkpoints(cfg: &Config) {
    assert!(
        cfg.disk_offload,
        "graph_drop_survives_repeated_checkpoints requires disk-offload enable"
    );
    let dir = harness::unique_dir(&format!("graphdrop-{}", cfg.label));
    let extra = [
        "--wal-segment-size",
        "32kb",
        "--max-wal-size",
        "96kb",
        "--checkpoint-timeout",
        "3600",
        // Default is 30s (`--autovacuum-interval-secs`, `src/config.rs`) —
        // far too slow for this cell to observe a Pass C tick. Ordinary
        // checkpoint recycle (`recycle_segments_before`) respects a
        // hardcoded 48 MiB `min_wal_bytes` floor with no CLI override, so
        // it NEVER physically removes segments at this test's WAL volume
        // regardless of how low the (buggy) floor computes — only Pass C's
        // `recycle_aggressive` (which skips that floor check) can, so this
        // cell is unreproducible without it.
        "--autovacuum-interval-secs",
        "1",
    ];
    let (guard, port) = harness::spawn_moon_on(&dir, cfg, &extra);
    let mut c = Conn::open(port);
    let name = graph_name("graphdrop");

    graph_create(&mut c, &name);
    let handles: Vec<i64> = (0..6)
        .map(|i| graph_addnode(&mut c, &name, "N", i))
        .collect();
    for i in 0..6 {
        graph_addedge(&mut c, &name, handles[i], handles[(i + 1) % 6]);
    }

    // Checkpoint 1: the graph exists — durably snapshots it. This is the
    // pre-delete state `graph_metadata.json` must NOT still describe once
    // the delete below has been through a later checkpoint.
    wait_for_bgsave(&mut c);

    graph_delete(&mut c, &name);

    // Several more checkpoints under WAL-v3 traffic, each round padded
    // enough to roll the tiny WAL ceiling and each checkpoint fully
    // awaited. Padding must NOT (a) touch the SAME `GraphStore` in a way
    // that leaves `graph_count() > 0` at a later checkpoint time, or
    // (b) land on a plane `segment_plane_scan` treats as having no
    // snapshot format (WorkspaceCreate/Drop, MQ, GraphTemporal,
    // TemporalUpsert) — those block recycling of ANY segment that holds
    // them, including a segment that also holds the DELETE record this
    // cell targets. Two earlier designs were hand-verified against the
    // pre-fix binary and BOTH failed to reproduce the bug for exactly
    // these reasons before this one was found:
    //   1. A single live padding graph: creating it un-empties
    //      `graph_count()`, so the very next checkpoint takes the NORMAL
    //      (non-buggy) path and correctly rewrites `graph_metadata.json`
    //      — which also correctly excludes the deleted graph, self-healing
    //      the exact precondition under test before any crash occurs.
    //   2. MQ pushes: `segment_plane_scan` (`src/persistence/wal_v3/segment.rs`)
    //      blocks recycling of any segment holding an `MqPush` record, and
    //      that block covers the SAME segment holding the DELETE (pushes
    //      immediately follow it) — so the DELETE's segment survives
    //      autovacuum and WAL replay correctly re-applies the delete on
    //      restart despite the stale metadata, masking the bug.
    //
    // Fix: pad with THROWAWAY graphs that are created AND deleted within
    // the same round, before that round's checkpoint — `graph_count()` is
    // back to 0 by checkpoint time every round, so the buggy short-circuit
    // keeps firing (never self-heals) while `GRAPH.ADDNODE`/`GRAPH.DELETE`
    // are still real WAL-v3 `Command` records (unconditionally logged,
    // unlike plain KV SETs which need `--wal-kv-log on` and still bypass
    // wal-v3 on connection-local writes per the recovery warning text) —
    // enough volume to roll the tiny segment ceiling. Same hash tag as
    // `name` so the padding lands on the SAME shard at `prod_s4` (cross-tag
    // padding would roll a sibling shard's WAL, not the one holding the
    // DELETE).
    let padding = "x".repeat(2048);
    const ROUNDS: u64 = 4;
    for round in 0..ROUNDS {
        let pad_graph = format!("{{graphdrop}}pad{round}");
        graph_create(&mut c, &pad_graph);
        for i in 0..80u64 {
            graph_addnode_marker(&mut c, &pad_graph, &format!("{padding}{round}-{i}"));
        }
        graph_delete(&mut c, &pad_graph);
        wait_for_bgsave(&mut c);
    }
    // Give the 1s autovacuum Pass C tick several chances to aggressively
    // recycle (pattern: `mixed_mid_pass_c`) before the kill — this is what
    // physically removes the WAL segment holding the DELETE record.
    std::thread::sleep(Duration::from_secs(4));

    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &extra);
    let mut c2 = Conn::open(port2);
    let names = graph_list_names(&mut c2);
    assert!(
        !names.contains(&name),
        "cell {}: GRAPH.DELETE'd graph {name:?} resurrected after \
         {ROUNDS} post-delete checkpoints + kill-9 — GRAPH.LIST = {names:?}",
        cfg.label,
    );
    drop(guard2);
}

/// Genuinely-concurrent mid-burst kill: no synchronization at all. Proves
/// only "no corruption" (every plane still answers well-formed queries) —
/// see `mixed::start_concurrent_burst`'s doc for why no content assertion
/// is possible here.
pub fn concurrent_burst_no_corruption(cfg: &Config) {
    for iter in 0..crate::soak_iters() {
        // Scoped to this ONE iteration (see the guard's own doc for why
        // process-wide would be unsafe in a 37-test binary) — the burst
        // writer threads dying at kill -9 with connection-reset/mid-frame
        // panics is expected noise, not signal; this keeps stderr readable
        // without risking silently swallowing a genuine panic elsewhere.
        let _panic_filter = mixed::BenignDisconnectPanicFilter::install();
        let dir = harness::unique_dir(&format!("burst-{}-{iter}", cfg.label));
        let (guard, port) = harness::spawn_moon_on(&dir, cfg, &[]);
        let tag = format!("burst{iter}");
        mixed::start_concurrent_burst(port, &tag);
        harness::crash(guard, port);

        let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
        let mut c2 = Conn::open(port2);
        assert!(
            matches!(c2.cmd_s(&["PING"]), crate::resp::Resp::Simple(_)),
            "cell {}: server must come back up cleanly after a mid-burst kill",
            cfg.label
        );
        // No-corruption spot checks: every plane must either answer cleanly
        // OR fail with the ordinary "this schema object doesn't exist"
        // error — that is a legitimate outcome here, not corruption, since
        // `start_concurrent_burst` deliberately does NOT sync-wait the
        // one-shot GRAPH.CREATE/FT.CREATE/MQ.CREATE calls before returning
        // (nothing in this scenario is synced — see its own doc). A crash
        // landing before those schema ops reach disk is a real, valid
        // outcome, not a bug. What must NEVER happen is any OTHER error —
        // that would mean the crash corrupted a plane's on-disk structures
        // badly enough that even a fresh boot can't parse them, which is
        // the actual invariant this scenario exists to check.
        assert!(
            !is_unexpected_plane_error(
                &c2.cmd_s(&[
                    "GRAPH.QUERY",
                    &graph_name(&tag),
                    "MATCH (n:BurstN) RETURN count(n)"
                ]),
                &["ERR graph not found"],
            ),
            "cell {}: graph plane must not error (beyond ordinary \
             not-found) after a mid-burst kill",
            cfg.label
        );
        // `FT.SEARCH ... "*" LIMIT 0 1` is not valid syntax for this engine
        // at all (it requires a `*=>[KNN k @field $B]` clause — see
        // `planes::vec_search_contains`) and would error unconditionally
        // regardless of corruption, so it cannot serve as a no-corruption
        // probe. `FT.INFO` needs no query vector and answers cleanly with
        // just "Unknown Index name" when the index was never durable —
        // exactly the same benign-not-found shape as the graph/MQ checks.
        // Exact strings (`src/command/vector_search/ft_info.rs`,
        // `ft_aggregate.rs`/`hybrid.rs`/`ft_search/dispatch.rs`), not bare
        // substrings (review round 3, P2): a substring match on "unknown
        // index" would ALSO classify a hypothetical future corruption
        // message like "unknown index format" as benign — every not-found
        // error this engine emits is a short, fixed, exact literal (never a
        // longer message with a variable/diagnostic suffix), so exact
        // equality is strictly correct here and closes that door.
        assert!(
            !is_unexpected_plane_error(
                &c2.cmd_s(&["FT.INFO", &vec_index_name(&tag)]),
                &[
                    "ERR unknown index",
                    "Unknown Index name",
                    "ERR no such index"
                ],
            ),
            "cell {}: vector plane must not error (beyond ordinary \
             not-found) after a mid-burst kill",
            cfg.label
        );
        // XLEN on a never-durable queue replies Int(0), so no benign
        // not-found error strings need allowlisting — any error reply at
        // all is an unexpected MQ-plane failure. (The previous
        // `as_int_or_zero(..) >= 0` form was vacuous: it mapped error
        // replies to 0 and could never fail.)
        let mq_reply = c2.cmd_s(&["XLEN", &mq_queue_name(&tag)]);
        assert!(
            !is_unexpected_plane_error(&mq_reply, &[]),
            "cell {}: MQ plane must not error after a mid-burst kill (got {mq_reply:?})",
            cfg.label
        );
        drop(guard2);
    }
}

/// True if `r` is an error reply whose text does NOT EXACTLY match (modulo
/// case/whitespace) any of the benign `allowed_exact` messages — i.e. a
/// genuine, unexpected plane error rather than an ordinary "this schema
/// object was never durable before the kill" not-found response.
///
/// Exact matching, not `.contains()` (review round 3, P2): every not-found
/// error this engine emits (`"ERR graph not found"`, `"ERR unknown
/// index"`, `"Unknown Index name"`, `"ERR no such index"`) is a short,
/// fixed, literal `Frame::Error` with no variable suffix — confirmed via a
/// full-repo grep, not assumed. A substring check would ALSO classify a
/// hypothetical future corruption message like `"unknown index format"` as
/// benign purely because it happens to start with the same words; exact
/// equality closes that door without weakening today's coverage (every
/// caller's allowlist already contains the exact real strings).
fn is_unexpected_plane_error(r: &crate::resp::Resp, allowed_exact: &[&str]) -> bool {
    match r {
        crate::resp::Resp::Error(msg) => {
            let lower = msg.trim().to_lowercase();
            !allowed_exact
                .iter()
                .any(|needle| lower == needle.to_lowercase())
        }
        _ => false,
    }
}

/// `appendonly=no` cells: no plane has a durability contract at all
/// (`persistence_dir` never constructed). Assert the documented no-RPO
/// contract explicitly — writes across every plane, kill -9, restart must
/// be clean (no crash-loop, no corrupted half-state) with an EMPTY
/// keyspace, never a silent partial-persist.
pub fn no_durability_liveness(cfg: &Config) {
    assert!(cfg.no_durability_contract());
    let dir = harness::unique_dir(&format!("nodur-{}", cfg.label));
    let (guard, port) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c = Conn::open(port);
    let tag = "nodur";

    kv_set(&mut c, &kv_key(tag, 0), "v0");
    let graph = graph_name(tag);
    graph_create(&mut c, &graph);
    graph_addnode(&mut c, &graph, "N", 0);
    let idx = vec_index_name(tag);
    ft_create_hnsw(&mut c, &idx, &format!("{{{tag}}}:vec:"));
    hset_vec(&mut c, &format!("{{{tag}}}:vec:0"), &vec_blob(0));
    let mq = mq_queue_name(tag);
    mq_create(&mut c, &mq, 0);
    mq_push(&mut c, &mq, "f", "v");
    let ws_id = ws_create(&mut c, "nodur-ws");
    std::thread::sleep(Duration::from_millis(500));
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, cfg, &[]);
    let mut c2 = Conn::open(port2);
    assert!(
        matches!(c2.cmd_s(&["PING"]), crate::resp::Resp::Simple(_)),
        "cell {}: server must restart cleanly even with zero WAL history",
        cfg.label
    );
    assert!(
        matches!(
            c2.cmd_s(&["GET", &kv_key(tag, 0)]),
            crate::resp::Resp::Bulk(None)
        ),
        "cell {}: appendonly=no must not silently persist KV — this cell's \
         entire contract is documented no-RPO, a survivor here would be a \
         worse bug (undocumented partial persistence)",
        cfg.label
    );
    let _ = ws_id; // WS is process-global; not expected to survive a restart either way here
    drop(guard2);
}
