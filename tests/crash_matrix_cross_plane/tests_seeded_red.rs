//! Seeded RED cell 2 (brief §1.4): MQ/WS resurrection via delete-then-
//! restart. Run on `Config::PROD_S1` (appendonly=yes, disk-offload=enable,
//! shards=1) — the config where MQ effect-record durability itself is
//! ALREADY fixed (PR #291), so a resurrection here isolates exactly the one
//! remaining variable: no MQ/WS tombstone record exists yet, so a generic
//! delete does not stop WAL replay from re-materializing the deleted
//! content on the next restart (same bug class as the already-fixed
//! KV/vector cold-plane resurrection, PR #257).
//!
//! Both writes-a-marker-after-the-delete-and-waits-for-it, never for the
//! delete's own payload bytes directly — `MQ CREATE`/`MQ PUSH` records
//! already contain the queue name, so waiting for that substring after a
//! `DEL` would false-positive-match the EARLIER create/push records. A
//! later, unrelated marker proves (by WAL append ordering) that everything
//! before it — including the delete — reached disk.

use crate::harness::{self, Config};
use crate::planes::*;
use crate::resp::Conn;

/// MQ: `DEL <queue>` (generic keyspace delete, NOT an `MQ.*` command) must
/// tombstone the stream so it does not resurrect on the next restart.
///
/// RED (expected, per PR #291's changelog and brief §1.4): `replay_mq_wal`
/// has no way to represent "this queue was deleted after these pushes" —
/// there is no MqDelete/tombstone WAL record — so replay re-materializes
/// the full pre-delete stream content regardless of the `DEL`.
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_seeded_red_mq_generic_del_resurrection() {
    if !harness::red_guard(
        "MQ streams deleted via generic DEL are not tombstoned and \
         resurrect with full content on the next restart — no MQ tombstone \
         WAL record exists yet (PR #291 changelog, kernel M3 brief §1.4). \
         Same bug class as the already-fixed KV/vector cold-plane \
         resurrection (PR #257), not yet applied to MQ. Tracked \
         separately — not this stage's job to fix.",
    ) {
        return;
    }
    let cfg = Config::PROD_S1;
    let dir = harness::unique_dir("seededred-mq");
    let (guard, port) = harness::spawn_moon_on(&dir, &cfg, &[]);
    let mut c = Conn::open(port);
    let q = "seededred-mq-queue";

    mq_create(&mut c, q, 0);
    for i in 1..=5u32 {
        mq_push(&mut c, q, "seq", &i.to_string());
    }
    let marker1 = "SEEDEDRED-MQ-PRE-DEL-SYNC".to_string();
    mq_push(&mut c, q, "final", &marker1);
    harness::wait_for_wal_v3_bytes_any_shard(&dir, cfg.shards, marker1.as_bytes());

    assert_eq!(
        xlen(&mut c, q),
        6,
        "sanity: 6 messages must be visible before DEL"
    );
    c.cmd_s(&["DEL", q]);
    assert_eq!(
        xlen(&mut c, q),
        0,
        "sanity: DEL removed the key immediately"
    );

    // Sync a LATER, unrelated write so its position proves the DEL (which
    // precedes it in append order) is also durable. `DEL` is a generic
    // keyspace command — same log family as any other keyspace write, i.e.
    // the AOF, not wal-v3 (confirmed empirically: plain commands never
    // appear in wal-v3 at all) — so the follow-up sync marker must be
    // AOF-family too, or it proves nothing about the DEL's own durability.
    let marker2 = "SEEDEDRED-MQ-POST-DEL-SYNC".to_string();
    kv_set(&mut c, "seededred-mq-post-del-marker", &marker2);
    harness::wait_for_aof_bytes_any_shard(&dir, marker2.as_bytes());
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, &cfg, &[]);
    let mut c2 = Conn::open(port2);
    assert_eq!(
        xlen(&mut c2, q),
        0,
        "RED: MQ stream {q:?} must NOT resurrect after restart — DEL was \
         issued (and synced) before the crash, so replay re-materializing \
         any content here is exactly the task #43-class resurrection bug"
    );
    drop(guard2);
}

/// WS analogue: `WS DROP <id>` (the workspace registry's OWN delete
/// operation — `WorkspaceRegistry` is process-global, not a normal keyspace
/// key, so a *generic* `DEL` does not apply to it the way it does to an MQ
/// stream) must not resurrect the workspace on the next restart.
///
/// This does NOT assume a RED label — `replay_workspace_wal`
/// (`src/shard/shared_databases.rs`) was read directly for this brief and
/// DOES process `WorkspaceDrop` records in on-disk order, so this specific
/// scenario is plausibly already GREEN; what the brief's §1.4 citation
/// verifies is MQ, grouped with WS only as "the same bug CLASS" at the
/// design-doc level. Run it and report the actual outcome rather than
/// asserting a predetermined answer — if this turns out GREEN, that is
/// itself useful signal (the WS half of "MQ/WS resurrection" may already be
/// closed, or may need a differently-shaped reproduction); if RED, annotate
/// with what actually failed.
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_seeded_red_ws_drop_durability() {
    let cfg = Config::PROD_S1;
    let dir = harness::unique_dir("seededred-ws");
    let (guard, port) = harness::spawn_moon_on(&dir, &cfg, &[]);
    let mut c = Conn::open(port);
    let name = "seededred-ws-workspace";

    let ws_id = ws_create(&mut c, name);
    harness::wait_for_wal_v3_bytes_any_shard(&dir, cfg.shards, name.as_bytes());
    assert!(
        ws_list_contains(&mut c, &ws_id),
        "sanity: workspace visible before DROP"
    );

    ws_drop(&mut c, &ws_id);
    assert!(
        !ws_list_contains(&mut c, &ws_id),
        "sanity: DROP removed it immediately (live path)"
    );

    // Sync a LATER, unrelated wal-v3-family write (same log file, same
    // per-shard append order as WorkspaceDrop itself) so its WAL position
    // proves the DROP is also durable. A KV/AOF-family marker would NOT
    // prove this — the AOF and wal-v3 writers flush independently, so
    // waiting on the AOF gives no guarantee wal-v3 has caught up to the
    // corresponding point.
    let marker = "SEEDEDRED-WS-POST-DROP-SYNC".to_string();
    let sync_graph = "seededred-ws-sync-graph";
    graph_create(&mut c, sync_graph);
    graph_addnode_marker(&mut c, sync_graph, &marker);
    harness::wait_for_wal_v3_bytes_any_shard(&dir, cfg.shards, marker.as_bytes());
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, &cfg, &[]);
    let mut c2 = Conn::open(port2);
    let resurrected = ws_list_contains(&mut c2, &ws_id);
    assert!(
        !resurrected,
        "workspace {name:?} resurrected after WS DROP + kill-9 — \
         WorkspaceDrop WAL record did not replay (or replayed out of order \
         relative to WorkspaceCreate); this is the WS half of the brief \
         §1.4 'MQ/WS resurrection' gap and should be annotated \
         #[ignore = \"RED: ...\"] with this exact failure mode if it \
         reproduces on the VM"
    );
    drop(guard2);
}
