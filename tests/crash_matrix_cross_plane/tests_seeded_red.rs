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
//! before it — including the delete — reached disk. As of task #46, DEL/
//! FLUSHALL's `MqDrop` tombstone lands on the wal-v3 MQ plane (its own
//! fire-and-forget channel, drained on a separate 1ms tick from the AOF
//! writer), so the follow-up sync marker must ALSO be a wal-v3-family write
//! (a throwaway durable queue's own `MQ.PUSH`) — an AOF-only marker proves
//! nothing about whether the MqDrop record itself reached disk.
//!
//! GREEN as of kernel M3 stage 3 (task #46): `MqDrop` (WAL discriminant
//! 0x75, `src/mq/wal.rs`) tombstones a durable stream deleted via generic
//! `DEL`/`UNLINK`/`FLUSHDB`/`FLUSHALL`, applied strictly in WAL order by
//! `replay_mq_wal`/`apply_mq_drop` (`src/shard/shared_databases.rs`) — the
//! former RED cell below (`cross_plane_seeded_red_mq_generic_del_resurrection`,
//! now un-gated) and its FLUSHALL sibling both pass.

use crate::harness::{self, Config};
use crate::planes::*;
use crate::resp::Conn;

/// MQ: `DEL <queue>` (generic keyspace delete, NOT an `MQ.*` command) must
/// tombstone the stream so it does not resurrect on the next restart.
///
/// Formerly RED (PR #291's changelog and brief §1.4): `replay_mq_wal` had
/// no way to represent "this queue was deleted after these pushes" — there
/// was no MqDrop tombstone WAL record — so replay re-materialized the full
/// pre-delete stream content regardless of the `DEL`. Fixed by the `MqDrop`
/// record (task #46); un-gated (no more `harness::red_guard`).
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_seeded_red_mq_generic_del_resurrection() {
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
    // precedes it in append order) is also durable. `DEL` itself logs to
    // the AOF (generic keyspace command family), but as of task #46 it ALSO
    // emits an `MqDrop` record on the wal-v3 MQ plane (a separate
    // fire-and-forget channel, drained on its own 1ms tick) — an AOF-only
    // sync marker proves the AOF is flushed but says nothing about whether
    // the wal-v3 MqDrop record reached disk before the crash. The follow-up
    // sync marker must therefore be wal-v3-family too: a throwaway durable
    // queue's own MQ.PUSH, appended (and hence flushed) strictly AFTER the
    // DEL's MqDrop record in this shard's wal-v3 file.
    let sync_q = "seededred-mq-post-del-sync-queue";
    mq_create(&mut c, sync_q, 0);
    let marker2 = "SEEDEDRED-MQ-POST-DEL-SYNC".to_string();
    mq_push(&mut c, sync_q, "final", &marker2);
    harness::wait_for_wal_v3_bytes_any_shard(&dir, cfg.shards, marker2.as_bytes());

    // Keep the pre-existing AOF-family sync too — DEL's own AOF entry is
    // still a durability requirement independent of the MqDrop tombstone.
    let marker2_aof = "SEEDEDRED-MQ-POST-DEL-AOF-SYNC".to_string();
    kv_set(&mut c, "seededred-mq-post-del-marker", &marker2_aof);
    harness::wait_for_aof_bytes_any_shard(&dir, marker2_aof.as_bytes());
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

/// MQ: `FLUSHALL` must tombstone every durable stream it cleared, the same
/// as a targeted `DEL` above — `auto_drop_mq_streams_on_flush`
/// (`src/shard/mq_exec.rs`) is the FLUSHALL/FLUSHDB sibling of the DEL/
/// UNLINK hook proven by `cross_plane_seeded_red_mq_generic_del_resurrection`.
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_mq_flushall_resurrection() {
    let cfg = Config::PROD_S1;
    let dir = harness::unique_dir("mq-flushall");
    let (guard, port) = harness::spawn_moon_on(&dir, &cfg, &[]);
    let mut c = Conn::open(port);
    let q = "mq-flushall-queue";

    mq_create(&mut c, q, 0);
    mq_push(&mut c, q, "seq", "1");
    let marker1 = "MQ-FLUSHALL-PRE-SYNC".to_string();
    mq_push(&mut c, q, "final", &marker1);
    harness::wait_for_wal_v3_bytes_any_shard(&dir, cfg.shards, marker1.as_bytes());

    assert_eq!(xlen(&mut c, q), 2, "sanity: 2 messages before FLUSHALL");
    match c.cmd_s(&["FLUSHALL"]) {
        crate::resp::Resp::Simple(_) => {}
        other => panic!("FLUSHALL failed: {other:?}"),
    }
    assert_eq!(
        xlen(&mut c, q),
        0,
        "sanity: FLUSHALL cleared it immediately"
    );

    // Same wal-v3-family sync requirement as the DEL test above: FLUSHALL's
    // `MqDrop` tombstones land on the wal-v3 MQ plane, not the AOF, so the
    // proof-of-durability marker must be a wal-v3 write too. A NEW queue
    // created after the FLUSHALL (FLUSHALL clears definitions' contents but
    // this is a fresh key) whose own MQ.PUSH is appended strictly after
    // every tombstone this FLUSHALL emitted.
    let sync_q = "mq-flushall-post-sync-queue";
    mq_create(&mut c, sync_q, 0);
    let marker2 = "MQ-FLUSHALL-POST-SYNC".to_string();
    mq_push(&mut c, sync_q, "final", &marker2);
    harness::wait_for_wal_v3_bytes_any_shard(&dir, cfg.shards, marker2.as_bytes());
    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, &cfg, &[]);
    let mut c2 = Conn::open(port2);
    assert_eq!(
        xlen(&mut c2, q),
        0,
        "MQ stream {q:?} must NOT resurrect after FLUSHALL + kill-9"
    );
    drop(guard2);
}

/// MQ: create -> drop -> create -> kill-9 must survive with the SECOND
/// incarnation intact — an `MqDrop` tombstone only kills the records that
/// precede it for a key, never a later `MqCreate`/`MqPush` for the same
/// key (replay is strictly WAL-ordered; see `apply_mq_drop`'s doc comment).
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_mq_create_drop_create_survives() {
    let cfg = Config::PROD_S1;
    let dir = harness::unique_dir("mq-recreate");
    let (guard, port) = harness::spawn_moon_on(&dir, &cfg, &[]);
    let mut c = Conn::open(port);
    let q = "mq-recreate-queue";

    // First incarnation: created, pushed, then deleted.
    mq_create(&mut c, q, 0);
    mq_push(&mut c, q, "seq", "1");
    c.cmd_s(&["DEL", q]);
    assert_eq!(
        xlen(&mut c, q),
        0,
        "sanity: DEL removed the first incarnation"
    );

    // Second incarnation: re-created with DIFFERENT content — must survive
    // the crash intact, not be killed by the first incarnation's tombstone.
    mq_create(&mut c, q, 0);
    mq_push(&mut c, q, "seq", "100");
    mq_push(&mut c, q, "seq", "101");
    let marker = "MQ-RECREATE-POST-SYNC".to_string();
    mq_push(&mut c, q, "final", &marker);
    harness::wait_for_wal_v3_bytes_any_shard(&dir, cfg.shards, marker.as_bytes());
    assert_eq!(
        xlen(&mut c, q),
        3,
        "sanity: 3 messages in second incarnation"
    );

    harness::crash(guard, port);

    let (guard2, port2) = harness::spawn_moon_on(&dir, &cfg, &[]);
    let mut c2 = Conn::open(port2);
    assert_eq!(
        xlen(&mut c2, q),
        3,
        "second incarnation of {q:?} must survive kill-9 with all 3 \
         messages — the first incarnation's MqDrop tombstone must not \
         kill records that come AFTER it in WAL order"
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
