//! Thread-local database pointer bridge for redis.call/pcall from Lua scripts.
//!
//! The CURRENT_DB thread-local stores a raw pointer to the current Database,
//! set before script execution and cleared after. This is safe because:
//! 1. Each shard is single-threaded (no concurrent access)
//! 2. The pointer is valid for the entire duration of script execution
//! 3. The pointer is cleared immediately after script execution
//!
//! ## Write-effects replication (task #34 Wave A part 2)
//!
//! `EVAL`/`EVALSHA` deliberately carry no `WRITE` command-metadata flag (see
//! `command::metadata::eval_evalsha_never_write_flagged`), so the generic
//! per-command AOF/replication gate in `handler_monoio` never sees the
//! literal `EVAL <script> ...` invocation. Instead, [`make_redis_call_fn`]
//! itself records each successfully-executed, `WRITE`-flagged inner
//! `redis.call`/`redis.pcall` to both durability planes as it happens (see
//! [`LuaEvictionCtx::emit_effect`] / `replication::reason_del::
//! record_effect_write`) — this is the ONLY emission path for a script's
//! writes. Flipping EVAL/EVALSHA to `WRITE` would double-log every write a
//! script makes (the generic gate would replay the raw EVAL a second time
//! on top of the effect records already emitted here) — do not do that.
//!
//! `FCALL` (unlike EVAL/EVALSHA) IS `WRITE`-flagged, mirroring upstream
//! Redis Functions (FCALL always requires write permission; FCALL_RO is the
//! read-only variant) — but that flag only feeds ACL / `READONLY`-replica
//! gating. `try_handle_functions` always consumes FCALL with `continue`
//! before the generic per-command AOF/replication block runs, so FCALL
//! rides the exact same single-emission bridge path as EVAL/EVALSHA.
//!
//! `redis.call('SELECT', ...)` is rejected with a loud script error rather
//! than executed — see the intercept in [`make_redis_call_fn`] for why
//! silently allowing it would corrupt state.
//!
//! ## Layout
//!
//! - `eviction_ctx`: [`LuaEvictionCtx`], the shard handles a script's
//!   eviction/OOM gate and its dual-plane effect emission use.
//! - `txn_capture`: moon#894's MULTI/EXEC capture of a queued script's
//!   effect records, and moon#1285's undo capture of a script's writes
//!   inside an open cross-store TXN (`TXN.ABORT` restores them).
//! - `script_state`: the per-script thread-locals (`CURRENT_DB`, the ACL,
//!   read-only / OOM mode, the write flag) and their setters.
//! - `redis_call`: [`make_redis_call_fn`], the `redis.call`/`redis.pcall`
//!   bridge itself, and the two-database `MOVE`/`COPY` leg.

mod eviction_ctx;
mod redis_call;
mod script_state;
mod txn_capture;

pub use eviction_ctx::LuaEvictionCtx;
pub use redis_call::make_redis_call_fn;
pub use script_state::{
    ScriptOomMode, clear_script_db, is_script_read_only, script_had_write, set_script_db,
    set_script_oom_mode, set_script_read_only, take_script_had_write,
};
pub(crate) use txn_capture::{CapturedEffect, capture_txn_effects, capture_txn_undo};

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::acl::ScriptAcl;
    use crate::config::RuntimeConfig;
    use crate::protocol::Frame;
    use crate::shard::shared_databases::ShardDatabases;
    use crate::storage::Database;

    fn make_config(maxmemory: usize, policy: &str) -> RuntimeConfig {
        RuntimeConfig {
            maxmemory,
            maxmemory_policy: policy.to_string(),
            maxmemory_samples: 5,
            db_maxmemory: Vec::new(),
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            save: None,
            appendonly: "no".to_string(),
            appendfsync: "everysec".to_string(),
            aclfile: None,
            dir: ".".to_string(),
            requirepass: None,
            protected_mode: "yes".to_string(),
            acllog_max_len: 128,
            client_pause_deadline_ms: 0,
            client_pause_write_only: false,
            lazyfree_threshold: 64,
            maxclients: 10000,
            client_query_buffer_limit: 1024 * 1024 * 1024,
            client_query_buffer_limit_preauth: 64 * 1024,
            client_write_timeout_ms: 60_000,
            client_output_buffer_limit_normal: 256 * 1024 * 1024,
            timeout: 0,
            tcp_keepalive: 300,
            num_shards: 1,
        }
    }

    /// Unit-level test of the gate wiring (defect 3, task #34 review): a
    /// black-box EVAL repro (fill via normal SETs, EVAL a write that tips
    /// the shard over `maxmemory`, assert an attached replica also loses the
    /// bystander) is possible but adds a full Lua VM + replica harness for a
    /// single wiring check — this pins the same fact directly against
    /// `LuaEvictionCtx::gate`, which is what actually changed.
    ///
    /// RED (before the fix): `gate` called the non-reporting
    /// `evict_to_budget` with a plain or async-spill `EvictionRun`,
    /// which pass a hardcoded no-op sink all the way down — a bystander key
    /// evicted here to make room for the script's write never reached
    /// `record_reason_del_conn`, so it never reached the AOF pool (nor an
    /// attached replica). GREEN (after): the gate now threads a real sink
    /// through to `record_reason_del_conn`, and the AOF pool observes a
    /// `DEL` for the evicted bystander key.
    #[test]
    #[cfg(feature = "runtime-monoio")]
    fn gate_reports_bystander_eviction_to_aof() {
        // Hold the gate OPEN for this thread regardless of
        // `MAXMEMORY_GLOBAL`'s CURRENT value — that atomic is process-global
        // and mutated by other tests running concurrently in this same
        // `cargo test --lib` binary
        // (`eviction::tests::maxmemory_publish_and_is_set_roundtrip`), so
        // depending on it here would be flaky under parallel execution.
        // Before G1/L3a a wired `spill_sender` bypassed the fast path by
        // itself; the gate is now keyed on `write_gate_active()` alone.
        // `manifest` is always `None` at this call site (real production
        // behavior, not a test shortcut — see `EvictionSink::AsyncSpill`'s
        // doc comment), and `config.appendonly == "no"` (the `make_config`
        // default), so this deterministically takes the "no manifest
        // reachable" plain-drop fallback inside `evict_to_budget` — the
        // sender/shard_dir below are never actually touched by that branch,
        // just required by the signature.
        let _gate_open = crate::storage::eviction::force_write_gate(true);
        let (shard_databases, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        let runtime_config = Arc::new(parking_lot::RwLock::new(make_config(1, "allkeys-lru")));

        let (tx, rx) =
            crate::runtime::channel::mpsc_bounded::<crate::persistence::aof::AofMessage>(64);
        let pool = crate::persistence::aof::AofWriterPool::top_level(tx);

        let (spill_tx, _spill_rx) =
            flume::bounded::<crate::storage::tiered::spill_thread::SpillRequest>(4);
        let tmp = tempfile::tempdir().unwrap();

        let ctx = LuaEvictionCtx::new(
            shard_databases,
            runtime_config,
            0,
            Some(spill_tx),
            Rc::new(Cell::new(1)),
            Some(tmp.path().to_path_buf()),
            1,    // num_shards
            None, // repl_state
            Some(pool),
        );

        let mut db = Database::new();
        for i in 0..50 {
            db.set_string(
                &Bytes::from(format!("bystander:{i}")),
                Bytes::from(vec![0u8; 200]),
            );
        }
        let before_len = db.len();
        assert!(before_len > 0, "setup invariant: fixture must be non-empty");

        // maxmemory=1 byte forces the allkeys-lru policy to evict bystander
        // keys down toward empty on the very next gate check — none of them
        // are anything a script wrote (this call simulates the check BEFORE
        // the script's own write executes).
        let result = ctx.gate(&mut db, 0);
        assert!(result.is_ok(), "gate must succeed once the db is emptied");
        assert!(
            db.len() < before_len,
            "setup invariant: gate must have evicted at least one bystander key"
        );

        let mut saw_del_for_bystander = false;
        while let Ok(crate::persistence::aof::AofMessage::Append { bytes, .. }) = rx.try_recv() {
            let text = String::from_utf8_lossy(&bytes);
            if text.contains("DEL") && text.contains("bystander:") {
                saw_del_for_bystander = true;
            }
        }
        assert!(
            saw_del_for_bystander,
            "bystander eviction inside the Lua gate must emit a DEL record to the AOF plane"
        );
    }

    /// G1/L3a: with no `maxmemory` and no per-db quota configured, a wired
    /// spill sender must NOT pull every script write through
    /// `evict_to_budget`. Eviction ROUTING (spill vs plain drop) only exists
    /// once a victim is selected, and `evict_to_budget` selects none when
    /// `maxmemory == 0` — so the call was a pure no-op costing a
    /// `RuntimeConfig` read-lock pair, an `elastic_budget` load and an
    /// `EvictionRun` build per `redis.call` on every default server
    /// (`--disk-offload enable` wires the sender).
    ///
    /// RED (before L3a): the fast path was keyed on `spill_sender.is_none()`
    /// — a CONFIG predicate — so `Some(sender)` entered the gate on every
    /// write. GREEN: keyed on the STATE predicate
    /// `eviction::write_gate_active()`.
    ///
    /// `MAXMEMORY_GLOBAL` / `DB_MAXMEMORY_ANY_SET` are process-global, so this
    /// test ESTABLISHES the unset state it needs under a `PublishedLimits`
    /// guard rather than observing whatever ambient state the suite left
    /// behind, and the guard puts back what was there on drop.
    ///
    /// It used to loop 100 attempts, sampling both atomics around each gate
    /// call and `continue`-ing when a sibling had a limit published — then
    /// panicking with "could not observe an unset maxmemory in 100 attempts".
    /// That retry made a PERMANENT leak (moon#856: five `command::config`
    /// tests published a limit and never restored it) read as a flake for
    /// months, and the CI waiver built on top of it kept the suite green. Both
    /// are gone: the assertion below fires immediately and names the state it
    /// actually saw. The probe counter is thread-local, so no other test's
    /// `evict_to_budget` call can move it.
    #[test]
    fn gate_is_skipped_with_spill_sender_when_no_limit_is_configured() {
        use crate::storage::db_quota::db_maxmemory_any_set;
        use crate::storage::eviction::{
            PublishedLimits, evict_to_budget_entries_on_this_thread, maxmemory_bytes,
            maxmemory_is_set,
        };

        let _limits = PublishedLimits::capture();
        crate::storage::eviction::publish_maxmemory(0);
        crate::storage::db_quota::publish_db_maxmemory_any_set(&RuntimeConfig::default());

        let (shard_databases, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        let runtime_config = Arc::new(parking_lot::RwLock::new(make_config(0, "allkeys-lru")));
        let (spill_tx, _spill_rx) =
            flume::bounded::<crate::storage::tiered::spill_thread::SpillRequest>(4);
        let tmp = tempfile::tempdir().unwrap();
        let ctx = LuaEvictionCtx::new(
            shard_databases,
            runtime_config,
            0,
            Some(spill_tx),
            Rc::new(Cell::new(1)),
            Some(tmp.path().to_path_buf()),
            1,
            None,
            None,
        );
        let mut db = Database::new();
        for i in 0..8 {
            db.set_string(&Bytes::from(format!("k:{i}")), Bytes::from(vec![0u8; 64]));
        }
        let len_before = db.len();

        // PRECONDITION, asserted rather than hoped for: the guard above put
        // both atomics in the unset state this test is about. If this fires,
        // something published a limit between the guard and here.
        assert!(
            !maxmemory_is_set() && !db_maxmemory_any_set(),
            "precondition: no limit may be published here, but maxmemory={} \
             db_maxmemory_any_set={} — a sibling test is leaking one (moon#856)",
            maxmemory_bytes(),
            db_maxmemory_any_set()
        );

        let entries_before = evict_to_budget_entries_on_this_thread();
        let result = ctx.gate(&mut db, 0);
        let entries_after = evict_to_budget_entries_on_this_thread();

        assert!(
            result.is_ok(),
            "no limit configured: the gate must never reject"
        );
        assert_eq!(
            db.len(),
            len_before,
            "nothing may be evicted without a limit (maxmemory={}, db_maxmemory_any_set={})",
            maxmemory_bytes(),
            db_maxmemory_any_set()
        );
        assert_eq!(
            entries_after,
            entries_before,
            "no maxmemory and no db quota: the Lua write gate must not enter \
             evict_to_budget just because a spill sender is wired \
             (observed maxmemory={}, db_maxmemory_any_set={})",
            maxmemory_bytes(),
            db_maxmemory_any_set()
        );
    }

    /// Task #38: `LuaEvictionCtx::is_replica()` must track
    /// `ReplicationState::is_replica_mirror` exactly, including transitions
    /// made AFTER the ctx was constructed — `make_redis_call_fn` calls this
    /// per `redis.call`, so a `REPLICAOF`/`REPLICAOF NO ONE` mid-lifetime
    /// role flip (S3.5a's whole reason for the mirror existing) must be
    /// visible to a long-lived shard's Lua bridge without rebuilding the
    /// ctx.
    ///
    /// RED (before this task): `LuaEvictionInner` carried no
    /// `is_replica_mirror` field at all — `LuaEvictionCtx` had no way to
    /// answer "is this shard a read-only replica right now," so
    /// `make_redis_call_fn` could not reject a writing script on a replica.
    #[test]
    fn is_replica_tracks_role_transitions_after_construction() {
        use crate::replication::state::{ReplicaHandshakeState, ReplicationRole, ReplicationState};

        let (shard_databases, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        let runtime_config = Arc::new(parking_lot::RwLock::new(make_config(0, "noeviction")));
        let repl_state = Arc::new(parking_lot::RwLock::new(ReplicationState::new(
            1,
            "a".repeat(40),
            "0".repeat(40),
        )));

        let ctx = LuaEvictionCtx::new(
            shard_databases,
            runtime_config,
            0,
            None,
            Rc::new(Cell::new(1)),
            None,
            1,
            Some(repl_state.clone()),
            None,
        );

        assert!(
            !ctx.is_replica(),
            "fresh ReplicationState defaults to Master"
        );

        repl_state.write().set_role(ReplicationRole::Replica {
            host: "127.0.0.1".to_string(),
            port: 6379,
            state: ReplicaHandshakeState::PingPending,
        });
        assert!(
            ctx.is_replica(),
            "is_replica() must observe a role flip that happened after ctx construction"
        );

        repl_state.write().set_role(ReplicationRole::Master);
        assert!(
            !ctx.is_replica(),
            "REPLICAOF NO ONE must flip is_replica() back to false"
        );
    }

    /// `is_replica()` on a `disabled()` ctx (no shard context — plain unit
    /// tests of Lua scripts that don't go through a real shard) must be
    /// `false`, never panic.
    #[test]
    fn is_replica_false_for_disabled_ctx() {
        let ctx = LuaEvictionCtx::disabled();
        assert!(!ctx.is_replica());
    }

    /// moon#517 gap 2 — a script write that lands while a snapshot is in
    /// flight must leave the key's epoch-start value behind for the
    /// snapshot, on every runtime and from all three `handle_eval` call
    /// sites (local monoio, local tokio, routed).
    ///
    /// RED (before the fix): `cow_intercept` is reachable only from the
    /// shard event loop's own stack, and all three script sites run
    /// `handle_eval` inside a bare `with_shard(...)`. Nothing captured a
    /// pre-image, so a `BGSAVE` racing an `EVAL` wrote the POST-write value
    /// into a segment the WAL then replays on top of — the double-apply
    /// this whole COW mechanism exists to prevent. GREEN: the bridge takes
    /// the pre-image at the `redis.call` level (per inner command, where a
    /// real key exists — `EVAL`'s own `command[1]` is the script body, so
    /// wrapping the call sites in `cow_intercept` would have captured the
    /// script text as a key).
    ///
    /// Asserted at the queue, not through a full snapshot round-trip: the
    /// drain-and-serialize half is pinned by
    /// `persistence::snapshot_cow::tests::
    /// drain_folds_pending_segments_and_drops_serialized_ones`.
    #[test]
    fn script_write_captures_a_cow_pre_image() {
        use crate::persistence::snapshot_cow;

        let lua = crate::scripting::setup_lua_vm(LuaEvictionCtx::disabled()).unwrap();
        let cache = std::rc::Rc::new(std::cell::RefCell::new(crate::scripting::ScriptCache::new()));
        let mut db = Database::new();
        db.set_string(b"cow517", Bytes::from_static(b"old"));

        snapshot_cow::arm();
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"redis.call('SET', KEYS[1], 'new')")),
            Frame::BulkString(Bytes::from_static(b"1")),
            Frame::BulkString(Bytes::from_static(b"cow517")),
        ];
        let run_script = |lua: &_, cache: &_, args: &_, db: &mut _, a, b, c, d, acl: &_| {
            crate::scripting::handle_eval(lua, cache, args, db, a, b, c, d, acl, false)
        };
        let result = run_script(
            &lua,
            &cache,
            &args,
            &mut db,
            0,
            1,
            0,
            1,
            &ScriptAcl::trusted(),
        );
        let pending = snapshot_cow::pending_for_test();
        snapshot_cow::disarm();

        assert!(
            !matches!(result, Frame::Error(_)),
            "setup invariant: the script must succeed, got {result:?}"
        );
        let captured = pending
            .iter()
            .find(|(db_index, key, _)| *db_index == 0 && key.as_ref() == b"cow517")
            .expect("a script write during a snapshot must capture the key's pre-image");
        match captured.2.value.as_redis_value() {
            crate::storage::compact_value::RedisValueRef::String(s) => {
                assert_eq!(
                    s as &[u8], b"old",
                    "the captured pre-image must be the epoch-start value"
                );
            }
            _ => panic!("expected a string pre-image"),
        }
    }

    /// moon#1068 — a script's `COPY ... DB n` must never land in the script's
    /// OWN database. Before the fix the bridge ran it through the single-db
    /// dispatch, which ignored the DB clause: `:1`, and `b` written into the
    /// source db while the effect record named db 4.
    ///
    /// A unit-test thread is outside the registered database plane, so the
    /// destination cannot be reached here: the command must refuse and leave
    /// the database untouched rather than fall back to the one it holds.
    /// (The reachable case — the copy landing in db 4 — is pinned end to end
    /// by `tests/script_move_copy_db_1068.rs`.) `MOVE`, which errored before
    /// the fix, must likewise leave the key where it was.
    #[test]
    fn script_two_db_write_never_lands_in_the_scripts_own_db() {
        let lua = crate::scripting::setup_lua_vm(LuaEvictionCtx::disabled()).unwrap();
        let cache = std::rc::Rc::new(std::cell::RefCell::new(crate::scripting::ScriptCache::new()));
        let mut db = Database::new();
        db.set_string(b"a", Bytes::from_static(b"1"));
        let run = |db: &mut Database, body: &'static [u8], keys: &[&'static [u8]]| {
            let mut args = vec![
                Frame::BulkString(Bytes::from_static(body)),
                Frame::BulkString(Bytes::from(keys.len().to_string())),
            ];
            args.extend(
                keys.iter()
                    .map(|k| Frame::BulkString(Bytes::from_static(k))),
            );
            crate::scripting::handle_eval(
                &lua,
                &cache,
                &args,
                db,
                0,
                1,
                0,
                16,
                &ScriptAcl::trusted(),
                false,
            )
        };

        let copy = run(
            &mut db,
            b"return redis.pcall('COPY', KEYS[1], KEYS[2], 'DB', '4')",
            &[b"a", b"b"],
        );
        assert!(
            matches!(copy, Frame::Error(_)),
            "an unreachable destination must refuse, got {copy:?}"
        );
        assert!(
            !db.exists(b"b"),
            "COPY ... DB 4 wrote the copy into the script's own db"
        );

        let mv = run(
            &mut db,
            b"return redis.pcall('MOVE', KEYS[1], '3')",
            &[b"a"],
        );
        assert!(matches!(mv, Frame::Error(_)), "got {mv:?}");
        assert!(
            db.exists(b"a"),
            "a refused MOVE must leave the key in place"
        );

        // The two-db errors are redis's, decided before any database is
        // touched: the script's own db, and a db that does not exist.
        let same = run(
            &mut db,
            b"return redis.pcall('MOVE', KEYS[1], '0')",
            &[b"a"],
        );
        assert_eq!(
            same,
            Frame::Error(Bytes::from_static(
                b"ERR source and destination objects are the same"
            ))
        );
        let range = run(
            &mut db,
            b"return redis.pcall('COPY', KEYS[1], KEYS[2], 'DB', '99')",
            &[b"a", b"b"],
        );
        assert_eq!(
            range,
            Frame::Error(Bytes::from_static(b"ERR DB index is out of range"))
        );
        assert!(!db.exists(b"b"));
    }

    /// moon#517 gap 1 — a script's write effect must reach the AOF plane on
    /// EVERY supported runtime, not only `runtime-monoio`.
    ///
    /// RED (before the fix, `--no-default-features --features
    /// runtime-tokio,jemalloc`): `emit_effect`'s only body was
    /// `#[cfg(feature = "runtime-monoio")]`, with a
    /// `#[cfg(not(...))] { let _ = (db_index, cmd_and_args); }` arm that
    /// DISCARDED the effect — so `EVAL "redis.call('SET',KEYS[1],'v')" 1 k`
    /// mutated the keyspace, answered OK, and left nothing behind for AOF
    /// replay to restore. GREEN: the AOF leg is runtime-agnostic (it is a
    /// channel send to the writer pool, exactly what the tokio connection
    /// handler already does for every ordinary write) and now always runs.
    ///
    /// The replication leg stays monoio-only ON PURPOSE — see
    /// `reason_del::record_bytes_conn`: it pushes through
    /// `shard::self_msg`, which only a monoio shard thread may touch, and
    /// master-side PSYNC does not exist under tokio at all (no ordinary
    /// tokio write replicates either). Parity with ordinary writes on the
    /// same runtime is the bar this pins.
    #[test]
    fn emit_effect_reaches_the_aof_plane_on_every_runtime() {
        let (shard_databases, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        let runtime_config = Arc::new(parking_lot::RwLock::new(make_config(0, "noeviction")));
        let (tx, rx) =
            crate::runtime::channel::mpsc_bounded::<crate::persistence::aof::AofMessage>(16);
        let pool = crate::persistence::aof::AofWriterPool::top_level(tx);

        let ctx = LuaEvictionCtx::new(
            shard_databases,
            runtime_config,
            0,
            None,
            Rc::new(Cell::new(1)),
            None,
            1,    // num_shards
            None, // repl_state
            Some(pool),
        );

        let effect = [
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"lua517")),
            Frame::BulkString(Bytes::from_static(b"v")),
        ];
        ctx.emit_effect(0, &effect, &Frame::SimpleString(Bytes::from_static(b"OK")));

        let mut saw_effect = false;
        while let Ok(crate::persistence::aof::AofMessage::Append { bytes, .. }) = rx.try_recv() {
            let text = String::from_utf8_lossy(&bytes);
            if text.contains("SET") && text.contains("lua517") {
                saw_effect = true;
            }
        }
        assert!(
            saw_effect,
            "a script's write effect must be recorded to the AOF plane on this runtime — \
             without it the write is lost on restart"
        );
    }

    /// moon#1241: over budget under `noeviction`, a shrink-only command in a
    /// script passes the gate as it does on the connection path, a growing
    /// one is refused, and a FUNCTION that declared no `allow-oom` keeps the
    /// refusal for both.
    #[test]
    fn shrink_only_script_commands_pass_the_oom_gate_where_redis_allows_them() {
        let _gate_open = crate::storage::eviction::force_write_gate(true);
        let (shard_databases, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        let runtime_config = Arc::new(parking_lot::RwLock::new(make_config(1, "noeviction")));
        let ctx = LuaEvictionCtx::new(
            shard_databases,
            runtime_config,
            0,
            None,
            Rc::new(Cell::new(1)),
            None,
            1,
            None,
            None,
        );
        let mut db = Database::new();
        for i in 0..8 {
            db.set_string(&Bytes::from(format!("k:{i}")), Bytes::from(vec![0u8; 64]));
        }
        assert!(ctx.gate(&mut db, 0).is_err(), "fixture: over budget");

        set_script_oom_mode(ScriptOomMode::Compat);
        for cmd in [&b"DEL"[..], b"unlink", b"HDEL", b"LPOP", b"EXPIRE"] {
            assert!(
                ctx.gate_command(cmd, &mut db, 0).is_ok(),
                "{} must not be refused for memory",
                String::from_utf8_lossy(cmd)
            );
        }
        for cmd in [&b"SET"[..], b"APPEND", b"LPUSH"] {
            assert!(
                ctx.gate_command(cmd, &mut db, 0).is_err(),
                "{} must still be refused",
                String::from_utf8_lossy(cmd)
            );
        }
        assert_eq!(db.len(), 8, "noeviction: nothing was evicted");

        set_script_oom_mode(ScriptOomMode::Deny);
        assert!(
            ctx.gate_command(b"DEL", &mut db, 0).is_err(),
            "no allow-oom declared: refused, as redis refuses the script"
        );
        // PR #1268 review: redis 7.0.15 runs ANY command in an `allow-oom`
        // function over maxmemory (`FCALL` of a SET answers +OK).
        set_script_oom_mode(ScriptOomMode::AllowOom);
        for cmd in [&b"SET"[..], b"APPEND", b"DEL"] {
            assert!(
                ctx.gate_command(cmd, &mut db, 0).is_ok(),
                "allow-oom: {cmd:?}"
            );
        }
        set_script_oom_mode(ScriptOomMode::Compat);
    }
}
