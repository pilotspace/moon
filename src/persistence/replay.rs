//! Command replay for AOF / WAL recovery.
//!
//! ## HEXPIRE-family replay clock semantics
//!
//! Relative-form expiries (`HEXPIRE` seconds-from-now, `HPEXPIRE`
//! millis-from-now) re-base to the **replay-time clock**, not the original
//! wall-clock at write time. An `HEXPIRE k 60 FIELDS 1 f` segment written
//! at T0 and replayed at T0+10min sets the field TTL to (T0+10min) + 60s,
//! not T0+60s.
//!
//! This matches Valkey / Redis `EXPIRE` replay semantics — relative TTLs
//! are intentionally re-based so a stale AOF doesn't immediately re-expire
//! everything on restart. In practice it only affects un-rewritten AOF
//! streams: `BGREWRITEAOF` always emits the absolute `HPEXPIREAT` form
//! (see `aof::generate_rewrite_commands`), which carries the original
//! absolute expiry verbatim and has zero drift.
//!
//! Do not "fix" the relative-form drift without coordinating with the
//! main expiration path — they share the same trade-off by design.

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::HashTtlCond;

/// Parse a Frame as an unsigned integer (BulkString or Integer).
#[inline]
fn frame_as_u64(f: &Frame) -> Option<u64> {
    match f {
        Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<u64>().ok(),
        Frame::Integer(n) => u64::try_from(*n).ok(),
        _ => None,
    }
}

/// Parse a Frame as a non-negative integer (BulkString or Integer) for FIELDS count.
#[inline]
fn frame_as_usize(f: &Frame) -> Option<usize> {
    match f {
        Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<usize>().ok(),
        Frame::Integer(n) => usize::try_from(*n).ok(),
        _ => None,
    }
}

/// Extract a bulk-string Frame as a byte slice.
#[inline]
fn frame_as_bytes(f: &Frame) -> Option<&[u8]> {
    match f {
        Frame::BulkString(b) => Some(b.as_ref()),
        _ => None,
    }
}

/// Convert a HEXPIRE-family expiry argument to absolute unix milliseconds.
///
/// - `kind = "HEXPIRE"` → seconds-from-now
/// - `kind = "HPEXPIRE"` → millis-from-now
/// - `kind = "HEXPIREAT"` → unix seconds
/// - `kind = "HPEXPIREAT"` → unix millis
///
/// `now_ms` is captured from the live database clock at replay time. AOF
/// rewrite always emits HPEXPIREAT so this drift only matters for raw
/// HEXPIRE/HPEXPIRE replays via the original (non-rewritten) AOF stream.
fn replay_expiry_to_abs_ms(kind: &[u8], when: u64, now_ms: u64) -> u64 {
    match kind {
        c if c.eq_ignore_ascii_case(b"HPEXPIREAT") => when,
        c if c.eq_ignore_ascii_case(b"HEXPIREAT") => when.saturating_mul(1000),
        c if c.eq_ignore_ascii_case(b"HPEXPIRE") => now_ms.saturating_add(when),
        c if c.eq_ignore_ascii_case(b"HEXPIRE") => now_ms.saturating_add(when.saturating_mul(1000)),
        _ => when,
    }
}

/// Parse `key, when_value, FIELDS, numfields, field [field...]` after the
/// command verb. The optional NX/XX/GT/LT token is ignored on replay — the
/// recorded TTL value is the authority (the writer already evaluated the
/// gate before recording the entry).
fn replay_hexpire_family(
    db: &mut Database,
    cmd: &[u8],
    args: &[Frame],
) -> Option<()> {
    let key = frame_as_bytes(args.first()?)?;
    let when = frame_as_u64(args.get(1)?)?;

    // Optional cond token between `when` and `FIELDS`. We just skip it.
    let mut idx = 2;
    let after_when = frame_as_bytes(args.get(idx)?)?;
    let fields_idx = if after_when.eq_ignore_ascii_case(b"FIELDS") {
        idx
    } else {
        // NX/XX/GT/LT
        idx += 1;
        let tok = frame_as_bytes(args.get(idx)?)?;
        if !tok.eq_ignore_ascii_case(b"FIELDS") {
            return None;
        }
        idx
    };

    let numfields = frame_as_usize(args.get(fields_idx + 1)?)?;
    let now_ms = db.now_ms();
    let abs_ms = replay_expiry_to_abs_ms(cmd, when, now_ms);

    for i in 0..numfields {
        let field = frame_as_bytes(args.get(fields_idx + 2 + i)?)?;
        let _ = db.hash_set_field_ttl(key, field, abs_ms, HashTtlCond::Always);
    }
    Some(())
}

/// Replay `HPERSIST key FIELDS numfields field [field...]`.
fn replay_hpersist(db: &mut Database, args: &[Frame]) -> Option<()> {
    let key = frame_as_bytes(args.first()?)?;
    let tok = frame_as_bytes(args.get(1)?)?;
    if !tok.eq_ignore_ascii_case(b"FIELDS") {
        return None;
    }
    let numfields = frame_as_usize(args.get(2)?)?;
    for i in 0..numfields {
        let field = frame_as_bytes(args.get(3 + i)?)?;
        let _ = db.hash_persist_field(key, field);
    }
    Some(())
}

/// True if `cmd` is one of the 5 HEXPIRE-family write commands that the
/// replay engine intercepts directly (bypassing the phase-196 handlers).
#[inline]
fn is_hexpire_family_write(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"HEXPIRE")
        || cmd.eq_ignore_ascii_case(b"HPEXPIRE")
        || cmd.eq_ignore_ascii_case(b"HEXPIREAT")
        || cmd.eq_ignore_ascii_case(b"HPEXPIREAT")
        || cmd.eq_ignore_ascii_case(b"HPERSIST")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::DispatchResult;
    use crate::framevec;
    use crate::storage::entry::{Entry, RedisValue};

    fn make_db_with_key(key: &[u8], value: &[u8]) -> Database {
        let mut db = Database::new();
        let mut selected = 0usize;
        let args = framevec![
            Frame::BulkString(bytes::Bytes::copy_from_slice(key)),
            Frame::BulkString(bytes::Bytes::copy_from_slice(value)),
        ];
        let _ = crate::command::dispatch(&mut db, b"SET", &args, &mut selected, 16);
        db
    }

    fn make_db_with_hash(key: &[u8], fields: &[(&[u8], &[u8])]) -> Database {
        let mut db = Database::new();
        let mut entry = Entry::new_hash();
        if let Some(RedisValue::Hash(map)) = entry.value.as_redis_value_mut() {
            for (f, v) in fields {
                map.insert(
                    bytes::Bytes::copy_from_slice(f),
                    bytes::Bytes::copy_from_slice(v),
                );
            }
        }
        db.set(bytes::Bytes::copy_from_slice(key), entry);
        db
    }

    fn get_key(db: &mut Database, key: &[u8]) -> Option<bytes::Bytes> {
        let mut selected = 0usize;
        let args = framevec![Frame::BulkString(bytes::Bytes::copy_from_slice(key))];
        match crate::command::dispatch(db, b"GET", &args, &mut selected, 16) {
            DispatchResult::Response(Frame::BulkString(v)) => Some(v),
            _ => None,
        }
    }

    // ── SWAPDB replay intercept ───────────────────────────────────────────────

    /// SWAPDB during WAL replay must exchange the two databases.
    #[test]
    fn replay_swapdb_exchanges_databases() {
        let engine = DispatchReplayEngine::new();
        let mut databases = vec![
            make_db_with_key(b"key_a", b"val_a"), // db-0
            make_db_with_key(b"key_b", b"val_b"), // db-1
        ];
        let mut selected = 0usize;
        let args = framevec![
            Frame::BulkString(bytes::Bytes::from_static(b"0")),
            Frame::BulkString(bytes::Bytes::from_static(b"1")),
        ];
        engine.replay_command(&mut databases, b"SWAPDB", &args, &mut selected);

        // After swap: db-0 has key_b, db-1 has key_a.
        assert_eq!(
            get_key(&mut databases[0], b"key_b").as_deref(),
            Some(b"val_b".as_ref()),
            "db-0 should have key_b after SWAPDB replay"
        );
        assert_eq!(
            get_key(&mut databases[1], b"key_a").as_deref(),
            Some(b"val_a".as_ref()),
            "db-1 should have key_a after SWAPDB replay"
        );
    }

    /// Same-index SWAPDB is a no-op during replay.
    #[test]
    fn replay_swapdb_same_index_noop() {
        let engine = DispatchReplayEngine::new();
        let mut databases = vec![make_db_with_key(b"mykey", b"myval"), Database::new()];
        let mut selected = 0usize;
        let args = framevec![
            Frame::BulkString(bytes::Bytes::from_static(b"0")),
            Frame::BulkString(bytes::Bytes::from_static(b"0")),
        ];
        engine.replay_command(&mut databases, b"SWAPDB", &args, &mut selected);
        assert_eq!(
            get_key(&mut databases[0], b"mykey").as_deref(),
            Some(b"myval".as_ref()),
            "db-0 should be unchanged after same-index SWAPDB replay"
        );
    }

    /// Out-of-range SWAPDB silently skips during replay (no panic).
    #[test]
    fn replay_swapdb_out_of_range_skips() {
        let engine = DispatchReplayEngine::new();
        let mut databases = vec![make_db_with_key(b"mykey", b"myval"), Database::new()];
        let mut selected = 0usize;
        let args = framevec![
            Frame::BulkString(bytes::Bytes::from_static(b"0")),
            Frame::BulkString(bytes::Bytes::from_static(b"99")),
        ];
        // Must not panic.
        engine.replay_command(&mut databases, b"SWAPDB", &args, &mut selected);
        assert_eq!(
            get_key(&mut databases[0], b"mykey").as_deref(),
            Some(b"myval".as_ref()),
            "db-0 should be unchanged after out-of-range SWAPDB replay"
        );
    }

    // ── HEXPIRE-family replay intercepts (phase 200) ─────────────────────────
    //
    // BGREWRITEAOF emits `HSET k f v` followed by `HPEXPIREAT k abs_ms FIELDS
    // 1 f` for every TTL'd field. The replay engine intercepts these commands
    // before they reach `command::dispatch` (where phase 196 will add user-
    // facing handlers) and routes them directly to `Database::hash_set_field_ttl`.

    #[test]
    fn replay_hpexpireat_sets_field_ttl() {
        let engine = DispatchReplayEngine::new();
        let mut databases = vec![make_db_with_hash(b"h", &[(b"f1", b"v1")])];
        let mut selected = 0usize;
        let abs_ms = databases[0].now_ms() + 60_000;
        let args = framevec![
            Frame::BulkString(bytes::Bytes::from_static(b"h")),
            Frame::BulkString(bytes::Bytes::copy_from_slice(abs_ms.to_string().as_bytes())),
            Frame::BulkString(bytes::Bytes::from_static(b"FIELDS")),
            Frame::BulkString(bytes::Bytes::from_static(b"1")),
            Frame::BulkString(bytes::Bytes::from_static(b"f1")),
        ];
        engine.replay_command(&mut databases, b"HPEXPIREAT", &args, &mut selected);
        assert_eq!(
            databases[0].hash_get_field_ttl_ms(b"h", b"f1"),
            Some(abs_ms),
            "HPEXPIREAT replay must set the field TTL on the live database"
        );
    }

    #[test]
    fn replay_hpexpireat_handles_multiple_fields() {
        let engine = DispatchReplayEngine::new();
        let mut databases = vec![make_db_with_hash(b"h", &[(b"a", b"1"), (b"b", b"2")])];
        let mut selected = 0usize;
        let abs_ms = databases[0].now_ms() + 30_000;
        let args = framevec![
            Frame::BulkString(bytes::Bytes::from_static(b"h")),
            Frame::BulkString(bytes::Bytes::copy_from_slice(abs_ms.to_string().as_bytes())),
            Frame::BulkString(bytes::Bytes::from_static(b"FIELDS")),
            Frame::BulkString(bytes::Bytes::from_static(b"2")),
            Frame::BulkString(bytes::Bytes::from_static(b"a")),
            Frame::BulkString(bytes::Bytes::from_static(b"b")),
        ];
        engine.replay_command(&mut databases, b"HPEXPIREAT", &args, &mut selected);
        assert_eq!(databases[0].hash_get_field_ttl_ms(b"h", b"a"), Some(abs_ms));
        assert_eq!(databases[0].hash_get_field_ttl_ms(b"h", b"b"), Some(abs_ms));
    }

    #[test]
    fn replay_hpersist_removes_field_ttl() {
        let engine = DispatchReplayEngine::new();
        let mut databases = vec![make_db_with_hash(b"h", &[(b"f", b"v")])];
        let mut selected = 0usize;
        let abs_ms = databases[0].now_ms() + 60_000;
        databases[0]
            .hash_set_field_ttl(b"h", b"f", abs_ms, HashTtlCond::Always)
            .unwrap();

        let args = framevec![
            Frame::BulkString(bytes::Bytes::from_static(b"h")),
            Frame::BulkString(bytes::Bytes::from_static(b"FIELDS")),
            Frame::BulkString(bytes::Bytes::from_static(b"1")),
            Frame::BulkString(bytes::Bytes::from_static(b"f")),
        ];
        engine.replay_command(&mut databases, b"HPERSIST", &args, &mut selected);
        assert_eq!(
            databases[0].hash_get_field_ttl_ms(b"h", b"f"),
            None,
            "HPERSIST replay must drop the field TTL"
        );
    }

    #[test]
    fn replay_hexpire_seconds_form_converts_to_abs_ms() {
        let engine = DispatchReplayEngine::new();
        let mut databases = vec![make_db_with_hash(b"h", &[(b"f", b"v")])];
        let mut selected = 0usize;
        let now = databases[0].now_ms();
        let args = framevec![
            Frame::BulkString(bytes::Bytes::from_static(b"h")),
            Frame::BulkString(bytes::Bytes::from_static(b"60")), // 60s
            Frame::BulkString(bytes::Bytes::from_static(b"FIELDS")),
            Frame::BulkString(bytes::Bytes::from_static(b"1")),
            Frame::BulkString(bytes::Bytes::from_static(b"f")),
        ];
        engine.replay_command(&mut databases, b"HEXPIRE", &args, &mut selected);
        let got = databases[0].hash_get_field_ttl_ms(b"h", b"f").unwrap();
        let drift = got.saturating_sub(now + 60_000);
        assert!(
            drift < 1_000,
            "HEXPIRE replay should land within ~1s of now+60s; drift={}",
            drift
        );
    }

    #[test]
    fn replay_hpexpireat_skips_nx_xx_gt_lt_token() {
        let engine = DispatchReplayEngine::new();
        let mut databases = vec![make_db_with_hash(b"h", &[(b"f", b"v")])];
        let mut selected = 0usize;
        let abs_ms = databases[0].now_ms() + 90_000;
        let args = framevec![
            Frame::BulkString(bytes::Bytes::from_static(b"h")),
            Frame::BulkString(bytes::Bytes::copy_from_slice(abs_ms.to_string().as_bytes())),
            Frame::BulkString(bytes::Bytes::from_static(b"GT")),
            Frame::BulkString(bytes::Bytes::from_static(b"FIELDS")),
            Frame::BulkString(bytes::Bytes::from_static(b"1")),
            Frame::BulkString(bytes::Bytes::from_static(b"f")),
        ];
        engine.replay_command(&mut databases, b"HPEXPIREAT", &args, &mut selected);
        assert_eq!(
            databases[0].hash_get_field_ttl_ms(b"h", b"f"),
            Some(abs_ms),
            "replay must accept the recorded TTL irrespective of NX/XX/GT/LT"
        );
    }
}

/// Trait that abstracts command dispatch for AOF/WAL replay.
///
/// This decouples persistence replay from `command::dispatch`, allowing
/// replay logic to work through a trait object on the cold startup path.
pub trait CommandReplayEngine {
    /// Replay a single parsed command against the database slice.
    ///
    /// `selected_db` may be mutated by SELECT commands during replay.
    /// The response is intentionally discarded -- replay cares only about
    /// side effects on the databases.
    fn replay_command(
        &self,
        databases: &mut [Database],
        cmd: &[u8],
        args: &[Frame],
        selected_db: &mut usize,
    );
}

/// Concrete implementation that delegates to `command::dispatch`.
///
/// This is the **only** place that imports `command::dispatch` for replay
/// purposes, centralizing the dependency. When the `graph` feature is
/// enabled, graph WAL commands (GRAPH.CREATE, GRAPH.ADDNODE, etc.) are
/// collected into a `GraphReplayCollector` instead of being dispatched
/// to the KV command path. After replay, call `replay_graph_commands()`
/// to apply them to a `GraphStore`.
pub struct DispatchReplayEngine {
    #[cfg(feature = "graph")]
    graph_collector: std::cell::RefCell<crate::graph::replay::GraphReplayCollector>,
}

impl DispatchReplayEngine {
    /// Create a new replay engine.
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "graph")]
            graph_collector: std::cell::RefCell::new(
                crate::graph::replay::GraphReplayCollector::new(),
            ),
        }
    }

    /// Replay collected graph commands into a `GraphStore`.
    ///
    /// Must be called after WAL/AOF replay completes. Returns the number
    /// of graph commands successfully replayed.
    #[cfg(feature = "graph")]
    pub fn replay_graph_commands(&self, store: &mut crate::graph::store::GraphStore) -> usize {
        self.graph_collector.borrow().replay_into(store)
    }

    /// Number of graph commands collected during replay.
    #[cfg(feature = "graph")]
    pub fn graph_command_count(&self) -> usize {
        self.graph_collector.borrow().command_count()
    }
}

impl CommandReplayEngine for DispatchReplayEngine {
    fn replay_command(
        &self,
        databases: &mut [Database],
        cmd: &[u8],
        args: &[Frame],
        selected_db: &mut usize,
    ) {
        // Intercept graph commands and route to the collector instead of KV dispatch.
        // Graph WAL records are collected during the first pass, then replayed in
        // correct order (creates -> nodes -> edges -> removes -> drops) via
        // replay_graph_commands() after all KV replay completes.
        #[cfg(feature = "graph")]
        {
            if crate::graph::replay::GraphReplayCollector::is_graph_command(cmd) {
                // Check if any args are Integer frames (node/edge IDs may be
                // encoded as RESP integers). If so, convert to string bytes
                // for GraphReplayCollector which expects all-text args.
                let has_integers = args.iter().any(|f| matches!(f, Frame::Integer(_)));
                let collected = if has_integers {
                    let owned: smallvec::SmallVec<[Vec<u8>; 8]> = args
                        .iter()
                        .filter_map(|f| match f {
                            Frame::BulkString(b) => Some(b.to_vec()),
                            Frame::Integer(i) => Some(i.to_string().into_bytes()),
                            _ => None,
                        })
                        .collect();
                    let refs: smallvec::SmallVec<[&[u8]; 8]> =
                        owned.iter().map(|v| v.as_slice()).collect();
                    self.graph_collector
                        .borrow_mut()
                        .collect_command(cmd, &refs)
                } else {
                    let bulk_args: smallvec::SmallVec<[&[u8]; 8]> = args
                        .iter()
                        .filter_map(|f| match f {
                            Frame::BulkString(b) => Some(b.as_ref()),
                            _ => None,
                        })
                        .collect();
                    self.graph_collector
                        .borrow_mut()
                        .collect_command(cmd, &bulk_args)
                };
                if !collected {
                    tracing::warn!(
                        "WAL replay: malformed graph command {:?} with {} args — skipping",
                        std::str::from_utf8(cmd).unwrap_or("<non-utf8>"),
                        args.len()
                    );
                }
                return;
            }
        }

        // SWAPDB requires access to the full database slice — intercept before
        // calling `command::dispatch` which only sees a single `&mut Database`.
        if cmd.eq_ignore_ascii_case(b"SWAPDB") {
            let a = args.first().and_then(|f| match f {
                Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<usize>().ok(),
                Frame::Integer(n) => usize::try_from(*n).ok(),
                _ => None,
            });
            let b_idx = args.get(1).and_then(|f| match f {
                Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<usize>().ok(),
                Frame::Integer(n) => usize::try_from(*n).ok(),
                _ => None,
            });
            match (a, b_idx) {
                (Some(a), Some(b)) if a != b && a < databases.len() && b < databases.len() => {
                    let (lo, hi) = if a < b { (a, b) } else { (b, a) };
                    // Split the slice to get two non-overlapping mutable references.
                    let (left, right) = databases.split_at_mut(lo + 1);
                    std::mem::swap(&mut left[lo], &mut right[hi - lo - 1]);
                }
                _ => {
                    // Out-of-range or same-index — silently skip (same as Redis).
                }
            }
            return;
        }

        let db_count = databases.len();
        if *selected_db >= db_count {
            tracing::warn!(
                "WAL replay: selected_db {} out of range (have {} databases), resetting to 0",
                *selected_db,
                db_count
            );
            *selected_db = 0;
        }

        // Phase 200 — HEXPIRE-family replay shims.
        //
        // The user-facing command handlers for HEXPIRE / HPEXPIRE / HEXPIREAT
        // / HPEXPIREAT / HPERSIST land in phase 196 (issue #107). Until then,
        // replay has to honor commands that BGREWRITEAOF already emits for
        // every TTL'd field (`feat(persistence): RDB v2 — per-field TTL
        // trailer for hashes`, commit 9713b63). We route those commands
        // directly to the storage primitive `Database::hash_set_field_ttl` /
        // `hash_persist_field` so a SIGKILL between an HEXPIRE call and the
        // next snapshot still recovers the TTL sidecar.
        if is_hexpire_family_write(cmd) {
            let db = &mut databases[*selected_db];
            if cmd.eq_ignore_ascii_case(b"HPERSIST") {
                let _ = replay_hpersist(db, args);
            } else {
                let _ = replay_hexpire_family(db, cmd, args);
            }
            return;
        }

        let _ = crate::command::dispatch(
            &mut databases[*selected_db],
            cmd,
            args,
            selected_db,
            db_count,
        );
    }
}
