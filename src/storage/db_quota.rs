//! Per-logical-db memory quotas (WS5b, v0.6.0).
//!
//! `--maxmemory` / `CONFIG SET maxmemory` bound the WHOLE instance. This
//! module adds an orthogonal, optional, finer-grained bound: `db-maxmemory`
//! caps a single `SELECT`-able db slot (independent of workspaces, which are
//! a key-prefix layer within a db — see `docs/guides/isolation.md`).
//!
//! # Design
//!
//! - **Config surface**: `--db-maxmemory <db>:<bytes>` (repeatable CLI flag)
//!   and `CONFIG SET db-maxmemory <db> <bytes>` / `CONFIG GET db-maxmemory`.
//!   Chosen over a single `CONFIG SET db-maxmemory <db>:<bytes>` string
//!   because Redis-style `CONFIG SET` is strictly `name value` pairs; a
//!   two-token value (`<db> <bytes>`) fits that shape more naturally than
//!   embedding a delimiter inside one value token, and mirrors how
//!   `CLIENT PAUSE <ms> [WRITE]` already accepts positional sub-args in this
//!   codebase's CONFIG dispatch style.
//! - **Enforcement mirrors global maxmemory exactly**: same eviction policy
//!   (`config.maxmemory_policy`), same per-shard division
//!   (`limit.div_ceil(num_shards)`, since each shard's `Database` for db `i`
//!   is independent — shared-nothing, see `RuntimeConfig::maxmemory_per_shard`),
//!   `noeviction` rejects the write, an evicting policy sheds THIS db's own
//!   keys first (never a neighbor db's).
//! - **Zero-cost when unset**: [`RuntimeConfig::db_maxmemory_per_shard`] is a
//!   `Vec::get` + one branch — no lock beyond whatever the caller already
//!   holds. The ultra-hot inline write path (`server/conn/blocking.rs`) additionally
//!   gates on the process-global [`db_maxmemory_any_set`] atomic so the
//!   common (unconfigured) case never even indexes the `Vec` under lock.
//! - **Known limitation — no disk-offload spill integration**: unlike the
//!   global maxmemory gate's spill-aware variants
//!   (`eviction::try_evict_if_needed_async_spill_budget`), db-quota eviction
//!   always deletes the victim outright. A db-quota'd instance running
//!   `--disk-offload` still spills for the *global* gate, but a purely
//!   db-quota-triggered eviction is NOT spilled to the cold tier. This keeps
//!   the enforcement surface small and is safe (data loss is the documented
//!   contract of a policy other than `noeviction`) but is a real behavioral
//!   difference worth knowing about; see `docs/guides/isolation.md`.

use bytes::Bytes;

use crate::config::RuntimeConfig;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::eviction::{self, EvictionPolicy};

/// Process-global "is ANY `db-maxmemory` entry configured?" flag.
///
/// Mirrors `eviction::MAXMEMORY_GLOBAL`'s role: a single lock-free Relaxed
/// atomic so the ultra-hot inline write path (`blocking.rs`) can skip taking
/// the `RuntimeConfig` read lock entirely in the overwhelmingly common case
/// (feature never configured). MUST be republished at every write site of
/// `RuntimeConfig.db_maxmemory`: server startup and `CONFIG SET db-maxmemory`.
static DB_MAXMEMORY_ANY_SET: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Publish whether any per-db quota is currently configured. Call at server
/// startup (after `RuntimeConfig::db_maxmemory` is populated from
/// `--db-maxmemory`) and on every `CONFIG SET db-maxmemory`.
#[inline]
pub fn publish_db_maxmemory_any_set(config: &RuntimeConfig) {
    let any = config.db_maxmemory.iter().any(|&limit| limit > 0);
    DB_MAXMEMORY_ANY_SET.store(any, std::sync::atomic::Ordering::Relaxed);
}

/// `true` iff at least one db currently has a nonzero quota. Lock-free.
#[inline]
#[must_use]
pub fn db_maxmemory_any_set() -> bool {
    DB_MAXMEMORY_ANY_SET.load(std::sync::atomic::Ordering::Relaxed)
}

/// OOM error frame for a db-quota breach — distinct wording from the global
/// `OOM command not allowed when used memory > 'maxmemory'` so operators can
/// tell the two gates apart in logs/client errors.
fn db_quota_error(db_index: usize, limit: u64) -> Frame {
    Frame::Error(Bytes::from(format!(
        "MOONERR db maxmemory exceeded: db {db_index} is at its {limit}-byte \
         quota (CONFIG SET db-maxmemory {db_index} <bytes> to raise it, or 0 to remove it)"
    )))
}

/// Commands that reach the write-path eviction gate (`metadata::is_write`
/// flags them `W`/`WF` for ACL/dispatch reasons) but do NOT write to, or
/// grow, `db_index`'s OWN keyspace, so must never be blocked by that db's
/// quota:
///
/// - `SELECT`: found during WS5b integration testing — `is_write(b"SELECT")`
///   is true (it's WF-flagged for ACL purposes), and the write-path gate
///   runs against `conn.selected_db` BEFORE the command dispatches (i.e.
///   before SELECT actually switches db). A quota'd, `noeviction` db at
///   capacity therefore rejected `SELECT` itself — the client could not even
///   switch away from the full db on that connection. Since SELECT doesn't
///   grow the current db's memory, it must never consult that db's quota.
/// - `SWAPDB`: reassigns which db INDEX points at which data, but the total
///   bytes on this shard are unchanged — the on-write gate would fire (or
///   not) based on the state BEFORE the swap, which is meaningless for the
///   AFTER state. The periodic background sweep (`shard::timers::run_eviction`)
///   catches a db that ends up over quota after a swap within one tick
///   instead (documented in docs/guides/isolation.md's SWAPDB caveat).
///
/// This list intentionally mirrors real Redis's `CMD_DENYOOM` flag semantics
/// (the global maxmemory OOM check only gates commands that actually grow
/// memory) more closely than moon's existing `metadata::is_write`-gated
/// global maxmemory check does — the global check inherits the same
/// SELECT-blocking quirk pre-existing this feature; fixing that is out of
/// scope here (touches the shared write-path gate for ALL callers, not just
/// db-quota) and is noted as a known finding, not fixed, in this PR.
fn command_exempt_from_db_quota(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"SELECT") || cmd.eq_ignore_ascii_case(b"SWAPDB")
}

/// Commands that are structurally incapable of GROWING the memory footprint
/// of the key(s) they touch — they only remove data, shrink a container, or
/// hasten reclamation. Mirrors real Redis's `CMD_DENYOOM` semantics: neither
/// the global `--maxmemory` `noeviction` gate nor the per-db `--db-maxmemory`
/// quota gate may reject these, or a key/db that crosses its `noeviction`
/// boundary has no self-recovery path — a tenant wedges permanently, unable
/// to even `HDEL` its way back under budget (WS6 adversarial-review finding,
/// HIGH, 2026-07-08; the finding was previously unreachable because container
/// growth itself was invisible to `used_memory` before that same WS6 fix, so
/// this is newly OBSERVABLE, not newly introduced).
///
/// This is a deliberately conservative, provably-shrink-only allowlist —
/// "when in doubt, exclude":
///
/// - `DEL` / `UNLINK`: whole-key removal.
/// - `HDEL` / `HGETDEL`: hash field removal.
/// - `SREM` / `SPOP`: set member removal.
/// - `LPOP` / `RPOP` / `LREM` / `LTRIM` / `LMPOP` / `BLMPOP`: list shrink —
///   a blocking pop only ever removes on the success path that reaches this
///   gate at all.
/// - `ZREM` / `ZPOPMIN` / `ZPOPMAX` / `ZMPOP` / `BZPOPMIN` / `BZPOPMAX` /
///   `BZMPOP` / `ZREMRANGEBYSCORE` / `ZREMRANGEBYRANK` / `ZREMRANGEBYLEX`:
///   sorted-set shrink. (The three `ZREMRANGEBY*` entries are forward
///   declarations — moon does not dispatch them yet; they are inert here
///   until the commands exist, at which point they are already exempted.)
/// - `GETDEL`: string delete-on-read.
/// - `EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT` / `PERSIST`: can only
///   hasten or cancel reclamation of an already-live key; a TTL sidecar's
///   fixed-size cost is already charged, a shorter (or removed) TTL never
///   adds bytes.
/// - `FLUSHDB` / `FLUSHALL`: keyspace wipe.
///
/// Deliberately EXCLUDED (can grow a destination key, or is not statically
/// classifiable as shrink-only):
///
/// - `LMOVE` / `SMOVE` / `COPY` / `RESTORE` / any `*STORE` variant
///   (`ZUNIONSTORE`, `ZINTERSTORE`, `ZRANGESTORE`, `SINTERSTORE`,
///   `SUNIONSTORE`, `SDIFFSTORE`, `GEORADIUS...STORE`, ...): these write a
///   (possibly new, possibly larger) destination key.
/// - `SET`, even with a value shorter than the key's current one: not
///   statically classifiable (KEEPTTL/EX/PX interactions, and the general
///   overwrite path is the primary growth vector this whole gate exists to
///   catch in the first place).
#[inline]
#[must_use]
pub fn is_shrink_only_command(cmd: &[u8]) -> bool {
    const SHRINK_ONLY: &[&[u8]] = &[
        b"DEL",
        b"UNLINK",
        b"HDEL",
        b"HGETDEL",
        b"SREM",
        b"SPOP",
        b"LPOP",
        b"RPOP",
        b"LREM",
        b"LTRIM",
        b"LMPOP",
        b"BLMPOP",
        b"ZREM",
        b"ZPOPMIN",
        b"ZPOPMAX",
        b"ZMPOP",
        b"BZPOPMIN",
        b"BZPOPMAX",
        b"BZMPOP",
        b"ZREMRANGEBYSCORE",
        b"ZREMRANGEBYRANK",
        b"ZREMRANGEBYLEX",
        b"GETDEL",
        b"EXPIRE",
        b"PEXPIRE",
        b"EXPIREAT",
        b"PEXPIREAT",
        b"PERSIST",
        b"FLUSHDB",
        b"FLUSHALL",
    ];
    SHRINK_ONLY.iter().any(|c| cmd.eq_ignore_ascii_case(c))
}

/// Like [`check_db_maxmemory`] but skips enforcement for commands exempted
/// by [`command_exempt_from_db_quota`] (`SELECT`, `SWAPDB`). Use this at any
/// write-path chokepoint that gates on `metadata::is_write` and therefore
/// sees non-mutating-for-this-db commands like `SELECT` — i.e. the
/// connection-handler write paths (`handler_monoio`, `handler_sharded`,
/// `handler_single`). Chokepoints that only ever see genuine data-mutating
/// commands (cross-shard SPSC execution, the Lua bridge, the periodic
/// background sweep) can call [`check_db_maxmemory`] directly.
pub fn check_db_maxmemory_for_command(
    db: &mut Database,
    db_index: usize,
    config: &RuntimeConfig,
    cmd: &[u8],
) -> Result<(), Frame> {
    if command_exempt_from_db_quota(cmd) {
        return Ok(());
    }
    check_db_maxmemory(db, db_index, config)
}

/// Check (and, for an evicting policy, enforce) db `db_index`'s memory quota.
///
/// Fast path: `config.db_maxmemory_per_shard(db_index) == 0` (no quota
/// configured for this db) returns `Ok(())` immediately — a `Vec::get` plus
/// one branch, no allocation, no additional lock beyond the `RuntimeConfig`
/// read guard the caller already holds for the global gate.
///
/// Call this from the SAME write-path chokepoints as the global maxmemory
/// gate (`eviction::try_evict_if_needed_budget` and friends), after the
/// global gate — a write that survives the whole-instance cap must still
/// clear its own db's finer-grained cap. Prefer
/// [`check_db_maxmemory_for_command`] at chokepoints that can see `SELECT`.
pub fn check_db_maxmemory(
    db: &mut Database,
    db_index: usize,
    config: &RuntimeConfig,
) -> Result<(), Frame> {
    let budget = config.db_maxmemory_per_shard(db_index);
    if budget == 0 {
        return Ok(());
    }
    let policy = EvictionPolicy::from_str(&config.maxmemory_policy);
    let mut current = db.estimated_memory() as u64;
    while current > budget {
        if policy == EvictionPolicy::NoEviction {
            return Err(db_quota_error(db_index, budget));
        }
        let before = db.estimated_memory();
        // No disk-offload spill integration for db-quota eviction — see the
        // module doc's "Known limitation" note.
        //
        // task #34 (Wave A): db-quota plain-drops are NOT wired into
        // `record_reason_del` yet (a no-op sink here matches pre-#34
        // behavior exactly) — `--db-maxmemory` is a separate, narrower
        // knob from the whole-instance `--maxmemory` gates Wave A covers;
        // tracked as a follow-up alongside the other documented Wave-A gaps
        // (hash-field TTL reaps, Lua script effects).
        if !eviction::evict_one_with_spill(db, config, &policy, None, &mut |_| {}) {
            return Err(db_quota_error(db_index, budget));
        }
        let after = db.estimated_memory();
        current = current.saturating_sub(before.saturating_sub(after) as u64);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::db::Database;

    fn fill(db: &mut Database, prefix: &str, count: usize) {
        for i in 0..count {
            db.set_string(
                Bytes::from(format!("{prefix}{i}")),
                Bytes::from(vec![0u8; 64]),
            );
        }
    }

    fn rt_with_quota(db_maxmemory: Vec<u64>, policy: &str) -> RuntimeConfig {
        let mut rt = RuntimeConfig::default();
        rt.db_maxmemory = db_maxmemory;
        rt.maxmemory_policy = policy.to_string();
        rt.num_shards = 1;
        rt
    }

    #[test]
    fn unconfigured_db_is_zero_cost_noop() {
        let rt = RuntimeConfig::default();
        assert!(rt.db_maxmemory.is_empty());
        let mut db = Database::new();
        // A db index far beyond any configured quota must be a pure Ok(()),
        // never panic on out-of-bounds indexing.
        assert!(check_db_maxmemory(&mut db, 5, &rt).is_ok());
    }

    #[test]
    fn out_of_range_db_index_is_unlimited() {
        let rt = rt_with_quota(vec![100], "noeviction");
        let mut db = Database::new();
        // Only db 0 has a quota; db 1 is out of range for the Vec -> unlimited.
        assert!(check_db_maxmemory(&mut db, 1, &rt).is_ok());
    }

    #[test]
    fn zero_entry_means_unlimited_for_that_db() {
        let rt = rt_with_quota(vec![0, 500], "noeviction");
        let mut db = Database::new();
        fill(&mut db, "k", 2000);
        // db 0's entry is 0 (unlimited) even though db 1 has a real quota.
        assert!(check_db_maxmemory(&mut db, 0, &rt).is_ok());
    }

    #[test]
    fn noeviction_rejects_write_over_quota() {
        let rt = rt_with_quota(vec![1024], "noeviction");
        let mut db = Database::new();
        fill(&mut db, "key", 500);
        assert!(db.estimated_memory() as u64 > 1024);
        let err = check_db_maxmemory(&mut db, 0, &rt).unwrap_err();
        match err {
            Frame::Error(msg) => {
                let s = String::from_utf8_lossy(&msg);
                assert!(s.contains("MOONERR db maxmemory exceeded"));
                assert!(s.contains("db 0"));
            }
            other => panic!("expected Frame::Error, got {other:?}"),
        }
    }

    #[test]
    fn evicting_policy_sheds_keys_until_under_quota() {
        // Budget chosen to require substantial-but-partial eviction (roughly
        // half the keys), staying well clear of the existing random-sampling
        // evictor's low-candidate-density tail (`find_victim_random` takes a
        // single sample per attempt with a bounded 8-attempt retry budget —
        // pre-existing behavior in `eviction::sample_random_keys`, shared by
        // the global maxmemory gate, not something this quota gate changes).
        let rt = rt_with_quota(vec![50_000], "allkeys-random");
        let mut db = Database::new();
        fill(&mut db, "key", 500);
        let before_count = db.data().len();
        assert!(db.estimated_memory() as u64 > 50_000);
        let result = check_db_maxmemory(&mut db, 0, &rt);
        assert!(result.is_ok(), "evicting policy must not OOM-reject");
        assert!(db.estimated_memory() as u64 <= 50_000);
        assert!(
            db.data().len() < before_count,
            "eviction must have removed at least one key"
        );
    }

    #[test]
    fn neighbor_db_is_unaffected_by_sibling_quota() {
        // db 0 has a tiny quota; db 1 has none. Only db 0's Database is ever
        // touched by check_db_maxmemory(_, 0, _) — this test documents that
        // invariant structurally: the function takes a single &mut Database,
        // so it is mechanically impossible for it to reach into a sibling
        // db's storage. Callers pass `&mut s.databases[db_index]` per db.
        let rt = rt_with_quota(vec![256, 0], "allkeys-random");
        let mut db0 = Database::new();
        let mut db1 = Database::new();
        fill(&mut db0, "d0k", 200);
        fill(&mut db1, "d1k", 200);
        let db1_count_before = db1.data().len();
        let _ = check_db_maxmemory(&mut db0, 0, &rt);
        // db1 was never passed to check_db_maxmemory for its own quota check
        // in this test — its key count must be untouched.
        assert_eq!(db1.data().len(), db1_count_before);
    }

    #[test]
    fn publish_and_read_any_set_flag() {
        publish_db_maxmemory_any_set(&RuntimeConfig::default());
        assert!(!db_maxmemory_any_set());

        let rt = rt_with_quota(vec![0, 0, 1024], "noeviction");
        publish_db_maxmemory_any_set(&rt);
        assert!(db_maxmemory_any_set());

        // Reset for other tests running in the same process (atomics are
        // process-global — tests in this module run serially enough that a
        // final reset avoids leaking state, though `cargo test` may
        // interleave with other modules touching the same atomic; this
        // module only asserts its own writes/reads, not global ordering).
        publish_db_maxmemory_any_set(&RuntimeConfig::default());
    }
}
