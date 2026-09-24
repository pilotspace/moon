# WS7-conn-hotpath — PLAN (wave 2)
personas: `.add/personas/performance-engineer.md` (lead) · `.add/personas/acl-security-gatekeeper.md` (every ACL/auth change is HARD-STOP-reviewed: fail CLOSED)

Review measurements: ACL SETUSER mid-run → existing connections' GET −48.6% (redis −1.8%); one idle CLIENT TRACKING client → SET −41% (redis ≈ −9%).

## Issues
1. **moon#1175** global CLIENT PAUSE write lock + RuntimeConfig read lock per batch.
   - Must: gate `expire_if_needed` + `check_pause` on `pause_possibly_active()` in both handlers (keep the documented memory-ordering argument; add a test that pause still delays/blocks when active and expires); query-buffer limits published lock-free (atomics updated by CONFIG SET, or per-connection read) — never a global lock per read iteration; drop/gate the tokio per-command `client_pause_deadline_ms` read.
2. **moon#1165** stale ACL cache never refreshed after any ACL mutation (+ related).
   - Must: refresh at batch top when `!acl_cache_fresh()` in all three handlers (monoio, tokio sharded, single); test: after `ACL SETUSER probe …` an EXISTING unrestricted connection is back on the inline path (`moon_dispatch_path_total{path="local_inline"}` or an equivalent counter) and a restricted user's revocation still applies immediately (fail closed).
   - Must: `AclTable` lock → `parking_lot::RwLock` (CLAUDE.md), all `.read().unwrap()` sites updated.
   - Must: PUBLISH/SPUBLISH/script ACL re-checks skipped when `conn.acl_skip_allowed()`; `ScriptAcl` built without the table lock for unrestricted users.
   - Should: restricted users — no per-command `to_ascii_lowercase` String, one username lookup, first-arg rule probes only for commands that can have first-arg rules; a per-connection compiled snapshot (command bitset + classified key patterns) rebuilt on version change if it fits.
   - OUT OF SCOPE (do not change): RESET / `try_handle_reset` identity semantics — tracked privately. Do not add a `current_user` setter that changes RESET behaviour in this branch.
3. **moon#1176** per-write global ReplicationState read lock + process-wide fetch_adds.
   - Must: connection write paths use a lock-free `OffsetHandle` in the connection context; `shard_offsets` cache-padded; replica fan-out probe lock-free (no shard-0 backlog mutex per write); pre-serialized `SELECT 0..15`; backlog append via slice copy. `master_repl_offset` per-write fetch_add removed ONLY if INFO/ROLE/WAIT/PSYNC semantics are provably unchanged (sum on read) — otherwise keep it and say why.
   - Tests: replication offset / WAIT / PSYNC suites by name stay green; AOF LSN monotonicity per shard.
4. **moon#1178** Prometheus: per-GET `counter!` registry lookup; one-entry handle cache; per-drain String alloc; unsharded SPSC counters.
   - Must: counters that have per-thread slots are published from the slots at scrape (`.absolute(sum)`), not per event; per-command metrics via a per-thread array indexed by command id flushed by the chore (or handles indexed by id); drain histogram handle cached per thread; SPSC counters moved to per-thread padded slots. `/metrics` output names/labels unchanged (diff a scrape before/after).
5. **moon#1166** CLIENT TRACKING: global mutex on every write + inline SET disabled server-wide.
   - Must: striped tracking table (by key hash) with BCAST prefixes behind a lock-free count gate; inline SET stays enabled under tracking and invalidates the key itself; untracked keys never take a lock (pre-filter). All tracking integration suites by name green (incl. REDIRECT, BCAST, OPTIN/OPTOUT, NOLOOP, RESP2 redirect).
6. **moon#1198 (conn items)**: cluster-mode lock-free served-slot bitmap for routing (+ inline path when bit set and !asking); non-inline local GET cold-peek under the shared guard; inline SET pre-gate without a second exclusive acquisition; resolve `COMMAND_META` once per command. Commits `refs moon#1198`.

## Owned files
`src/server/conn/**` EXCEPT the remote-dispatch / `remote_groups` / coordinator-call regions (WS8), `src/client_pause.rs`, `src/acl/**`, `src/tracking/**`, `src/replication/state.rs`, `src/replication/backlog.rs`, `src/persistence/aof/pool.rs` (`issue_append_lsn` only), `src/admin/metrics_setup/**`, `src/admin/http_server.rs` (scrape hook), `src/cluster/**` (slot bitmap), `src/config.rs` / `src/main.rs` (context wiring only), tests `tests/perf_ws7_*.rs`.

## Not yours
`src/shard/**` (WS8), `src/scripting/**`, `src/pubsub/**` (WS9), command write sites / `src/storage/**` (WS10).
