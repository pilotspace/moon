# WS9-scripting-pubsub — PLAN (wave 2)
personas: `.add/personas/acl-security-gatekeeper.md` (lead: the Lua sandbox is a security boundary — caching compiled functions must not leak globals/state between scripts or users) · `.add/personas/performance-engineer.md`

Review measurement (--shards 1, -P 16 -c 50): GET 1.47M rps (1.59× redis) but EVALSHA 1-line script 118.7K (0.29× redis 408.6K), 1.4 KB rate-limiter 19.5K (0.15× redis 128.6K).

## Issues
1. **moon#1167** EVAL/EVALSHA recompile Lua per call.
   - Must: per-shard compiled-function cache keyed by SHA (`lua.load(src).set_name("@user_script").into_function()` once), used by EVAL, EVALSHA and EVAL_RO/EVALSHA_RO; `SCRIPT FLUSH` clears it; bounded (LRU cap, configurable or const) so unique-body EVAL storms cannot grow the Lua heap without bound; the sandbox semantics (globals protection, KEYS/ARGV per call, error text `@user_script`, redis.call ACL enforcement) byte-identical — run the scripting suites by name + a test that a script cannot observe another script's locals/globals through the cache.
   - Must: EVALSHA lowercases into a stack `[u8; 40]`, no synthetic `eval_args` Vec; EVAL computes SHA1 once (thread it through `claim_fanout_duty`); `parse_eval_args` runs once per EVAL at --shards > 1 if the call sites allow (cross-ownership note if it touches `server/conn/shared.rs`).
   - Should: `ScriptCache::resident_bytes` O(1) (running counter) instead of a per-100ms walk.
   - Evidence: release-fast A/B of the review's EVALSHA table vs the baseline binary and redis.
2. **moon#1180** PUBLISH clones every subscriber handle per message.
   - Must: subscribers per channel/pattern stored as `Arc<[Subscriber]>` rebuilt copy-on-write on (un)subscribe / remove_slow; publish snapshot = one Arc clone per channel + per matching pattern; delivery order and slow-subscriber eviction semantics unchanged (pubsub suites by name, incl. RESP3 push, sharded pubsub, keyspace notifications).
   - Evidence: criterion bench or release-fast timing of `publish_shared` at 1/100/1K/10K subscribers before/after.

3. **moon#1214 item 2** keyspace notifications allocate even when nobody subscribes to `__keyspace@*`/`__keyevent@*`: a lock-free global keyspace-listener count maintained by (P)SUBSCRIBE/(P)UNSUBSCRIBE/disconnect gates the event construction; notification delivery unchanged when a listener exists (pubsub/notify suites by name; a test that a late PSUBSCRIBE still receives events).

## Owned files
`src/scripting/**`, `src/pubsub/**`, `src/notify*.rs` (only if the pubsub API change requires it), tests `tests/perf_ws9_*.rs`, benches for publish if added.
Cross-ownership (own commit + note): `parse_eval_args` call sites in `src/server/conn/shared.rs` / handler dispatch.

## Not yours
`src/acl/**` (WS7 — `acl/script.rs` ScriptAcl lock change is WS7's), connection handler gates (WS7), `src/shard/**` (WS8).
