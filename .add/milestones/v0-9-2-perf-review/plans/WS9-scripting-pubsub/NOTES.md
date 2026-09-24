# WS9-scripting-pubsub — working notes (refs moon#1199)

Base `f32546c` (wave-1 integration). Branch `perf/ws9-scripting-pubsub`.
`export CARGO_TARGET_DIR=/home/user/wt/target CARGO_INCREMENTAL=0`. Ports 7280–7299.

## moon#1167 — EVAL/EVALSHA recompile per call

Mechanism on HEAD: `run_script` did `lua.load(script).set_name("@user_script").eval()`
on EVERY EVAL/EVALSHA — the Lua lexer/parser/codegen run per call. EVALSHA also
allocated a `String` (lowercase sha) and a synthetic `eval_args` `Vec` (clone of
every arg) per call.

Fix:
- Per-shard compiled-function cache in `ScriptCache` (`compiled: CompiledFnCache`),
  keyed by the 40-byte lowercase hex sha (`[u8; 40]`), storing `mlua::Function`.
  Bounded LRU (cap `COMPILED_CACHE_CAP = 1024`) so unique-body EVAL storms cannot
  grow the Lua heap without bound. The SOURCE map stays unbounded (Redis parity:
  SCRIPT FLUSH is the only source eviction, SCRIPT EXISTS must keep reporting).
- `compile_user_script` replicates `Chunk::eval`'s mode choice EXACTLY
  (expression-first: `"return "+src`, else statement) so a cached function is
  byte-identical to what `.eval()` produced — including the `1+1`→2 leniency HEAD
  has vs redis, which we deliberately preserve (behaviour-frozen; not ours to fix).
- EVALSHA lowercases into a stack `[u8; 40]`, looks the source up by
  `str::from_utf8(&buf)` (zero-copy, no String), parses numkeys/keys/argv straight
  from `args[1..]` (no synthetic `eval_args` Vec).
- EVAL computes the sha ONCE (in `ensure_compiled_eval`) for both source-store and
  compiled lookup.
- `ScriptCache::resident_bytes` is now O(1) (running `source_bytes` counter),
  replacing the per-100ms walk over every cached body.
- SCRIPT FLUSH (incl. ASYNC/SYNC, ignored as on HEAD) clears both maps.

Deferred (cross-ownership into `src/server/conn/shared.rs`, WS7 territory): at
`--shards > 1` the fan-out path (`eval_script_fanout` → `claim_fanout_duty`) still
computes the sha a second time and `parse_eval_args` still runs 3×. Unifying needs
threading a precomputed sha/parse through `route_script_elsewhere` +
`eval_script_fanout` + the dispatch sites — a large shared.rs change that would
collide with WS7. The measured win (EVALSHA/EVAL at `--shards 1`) does not touch
that path.

## moon#1180 — PUBLISH clones every subscriber handle per message

HEAD: `publish_shared`/`spublish_shared` snapshot `subs.iter().cloned().collect()`
into a SmallVec — a flume `Sender` clone (2 atomic RMW) + drop (2 more) per
subscriber per message.

Fix: store each channel/pattern/shard-channel's subscribers as
`Arc<[Subscriber]>`, rebuilt copy-on-write on subscribe/unsubscribe/remove_slow
(all already O(N) under the write lock, all rare). Publish snapshot = one Arc
clone per channel + one per matching pattern, regardless of subscriber count.
Delivery order, slow-drop eviction, RESP3 push, sharded pubsub, keyspace
notifications all unchanged.

## moon#1214 item 2 — keyspace notifications allocate with no `__key*` subscriber

HEAD: every mutating command with `notify-keyspace-events` on paid a key copy +
allocations even when nobody subscribed to a `__keyspace@*`/`__keyevent@*` channel
or pattern. Fix: a lock-free process-global keyspace-listener count
(`KEYSPACE_LISTENERS: AtomicUsize`), bumped by (P)SUBSCRIBE on a `__key*`
channel/pattern and decremented on (P)UNSUBSCRIBE / disconnect. `notify_keyspace_event`
gates event construction on `flags.is_enabled() && listeners>0`. A late PSUBSCRIBE
that raises the count from 0 still receives subsequent events. Counts never go
negative (saturating).
