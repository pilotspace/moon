# WS3-lists-listpack — working notes

## Orientation (read before building)
- Dispatch paths: LRANGE/LINDEX/LPOS/HKEYS/HVALS/HEXISTS/HSTRLEN are served by
  `command::dispatch` (mutable handler delegates to the `_readonly` twin, or duplicates it
  for the hash group) and `command::dispatch_read` (the `_readonly` twin). None is wired
  into `try_inline_dispatch`. LREM/LTRIM/LINSERT/LMOVE/RPOPLPUSH/LMPOP are `dispatch` only.
  The shared implementations (`ListRef`/`HashRef` helpers and the `_readonly` twins) are
  what change, so every path sees the same code.
- Blocking family: BLMOVE/BRPOPLPUSH/BLMPOP immediate path rewrites to LMOVE/LMPOP, so it
  inherits the new arms. The WAKE path (`server/conn/blocking.rs`) still uses
  `Database::list_pop_*`/`list_push_*` (accessors.rs, WS1-owned) which flatten. Follow-up.
- Wakeups are keyed on the command's written key positions (`for_each_written_key`),
  not on the encoding, so LMOVE's destination wake is unaffected.

## Oracle
- `redis-server` 7.0.15 reports `quicklist` for EVERY list (`RPUSH l a b c; OBJECT
  ENCODING l` -> quicklist). There is no list listpack encoding in 7.0, so there is no
  shrink-back to match. moon's `listpack`/`linkedlist` names follow 7.2+ (moon#897 tests
  cite 8.6.1). Decision: implement encoding PRESERVATION (what 7.2+ does for these
  commands), NOT shrink-back (plan: "match it, don't exceed it").
- Baseline reply gaps found by `scratchpad/ws3/parity.py` (redis 7141 vs baseline 7142):
  1. `LPOS l a FOO bar` -> redis `ERR syntax error`, moon `ERR value is not an integer`
     (moon parsed the value before the option name).
  2. `LPOS l a RANK 0` message text differs ("negative to start from the end" in redis).
  3. `LPOS l a COUNT abc` / `MAXLEN abc` -> redis `ERR COUNT/MAXLEN can't be negative`.
  4. `LMOVE k k LEFT RIGHT` on a one-element list with a TTL -> redis keeps TTL 100, moon
     returns -1 (pop deleted the key, push recreated it).
  All four are in code this workstream rewrites; fixed toward redis in the same commits.
- `LPOS ... RANK -9223372036854775808`: redis 7.0.15 negates LONG_MIN (UB) and answers 0;
  7.2+ refuses it. moon keeps HEAD's release behaviour (nil) but no longer overflows in
  debug builds (`unsigned_abs`).

## Design decisions
- New listpack primitives live in `src/storage/listpack/list_ops.rs` (child module of
  `listpack.rs`, which is already past the 1500-line ceiling).
- LREM (deque): swap-compaction with read/write cursors, early stop at `max_remove`, one
  `drain` of the gap. Forward for count >= 0, mirrored for count < 0.
- LREM (listpack): byte-level compaction, forward; backward walk via backlens for count < 0,
  then one `drain` of the gap. No re-encode of kept entries.
- LTRIM (listpack): two nearer-end seeks, one `copy_within`, one truncate.
- LINSERT (listpack): one borrowed walk to the pivot, one `write_entry`. Pivot missing ->
  -1 with no promotion (redis 7.2 converts before searching; moon's policy is a 128/64 B
  limit, not redis's 8 KB budget, so exact encoding parity for oversized elements is not
  reachable either way — not promoting on a no-op is the conservative choice).
- LMOVE: `list_route` gates (1 probe each), listpack pop/push arms mirroring LPOP/LPUSH;
  `src == dst` rotates in place (fixes the TTL loss).
- Capacity: bulk removals (LTRIM/LREM) shrink the buffer only when capacity > 2x len
  (no realloc thrash for the steady-state LPUSH+LTRIM idiom).

## Found during build (see commit 974a13f)
- Backlen byte order: `encode_backlen_into` writes `[low|0x80, high]` (goldens pin it),
  `decode_backlen` reads `[high, low|0x80]` (redis order). Backward walks are wrong for
  entries >= 128 B; forward walks never read backlen bytes. Today's 64 B policy keeps it
  latent. All WS3 walks are forward-only; the encoder flip is an owner decision (it also
  fixes `Listpack::iter_rev` and DUMP/RDB interop with real redis for wide entries).
- Remaining capped-list memory gap vs redis (4309 vs 2739 B/key) is the listpack `Vec`
  doubling growth; redis reallocs to the exact size. Separate follow-up.

## Method learnings
- Shared CARGO_TARGET_DIR: every worktree's lib-test binary is `deps/moon-<same hash>`
  and cargo judges freshness from whichever worktree built last, so `cargo test` can run
  ANOTHER worktree's binary (0 tests matched my filter, twice). Workaround used: set a
  future mtime on `src/lib.rs` before each build, snapshot the executable path from
  `--message-format=json` immediately, and verify with `--list | grep <new test>`.
- `grep -q` under `set -o pipefail` SIGPIPEs the producer and reads as a mismatch — use
  `grep ... >/dev/null` when the producer is a long `--list`.
- Red-on-HEAD without a git worktree: `git archive <base> | tar -x` into scratch, copy the
  handler-level test files in, one build; 11/12 red (the 12th is the equivalence guard).
