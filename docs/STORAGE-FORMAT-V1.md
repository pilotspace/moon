# Moon Storage Format v1

> **Audience:** operators and tooling authors. This document is the public
> commitment Moon makes about on-disk formats. **For exact byte layouts the
> authoritative source is the Rust constants and module-level rustdoc** in
> `src/persistence/`; this doc summarizes the guarantees, not the bits.

## 1. What "Storage Format v1" Means

Starting with **v0.2.0**, Moon's on-disk file formats are versioned as a
single umbrella tag, **storage format v1**. The umbrella covers three
sub-formats that together make a Moon shard durable:

| Sub-format | File(s) | Source of truth | Tag |
|---|---|---|---|
| **WAL v3** | `<persistence_dir>/wal/<shard>/<segment>.wal` | `src/persistence/wal_v3/{record.rs,segment.rs}` | `RRDWAL` magic, version byte = `3` |
| **RDB v2** (per-shard snapshot) | `<persistence_dir>/<shard>.rdb` | `src/persistence/snapshot.rs` | `RRDSHARD` magic, version byte = `2` |
| **AOF multi-part** | `<persistence_dir>/appendonlydir/{moon.aof.manifest, *.aof, *.rdb}` | `src/persistence/aof_manifest.rs`, `src/persistence/aof.rs` | manifest framing |

The version *byte* inside each file is the canonical machine-readable
marker. The "storage format v1" *umbrella* is the human-readable, release-
level commitment that all three of those byte-level versions are guaranteed
to be supported.

## 2. The Compatibility Guarantee

For the duration of the Moon v0.2.x minor series (≥ 18 months of LTS — see
`docs/SUPPORT.md`):

1. **Forward read.** Every v0.2.x release reads files written by every
   earlier v0.2.x release.
2. **Reverse read on retirement.** When v0.2 enters maintenance and v0.3
   becomes the active line, v0.3 reads v0.2 files using v0.3's recovery
   path. v0.3 may, however, write files only v0.3+ can read.
3. **Crash recovery.** Every file format includes per-record or per-page
   CRC32C / CRC32 checksums; corruption is detected and quarantined rather
   than silently propagated.
4. **No silent format bumps.** Any change to the WAL v3 record byte layout,
   the RDB v2 preamble, or the AOF manifest framing requires the parent
   version byte to increment. The umbrella tag becomes "storage format
   v2" simultaneously.
5. **Migration.** If a future Moon release retires a format inside the v1
   umbrella, an automatic in-place rewrite (or an offline `moon migrate`
   tool) is guaranteed before the format is dropped.

## 3. Sub-format Summary

### 3.1 WAL v3 — Per-Shard Write-Ahead Log

Authoritative source: `src/persistence/wal_v3/`.

- **Segment header (64 bytes):** `RRDWAL` magic + version=3 + flags + shard id + epoch + `redo_lsn` + `base_lsn` + segment size + reserved (byte layout in `segment.rs`).
  `base_lsn` is the LSN of the segment's first record (the next LSN to be assigned, while the segment holds none).
  Readers — the WAL recyclers and CDC.READ's seek — rely only on the bound it implies: every record of every *earlier*
  segment has an LSN `< base_lsn`. A header that overstates it therefore makes them conservative (a segment recycled
  later, a seek that starts one segment early), never wrong. Such headers exist only in WAL directories written by the
  unreleased moon#1188 build before the moon#1221 review fix, which stamped a segment opened after an off-loop rotation
  fsync with the LSN *after* the records appended while that fsync was in flight.
- **Record (variable):** little-endian, self-describing.
  ```
  Offset  Size  Field
  0       4     record_len (u32 LE)
  4       8     lsn (u64 LE) — monotonic log sequence number
  12      1     record_type (u8) — Command / FullPageImage / Checkpoint / VectorUpsert / VectorDelete / …
  13      1     flags (u8) — bit 0 = LZ4-compressed payload (FPI)
  14      2     reserved (zeroes)
  16      N     payload
  16+N    4     crc32c (u32 LE) — over bytes [4 .. 16+N]
  ```
- **FPI compression:** payloads ≥ `FPI_COMPRESS_THRESHOLD` (256 B) are LZ4-compressed; flag bit 0 set.
- **Atomicity:** segment rotation is fsync-fenced; partial trailing records are detected by CRC and truncated on recovery.

### 3.2 RDB v2 — Per-Shard Point-in-Time Snapshot

Authoritative source: `src/persistence/snapshot.rs`.

- **Preamble (35 bytes):** `RRDSHARD` magic + version=2 + shard_id (u16 LE) + epoch (u64 LE) + last_lsn (u64 LE) + created_at_unix_ms (u64 LE).
- **Body:** value-encoded keys + entries (custom RDB-style, supports listpack / intset / hashtable / sorted-set / stream encodings).
- **Per-field hash TTL trailer (v2-only):** every `TYPE_HASH` body is followed by
  `[ttl_count u32][field_len varint | field_bytes | ttl_ms u64]*`. `ttl_count = 0`
  for plain hashes (no per-field TTL). v1 readers stop after the hash body and
  reconstruct a plain `Hash`; v2 readers consume the trailer to rebuild
  `HashWithTtl`. Authoritative encoder/decoder: `src/persistence/rdb.rs` —
  search for `has_hash_ttl_trailer`.
- **Trailer:** `0xFF` EOF byte + CRC32 over the whole file.
- **Cold-graves trailer (optional, moon#1281):** between the `0xFF` EOF byte and
  the global CRC32 a snapshot taken WITHOUT an AOF may carry the spill-file
  slots that were already dead when it started:
  `"MCGV" | ver u8 = 1 | file_count u32 | { file_id u64 | slot_count u32 |
  { page_idx u32 | slot_idx u16 }* }* | crc32 u32` (all little-endian; the
  trailer's CRC covers `"MCGV"` through the last slot). Boot of a no-AOF
  process drops exactly those slots before rebuilding the cold index, so a cold
  key deleted before a successful snapshot stays deleted after a crash. Every
  reader stops at the EOF byte, so readers that predate the trailer load the
  file unchanged and ignore it (and resurrect those keys, as before) — the
  version byte is NOT bumped. Absent trailer = no graves. A trailer that fails
  its own checks is logged and ignored; the global CRC still guards the keys.
  Authoritative codec: `src/persistence/snapshot/cold_graves.rs` (fuzz target
  `snapshot_cold_graves`).
- **PITR:** the embedded `last_lsn` ties each snapshot to the WAL position it shadows; replay resumes at `last_lsn + 1`.
- **Forkless:** snapshots are produced by cooperative segment iteration with per-snapshot overflow buffers — no `fork()`, no COW RSS spike.

A v0.2.x reader still accepts the legacy v1 preamble (19 bytes, no
`last_lsn` / `created_at_unix_ms`) for snapshots produced by v0.1.x.

### 3.3 AOF Multi-Part — Append-Only Log Bundle

Authoritative source: `src/persistence/aof.rs`, `src/persistence/aof_manifest.rs`.

- **Directory:** `<persistence_dir>/appendonlydir/`
- **Manifest:** `moon.aof.manifest` — versioned listing of (base RDB, sequence of incremental AOF segments).
- **Base file:** optional RDB-format snapshot of state at last rewrite.
- **Incremental files:** RESP-encoded write commands appended since last rewrite.
- **Legacy single-file `appendonly.aof`** is recognized on first boot, captured as seq-1 of the multi-part structure, and the legacy file is renamed to `appendonly.aof.legacy`. Older v0.1.x AOF files are read once, never written.
- **Per-shard incremental files** (`--shards` ≥ 2) frame every record as `[u64 lsn LE][u32 len LE][RESP command]`; the multi-part top-level incr and the flat `appendonly.aof` hold bare RESP.

#### Replay-only records (`MOON.*` pseudo-commands)

Besides client write commands, the writer interleaves a few records that only
a replay acts on. Each is an ordinary RESP array (never a `#…` annotation
line, which moon's parser reads as a malformed RESP3 boolean, and never a new
WAL record type byte), written with `lsn = 0` in the framed layout so it never
moves the replication offset a replay recovers. A client that sends one gets
`ERR unknown command`. Authoritative source:
`src/persistence/replay/pseudo.rs` and `src/persistence/cold_records.rs`.

| Record | Written | Replay effect |
|---|---|---|
| `SELECT <db>` | before a record whose execution db differs from the stream's | switches the db the following records apply to |
| `MOON.COLDCUT <watermark>` | first record of every generation (boot head, rewrite head) | opens the cold-tier replay gate: cold files with `file_id < watermark` are a valid base for what follows |
| `MOON.TS <ms>` (moon#1283) | right after `MOON.COLDCUT` in every generation head, before the first record a writer appends to a file it reopened, then before any record whose shard clock differs from the last `MOON.TS` in the stream | sets the expiry-judgment clock (see below) |
| `MOON.TS <ms> CLOSE` (moon#1283) | last record a writer appends when it stops in order (SHUTDOWN, SIGTERM) with the file still the live incr, made durable by its final sync | marks a clean close: what follows it up to the next stamp is another binary's (see below) |
| `MOON.SPILLED <file_id> key…` | when a spill publishes keys into the cold index | demotes replay-built hot copies of those keys to their cold entries |

`MOON.TS <ms>` carries the shard's cached clock in unix milliseconds — the
clock the command after it judged key expiry with — read in the same
synchronous section as the mutation. It is at most one record per 1 ms clock
tick in which the shard logged a write (≤ 44 bytes each), plus one per record
whose producer parked between its mutation and its enqueue. It is AOF-only:
the replication stream never carries it.

A replay judges every record by the **last** `MOON.TS` read in the current
file (not a running maximum: a parked producer's record carries an older
stamp than the records it lands after), except a foreign segment (below).
Until a file's first `MOON.TS` — an older binary's log, or the stamp-less
prefix of a file an older binary started — it falls back to the file's
modification time capped at the wall clock (moon#1277), exactly as before. A
`MOON.TS` of 0, past the year 9999, or malformed is skipped. Clock stamps are
observations, not data: a replay that skips a block of data records still
applies the stamps inside it.

**Foreign segments (positional).** A writer that reopens a file writes a
`MOON.TS` before its first append, and one that stops in order ends the file
with `MOON.TS <ms> CLOSE`. So the records after a `CLOSE` and before the next
stamp (plain or `CLOSE`) were not written by a binary that knows this rule:
they are a foreign segment — an older binary's appends after a downgrade.
The replay judges them by that next stamp (the later session's first stamp,
never earlier than their own write time; late is how the older binary itself
judges them, by the mtime), or, when the segment runs to the end of the file,
by `max(<ms> of the CLOSE, the file's mtime capped at the wall clock)`. The
boot that meets such a segment at the end of the file writes that judgment as
the first stamp of its own appends, so every later boot judges the segment
exactly as it did. The rule depends only on record positions: no rewrite is
needed, a file's mtime moved forward (`touch`, a `cp` restore) re-judges
nothing, and a `CLOSE` immediately followed by a stamp (a clean restart) is
an empty segment.

**Compatibility.** Adding `MOON.TS` changes no byte layout covered by §2
rule 4 (WAL v3 records, the RDB v2 preamble, the manifest framing), so it is
not a storage-format bump:
- *Upgrade:* a log without stamps replays exactly as before (mtime judgment).
- *Downgrade:* a binary that predates `MOON.TS` sends it — and `MOON.TS <ms>
  CLOSE` — to command dispatch, gets "unknown command", counts it as an
  unhandled record, and replays every data record around it with its own
  (mtime) expiry judgment. The boot does not fail. (A build that knows only
  the one-argument `MOON.TS` skips the `CLOSE` form as a malformed stamp.)
  An older binary's own `BGREWRITEAOF` drops every marker, which is fine.
- *Downgrade, then re-upgrade:* the older binary appends to the same file
  with no stamps. After a CLEAN stop of the newer binary those records form
  a foreign segment and are judged as above; keys the older binary saw expire
  and restarted (`INCR` onto an expired counter, with no `DEL` logged first)
  come back as they were live, on the re-upgrade boot and on every boot after
  it, however the re-upgraded server is stopped later.

  **Not protected:** a downgrade after an UNCLEAN stop of the newer binary
  (kill -9, OOM kill, crash, power loss) — there is no `CLOSE`, so the older
  binary's records follow the newer binary's last stamp, hours or days stale,
  and those keys replay onto their old value and deadline and are lost.
  **Procedure:** stop the newer binary cleanly (`SHUTDOWN`, SIGTERM) before
  downgrading; after it crashed, start it once and stop it cleanly first.
  Otherwise, as the older binary's last action, run `BGREWRITEAOF` and stop
  it once the rewrite completed: its history is then in the new base, an
  image that judges nothing, and only what it appended after the rewrite
  replays as a stamp-less prefix (mtime judgment, as on any upgrade). Also
  not protected:
  a newer binary whose writer could not finish its stop (abandoned after the
  stop timeout, or a torn write it latched) — its log shows no
  "AOF writers drained and synced" line.
- *Redis:* a redis server cannot load a moon AOF regardless (`MOON.COLDCUT`
  is already an unknown command there).

The per-shard WAL v3 KV log (`--appendonly no`) does not carry `MOON.TS`
yet: its replay keeps the mtime judgment of its newest segment.

## 4. Configuration Surface

A `--storage-format <v1>` CLI flag is reserved and will be introduced in a
follow-up PR (issue #103 second checkbox). It accepts only `v1` today; the
flag exists so that future Moon releases can offer `v2`-or-newer write
opt-in while still defaulting to `v1` for compatibility.

Operators do not need to set this flag in v0.2.x. It is documented here so
that automation written today against v0.2 continues to be correct when
storage format v2 is introduced.

## 5. Migration Policy

When the umbrella tag bumps to **storage format v2** (no earlier than
v0.3), the release notes will contain:

1. The list of byte-level format changes (which sub-formats changed; which
   stayed identical).
2. An automatic in-place rewrite path — Moon detects v1 files at startup,
   rewrites them to v2 atomically with a `*.v1.bak` retained, and proceeds.
3. A `moon migrate --storage-format v2 --dry-run` offline tool for
   operators who prefer to validate before bumping production.
4. A minimum-required Moon version for the rewrite to be safe.

No v1 file will ever be silently rewritten to v2 without operator opt-in
once v2 is the default.

## 6. Backwards-Compatibility Promise (Summary)

| Promise | Scope |
|---|---|
| v0.2.x reads files written by all v0.2.x releases | hard guarantee |
| v0.3.x reads v0.2.x files (one-way) | hard guarantee |
| v0.2.x reads v0.1.x files (one-way, best-effort) | currently implemented for RDB v1 + legacy `appendonly.aof`; not a hard guarantee |
| Major version bump never requires manual format conversion | hard guarantee — in-place rewrite is automated |
| Crash recovery from any v1 file at any LSN | hard guarantee — every record / page is CRC-protected |

## 7. Cross-References

- WAL v3 record layout: `src/persistence/wal_v3/record.rs`
- WAL v3 segment header: `src/persistence/wal_v3/segment.rs`
- RDB v2 preamble: `src/persistence/snapshot.rs`
- AOF manifest framing: `src/persistence/aof_manifest.rs`
- Support / LTS policy: `docs/SUPPORT.md` *(to be added in #104)*
- Security disclosure: [`SECURITY.md`](security.md)
- Operator capacity planning: [`docs/OPERATOR-GUIDE.md`](OPERATOR-GUIDE.md)
- v0.3.0 roadmap (this file is a v0.2.0 deliverable): `.planning/milestones/v0.3.0-ROADMAP.md`
