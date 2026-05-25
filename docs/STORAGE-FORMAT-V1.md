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
[`docs/SUPPORT.md`](SUPPORT.md)):

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

- **Segment header (64 bytes):** `RRDWAL` magic + version=3 + shard id + epoch + segment number + reserved.
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
- **Trailer:** `0xFF` EOF byte + CRC32 over the whole file.
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
- Support / LTS policy: [`docs/SUPPORT.md`](SUPPORT.md) *(to be added in #104)*
- Security disclosure: [`SECURITY.md`](../SECURITY.md)
- Operator capacity planning: [`docs/OPERATOR-GUIDE.md`](OPERATOR-GUIDE.md)
- v0.3.0 roadmap (this file is a v0.2.0 deliverable): [`.planning/milestones/v0.3.0-ROADMAP.md`](../.planning/milestones/v0.3.0-ROADMAP.md)
