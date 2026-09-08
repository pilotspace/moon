# Text postings persistence (`.tpost`)

Why: until this landed, a text index had no durable form. `text-indexes.meta`
carried definitions and `.tfst` (written only by `FT.COMPACT`) carried term
dictionaries; every hash under a text prefix was re-tokenised on every boot.
After #873 that phase is linear in the corpus' total tokens (~0.22 µs/token,
half tokenising and half building postings — measured, see the PR) and grows
with the corpus forever. This file turns it into a load.

## What is persisted

One file per text index: `{persist_dir}/{xxh64(name):016x}.tpost`, next to
`text-indexes.meta`. It carries the complete queryable state of the index —
everything `index_document` / `tag_index_document` / `numeric_index_document`
build — plus one 64-bit **content checksum per document** (see Validity).

```
header  "TPS1" | version u8 = 1 | flags u8 | reserved u16 | schema_hash u64 | payload_len u64
payload name | db_index | next_doc_id
        docs[]      doc_id, key, content_checksum, insert_lsn, per-field token lengths
        fields[]    term dict (next_id, fst_high_water_mark, terms), FST bytes,
                    postings[] term_id, has_positions, doc_ids (strictly increasing),
                               term_freqs, positions
        tag_docs[]  doc_id -> (field, normalized value)*
        num_docs[]  doc_id -> (field, f64)*
trailer xxh64 over header + payload
```

`field_stats` are **not** stored: they are recomputed from the per-doc lengths
on load, which makes a loaded index equal to a rebuilt one even where the live
accounting had drifted (`remove_doc_by_doc_id` only decrements `num_docs` for
non-empty fields; a rebuild counts every doc).

## Validity contract

The postings must be provably in sync with the hashes they index. Two stamps,
mirroring the vector plane's `key_hash_to_vec_checksum` reconcile:

1. **Schema hash** (header): xxh64 over name, db, prefixes, every TEXT/TAG/
   NUMERIC field definition and the BM25 config. Mismatch = the definition
   changed under the file (`FT.DROPINDEX` + `FT.CREATE` with another schema)
   → rebuild the whole index.
2. **Per-document content checksum**: xxh64 over the schema fields' values
   *as the index saw them at index time* (`TextIndex::content_checksum`).
   At boot the keyspace walk computes the same function over the live hash.
   Equal → the doc is skipped (no tokenising). Different (partial `HSET`,
   `HDEL`, type change, or the file predating the write) → the doc is
   re-indexed from the live hash, exactly as a rebuild would.
   After the walk, every loaded doc whose key was **not observed** is removed
   (deletion probe) — this also preserves today's semantics, where `DEL` never
   unindexes text and the boot rebuild is what drops deleted keys.

So the file may be arbitrarily stale (a crash after the last flush) and the
result is still identical to a rebuild; the checksum only decides how much
work the walk does.

## Fallback triggers (all per index, all → the pre-existing rebuild path)

Missing file · short file · bad magic · unknown version · `payload_len`
mismatch · trailer checksum mismatch · name mismatch · schema-hash mismatch ·
any structural violation (duplicate doc_id/key, `doc_id >= next_doc_id`,
`term_id >= next_id` or duplicate, posting for an unknown term or doc,
non-increasing doc_ids, `tf == 0`, length mismatches, `positions` count
mismatch, non-finite numeric, field count ≠ schema) · count that does not fit
in the remaining bytes (no pre-allocation on a hostile count) · `.tfst`
seeding then runs as before. A loaded index is all-or-nothing: the decoder
returns a plain struct and nothing is installed until it validated in full.

## Write path — never on the shard thread's I/O budget

- Every mutator bumps `TextIndex::mutation_seq`; `persisted_seq` trails it.
- `TextStore::persist_dirty_postings(budget)` runs from the 1 s tick of both
  event-loop legs. It **encodes** dirty indexes on the shard thread (pure
  memcpy-class CPU, no syscalls) under a 2 ms budget per tick and a per-index
  1 % duty cycle (an index that took 50 ms to encode is not encoded again for
  5 s), then hands the bytes to `text::persist_writer` — one aux thread that
  does tmp → fsync → rename → dir fsync (`atomic_write_durable`, the
  `kv_spill.rs` pattern). Pending bytes are capped (256 MiB) and deduplicated
  by path (latest wins).
- Graceful shutdown encodes everything dirty and blocks on the writer's
  drain (bounded), after the KV checkpoint.
- `FT.DROPINDEX` deletes the file through the same thread; unknown `.tpost`
  files are swept at boot.
- `FT.COMPACT` still writes `.tfst`; `build_fst` marks the index dirty so
  the FST also lands in `.tpost`.

## Load path

Boot: restore definitions (meta v2 + TAG/NUMERIC extension block) →
`load_postings_files` (yielding every 64 indexes) → `.tfst` seeding for the
ones that fell back → keyspace walk with `reconcile_key` (text plane now
inside `RecoveryState`) → `finish`: deletion probe, `field_stats` recompute,
one summary line per loaded index:
`text index X: loaded N doc(s), K verified unchanged, R re-indexed, D removed`.

## Version skew

An older binary ignores `.tpost` files and the meta extension block (the meta
file keeps version 2; the block is appended after the v2 body and the v2
reader stops at `count`). A newer binary rejects any `.tpost` version it does
not know and rebuilds. Nothing here is platform-specific; the tokio leg
(`--no-default-features`) compiles the store without the persistence module
and keeps rebuilding.
