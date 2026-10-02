//! Multi-part / per-shard AOF replay: base RDB load + framed/RESP incr replay.
//!
//! Split out of the `aof_manifest` parent module (issue #143) to keep each file
//! under the 1500-line cap. `use super::*` pulls the manifest types, std/tracing
//! imports, and private helpers down from the parent module.

use super::*;

/// Cold-tier wiring (`cold_shard_dir` + `cold_index`) captured per database so
/// it survives the `rdb::load` swap. See [`take_cold_wiring`].
type ColdWiring = Vec<(
    Option<std::path::PathBuf>,
    Option<crate::storage::tiered::cold_index::ColdIndex>,
)>;

/// Detach cold-tier wiring from every database before an `rdb::load`.
///
/// `rdb::load` loads the base RDB into fresh `Database::new()` temporaries and
/// swaps them wholesale into the live databases (`*live = temp`). That swap
/// silently drops `cold_shard_dir` and the recovery-rebuilt `cold_index` —
/// live-tier topology, not part of the RDB hot snapshot. Capture both here and
/// re-attach with [`restore_cold_wiring`] BEFORE the incr replay: replayed
/// DEL/UNLINK/FLUSH*/EXPIRE-past must tombstone the cold plane, and with
/// `cold_index == None` those paths (`remove_counting_cold()`/`clear()`) are
/// silent cold no-ops, so a key deleted in the incr tail resurrects via cold
/// read-through after restart (its manifest entry stays Active until the
/// orphan sweep).
use crate::persistence::aof::torn_tail::TornTail;

fn take_cold_wiring(databases: &mut [crate::storage::Database]) -> ColdWiring {
    databases
        .iter_mut()
        .map(|db| (db.cold_shard_dir.take(), db.cold_index.take()))
        .collect()
}

/// Re-attach wiring captured by [`take_cold_wiring`].
fn restore_cold_wiring(databases: &mut [crate::storage::Database], wiring: ColdWiring) {
    for (db, (cold_shard_dir, cold_index)) in databases.iter_mut().zip(wiring) {
        db.cold_shard_dir = cold_shard_dir;
        db.cold_index = cold_index;
    }
}

/// Replay multi-part AOF: load base RDB then replay incremental RESP.
///
/// Returns total keys/commands loaded.
pub fn replay_multi_part(
    databases: &mut [crate::storage::Database],
    manifest: &AofManifest,
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
) -> Result<usize, crate::error::MoonError> {
    let mut total = 0usize;

    // Load base RDB. Cold wiring must survive the swap AND be live for the
    // incr replay below (replayed DEL/FLUSH must tombstone the cold plane) —
    // see take_cold_wiring.
    let base_path = manifest.base_path();
    if base_path.exists() {
        let cold_wiring = take_cold_wiring(databases);
        let load_result = crate::persistence::rdb::load(databases, &base_path);
        restore_cold_wiring(databases, cold_wiring);
        match load_result {
            Ok(n) => {
                info!(
                    "AOF base RDB loaded: {} keys from {}",
                    n,
                    base_path.display()
                );
                total += n;
            }
            Err(e) => {
                // Base RDB is corrupt or unreadable — applying incremental
                // deltas on top of missing/corrupt base gives wrong results.
                error!("AOF base RDB load failed: {}", e);
                return Err(e);
            }
        }
    } else {
        // Missing base is tolerable only when the incr log is also empty
        // (fresh manifest from initialize(), or first boot after legacy
        // upgrade). If there's incremental content but no base, replaying
        // deltas (DEL, EXPIRE, HINCRBY, …) on an empty database produces
        // incorrect state — fail loudly rather than silently corrupt.
        let incr_path = manifest.incr_path();
        let incr_len = std::fs::metadata(&incr_path).map(|m| m.len()).unwrap_or(0);
        if incr_len > 0 {
            return Err(crate::error::MoonError::from(
                crate::error::AofError::RewriteFailed {
                    detail: format!(
                        "AOF base RDB missing at {} but incr {} is {} bytes; refusing to replay incr against empty state",
                        base_path.display(),
                        incr_path.display(),
                        incr_len,
                    ),
                },
            ));
        }
        warn!(
            "AOF base RDB not found: {} (incr empty, treating as fresh init)",
            base_path.display()
        );
    }

    // Replay incremental RESP — streamed through a bounded buffer (moon#1160):
    // the whole-file read plus `BytesMut::from(&data[..])` held the incr file
    // twice, and every replayed element sliced that one buffer.
    let incr_path = manifest.incr_path();
    if incr_path.exists() {
        let file = std::fs::File::open(&incr_path)?;
        if file.metadata()?.len() > 0 {
            // moon#1277: judge expiry by the time the log was last written.
            let _clock = crate::persistence::replay::clock::pin_replay_clock_to_log(
                &incr_path,
                crate::persistence::replay::clock::LogFormat::Resp,
            );
            // Pure RESP — no RDB preamble detection needed.
            let (count, torn) =
                replay_incr_resp(databases, file, engine).map_err(|e| in_file(&incr_path, e))?;
            info!(
                "AOF incr replayed: {} commands from {}",
                count,
                incr_path.display()
            );
            total += count;
            // R2b round 3 F-B: the writer appends to this incr — cut a torn
            // tail first, or it swallows every later record.
            cut_torn_tail(&incr_path, torn)?;
        }
    }

    Ok(total)
}

/// `e`, naming the incr file it was read from and the boot's options.
fn in_file(path: &std::path::Path, e: crate::error::MoonError) -> crate::error::MoonError {
    crate::error::MoonError::from(crate::error::AofError::RewriteFailed {
        detail: format!(
            "{}: {e}. The boot refuses a damaged AOF rather than serve a partial dataset and \
             append behind the damage: restore appendonlydir/ from a backup, or truncate a \
             copy of the file at the offset named above and boot from it",
            path.display()
        ),
    })
}

/// Cut the torn tail a replay of `path` reported ([`crate::persistence::aof::torn_tail`]).
fn cut_torn_tail(
    path: &std::path::Path,
    torn: Option<crate::persistence::aof::torn_tail::TornTail>,
) -> Result<(), crate::error::MoonError> {
    let Some(tail) = torn else {
        return Ok(());
    };
    crate::persistence::aof::torn_tail::cut_at_boot(path, tail)
        .map(|_| ())
        .map_err(|detail| {
            crate::error::MoonError::from(crate::error::AofError::Corrupted {
                offset: tail.offset,
                detail,
            })
        })
}

/// Replay pure RESP commands from `src`, streamed through the bounded
/// [`ReplayChunks`](crate::persistence::replay::chunks::ReplayChunks) buffer
/// (moon#1160) rather than held — twice — in memory.
///
/// **Corruption handling:** On mid-stream parse errors this returns an error
/// rather than silently resyncing to the next `*` byte. Silent resync in a
/// multi-part AOF is dangerous: an undetected run of dropped commands leaves
/// the database in an inconsistent state that cannot be reconstructed.
/// Truncated tails (parser returns `Ok(None)` with bytes remaining) are
/// logged and treated as the legitimate end of the incremental log, matching
/// `replay_aof` semantics for crash-time tail truncation.
fn replay_incr_resp(
    databases: &mut [crate::storage::Database],
    src: impl std::io::Read,
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
) -> Result<(usize, Option<TornTail>), crate::error::MoonError> {
    let replayed = replay_incr_resp_records(databases, src, engine);
    // moon#1300: the end of the incr — roll back a `MOON.TXN` block a crash
    // cut (on an error too: the engine must not carry it further).
    engine.finish_log(databases);
    replayed
}

/// The body of [`replay_incr_resp`]: every record of the incr.
fn replay_incr_resp_records(
    databases: &mut [crate::storage::Database],
    src: impl std::io::Read,
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
) -> Result<(usize, Option<TornTail>), crate::error::MoonError> {
    use crate::persistence::replay::chunks::{ReplayChunks, ReplayNext};
    use crate::protocol::Frame;

    let mut chunks = ReplayChunks::new(src, 0);
    let mut selected_db: usize = 0;
    let mut count: usize = 0;
    let mut torn = None;

    loop {
        match chunks.next_frame()? {
            ReplayNext::Frame(frame) => {
                let (cmd, cmd_args) = match &frame {
                    Frame::Array(arr) if !arr.is_empty() => {
                        let name = match &arr[0] {
                            Frame::BulkString(s) => s.as_ref(),
                            Frame::SimpleString(s) => s.as_ref(),
                            other => {
                                return Err(crate::error::MoonError::from(
                                    crate::error::AofError::RewriteFailed {
                                        detail: format!(
                                            "AOF incr command at offset {} has non-string name frame: {:?}",
                                            chunks.offset(),
                                            std::mem::discriminant(other)
                                        ),
                                    },
                                ));
                            }
                        };
                        (name as &[u8], &arr[1..])
                    }
                    other => {
                        return Err(crate::error::MoonError::from(
                            crate::error::AofError::RewriteFailed {
                                detail: format!(
                                    "AOF incr non-array frame at offset {}: {:?}",
                                    chunks.offset(),
                                    std::mem::discriminant(other)
                                ),
                            },
                        ));
                    }
                };
                // R2 review of moon#1283: where a clean-close marker's
                // segment starts.
                crate::persistence::replay::clock::at_record_end(chunks.offset());
                engine.replay_command(databases, cmd, cmd_args, &mut selected_db);
                count += 1;
            }
            ReplayNext::End => break,
            ReplayNext::Truncated { offset, len } => {
                warn!(
                    "AOF incr truncated tail: {} bytes at offset {} (treating as crash-time EOF)",
                    len, offset
                );
                torn = Some(TornTail {
                    offset,
                    len: len as u64,
                });
                break;
            }
            ReplayNext::Corrupt { offset, err } => {
                return Err(crate::error::MoonError::from(
                    crate::error::AofError::RewriteFailed {
                        detail: format!("AOF incr parse error at offset {}: {:?}", offset, err),
                    },
                ));
            }
        }
    }

    Ok((count, torn))
}

/// An entry that was tagged `OrderedAcrossShards` (RFC § 2 Rule 2) and
/// must be merge-replayed in global LSN order after per-shard replay
/// completes. The `shard_id` records which shard's file it came from so
/// the merge step can dispatch each entry back to its origin shard's
/// databases.
#[derive(Debug, Clone)]
pub struct OrderedEntry {
    pub shard_id: u16,
    pub lsn: u64,
    pub bytes: bytes::Bytes,
}

/// Replay a framed PerShard incr file: `[u64 lsn LE][u32 len LE][RESP bytes]`.
///
/// Step 3 wrote this format; step 4 reads it. Step 5 extends the LSN field:
/// the high bit (`crate::persistence::aof::ORDERED_LSN_FLAG`) marks the
/// entry as `OrderedAcrossShards` — those entries are NOT replayed inline,
/// instead they are pushed into `ordered_buf` for the caller to merge-replay
/// in global LSN order across all shards.
///
/// Returns `(commands_replayed, max_lsn)` — the count covers only inline
/// (non-ordered) replays. `max_lsn` is the NEXT-FREE replication offset:
/// `max(entry.lsn + entry.len)` across both inline AND ordered entries (the
/// high bit is masked out before the computation). It is the offset AFTER the
/// last byte on disk, NOT the start LSN of the last entry — because
/// `ReplicationState::issue_lsn` returns the offset BEFORE adding the entry
/// length, seeding `master_repl_offset` with a start LSN would reissue the
/// last on-disk LSN and break the lsn->entry uniqueness invariant (F5).
///
/// **Truncated entries:** a header partly written at crash time is treated as
/// EOF (parity with `replay_incr_resp` semantics). A whole header followed by
/// a truncated payload is also EOF — the writer's invariant is that the
/// header is written first then the payload, and on partial write the most we
/// can lose is the last entry's payload tail.
///
/// **Corruption:** a mid-stream RESP parse error inside an otherwise-complete
/// payload is fatal (same reasoning as `replay_incr_resp`).
fn replay_incr_framed(
    shard_id: u16,
    databases: &mut [crate::storage::Database],
    data: &[u8],
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
    ordered_buf: &mut Vec<OrderedEntry>,
) -> Result<(usize, u64, Option<TornTail>), crate::error::MoonError> {
    let replayed = replay_incr_framed_records(shard_id, databases, data, engine, ordered_buf);
    // moon#1300: the end of the incr — see `replay_incr_resp`.
    engine.finish_log(databases);
    replayed
}

/// The body of [`replay_incr_framed`]: every record of the incr.
fn replay_incr_framed_records(
    shard_id: u16,
    databases: &mut [crate::storage::Database],
    data: &[u8],
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
    ordered_buf: &mut Vec<OrderedEntry>,
) -> Result<(usize, u64, Option<TornTail>), crate::error::MoonError> {
    use crate::protocol::{Frame, ParseConfig, parse};
    use bytes::BytesMut;

    const HEADER_LEN: usize = 12; // u64 lsn LE + u32 len LE

    let total_len = data.len();
    let mut offset: usize = 0;
    let config = ParseConfig::default();
    let mut selected_db: usize = 0;
    let mut count: usize = 0;
    let mut max_lsn: u64 = 0;
    let mut torn = None;

    while offset < total_len {
        if total_len - offset < HEADER_LEN {
            warn!(
                "AOF incr framed truncated header: {} bytes at offset {} (treating as crash-time EOF)",
                total_len - offset,
                offset
            );
            torn = Some(TornTail {
                offset: offset as u64,
                len: (total_len - offset) as u64,
            });
            break;
        }
        // SAFETY: line 1491 guarantees `total_len - offset >= HEADER_LEN` (=12),
        // so the [offset..offset+8] and [offset+8..offset+12] slices are valid
        // and `try_into()` to a fixed-size array cannot fail (length-matched).
        #[allow(clippy::unwrap_used)] // bounds-checked above; try_into is statically length-matched
        let raw_lsn = u64::from_le_bytes(data[offset..offset + 8].try_into().expect("8 bytes"));
        #[allow(clippy::unwrap_used)] // same bounds-check guarantee
        let len =
            u32::from_le_bytes(data[offset + 8..offset + 12].try_into().expect("4 bytes")) as usize;
        let payload_start = offset + HEADER_LEN;
        let payload_end = payload_start.saturating_add(len);
        if payload_end > total_len {
            warn!(
                "AOF incr framed truncated payload at offset {} (lsn {:#x}, declared len {}, have {} bytes); treating as crash-time EOF",
                offset,
                raw_lsn,
                len,
                total_len - payload_start
            );
            torn = Some(TornTail {
                offset: offset as u64,
                len: (total_len - offset) as u64,
            });
            break;
        }

        // H1-BARRIER defense-in-depth: a zero-length record carries no RESP
        // command (the writer skips barrier records entirely, but tolerate one
        // if it ever lands on disk) — skip it rather than reject the file.
        if len == 0 {
            offset = payload_end;
            continue;
        }

        // Strip the OrderedAcrossShards flag to recover the true LSN.
        let is_ordered = raw_lsn & crate::persistence::aof::ORDERED_LSN_FLAG != 0;
        let lsn = raw_lsn & !crate::persistence::aof::ORDERED_LSN_FLAG;

        // Ordered entries: buffer for cross-shard merge replay; do NOT
        // dispatch inline.
        if is_ordered {
            let bytes = bytes::Bytes::copy_from_slice(&data[payload_start..payload_end]);
            ordered_buf.push(OrderedEntry {
                shard_id,
                lsn,
                bytes,
            });
            // F5: track the next-free replication offset (entry end), not the
            // start LSN — `issue_lsn` returns the offset BEFORE adding the
            // entry length, so the seed must clear every byte already on disk.
            let entry_end = lsn + len as u64;
            if entry_end > max_lsn {
                max_lsn = entry_end;
            }
            offset = payload_end;
            continue;
        }

        // R2b round 3 F-B: a record is a RESP array; anything else is
        // corruption (the parser's inline fallback would take it for a
        // command).
        if data[payload_start] != b'*' {
            return Err(crate::error::MoonError::from(
                crate::error::AofError::RewriteFailed {
                    detail: format!(
                        "AOF incr framed payload at offset {} (lsn {}, len {}) does not open \
                         with '*' (found {:#04x}); corrupt entry",
                        offset, lsn, len, data[payload_start]
                    ),
                },
            ));
        }
        // Parse RESP from the payload slice. A standalone slice ensures one
        // header maps to exactly one command — no implicit pipelining across
        // headers.
        let mut buf = BytesMut::from(&data[payload_start..payload_end]);
        match parse::parse(&mut buf, &config) {
            Ok(Some(frame)) => {
                let (cmd, cmd_args) = match &frame {
                    Frame::Array(arr) if !arr.is_empty() => {
                        let name = match &arr[0] {
                            Frame::BulkString(s) => s.as_ref(),
                            Frame::SimpleString(s) => s.as_ref(),
                            other => {
                                return Err(crate::error::MoonError::from(
                                    crate::error::AofError::RewriteFailed {
                                        detail: format!(
                                            "AOF incr framed command at offset {} (lsn {}) has non-string name frame: {:?}",
                                            offset,
                                            lsn,
                                            std::mem::discriminant(other)
                                        ),
                                    },
                                ));
                            }
                        };
                        (name as &[u8], &arr[1..])
                    }
                    other => {
                        return Err(crate::error::MoonError::from(
                            crate::error::AofError::RewriteFailed {
                                detail: format!(
                                    "AOF incr framed non-array frame at offset {} (lsn {}): {:?}",
                                    offset,
                                    lsn,
                                    std::mem::discriminant(other)
                                ),
                            },
                        ));
                    }
                };
                // R2 review of moon#1283: where a clean-close marker's
                // segment starts.
                crate::persistence::replay::clock::at_record_end(payload_end as u64);
                engine.replay_command(databases, cmd, cmd_args, &mut selected_db);
                count += 1;
                // F5: next-free offset = entry start LSN + RESP byte length.
                let entry_end = lsn + len as u64;
                if entry_end > max_lsn {
                    max_lsn = entry_end;
                }
            }
            Ok(None) => {
                // Header said `len` bytes of RESP, but parser can't make a
                // frame from those bytes. That's corruption inside a fully
                // declared payload, not a truncated tail — escalate.
                return Err(crate::error::MoonError::from(
                    crate::error::AofError::RewriteFailed {
                        detail: format!(
                            "AOF incr framed payload at offset {} (lsn {}, len {}) parsed as incomplete frame; corrupt entry",
                            offset, lsn, len
                        ),
                    },
                ));
            }
            Err(e) => {
                return Err(crate::error::MoonError::from(
                    crate::error::AofError::RewriteFailed {
                        detail: format!(
                            "AOF incr framed parse error at offset {} (lsn {}, len {}): {:?}",
                            offset, lsn, len, e
                        ),
                    },
                ));
            }
        }

        offset = payload_end;
    }

    Ok((count, max_lsn, torn))
}

/// Replay a PerShard multi-part AOF into N parallel `Vec<Database>` buffers.
///
/// `per_shard_databases[i]` is shard `i`'s database vector. The manifest's
/// `shards` length MUST equal `per_shard_databases.len()`; the caller is
/// expected to have run [`AofManifest::verify_shard_count`] at boot.
///
/// Per-shard replay is fully parallel: each shard's base RDB load and incr
/// replay run in a separate OS thread via `std::thread::scope`. Shards are
/// independent (different `DashTable` instances, no shared mutable state), so
/// this is safe and correct. Parallelism delivers the RFC § 1 benefit on
/// multi-shard deployments with large AOF files.
///
/// The `engine_factory` closure is called once per shard thread to produce an
/// independent replay engine. This is required because `CommandReplayEngine`
/// implementations (e.g., `DispatchReplayEngine` under the `graph` feature)
/// may contain non-`Sync` state (`RefCell`) that cannot be safely shared across
/// threads. Each thread owns its own engine; results (total count, max LSN,
/// ordered entries) are collected and merged in the caller thread after all
/// shard threads complete.
///
/// Returns `(total_commands_replayed, global_max_lsn, ordered_entries)`:
///   - `total_commands_replayed` covers all inline (non-ordered) entries
///     plus the base-RDB key count.
///   - `global_max_lsn` is `max(per-shard max LSN)` across both inline and
///     ordered entries; the caller is expected to call
///     `ReplicationState::seed_master_offset(global_max_lsn)` before
///     accepting client traffic (RFC § 2 Rule 3).
///   - `ordered_entries` is the set of `OrderedAcrossShards`-tagged entries
///     across ALL shards; the caller passes them to
///     [`replay_ordered_merge`] for the cross-shard merge replay.
pub fn replay_per_shard(
    per_shard_databases: &mut [&mut [crate::storage::Database]],
    manifest: &AofManifest,
    engine_factory: &(
         dyn Fn() -> Box<dyn crate::persistence::replay::CommandReplayEngine + Send> + Sync
     ),
) -> Result<(usize, u64, Vec<OrderedEntry>), crate::error::MoonError> {
    debug_assert_eq!(
        manifest.layout,
        AofLayout::PerShard,
        "replay_per_shard called on TopLevel manifest"
    );
    if manifest.shards.len() != per_shard_databases.len() {
        return Err(crate::error::MoonError::from(
            crate::error::AofError::RewriteFailed {
                detail: format!(
                    "replay_per_shard shard-count mismatch: manifest has {} shards, caller passed {} database vectors",
                    manifest.shards.len(),
                    per_shard_databases.len()
                ),
            },
        ));
    }

    // Per-shard type alias for the thread result.
    type ShardResult = Result<(usize, u64, Vec<OrderedEntry>), crate::error::MoonError>;

    // Use std::thread::scope so each shard thread borrows its databases slice
    // without a 'static lifetime requirement. All threads complete before scope
    // exits, which satisfies the borrow checker. Errors are propagated via
    // a Vec<ShardResult> collected after join.
    let shard_results: Vec<ShardResult> = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(per_shard_databases.len());

        for (shard_id, databases) in per_shard_databases.iter_mut().enumerate() {
            let sid = shard_id as u16;
            let base_path = manifest.shard_base_path(sid);
            let incr_path = manifest.shard_incr_path(sid);
            let engine = engine_factory();

            handles.push(scope.spawn(move || -> ShardResult {
                let mut shard_total: usize = 0;
                let mut shard_max_lsn: u64 = 0;
                let mut shard_ordered: Vec<OrderedEntry> = Vec::new();

                // Load this shard's base RDB. Cold wiring must survive the
                // swap AND be live for the incr replay below (replayed
                // DEL/FLUSH must tombstone the cold plane) — see
                // take_cold_wiring.
                if base_path.exists() {
                    let cold_wiring = take_cold_wiring(databases);
                    let load_result = crate::persistence::rdb::load(*databases, &base_path);
                    restore_cold_wiring(databases, cold_wiring);
                    match load_result {
                        Ok(n) => {
                            info!(
                                "AOF shard-{} base RDB loaded: {} keys from {}",
                                sid,
                                n,
                                base_path.display()
                            );
                            shard_total += n;
                        }
                        Err(e) => {
                            error!("AOF shard-{} base RDB load failed: {}", sid, e);
                            return Err(e);
                        }
                    }
                } else {
                    // Missing base is tolerable only when this shard's incr file is
                    // empty (or absent). Same invariant as `replay_multi_part`.
                    let incr_len =
                        std::fs::metadata(&incr_path).map(|m| m.len()).unwrap_or(0);
                    if incr_len > 0 {
                        return Err(crate::error::MoonError::from(
                            crate::error::AofError::RewriteFailed {
                                detail: format!(
                                    "AOF shard-{} base RDB missing at {} but incr {} is {} bytes; refusing to replay incr against empty state",
                                    sid,
                                    base_path.display(),
                                    incr_path.display(),
                                    incr_len,
                                ),
                            },
                        ));
                    }
                    warn!(
                        "AOF shard-{} base RDB not found: {} (incr empty, treating as fresh init)",
                        sid,
                        base_path.display()
                    );
                }

                // Replay this shard's framed incr file.
                if incr_path.exists() {
                    let data = std::fs::read(&incr_path).map_err(|e| {
                        crate::error::MoonError::from(crate::error::AofError::Io {
                            path: incr_path.clone(),
                            source: e,
                        })
                    })?;
                    if !data.is_empty() {
                        // moon#1277: judge expiry by the log's last write;
                        // a foreign segment positionally (R2 review of #1283).
                        let _clock = crate::persistence::replay::clock::pin_replay_clock_to_log(
                            &incr_path,
                            crate::persistence::replay::clock::LogFormat::Framed,
                        );
                        let (count, max_lsn, torn) = replay_incr_framed(
                            sid,
                            *databases,
                            &data,
                            engine.as_ref(),
                            &mut shard_ordered,
                        )
                        .map_err(|e| in_file(&incr_path, e))?;
                        drop(data);
                        // R2b round 3 F-B: cut a torn tail before this
                        // shard's writer appends behind it.
                        cut_torn_tail(&incr_path, torn)?;
                        info!(
                            "AOF shard-{} incr replayed: {} commands from {} (max lsn {})",
                            sid,
                            count,
                            incr_path.display(),
                            max_lsn
                        );
                        shard_total += count;
                        if max_lsn > shard_max_lsn {
                            shard_max_lsn = max_lsn;
                        }
                    }
                }

                Ok((shard_total, shard_max_lsn, shard_ordered))
            }));
        }

        // Collect results in shard order.
        handles
            .into_iter()
            .map(|h| {
                h.join().unwrap_or_else(|_| {
                    Err(crate::error::MoonError::from(
                        crate::error::AofError::RewriteFailed {
                            detail: "replay_per_shard worker thread panicked".to_owned(),
                        },
                    ))
                })
            })
            .collect()
    });

    // Merge per-shard results.
    let mut total: usize = 0;
    let mut global_max_lsn: u64 = 0;
    let mut ordered_entries: Vec<OrderedEntry> = Vec::new();

    for result in shard_results {
        let (shard_total, shard_max_lsn, shard_ordered) = result?;
        total += shard_total;
        if shard_max_lsn > global_max_lsn {
            global_max_lsn = shard_max_lsn;
        }
        ordered_entries.extend(shard_ordered);
    }

    Ok((total, global_max_lsn, ordered_entries))
}

/// Merge-replay `OrderedAcrossShards` entries collected across all shards
/// in global LSN order (RFC § 2 Rule 2).
///
/// `entries` is sorted by `lsn` ascending, then each entry is dispatched
/// against its origin shard's databases — the per-shard partition is
/// preserved because each `OrderedEntry` carries the `shard_id` it was
/// read from. This guarantees that a cross-shard atomic operation
/// committed at LSN N is replayed as a coherent group (every
/// shard's portion at LSN N is applied before any shard's LSN N+1 work).
///
/// **Crash-time atomicity:** if a cross-shard commit was mid-write at
/// crash time, some shards may have the LSN-N entry while others don't.
/// Step 5 ships the merge mechanism only; detecting partial commits and
/// performing the corresponding rollback is left to the future cross-shard
/// TXN consumer — `replay_ordered_merge` currently best-effort-applies
/// whichever entries survived. A `warn!` is emitted when the entry count
/// per LSN is uneven across shards so operators have a forensic trail.
///
/// **Today's emitters:** none in production code. The path is exercised
/// by tests so the round-trip wiring is verified end-to-end and ready for
/// future use.
pub fn replay_ordered_merge(
    per_shard_databases: &mut [&mut [crate::storage::Database]],
    mut entries: Vec<OrderedEntry>,
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
) -> Result<usize, crate::error::MoonError> {
    use crate::protocol::{Frame, ParseConfig, parse};
    use bytes::BytesMut;

    if entries.is_empty() {
        return Ok(0);
    }

    entries.sort_by_key(|e| e.lsn);

    // Per-LSN cardinality audit: detect torn cross-shard commits.
    //
    // A "torn" commit is one where LSN N appears in fewer shard files than
    // the maximum cardinality seen for any other LSN in this batch. Applying
    // partial entries violates atomicity — if the write was interrupted mid-
    // commit (e.g., crash between shard-0 and shard-1 writes), replaying only
    // the shard-0 portion produces an inconsistent state that cannot be
    // compensated. DROP the entire torn LSN instead of applying partial data.
    //
    // NOTE: "torn" detection is heuristic — it compares each LSN's count
    // against the maximum cardinality observed. An LSN that legitimately spans
    // fewer shards (e.g. single-shard ordered op) can only occur if the batch
    // is heterogeneous. Production emitters (future cross-shard TXN) must
    // guarantee uniform cardinality per LSN, so this heuristic is correct for
    // all currently-reachable code paths.
    let mut counts: std::collections::BTreeMap<u64, usize> = std::collections::BTreeMap::new();
    for e in &entries {
        *counts.entry(e.lsn).or_insert(0) += 1;
    }
    let max_count = counts.values().copied().max().unwrap_or(0);
    let mut torn_lsns: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for (&lsn, &n) in &counts {
        if n < max_count {
            warn!(
                "OrderedAcrossShards LSN {} appears in only {} of {} shard files; \
                 torn cross-shard commit detected — dropping entry for atomicity",
                lsn, n, max_count
            );
            torn_lsns.insert(lsn);
        }
    }

    let config = ParseConfig::default();
    let mut replayed: usize = 0;

    for entry in entries {
        // Skip entries belonging to a torn (partially-written) commit.
        if torn_lsns.contains(&entry.lsn) {
            continue;
        }
        let shard_idx = entry.shard_id as usize;
        if shard_idx >= per_shard_databases.len() {
            return Err(crate::error::MoonError::from(
                crate::error::AofError::RewriteFailed {
                    detail: format!(
                        "OrderedAcrossShards entry references shard {} but only {} shards present",
                        entry.shard_id,
                        per_shard_databases.len()
                    ),
                },
            ));
        }
        let mut buf = BytesMut::from(entry.bytes.as_ref());
        match parse::parse(&mut buf, &config) {
            Ok(Some(Frame::Array(arr))) if !arr.is_empty() => {
                let cmd = match &arr[0] {
                    Frame::BulkString(s) => s.as_ref(),
                    Frame::SimpleString(s) => s.as_ref(),
                    _ => {
                        return Err(crate::error::MoonError::from(
                            crate::error::AofError::RewriteFailed {
                                detail: format!(
                                    "OrderedAcrossShards entry at lsn {} has non-string command frame",
                                    entry.lsn
                                ),
                            },
                        ));
                    }
                };
                let mut selected_db: usize = 0;
                let databases = &mut *per_shard_databases[shard_idx];
                engine.replay_command(databases, cmd, &arr[1..], &mut selected_db);
                // moon#1300: an ordered entry is one shard's one record, never
                // a transaction block (a TXN writes on its own shard only);
                // a block one names is closed here, on that shard.
                engine.finish_log(databases);
                replayed += 1;
            }
            other => {
                return Err(crate::error::MoonError::from(
                    crate::error::AofError::RewriteFailed {
                        detail: format!(
                            "OrderedAcrossShards entry at lsn {} on shard {} did not parse as RESP array: {:?}",
                            entry.lsn,
                            entry.shard_id,
                            other.map(|_| ()).err()
                        ),
                    },
                ));
            }
        }
    }

    Ok(replayed)
}

/// Fuzzing entry points into the readers above (moon#1283).
#[cfg(any(test, feature = "fuzzing"))]
#[path = "shard_replay_fuzz.rs"]
pub mod fuzz;

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir() -> PathBuf {
        // Use a global atomic counter so parallel test threads (cargo test runs
        // unit tests in parallel) never produce the same directory name even
        // when PID and nanosecond clock resolution are the same for two threads.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!(
            "moon-aof-manifest-test-replay-{}-{}",
            std::process::id(),
            n,
        ));
        fs::create_dir_all(&d).expect("temp dir create");
        d
    }

    // -- Step 4 (per-shard replay) tests ---------------------------------

    fn frame_entry(lsn: u64, resp: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(12 + resp.len());
        buf.extend_from_slice(&lsn.to_le_bytes());
        buf.extend_from_slice(&(resp.len() as u32).to_le_bytes());
        buf.extend_from_slice(resp);
        buf
    }

    /// Minimal `CommandReplayEngine` that records (lsn-implicit-via-order, cmd
    /// name) calls without touching real storage. Tests use this to assert
    /// the framed parser hands the right command sequence to the engine.
    struct RecordingEngine {
        calls: std::cell::RefCell<Vec<String>>,
    }

    impl RecordingEngine {
        fn new() -> Self {
            Self {
                calls: std::cell::RefCell::new(Vec::new()),
            }
        }
    }

    impl crate::persistence::replay::CommandReplayEngine for RecordingEngine {
        fn replay_command(
            &self,
            _databases: &mut [crate::storage::Database],
            cmd: &[u8],
            _args: &[crate::protocol::Frame],
            _selected_db: &mut usize,
        ) -> crate::persistence::replay::ReplayRoute {
            self.calls
                .borrow_mut()
                .push(String::from_utf8_lossy(cmd).into_owned());
            crate::persistence::replay::ReplayRoute::Keyspace
        }
    }

    #[test]
    fn replay_incr_framed_decodes_lsn_and_resp() {
        // Two framed entries: PING and DBSIZE (no args, both small RESP arrays).
        let mut bytes = frame_entry(7, b"*1\r\n$4\r\nPING\r\n");
        bytes.extend_from_slice(&frame_entry(11, b"*1\r\n$6\r\nDBSIZE\r\n"));

        let mut dbs: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let engine = RecordingEngine::new();
        let mut ordered: Vec<OrderedEntry> = Vec::new();
        let (count, max_lsn, _) =
            replay_incr_framed(0, &mut dbs, &bytes, &engine, &mut ordered).expect("framed replay");
        assert!(ordered.is_empty(), "no ordered entries in this stream");

        assert_eq!(count, 2);
        // F5: max_lsn is the NEXT-FREE offset = max(lsn + len) = max(7+14, 11+16) = 27.
        assert_eq!(max_lsn, 27);
        let calls = engine.calls.borrow();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], "PING");
        assert_eq!(calls[1], "DBSIZE");
    }

    #[test]
    fn replay_incr_framed_max_lsn_is_next_free_offset() {
        // F5: replay must return the next-free replication offset (entry end =
        // start LSN + RESP byte length), not the START LSN of the last entry.
        // `issue_lsn` hands out the offset BEFORE adding the entry's length, so
        // seeding `master_repl_offset` with a start LSN reissues the last
        // pre-crash entry's LSN — breaking lsn->entry uniqueness (RFC § 2 Rule 3).
        let ping = b"*1\r\n$4\r\nPING\r\n"; // 14 bytes
        let dbsize = b"*1\r\n$6\r\nDBSIZE\r\n"; // 16 bytes
        // Cumulative LSNs as the writer issues them: each entry starts at the
        // previous entry's end.
        let mut bytes = frame_entry(100, ping);
        bytes.extend_from_slice(&frame_entry(100 + ping.len() as u64, dbsize));

        let mut dbs: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let engine = RecordingEngine::new();
        let mut ordered: Vec<OrderedEntry> = Vec::new();
        let (_count, max_lsn, _) =
            replay_incr_framed(0, &mut dbs, &bytes, &engine, &mut ordered).expect("framed replay");

        // Last entry: start 114 + len 16 = 130. The next write MUST get >= 130.
        let expected_next_free = 100 + ping.len() as u64 + dbsize.len() as u64;
        assert_eq!(expected_next_free, 130);
        assert_eq!(
            max_lsn, expected_next_free,
            "max_lsn must be the next-free offset (entry end), not the last start LSN"
        );
    }

    #[test]
    fn replay_incr_framed_skips_zero_length_barrier_record() {
        // H1-BARRIER defense-in-depth: a len=0 record (an fsync barrier that
        // leaked to disk) carries no RESP command. Replay must SKIP it and
        // continue — not reject the file as corrupt, which would brick boot.
        let mut bytes = frame_entry(7, b"*1\r\n$4\r\nPING\r\n");
        bytes.extend_from_slice(&frame_entry(0, b"")); // barrier: lsn=0, len=0
        bytes.extend_from_slice(&frame_entry(21, b"*1\r\n$6\r\nDBSIZE\r\n"));

        let mut dbs: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let engine = RecordingEngine::new();
        let mut ordered: Vec<OrderedEntry> = Vec::new();
        let (count, max_lsn, _) = replay_incr_framed(0, &mut dbs, &bytes, &engine, &mut ordered)
            .expect("zero-length record must not be treated as corruption");

        assert_eq!(count, 2, "both real commands replay; barrier is skipped");
        assert_eq!(max_lsn, 37, "next-free offset = 21 + 16");
        let calls = engine.calls.borrow();
        assert_eq!(&*calls, &["PING".to_string(), "DBSIZE".to_string()]);
    }

    #[test]
    fn replay_incr_framed_truncated_header_is_crash_eof() {
        // One valid entry, then a partial 5-byte header (crash mid-write).
        let mut bytes = frame_entry(3, b"*1\r\n$4\r\nPING\r\n");
        bytes.extend_from_slice(&[0u8; 5]);

        let mut dbs: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let engine = RecordingEngine::new();
        let mut ordered: Vec<OrderedEntry> = Vec::new();
        let (count, max_lsn, torn) = replay_incr_framed(0, &mut dbs, &bytes, &engine, &mut ordered)
            .expect("truncated-header is EOF");

        assert_eq!(count, 1);
        // R2b round 3 F-B: reported, so the boot cuts it.
        assert_eq!(torn, Some(TornTail { offset: 26, len: 5 }));
        // F5: next-free offset = PING entry start 3 + RESP len 14 = 17.
        assert_eq!(max_lsn, 17);
    }

    #[test]
    fn replay_incr_framed_truncated_payload_is_crash_eof() {
        // Header declares 14 bytes of RESP but only 5 actually present.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&5u64.to_le_bytes());
        bytes.extend_from_slice(&14u32.to_le_bytes());
        bytes.extend_from_slice(b"*1\r\n$"); // 5 bytes, payload truncated

        let mut dbs: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let engine = RecordingEngine::new();
        let mut ordered: Vec<OrderedEntry> = Vec::new();
        let (count, max_lsn, torn) = replay_incr_framed(0, &mut dbs, &bytes, &engine, &mut ordered)
            .expect("truncated-payload is EOF");

        assert_eq!(count, 0);
        assert_eq!(torn, Some(TornTail { offset: 0, len: 17 }));
        assert_eq!(max_lsn, 0);
    }

    #[test]
    fn replay_incr_framed_complete_but_corrupt_payload_errors() {
        // Header declares 4 bytes, payload is 4 bytes of garbage that won't
        // parse as a RESP frame.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&4u32.to_le_bytes());
        bytes.extend_from_slice(b"XXXX");

        let mut dbs: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let engine = RecordingEngine::new();
        let mut ordered: Vec<OrderedEntry> = Vec::new();
        let err = replay_incr_framed(0, &mut dbs, &bytes, &engine, &mut ordered)
            .expect_err("complete-but-corrupt should error");
        let msg = format!("{err}");
        assert!(
            msg.contains("framed"),
            "error should mention framed context, got: {msg}"
        );
    }

    #[test]
    fn replay_per_shard_round_trips_two_shards() {
        let dir = temp_dir();
        let manifest = AofManifest::initialize_multi(&dir, 2).expect("initialize_multi 2 shards");

        // Hand-author framed incr files: shard-0 SETs k0/v0 at lsn=10,
        // shard-1 SETs k1/v1 at lsn=20.
        let set_k0 = frame_entry(10, b"*3\r\n$3\r\nSET\r\n$2\r\nk0\r\n$2\r\nv0\r\n");
        let set_k1 = frame_entry(20, b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n");
        fs::write(manifest.shard_incr_path(0), &set_k0).expect("write shard-0 incr");
        fs::write(manifest.shard_incr_path(1), &set_k1).expect("write shard-1 incr");

        // Two independent shard database vectors.
        let mut shard0: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let mut shard1: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];

        let (total, global_max_lsn, ordered) = {
            let mut slices: Vec<&mut [crate::storage::Database]> = vec![&mut shard0, &mut shard1];
            replay_per_shard(
                &mut slices,
                &manifest,
                &(|| {
                    Box::new(crate::persistence::replay::DispatchReplayEngine::new())
                        as Box<dyn crate::persistence::replay::CommandReplayEngine + Send>
                }),
            )
            .expect("per-shard replay")
        };

        assert_eq!(total, 2, "two SETs replayed");
        // F5: global_max_lsn = max next-free offset across shards. shard-1 SET
        // is 29 RESP bytes at lsn 20 → next-free 49 (> shard-0's 10+29=39).
        assert_eq!(
            global_max_lsn, 49,
            "global max lsn = max(shard next-free offsets)"
        );
        assert!(ordered.is_empty(), "no ordered entries in this stream");

        // Each shard's DB now holds its key (and only its key).
        assert!(shard0[0].len() >= 1, "shard 0 has k0");
        assert!(shard1[0].len() >= 1, "shard 1 has k1");

        fs::remove_dir_all(&dir).ok();
    }

    /// R2b round 3 F-B: a torn framed record at the end of a shard's incr
    /// is cut before that shard's writer appends; a later append replays.
    /// The single-shard RESP incr is cut the same way (`replay_multi_part`).
    #[test]
    fn a_torn_incr_tail_is_cut_so_the_next_append_replays() {
        let dir = temp_dir();
        let manifest = AofManifest::initialize_multi(&dir, 2).expect("initialize_multi 2 shards");
        let set_k0 = frame_entry(10, b"*3\r\n$3\r\nSET\r\n$2\r\nk0\r\n$2\r\nv0\r\n");
        let mut torn = set_k0.clone();
        // A header declaring 400 bytes, 7 of them written.
        torn.extend_from_slice(&39u64.to_le_bytes());
        torn.extend_from_slice(&400u32.to_le_bytes());
        torn.extend_from_slice(b"*3\r\n$3\r");
        fs::write(manifest.shard_incr_path(0), &torn).expect("write shard-0 incr");
        fs::write(manifest.shard_incr_path(1), b"").expect("write shard-1 incr");
        let engine = || {
            Box::new(crate::persistence::replay::DispatchReplayEngine::new())
                as Box<dyn crate::persistence::replay::CommandReplayEngine + Send>
        };
        let replay = |manifest: &AofManifest| {
            let mut shard0 = vec![crate::storage::Database::new()];
            let mut shard1 = vec![crate::storage::Database::new()];
            {
                let mut slices: Vec<&mut [crate::storage::Database]> =
                    vec![&mut shard0, &mut shard1];
                replay_per_shard(&mut slices, manifest, &engine).expect("per-shard replay");
            }
            shard0
        };
        replay(&manifest);
        assert_eq!(
            fs::read(manifest.shard_incr_path(0)).unwrap(),
            set_k0,
            "cut at the torn record"
        );
        let mut later = fs::OpenOptions::new()
            .append(true)
            .open(manifest.shard_incr_path(0))
            .unwrap();
        std::io::Write::write_all(
            &mut later,
            &frame_entry(39, b"*3\r\n$3\r\nSET\r\n$2\r\nk9\r\n$2\r\nv9\r\n"),
        )
        .unwrap();
        drop(later);
        let mut shard0 = replay(&manifest);
        assert!(shard0[0].get(b"k9").is_some(), "the later append replays");
        fs::remove_dir_all(&dir).ok();
    }

    /// Mid-stream corruption of an incr names the file and refuses.
    #[test]
    fn a_corrupt_incr_names_its_file() {
        let dir = temp_dir();
        let manifest = AofManifest::initialize_multi(&dir, 2).expect("initialize_multi");
        let mut bytes = frame_entry(1, b"XXXX");
        bytes.extend_from_slice(&frame_entry(5, b"*1\r\n$4\r\nPING\r\n"));
        fs::write(manifest.shard_incr_path(0), &bytes).unwrap();
        let mut shard0 = vec![crate::storage::Database::new()];
        let mut shard1 = vec![crate::storage::Database::new()];
        let mut slices: Vec<&mut [crate::storage::Database]> = vec![&mut shard0, &mut shard1];
        let err = replay_per_shard(
            &mut slices,
            &manifest,
            &(|| {
                Box::new(crate::persistence::replay::DispatchReplayEngine::new())
                    as Box<dyn crate::persistence::replay::CommandReplayEngine + Send>
            }),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("shard-0"), "{err}");
        assert!(err.contains("refuses a damaged AOF"), "{err}");
        assert_eq!(fs::read(manifest.shard_incr_path(0)).unwrap(), bytes);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn replay_per_shard_rejects_shard_count_mismatch() {
        let dir = temp_dir();
        let manifest = AofManifest::initialize_multi(&dir, 2).expect("initialize_multi 2 shards");

        // Only one slice — manifest says 2.
        let mut shard0: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let mut slices: Vec<&mut [crate::storage::Database]> = vec![&mut shard0];

        let err = replay_per_shard(
            &mut slices,
            &manifest,
            &(|| {
                Box::new(crate::persistence::replay::DispatchReplayEngine::new())
                    as Box<dyn crate::persistence::replay::CommandReplayEngine + Send>
            }),
        )
        .expect_err("shard count mismatch must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("shard-count mismatch"),
            "error message should call out the mismatch, got: {msg}"
        );

        fs::remove_dir_all(&dir).ok();
    }

    /// FIX-W3-1: parallel per-shard replay must produce identical results to
    /// sequential replay. N=4 shards, one key per shard.
    ///
    /// Test gate: correctness (same total/max_lsn/key distribution as sequential).
    /// Wall-time comparison is flaky in CI and omitted.
    #[test]
    fn replay_per_shard_parallel_matches_sequential() {
        let dir = temp_dir();
        let n_shards: u16 = 4;
        let manifest =
            AofManifest::initialize_multi(&dir, n_shards).expect("initialize_multi 4 shards");

        // Each shard gets one SET at lsn = shard_id * 10 + 10.
        for sid in 0..n_shards {
            let lsn = (sid as u64 + 1) * 10;
            let key = format!("k{sid}");
            let val = format!("v{sid}");
            let resp = format!(
                "*3\r\n$3\r\nSET\r\n${klen}\r\n{key}\r\n${vlen}\r\n{val}\r\n",
                klen = key.len(),
                vlen = val.len(),
            );
            let entry = frame_entry(lsn, resp.as_bytes());
            fs::write(manifest.shard_incr_path(sid), &entry).expect("write shard incr");
        }

        let mut shards: Vec<Vec<crate::storage::Database>> = (0..n_shards as usize)
            .map(|_| vec![crate::storage::Database::new()])
            .collect();

        let engine_factory = || {
            Box::new(crate::persistence::replay::DispatchReplayEngine::new())
                as Box<dyn crate::persistence::replay::CommandReplayEngine + Send>
        };
        let (total, global_max_lsn, ordered) = {
            let mut slices: Vec<&mut [crate::storage::Database]> =
                shards.iter_mut().map(|s| s.as_mut_slice()).collect();
            replay_per_shard(&mut slices, &manifest, &engine_factory)
                .expect("parallel per-shard replay")
        };

        assert_eq!(total, n_shards as usize, "one SET per shard = N total");
        // F5: global_max_lsn = highest shard's NEXT-FREE offset. The highest
        // shard (sid=N-1) SETs at lsn N*10 with a 29-byte RESP → next-free
        // N*10 + 29 (here 40 + 29 = 69), not the bare start LSN.
        assert_eq!(
            global_max_lsn,
            n_shards as u64 * 10 + 29,
            "global max lsn = highest shard next-free offset"
        );
        assert!(ordered.is_empty(), "no ordered entries");

        // Each shard must have exactly one key.
        for (sid, shard) in shards.iter().enumerate() {
            assert_eq!(
                shard[0].len(),
                1,
                "shard {} must have exactly 1 key after parallel replay",
                sid
            );
        }

        fs::remove_dir_all(&dir).ok();
    }

    // -- Step 5 (OrderedAcrossShards merge) tests ------------------------

    /// Frame an ordered entry: same on-disk layout as `frame_entry`, with
    /// the high bit of LSN set.
    fn frame_ordered(lsn: u64, resp: &[u8]) -> Vec<u8> {
        assert_eq!(
            lsn & crate::persistence::aof::ORDERED_LSN_FLAG,
            0,
            "test helper expects raw lsn without the ordered flag"
        );
        let tagged = lsn | crate::persistence::aof::ORDERED_LSN_FLAG;
        let mut buf = Vec::with_capacity(12 + resp.len());
        buf.extend_from_slice(&tagged.to_le_bytes());
        buf.extend_from_slice(&(resp.len() as u32).to_le_bytes());
        buf.extend_from_slice(resp);
        buf
    }

    #[test]
    fn replay_incr_framed_buffers_ordered_entries() {
        // Mix: normal PING, then an ordered SET, then normal DBSIZE.
        let mut bytes = frame_entry(5, b"*1\r\n$4\r\nPING\r\n");
        bytes.extend_from_slice(&frame_ordered(
            8,
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
        ));
        bytes.extend_from_slice(&frame_entry(12, b"*1\r\n$6\r\nDBSIZE\r\n"));

        let mut dbs: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let engine = RecordingEngine::new();
        let mut ordered: Vec<OrderedEntry> = Vec::new();
        let (count, max_lsn, _) = replay_incr_framed(3, &mut dbs, &bytes, &engine, &mut ordered)
            .expect("framed replay with ordered");

        assert_eq!(count, 2, "two inline entries dispatched (PING, DBSIZE)");
        // F5: max_lsn is the next-free offset across inline AND ordered. The
        // ordered SET (27 RESP bytes) at lsn 8 → next-free 35, exceeding the
        // PING (5+14=19) and DBSIZE (12+16=28) ends.
        assert_eq!(
            max_lsn, 35,
            "max LSN = next-free offset across inline and ordered"
        );
        assert_eq!(ordered.len(), 1, "one entry buffered as ordered");
        let buffered = &ordered[0];
        assert_eq!(buffered.shard_id, 3, "shard_id forwarded");
        assert_eq!(buffered.lsn, 8, "buffered LSN has the high bit masked off");
        let calls = engine.calls.borrow();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], "PING");
        assert_eq!(calls[1], "DBSIZE", "ordered SET was NOT dispatched inline");
    }

    #[test]
    fn replay_ordered_merge_sorts_by_lsn_across_shards() {
        use crate::persistence::replay::DispatchReplayEngine;

        // Three ordered entries across two shards, deliberately out of LSN
        // order on the wire so the merge step has work to do.
        let entries = vec![
            OrderedEntry {
                shard_id: 1,
                lsn: 30,
                bytes: bytes::Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$2\r\nb1\r\n$1\r\n3\r\n"),
            },
            OrderedEntry {
                shard_id: 0,
                lsn: 10,
                bytes: bytes::Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$2\r\na1\r\n$1\r\n1\r\n"),
            },
            OrderedEntry {
                shard_id: 0,
                lsn: 20,
                bytes: bytes::Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$2\r\na2\r\n$1\r\n2\r\n"),
            },
        ];

        let mut shard0: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let mut shard1: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let replayed = {
            let mut slices: Vec<&mut [crate::storage::Database]> = vec![&mut shard0, &mut shard1];
            replay_ordered_merge(&mut slices, entries, &DispatchReplayEngine::new())
                .expect("ordered merge replay")
        };

        assert_eq!(replayed, 3);
        assert!(shard0[0].len() >= 2, "shard 0 received a1 + a2");
        assert!(shard1[0].len() >= 1, "shard 1 received b1");
    }

    #[test]
    fn replay_ordered_merge_empty_returns_zero() {
        use crate::persistence::replay::DispatchReplayEngine;

        let mut shard0: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let mut slices: Vec<&mut [crate::storage::Database]> = vec![&mut shard0];
        let replayed = replay_ordered_merge(&mut slices, Vec::new(), &DispatchReplayEngine::new())
            .expect("empty merge ok");
        assert_eq!(replayed, 0);
    }

    /// FIX-W3-3: torn cross-shard commit must be DROPPED entirely, not partially applied.
    ///
    /// Synthesize a 2-shard AOF where LSN 100 appears on shard 0 only (N=1
    /// of K=2 expected). After replay, shard 0 must NOT have the key written
    /// by the LSN-100 entry (it was dropped for atomicity).
    #[test]
    fn replay_ordered_merge_drops_torn_commit() {
        use crate::persistence::replay::DispatchReplayEngine;

        // Two shards, two complete entries at LSN 10 (one per shard) — these
        // should succeed. LSN 100 appears only on shard 0 (torn) — must be dropped.
        let entries = vec![
            // Complete pair: LSN 10 on both shards
            OrderedEntry {
                shard_id: 0,
                lsn: 10,
                bytes: bytes::Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$2\r\nc0\r\n$1\r\n1\r\n"),
            },
            OrderedEntry {
                shard_id: 1,
                lsn: 10,
                bytes: bytes::Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$2\r\nc1\r\n$1\r\n1\r\n"),
            },
            // Torn entry: LSN 100 only on shard 0, not shard 1
            OrderedEntry {
                shard_id: 0,
                lsn: 100,
                bytes: bytes::Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$5\r\ntorn0\r\n$1\r\nv\r\n"),
            },
        ];

        let mut shard0: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let mut shard1: Vec<crate::storage::Database> = vec![crate::storage::Database::new()];
        let replayed = {
            let mut slices: Vec<&mut [crate::storage::Database]> = vec![&mut shard0, &mut shard1];
            replay_ordered_merge(&mut slices, entries, &DispatchReplayEngine::new())
                .expect("ordered merge replay")
        };

        // The torn LSN-100 entry must NOT be applied (dropped for atomicity).
        assert_eq!(replayed, 2, "only the complete LSN-10 pair is replayed");
        assert_eq!(
            shard0[0].len(),
            1,
            "shard-0 only has the complete LSN-10 key; torn LSN-100 entry must not be applied"
        );
        // Verify the torn key is absent
        assert!(
            shard0[0].get(b"torn0").is_none(),
            "torn shard-0 entry (LSN 100) must NOT be applied"
        );
    }

    #[test]
    fn ordered_entry_lsn_flag_set_via_try_send_append_ordered() {
        use crate::persistence::aof::{AofMessage, AofWriterPool, ORDERED_LSN_FLAG};
        use crate::runtime::channel;

        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        // Raw lsn = 42; high bit must end up set on the receive side.
        pool.try_send_append_ordered(0, 42, 0, bytes::Bytes::from_static(b"x"));
        let msg = rx0.try_recv().expect("ordered append delivered");
        match msg {
            AofMessage::Append { lsn, .. } => {
                assert_eq!(
                    lsn & ORDERED_LSN_FLAG,
                    ORDERED_LSN_FLAG,
                    "ordered flag set on lsn"
                );
                assert_eq!(
                    lsn & !ORDERED_LSN_FLAG,
                    42,
                    "low bits preserve the original lsn"
                );
            }
            _ => panic!("expected Append"),
        }
    }

    // -----------------------------------------------------------------------
    // FIX-W2-1: cleanup_orphans must recurse into shard-N/ subdirectories
    // -----------------------------------------------------------------------
}
