//! Immutable-segment consolidation (the GraphUnion merge path).
//!
//! Split out of the former single-file `compaction.rs` (moon#479, file-size
//! ceiling); `merge_immutable` and `merge_graph_union` are unchanged.

use std::path::Path;
use std::sync::Arc;

use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::hnsw::build::HnswBuilder;
use crate::vector::persistence::segment_io;
use crate::vector::segment::compaction::recall::verify_merge_recall;
use crate::vector::segment::compaction::{
    CompactionError, HNSW_EF_CONSTRUCTION, HNSW_M, MERGE_MEMORY_CEILING, MergeMode,
};
use crate::vector::segment::immutable::{ImmutableSegment, MvccHeader};
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::turbo_quant::sq8::{SQ8_PARAMS_BYTES, decode_sq8, sq8_params};

/// Merge N immutable segments into one using the specified merge mode.
///
/// # Graph-union algorithm (default)
///
/// 1. Walk all live entries across input segments; deduplicate by `key_hash`
///    (highest `insert_lsn` wins for duplicates).
/// 2. Concatenate TQ codes verbatim — no decode/re-encode.
/// 3. Decode TQ → centroid vectors (FWHT-rotated) for HNSW build oracle.
/// 4. Build a single HNSW over the union with the centroid distance function.
/// 5. BFS-reorder TQ buffer (same as compact pipeline).
/// 6. Build merged `ImmutableSegment`.
/// 7. Verify recall ≥ `recall_tolerance` against the fan-out pre-merge results.
///    Returns `Err(RecallTooLow)` if gate fails; old segments remain intact.
///
/// # Overlap handling
///
/// When the same `key_hash` appears in more than one segment (e.g., after a
/// re-insert), the entry with the **highest `insert_lsn`** is kept and all
/// others are dropped. Tombstoned entries (`delete_lsn != 0`) are skipped.
///
/// # Memory ceiling
///
/// Refuses to merge when the estimated live-vector TQ code bytes plus the
/// HNSW graph overhead would exceed `MERGE_MEMORY_CEILING`.
///
/// `persist`: when `Some((dir, segment_id))`, writes the merged segment to
/// disk (staged + atomic rename, see [`segment_io::write_immutable_segment_staged`])
/// after a successful build, and tags the returned segment with `segment_id`
/// via [`ImmutableSegment::with_disk_segment_id`] (B2, durability).
pub fn merge_immutable(
    segments: &[Arc<ImmutableSegment>],
    collection: &Arc<CollectionMetadata>,
    seed: u64,
    mode: MergeMode,
    recall_tolerance: f32,
    persist: Option<(&Path, u64)>,
) -> Result<ImmutableSegment, CompactionError> {
    match mode {
        MergeMode::None => Err(CompactionError::EmptySegment), // caller should not call
        MergeMode::GraphUnion => {
            merge_graph_union(segments, collection, seed, recall_tolerance, persist)
        }
        MergeMode::KeepRaw => {
            // KeepRaw: fall back to graph-union if no raw f32 available (warn only).
            // Full raw-vector path requires raw_f32 stored on ImmutableSegment (TODO P2.5).
            tracing::warn!(
                "MERGE_MODE=keep_raw: raw f32 not yet persisted on ImmutableSegment; \
                 falling back to graph-union. Add KEEP_RAW sidecar in P2.5."
            );
            merge_graph_union(segments, collection, seed, recall_tolerance, persist)
        }
    }
}

/// Core graph-union merge implementation.
///
/// Builds a single HNSW graph over the union of live entries from all input
/// segments. TQ codes are concatenated verbatim; only graph topology is rebuilt.
fn merge_graph_union(
    segments: &[Arc<ImmutableSegment>],
    collection: &Arc<CollectionMetadata>,
    seed: u64,
    recall_tolerance: f32,
    persist: Option<(&Path, u64)>,
) -> Result<ImmutableSegment, CompactionError> {
    if segments.is_empty() {
        return Err(CompactionError::EmptySegment);
    }

    let padded = collection.padded_dimension as usize;
    let dim = collection.dimension as usize;
    // SQ8 stores `dim` u8 codes + an 8-byte (min, scale) trailer (= dim + 8),
    // sized by the true dimension. TQ stores padded nibble-packed codes + a
    // 4-byte norm trailer. Deriving the stride from the collection's TQ helper
    // (padded/2 + 4) would mis-stride the dim+8 SQ8 buffer and corrupt the merge.
    let is_sq8 = collection.quantization == QuantizationConfig::Sq8;
    let bytes_per_code = if is_sq8 {
        dim + SQ8_PARAMS_BYTES
    } else {
        collection.bytes_per_code_per_vector() as usize
    };
    // Length of the code portion (excluding the trailer): `dim` for SQ8 (8-byte
    // trailer), `bytes_per_code - 4` for TQ (4-byte norm trailer).
    let code_len = if is_sq8 { dim } else { bytes_per_code - 4 };

    // ── Step 1: Collect live entries, deduplicate by key_hash ────────────────
    // Map key_hash → (insert_lsn, global_id, tq_code_bytes, qjl_bytes, residual_norm,
    //                  sub_centroid_bytes, raw_f16_bytes)
    #[allow(clippy::type_complexity)]
    let mut by_key_hash: std::collections::HashMap<
        u64,
        (u64, u32, Vec<u8>, Vec<u8>, f32, Vec<u8>, Vec<u16>),
    > = std::collections::HashMap::new();

    let qjl_bpv = {
        // QJL bytes per vector: derived from first segment or computed from padded_dim.
        // All segments in the same index use the same qjl layout.
        let first = &segments[0];
        let headers = first.mvcc_headers();
        if headers.is_empty() {
            0usize
        } else {
            let total_qjl = first.qjl_bytes();
            if total_qjl > 0 && first.total_count() > 0 {
                total_qjl / first.total_count() as usize
            } else {
                0
            }
        }
    };
    let sub_bpv = (padded + 7) / 8;

    // HQ-1: the merged segment keeps the exact-rerank sidecar only when every
    // surviving entry can supply its f16 vector (all-or-nothing — a partial
    // sidecar would silently mix exact and ADC distances within one segment).
    let mut all_have_raw = true;

    for seg in segments {
        let tq_buf = seg.vectors_tq().as_slice();
        let headers = seg.mvcc_headers();
        let seg_raw = seg.raw_f16();

        for hdr in headers {
            // Skip tombstoned entries.
            if hdr.delete_lsn != 0 {
                continue;
            }

            // Get TQ code for this entry.
            // Codes are stored in BFS order; bfs_pos = internal_id (after compaction BFS reorder).
            let bfs_pos = hdr.internal_id as usize;
            let code_offset = bfs_pos * bytes_per_code;
            if code_offset + bytes_per_code > tq_buf.len() {
                continue; // defensive: skip out-of-bounds
            }
            let code_bytes = tq_buf[code_offset..code_offset + bytes_per_code].to_vec();
            // SQ8 has no residual-norm trailer: its (min, scale) live inside the
            // slot and are read directly during search, so residual_norms is unused.
            let norm = if is_sq8 {
                0.0
            } else {
                let norm_bytes = &code_bytes[code_len..];
                f32::from_le_bytes([norm_bytes[0], norm_bytes[1], norm_bytes[2], norm_bytes[3]])
            };

            // QJL bytes for this entry.
            let qjl_bytes = if qjl_bpv > 0 {
                seg.qjl_bytes_for(bfs_pos, qjl_bpv)
            } else {
                Vec::new()
            };

            // Sub-centroid sign bytes.
            let sub_bytes = seg.sub_centroid_bytes_for(bfs_pos, sub_bpv);

            // Exact-rerank sidecar slice for this entry (HQ-1).
            let raw_bytes: Vec<u16> =
                match seg_raw.and_then(|r| r.get(bfs_pos * dim..(bfs_pos + 1) * dim)) {
                    Some(slice) => slice.to_vec(),
                    None => {
                        all_have_raw = false;
                        Vec::new()
                    }
                };

            // Deduplicate: keep highest insert_lsn.
            let entry = by_key_hash.entry(hdr.key_hash).or_insert((
                0,
                0,
                Vec::new(),
                Vec::new(),
                0.0,
                Vec::new(),
                Vec::new(),
            ));
            if hdr.insert_lsn >= entry.0 {
                *entry = (
                    hdr.insert_lsn,
                    hdr.global_id,
                    code_bytes,
                    qjl_bytes,
                    norm,
                    sub_bytes,
                    raw_bytes,
                );
            }
        }
    }

    let n = by_key_hash.len();
    if n == 0 {
        return Err(CompactionError::EmptySegment);
    }

    // ── Memory ceiling check ─────────────────────────────────────────────────
    // Estimate: TQ codes + HNSW layer-0 (M0=32 nodes * 4 bytes * n) + overhead.
    let estimated_bytes = n * bytes_per_code + n * 32 * 4 + n * sub_bpv + n * qjl_bpv;
    if estimated_bytes > MERGE_MEMORY_CEILING {
        return Err(CompactionError::PersistFailed(format!(
            "merge union would require ~{estimated_bytes} bytes > {MERGE_MEMORY_CEILING} ceiling; \
             reduce index size or use larger COMPACT_THRESHOLD"
        )));
    }

    // ── Step 2: Lay out entries in deterministic order ───────────────────────
    // Sort by (insert_lsn asc, key_hash asc) for determinism.
    #[allow(clippy::type_complexity)]
    let mut entries: Vec<(u64, u32, Vec<u8>, Vec<u8>, f32, Vec<u8>, u64, Vec<u16>)> = by_key_hash
        .into_iter()
        .map(|(kh, (lsn, gid, code, qjl, norm, sub, raw))| {
            (lsn, gid, code, qjl, norm, sub, kh, raw)
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.6.cmp(&b.6)));

    // R5 (persistence review): a dropped sidecar must be LOUD. The merged
    // segment loses exact rerank for ALL entries when any source lacks the
    // sidecar; without a warning this silent recall degradation is invisible
    // (FT.INFO `segments_with_exact_rerank` exposes the steady state).
    if !all_have_raw {
        let with_raw = segments.iter().filter(|s| s.raw_f16().is_some()).count();
        if with_raw > 0 {
            tracing::warn!(
                sources = segments.len(),
                sources_with_sidecar = with_raw,
                "GraphUnion merge drops the exact-rerank f16 sidecar for the \
                 ENTIRE merged segment (all-or-nothing propagation): at least \
                 one source segment lacks it — merged-segment queries fall \
                 back to quantized ADC distances (recall degrades, not breaks)"
            );
        }
    }

    // ── Step 3: Build TQ buffer (verbatim codes, no re-encode) ───────────────
    let mut tq_buffer_orig: Vec<u8> = Vec::with_capacity(n * bytes_per_code);
    let mut qjl_orig: Vec<u8> = Vec::with_capacity(n * qjl_bpv);
    let mut residual_norms: Vec<f32> = Vec::with_capacity(n);
    let mut sub_orig: Vec<u8> = Vec::with_capacity(n * sub_bpv);
    let mut mvcc_orig: Vec<MvccHeader> = Vec::with_capacity(n);
    let mut raw_orig: Vec<u16> = if all_have_raw {
        Vec::with_capacity(n * dim)
    } else {
        Vec::new()
    };

    for (i, (lsn, gid, code, qjl, _norm, sub, kh, raw)) in entries.iter().enumerate() {
        tq_buffer_orig.extend_from_slice(code);
        if all_have_raw {
            raw_orig.extend_from_slice(raw);
        }
        if qjl_bpv > 0 {
            if qjl.len() == qjl_bpv {
                qjl_orig.extend_from_slice(qjl);
            } else {
                // Pad with zeros if QJL not available for this segment.
                qjl_orig.extend(std::iter::repeat_n(0u8, qjl_bpv));
            }
        }
        // Residual norm from the norm bytes in the TQ code (unused for SQ8).
        let entry_norm = if is_sq8 {
            0.0
        } else {
            let code_slice = &code[..];
            let norm_b = &code_slice[code_len..];
            f32::from_le_bytes([norm_b[0], norm_b[1], norm_b[2], norm_b[3]])
        };
        residual_norms.push(entry_norm);

        if sub_bpv > 0 {
            if sub.len() == sub_bpv {
                sub_orig.extend_from_slice(sub);
            } else {
                sub_orig.extend(std::iter::repeat_n(0u8, sub_bpv));
            }
        }

        mvcc_orig.push(MvccHeader {
            internal_id: i as u32,
            global_id: *gid,
            key_hash: *kh,
            insert_lsn: *lsn,
            delete_lsn: 0,
            hint_committed: 0,
        });
    }

    // ── Step 4: Decode TQ → centroids for HNSW build oracle ─────────────────
    let is_a2 = collection.quantization
        == crate::vector::turbo_quant::collection::QuantizationConfig::TurboQuant4A2;
    let a2_cb = if is_a2 {
        Some(crate::vector::turbo_quant::a2_lattice::A2Codebook::new(
            collection.padded_dimension,
        ))
    } else {
        Option::None
    };
    // SQ8 has no codebook (`codebook_16()` would log a spurious empty-codebook
    // error and return a zeroed table); only TQ scalar paths need it.
    let codebook_opt: Option<&[f32; 16]> = if !is_a2 && !is_sq8 {
        Some(collection.codebook_16())
    } else {
        Option::None
    };

    let all_rotated: Vec<Vec<f32>> = {
        let mut rotated: Vec<Vec<f32>> = Vec::with_capacity(n);
        if is_sq8 {
            // SQ8: decode `dim` u8 codes via the per-vector (min, scale) trailer
            // into a `dim`-length f32 vector. The HNSW builder uses symmetric L2
            // over these decoded vectors — no codebook or FWHT (mirrors `compact`).
            for i in 0..n {
                let offset = i * bytes_per_code;
                let slot = &tq_buffer_orig[offset..offset + bytes_per_code];
                let (min, scale) = sq8_params(slot, dim);
                rotated.push(decode_sq8(&slot[..dim], min, scale));
            }
        } else if is_a2 {
            let cb = match a2_cb.as_ref() {
                Some(c) => c,
                None => {
                    return Err(CompactionError::PersistFailed(
                        "A2 codebook missing in merge".into(),
                    ));
                }
            };
            for i in 0..n {
                let offset = i * bytes_per_code;
                let code_slice = &tq_buffer_orig[offset..offset + code_len];
                let mut q_rot = Vec::with_capacity(padded);
                for &byte in code_slice {
                    let (x0, y0) = cb.decode_pair(byte & 0x0F);
                    let (x1, y1) = cb.decode_pair(byte >> 4);
                    q_rot.push(x0);
                    q_rot.push(y0);
                    q_rot.push(x1);
                    q_rot.push(y1);
                }
                q_rot.truncate(padded);
                rotated.push(q_rot);
            }
        } else {
            let codebook = match codebook_opt {
                Some(c) => c,
                None => {
                    return Err(CompactionError::PersistFailed(
                        "scalar codebook missing in merge".into(),
                    ));
                }
            };
            for i in 0..n {
                let offset = i * bytes_per_code;
                let code_slice = &tq_buffer_orig[offset..offset + code_len];
                let mut q_rot = Vec::with_capacity(padded);
                for &byte in code_slice {
                    q_rot.push(codebook[(byte & 0x0F) as usize]);
                    q_rot.push(codebook[(byte >> 4) as usize]);
                }
                q_rot.truncate(padded);
                rotated.push(q_rot);
            }
        }
        rotated
    };

    // ── Step 5: Build HNSW graph over the union ──────────────────────────────
    let dist_table = crate::vector::distance::table();
    let mut builder = HnswBuilder::new(HNSW_M, HNSW_EF_CONSTRUCTION, seed);
    for _i in 0..n {
        builder.insert(|a: u32, b: u32| {
            let ra = &all_rotated[a as usize];
            let rb = &all_rotated[b as usize];
            (dist_table.l2_f32)(ra, rb)
        });
    }
    let graph = builder.build(bytes_per_code as u32);

    // ── Step 6: BFS-reorder TQ buffer ────────────────────────────────────────
    let mut tq_bfs = vec![0u8; n * bytes_per_code];
    for bfs_pos in 0..n {
        let orig_id = graph.to_original(bfs_pos as u32) as usize;
        let src = orig_id * bytes_per_code;
        let dst = bfs_pos * bytes_per_code;
        tq_bfs[dst..dst + bytes_per_code]
            .copy_from_slice(&tq_buffer_orig[src..src + bytes_per_code]);
    }

    // BFS-reorder QJL, residual norms, sub-centroid signs.
    let mut qjl_bfs = vec![0u8; n * qjl_bpv];
    let mut norms_bfs = vec![0.0f32; n];
    let mut sub_bfs = vec![0u8; n * sub_bpv];
    for bfs_pos in 0..n {
        let orig_id = graph.to_original(bfs_pos as u32) as usize;
        if qjl_bpv > 0 {
            let src = orig_id * qjl_bpv;
            let dst = bfs_pos * qjl_bpv;
            if src + qjl_bpv <= qjl_orig.len() {
                qjl_bfs[dst..dst + qjl_bpv].copy_from_slice(&qjl_orig[src..src + qjl_bpv]);
            }
        }
        if orig_id < residual_norms.len() {
            norms_bfs[bfs_pos] = residual_norms[orig_id];
        }
        if sub_bpv > 0 {
            let src = orig_id * sub_bpv;
            let dst = bfs_pos * sub_bpv;
            if src + sub_bpv <= sub_orig.len() {
                sub_bfs[dst..dst + sub_bpv].copy_from_slice(&sub_orig[src..src + sub_bpv]);
            }
        }
    }

    // BFS-reorder MVCC headers.
    let mut mvcc_bfs: Vec<MvccHeader> = Vec::with_capacity(n);
    for bfs_pos in 0..n {
        let orig_id = graph.to_original(bfs_pos as u32) as usize;
        let mut hdr = mvcc_orig[orig_id];
        hdr.internal_id = bfs_pos as u32;
        mvcc_bfs.push(hdr);
    }

    // BFS-reorder the exact-rerank sidecar (HQ-1), same permutation as tq_bfs.
    let raw_f16_bfs: Option<Vec<u16>> = if all_have_raw && raw_orig.len() == n * dim {
        let mut buf = vec![0u16; n * dim];
        for bfs_pos in 0..n {
            let orig_id = graph.to_original(bfs_pos as u32) as usize;
            buf[bfs_pos * dim..(bfs_pos + 1) * dim]
                .copy_from_slice(&raw_orig[orig_id * dim..(orig_id + 1) * dim]);
        }
        Some(buf)
    } else {
        None
    };

    // ── Step 7: Recall verification ──────────────────────────────────────────
    // Sample queries from the merged TQ codes and compare against fan-out
    // search across the original segments.
    let recall = verify_merge_recall(&graph, &tq_bfs, segments, collection, dim, n, seed);

    // NOTE: no `&& recall > 0.0` guard — recall == 0.0 is a genuine total
    // collapse that MUST abort. verify_merge_recall returns 1.0 (not 0.0) for
    // the "too few vectors to measure" cases, so 0.0 only ever means a real
    // measured zero-recall merge.
    if recall < recall_tolerance {
        tracing::warn!(
            "merge recall {recall:.4} < tolerance {recall_tolerance:.4}; aborting merge"
        );
        return Err(CompactionError::RecallTooLow {
            recall,
            required: recall_tolerance,
        });
    }

    // ── Step 8: Build merged ImmutableSegment ────────────────────────────────
    let merged = ImmutableSegment::new(
        graph,
        AlignedBuffer::from_vec(tq_bfs),
        qjl_bfs,
        norms_bfs,
        qjl_bpv,
        sub_bfs,
        sub_bpv,
        mvcc_bfs,
        collection.clone(),
        n as u32,
        n as u32,
    )
    .with_raw_f16(raw_f16_bfs)
    // AE-1: measure this build's adaptive-ef estimate while we're on the
    // compaction thread (no-op without a sidecar).
    .with_adaptive_ef()
    // B2 (durability): tag with the disk id before writing, mirroring `compact()`.
    .with_disk_segment_id(persist.map(|(_, segment_id)| segment_id));

    // Persist to disk if requested — same staged (staging + fsync + rename +
    // dir-fsync) writer as `compact()`. Merge persists identically to compact.
    if let Some((dir, segment_id)) = persist {
        segment_io::write_immutable_segment_staged(dir, segment_id, &merged, collection)
            .map_err(|e| CompactionError::PersistFailed(format!("{e}")))?;
    }

    Ok(merged)
}
