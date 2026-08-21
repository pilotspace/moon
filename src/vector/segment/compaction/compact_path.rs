//! The frozen-mutable -> immutable compaction path.
//!
//! Split out of the former single-file `compaction.rs` (moon#479, file-size
//! ceiling); `compact` itself is unchanged.

use std::path::Path;
use std::sync::Arc;

use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::persistence::segment_io;
use crate::vector::segment::compaction::CompactionError;
use crate::vector::segment::compaction::graph_build::build_graph_auto;
// Only the GPU (CAGRA) build path reads the HNSW hyper-parameters directly;
// the CPU path takes them via `build_graph_auto`.
#[cfg(feature = "gpu-cuda")]
use crate::vector::segment::compaction::{HNSW_EF_CONSTRUCTION, HNSW_M};
use crate::vector::segment::immutable::{ImmutableSegment, MvccHeader};
use crate::vector::segment::mutable::FrozenSegment;
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::turbo_quant::sq8::{decode_sq8, sq8_params};

/// Convert a frozen mutable segment into an optimized immutable segment.
///
/// Steps: filter dead -> encode TQ -> build HNSW -> verify recall -> BFS reorder ->
/// persist (optional) -> construct ImmutableSegment.
///
/// `persist`: when `Some((dir, segment_id))`, writes the segment to disk after construction.
///
/// Returns `Err(CompactionError::RecallTooLow)` if recall < 0.95.
/// Returns `Err(CompactionError::EmptySegment)` if all entries are deleted.
#[tracing::instrument(skip_all, level = "debug")]
pub fn compact(
    frozen: &FrozenSegment,
    collection: &Arc<CollectionMetadata>,
    seed: u64,
    persist: Option<(&Path, u64)>,
) -> Result<ImmutableSegment, CompactionError> {
    let _dim = frozen.dimension as usize;
    let padded = collection.padded_dimension as usize;
    let signs = collection.fwht_sign_flips.as_slice();
    let bytes_per_code = frozen.bytes_per_code;

    // ── Step 1: Filter dead entries ──────────────────────────────────
    let mut live_entries = Vec::new();

    for entry in &frozen.entries {
        if entry.delete_lsn != 0 {
            continue;
        }
        live_entries.push(entry);
    }

    let n = live_entries.len();
    if n == 0 {
        return Err(CompactionError::EmptySegment);
    }

    // ── Step 2: TQ codes already encoded at insert time ─────────────
    // Build flat TQ buffer from frozen TQ codes (filter dead entries)
    let mut tq_buffer_orig: Vec<u8> = Vec::with_capacity(n * bytes_per_code);
    for entry in &live_entries {
        let offset = entry.internal_id as usize * bytes_per_code;
        tq_buffer_orig.extend_from_slice(&frozen.tq_codes[offset..offset + bytes_per_code]);
    }

    // ── Step 3: Build HNSW ───────────────────────────────────────────

    // Note: the shared TQ codebook is fetched later (guarded by `!is_a2 && !is_sq8`).
    // It must NOT be fetched unconditionally here — `codebook_16()` debug-asserts a
    // 16-entry codebook, which SQ8 collections (empty codebook) do not have, so an
    // eager call panics SQ8 under debug/CI builds while silently returning a zeroed
    // fallback under release.

    // Build raw f32 vectors for live entries (for exact pairwise HNSW build
    // and GPU path). Also needed later for sub-centroid sign computation.
    // Falls back to TQ-decoded centroids if raw_f32 is empty (persistence reload).
    let has_raw = !frozen.raw_f32.is_empty();
    let dim = frozen.dimension as usize;

    let _live_f32: Vec<&[f32]> = if has_raw {
        live_entries
            .iter()
            .map(|e| {
                let start = e.internal_id as usize * dim;
                &frozen.raw_f32[start..start + dim]
            })
            .collect()
    } else {
        Vec::new()
    };

    // --- GPU HNSW build path (feature-gated) ---
    // When gpu-cuda is enabled and the batch is large enough, attempt a
    // GPU-accelerated HNSW construction via CAGRA. On any failure the GPU
    // path returns None and we fall through to the CPU builder below.
    #[cfg(feature = "gpu-cuda")]
    let gpu_graph: Option<crate::vector::hnsw::graph::HnswGraph> = {
        use crate::vector::gpu::{MIN_VECTORS_FOR_GPU, try_gpu_build_hnsw};
        if n >= MIN_VECTORS_FOR_GPU {
            try_gpu_build_hnsw(&live_f32, dim, HNSW_M, HNSW_EF_CONSTRUCTION, seed)
        } else {
            None
        }
    };

    // Determine whether we need the CPU path. When GPU succeeded we skip
    // the expensive all_rotated precomputation and HnswBuilder entirely.
    #[cfg(feature = "gpu-cuda")]
    let need_cpu_build = gpu_graph.is_none();
    #[cfg(not(feature = "gpu-cuda"))]
    let need_cpu_build = true;

    let is_a2 = collection.quantization
        == crate::vector::turbo_quant::collection::QuantizationConfig::TurboQuant4A2;
    let a2_cb = if is_a2 {
        Some(crate::vector::turbo_quant::a2_lattice::A2Codebook::new(
            collection.padded_dimension,
        ))
    } else {
        None
    };
    // SQ8 has no shared codebook; leave it None so the scalar-TQ decode/sub-sign
    // branches skip (the dedicated SQ8 branch handles decoding) and codebook_16()
    // never logs a spurious "empty codebook" error for SQ8.
    let is_sq8 = collection.quantization == QuantizationConfig::Sq8;
    let codebook_opt: Option<&[f32; 16]> = if !is_a2 && !is_sq8 {
        Some(collection.codebook_16())
    } else {
        None
    };
    let _codebook_for_adc: &[f32; 16] = if !is_a2 && !is_sq8 {
        collection.codebook_16()
    } else {
        &[0.0; 16]
    };
    // SQ8 slots are `dim` u8 codes + 8 params bytes (not the TQ `padded/2` nibble
    // layout + 4-byte norm) — same ternary as the read sites at ~:1079/:1450.
    // Without the branch this is `dim + 4` for SQ8: latent today (every SQ8 read
    // is guarded by `codebook_opt.is_none() → continue`) but a slice-OOB landmine.
    let code_len = if is_sq8 { dim } else { bytes_per_code - 4 };

    let has_raw = !frozen.raw_f32.is_empty();
    let dim = frozen.dimension as usize;

    let live_f32: Vec<&[f32]> = if has_raw && need_cpu_build {
        live_entries
            .iter()
            .map(|e| {
                let start = e.internal_id as usize * dim;
                &frozen.raw_f32[start..start + dim]
            })
            .collect()
    } else {
        Vec::new()
    };

    // Also decode TQ → centroid for sub-centroid sign computation (needed later).
    let all_rotated: Vec<Vec<f32>> = if need_cpu_build {
        let mut rotated: Vec<Vec<f32>> = Vec::with_capacity(n);
        if is_sq8 {
            // SQ8: decode `dim` u8 codes via per-vector (min, scale) into an f32
            // vector. The HNSW builder then uses symmetric L2 over these decoded
            // vectors (no FWHT, no centroids) — the same approach the scalar-TQ
            // path uses, just with affine decode instead of codebook lookup.
            for i in 0..n {
                let offset = i * bytes_per_code;
                let slot = &tq_buffer_orig[offset..offset + bytes_per_code];
                let (min, scale) = sq8_params(slot, dim);
                rotated.push(decode_sq8(&slot[..dim], min, scale));
            }
        } else if is_a2 {
            // A2: each nibble is a pair index; decode via A2Codebook
            // is_a2 branch guarantees a2_cb is Some
            let cb = match a2_cb.as_ref() {
                Some(c) => c,
                None => return Err(CompactionError::PersistFailed("A2 codebook missing".into())),
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
            // Scalar TQ: each nibble is a single-coordinate index
            let codebook = match codebook_opt {
                Some(c) => c,
                None => {
                    return Err(CompactionError::PersistFailed(
                        "scalar codebook missing".into(),
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
    } else {
        Vec::new()
    };

    // Cell-parallel (spatial partitioning) stays disabled: 2-coordinate
    // partitioning is meaningless at 384d+ and produces poorly stitched
    // graphs; compact_parallel() is retained for tests only. Large builds
    // instead use the shared-graph concurrent builder (parallel_build):
    // same insertion algorithm under per-node locks, near-linear scaling —
    // the single-threaded insert loop was 99.3% of FT.COMPACT wall time
    // (30 s at 50K × 384d, measured 2026-07-08).
    let graph = if need_cpu_build {
        let dist_table = crate::vector::distance::table();
        if has_raw {
            // EXACT f32 L2 pairwise distance — optimal HNSW graph topology
            build_graph_auto(n, seed, bytes_per_code as u32, |a: u32, b: u32| {
                let va = live_f32[a as usize];
                let vb = live_f32[b as usize];
                (dist_table.l2_f32)(va, vb)
            })
        } else {
            // Decoded-vector fallback (A2 and scalar-TQ light mode): symmetric
            // L2 over decoded vectors. TQ-ADC (asymmetric) was previously used
            // here but its noise causes poor HNSW graph topology at 384d+ —
            // greedy routing gets stuck. Decoded L2 is symmetric,
            // deterministic, and much more accurate for pairwise neighbor
            // selection during graph construction.
            build_graph_auto(n, seed, bytes_per_code as u32, |a: u32, b: u32| {
                let ra = &all_rotated[a as usize];
                let rb = &all_rotated[b as usize];
                (dist_table.l2_f32)(ra, rb)
            })
        }
    } else {
        #[cfg(feature = "gpu-cuda")]
        {
            // SAFETY: gpu_graph is Some when need_cpu_build is false
            gpu_graph.expect("gpu_graph must be Some when need_cpu_build is false")
        }
        #[cfg(not(feature = "gpu-cuda"))]
        {
            unreachable!("need_cpu_build is always true without gpu-cuda feature")
        }
    };

    // ── Step 5: BFS reorder TQ and SQ buffers ────────────────────────
    // (Step 5 before Step 4 because verify_recall needs BFS-ordered buffer)
    let mut tq_bfs = vec![0u8; n * bytes_per_code];
    for bfs_pos in 0..n {
        let orig_id = graph.to_original(bfs_pos as u32) as usize;
        let src = orig_id * bytes_per_code;
        let dst = bfs_pos * bytes_per_code;
        tq_bfs[dst..dst + bytes_per_code]
            .copy_from_slice(&tq_buffer_orig[src..src + bytes_per_code]);
    }

    // BFS reorder QJL signs and residual norms for TurboQuant_prod reranking.
    let qjl_bpv = frozen.qjl_bytes_per_vec;
    let mut qjl_signs_bfs = vec![0u8; n * qjl_bpv];
    let mut residual_norms_bfs = vec![0.0f32; n];
    for bfs_pos in 0..n {
        let orig_id = graph.to_original(bfs_pos as u32) as usize;
        let live_idx = orig_id;
        // QJL signs
        let src_qjl = live_idx * qjl_bpv;
        let dst_qjl = bfs_pos * qjl_bpv;
        if src_qjl + qjl_bpv <= frozen.qjl_signs.len() {
            qjl_signs_bfs[dst_qjl..dst_qjl + qjl_bpv]
                .copy_from_slice(&frozen.qjl_signs[src_qjl..src_qjl + qjl_bpv]);
        }
        // Residual norms
        if live_idx < frozen.residual_norms.len() {
            residual_norms_bfs[bfs_pos] = frozen.residual_norms[live_idx];
        }
    }

    // Compute sub-centroid sign bits from raw f32 vectors (FWHT-rotated).
    // For each coordinate: compare the ACTUAL rotated value against its quantized centroid.
    // Sign bit = 1 if original >= centroid (upper sub-bin), 0 if below.
    let sub_bpv = (padded + 7) / 8;
    let mut sub_signs_bfs = vec![0u8; n * sub_bpv];
    // SQ8 has no sub-centroid refinement (no codebook): both inner branches
    // below `continue` unconditionally, so without this gate the loop spends
    // O(n · padded log padded) on normalize+FWHT whose results are discarded.
    // The zero-filled buffer is exactly the SQ8 contract.
    if has_raw && !is_sq8 {
        // Use raw f32 → FWHT rotate → compare against centroid per TQ index
        let mut work = vec![0.0f32; padded];
        for bfs_pos in 0..n {
            let orig_id = graph.to_original(bfs_pos as u32) as usize;
            let live_idx = orig_id;
            let raw = &frozen.raw_f32[live_entries[live_idx].internal_id as usize * dim
                ..(live_entries[live_idx].internal_id as usize + 1) * dim];

            // Normalize + pad + FWHT to get actual rotated coordinates
            let norm_sq: f32 = raw.iter().map(|x| x * x).sum();
            let norm = norm_sq.sqrt();
            if norm > 0.0 {
                let inv = 1.0 / norm;
                for (dst, &src) in work[..dim].iter_mut().zip(raw.iter()) {
                    *dst = src * inv;
                }
            } else {
                for v in work[..dim].iter_mut() {
                    *v = 0.0;
                }
            }
            for v in work[dim..padded].iter_mut() {
                *v = 0.0;
            }
            crate::vector::turbo_quant::fwht::fwht(&mut work[..padded], signs);

            let code_offset = bfs_pos * bytes_per_code;
            let code_slice = &tq_bfs[code_offset..code_offset + code_len];
            let sign_offset = bfs_pos * sub_bpv;

            if is_a2 {
                // A2: each nibble is a pair index, decode via A2Codebook
                let cb = if let Some(c) = a2_cb.as_ref() {
                    c
                } else {
                    continue;
                };
                for j in 0..code_slice.len() {
                    let byte = code_slice[j];
                    let qi = j * 4; // each byte = 2 pairs = 4 coordinates
                    let (x0, y0) = cb.decode_pair(byte & 0x0F);
                    let (x1, y1) = cb.decode_pair(byte >> 4);
                    if qi < padded && work[qi] >= x0 {
                        sub_signs_bfs[sign_offset + qi / 8] |= 1 << (qi % 8);
                    }
                    if qi + 1 < padded && work[qi + 1] >= y0 {
                        sub_signs_bfs[sign_offset + (qi + 1) / 8] |= 1 << ((qi + 1) % 8);
                    }
                    if qi + 2 < padded && work[qi + 2] >= x1 {
                        sub_signs_bfs[sign_offset + (qi + 2) / 8] |= 1 << ((qi + 2) % 8);
                    }
                    if qi + 3 < padded && work[qi + 3] >= y1 {
                        sub_signs_bfs[sign_offset + (qi + 3) / 8] |= 1 << ((qi + 3) % 8);
                    }
                }
            } else {
                // Scalar TQ: each nibble is a single-coordinate index
                let codebook = if let Some(c) = codebook_opt {
                    c
                } else {
                    continue;
                };
                for j in 0..code_slice.len() {
                    let byte = code_slice[j];
                    let qi = j * 2;
                    if work[qi] >= codebook[(byte & 0x0F) as usize] {
                        sub_signs_bfs[sign_offset + qi / 8] |= 1 << (qi % 8);
                    }
                    if work[qi + 1] >= codebook[(byte >> 4) as usize] {
                        sub_signs_bfs[sign_offset + (qi + 1) / 8] |= 1 << ((qi + 1) % 8);
                    }
                }
            }
        }
    } else if need_cpu_build && !is_sq8 && !frozen.sub_centroid_signs.is_empty() {
        // Light mode with insert-time sub-centroid signs: remap to BFS order.
        // graph.to_original(bfs_pos) returns the builder's sequential ID (0..n-1),
        // which is the index into live_entries. Use it directly, not as internal_id.
        for bfs_pos in 0..n {
            let orig_id = graph.to_original(bfs_pos as u32) as usize;
            if orig_id < live_entries.len() {
                let src_internal = live_entries[orig_id].internal_id as usize;
                let src_offset = src_internal * sub_bpv;
                let dst_offset = bfs_pos * sub_bpv;
                if src_offset + sub_bpv <= frozen.sub_centroid_signs.len() {
                    sub_signs_bfs[dst_offset..dst_offset + sub_bpv].copy_from_slice(
                        &frozen.sub_centroid_signs[src_offset..src_offset + sub_bpv],
                    );
                }
            }
        }
    }

    // ── Step 5: Create ImmutableSegment ─────────────────────────────
    let mvcc: Vec<MvccHeader> = (0..n)
        .map(|bfs_pos| {
            let orig_id = graph.to_original(bfs_pos as u32) as usize;
            let entry = live_entries[orig_id];
            MvccHeader {
                internal_id: bfs_pos as u32,
                global_id: frozen.global_id_base + entry.internal_id,
                key_hash: entry.key_hash,
                insert_lsn: entry.insert_lsn,
                delete_lsn: entry.delete_lsn,
                hint_committed: 0,
            }
        })
        .collect();

    let total_count = frozen.entries.len() as u32;
    let live_count = n as u32;

    // HQ-1: exact-rerank sidecar — f16 copies of the original vectors in BFS
    // order (same permutation as tq_bfs), lifted verbatim from the mutable
    // segment's f16 buffer (kept in BOTH build modes; unlike raw_f32, which is
    // Exact-only). Disk-reloaded rebuilds have no f16 buffer and fall back to
    // quantized ADC distances.
    let expected_f16 = frozen.entries.len() * dim;
    let raw_f16_bfs: Option<Vec<u16>> = if frozen.raw_f16.len() == expected_f16 && n > 0 {
        let mut buf: Vec<u16> = Vec::with_capacity(n * dim);
        for bfs_pos in 0..n {
            let orig_id = graph.to_original(bfs_pos as u32) as usize;
            let src = live_entries[orig_id].internal_id as usize * dim;
            buf.extend_from_slice(&frozen.raw_f16[src..src + dim]);
        }
        Some(buf)
    } else {
        None
    };

    let segment = ImmutableSegment::new(
        graph,
        AlignedBuffer::from_vec(tq_bfs),
        qjl_signs_bfs,
        residual_norms_bfs,
        qjl_bpv,
        sub_signs_bfs,
        sub_bpv,
        mvcc,
        collection.clone(),
        live_count,
        total_count,
    )
    .with_raw_f16(raw_f16_bfs)
    // AE-1: measure this build's adaptive-ef estimate while we're on the
    // compaction thread (no-op without a sidecar).
    .with_adaptive_ef()
    // B2 (durability): tag the segment with the disk id it will be persisted
    // under (if any) BEFORE writing, so callers can read it back off the
    // returned segment without threading the id separately.
    .with_disk_segment_id(persist.map(|(_, segment_id)| segment_id));

    // Step 7 (continued): persist to disk if requested. Uses the staged
    // (write-to-staging + fsync + rename + dir-fsync) writer so a segment
    // only becomes visible under its final `segment-{id}` name once it is
    // fully durable — see VECTOR-DURABILITY-DESIGN.md.
    if let Some((dir, segment_id)) = persist {
        segment_io::write_immutable_segment_staged(dir, segment_id, &segment, collection)
            .map_err(|e| CompactionError::PersistFailed(format!("{e}")))?;
    }

    Ok(segment)
}
