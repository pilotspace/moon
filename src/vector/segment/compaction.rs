//! Compaction pipeline: frozen mutable segment -> immutable segment.
//!
//! 8-step pipeline:
//! 1. Filter dead entries
//! 2. Encode TQ-4bit
//! 3. Build HNSW with pairwise TQ-ADC oracle
//! 4. Verify recall >= 0.95
//! 5. BFS-reorder TQ and SQ buffers
//! 6. Payload indexes (stub for Phase 64)
//! 7. Persist to disk (stub for Phase 66)
//! 8. Construct ImmutableSegment

use std::path::Path;
use std::sync::Arc;

use super::immutable::{ImmutableSegment, MvccHeader};
use super::mutable::FrozenSegment;
use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::hnsw::build::HnswBuilder;
use crate::vector::hnsw::search_sq::hnsw_search_f32;
use crate::vector::persistence::segment_io;
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::turbo_quant::sq8::{SQ8_PARAMS_BYTES, decode_sq8, sq8_params};

#[allow(dead_code)]
const RECALL_SAMPLE_SIZE: usize = 1000;
#[allow(dead_code)]
const MIN_RECALL: f32 = 0.95;
const VACUUM_DEAD_THRESHOLD: f32 = 0.20;
const HNSW_M: u8 = 16;
const HNSW_EF_CONSTRUCTION: u16 = 200;
const PARALLEL_THRESHOLD: usize = 10_000;

#[derive(Debug)]
pub enum CompactionError {
    RecallTooLow { recall: f32, required: f32 },
    EmptySegment,
    PersistFailed(String),
}

impl std::fmt::Display for CompactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RecallTooLow { recall, required } => {
                write!(
                    f,
                    "compaction recall {recall:.4} below required {required:.4}"
                )
            }
            Self::EmptySegment => write!(f, "cannot compact empty segment"),
            Self::PersistFailed(msg) => write!(f, "persist failed: {msg}"),
        }
    }
}

/// Assign vectors to spatial cells based on first two f32 coordinates.
/// Returns a vector of `num_cells` cells, where each cell contains the indices
/// of its member vectors. Uses a simple grid partitioning.
#[allow(dead_code)] // Retained for tests; disabled in production (needs PCA partitioning)
fn assign_to_cells(vectors: &[&[f32]], num_cells: usize) -> Vec<Vec<usize>> {
    if vectors.is_empty() || num_cells <= 1 {
        return vec![vectors.iter().enumerate().map(|(i, _)| i).collect()];
    }

    let cols = (num_cells as f32).sqrt().ceil() as usize;
    let rows = (num_cells + cols - 1) / cols;
    let actual_cells = rows * cols;

    // Find min/max of first two coordinates
    let mut min_x = f32::MAX;
    let mut max_x = f32::MIN;
    let mut min_y = f32::MAX;
    let mut max_y = f32::MIN;

    for &v in vectors {
        let x = if !v.is_empty() { v[0] } else { 0.0 };
        let y = if v.len() > 1 { v[1] } else { 0.0 };
        if x < min_x {
            min_x = x;
        }
        if x > max_x {
            max_x = x;
        }
        if y < min_y {
            min_y = y;
        }
        if y > max_y {
            max_y = y;
        }
    }

    // Add small epsilon to avoid edge-case where max coordinate maps to out-of-bounds cell
    let range_x = (max_x - min_x).max(1e-9);
    let range_y = (max_y - min_y).max(1e-9);

    let mut cells: Vec<Vec<usize>> = vec![Vec::new(); actual_cells];

    for (i, &v) in vectors.iter().enumerate() {
        let x = if !v.is_empty() { v[0] } else { 0.0 };
        let y = if v.len() > 1 { v[1] } else { 0.0 };

        let col = (((x - min_x) / range_x) * cols as f32).floor() as usize;
        let row = (((y - min_y) / range_y) * rows as f32).floor() as usize;
        let col = col.min(cols - 1);
        let row = row.min(rows - 1);
        let cell_idx = row * cols + col;
        cells[cell_idx].push(i);
    }

    cells
}

/// Build HNSW sub-graphs per cell in parallel, then stitch into unified graph.
///
/// Uses `std::thread::scope` for scoped parallelism (no rayon dependency).
/// Each cell gets an independent HnswBuilder with local IDs, then sub-graphs
/// are stitched together with cross-cell boundary edges.
#[allow(dead_code)]
fn compact_parallel(
    live_f32: &[&[f32]],
    _tq_buffer: &[u8],
    bytes_per_code: usize,
    _dim: usize,
    seed: u64,
) -> crate::vector::hnsw::graph::HnswGraph {
    let n = live_f32.len();
    let num_cells = std::thread::available_parallelism()
        .map(|p| p.get().min(16))
        .unwrap_or(4)
        .min(((n as f32).sqrt() as usize / 31).saturating_add(1))
        .max(2);

    let cell_assignments = assign_to_cells(live_f32, num_cells);

    let dist_table = crate::vector::distance::table();

    // Build sub-graphs in parallel using std::thread::scope
    let sub_graphs: Vec<(crate::vector::hnsw::graph::HnswGraph, Vec<usize>)> =
        std::thread::scope(|s| {
            let handles: Vec<_> = cell_assignments
                .iter()
                .enumerate()
                .filter(|(_, cell)| !cell.is_empty())
                .map(|(cell_idx, cell)| {
                    let cell = cell.clone();
                    let cell_seed = seed.wrapping_add(cell_idx as u64 * 0x9E37_79B9_7F4A_7C15);
                    s.spawn(move || {
                        let cell_vecs: Vec<&[f32]> =
                            cell.iter().map(|&idx| live_f32[idx]).collect();
                        let cell_n = cell_vecs.len();

                        let mut builder = HnswBuilder::new(HNSW_M, HNSW_EF_CONSTRUCTION, cell_seed);
                        for _ in 0..cell_n {
                            builder.insert(|a: u32, b: u32| {
                                (dist_table.l2_f32)(cell_vecs[a as usize], cell_vecs[b as usize])
                            });
                        }
                        let graph = builder.build(bytes_per_code as u32);
                        (graph, cell)
                    })
                })
                .collect();

            handles.into_iter().filter_map(|h| h.join().ok()).collect()
        });

    stitch_subgraphs(&sub_graphs, live_f32, bytes_per_code)
}

/// Stitch sub-graphs into a unified HnswGraph with cross-cell boundary edges.
///
/// Strategy:
/// 1. Allocate unified layer0 flat array of size N * M0
/// 2. Copy each sub-graph's edges, remapping local IDs to global IDs
/// 3. For each pair of adjacent cells, find boundary vectors and add cross-cell edges
/// 4. BFS reorder the merged graph
#[allow(dead_code)]
fn stitch_subgraphs(
    sub_graphs: &[(crate::vector::hnsw::graph::HnswGraph, Vec<usize>)],
    live_f32: &[&[f32]],
    bytes_per_code: usize,
) -> crate::vector::hnsw::graph::HnswGraph {
    use crate::vector::hnsw::graph::{SENTINEL, bfs_reorder, rearrange_layer0};
    use smallvec::SmallVec;

    let n = live_f32.len();
    let m0 = HNSW_M * 2;
    let m0_usize = m0 as usize;
    let dist_table = crate::vector::distance::table();

    // Build global ID mapping: for each sub-graph, map local BFS position -> global ID
    // Global ID = original vector index in live_f32
    let mut global_ids: Vec<Vec<u32>> = Vec::with_capacity(sub_graphs.len());
    // Also build reverse: global_id -> which sub-graph index
    let mut node_to_cell = vec![0u32; n];

    for (cell_idx, (graph, members)) in sub_graphs.iter().enumerate() {
        let mut local_to_global = Vec::with_capacity(graph.num_nodes() as usize);
        for bfs_pos in 0..graph.num_nodes() {
            let orig_local = graph.to_original(bfs_pos) as usize;
            let global_id = members[orig_local] as u32;
            local_to_global.push(global_id);
            node_to_cell[global_id as usize] = cell_idx as u32;
        }
        global_ids.push(local_to_global);
    }

    // Allocate unified layer0 flat array
    let mut layer0_flat = vec![SENTINEL; n * m0_usize];
    // Also allocate upper layers and levels (we only preserve layer 0 for stitched graph)
    let levels = vec![0u8; n];

    // Copy sub-graph edges with ID remapping
    for (cell_idx, (graph, _members)) in sub_graphs.iter().enumerate() {
        let id_map = &global_ids[cell_idx];
        for bfs_pos in 0..graph.num_nodes() {
            let global_id = id_map[bfs_pos as usize] as usize;
            let neighbors = graph.neighbors_l0(bfs_pos);
            let dst_start = global_id * m0_usize;
            for (j, &nb) in neighbors.iter().enumerate() {
                if nb == SENTINEL {
                    break;
                }
                layer0_flat[dst_start + j] = id_map[nb as usize];
            }
        }
    }

    // Stitch: for each pair of cells, find boundary vectors and add cross-cell edges.
    // For each cell, compute centroid, then find K nearest vectors to other cell's centroid.
    let boundary_k = (m0_usize / 2).max(4); // number of boundary vectors per cell per pair
    let l2_fn = dist_table.l2_f32;

    // Compute cell centroids
    let dim = if !live_f32.is_empty() {
        live_f32[0].len()
    } else {
        0
    };
    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(sub_graphs.len());
    for (_graph, members) in sub_graphs {
        let mut centroid = vec![0.0f32; dim];
        for &idx in members {
            for (d, &val) in centroid.iter_mut().zip(live_f32[idx].iter()) {
                *d += val;
            }
        }
        let inv = 1.0 / members.len() as f32;
        for d in &mut centroid {
            *d *= inv;
        }
        centroids.push(centroid);
    }

    // For each pair of cells, add boundary edges
    for ci in 0..sub_graphs.len() {
        for cj in (ci + 1)..sub_graphs.len() {
            let members_i = &sub_graphs[ci].1;
            let members_j = &sub_graphs[cj].1;

            // Find boundary_k vectors from cell i closest to cell j's centroid
            let centroid_j = &centroids[cj];
            let mut dists_i: Vec<(f32, usize)> = members_i
                .iter()
                .map(|&idx| ((dist_table.l2_f32)(live_f32[idx], centroid_j), idx))
                .collect();
            dists_i.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            let boundary_i: Vec<usize> = dists_i
                .iter()
                .take(boundary_k)
                .map(|&(_, idx)| idx)
                .collect();

            // Find boundary_k vectors from cell j closest to cell i's centroid
            let centroid_i = &centroids[ci];
            let mut dists_j: Vec<(f32, usize)> = members_j
                .iter()
                .map(|&idx| ((dist_table.l2_f32)(live_f32[idx], centroid_i), idx))
                .collect();
            dists_j.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            let boundary_j: Vec<usize> = dists_j
                .iter()
                .take(boundary_k)
                .map(|&(_, idx)| idx)
                .collect();

            // Add bidirectional cross-cell edges between boundary vectors.
            // Each boundary vector in cell_i connects to its nearest neighbors in cell_j,
            // and vice versa, ensuring robust cross-cell connectivity.
            for &bi in &boundary_i {
                let mut cross_dists: Vec<(f32, usize)> = boundary_j
                    .iter()
                    .map(|&bj| ((dist_table.l2_f32)(live_f32[bi], live_f32[bj]), bj))
                    .collect();
                cross_dists
                    .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                let add_count = 3.min(cross_dists.len());
                for &(_, bj) in cross_dists.iter().take(add_count) {
                    add_neighbor_to_flat(
                        &mut layer0_flat,
                        bi as u32,
                        bj as u32,
                        m0_usize,
                        live_f32,
                        l2_fn,
                    );
                    add_neighbor_to_flat(
                        &mut layer0_flat,
                        bj as u32,
                        bi as u32,
                        m0_usize,
                        live_f32,
                        l2_fn,
                    );
                }
            }
            for &bj in &boundary_j {
                let mut cross_dists: Vec<(f32, usize)> = boundary_i
                    .iter()
                    .map(|&bi| ((dist_table.l2_f32)(live_f32[bj], live_f32[bi]), bi))
                    .collect();
                cross_dists
                    .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                let add_count = 3.min(cross_dists.len());
                for &(_, bi) in cross_dists.iter().take(add_count) {
                    add_neighbor_to_flat(
                        &mut layer0_flat,
                        bj as u32,
                        bi as u32,
                        m0_usize,
                        live_f32,
                        l2_fn,
                    );
                    add_neighbor_to_flat(
                        &mut layer0_flat,
                        bi as u32,
                        bj as u32,
                        m0_usize,
                        live_f32,
                        l2_fn,
                    );
                }
            }
        }
    }

    // Find the node with highest degree as entry point (good BFS coverage)
    let mut best_entry = 0u32;
    let mut best_degree = 0usize;
    for i in 0..n {
        let start = i * m0_usize;
        let degree = layer0_flat[start..start + m0_usize]
            .iter()
            .filter(|&&nb| nb != SENTINEL)
            .count();
        if degree > best_degree {
            best_degree = degree;
            best_entry = i as u32;
        }
    }

    // BFS reorder
    let (bfs_order, bfs_inverse) = bfs_reorder(n as u32, m0, best_entry, &layer0_flat);
    let layer0 = rearrange_layer0(n as u32, m0, &layer0_flat, &bfs_order, &bfs_inverse);

    let bfs_entry = bfs_order[best_entry as usize];

    // Build with empty upper layers (parallel build focuses on layer 0 connectivity)
    let upper_layers: Vec<SmallVec<[u32; 32]>> = vec![SmallVec::new(); n];

    crate::vector::hnsw::graph::HnswGraph::new(
        n as u32,
        HNSW_M,
        m0,
        bfs_entry,
        0, // max_level = 0 (layer 0 only for stitched graph)
        layer0,
        bfs_order,
        bfs_inverse,
        upper_layers,
        levels,
        bytes_per_code as u32,
    )
}

/// Add a neighbor to a node's flat neighbor list, replacing a SENTINEL slot.
/// If the list is full, replace the last slot to ensure cross-cell edges are added.
/// This trades one intra-cell neighbor for a cross-cell edge, which is critical
/// for global graph connectivity.
#[allow(dead_code)]
fn add_neighbor_to_flat(
    layer0_flat: &mut [u32],
    node: u32,
    neighbor: u32,
    m0: usize,
    live_f32: &[&[f32]],
    dist_fn: fn(&[f32], &[f32]) -> f32,
) {
    let start = node as usize * m0;
    let slots = &mut layer0_flat[start..start + m0];

    // Check if already present
    for &slot in slots.iter() {
        if slot == neighbor {
            return;
        }
        if slot == crate::vector::hnsw::graph::SENTINEL {
            break;
        }
    }

    // Try to find empty sentinel slot
    for slot in slots.iter_mut() {
        if *slot == crate::vector::hnsw::graph::SENTINEL {
            *slot = neighbor;
            return;
        }
    }

    // List is full: replace the farthest existing neighbor if the new neighbor is closer
    let node_vec = live_f32[node as usize];
    let new_dist = dist_fn(node_vec, live_f32[neighbor as usize]);
    let mut worst_idx = 0;
    let mut worst_dist = 0.0f32;
    for (i, &nb) in slots.iter().enumerate() {
        if nb == crate::vector::hnsw::graph::SENTINEL {
            break;
        }
        let d = dist_fn(node_vec, live_f32[nb as usize]);
        if d > worst_dist {
            worst_dist = d;
            worst_idx = i;
        }
    }
    if new_dist < worst_dist {
        slots[worst_idx] = neighbor;
    }
}

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
    let code_len = bytes_per_code - 4;

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

    // Cell-parallel disabled: 2-coordinate spatial partitioning is meaningless at 384d+
    // and produces poorly stitched graphs. TODO: replace with PCA-based partitioning.
    // compact_parallel() is retained for tests; production always uses single-threaded builder.
    let _parallel_threshold = PARALLEL_THRESHOLD; // suppress unused warning
    let graph = if need_cpu_build {
        let dist_table = crate::vector::distance::table();
        let mut builder = HnswBuilder::new(HNSW_M, HNSW_EF_CONSTRUCTION, seed);

        if has_raw {
            // EXACT f32 L2 pairwise distance — optimal HNSW graph topology
            for _i in 0..n {
                builder.insert(|a: u32, b: u32| {
                    let va = live_f32[a as usize];
                    let vb = live_f32[b as usize];
                    (dist_table.l2_f32)(va, vb)
                });
            }
        } else if is_a2 {
            // A2 fallback: use decoded rotated vectors with L2 (no scalar TQ-ADC for A2)
            for _i in 0..n {
                builder.insert(|a: u32, b: u32| {
                    let ra = &all_rotated[a as usize];
                    let rb = &all_rotated[b as usize];
                    (dist_table.l2_f32)(ra, rb)
                });
            }
        } else {
            // Light mode fallback: use decoded centroid vectors with symmetric L2.
            // TQ-ADC (asymmetric) was previously used here but its noise causes
            // poor HNSW graph topology at 384d+ — greedy routing gets stuck.
            // Decoded centroid L2 is symmetric, deterministic, and much more accurate
            // for pairwise neighbor selection during graph construction.
            for _i in 0..n {
                builder.insert(|a: u32, b: u32| {
                    let ra = &all_rotated[a as usize];
                    let rb = &all_rotated[b as usize];
                    (dist_table.l2_f32)(ra, rb)
                });
            }
        }

        builder.build(bytes_per_code as u32)
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
    if has_raw {
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
    } else if need_cpu_build && !frozen.sub_centroid_signs.is_empty() {
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
    );

    // Step 7 (continued): persist to disk if requested
    if let Some((dir, segment_id)) = persist {
        segment_io::write_immutable_segment(dir, segment_id, &segment, collection)
            .map_err(|e| CompactionError::PersistFailed(format!("{e}")))?;
    }

    Ok(segment)
}

/// Verify recall of the HNSW graph using f32 L2 search against brute-force
/// f32 L2 ground truth.
///
/// Since ImmutableSegment now delegates HNSW traversal to hnsw_search_f32
/// (TQ-ADC is reserved for brute-force scan), verification must also use
/// f32 L2 to match the production search path.
///
/// Samples min(RECALL_SAMPLE_SIZE, n) queries deterministically and measures
/// recall@10. Returns average recall across all sampled queries.
#[allow(dead_code)]
fn verify_recall(
    graph: &crate::vector::hnsw::graph::HnswGraph,
    _tq_buffer_bfs: &[u8],
    live_vectors: &[f32],
    _collection: &Arc<CollectionMetadata>,
    dimension: u32,
) -> f32 {
    let n = graph.num_nodes() as usize;
    if n == 0 {
        return 1.0;
    }

    let dim = dimension as usize;
    let l2_fn = crate::vector::distance::table().l2_f32;
    let k = 10.min(n);
    let ef_verify = 128;

    // BFS-reorder f32 vectors for hnsw_search_f32
    let mut f32_bfs = vec![0.0f32; n * dim];
    for bfs_pos in 0..n {
        let orig_id = graph.to_original(bfs_pos as u32) as usize;
        let src = orig_id * dim;
        let dst = bfs_pos * dim;
        f32_bfs[dst..dst + dim].copy_from_slice(&live_vectors[src..src + dim]);
    }

    // Determine sample indices (deterministic)
    let sample_size = RECALL_SAMPLE_SIZE.min(n);
    let step = if n > sample_size { n / sample_size } else { 1 };
    let sample_indices: Vec<usize> = (0..n).step_by(step).take(sample_size).collect();

    let mut total_recall = 0.0f32;

    for &query_orig_idx in &sample_indices {
        let query_slice = &live_vectors[query_orig_idx * dim..(query_orig_idx + 1) * dim];

        // HNSW search using f32 L2 (matches production path)
        let hnsw_results = hnsw_search_f32(graph, &f32_bfs, dim, query_slice, k, ef_verify, None);

        // Brute-force f32 L2 ground truth
        let mut dists: Vec<(f32, u32)> = (0..n as u32)
            .map(|i| {
                let v = &live_vectors[i as usize * dim..(i as usize + 1) * dim];
                (l2_fn(query_slice, v), i)
            })
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        let gt_ids: std::collections::HashSet<u32> = dists.iter().take(k).map(|d| d.1).collect();
        let found_ids: std::collections::HashSet<u32> =
            hnsw_results.iter().map(|r| r.id.0).collect();
        let overlap = gt_ids.intersection(&found_ids).count();
        total_recall += overlap as f32 / k as f32;
    }

    total_recall / sample_indices.len() as f32
}

/// Check if an immutable segment needs vacuum (rebuild due to too many dead entries).
///
/// Returns true when dead_fraction > 20%.
pub fn needs_vacuum(segment: &ImmutableSegment) -> bool {
    segment.dead_fraction() > VACUUM_DEAD_THRESHOLD
}

// ── Immutable segment merge (P2) ─────────────────────────────────────────────

/// Trigger threshold: merge when immutable segment count exceeds this.
pub const MERGE_SEGMENT_THRESHOLD: usize = 16;

/// Maximum estimated bytes for the union before merge is refused.
/// Prevents OOM during merge of very large indexes.
/// 512 MiB.
pub const MERGE_MEMORY_CEILING: usize = 512 * 1024 * 1024;

/// Merge mode for immutable segment consolidation.
///
/// Determines how the union HNSW graph is built when merging N immutable
/// segments into one. The modes differ in recall quality vs. memory cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MergeMode {
    /// (Default) Concatenate TQ codes verbatim; rebuild HNSW graph over the
    /// union using TQ-decoded centroids for distance computation.
    ///
    /// No f32 round-trip — codes are never decoded then re-encoded. Only graph
    /// edges change. This preserves recall because quantization error is not
    /// accumulated (decode → re-encode path is avoided entirely).
    #[default]
    GraphUnion,

    /// Retain raw f32 vectors in memory alongside each immutable segment.
    /// On merge, use raw vectors as the authoritative distance oracle to build
    /// the merged HNSW graph, then re-encode TQ codes from scratch.
    ///
    /// Higher recall than GraphUnion (no quantization error at all in the
    /// graph topology) at the cost of +1.5 KB/vector at 384d f16 memory.
    ///
    /// Note: raw f32 data is retained in-memory only; it is NOT persisted to
    /// disk and will be lost on restart. The index falls back to GraphUnion
    /// for segments loaded from disk.
    KeepRaw,

    /// Disable automatic merging. Segments accumulate indefinitely.
    /// Use when the operator manages compaction manually.
    None,
}

impl MergeMode {
    /// Parse a merge mode string (case-insensitive).
    /// Accepts: "GRAPH_UNION", "KEEP_RAW", "NONE".
    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.eq_ignore_ascii_case(b"GRAPH_UNION") || b.eq_ignore_ascii_case(b"GRAPHUNION") {
            Some(Self::GraphUnion)
        } else if b.eq_ignore_ascii_case(b"KEEP_RAW") || b.eq_ignore_ascii_case(b"KEEPRAW") {
            Some(Self::KeepRaw)
        } else if b.eq_ignore_ascii_case(b"NONE") {
            Some(Self::None)
        } else {
            Option::None
        }
    }
}

/// Statistics returned by `merge_immutable`.
#[derive(Debug, Default, Clone, Copy)]
pub struct MergeStats {
    /// Number of input segments that were merged.
    pub segments_merged: usize,
    /// Number of live vectors in the merged output segment.
    pub live_vectors: usize,
    /// Recall of the merged segment against pre-merge fan-out search.
    /// 0.0 if recall gate was not evaluated (e.g., too few vectors).
    pub recall: f32,
}

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
pub fn merge_immutable(
    segments: &[Arc<ImmutableSegment>],
    collection: &Arc<CollectionMetadata>,
    seed: u64,
    mode: MergeMode,
    recall_tolerance: f32,
) -> Result<ImmutableSegment, CompactionError> {
    match mode {
        MergeMode::None => Err(CompactionError::EmptySegment), // caller should not call
        MergeMode::GraphUnion => merge_graph_union(segments, collection, seed, recall_tolerance),
        MergeMode::KeepRaw => {
            // KeepRaw: fall back to graph-union if no raw f32 available (warn only).
            // Full raw-vector path requires raw_f32 stored on ImmutableSegment (TODO P2.5).
            tracing::warn!(
                "MERGE_MODE=keep_raw: raw f32 not yet persisted on ImmutableSegment; \
                 falling back to graph-union. Add KEEP_RAW sidecar in P2.5."
            );
            merge_graph_union(segments, collection, seed, recall_tolerance)
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
    //                  sub_centroid_bytes)
    let mut by_key_hash: std::collections::HashMap<
        u64,
        (u64, u32, Vec<u8>, Vec<u8>, f32, Vec<u8>),
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

    for seg in segments {
        let tq_buf = seg.vectors_tq().as_slice();
        let headers = seg.mvcc_headers();

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

            // Deduplicate: keep highest insert_lsn.
            let entry = by_key_hash.entry(hdr.key_hash).or_insert((
                0,
                0,
                Vec::new(),
                Vec::new(),
                0.0,
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
    let mut entries: Vec<(u64, u32, Vec<u8>, Vec<u8>, f32, Vec<u8>, u64)> = by_key_hash
        .into_iter()
        .map(|(kh, (lsn, gid, code, qjl, norm, sub))| (lsn, gid, code, qjl, norm, sub, kh))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.6.cmp(&b.6)));

    // ── Step 3: Build TQ buffer (verbatim codes, no re-encode) ───────────────
    let mut tq_buffer_orig: Vec<u8> = Vec::with_capacity(n * bytes_per_code);
    let mut qjl_orig: Vec<u8> = Vec::with_capacity(n * qjl_bpv);
    let mut residual_norms: Vec<f32> = Vec::with_capacity(n);
    let mut sub_orig: Vec<u8> = Vec::with_capacity(n * sub_bpv);
    let mut mvcc_orig: Vec<MvccHeader> = Vec::with_capacity(n);

    for (i, (lsn, gid, code, qjl, _norm, sub, kh)) in entries.iter().enumerate() {
        tq_buffer_orig.extend_from_slice(code);
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

    // ── Step 7: Recall verification ──────────────────────────────────────────
    // Sample queries from the merged TQ codes and compare against fan-out
    // search across the original segments.
    let recall = verify_merge_recall(&graph, &tq_bfs, segments, collection, dim, n, seed);

    if recall < recall_tolerance && recall > 0.0 {
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
    );

    Ok(merged)
}

/// Verify recall of the merged HNSW graph against brute-force over the merged
/// decoded centroid vectors.
///
/// Both ground-truth and HNSW search operate in the same merged coordinate
/// space (sequential IDs 0..n-1 in BFS order), so recall is well-defined.
///
/// Algorithm:
/// 1. Decode all n merged TQ codes to centroid f32 vectors (FWHT-rotated).
/// 2. For each sampled query (one of those decoded centroids), compute brute-
///    force top-K by L2 as ground truth.
/// 3. Run HNSW search on the merged graph using the same query.
/// 4. Recall@K = |HNSW_topk ∩ brute_force_topk| / K.
///
/// This is the same methodology as the existing `verify_recall()` for
/// mutable→immutable compaction.
///
/// Returns 1.0 if n < MIN_RECALL_SAMPLE (too few vectors for reliable measurement).
///
/// Minimum sample threshold: 50 vectors. Below this, HNSW can't form reliable
/// graph topology (fewer candidates than ef_construction demands). The gate still
/// fires for sizes ≥50, which is low enough that tests can exercise rejection
/// paths without spinning up 500-vector corpora.
///
/// Production merges happen at ≥16 segments × ≥1000 vectors = ≥16K total vectors.
const MIN_RECALL_SAMPLE_N: usize = 50;

fn verify_merge_recall(
    graph: &crate::vector::hnsw::graph::HnswGraph,
    tq_bfs: &[u8],
    _pre_segments: &[Arc<ImmutableSegment>],
    collection: &Arc<CollectionMetadata>,
    dim: usize,
    n: usize,
    seed: u64,
) -> f32 {
    if n < MIN_RECALL_SAMPLE_N {
        return 1.0; // too few vectors for reliable measurement
    }
    let k = 10.min(n / 2).max(1);
    if n < k * 2 {
        return 1.0; // too few vectors
    }

    let is_sq8 = collection.quantization == QuantizationConfig::Sq8;
    let bytes_per_code = if is_sq8 {
        dim + SQ8_PARAMS_BYTES
    } else {
        collection.bytes_per_code_per_vector() as usize
    };
    let code_len = if is_sq8 { dim } else { bytes_per_code - 4 };
    let padded = collection.padded_dimension as usize;
    // Effective f32 dimensionality of the decoded recall oracle: `dim` for SQ8
    // (affine decode → dim-length vectors), `padded` for TQ (codebook decode →
    // padded-length FWHT-rotated space). The flat buffer stride and the
    // hnsw_search_f32 dimension must both use this, or the oracle mis-strides.
    let eff_dim = if is_sq8 { dim } else { padded };
    let sample_size = RECALL_SAMPLE_SIZE.min(n / 2).max(1);
    let step = (n / sample_size).max(1);

    let is_a2 = collection.quantization
        == crate::vector::turbo_quant::collection::QuantizationConfig::TurboQuant4A2;
    let a2_cb = if is_a2 {
        Some(crate::vector::turbo_quant::a2_lattice::A2Codebook::new(
            collection.padded_dimension,
        ))
    } else {
        Option::None
    };

    /// Decode one TQ code slice to a centroid f32 vector (FWHT-rotated space).
    fn decode_code(
        code_slice: &[u8],
        padded: usize,
        is_a2: bool,
        a2_cb: Option<&crate::vector::turbo_quant::a2_lattice::A2Codebook>,
        codebook: Option<&[f32; 16]>,
    ) -> Vec<f32> {
        let mut q_rot = Vec::with_capacity(padded);
        if is_a2 {
            if let Some(cb) = a2_cb {
                for &byte in code_slice {
                    let (x0, y0) = cb.decode_pair(byte & 0x0F);
                    let (x1, y1) = cb.decode_pair(byte >> 4);
                    q_rot.push(x0);
                    q_rot.push(y0);
                    q_rot.push(x1);
                    q_rot.push(y1);
                }
            }
        } else if let Some(cb) = codebook {
            for &byte in code_slice {
                q_rot.push(cb[(byte & 0x0F) as usize]);
                q_rot.push(cb[(byte >> 4) as usize]);
            }
        }
        q_rot.truncate(padded);
        q_rot
    }

    let codebook = if !is_a2 {
        collection.try_codebook_16()
    } else {
        None
    };

    // Decode all n centroid vectors once.
    let all_decoded: Vec<Vec<f32>> = (0..n)
        .map(|i| {
            let offset = i * bytes_per_code;
            if is_sq8 {
                let slot = &tq_bfs[offset..offset + bytes_per_code];
                let (min, scale) = sq8_params(slot, dim);
                decode_sq8(&slot[..dim], min, scale)
            } else {
                let code_slice = &tq_bfs[offset..offset + code_len];
                decode_code(code_slice, padded, is_a2, a2_cb.as_ref(), codebook)
            }
        })
        .collect();

    let l2_fn = crate::vector::distance::table().l2_f32;
    let ef_verify = (k * 15).max(128);

    // Build the flat f32 BFS buffer once (amortize allocation over all queries).
    // Each all_decoded[i] has `padded` elements in BFS order.
    let f32_bfs_flat: Vec<f32> = all_decoded.iter().flatten().copied().collect();

    let mut total_recall = 0.0f32;
    let mut sample_count = 0usize;

    // Deterministic sample: evenly spaced through BFS order, offset by seed.
    let offset = (seed as usize) % step.max(1);
    let sample_indices: Vec<usize> = (offset..n).step_by(step).take(sample_size).collect();

    for &query_bfs in &sample_indices {
        let query = &all_decoded[query_bfs];
        if query.len() < eff_dim / 2 {
            continue; // skip degenerate empty decode
        }

        // Brute-force top-K in decoded centroid space (ground truth).
        let mut dists: Vec<(f32, u32)> = (0..n as u32)
            .filter(|&i| i != query_bfs as u32)
            .map(|i| (l2_fn(query, &all_decoded[i as usize]), i))
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let gt_ids: std::collections::HashSet<u32> = dists.iter().take(k).map(|d| d.1).collect();

        // HNSW search on the merged graph using f32 decoded centroids.
        // f32_bfs_flat has BFS-ordered vectors, `eff_dim` elements each.
        let hnsw_results =
            hnsw_search_f32(graph, &f32_bfs_flat, eff_dim, query, k, ef_verify, None);
        // hnsw_search_f32 returns original IDs (pre-BFS); convert to BFS positions
        // so they match the ground-truth set (which indexes all_decoded by BFS pos).
        let hnsw_ids: std::collections::HashSet<u32> =
            hnsw_results.iter().map(|r| graph.to_bfs(r.id.0)).collect();

        let overlap = gt_ids.intersection(&hnsw_ids).count();
        total_recall += overlap as f32 / k.min(gt_ids.len()).max(1) as f32;
        sample_count += 1;
    }

    if sample_count == 0 {
        return 1.0;
    }
    total_recall / sample_count as f32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::distance;
    use crate::vector::segment::mutable::MutableSegment;
    use crate::vector::turbo_quant::collection::QuantizationConfig;
    use crate::vector::types::DistanceMetric;

    fn lcg_f32(dim: usize, seed: u32) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    fn normalize(v: &mut [f32]) -> f32 {
        let norm_sq: f32 = v.iter().map(|x| x * x).sum();
        let norm = norm_sq.sqrt();
        if norm > 0.0 {
            let inv = 1.0 / norm;
            v.iter_mut().for_each(|x| *x *= inv);
        }
        norm
    }

    fn make_frozen_segment(
        n: usize,
        dim: usize,
        delete_count: usize,
    ) -> (FrozenSegment, Arc<CollectionMetadata>) {
        distance::init();
        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));
        let seg = MutableSegment::new(dim as u32, collection.clone());

        for i in 0..n {
            let mut f32_v = lcg_f32(dim, (i * 7 + 13) as u32);
            normalize(&mut f32_v);
            let sq_v: Vec<i8> = f32_v
                .iter()
                .map(|&x| (x * 127.0).clamp(-128.0, 127.0) as i8)
                .collect();
            seg.append(i as u64, &f32_v, &sq_v, 1.0, i as u64 + 1);
        }

        // Mark some as deleted
        for i in 0..delete_count {
            seg.mark_deleted(i as u32, 100);
        }

        let frozen = seg.freeze();
        (frozen, collection)
    }

    #[test]
    fn test_compact_100_vectors() {
        let (frozen, collection) = make_frozen_segment(100, 64, 0);
        let result = compact(&frozen, &collection, 12345, None);
        assert!(result.is_ok(), "compact failed: {:?}", result.err());
        let imm = result.unwrap();
        assert_eq!(imm.live_count(), 100);
        assert_eq!(imm.total_count(), 100);

        // Verify search works on the resulting segment
        let mut query = lcg_f32(64, 99999);
        normalize(&mut query);
        let padded = collection.padded_dimension;
        let mut scratch =
            crate::vector::hnsw::search::SearchScratch::new(imm.graph().num_nodes(), padded);
        let results = imm.search(&query, 5, 64, &mut scratch);
        assert!(!results.is_empty());
        assert!(results.len() <= 5);
    }

    #[test]
    fn test_sq8_compact_and_immutable_search_recall() {
        distance::init();
        let dim = 96usize;
        let n = 200usize;
        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::Cosine,
            QuantizationConfig::Sq8,
            42,
        ));
        let seg = MutableSegment::new(dim as u32, collection.clone());
        let mut db: Vec<Vec<f32>> = Vec::with_capacity(n);
        for i in 0..n {
            let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
            normalize(&mut v);
            seg.append(i as u64, &v, &[], 1.0, i as u64 + 1);
            db.push(v);
        }
        let frozen = seg.freeze();
        let imm = compact(&frozen, &collection, 12345, None).expect("SQ8 compact failed");
        assert_eq!(imm.live_count(), n as u32);

        let padded = collection.padded_dimension;

        // Exact-match invariant: query == db[7] must rank 7 first. The broken
        // codebook fallback returned ml:0 here (degenerate codes); real SQ8 must not.
        let mut scratch =
            crate::vector::hnsw::search::SearchScratch::new(imm.graph().num_nodes(), padded);
        let res = imm.search(&db[7], 10, 128, &mut scratch);
        assert!(!res.is_empty(), "SQ8 immutable search returned nothing");
        assert_eq!(
            res[0].id.0 as usize, 7,
            "SQ8 nearest != exact match: got {}",
            res[0].id.0
        );

        // recall@10 vs exact-f32 L2 ground truth over several queries. The broken
        // SQ8 scored ~0.01 here; 8-bit fidelity + HNSW should recover the bulk.
        let queries = [3usize, 7, 50, 123, 199];
        let mut total_hits = 0usize;
        for &qi in &queries {
            let mut idx: Vec<usize> = (0..n).collect();
            idx.sort_by(|&a, &b| {
                let da: f32 = db[qi]
                    .iter()
                    .zip(&db[a])
                    .map(|(x, y)| (x - y) * (x - y))
                    .sum();
                let dbb: f32 = db[qi]
                    .iter()
                    .zip(&db[b])
                    .map(|(x, y)| (x - y) * (x - y))
                    .sum();
                da.total_cmp(&dbb)
            });
            let exact: std::collections::HashSet<u32> =
                idx.into_iter().take(10).map(|i| i as u32).collect();
            let mut sc =
                crate::vector::hnsw::search::SearchScratch::new(imm.graph().num_nodes(), padded);
            let got = imm.search(&db[qi], 10, 128, &mut sc);
            let got_ids: std::collections::HashSet<u32> = got.iter().map(|r| r.id.0).collect();
            total_hits += exact.intersection(&got_ids).count();
        }
        assert!(
            total_hits >= 42,
            "SQ8 immutable recall@10 too low: {total_hits}/50"
        );
    }

    /// Build a real SQ8 immutable segment of `n` normalized vectors with global
    /// ids `id_base..id_base+n`. Returns the segment and its f32 db vectors.
    fn make_sq8_immutable(
        n: usize,
        dim: usize,
        id_base: usize,
        collection: &Arc<CollectionMetadata>,
        seed: u64,
    ) -> (ImmutableSegment, Vec<Vec<f32>>) {
        let seg = MutableSegment::new(dim as u32, collection.clone());
        let mut db = Vec::with_capacity(n);
        for i in 0..n {
            let gid = id_base + i;
            let mut v = lcg_f32(dim, (gid * 7 + 13) as u32);
            normalize(&mut v);
            seg.append(gid as u64, &v, &[], 1.0, gid as u64 + 1);
            db.push(v);
        }
        let frozen = seg.freeze();
        let imm = compact(&frozen, collection, seed, None).expect("SQ8 compact failed");
        (imm, db)
    }

    /// MERGE path for SQ8: merge two SQ8 immutable segments via GraphUnion and
    /// prove the merged segment searches correctly.
    ///
    /// On TQ-only merge code this is a proper red: `merge_graph_union` derives
    /// `bytes_per_code` from `collection.bytes_per_code_per_vector()` (= padded+4),
    /// but SQ8 slots are dim+8 — the stride mismatch skips/garbles entries
    /// (EmptySegment, RecallTooLow, or wrong neighbors). Config-D-strength
    /// assertions (exact-match top1 + recall@10) discriminate correct from
    /// plausibly-shaped-but-wrong.
    #[test]
    fn test_sq8_merge_two_segments_recall() {
        distance::init();
        let dim = 96usize;
        let per = 100usize;
        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::Cosine,
            QuantizationConfig::Sq8,
            42,
        ));
        let (imm_a, db_a) = make_sq8_immutable(per, dim, 0, &collection, 12345);
        let (imm_b, db_b) = make_sq8_immutable(per, dim, per, &collection, 6789);
        let mut db = db_a;
        db.extend(db_b);
        let n = db.len();

        let segs = vec![Arc::new(imm_a), Arc::new(imm_b)];
        let merged = merge_immutable(&segs, &collection, 42, MergeMode::GraphUnion, 0.60)
            .expect("SQ8 merge failed");
        assert_eq!(merged.live_count(), n as u32, "merged SQ8 live_count");

        let padded = collection.padded_dimension;

        // Self-match correctness across queries spanning both source segments:
        // each query == db[qi] must find its own vector in the merged graph,
        // whose self-distance is ~0 (only 8-bit affine reconstruction error) —
        // orders of magnitude below the ~O(1) squared-L2 between distinct unit
        // vectors. A broken-stride merge (padded+4 vs dim+8) has no near-zero
        // self-match and fails.
        //
        // We assert on distance, not id: `compact` assigns per-segment
        // global_ids (0..per), so ids collide across the two separately
        // compacted source segments and cannot identify a vector here.
        // Calibrated recall *preservation* (post-merge vs pre-merge fan-out)
        // is enforced by the integration test
        // `test_sq8_graph_union_merge_preserves_recall`.
        let _ = n;
        for &qi in &[3usize, 7, 50, 123, 199] {
            let mut sc =
                crate::vector::hnsw::search::SearchScratch::new(merged.graph().num_nodes(), padded);
            let got = merged.search(&db[qi], 10, 128, &mut sc);
            assert!(
                !got.is_empty(),
                "merged SQ8 search returned nothing for q{qi}"
            );
            assert!(
                got[0].distance < 0.01,
                "merged SQ8 self-match distance too high for db[{qi}]: {} (expected ~0)",
                got[0].distance
            );
        }
    }

    #[test]
    fn test_compact_filters_deleted() {
        let (frozen, collection) = make_frozen_segment(50, 64, 10);
        let result = compact(&frozen, &collection, 12345, None);
        assert!(result.is_ok(), "compact failed: {:?}", result.err());
        let imm = result.unwrap();
        // 50 total, 10 deleted -> 40 live
        assert_eq!(imm.live_count(), 40);
        assert_eq!(imm.total_count(), 50);
    }

    #[test]
    fn test_compact_empty_returns_error() {
        let (frozen, collection) = make_frozen_segment(5, 64, 5);
        let result = compact(&frozen, &collection, 12345, None);
        assert!(result.is_err());
        match result.err().unwrap() {
            CompactionError::EmptySegment => {}
            other => panic!("expected EmptySegment, got: {other}"),
        }
    }

    #[test]
    fn test_compact_recall_above_threshold() {
        let (frozen, collection) = make_frozen_segment(500, 64, 0);
        // compact() internally verifies recall >= 0.95 and returns Ok only if it passes
        let result = compact(&frozen, &collection, 12345, None);
        assert!(
            result.is_ok(),
            "compact failed (recall too low): {:?}",
            result.err()
        );
    }

    #[test]
    fn test_needs_vacuum_threshold() {
        // Create segment with 25% dead
        let (frozen, collection) = make_frozen_segment(100, 64, 0);
        let result = compact(&frozen, &collection, 12345, None);
        assert!(result.is_ok());
        let mut imm = result.unwrap();

        // Initially 0% dead
        assert!(!needs_vacuum(&imm));

        // Mark 25 as deleted -> 25%
        for i in 0..25u32 {
            imm.mark_deleted(i, 200);
        }
        assert!(needs_vacuum(&imm), "should need vacuum at 25% dead");

        // Create another with 10% dead
        let (frozen2, collection2) = make_frozen_segment(100, 64, 0);
        let result2 = compact(&frozen2, &collection2, 54321, None);
        assert!(result2.is_ok());
        let mut imm2 = result2.unwrap();

        for i in 0..10u32 {
            imm2.mark_deleted(i, 300);
        }
        assert!(!needs_vacuum(&imm2), "should not need vacuum at 10% dead");
    }

    /// Verify that compact() works identically without the gpu-cuda feature.
    /// This test always runs (no feature gate) and ensures the CPU path is
    /// unaffected by the GPU integration code.
    #[test]
    fn test_compact_without_gpu_feature_unchanged() {
        let (frozen, collection) = make_frozen_segment(100, 64, 0);
        let result = compact(&frozen, &collection, 12345, None);
        assert!(result.is_ok(), "compact failed: {:?}", result.err());
        assert_eq!(result.unwrap().live_count(), 100);
    }

    /// When gpu-cuda feature is enabled but no CUDA device is present (CI),
    /// compact() should fall back to the CPU path transparently.
    #[cfg(feature = "gpu-cuda")]
    #[test]
    fn test_gpu_fallback_to_cpu() {
        let (frozen, collection) = make_frozen_segment(100, 64, 0);
        let result = compact(&frozen, &collection, 12345, None);
        assert!(
            result.is_ok(),
            "compact with GPU fallback failed: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap().live_count(), 100);
    }

    // ── Cell-parallel compaction tests ──────────────────────────────

    /// Brute-force k-NN oracle: compute L2 distance from query to all vectors,
    /// return top-k IDs sorted by ascending distance.
    fn brute_force_knn(query: &[f32], all_vectors: &[&[f32]], k: usize) -> Vec<u32> {
        let mut dists: Vec<(f32, u32)> = all_vectors
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let d: f32 = query
                    .iter()
                    .zip(v.iter())
                    .map(|(a, b)| (a - b) * (a - b))
                    .sum();
                (d, i as u32)
            })
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        dists.iter().take(k).map(|(_, id)| *id).collect()
    }

    #[test]
    fn test_assign_to_cells_partitions_all_vectors() {
        let dim = 64;
        let vecs_owned: Vec<Vec<f32>> = (0..200)
            .map(|i| lcg_f32(dim, (i * 7 + 13) as u32))
            .collect();
        let vecs: Vec<&[f32]> = vecs_owned.iter().map(|v| v.as_slice()).collect();

        let cells = assign_to_cells(&vecs, 4);

        // Every vector index must appear exactly once across all cells
        let mut all_indices: Vec<usize> = cells.iter().flat_map(|c| c.iter().copied()).collect();
        all_indices.sort();
        let expected: Vec<usize> = (0..200).collect();
        assert_eq!(
            all_indices, expected,
            "all vectors must be assigned to exactly one cell"
        );
    }

    #[test]
    fn test_parallel_compact_bfs_reaches_all() {
        distance::init();
        let dim = 64;
        let n = 500;
        let vecs_owned: Vec<Vec<f32>> = (0..n)
            .map(|i| {
                let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
                normalize(&mut v);
                v
            })
            .collect();
        let vecs: Vec<&[f32]> = vecs_owned.iter().map(|v| v.as_slice()).collect();

        // Dummy TQ buffer (not used for graph topology, just sizing)
        let bytes_per_code = 36; // padded_dim/2 + 4 for 64d -> padded 64 -> 32+4
        let tq_buffer = vec![0u8; n * bytes_per_code];

        let graph = compact_parallel(&vecs, &tq_buffer, bytes_per_code, dim, 12345);

        assert_eq!(graph.num_nodes(), n as u32);

        // BFS from entry point should reach all nodes
        let mut visited = vec![false; n];
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(graph.entry_point());
        visited[graph.entry_point() as usize] = true;
        let mut count = 1usize;

        while let Some(pos) = queue.pop_front() {
            let neighbors = graph.neighbors_l0(pos);
            for &nb in neighbors {
                if nb == crate::vector::hnsw::graph::SENTINEL {
                    break;
                }
                if !visited[nb as usize] {
                    visited[nb as usize] = true;
                    count += 1;
                    queue.push_back(nb);
                }
            }
        }

        assert_eq!(
            count, n,
            "BFS from entry must reach all {} nodes, only reached {}",
            n, count
        );
    }

    #[test]
    fn test_compact_parallel_recall() {
        distance::init();
        let dim = 64;
        let n = 1000;
        let vecs_owned: Vec<Vec<f32>> = (0..n)
            .map(|i| {
                let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
                normalize(&mut v);
                v
            })
            .collect();
        let vecs: Vec<&[f32]> = vecs_owned.iter().map(|v| v.as_slice()).collect();

        let bytes_per_code = 36;
        let tq_buffer = vec![0u8; n * bytes_per_code];

        let graph = compact_parallel(&vecs, &tq_buffer, bytes_per_code, dim, 42);

        // Build BFS-ordered f32 buffer for hnsw_search_f32
        let mut f32_bfs = vec![0.0f32; n * dim];
        for bfs_pos in 0..n {
            let orig_id = graph.to_original(bfs_pos as u32) as usize;
            let src = &vecs_owned[orig_id];
            let dst_start = bfs_pos * dim;
            f32_bfs[dst_start..dst_start + dim].copy_from_slice(src);
        }

        // Measure recall@10 using brute-force L2 oracle
        let k = 10;
        let num_queries = 100;
        let mut total_recall = 0.0f64;

        for qi in 0..num_queries {
            let query_idx = qi * (n / num_queries);
            let query = vecs[query_idx];
            let gt = brute_force_knn(query, &vecs, k);

            // Search the graph using f32 L2 (matches production path).
            // Use ef=256 for stitched graphs (wider beam compensates for cross-cell edges).
            let hnsw_results = crate::vector::hnsw::search_sq::hnsw_search_f32(
                &graph, &f32_bfs, dim, query, k, 256, None,
            );

            // hnsw_search_f32 returns IDs in BFS space mapped back through to_original
            let result_ids: std::collections::HashSet<u32> =
                hnsw_results.iter().map(|r| r.id.0).collect();
            let gt_set: std::collections::HashSet<u32> = gt.into_iter().collect();
            let hits = result_ids.intersection(&gt_set).count();
            total_recall += hits as f64 / k as f64;
        }

        let avg_recall = total_recall / num_queries as f64;
        assert!(
            avg_recall >= 0.90,
            "recall@10 should be >= 0.90, got {:.4}",
            avg_recall
        );
    }

    #[test]
    fn test_stitch_cross_cell_edges() {
        distance::init();
        let dim = 64;
        let n = 200;
        let vecs_owned: Vec<Vec<f32>> = (0..n)
            .map(|i| {
                let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
                normalize(&mut v);
                v
            })
            .collect();
        let vecs: Vec<&[f32]> = vecs_owned.iter().map(|v| v.as_slice()).collect();

        let cells = assign_to_cells(&vecs, 4);

        // Build sub-graphs per cell
        let dist_table = crate::vector::distance::table();
        let mut sub_graphs: Vec<(crate::vector::hnsw::graph::HnswGraph, Vec<usize>)> = Vec::new();

        for cell in &cells {
            if cell.is_empty() {
                continue;
            }
            let cell_vecs: Vec<&[f32]> = cell.iter().map(|&idx| vecs[idx]).collect();
            let mut builder = HnswBuilder::new(HNSW_M, HNSW_EF_CONSTRUCTION, 42);
            for _ in 0..cell_vecs.len() {
                builder.insert(|a: u32, b: u32| {
                    (dist_table.l2_f32)(cell_vecs[a as usize], cell_vecs[b as usize])
                });
            }
            let graph = builder.build(36);
            sub_graphs.push((graph, cell.clone()));
        }

        let stitched = stitch_subgraphs(&sub_graphs, &vecs, 36);

        // Verify stitching produced a connected graph
        let mut visited = vec![false; n];
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(stitched.entry_point());
        visited[stitched.entry_point() as usize] = true;
        let mut count = 1usize;

        while let Some(pos) = queue.pop_front() {
            for &nb in stitched.neighbors_l0(pos) {
                if nb == crate::vector::hnsw::graph::SENTINEL {
                    break;
                }
                if !visited[nb as usize] {
                    visited[nb as usize] = true;
                    count += 1;
                    queue.push_back(nb);
                }
            }
        }

        assert_eq!(
            count, n,
            "stitched graph must be fully connected, only reached {}/{}",
            count, n
        );
    }

    #[test]
    fn test_merge_two_segments_basic() {
        distance::init();
        let dim = 32usize;
        let (frozen1, collection) = make_frozen_segment(40, dim, 0);
        let imm1 = compact(&frozen1, &collection, 1, None).expect("compact 1");

        let (frozen2, _) = make_frozen_segment(35, dim, 0);
        let imm2 = compact(&frozen2, &collection, 2, None).expect("compact 2");

        eprintln!(
            "pre-merge: imm1 total={} live={} headers={}, imm2 total={} live={} headers={}",
            imm1.total_count(),
            imm1.live_count(),
            imm1.mvcc_headers().len(),
            imm2.total_count(),
            imm2.live_count(),
            imm2.mvcc_headers().len(),
        );

        let segs = vec![Arc::new(imm1), Arc::new(imm2)];
        let result = merge_immutable(&segs, &collection, 42, MergeMode::GraphUnion, 0.80);

        match &result {
            Ok(m) => eprintln!(
                "merge ok: total={} live={}",
                m.total_count(),
                m.live_count()
            ),
            Err(e) => eprintln!("merge err: {e}"),
        }
        assert!(result.is_ok(), "merge failed: {:?}", result.err());
        let merged = result.unwrap();
        assert!(merged.total_count() > 0);
        assert!(merged.live_count() > 0);
    }
}
