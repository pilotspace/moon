//! HNSW graph construction for the compaction pipeline.
//!
//! Split out of the former single-file `compaction.rs` (moon#479, file-size
//! ceiling). Contents are unchanged: the spatial cell partitioning, the
//! parallel per-cell sub-graph build, the cross-cell stitcher, and the
//! sequential/parallel builder selector.

use crate::vector::hnsw::build::HnswBuilder;

use crate::vector::segment::compaction::{HNSW_EF_CONSTRUCTION, HNSW_M, PARALLEL_THRESHOLD};

/// Assign vectors to spatial cells based on first two f32 coordinates.
/// Returns a vector of `num_cells` cells, where each cell contains the indices
/// of its member vectors. Uses a simple grid partitioning.
#[allow(dead_code)] // Retained for tests; disabled in production (needs PCA partitioning)
pub(super) fn assign_to_cells(vectors: &[&[f32]], num_cells: usize) -> Vec<Vec<usize>> {
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
pub(super) fn compact_parallel(
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
pub(super) fn stitch_subgraphs(
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

/// Build the HNSW graph for `n` vectors, choosing the concurrent
/// shared-graph builder for large segments and the sequential (bitwise
/// deterministic for a given seed) builder for small ones.
///
/// The threshold split keeps small-segment builds — and every existing
/// test fixture — byte-identical, while bulk-load compactions
/// (`FT.COMPACT` after ingest, background builds) scale across cores.
/// Thread count mirrors the background-compactor clamp (≤ 8).
pub(super) fn build_graph_auto(
    n: usize,
    seed: u64,
    bytes_per_code: u32,
    dist_fn: impl Fn(u32, u32) -> f32 + Sync,
) -> crate::vector::hnsw::graph::HnswGraph {
    // system_parallelism, NOT available_parallelism: compact() runs on
    // core-pinned shard/compactor threads whose affinity mask makes
    // available_parallelism() report 1 (which silently forced every build
    // down the sequential path — measured at exactly sequential speed).
    let threads = crate::shard::numa::system_parallelism().min(8);
    if n >= PARALLEL_THRESHOLD && threads > 1 {
        crate::vector::hnsw::parallel_build::build_parallel(
            n as u32,
            HNSW_M,
            HNSW_EF_CONSTRUCTION,
            seed,
            threads,
            &dist_fn,
            bytes_per_code,
        )
    } else {
        let mut builder = HnswBuilder::new(HNSW_M, HNSW_EF_CONSTRUCTION, seed);
        for _ in 0..n {
            builder.insert(&dist_fn);
        }
        builder.build(bytes_per_code)
    }
}
