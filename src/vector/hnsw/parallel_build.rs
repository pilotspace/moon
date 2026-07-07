//! Parallel HNSW construction — concurrent inserts into one shared graph.
//!
//! The sequential [`super::build::HnswBuilder`] burns one core for the whole
//! mutable→immutable compaction build (measured 30 s for 50K × 384d — 99.3%
//! of `FT.COMPACT` wall time). This module is the industry-standard fix
//! (hnswlib / Qdrant / faiss): all worker threads insert into a single
//! shared graph, guarded by one `parking_lot::Mutex` per node.
//!
//! Concurrency design (deadlock-free by construction):
//! - At most ONE node lock is held at any time. Neighbor lists are copied
//!   out under the lock into a caller-owned scratch buffer; distance
//!   computation happens unlocked. The only exception is the back-link
//!   re-prune, which computes distances while holding the *target* node's
//!   lock — bounded work (≤ M0+1 distance calls, microseconds), still a
//!   single lock.
//! - The entry point / max level pair lives under one `RwLock`, read once
//!   per insert and write-locked only on the rare level promotion.
//! - Levels are pre-generated from the same seeded LCG as the sequential
//!   builder, so the level distribution (and therefore memory layout) is
//!   identical for a given seed.
//!
//! Determinism trade-off: concurrent insert order makes edge sets
//! run-to-run nondeterministic (recall is statistically unchanged — see
//! `test_parallel_recall_parity`). Callers that need bitwise-reproducible
//! graphs (small segments, tests) keep using the sequential builder; the
//! compaction path switches to this builder only at
//! `PARALLEL_THRESHOLD`-sized segments.

use super::build::select_neighbors_heuristic;
use super::graph::{HnswGraph, SENTINEL, bfs_reorder, rearrange_layer0};
use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU32, Ordering};

/// Insert the first `SEQ_WARMUP` nodes from the coordinating thread before
/// fanning out. A tiny sequential prefix gives the concurrent phase a
/// well-connected core to route through (matches hnswlib practice) and
/// keeps degenerate races (empty graph, first entry promotion) off the
/// hot concurrent path.
const SEQ_WARMUP: u32 = 1024;

/// Max neighbors copied out of a node under its lock. M0 = 2·M and the
/// compaction path builds with M=16, so 32 covers layer 0; upper layers
/// use M=16 slots.
type NeighborBuf = SmallVec<[u32; 32]>;

/// (distance, id) pair ordered by distance then id — mirrors the private
/// `OrdF32Pair` in `build.rs`.
#[derive(Clone, Copy, PartialEq)]
struct DistPair(f32, u32);

impl Eq for DistPair {}

impl PartialOrd for DistPair {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DistPair {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.1.cmp(&other.1))
    }
}

/// Per-node adjacency storage. `l0` always holds exactly `m0` slots;
/// `upper` holds `level · m` slots (empty for level-0 nodes). Slots are
/// `SENTINEL`-padded, same layout contract as the sequential builder.
struct NodeLinks {
    l0: NeighborBuf,
    upper: NeighborBuf,
}

/// Per-worker reusable scratch: epoch-stamped visited set plus the two
/// search heaps. Avoids per-insert allocation in the build loop.
struct BuildScratch {
    visited: Vec<u32>,
    epoch: u32,
    candidates: BinaryHeap<Reverse<DistPair>>,
    results: BinaryHeap<DistPair>,
    neighbor_buf: NeighborBuf,
}

impl BuildScratch {
    fn new(n: usize) -> Self {
        Self {
            visited: vec![0u32; n],
            epoch: 0,
            candidates: BinaryHeap::with_capacity(256),
            results: BinaryHeap::with_capacity(256),
            neighbor_buf: NeighborBuf::new(),
        }
    }

    fn next_epoch(&mut self) -> u32 {
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            // Epoch wrapped: reset stamps so stale marks can't alias.
            self.visited.iter_mut().for_each(|v| *v = 0);
            self.epoch = 1;
        }
        self.epoch
    }
}

/// Shared build state across worker threads.
struct SharedGraph {
    m: u8,
    m0: u8,
    ef_construction: u16,
    levels: Vec<u8>,
    nodes: Vec<Mutex<NodeLinks>>,
    /// (entry_point, max_level)
    entry: RwLock<(u32, u8)>,
}

impl SharedGraph {
    /// Copy `node`'s neighbor slots at `level` into `buf` under the node
    /// lock. Distances are computed by the caller AFTER the lock drops.
    fn copy_neighbors(&self, node: u32, level: usize, buf: &mut NeighborBuf) {
        buf.clear();
        let links = self.nodes[node as usize].lock();
        let slots: &[u32] = if level == 0 {
            &links.l0
        } else {
            let m = self.m as usize;
            let start = (level - 1) * m;
            let end = start + m;
            if end > links.upper.len() {
                return;
            }
            &links.upper[start..end]
        };
        for &nb in slots {
            if nb == SENTINEL {
                break;
            }
            buf.push(nb);
        }
    }

    /// Overwrite `node`'s slots at `level` with `selected` (SENTINEL-padding
    /// the rest).
    fn set_neighbors(&self, node: u32, level: usize, selected: &[(f32, u32)]) {
        let mut links = self.nodes[node as usize].lock();
        let (slots, max_nb): (&mut [u32], usize) = if level == 0 {
            let m0 = self.m0 as usize;
            (&mut links.l0, m0)
        } else {
            let m = self.m as usize;
            let start = (level - 1) * m;
            let end = start + m;
            if end > links.upper.len() {
                return;
            }
            (&mut links.upper[start..end], m)
        };
        for (i, slot) in slots.iter_mut().enumerate().take(max_nb) {
            *slot = if i < selected.len() {
                selected[i].1
            } else {
                SENTINEL
            };
        }
    }

    /// Add `new_id` to `target`'s neighbor list at `level`, re-pruning with
    /// the diversity heuristic when the list is full. Distances are computed
    /// while holding `target`'s lock (bounded: ≤ M0+1 calls) — this keeps
    /// the read-prune-write sequence atomic per node without a second lock.
    fn add_neighbor_with_prune(
        &self,
        target: u32,
        new_id: u32,
        level: usize,
        dist_fn: &(impl Fn(u32, u32) -> f32 + Sync),
    ) {
        let mut links = self.nodes[target as usize].lock();
        let (slots, max_nb): (&mut [u32], usize) = if level == 0 {
            let m0 = self.m0 as usize;
            (&mut links.l0, m0)
        } else {
            let m = self.m as usize;
            let start = (level - 1) * m;
            let end = start + m;
            if end > links.upper.len() {
                return;
            }
            (&mut links.upper[start..end], m)
        };

        for slot in slots.iter_mut() {
            if *slot == SENTINEL {
                *slot = new_id;
                return;
            }
            if *slot == new_id {
                return; // already linked (possible via concurrent re-prune)
            }
        }

        // Full: re-select with the diversity heuristic over current + new.
        // M0 caps at 32 on this path (compaction builds with M=16).
        let mut combined: SmallVec<[(f32, u32); 33]> = SmallVec::new();
        for &nb in slots.iter() {
            combined.push((dist_fn(target, nb), nb));
        }
        combined.push((dist_fn(target, new_id), new_id));
        combined.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.1.cmp(&b.1))
        });
        let pruned = select_neighbors_heuristic(&combined, max_nb, dist_fn);
        for (i, slot) in slots.iter_mut().enumerate().take(max_nb) {
            *slot = if i < pruned.len() {
                pruned[i].1
            } else {
                SENTINEL
            };
        }
    }

    /// Beam search one layer for `ef` nearest neighbors of `node_id`.
    /// Mirrors the sequential `search_layer` but reads adjacency through
    /// `copy_neighbors` (per-node lock, copy-then-release).
    fn search_layer(
        &self,
        node_id: u32,
        entry: u32,
        level: usize,
        ef: usize,
        dist_fn: &(impl Fn(u32, u32) -> f32 + Sync),
        scratch: &mut BuildScratch,
    ) -> Vec<(f32, u32)> {
        let epoch = scratch.next_epoch();
        scratch.candidates.clear();
        scratch.results.clear();

        let entry_dist = dist_fn(node_id, entry);
        scratch
            .candidates
            .push(Reverse(DistPair(entry_dist, entry)));
        scratch.results.push(DistPair(entry_dist, entry));
        scratch.visited[entry as usize] = epoch;

        // Split-borrow the scratch fields so the neighbor buffer can be
        // reused inside the loop while the heaps stay live.
        let BuildScratch {
            visited,
            candidates,
            results,
            neighbor_buf,
            ..
        } = scratch;

        while let Some(Reverse(DistPair(c_dist, c_id))) = candidates.pop() {
            if results.len() >= ef {
                if let Some(&DistPair(worst, _)) = results.peek() {
                    if c_dist > worst {
                        break;
                    }
                }
            }

            self.copy_neighbors(c_id, level, neighbor_buf);
            for &nb in neighbor_buf.iter() {
                if visited[nb as usize] == epoch {
                    continue;
                }
                visited[nb as usize] = epoch;
                let d = dist_fn(node_id, nb);
                let should_add = results.len() < ef || d < results.peek().map_or(f32::MAX, |p| p.0);
                if should_add {
                    candidates.push(Reverse(DistPair(d, nb)));
                    results.push(DistPair(d, nb));
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }

        let mut out: Vec<(f32, u32)> = results.drain().map(|DistPair(d, id)| (d, id)).collect();
        out.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.1.cmp(&b.1))
        });
        out
    }

    /// Insert one node. Never called twice for the same `node_id`; safe to
    /// run concurrently for distinct ids.
    fn insert(
        &self,
        node_id: u32,
        dist_fn: &(impl Fn(u32, u32) -> f32 + Sync),
        scratch: &mut BuildScratch,
    ) {
        let level = self.levels[node_id as usize];
        let (mut current, max_level) = *self.entry.read();

        // Greedy descent through levels above the new node's level.
        if max_level as usize > level as usize {
            let mut current_dist = dist_fn(node_id, current);
            for lev in (level as usize + 1..=max_level as usize).rev() {
                loop {
                    let mut improved = false;
                    self.copy_neighbors(current, lev, &mut scratch.neighbor_buf);
                    // Move the buffer out so search scratch stays borrowable.
                    let nbs = std::mem::take(&mut scratch.neighbor_buf);
                    for &nb in &nbs {
                        let d = dist_fn(node_id, nb);
                        if d < current_dist {
                            current = nb;
                            current_dist = d;
                            improved = true;
                        }
                    }
                    scratch.neighbor_buf = nbs;
                    if !improved {
                        break;
                    }
                }
            }
        }

        // Connect at each level from min(level, max_level) down to 0.
        let insert_from = level.min(max_level);
        for lev in (0..=insert_from as usize).rev() {
            let max_nb = if lev == 0 {
                self.m0 as usize
            } else {
                self.m as usize
            };
            let candidates = self.search_layer(
                node_id,
                current,
                lev,
                self.ef_construction as usize,
                dist_fn,
                scratch,
            );
            let selected = select_neighbors_heuristic(&candidates, max_nb, dist_fn);

            self.set_neighbors(node_id, lev, &selected);
            for &(_, nb_id) in &selected {
                self.add_neighbor_with_prune(nb_id, node_id, lev, dist_fn);
            }

            if let Some(&(_, nearest)) = candidates.first() {
                current = nearest;
            }
        }

        // Promote entry point if this node's level exceeds the current max.
        if level > max_level {
            let mut ep = self.entry.write();
            if level > ep.1 {
                *ep = (node_id, level);
            }
        }
    }
}

/// Reconnect layer-0 orphans left by concurrent back-link pruning.
///
/// Under concurrency, a node's back-links can all be pruned away by racing
/// re-prunes, leaving it with out-edges but zero in-edges from the reachable
/// component (~0.1–0.2% of nodes measured at 3K) — an unreachable node is a
/// permanent recall loss. Repair: BFS from the entry point over layer 0;
/// every unreached node gets force-linked from its nearest reached
/// out-neighbor (replacing that neighbor's farthest slot if full), which
/// makes it — and anything downstream of it — reachable. Loops until the
/// reachable set stops growing; each round strictly shrinks the orphan set,
/// so termination is guaranteed.
fn repair_connectivity(
    n: u32,
    m0: u8,
    entry_point: u32,
    layer0_flat: &mut [u32],
    dist_fn: &(impl Fn(u32, u32) -> f32 + Sync),
) {
    let m0 = m0 as usize;
    let nn = n as usize;
    let mut reached = vec![false; nn];
    let mut queue: Vec<u32> = Vec::with_capacity(nn);

    loop {
        // (Re-)BFS from entry over current edges, reusing marks.
        queue.clear();
        if !reached[entry_point as usize] {
            reached[entry_point as usize] = true;
            queue.push(entry_point);
        } else {
            // Subsequent rounds: restart frontier from every reached node's
            // still-unvisited neighbors by re-scanning reached nodes.
            for id in 0..nn {
                if reached[id] {
                    queue.push(id as u32);
                }
            }
        }
        let mut head = 0usize;
        while head < queue.len() {
            let cur = queue[head] as usize;
            head += 1;
            for &nb in &layer0_flat[cur * m0..cur * m0 + m0] {
                if nb == SENTINEL {
                    break;
                }
                if !reached[nb as usize] {
                    reached[nb as usize] = true;
                    queue.push(nb);
                }
            }
        }

        let orphans: Vec<u32> = (0..n).filter(|&id| !reached[id as usize]).collect();
        if orphans.is_empty() {
            return;
        }

        let mut repaired_any = false;
        for u in orphans {
            // Nearest REACHED node among u's own out-edges.
            let u_edges = &layer0_flat[u as usize * m0..u as usize * m0 + m0];
            let mut best: Option<(f32, u32)> = None;
            for &nb in u_edges {
                if nb == SENTINEL {
                    break;
                }
                if !reached[nb as usize] {
                    continue;
                }
                let d = dist_fn(u, nb);
                if best.is_none_or(|(bd, _)| d < bd) {
                    best = Some((d, nb));
                }
            }
            let Some((_, target)) = best else {
                // No reached out-neighbor this round; a later round will
                // pick it up once its neighborhood is repaired.
                continue;
            };
            // Force-link target -> u: free slot if any, else replace the
            // farthest neighbor (unconditional — connectivity beats one
            // marginally-better edge on a handful of nodes).
            let t_edges = &mut layer0_flat[target as usize * m0..target as usize * m0 + m0];
            let mut placed = false;
            for slot in t_edges.iter_mut() {
                if *slot == SENTINEL {
                    *slot = u;
                    placed = true;
                    break;
                }
            }
            if !placed {
                let mut worst = (f32::MIN, 0usize);
                for (i, &nb) in t_edges.iter().enumerate() {
                    let d = dist_fn(target, nb);
                    if d > worst.0 {
                        worst = (d, i);
                    }
                }
                t_edges[worst.1] = u;
            }
            repaired_any = true;
        }
        if !repaired_any {
            // Remaining orphans have no reached out-neighbors at all (a
            // fully isolated cluster) — bail instead of spinning. In
            // practice every node has ≥1 out-edge into the warmup core, so
            // this branch is effectively unreachable.
            return;
        }
    }
}

/// Pre-generate per-node levels with the exact LCG the sequential builder
/// uses (`HnswBuilder::random_level`) so a given seed produces the same
/// level sequence in both builders.
fn generate_levels(n: u32, m: u8, seed: u64) -> Vec<u8> {
    let ml = 1.0 / (m as f64).ln();
    let mut state = seed;
    let mut levels = Vec::with_capacity(n as usize);
    for _ in 0..n {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let uniform = (state >> 33) as f64 / (1u64 << 31) as f64;
        let level = if uniform <= 0.0 {
            0
        } else {
            ((-uniform.ln() * ml).floor() as u8).min(32)
        };
        levels.push(level);
    }
    levels
}

/// Build an HNSW graph over `n` pre-encoded vectors using `threads` worker
/// threads inserting concurrently into one shared graph.
///
/// `dist_fn(a, b)` must return the distance between vectors `a` and `b`
/// (both in `0..n`) and be safe to call from multiple threads.
///
/// Finalization (BFS reorder → [`HnswGraph`]) matches the sequential
/// builder exactly, so the output is drop-in for every downstream consumer
/// (search, persistence, GraphUnion merge).
pub fn build_parallel(
    n: u32,
    m: u8,
    ef_construction: u16,
    seed: u64,
    threads: usize,
    dist_fn: &(impl Fn(u32, u32) -> f32 + Sync),
    bytes_per_code: u32,
) -> HnswGraph {
    let m0 = m * 2;
    if n == 0 {
        return HnswGraph::new(
            0,
            m,
            m0,
            0,
            0,
            crate::vector::aligned_buffer::AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            bytes_per_code,
        );
    }

    let levels = generate_levels(n, m, seed);
    let nodes: Vec<Mutex<NodeLinks>> = levels
        .iter()
        .map(|&lvl| {
            let mut l0 = NeighborBuf::new();
            l0.resize(m0 as usize, SENTINEL);
            let mut upper = NeighborBuf::new();
            upper.resize(lvl as usize * m as usize, SENTINEL);
            Mutex::new(NodeLinks { l0, upper })
        })
        .collect();

    let shared = SharedGraph {
        m,
        m0,
        ef_construction,
        entry: RwLock::new((0, levels[0])),
        levels,
        nodes,
    };

    // Sequential warmup from the coordinating thread (node 0 is the initial
    // entry point and needs no insertion work).
    let warmup = SEQ_WARMUP.min(n);
    {
        let mut scratch = BuildScratch::new(n as usize);
        for id in 1..warmup {
            shared.insert(id, dist_fn, &mut scratch);
        }
    }

    // Concurrent phase: dynamic work distribution via an atomic cursor.
    if warmup < n {
        let cursor = AtomicU32::new(warmup);
        let workers = threads.max(1).min(n as usize - warmup as usize).max(1);
        let total_cores = crate::shard::numa::system_parallelism();
        // Stagger concurrent builds (multi-shard bulk loads compact several
        // segments at once): each build starts its core round-robin at a
        // different offset so capped builds (workers < cores) don't all pile
        // onto cores 0..workers while higher cores idle.
        static BUILD_OFFSET: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let base = BUILD_OFFSET.fetch_add(workers, Ordering::Relaxed);
        let cursor = &cursor;
        let shared = &shared;
        std::thread::scope(|s| {
            for k in 0..workers {
                s.spawn(move || {
                    // Escape the inherited affinity mask: threads spawned
                    // from a core-pinned shard/compactor thread inherit its
                    // SINGLE-core mask on Linux, which would serialize all
                    // workers onto one core (measured: "parallel" build ran
                    // at exactly sequential speed). Spread round-robin
                    // across the machine instead. No-op on macOS.
                    crate::shard::numa::pin_worker_to_core((base + k) % total_cores);
                    let mut scratch = BuildScratch::new(n as usize);
                    loop {
                        let id = cursor.fetch_add(1, Ordering::Relaxed);
                        if id >= n {
                            break;
                        }
                        shared.insert(id, dist_fn, &mut scratch);
                    }
                });
            }
        });
    }

    // ── Finalize: flatten into the sequential builder's layout, then the
    // same BFS reorder + HnswGraph construction. ──
    let (entry_point, max_level) = *shared.entry.read();
    let m0_usize = m0 as usize;
    let mut layer0_flat = vec![SENTINEL; n as usize * m0_usize];
    let mut upper_layers: Vec<SmallVec<[u32; 32]>> = Vec::with_capacity(n as usize);
    for (i, node) in shared.nodes.into_iter().enumerate() {
        let links = node.into_inner();
        layer0_flat[i * m0_usize..i * m0_usize + m0_usize].copy_from_slice(&links.l0);
        upper_layers.push(links.upper);
    }

    repair_connectivity(n, m0, entry_point, &mut layer0_flat, dist_fn);

    let (bfs_order, bfs_inverse) = bfs_reorder(n, m0, entry_point, &layer0_flat);
    let layer0 = rearrange_layer0(n, m0, &layer0_flat, &bfs_order, &bfs_inverse);
    let bfs_entry = bfs_order[entry_point as usize];

    HnswGraph::new(
        n,
        m,
        m0,
        bfs_entry,
        max_level,
        layer0,
        bfs_order,
        bfs_inverse,
        upper_layers,
        shared.levels,
        bytes_per_code,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::hnsw::build::HnswBuilder;

    /// Deterministic pseudo-random vectors in [-1, 1] (same generator as
    /// `build.rs` tests).
    fn lcg_vecs(n: usize, dim: usize, seed: u32) -> Vec<Vec<f32>> {
        let mut s = seed;
        (0..n)
            .map(|_| {
                (0..dim)
                    .map(|_| {
                        s = s.wrapping_mul(1664525).wrapping_add(1013904223);
                        (s as f32) / (u32::MAX as f32) * 2.0 - 1.0
                    })
                    .collect()
            })
            .collect()
    }

    fn l2(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
    }

    /// Brute-force top-k ids for query index `q` (excluding itself).
    fn brute_topk(vecs: &[Vec<f32>], q: usize, k: usize) -> Vec<u32> {
        let mut d: Vec<(f32, u32)> = (0..vecs.len())
            .filter(|&i| i != q)
            .map(|i| (l2(&vecs[q], &vecs[i]), i as u32))
            .collect();
        d.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        d.truncate(k);
        d.into_iter().map(|(_, i)| i).collect()
    }

    /// Graph-based top-k via layer-0 beam search from the entry point,
    /// operating in BFS (post-reorder) id space, mapped back to original ids.
    fn graph_topk(graph: &HnswGraph, vecs: &[Vec<f32>], q: usize, k: usize, ef: usize) -> Vec<u32> {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let n = graph.num_nodes() as usize;
        let dist_bfs = |bfs: u32| {
            let orig = graph.to_original(bfs) as usize;
            l2(&vecs[q], &vecs[orig])
        };
        let entry = graph.entry_point();
        let mut visited = vec![false; n];
        let mut cand: BinaryHeap<Reverse<DistPair>> = BinaryHeap::new();
        let mut res: BinaryHeap<DistPair> = BinaryHeap::new();
        visited[entry as usize] = true;
        let ed = dist_bfs(entry);
        cand.push(Reverse(DistPair(ed, entry)));
        res.push(DistPair(ed, entry));
        while let Some(Reverse(DistPair(cd, cid))) = cand.pop() {
            if res.len() >= ef {
                if let Some(&DistPair(w, _)) = res.peek() {
                    if cd > w {
                        break;
                    }
                }
            }
            for &nb in graph.neighbors_l0(cid) {
                if nb == SENTINEL {
                    break;
                }
                if visited[nb as usize] {
                    continue;
                }
                visited[nb as usize] = true;
                let d = dist_bfs(nb);
                if res.len() < ef || d < res.peek().map_or(f32::MAX, |p| p.0) {
                    cand.push(Reverse(DistPair(d, nb)));
                    res.push(DistPair(d, nb));
                    if res.len() > ef {
                        res.pop();
                    }
                }
            }
        }
        let mut out: Vec<(f32, u32)> = res
            .into_vec()
            .into_iter()
            .map(|DistPair(d, id)| (d, graph.to_original(id)))
            .collect();
        out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        out.truncate(k);
        out.into_iter()
            .filter(|&(_, id)| id as usize != q)
            .map(|(_, id)| id)
            .collect()
    }

    fn recall_at_k(graph: &HnswGraph, vecs: &[Vec<f32>], nq: usize, k: usize, ef: usize) -> f32 {
        let mut total = 0.0f32;
        let step = (vecs.len() / nq).max(1);
        let mut queries = 0;
        for q in (0..vecs.len()).step_by(step).take(nq) {
            let gt = brute_topk(vecs, q, k);
            let got = graph_topk(graph, vecs, q, k + 1, ef);
            let hits = gt.iter().filter(|id| got.contains(id)).count();
            total += hits as f32 / k as f32;
            queries += 1;
        }
        total / queries as f32
    }

    #[test]
    fn test_generate_levels_matches_sequential_lcg() {
        // The parallel builder must assign the exact level sequence the
        // sequential builder would for the same seed.
        let n = 500u32;
        let seed = 987_654_321u64;
        let expected = {
            let mut builder = HnswBuilder::new(16, 8, seed);
            for _ in 0..n {
                builder.insert(|_, _| 0.0);
            }
            builder.levels_for_test()
        };
        let got = generate_levels(n, 16, seed);
        assert_eq!(got, expected);
    }

    #[test]
    fn test_parallel_build_all_reachable() {
        let n = 3000usize;
        let vecs = lcg_vecs(n, 24, 77);
        let dist = |a: u32, b: u32| l2(&vecs[a as usize], &vecs[b as usize]);
        let graph = build_parallel(n as u32, 16, 100, 42, 4, &dist, 8);
        assert_eq!(graph.num_nodes(), n as u32);

        // BFS from entry must reach (almost) every node; HNSW guarantees
        // connectivity through layer 0 in practice.
        let mut visited = vec![false; n];
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(graph.entry_point());
        visited[graph.entry_point() as usize] = true;
        let mut count = 1usize;
        while let Some(pos) = queue.pop_front() {
            for &nb in graph.neighbors_l0(pos) {
                if nb == SENTINEL {
                    break;
                }
                assert!((nb as usize) < n, "neighbor id out of range");
                if !visited[nb as usize] {
                    visited[nb as usize] = true;
                    count += 1;
                    queue.push_back(nb);
                }
            }
        }
        assert_eq!(
            count, n,
            "repair_connectivity must leave every node reachable from entry"
        );
    }

    #[test]
    fn test_parallel_recall_parity_with_sequential() {
        // Recall of the concurrently-built graph must match the sequential
        // builder's within noise. 4K × 32d keeps the test under a second.
        let n = 4096usize;
        let vecs = lcg_vecs(n, 32, 12345);
        let dist = |a: u32, b: u32| l2(&vecs[a as usize], &vecs[b as usize]);

        let mut seq = HnswBuilder::new(16, 100, 42);
        for _ in 0..n {
            seq.insert(dist);
        }
        let seq_graph = seq.build(8);
        let par_graph = build_parallel(n as u32, 16, 100, 42, 4, &dist, 8);

        let seq_recall = recall_at_k(&seq_graph, &vecs, 64, 10, 64);
        let par_recall = recall_at_k(&par_graph, &vecs, 64, 10, 64);
        assert!(
            par_recall >= seq_recall - 0.02,
            "parallel recall {par_recall:.4} fell more than 0.02 below sequential {seq_recall:.4}"
        );
        assert!(
            par_recall >= 0.85,
            "parallel recall {par_recall:.4} below absolute floor"
        );
    }

    #[test]
    fn test_parallel_single_thread_and_tiny_inputs() {
        // threads=1 and n < warmup exercise the pure-sequential fallback
        // inside build_parallel.
        let vecs = lcg_vecs(64, 8, 5);
        let dist = |a: u32, b: u32| l2(&vecs[a as usize], &vecs[b as usize]);
        let graph = build_parallel(64, 16, 50, 7, 1, &dist, 8);
        assert_eq!(graph.num_nodes(), 64);

        let empty = build_parallel(0, 16, 50, 7, 4, &|_, _| 0.0, 8);
        assert_eq!(empty.num_nodes(), 0);

        let one = build_parallel(1, 16, 50, 7, 4, &|_, _| 0.0, 8);
        assert_eq!(one.num_nodes(), 1);
    }
}
