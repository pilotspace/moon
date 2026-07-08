//! CSR row-space BFS fast path.
//!
//! When a graph is fully frozen into a SINGLE CSR segment (steady state
//! after compaction) and the mutable tier is empty, BFS does not need
//! NodeKey hashing at all: rows are dense `u32`s, so the visited set is a
//! bitmap, adjacency is two slice lookups (`row_offsets`/`col_indices`),
//! and NodeKey materialization happens once per RESULT row instead of once
//! per EDGE probe. On top of the row space this module adds what the old
//! "ParallelBfs" could not (its reader borrowed `!Send` MemGraph, so
//! neighbor expansion ran sequentially and threads only merged sets):
//!
//! - **True parallel expansion**: `CsrStorage` is `Send + Sync`, so
//!   frontier chunks expand on worker threads against a shared
//!   `AtomicU64` visited bitmap (`fetch_or` test-and-set).
//! - **Direction-optimizing BFS** (Beamer α/β heuristic): when the
//!   frontier's out-edge count dwarfs the unexplored remainder, a level
//!   switches from top-down push to bottom-up pull over the v3-2
//!   IncomingIndex (each unvisited row scans its in-edges for a frontier
//!   parent). Outgoing direction only — pull for Incoming/Both would
//!   need the transposed heuristic and buys little.
//!
//! Semantics parity with `SegmentMergeReader`-based BFS (the fallback):
//! same edge-validity bitmap, same edge-type filter, same node visibility
//! (`deleted_lsn <= snapshot`), same `MergedNeighbor` materialization for
//! CSR edges (placeholder edge key, weight 1.0, segment-LSN timestamp).
//! Within a level, PARALLEL discovery order is unspecified (the visited
//! set and depths are deterministic; sequential levels keep FIFO order).
//!
//! MULTI-segment frozen graphs (W2-6) also stay in row space: cross-segment
//! edges cannot exist (freeze retains them as mutable-tier delta edges, and
//! the gate requires an empty mutable tier), so the only boundary is a
//! NodeKey resident in several segments (stale copy-up rows) — handled by a
//! key-level sync per emitted node. See `multi_row_bfs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::graph::csr::CsrStorage;
use crate::graph::memgraph::MemGraph;
use crate::graph::traversal::{BfsResult, MergedNeighbor, TraversalError};
use crate::graph::types::{Direction, EdgeMeta, NodeKey};

/// Frontier size at which a level's expansion goes parallel.
const PARALLEL_LEVEL_THRESHOLD: usize = 256;
/// Beamer top-down → bottom-up switch: pull when m_f > m_u / ALPHA.
const ALPHA: u64 = 14;
/// Beamer bottom-up → top-down switch: push when n_f < n / BETA.
const BETA: usize = 24;
/// Upper bound on worker threads for a parallel level.
const MAX_WORKERS: usize = 8;

/// Push/pull selection, exposed for tests (`Auto` in production).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BfsMode {
    /// Beamer heuristic per level.
    Auto,
    /// Always top-down (frontier expands its out-edges).
    ForcePush,
    /// Always bottom-up (unvisited rows scan in-edges for frontier parents).
    ForcePull,
}

/// Row-space BFS if the fast-path gate holds, else `None` (caller falls
/// back to the `SegmentMergeReader` path).
///
/// Gate: mutable tier absent-or-empty ∧ every CSR segment visible at
/// `snapshot_lsn`. A single segment takes the full fast path (parallel
/// levels + direction-optimizing pull); multiple segments take the W2-6
/// multi-segment row path (sequential push, per-segment bitmaps, key-level
/// boundary sync). Each condition the reader path handles generically that
/// row space cannot (delta edges, a future segment needing per-segment
/// snapshot skips) is a reason NOT to take the fast path — never a
/// behavior fork.
#[allow(clippy::too_many_arguments)]
pub fn try_row_bfs(
    memgraph: Option<&MemGraph>,
    csr_segments: &[Arc<CsrStorage>],
    direction: Direction,
    snapshot_lsn: u64,
    edge_type_filter: Option<u16>,
    start: NodeKey,
    depth_limit: u32,
    frontier_cap: usize,
) -> Option<Result<BfsResult, TraversalError>> {
    if let Some(mg) = memgraph {
        if mg.node_count() != 0 || mg.edge_count() != 0 {
            return None;
        }
    }
    if csr_segments
        .iter()
        .any(|seg| seg.created_lsn() > snapshot_lsn)
    {
        return None;
    }
    match csr_segments {
        [] => None,
        [seg] => Some(row_bfs(
            seg,
            direction,
            snapshot_lsn,
            edge_type_filter,
            start,
            depth_limit,
            frontier_cap,
            BfsMode::Auto,
        )),
        segs => Some(multi_row_bfs(
            segs,
            direction,
            snapshot_lsn,
            edge_type_filter,
            start,
            depth_limit,
            frontier_cap,
        )),
    }
}

/// Multi-segment row-space BFS (W2-6).
///
/// The single-segment path keeps the parallel + direction-optimizing
/// machinery; with several segments (and the gate's EMPTY mutable tier —
/// so no delta edges exist) expansion is push-only and sequential, but
/// still row-space: per-segment dense visited bitmaps, slice adjacency,
/// NodeKey materialization once per RESULT node.
///
/// Cross-segment boundary: a NodeKey can be resident in several segments
/// (a re-frozen copy-up shadow leaves a stale row in the older segment).
/// On first emission every resident row is marked visited and enqueued in
/// its own segment's frontier, so the node's edges from EVERY segment
/// expand — parity with `SegmentMergeReader`, which unions a node's CSR
/// adjacency across all segments. `emitted` (ffi-keyed) is the
/// authoritative once-per-node dedup; the bitmaps remain the per-edge-probe
/// fast reject.
#[allow(clippy::too_many_arguments)]
fn multi_row_bfs(
    segs: &[Arc<CsrStorage>],
    direction: Direction,
    snapshot_lsn: u64,
    edge_type_filter: Option<u16>,
    start: NodeKey,
    depth_limit: u32,
    frontier_cap: usize,
) -> Result<BfsResult, TraversalError> {
    use slotmap::Key;

    let visited: Vec<Vec<AtomicU64>> = segs
        .iter()
        .map(|s| {
            (0..(s.node_count() as usize).div_ceil(64))
                .map(|_| AtomicU64::new(0))
                .collect()
        })
        .collect();

    let mut emitted: crate::graph::fasthash::FxHashSet<u64> =
        crate::graph::fasthash::FxHashSet::default();
    let mut frontier: Vec<Vec<u32>> = vec![Vec::new(); segs.len()];
    for (si, seg) in segs.iter().enumerate() {
        if let Some(row) = seg.lookup_node(start) {
            test_and_set(&visited[si], row);
            frontier[si].push(row);
        }
    }
    if frontier.iter().all(|f| f.is_empty()) {
        return Err(TraversalError::NodeNotFound);
    }
    emitted.insert(start.data().as_ffi());
    for seg in segs {
        seg.madvise_sequential();
    }

    let mut result: Vec<(NodeKey, u32, Option<MergedNeighbor>)> = vec![(start, 0, None)];
    let mut visited_count: usize = 1;
    let mut depth: u32 = 0;

    while depth < depth_limit && frontier.iter().any(|f| !f.is_empty()) {
        let next_depth = depth + 1;

        let mut discovered: Vec<(usize, u32, EdgeMeta, u64)> = Vec::new();
        for (si, seg) in segs.iter().enumerate() {
            if frontier[si].is_empty() {
                continue;
            }
            let node_meta = seg.node_meta();
            let visited_si = &visited[si];
            for &row in &frontier[si] {
                let mut on_edge = |other: u32, meta: EdgeMeta, created_ms: u64| {
                    if let Some(filter) = edge_type_filter {
                        if meta.edge_type != filter {
                            return;
                        }
                    }
                    let Some(meta_n) = node_meta.get(other as usize) else {
                        return;
                    };
                    if !row_visible(meta_n, snapshot_lsn) {
                        return;
                    }
                    if test_and_set(visited_si, other) {
                        discovered.push((si, other, meta, created_ms));
                    }
                };
                if direction != Direction::Incoming {
                    seg.for_each_neighbor_edge_ms(row, &mut on_edge);
                }
                if direction != Direction::Outgoing {
                    seg.for_each_incoming_edge_ms(row, &mut on_edge);
                }
            }
        }

        let mut next: Vec<Vec<u32>> = vec![Vec::new(); segs.len()];
        for (si, row, meta, created_ms) in discovered {
            let ext = segs[si].node_meta()[row as usize].external_id;
            if !emitted.insert(ext) {
                continue; // stale copy of an already-emitted node
            }
            let key: NodeKey = slotmap::KeyData::from_ffi(ext).into();
            result.push((
                key,
                next_depth,
                Some(MergedNeighbor {
                    node: key,
                    edge: slotmap::KeyData::from_ffi(0).into(),
                    edge_type: meta.edge_type,
                    weight: 1.0,
                    timestamp: segs[si].created_lsn(),
                    created_ms,
                }),
            ));
            visited_count += 1;
            if visited_count >= frontier_cap {
                return Err(TraversalError::FrontierCapExceeded {
                    cap: frontier_cap,
                    depth: next_depth,
                });
            }
            // Boundary sync: enqueue EVERY resident row for this key so its
            // adjacency in every segment expands next level.
            for (ti, tseg) in segs.iter().enumerate() {
                let trow = if ti == si {
                    Some(row)
                } else {
                    tseg.lookup_node(key)
                };
                if let Some(trow) = trow {
                    test_and_set(&visited[ti], trow);
                    next[ti].push(trow);
                }
            }
        }

        frontier = next;
        depth = next_depth;
    }

    Ok(BfsResult { visited: result })
}

/// A node discovered during one level: (row, discovery edge, edge wall-ms).
type Discovery = (u32, EdgeMeta, u64);

#[allow(clippy::too_many_arguments)]
pub(crate) fn row_bfs(
    seg: &CsrStorage,
    direction: Direction,
    snapshot_lsn: u64,
    edge_type_filter: Option<u16>,
    start: NodeKey,
    depth_limit: u32,
    frontier_cap: usize,
    mode: BfsMode,
) -> Result<BfsResult, TraversalError> {
    let Some(start_row) = seg.lookup_node(start) else {
        return Err(TraversalError::NodeNotFound);
    };
    seg.madvise_sequential();

    let n = seg.node_count() as usize;
    let node_meta = seg.node_meta();
    let total_edges = seg.edge_count() as u64;

    let visited: Vec<AtomicU64> = (0..n.div_ceil(64)).map(|_| AtomicU64::new(0)).collect();
    test_and_set(&visited, start_row);

    let mut result: Vec<(NodeKey, u32, Option<MergedNeighbor>)> = vec![(start, 0, None)];
    let mut frontier: Vec<u32> = vec![start_row];
    let mut visited_count: usize = 1;
    let mut edges_explored: u64 = 0;
    let mut depth: u32 = 0;

    while !frontier.is_empty() && depth < depth_limit {
        let next_depth = depth + 1;

        // Direction-optimizing choice (Outgoing only; see module docs).
        let use_pull = match mode {
            BfsMode::ForcePush => false,
            BfsMode::ForcePull => direction == Direction::Outgoing,
            BfsMode::Auto => {
                direction == Direction::Outgoing && frontier.len() >= n / BETA.max(1) && {
                    let ro = seg.row_offsets();
                    let m_f: u64 = frontier
                        .iter()
                        .map(|&r| u64::from(ro[r as usize + 1] - ro[r as usize]))
                        .sum();
                    let m_u = total_edges.saturating_sub(edges_explored);
                    m_f > m_u / ALPHA
                }
            }
        };

        let discovered: Vec<Discovery> = if use_pull {
            expand_pull(seg, &frontier, &visited, snapshot_lsn, edge_type_filter, n)
        } else {
            let ro = seg.row_offsets();
            edges_explored += frontier
                .iter()
                .map(|&r| u64::from(ro[r as usize + 1] - ro[r as usize]))
                .sum::<u64>();
            expand_push(
                seg,
                &frontier,
                &visited,
                direction,
                snapshot_lsn,
                edge_type_filter,
            )
        };

        frontier = Vec::with_capacity(discovered.len());
        for (row, meta, created_ms) in discovered {
            let meta_n = &node_meta[row as usize];
            let key: NodeKey = slotmap::KeyData::from_ffi(meta_n.external_id).into();
            result.push((
                key,
                next_depth,
                Some(MergedNeighbor {
                    node: key,
                    edge: slotmap::KeyData::from_ffi(0).into(),
                    edge_type: meta.edge_type,
                    weight: 1.0,
                    timestamp: seg.created_lsn(),
                    created_ms,
                }),
            ));
            frontier.push(row);
            visited_count += 1;
            if visited_count >= frontier_cap {
                return Err(TraversalError::FrontierCapExceeded {
                    cap: frontier_cap,
                    depth: next_depth,
                });
            }
        }

        depth = next_depth;
    }

    Ok(BfsResult { visited: result })
}

/// Whether a CSR row is visible at the snapshot (mirrors the reader path's
/// deleted-node check exactly).
#[inline]
fn row_visible(meta: &crate::graph::types::NodeMeta, snapshot_lsn: u64) -> bool {
    !(meta.deleted_lsn != u64::MAX && meta.deleted_lsn <= snapshot_lsn)
}

/// Atomically set a row's visited bit; true iff it was previously clear.
#[inline]
fn test_and_set(bits: &[AtomicU64], row: u32) -> bool {
    let mask = 1u64 << (row % 64);
    bits[(row / 64) as usize].fetch_or(mask, Ordering::Relaxed) & mask == 0
}

#[inline]
fn is_set(bits: &[AtomicU64], row: u32) -> bool {
    bits[(row / 64) as usize].load(Ordering::Relaxed) & (1u64 << (row % 64)) != 0
}

/// Top-down level: every frontier row expands its (direction-appropriate)
/// edges; targets that pass the filter + visibility and win the visited
/// test-and-set are discovered. Parallel over frontier chunks when large.
fn expand_push(
    seg: &CsrStorage,
    frontier: &[u32],
    visited: &[AtomicU64],
    direction: Direction,
    snapshot_lsn: u64,
    edge_type_filter: Option<u16>,
) -> Vec<Discovery> {
    let expand_chunk = |chunk: &[u32], out: &mut Vec<Discovery>| {
        let node_meta = seg.node_meta();
        for &row in chunk {
            let mut on_edge = |other_row: u32, meta: EdgeMeta, created_ms: u64| {
                if let Some(filter) = edge_type_filter {
                    if meta.edge_type != filter {
                        return;
                    }
                }
                let Some(meta_n) = node_meta.get(other_row as usize) else {
                    return;
                };
                if !row_visible(meta_n, snapshot_lsn) {
                    return;
                }
                if test_and_set(visited, other_row) {
                    out.push((other_row, meta, created_ms));
                }
            };
            if direction != Direction::Incoming {
                seg.for_each_neighbor_edge_ms(row, &mut on_edge);
            }
            if direction != Direction::Outgoing {
                seg.for_each_incoming_edge_ms(row, &mut on_edge);
            }
        }
    };

    if frontier.len() < PARALLEL_LEVEL_THRESHOLD {
        let mut out = Vec::new();
        expand_chunk(frontier, &mut out);
        return out;
    }

    // Warm the lazy incoming index OUTSIDE the worker threads so the
    // OnceLock build isn't serialized inside the scope.
    if direction != Direction::Outgoing {
        seg.for_each_incoming_edge_ms(0, |_, _, _| {});
    }

    let workers = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
        .clamp(1, MAX_WORKERS);
    let chunk_size = frontier.len().div_ceil(workers);
    let expand_chunk = &expand_chunk;
    let mut outs: Vec<Vec<Discovery>> = Vec::new();
    std::thread::scope(|s| {
        let handles: Vec<_> = frontier
            .chunks(chunk_size)
            .map(|chunk| {
                s.spawn(move || {
                    let mut out = Vec::new();
                    expand_chunk(chunk, &mut out);
                    out
                })
            })
            .collect();
        for h in handles {
            // A worker panicking is a bug in expand_chunk itself; surface it.
            if let Ok(out) = h.join() {
                outs.push(out);
            }
        }
    });
    outs.concat()
}

/// Bottom-up level (Outgoing only): every UNVISITED visible row scans its
/// in-edges for a frontier parent; first passing parent wins. Rows are
/// partitioned across workers, so discovery is per-row disjoint.
fn expand_pull(
    seg: &CsrStorage,
    frontier: &[u32],
    visited: &[AtomicU64],
    snapshot_lsn: u64,
    edge_type_filter: Option<u16>,
    n: usize,
) -> Vec<Discovery> {
    // Frontier membership bitmap for O(1) parent tests.
    let mut frontier_bits = vec![0u64; n.div_ceil(64)];
    for &row in frontier {
        frontier_bits[(row / 64) as usize] |= 1u64 << (row % 64);
    }
    let frontier_bits = &frontier_bits;

    let scan_range = |range: std::ops::Range<usize>, out: &mut Vec<Discovery>| {
        let node_meta = seg.node_meta();
        for row in range {
            let row = row as u32;
            if is_set(visited, row) {
                continue;
            }
            if !row_visible(&node_meta[row as usize], snapshot_lsn) {
                continue;
            }
            let mut found: Option<(EdgeMeta, u64)> = None;
            seg.for_each_incoming_edge_ms(row, |src_row, meta, created_ms| {
                if found.is_some() {
                    return;
                }
                if let Some(filter) = edge_type_filter {
                    if meta.edge_type != filter {
                        return;
                    }
                }
                if frontier_bits[(src_row / 64) as usize] & (1u64 << (src_row % 64)) != 0 {
                    found = Some((meta, created_ms));
                }
            });
            if let Some((meta, created_ms)) = found {
                // Rows are partitioned per worker — the set cannot race.
                test_and_set(visited, row);
                out.push((row, meta, created_ms));
            }
        }
    };

    // Warm the lazy incoming index before any parallel section.
    seg.for_each_incoming_edge_ms(0, |_, _, _| {});

    if n < PARALLEL_LEVEL_THRESHOLD {
        let mut out = Vec::new();
        scan_range(0..n, &mut out);
        return out;
    }

    let workers = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
        .clamp(1, MAX_WORKERS);
    let chunk = n.div_ceil(workers);
    let scan_range = &scan_range;
    let mut outs: Vec<Vec<Discovery>> = Vec::new();
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                let range = (w * chunk).min(n)..((w + 1) * chunk).min(n);
                s.spawn(move || {
                    let mut out = Vec::new();
                    scan_range(range, &mut out);
                    out
                })
            })
            .collect();
        for h in handles {
            if let Ok(out) = h.join() {
                outs.push(out);
            }
        }
    });
    outs.concat()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::csr::CsrSegment;
    use crate::graph::traversal::SegmentMergeReader;
    use smallvec::SmallVec;
    use std::collections::HashMap;

    /// Deterministic pseudo-random frozen graph: `n` nodes, each node i
    /// gets out-edges to (i*7+k*13+1) % n for k in 0..deg(i).
    fn frozen_storage(n: u64, max_deg: u64) -> (CsrStorage, Vec<NodeKey>) {
        let mut mg = MemGraph::new(usize::MAX >> 1);
        let mut keys = Vec::new();
        for _ in 0..n {
            keys.push(mg.add_node(SmallVec::from_elem(1u16, 1), SmallVec::new(), None, 1));
        }
        for i in 0..n {
            let deg = 1 + (i % max_deg);
            for k in 0..deg {
                let j = (i * 7 + k * 13 + 1) % n;
                if j != i {
                    let etype = if k % 2 == 0 { 1u16 } else { 2u16 };
                    mg.add_edge(keys[i as usize], keys[j as usize], etype, 1.0, None, 2)
                        .expect("edge ok");
                }
            }
        }
        let frozen = mg.freeze().expect("freeze");
        let seg = CsrSegment::from_frozen(frozen, 3).expect("csr");
        (CsrStorage::from(seg), keys)
    }

    fn frozen_graph(n: u64, max_deg: u64) -> (Arc<CsrStorage>, Vec<NodeKey>) {
        let (storage, keys) = frozen_storage(n, max_deg);
        (Arc::new(storage), keys)
    }

    /// Oracle: reader-path BFS (the code path row_bfs replaces).
    fn reader_bfs(
        seg: &Arc<CsrStorage>,
        start: NodeKey,
        direction: Direction,
        depth_limit: u32,
        filter: Option<u16>,
    ) -> HashMap<NodeKey, u32> {
        let segs = vec![seg.clone()];
        let reader = SegmentMergeReader::new(None, &segs, direction, u64::MAX - 1, filter);
        // Hand-rolled reader-path BFS: BoundedBfs::execute takes the fast
        // path for this fixture, so it cannot serve as its own oracle.
        let mut depths = HashMap::new();
        depths.insert(start, 0u32);
        let mut frontier = vec![start];
        let mut d = 0;
        while !frontier.is_empty() && d < depth_limit {
            let mut next = Vec::new();
            for &node in &frontier {
                for nb in reader.neighbors(node) {
                    if let std::collections::hash_map::Entry::Vacant(e) = depths.entry(nb.node) {
                        e.insert(d + 1);
                        next.push(nb.node);
                    }
                }
            }
            frontier = next;
            d += 1;
        }
        depths
    }

    fn depth_map(result: &BfsResult) -> HashMap<NodeKey, u32> {
        result.visited.iter().map(|&(k, d, _)| (k, d)).collect()
    }

    #[test]
    fn test_row_bfs_matches_reader_path() {
        let (seg, keys) = frozen_graph(300, 5);
        for direction in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
            for filter in [None, Some(1u16)] {
                let got = row_bfs(
                    &seg,
                    direction,
                    u64::MAX - 1,
                    filter,
                    keys[0],
                    4,
                    usize::MAX,
                    BfsMode::ForcePush,
                )
                .expect("bfs ok");
                let want = reader_bfs(&seg, keys[0], direction, 4, filter);
                assert_eq!(
                    depth_map(&got),
                    want,
                    "push parity failed dir={direction:?} filter={filter:?}"
                );
            }
        }
    }

    #[test]
    fn test_pull_matches_push() {
        let (seg, keys) = frozen_graph(500, 8);
        let push = row_bfs(
            &seg,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
            keys[3],
            5,
            usize::MAX,
            BfsMode::ForcePush,
        )
        .expect("push ok");
        let pull = row_bfs(
            &seg,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
            keys[3],
            5,
            usize::MAX,
            BfsMode::ForcePull,
        )
        .expect("pull ok");
        assert_eq!(depth_map(&push), depth_map(&pull), "pull/push must agree");

        // With an edge-type filter too (pull checks the filter per parent).
        let push_f = row_bfs(
            &seg,
            Direction::Outgoing,
            u64::MAX - 1,
            Some(2),
            keys[3],
            5,
            usize::MAX,
            BfsMode::ForcePush,
        )
        .expect("push ok");
        let pull_f = row_bfs(
            &seg,
            Direction::Outgoing,
            u64::MAX - 1,
            Some(2),
            keys[3],
            5,
            usize::MAX,
            BfsMode::ForcePull,
        )
        .expect("pull ok");
        assert_eq!(depth_map(&push_f), depth_map(&pull_f));
    }

    #[test]
    fn test_parallel_level_matches_sequential() {
        // Star head fans out to >PARALLEL_LEVEL_THRESHOLD nodes → level 2
        // expands in parallel. Compare against the reader-path oracle.
        let n = 2_000u64;
        let mut mg = MemGraph::new(usize::MAX >> 1);
        let mut keys = Vec::new();
        for _ in 0..n {
            keys.push(mg.add_node(SmallVec::from_elem(1u16, 1), SmallVec::new(), None, 1));
        }
        for i in 1..600u64 {
            mg.add_edge(keys[0], keys[i as usize], 1, 1.0, None, 2)
                .expect("edge");
            // Second hop: each fan-out node points at a distinct tail node.
            let tail = 600 + (i % (n - 600));
            mg.add_edge(keys[i as usize], keys[tail as usize], 1, 1.0, None, 2)
                .expect("edge");
        }
        let frozen = mg.freeze().expect("freeze");
        let seg = Arc::new(CsrStorage::from(
            CsrSegment::from_frozen(frozen, 3).expect("csr"),
        ));

        let got = row_bfs(
            &seg,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
            keys[0],
            3,
            usize::MAX,
            BfsMode::ForcePush,
        )
        .expect("bfs ok");
        let want = reader_bfs(&seg, keys[0], Direction::Outgoing, 3, None);
        assert_eq!(depth_map(&got), want);
    }

    #[test]
    fn test_gate_falls_back_when_mutable_tier_nonempty_or_multiseg() {
        let (seg, keys) = frozen_graph(50, 3);

        // Empty mutable tier + single segment → fast path taken.
        let empty = MemGraph::new(1 << 20);
        let segs = vec![seg.clone()];
        assert!(
            try_row_bfs(
                Some(&empty),
                &segs,
                Direction::Outgoing,
                u64::MAX - 1,
                None,
                keys[0],
                3,
                usize::MAX
            )
            .is_some()
        );

        // Non-empty mutable tier → fall back.
        let mut busy = MemGraph::new(1 << 20);
        busy.add_node(SmallVec::new(), SmallVec::new(), None, 9);
        assert!(
            try_row_bfs(
                Some(&busy),
                &segs,
                Direction::Outgoing,
                u64::MAX - 1,
                None,
                keys[0],
                3,
                usize::MAX
            )
            .is_none()
        );

        // Two segments → the W2-6 multi-segment row path engages. Two
        // clones of one segment = total key overlap; the boundary sync must
        // dedup every node and match the single-segment answer exactly.
        let two = vec![seg.clone(), seg.clone()];
        let multi = try_row_bfs(
            None,
            &two,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
            keys[0],
            3,
            usize::MAX,
        )
        .expect("multi-seg gate engages")
        .expect("bfs ok");
        let single = row_bfs(
            &seg,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
            keys[0],
            3,
            usize::MAX,
            BfsMode::ForcePush,
        )
        .expect("bfs ok");
        assert_eq!(depth_map(&multi), depth_map(&single));

        // Segment newer than the snapshot → fall back.
        assert!(
            try_row_bfs(
                None,
                &segs,
                Direction::Outgoing,
                1, // snapshot before seg lsn 3
                None,
                keys[0],
                3,
                usize::MAX
            )
            .is_none()
        );
    }

    #[test]
    fn test_deleted_nodes_excluded() {
        // Soft-delete a node pre-freeze: its row carries deleted_lsn into
        // the CSR NodeMeta, and BFS must agree with the reader path (which
        // applies the same deleted_lsn visibility check on every probe).
        let mut mg = MemGraph::new(usize::MAX >> 1);
        let mut keys = Vec::new();
        for _ in 0..40u64 {
            keys.push(mg.add_node(SmallVec::from_elem(1u16, 1), SmallVec::new(), None, 1));
        }
        for i in 0..39usize {
            mg.add_edge(keys[i], keys[i + 1], 1, 1.0, None, 2)
                .expect("edge");
        }
        assert!(mg.remove_node(keys[8], 5), "victim soft-deleted");
        let frozen = mg.freeze().expect("freeze");
        let seg = Arc::new(CsrStorage::from(
            CsrSegment::from_frozen(frozen, 6).expect("csr"),
        ));

        let got = row_bfs(
            &seg,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
            keys[0],
            50,
            usize::MAX,
            BfsMode::ForcePush,
        )
        .expect("bfs ok");
        assert!(
            !depth_map(&got).contains_key(&keys[8]),
            "deleted node must not be discovered"
        );
        let want = reader_bfs(&seg, keys[0], Direction::Outgoing, 50, None);
        assert_eq!(depth_map(&got), want);
    }

    #[test]
    fn test_depth_limit_and_frontier_cap() {
        let (seg, keys) = frozen_graph(200, 4);
        let d1 = row_bfs(
            &seg,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
            keys[0],
            1,
            usize::MAX,
            BfsMode::ForcePush,
        )
        .expect("bfs ok");
        assert!(depth_map(&d1).values().all(|&d| d <= 1));

        let err = row_bfs(
            &seg,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
            keys[0],
            10,
            3, // tiny cap
            BfsMode::ForcePush,
        )
        .expect_err("cap must trip");
        assert!(matches!(
            err,
            TraversalError::FrontierCapExceeded { cap: 3, .. }
        ));
    }

    #[test]
    fn test_missing_start_is_node_not_found() {
        let (seg, _keys) = frozen_graph(10, 2);
        // A key from a fresh MemGraph would ALIAS the frozen rows (SlotMap
        // sequences restart) — use a synthetic key far outside the segment.
        let ghost: NodeKey = slotmap::KeyData::from_ffi((1u64 << 32) | 999_999).into();
        let err = row_bfs(
            &seg,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
            ghost,
            3,
            usize::MAX,
            BfsMode::ForcePush,
        )
        .expect_err("unknown start");
        assert!(matches!(err, TraversalError::NodeNotFound));
    }

    /// Reader-path oracle over an arbitrary segment list.
    fn reader_bfs_multi(
        segs: &[Arc<CsrStorage>],
        start: NodeKey,
        direction: Direction,
        depth_limit: u32,
        filter: Option<u16>,
    ) -> HashMap<NodeKey, u32> {
        let reader = SegmentMergeReader::new(None, segs, direction, u64::MAX - 1, filter);
        let mut depths = HashMap::new();
        depths.insert(start, 0u32);
        let mut frontier = vec![start];
        let mut d = 0;
        while !frontier.is_empty() && d < depth_limit {
            let mut next = Vec::new();
            for &node in &frontier {
                for nb in reader.neighbors(node) {
                    if let std::collections::hash_map::Entry::Vacant(e) = depths.entry(nb.node) {
                        e.insert(d + 1);
                        next.push(nb.node);
                    }
                }
            }
            frontier = next;
            d += 1;
        }
        depths
    }

    #[test]
    fn test_multi_segment_disjoint_clusters_match_reader() {
        // Freeze the SAME MemGraph twice (monotonic id cursors — no key
        // aliasing across freezes): two disjoint clusters, one per segment.
        let mut mg = MemGraph::new(usize::MAX >> 1);
        let a: Vec<NodeKey> = (0..30)
            .map(|_| mg.add_node(SmallVec::from_elem(1u16, 1), SmallVec::new(), None, 1))
            .collect();
        for i in 0..29 {
            mg.add_edge(a[i], a[i + 1], 1, 1.0, None, 2).expect("edge");
        }
        let seg_old = Arc::new(CsrStorage::from(
            CsrSegment::from_frozen(mg.freeze().expect("freeze"), 3).expect("csr"),
        ));
        mg.thaw();
        let b: Vec<NodeKey> = (0..20)
            .map(|_| mg.add_node(SmallVec::from_elem(1u16, 1), SmallVec::new(), None, 4))
            .collect();
        for i in 0..19 {
            mg.add_edge(b[i], b[i + 1], 1, 1.0, None, 5).expect("edge");
        }
        let seg_new = Arc::new(CsrStorage::from(
            CsrSegment::from_frozen(mg.freeze().expect("freeze"), 6).expect("csr"),
        ));

        // Newest-first, matching production segment ordering.
        let segs = vec![seg_new, seg_old];
        for start in [a[0], b[0]] {
            let got = try_row_bfs(
                None,
                &segs,
                Direction::Outgoing,
                u64::MAX - 1,
                None,
                start,
                50,
                usize::MAX,
            )
            .expect("gate engages")
            .expect("bfs ok");
            let want = reader_bfs_multi(&segs, start, Direction::Outgoing, 50, None);
            assert_eq!(depth_map(&got), want, "start {start:?}");
        }
    }

    #[test]
    fn test_multi_segment_overlapping_stale_copy_matches_reader() {
        // The W2-2 aftermath the boundary sync exists for: node X frozen in
        // an OLD segment (edge X->B), copied up, re-frozen into a NEW
        // segment (edge X->C). X is resident in both; BFS from X must reach
        // B (old segment's adjacency) AND C (new segment's) — exactly what
        // SegmentMergeReader's per-node segment union produces.
        use slotmap::Key;
        let mut mg = MemGraph::new(usize::MAX >> 1);
        let x = mg.add_node(SmallVec::from_elem(1u16, 1), SmallVec::new(), None, 1);
        let b = mg.add_node(SmallVec::from_elem(1u16, 1), SmallVec::new(), None, 1);
        mg.add_edge(x, b, 1, 1.0, None, 2).expect("edge");
        let seg_old = Arc::new(CsrStorage::from(
            CsrSegment::from_frozen(mg.freeze().expect("freeze"), 3).expect("csr"),
        ));
        mg.thaw();
        // Copy-up: re-materialize X at its ORIGINAL key, wire a new edge.
        let x2 = mg.add_node_with_id(
            x.data().as_ffi(),
            SmallVec::from_elem(1u16, 1),
            SmallVec::new(),
            None,
            4,
        );
        assert_eq!(x2, x, "copy-up re-inserts at the same key");
        let c = mg.add_node(SmallVec::from_elem(1u16, 1), SmallVec::new(), None, 4);
        mg.add_edge(x, c, 1, 1.0, None, 5).expect("edge");
        let seg_new = Arc::new(CsrStorage::from(
            CsrSegment::from_frozen(mg.freeze().expect("freeze"), 6).expect("csr"),
        ));

        let segs = vec![seg_new, seg_old];
        let got = try_row_bfs(
            None,
            &segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
            x,
            10,
            usize::MAX,
        )
        .expect("gate engages")
        .expect("bfs ok");
        let dm = depth_map(&got);
        assert_eq!(dm.get(&b), Some(&1), "old segment's adjacency reached");
        assert_eq!(dm.get(&c), Some(&1), "new segment's adjacency reached");
        let want = reader_bfs_multi(&segs, x, Direction::Outgoing, 10, None);
        assert_eq!(dm, want);

        // Both directions across the boundary too.
        for dir in [Direction::Incoming, Direction::Both] {
            let got = try_row_bfs(None, &segs, dir, u64::MAX - 1, None, b, 10, usize::MAX)
                .expect("gate engages")
                .expect("bfs ok");
            let want = reader_bfs_multi(&segs, b, dir, 10, None);
            assert_eq!(depth_map(&got), want, "dir {dir:?}");
        }
    }
}
