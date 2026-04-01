//! Recall@10 benchmark at multiple scales and dimensions.
//!
//! Measures HNSW search accuracy against brute-force L2 ground truth.
//! This is the definitive recall measurement — not TQ-ADC ground truth,
//! but raw L2 on original f32 vectors (same methodology as the competitor
//! benchmark uses for Redis and Qdrant).

use moon::vector::distance;
use moon::vector::hnsw::build::HnswBuilder;
use moon::vector::hnsw::search::{SearchScratch, hnsw_search};
use moon::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use moon::vector::turbo_quant::encoder::{encode_tq_mse_scaled, padded_dimension};
use moon::vector::turbo_quant::fwht;
use moon::vector::types::DistanceMetric;

/// Simple LCG-based pseudo-random f32 generator (deterministic, no deps).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn next_f32(&mut self) -> f32 {
        // Uniform [0, 1)
        (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
    }
    /// Approximate standard normal via Box-Muller
    fn randn(&mut self) -> f32 {
        let u1 = self.next_f32().max(1e-7);
        let u2 = self.next_f32();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos()
    }
}

/// Generate n random unit vectors of dimension d.
fn generate_unit_vectors(n: usize, d: usize, seed: u64) -> Vec<f32> {
    let mut rng = Rng::new(seed);
    let mut vecs = Vec::with_capacity(n * d);
    for _ in 0..n {
        let mut v: Vec<f32> = (0..d).map(|_| rng.randn()).collect();
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in v.iter_mut() {
                *x /= norm;
            }
        }
        vecs.extend_from_slice(&v);
    }
    vecs
}

/// Brute-force top-K by exact L2 distance.
fn brute_force_topk(vectors: &[f32], d: usize, query: &[f32], k: usize) -> Vec<u32> {
    let n = vectors.len() / d;
    let l2_fn = distance::table().l2_f32;
    let mut dists: Vec<(f32, u32)> = (0..n)
        .map(|i| {
            let v = &vectors[i * d..(i + 1) * d];
            (l2_fn(query, v), i as u32)
        })
        .collect();
    dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    dists.iter().take(k).map(|x| x.1).collect()
}

/// Build HNSW + TQ codes, search, measure recall against brute-force L2.
fn measure_recall(n: u32, d: usize, n_queries: usize, ef_search: usize, k: usize) -> f64 {
    let vectors = generate_unit_vectors(n as usize, d, 42);
    let queries = generate_unit_vectors(n_queries, d, 999);

    let meta = CollectionMetadata::new(
        0,
        d as u32,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        42,
    );
    let padded = padded_dimension(d as u32) as usize;
    let bytes_per_code = padded / 2 + 4;

    // Encode TQ codes
    let mut all_tq: Vec<u8> = Vec::with_capacity(n as usize * bytes_per_code);
    let mut work = vec![0.0f32; padded];
    for i in 0..n as usize {
        let v = &vectors[i * d..(i + 1) * d];
        let boundaries_arr: &[f32; 15] = meta
            .codebook_boundaries
            .as_slice()
            .try_into()
            .expect("boundaries must be 15 elements for 4-bit TQ");
        let code = encode_tq_mse_scaled(
            v,
            meta.fwht_sign_flips.as_slice(),
            boundaries_arr,
            &mut work,
        );
        all_tq.extend_from_slice(&code.codes);
        all_tq.extend_from_slice(&code.norm.to_le_bytes());
    }

    // Build HNSW using TQ-ADC distance (MUST match search metric for good recall).
    // Pre-rotate all vectors to compute TQ-ADC distances during construction.
    use moon::vector::turbo_quant::tq_adc::tq_l2_adc_scaled;
    let mut rotated_vecs = vec![0.0f32; n as usize * padded];
    for i in 0..n as usize {
        let v = &vectors[i * d..(i + 1) * d];
        let rot = &mut rotated_vecs[i * padded..(i + 1) * padded];
        rot[..d].copy_from_slice(v);
        // Normalize
        let norm: f32 = rot[..d].iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in rot[..d].iter_mut() {
                *x /= norm;
            }
        }
        for x in rot[d..padded].iter_mut() {
            *x = 0.0;
        }
        fwht::fwht(&mut rot[..padded], meta.fwht_sign_flips.as_slice());
    }

    let codebook: &[f32; 16] = meta
        .codebook
        .as_slice()
        .try_into()
        .expect("codebook must be 16 elements for 4-bit TQ");
    let mut builder = HnswBuilder::new(16, 200, 42);
    for _ in 0..n {
        builder.insert(|a, b| {
            // Use TQ-ADC(a as query, b as code) for symmetric-ish construction
            let q_rot = &rotated_vecs[a as usize * padded..(a as usize + 1) * padded];
            let b_code =
                &all_tq[b as usize * bytes_per_code..b as usize * bytes_per_code + padded / 2];
            let b_norm_bytes = &all_tq[b as usize * bytes_per_code + padded / 2
                ..b as usize * bytes_per_code + padded / 2 + 4];
            let b_norm = f32::from_le_bytes([
                b_norm_bytes[0],
                b_norm_bytes[1],
                b_norm_bytes[2],
                b_norm_bytes[3],
            ]);
            tq_l2_adc_scaled(q_rot, b_code, b_norm, codebook)
        });
    }
    let graph = builder.build(bytes_per_code as u32);

    // CRITICAL: Reorder TQ codes from original-ID order to BFS order.
    let mut all_tq_bfs = vec![0u8; n as usize * bytes_per_code];
    for orig_id in 0..n as usize {
        let bfs_pos = graph.to_bfs(orig_id as u32) as usize;
        let src = &all_tq[orig_id * bytes_per_code..(orig_id + 1) * bytes_per_code];
        let dst = &mut all_tq_bfs[bfs_pos * bytes_per_code..(bfs_pos + 1) * bytes_per_code];
        dst.copy_from_slice(src);
    }
    let all_tq = all_tq_bfs;

    // Search and measure recall
    let mut scratch = SearchScratch::new(n, padded as u32);
    let mut total_recall = 0.0f64;

    for qi in 0..n_queries {
        let q = &queries[qi * d..(qi + 1) * d];

        // Ground truth: brute-force L2 on original f32 vectors
        let gt = brute_force_topk(&vectors, d, q, k);

        // HNSW search (uses TQ-ADC distance internally)
        let results = hnsw_search(&graph, &all_tq, q, &meta, k, ef_search, &mut scratch);
        let predicted: Vec<u32> = results.iter().map(|r| r.id.0).collect();

        // Recall: fraction of true top-K found by HNSW
        let tp = predicted.iter().filter(|id| gt.contains(id)).count();
        total_recall += tp as f64 / k as f64;
    }

    total_recall / n_queries as f64
}

// ── Tests at multiple scales ───────────────────────────────────────────
//
// These tests measure TQ-ADC HNSW search recall against raw L2 ground truth.
// TQ-ADC introduces quantization distortion -- recall is inherently lower than
// f32 HNSW search. With the dimension-adaptive codebook (v2), TQ-ADC recall
// varies by dimension:
//   - 128d: ~0.70-0.78 (low dim = less benefit from 4-bit quantization)
//   - 768d: ~0.50-0.80 (higher dim = more quantization noise)
//
// The production HNSW search path uses f32 L2 (0.95+ recall). TQ-ADC is
// reserved for brute-force scan where it achieves paper-validated recall.

#[test]
fn recall_1k_128d_ef64() {
    distance::init();
    let recall = measure_recall(1_000, 128, 100, 64, 10);
    println!("RECALL 1K/128d ef=64: {recall:.4}");
    assert!(recall >= 0.70, "Recall {recall} below 0.70");
}

#[test]
fn recall_1k_128d_ef128() {
    distance::init();
    let recall = measure_recall(1_000, 128, 100, 128, 10);
    println!("RECALL 1K/128d ef=128: {recall:.4}");
    assert!(recall >= 0.70, "Recall {recall} below 0.70");
}

#[test]
fn recall_10k_128d_ef128() {
    distance::init();
    let recall = measure_recall(10_000, 128, 100, 128, 10);
    println!("RECALL 10K/128d ef=128: {recall:.4}");
    assert!(recall >= 0.60, "Recall {recall} below 0.60");
}

#[test]
fn recall_1k_768d_ef128() {
    distance::init();
    let recall = measure_recall(1_000, 768, 50, 128, 10);
    println!("RECALL 1K/768d ef=128: {recall:.4}");
    assert!(recall >= 0.70, "Recall {recall} below 0.70");
}

#[test]
fn recall_10k_768d_ef128() {
    distance::init();
    let recall = measure_recall(10_000, 768, 50, 128, 10);
    println!("RECALL 10K/768d ef=128: {recall:.4}");
    assert!(recall >= 0.40, "Recall {recall} below 0.40");
}

#[test]
fn recall_10k_768d_ef256() {
    distance::init();
    let recall = measure_recall(10_000, 768, 50, 256, 10);
    println!("RECALL 10K/768d ef=256: {recall:.4}");
    assert!(recall >= 0.55, "Recall {recall} below 0.55");
}

/// Recall test using the f32 HNSW search path (production path).
///
/// This validates VEC-FIX-01: recall@10 >= 0.95 at 10K/128d ef=200 against
/// true L2 ground truth. The f32 path is what ImmutableSegment.search uses.
#[test]
fn recall_f32_hnsw_10k_128d_ef200() {
    use moon::vector::hnsw::search_sq::hnsw_search_f32;

    distance::init();
    let n: u32 = 10_000;
    let d: usize = 128;
    let k = 10;
    let ef = 200;
    let n_queries = 50;

    let vectors = generate_unit_vectors(n as usize, d, 42);
    let queries = generate_unit_vectors(n_queries, d, 999);
    let l2_fn = distance::table().l2_f32;

    // Build HNSW using f32 L2 distance (same as production)
    let mut builder = HnswBuilder::new(16, 200, 42);
    for _ in 0..n {
        builder.insert(|a, b| {
            (l2_fn)(
                &vectors[a as usize * d..(a as usize + 1) * d],
                &vectors[b as usize * d..(b as usize + 1) * d],
            )
        });
    }
    // bytes_per_code is needed for graph construction but not for f32 search
    let padded = padded_dimension(d as u32) as usize;
    let bytes_per_code = padded / 2 + 4;
    let graph = builder.build(bytes_per_code as u32);

    // BFS-reorder f32 vectors
    let mut vf = vec![0.0f32; n as usize * d];
    for orig in 0..n as usize {
        let bfs = graph.to_bfs(orig as u32) as usize;
        vf[bfs * d..(bfs + 1) * d].copy_from_slice(&vectors[orig * d..(orig + 1) * d]);
    }

    let mut total_recall = 0.0f64;
    for qi in 0..n_queries {
        let q = &queries[qi * d..(qi + 1) * d];
        let gt = brute_force_topk(&vectors, d, q, k);
        let results = hnsw_search_f32(&graph, &vf, d, q, k, ef, None);
        let predicted: Vec<u32> = results.iter().map(|r| r.id.0).collect();
        let tp = predicted.iter().filter(|id| gt.contains(id)).count();
        total_recall += tp as f64 / k as f64;
    }

    let recall = total_recall / n_queries as f64;
    println!("F32 HNSW Recall@10 (10K/128d ef=200): {recall:.4}");
    assert!(
        recall >= 0.95,
        "F32 HNSW recall {recall} below 0.95 (VEC-FIX-01)"
    );
}

#[test]
fn recall_debug_1k_128d() {
    distance::init();
    let n: u32 = 1000;
    let d: usize = 128;
    let k = 10;
    let ef = 128;

    let vectors = generate_unit_vectors(n as usize, d, 42);
    let queries = generate_unit_vectors(5, d, 999);

    let meta = CollectionMetadata::new(
        0,
        d as u32,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        42,
    );
    let padded = padded_dimension(d as u32) as usize;
    let bytes_per_code = padded / 2 + 4;

    let mut all_tq: Vec<u8> = Vec::with_capacity(n as usize * bytes_per_code);
    let mut work = vec![0.0f32; padded];
    for i in 0..n as usize {
        let v = &vectors[i * d..(i + 1) * d];
        let boundaries_arr: &[f32; 15] = meta
            .codebook_boundaries
            .as_slice()
            .try_into()
            .expect("boundaries must be 15 elements for 4-bit TQ");
        let code = encode_tq_mse_scaled(
            v,
            meta.fwht_sign_flips.as_slice(),
            boundaries_arr,
            &mut work,
        );
        all_tq.extend_from_slice(&code.codes);
        all_tq.extend_from_slice(&code.norm.to_le_bytes());
    }

    let l2_fn = distance::table().l2_f32;
    let mut builder = HnswBuilder::new(16, 200, 42);
    for _ in 0..n {
        builder.insert(|a, b| {
            let va = &vectors[a as usize * d..(a as usize + 1) * d];
            let vb = &vectors[b as usize * d..(b as usize + 1) * d];
            l2_fn(va, vb)
        });
    }
    let graph = builder.build(bytes_per_code as u32);

    let mut scratch = SearchScratch::new(n, padded as u32);

    for qi in 0..5 {
        let q = &queries[qi * d..(qi + 1) * d];
        let gt = brute_force_topk(&vectors, d, q, k);
        let results = hnsw_search(&graph, &all_tq, q, &meta, k, ef, &mut scratch);
        let predicted: Vec<u32> = results.iter().map(|r| r.id.0).collect();
        let tp = predicted.iter().filter(|id| gt.contains(id)).count();
        println!("Query {qi}: GT={gt:?}");
        println!("         HNSW={predicted:?}");
        println!("         overlap={tp}/{k}");

        // Also check: are HNSW results at least close to query?
        let gt_dists: Vec<f32> = gt
            .iter()
            .map(|&id| l2_fn(q, &vectors[id as usize * d..(id as usize + 1) * d]))
            .collect();
        let hnsw_dists: Vec<f32> = predicted
            .iter()
            .map(|&id| l2_fn(q, &vectors[id as usize * d..(id as usize + 1) * d]))
            .collect();
        println!("         GT dists: {gt_dists:.4?}");
        println!("         HNSW dists: {hnsw_dists:.4?}");
        println!();
    }
}
