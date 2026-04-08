//! Product Quantization for DiskANN cold tier.
//!
//! Splits vectors into M subspaces, trains k-means codebooks per subspace,
//! and provides encode/decode plus asymmetric distance computation.
//! Used for compressed in-RAM distance estimation during cold-tier beam search.

/// Product Quantizer: M subspaces, each with `ksub` centroids of dimension `dsub`.
pub struct ProductQuantizer {
    dim: usize,
    m: usize,
    ksub: usize,
    dsub: usize,
    /// Flat codebook: `m * ksub * dsub` floats.
    /// Layout: `centroids[sub * ksub * dsub + k * dsub .. + dsub]`
    centroids: Vec<f32>,
}

impl ProductQuantizer {
    /// Train a product quantizer via k-means on the given vectors.
    ///
    /// * `vectors` -- flat f32 array of `n * dim` elements
    /// * `dim` -- vector dimensionality (must be divisible by `m`)
    /// * `m` -- number of subspaces
    /// * `nbits` -- bits per code (ksub = 1 << nbits, typically 8 -> 256 centroids)
    pub fn train(vectors: &[f32], dim: usize, m: usize, nbits: u8) -> Self {
        let n = vectors.len() / dim;
        assert!(n > 0, "need at least one vector");
        assert!(dim.is_multiple_of(m), "dim must be divisible by m");

        let ksub = 1usize << nbits;
        let dsub = dim / m;
        let mut centroids = vec![0.0_f32; m * ksub * dsub];

        for sub in 0..m {
            let sub_offset = sub * dsub;
            // Extract sub-vectors for this subspace
            let mut sub_vecs = vec![0.0_f32; n * dsub];
            for i in 0..n {
                let src = &vectors[i * dim + sub_offset..i * dim + sub_offset + dsub];
                sub_vecs[i * dsub..(i + 1) * dsub].copy_from_slice(src);
            }

            // k-means: init from first ksub data points (or wrap around)
            let codebook_offset = sub * ksub * dsub;
            for k in 0..ksub {
                let src_idx = k % n;
                centroids[codebook_offset + k * dsub..codebook_offset + (k + 1) * dsub]
                    .copy_from_slice(&sub_vecs[src_idx * dsub..(src_idx + 1) * dsub]);
            }

            // Lloyd's iterations
            let mut assignments = vec![0u16; n];
            for _iter in 0..20 {
                // Assign each vector to nearest centroid
                for i in 0..n {
                    let sv = &sub_vecs[i * dsub..(i + 1) * dsub];
                    let mut best_k = 0u16;
                    let mut best_dist = f32::MAX;
                    for k in 0..ksub {
                        let c = &centroids
                            [codebook_offset + k * dsub..codebook_offset + (k + 1) * dsub];
                        let d = l2_sub(sv, c, dsub);
                        if d < best_dist {
                            best_dist = d;
                            best_k = k as u16;
                        }
                    }
                    assignments[i] = best_k;
                }

                // Update centroids
                let mut sums = vec![0.0_f32; ksub * dsub];
                let mut counts = vec![0u32; ksub];
                for i in 0..n {
                    let k = assignments[i] as usize;
                    counts[k] += 1;
                    let sv = &sub_vecs[i * dsub..(i + 1) * dsub];
                    for d in 0..dsub {
                        sums[k * dsub + d] += sv[d];
                    }
                }
                for k in 0..ksub {
                    if counts[k] > 0 {
                        let inv = 1.0 / counts[k] as f32;
                        for d in 0..dsub {
                            centroids[codebook_offset + k * dsub + d] = sums[k * dsub + d] * inv;
                        }
                    }
                    // Empty clusters keep their previous centroid
                }
            }
        }

        Self {
            dim,
            m,
            ksub,
            dsub,
            centroids,
        }
    }

    /// Encode a vector into PQ codes (one u8 per subspace).
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        assert_eq!(vector.len(), self.dim);
        let mut codes = Vec::with_capacity(self.m);
        for sub in 0..self.m {
            let sv = &vector[sub * self.dsub..(sub + 1) * self.dsub];
            let codebook_offset = sub * self.ksub * self.dsub;
            let mut best_k = 0u8;
            let mut best_dist = f32::MAX;
            for k in 0..self.ksub {
                let c = &self.centroids
                    [codebook_offset + k * self.dsub..codebook_offset + (k + 1) * self.dsub];
                let d = l2_sub(sv, c, self.dsub);
                if d < best_dist {
                    best_dist = d;
                    best_k = k as u8;
                }
            }
            codes.push(best_k);
        }
        codes
    }

    /// Decode PQ codes back to a reconstructed vector.
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        assert_eq!(codes.len(), self.m);
        let mut vector = Vec::with_capacity(self.dim);
        for sub in 0..self.m {
            let k = codes[sub] as usize;
            let codebook_offset = sub * self.ksub * self.dsub;
            let c = &self.centroids
                [codebook_offset + k * self.dsub..codebook_offset + (k + 1) * self.dsub];
            vector.extend_from_slice(c);
        }
        vector
    }

    /// Precompute asymmetric distance table for a query vector.
    ///
    /// Returns a table of `m * ksub` floats: `table[sub * ksub + k]` is the
    /// squared L2 distance from the query's sub-vector to centroid k in subspace sub.
    pub fn asymmetric_distance_table(&self, query: &[f32]) -> Vec<f32> {
        assert_eq!(query.len(), self.dim);
        let mut table = Vec::with_capacity(self.m * self.ksub);
        for sub in 0..self.m {
            let qsub = &query[sub * self.dsub..(sub + 1) * self.dsub];
            let codebook_offset = sub * self.ksub * self.dsub;
            for k in 0..self.ksub {
                let c = &self.centroids
                    [codebook_offset + k * self.dsub..codebook_offset + (k + 1) * self.dsub];
                table.push(l2_sub(qsub, c, self.dsub));
            }
        }
        table
    }

    /// Compute asymmetric distance from a precomputed table and PQ codes.
    ///
    /// Sums `table[sub * ksub + codes[sub]]` across all subspaces.
    pub fn asymmetric_distance(&self, table: &[f32], codes: &[u8]) -> f32 {
        assert_eq!(table.len(), self.m * self.ksub);
        assert_eq!(codes.len(), self.m);
        let mut dist = 0.0_f32;
        for sub in 0..self.m {
            dist += table[sub * self.ksub + codes[sub] as usize];
        }
        dist
    }

    /// Number of subspaces.
    #[inline]
    pub fn m(&self) -> usize {
        self.m
    }

    /// Centroids per subspace.
    #[inline]
    pub fn ksub(&self) -> usize {
        self.ksub
    }

    /// Sub-vector dimensionality.
    #[inline]
    pub fn dsub(&self) -> usize {
        self.dsub
    }

    /// Full vector dimensionality.
    #[inline]
    pub fn dim(&self) -> usize {
        self.dim
    }
}

/// Scalar squared-L2 for sub-vectors.
#[inline]
fn l2_sub(a: &[f32], b: &[f32], dsub: usize) -> f32 {
    let mut sum = 0.0_f32;
    for i in 0..dsub {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic f32 vector via LCG PRNG, values in [-1.0, 1.0].
    fn deterministic_f32(dim: usize, seed: u64) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed as u32;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    fn random_vectors(n: usize, dim: usize, base_seed: u64) -> Vec<f32> {
        let mut all = Vec::with_capacity(n * dim);
        for i in 0..n {
            all.extend(deterministic_f32(dim, base_seed + i as u64));
        }
        all
    }

    /// True L2 distance between two full vectors.
    fn true_l2(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
    }

    #[test]
    fn test_pq_train_codebook_shape() {
        let n = 200;
        let dim = 128;
        let m = 16;
        let nbits = 8;
        let vectors = random_vectors(n, dim, 100);
        let pq = ProductQuantizer::train(&vectors, dim, m, nbits);
        assert_eq!(pq.m(), m);
        assert_eq!(pq.ksub(), 256);
        assert_eq!(pq.dsub(), 8); // 128 / 16
        assert_eq!(pq.dim(), dim);
    }

    #[test]
    fn test_pq_encode_decode_bounded_distortion() {
        let n = 200;
        let dim = 128;
        let m = 16;
        let vectors = random_vectors(n, dim, 200);
        let pq = ProductQuantizer::train(&vectors, dim, m, 8);

        // Measure reconstruction error
        let mut total_recon_error = 0.0_f64;
        for i in 0..n {
            let v = &vectors[i * dim..(i + 1) * dim];
            let codes = pq.encode(v);
            let recon = pq.decode(&codes);
            let err = true_l2(v, &recon);
            total_recon_error += err as f64;
        }
        let mean_recon_error = total_recon_error / n as f64;

        // Measure mean pairwise distance (sample 500 pairs)
        let mut total_pairwise = 0.0_f64;
        let mut pair_count = 0;
        let mut seed = 42u32;
        for _ in 0..500 {
            seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
            let i = (seed as usize) % n;
            seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
            let j = (seed as usize) % n;
            if i != j {
                total_pairwise += true_l2(
                    &vectors[i * dim..(i + 1) * dim],
                    &vectors[j * dim..(j + 1) * dim],
                ) as f64;
                pair_count += 1;
            }
        }
        let mean_pairwise = total_pairwise / pair_count as f64;

        // Reconstruction error should be < 50% of mean pairwise distance
        assert!(
            mean_recon_error < 0.50 * mean_pairwise,
            "reconstruction error {mean_recon_error:.4} >= 50% of mean pairwise {mean_pairwise:.4}",
        );
    }

    #[test]
    fn test_pq_asymmetric_distance_approximation() {
        let n = 200;
        let dim = 128;
        let m = 16;
        let vectors = random_vectors(n, dim, 300);
        let pq = ProductQuantizer::train(&vectors, dim, m, 8);

        // Encode all vectors
        let codes: Vec<Vec<u8>> = (0..n)
            .map(|i| pq.encode(&vectors[i * dim..(i + 1) * dim]))
            .collect();

        // Run 30 queries, measure relative error of asymmetric distance vs true L2
        let mut total_rel_error = 0.0_f64;
        let mut count = 0;
        for q in 0..30 {
            let query = deterministic_f32(dim, 400 + q);
            let table = pq.asymmetric_distance_table(&query);
            for i in 0..n {
                let approx = pq.asymmetric_distance(&table, &codes[i]);
                let exact = true_l2(&query, &vectors[i * dim..(i + 1) * dim]);
                if exact > 1e-6 {
                    let rel_err = ((approx - exact).abs() / exact) as f64;
                    total_rel_error += rel_err;
                    count += 1;
                }
            }
        }
        let mean_rel_error = total_rel_error / count as f64;
        assert!(
            mean_rel_error < 0.20,
            "mean relative error {mean_rel_error:.4} >= 0.20",
        );
    }
}
