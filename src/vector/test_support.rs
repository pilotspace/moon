//! Test-only fixtures shaped like real sentence embeddings (WS11).
//!
//! CLAUDE.md: recall on isotropic random Gaussians misleads at high
//! dimension — every pair is nearly orthogonal, distances concentrate, and
//! the "nearest" neighbours are noise. Real embeddings (MiniLM, OpenAI) are
//! nothing like that: they occupy a low-dimensional subspace with a decaying
//! spectrum, share a common mean direction (the anisotropy "cone"), and
//! cluster into topics of very uneven size. This generator reproduces those
//! four properties deterministically (no model or download needed):
//!
//! - latent rank `dim / 8` with a power-law spectrum `λ_j ∝ (1 + j)^-0.8`;
//! - a shared mean direction worth ~0.3 of each vector's energy;
//! - Zipf(1.1)-sized topics, each a latent centre plus per-doc spread;
//! - a little isotropic noise, then unit normalisation.
//!
//! Three query classes mirror moon#1192's watch item: in-distribution (a new
//! draw from an existing topic), near-duplicate (a document plus small
//! noise) and far (an isotropic direction — true neighbours barely similar).

/// Deterministic LCG + Box-Muller Gaussian stream.
pub(crate) struct Gauss(u64);

impl Gauss {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn uniform(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 40) as f32 + 0.5) / (1u64 << 24) as f32
    }

    pub(crate) fn next(&mut self) -> f32 {
        let (u1, u2) = (self.uniform(), self.uniform());
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos()
    }
}

pub(crate) fn normalize(v: &mut [f32]) {
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n > 0.0 {
        v.iter_mut().for_each(|x| *x /= n);
    }
}

/// A corpus generator: fixed basis, mean direction and topics.
pub(crate) struct EmbeddingLike {
    dim: usize,
    basis: Vec<Vec<f32>>,
    sqrt_lambda: Vec<f32>,
    mean: Vec<f32>,
    topics: Vec<Vec<f32>>,
    /// Cumulative Zipf weights over `topics`.
    cdf: Vec<f32>,
    rng: Gauss,
}

impl EmbeddingLike {
    pub(crate) fn new(dim: usize, n_topics: usize, seed: u64) -> Self {
        let mut rng = Gauss::new(seed);
        let rank = (dim / 8).max(4);
        let basis: Vec<Vec<f32>> = (0..rank)
            .map(|_| {
                let mut b: Vec<f32> = (0..dim).map(|_| rng.next()).collect();
                normalize(&mut b);
                b
            })
            .collect();
        let sqrt_lambda: Vec<f32> = (0..rank)
            .map(|j| (1.0 + j as f32).powf(-0.8).sqrt())
            .collect();
        let mut mean: Vec<f32> = (0..dim).map(|_| rng.next()).collect();
        normalize(&mut mean);
        let topics: Vec<Vec<f32>> = (0..n_topics)
            .map(|_| (0..rank).map(|j| rng.next() * sqrt_lambda[j]).collect())
            .collect();
        let mut acc = 0.0f32;
        let cdf = (0..n_topics)
            .map(|t| {
                acc += 1.0 / (1.0 + t as f32).powf(1.1);
                acc
            })
            .collect();
        Self {
            dim,
            basis,
            sqrt_lambda,
            mean,
            topics,
            cdf,
            rng,
        }
    }

    fn pick_topic(&mut self) -> usize {
        let total = *self.cdf.last().unwrap_or(&1.0);
        let u = self.rng.uniform() * total;
        self.cdf
            .partition_point(|&c| c < u)
            .min(self.topics.len() - 1)
    }

    fn from_latent(&mut self, latent: &[f32]) -> Vec<f32> {
        let mut v: Vec<f32> = self.mean.iter().map(|m| m * 0.55).collect();
        for (coef, b) in latent.iter().zip(&self.basis) {
            for (x, bj) in v.iter_mut().zip(b) {
                *x += coef * bj;
            }
        }
        for x in v.iter_mut() {
            *x += 0.02 * self.rng.next();
        }
        normalize(&mut v);
        v
    }

    /// One document drawn from a Zipf-chosen topic.
    pub(crate) fn doc(&mut self) -> Vec<f32> {
        let t = self.pick_topic();
        let latent: Vec<f32> = (0..self.sqrt_lambda.len())
            .map(|j| self.topics[t][j] + 0.45 * self.rng.next() * self.sqrt_lambda[j])
            .collect();
        self.from_latent(&latent)
    }

    pub(crate) fn docs(&mut self, n: usize) -> Vec<Vec<f32>> {
        (0..n).map(|_| self.doc()).collect()
    }

    /// A document plus small isotropic noise (cosine ≈ 0.97 to it).
    pub(crate) fn near_duplicate(&mut self, of: &[f32]) -> Vec<f32> {
        let s = 0.25 / (self.dim as f32).sqrt();
        let mut v: Vec<f32> = of.iter().map(|x| x + s * self.rng.next()).collect();
        normalize(&mut v);
        v
    }

    /// A far / out-of-distribution query: an isotropic unit direction.
    pub(crate) fn far(&mut self) -> Vec<f32> {
        let mut v: Vec<f32> = (0..self.dim).map(|_| self.rng.next()).collect();
        normalize(&mut v);
        v
    }
}

/// Exact top-k by cosine distance (`1 - cos`) over unit vectors.
pub(crate) fn exact_topk(data: &[Vec<f32>], q: &[f32], k: usize) -> Vec<usize> {
    let mut d: Vec<(f32, usize)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| (1.0 - v.iter().zip(q).map(|(a, b)| a * b).sum::<f32>(), i))
        .collect();
    d.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    d.into_iter().take(k).map(|(_, i)| i).collect()
}

/// Recall@k of `got` against `truth` (both top-k index lists).
pub(crate) fn recall(got: &[usize], truth: &[usize]) -> f32 {
    let hit = got.iter().filter(|g| truth.contains(g)).count();
    hit as f32 / truth.len().max(1) as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_has_embedding_statistics_not_isotropic_ones() {
        let mut g = EmbeddingLike::new(384, 64, 1);
        let docs = g.docs(400);
        let cos = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
        let mut pair = 0.0;
        for i in 0..200 {
            pair += cos(&docs[i], &docs[i + 200]);
        }
        let mean_pair = pair / 200.0;
        // Isotropic 384d unit vectors average ~0.0 ± 0.05; sentence
        // embeddings sit above that (MiniLM ~0.1-0.2) but far below duplicates.
        assert!(
            (0.08..0.8).contains(&mean_pair),
            "mean pairwise cos {mean_pair}"
        );
        let nd = g.near_duplicate(&docs[3]);
        assert!(cos(&nd, &docs[3]) > 0.9);
        let far = g.far();
        let best_far = docs.iter().map(|d| cos(d, &far)).fold(-1.0, f32::max);
        let best_in = {
            let q = g.doc();
            docs.iter().map(|d| cos(d, &q)).fold(-1.0, f32::max)
        };
        assert!(
            best_far < best_in,
            "far {best_far} vs in-distribution {best_in}"
        );
    }
}
