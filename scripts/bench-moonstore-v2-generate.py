#!/usr/bin/env python3
"""Generate MiniLM-L6-v2 embeddings for MoonStore v2 benchmarks.

Uses real sentence-transformers model to produce genuine 384d embeddings.
Falls back to normalized random vectors if model unavailable.
"""

import argparse
import json
import os
import sys
import time

import numpy as np


def generate_sentences(n):
    """Generate diverse synthetic sentences for embedding."""
    templates = [
        "The {} {} {} the {} {}.",
        "A {} {} is {} than a {} {}.",
        "How does {} {} when {} {} {}?",
        "{} and {} are both types of {} found in {}.",
        "The {} of {} depends on {} and {}.",
    ]
    nouns = ["cat", "dog", "house", "tree", "river", "mountain", "city", "book",
             "car", "phone", "computer", "garden", "ocean", "forest", "bridge",
             "robot", "artist", "scientist", "teacher", "musician", "doctor",
             "engineer", "pilot", "chef", "farmer", "server", "database",
             "algorithm", "network", "protocol", "vector", "matrix", "tensor"]
    verbs = ["runs", "jumps", "creates", "destroys", "transforms", "analyzes",
             "builds", "connects", "processes", "searches", "optimizes", "stores"]
    adjs = ["fast", "slow", "bright", "dark", "large", "small", "complex",
            "simple", "efficient", "powerful", "distributed", "scalable"]
    rng = np.random.RandomState(42)
    sentences = []
    for i in range(n):
        tmpl = templates[i % len(templates)]
        words = []
        for _ in range(tmpl.count("{}")):
            pools = [nouns, verbs, adjs]
            pool = pools[rng.randint(len(pools))]
            words.append(pool[rng.randint(len(pool))])
        sentences.append(tmpl.format(*words))
    return sentences


def main():
    p = argparse.ArgumentParser(description="Generate MiniLM embeddings for benchmarks")
    p.add_argument("--vectors", type=int, default=10000)
    p.add_argument("--queries", type=int, default=200)
    p.add_argument("--dim", type=int, default=384)
    p.add_argument("--output", type=str, default="target/moonstore-v2-data")
    args = p.parse_args()

    os.makedirs(args.output, exist_ok=True)

    use_model = False
    try:
        from sentence_transformers import SentenceTransformer
        print("  Loading MiniLM-L6-v2 model...")
        model = SentenceTransformer("all-MiniLM-L6-v2")
        use_model = True
    except ImportError:
        print("  sentence-transformers not available, using random vectors")

    if use_model:
        sentences = generate_sentences(args.vectors + args.queries)
        print(f"  Encoding {len(sentences)} sentences with MiniLM...")
        t0 = time.time()
        all_embeddings = model.encode(sentences, normalize_embeddings=True,
                                       show_progress_bar=True, batch_size=256)
        dt = time.time() - t0
        print(f"  Encoded in {dt:.1f}s ({len(sentences)/dt:.0f} sentences/s)")

        vectors = all_embeddings[:args.vectors].astype(np.float32)
        queries = all_embeddings[args.vectors:args.vectors + args.queries].astype(np.float32)
        dim = vectors.shape[1]
    else:
        dim = args.dim
        np.random.seed(42)
        vectors = np.random.randn(args.vectors, dim).astype(np.float32)
        vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
        queries = np.random.randn(args.queries, dim).astype(np.float32)
        queries /= np.linalg.norm(queries, axis=1, keepdims=True)

    # Compute ground truth (brute-force L2)
    print(f"  Computing ground truth ({args.queries} queries x {args.vectors} vectors)...")
    from numpy.linalg import norm
    gt = []
    for q in queries:
        dists = np.sum((vectors - q) ** 2, axis=1)
        top_k = np.argsort(dists)[:10]
        gt.append(top_k.tolist())

    # Save
    np.save(os.path.join(args.output, "vectors.npy"), vectors)
    np.save(os.path.join(args.output, "queries.npy"), queries)
    with open(os.path.join(args.output, "ground_truth.json"), "w") as f:
        json.dump(gt, f)
    with open(os.path.join(args.output, "meta.json"), "w") as f:
        json.dump({
            "n_vectors": args.vectors,
            "n_queries": args.queries,
            "dim": dim,
            "model": "all-MiniLM-L6-v2" if use_model else "random",
            "normalized": True,
        }, f, indent=2)

    print(f"  Saved: {args.vectors} vectors ({dim}d), {args.queries} queries, ground truth")


if __name__ == "__main__":
    main()
