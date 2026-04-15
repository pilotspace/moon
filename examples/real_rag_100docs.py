#!/usr/bin/env python3
"""
Moon Real-World RAG + GraphRAG Test — 100 Documents, MiniLM Embeddings

Tests Moon as a Knowledge Navigation Engine with:
  - 100 AI/ML domain documents with real sentence-transformer embeddings (384-dim)
  - Knowledge graph with prerequisite/enables/uses/related_to edges
  - Full test of: KNN, filtered search, GraphRAG, session dedup, recommend, navigate, cache

Prerequisites:
    pip install redis sentence-transformers
    # Start Moon: ./moon --port 6399 --shards 1
"""

import struct
import time
import sys
import redis
from sentence_transformers import SentenceTransformer

# ── Config ────────────────────────────────────────────────────────────────────

MOON_PORT = 6399
INDEX = "knowledge"
GRAPH = "kg"

# ── 100 AI/ML Knowledge Documents ────────────────────────────────────────────

DOCUMENTS = [
    # Foundations of ML (0-9)
    ("doc:0",  "Introduction to machine learning and statistical learning theory", "ml", "foundations", "beginner"),
    ("doc:1",  "Supervised learning: classification and regression algorithms", "ml", "foundations", "beginner"),
    ("doc:2",  "Unsupervised learning: clustering, dimensionality reduction, and density estimation", "ml", "foundations", "beginner"),
    ("doc:3",  "Reinforcement learning: agents, environments, and reward signals", "ml", "foundations", "intermediate"),
    ("doc:4",  "Bias-variance tradeoff and model selection strategies", "ml", "foundations", "intermediate"),
    ("doc:5",  "Cross-validation techniques for model evaluation and hyperparameter tuning", "ml", "foundations", "intermediate"),
    ("doc:6",  "Feature engineering: transformations, selection, and extraction methods", "ml", "foundations", "intermediate"),
    ("doc:7",  "Gradient descent optimization: SGD, Adam, AdaGrad, and learning rate schedules", "ml", "optimization", "intermediate"),
    ("doc:8",  "Regularization methods: L1, L2, dropout, and early stopping", "ml", "optimization", "intermediate"),
    ("doc:9",  "Ensemble methods: random forests, gradient boosting, and stacking", "ml", "algorithms", "intermediate"),

    # Neural Networks (10-19)
    ("doc:10", "Neural network fundamentals: perceptrons, activation functions, and backpropagation", "dl", "neural_nets", "beginner"),
    ("doc:11", "Convolutional neural networks for image recognition and computer vision", "dl", "neural_nets", "intermediate"),
    ("doc:12", "Recurrent neural networks and LSTM for sequential data processing", "dl", "neural_nets", "intermediate"),
    ("doc:13", "Generative adversarial networks: architecture, training, and applications", "dl", "neural_nets", "advanced"),
    ("doc:14", "Variational autoencoders for generative modeling and representation learning", "dl", "neural_nets", "advanced"),
    ("doc:15", "Graph neural networks for relational data and molecular property prediction", "dl", "neural_nets", "advanced"),
    ("doc:16", "Neural architecture search: automated model design and optimization", "dl", "neural_nets", "advanced"),
    ("doc:17", "Transfer learning: pre-training, fine-tuning, and domain adaptation strategies", "dl", "training", "intermediate"),
    ("doc:18", "Batch normalization, layer normalization, and training stabilization techniques", "dl", "training", "intermediate"),
    ("doc:19", "Mixed precision training and distributed training across multiple GPUs", "dl", "training", "advanced"),

    # Transformers & LLMs (20-34)
    ("doc:20", "Attention mechanism: scaled dot-product attention and multi-head attention", "llm", "transformers", "intermediate"),
    ("doc:21", "Transformer architecture: encoder-decoder, positional encoding, and layer structure", "llm", "transformers", "intermediate"),
    ("doc:22", "BERT: bidirectional encoder representations for language understanding", "llm", "transformers", "intermediate"),
    ("doc:23", "GPT family: autoregressive language models from GPT-1 to GPT-4", "llm", "transformers", "intermediate"),
    ("doc:24", "T5 and sequence-to-sequence models for text generation and translation", "llm", "transformers", "intermediate"),
    ("doc:25", "Large language model pre-training: data curation, tokenization, and scaling laws", "llm", "training", "advanced"),
    ("doc:26", "Instruction tuning and RLHF for aligning language models with human preferences", "llm", "alignment", "advanced"),
    ("doc:27", "Prompt engineering: zero-shot, few-shot, chain-of-thought, and tree-of-thought", "llm", "prompting", "intermediate"),
    ("doc:28", "In-context learning and emergent abilities in large language models", "llm", "capabilities", "advanced"),
    ("doc:29", "Tokenization strategies: BPE, WordPiece, SentencePiece, and their tradeoffs", "llm", "preprocessing", "intermediate"),
    ("doc:30", "Model quantization: INT8, INT4, GPTQ, and AWQ for efficient inference", "llm", "optimization", "advanced"),
    ("doc:31", "Knowledge distillation: compressing large models into smaller student models", "llm", "optimization", "advanced"),
    ("doc:32", "Fine-tuning techniques: LoRA, QLoRA, prefix tuning, and adapter layers", "llm", "training", "advanced"),
    ("doc:33", "Multimodal models: vision-language models like CLIP, LLaVA, and GPT-4V", "llm", "multimodal", "advanced"),
    ("doc:34", "Long context modeling: RoPE, ALiBi, and efficient attention for 100K+ tokens", "llm", "architecture", "advanced"),

    # RAG & Retrieval (35-49)
    ("doc:35", "Retrieval-augmented generation: combining search with language model generation", "rag", "core", "intermediate"),
    ("doc:36", "Dense passage retrieval: bi-encoder architecture for semantic search", "rag", "retrieval", "intermediate"),
    ("doc:37", "Hybrid search: combining dense vectors with sparse BM25 for robust retrieval", "rag", "retrieval", "intermediate"),
    ("doc:38", "Re-ranking models: cross-encoder scoring for retrieval result refinement", "rag", "retrieval", "advanced"),
    ("doc:39", "Chunking strategies: fixed-size, semantic, recursive, and document-aware splitting", "rag", "preprocessing", "intermediate"),
    ("doc:40", "Embedding models: sentence-transformers, E5, BGE, and domain-specific encoders", "rag", "embeddings", "intermediate"),
    ("doc:41", "Vector similarity metrics: cosine, L2, inner product, and their use cases", "rag", "embeddings", "beginner"),
    ("doc:42", "RAG evaluation: faithfulness, relevance, context recall, and answer correctness", "rag", "evaluation", "advanced"),
    ("doc:43", "Advanced RAG: query rewriting, hypothetical document embeddings, and self-RAG", "rag", "advanced", "advanced"),
    ("doc:44", "Multi-hop RAG: iterative retrieval for complex question answering", "rag", "advanced", "advanced"),
    ("doc:45", "Agentic RAG: using LLM agents to dynamically plan retrieval strategies", "rag", "agents", "advanced"),
    ("doc:46", "RAG with knowledge graphs: combining structured and unstructured retrieval", "rag", "graphrag", "advanced"),
    ("doc:47", "Semantic caching: reducing LLM API costs by caching similar query responses", "rag", "optimization", "intermediate"),
    ("doc:48", "Context window management: stuffing, map-reduce, and refine strategies", "rag", "generation", "intermediate"),
    ("doc:49", "Citation and attribution in RAG: grounding LLM outputs in retrieved sources", "rag", "generation", "intermediate"),

    # Vector Databases & Infrastructure (50-59)
    ("doc:50", "Vector database fundamentals: indexing, search, and metadata filtering", "infra", "vectordb", "beginner"),
    ("doc:51", "HNSW algorithm: hierarchical navigable small world graphs for ANN search", "infra", "algorithms", "intermediate"),
    ("doc:52", "Product quantization and scalar quantization for memory-efficient vector storage", "infra", "algorithms", "advanced"),
    ("doc:53", "Inverted file index with product quantization for billion-scale vector search", "infra", "algorithms", "advanced"),
    ("doc:54", "Vector database benchmarks: recall, QPS, latency, and memory efficiency metrics", "infra", "evaluation", "intermediate"),
    ("doc:55", "Filtered vector search: pre-filtering vs post-filtering strategies", "infra", "vectordb", "intermediate"),
    ("doc:56", "Multi-tenancy in vector databases: namespace isolation and access control", "infra", "vectordb", "advanced"),
    ("doc:57", "Vector database replication, sharding, and horizontal scaling patterns", "infra", "distributed", "advanced"),
    ("doc:58", "Hybrid storage: hot-warm-cold tiering for cost-effective vector management", "infra", "storage", "advanced"),
    ("doc:59", "Real-time vector indexing: streaming ingestion and incremental index updates", "infra", "vectordb", "advanced"),

    # Knowledge Graphs (60-69)
    ("doc:60", "Knowledge graph fundamentals: nodes, edges, properties, and ontologies", "kg", "core", "beginner"),
    ("doc:61", "Graph database query languages: Cypher, SPARQL, and Gremlin compared", "kg", "query", "intermediate"),
    ("doc:62", "Knowledge graph construction from unstructured text using NLP pipelines", "kg", "construction", "advanced"),
    ("doc:63", "Entity resolution and deduplication in knowledge graph building", "kg", "construction", "advanced"),
    ("doc:64", "Knowledge graph embeddings: TransE, RotatE, and ComplEx for link prediction", "kg", "embeddings", "advanced"),
    ("doc:65", "Graph traversal algorithms: BFS, DFS, shortest path, and centrality measures", "kg", "algorithms", "intermediate"),
    ("doc:66", "Community detection and graph clustering for knowledge organization", "kg", "algorithms", "advanced"),
    ("doc:67", "Temporal knowledge graphs: modeling time-evolving relationships", "kg", "advanced", "advanced"),
    ("doc:68", "GraphRAG: combining graph structure with vector retrieval for enhanced RAG", "kg", "graphrag", "advanced"),
    ("doc:69", "Ontology design patterns for domain-specific knowledge representation", "kg", "design", "intermediate"),

    # AI Agents (70-79)
    ("doc:70", "AI agent architectures: ReAct, plan-and-execute, and reflexion patterns", "agents", "core", "intermediate"),
    ("doc:71", "Tool use in LLM agents: function calling, API integration, and code execution", "agents", "tools", "intermediate"),
    ("doc:72", "Memory systems for AI agents: short-term, long-term, and episodic memory", "agents", "memory", "advanced"),
    ("doc:73", "Multi-agent systems: collaboration, debate, and task decomposition patterns", "agents", "multi_agent", "advanced"),
    ("doc:74", "Agent evaluation: task completion, efficiency, safety, and alignment metrics", "agents", "evaluation", "advanced"),
    ("doc:75", "Autonomous coding agents: code generation, debugging, and repository navigation", "agents", "coding", "advanced"),
    ("doc:76", "Web browsing agents: navigation, form filling, and information extraction", "agents", "web", "advanced"),
    ("doc:77", "Agent orchestration frameworks: LangGraph, CrewAI, and AutoGen comparison", "agents", "frameworks", "intermediate"),
    ("doc:78", "Guardrails and safety mechanisms for autonomous AI agent deployment", "agents", "safety", "advanced"),
    ("doc:79", "Agent-computer interfaces: designing APIs and tools for LLM consumption", "agents", "tools", "intermediate"),

    # Production & MLOps (80-89)
    ("doc:80", "ML model serving: REST APIs, gRPC, and batch inference pipelines", "mlops", "serving", "intermediate"),
    ("doc:81", "Model monitoring: drift detection, performance degradation, and alerting", "mlops", "monitoring", "intermediate"),
    ("doc:82", "Feature stores: centralized feature management for ML pipelines", "mlops", "data", "intermediate"),
    ("doc:83", "Experiment tracking: MLflow, Weights & Biases, and Neptune comparison", "mlops", "tooling", "intermediate"),
    ("doc:84", "CI/CD for ML: automated training, testing, and deployment pipelines", "mlops", "devops", "advanced"),
    ("doc:85", "GPU cluster management: scheduling, resource allocation, and cost optimization", "mlops", "infrastructure", "advanced"),
    ("doc:86", "Data versioning and lineage tracking for reproducible ML experiments", "mlops", "data", "intermediate"),
    ("doc:87", "A/B testing for ML models: statistical methods and deployment strategies", "mlops", "evaluation", "intermediate"),
    ("doc:88", "Edge deployment: ONNX, TensorRT, and Core ML for on-device inference", "mlops", "deployment", "advanced"),
    ("doc:89", "LLM serving optimization: KV-cache, speculative decoding, and continuous batching", "mlops", "serving", "advanced"),

    # Ethics & Safety (90-99)
    ("doc:90", "AI fairness: bias detection, mitigation, and equitable model development", "ethics", "fairness", "intermediate"),
    ("doc:91", "Interpretable ML: SHAP, LIME, attention visualization, and feature importance", "ethics", "interpretability", "intermediate"),
    ("doc:92", "AI safety: alignment problem, reward hacking, and corrigibility", "ethics", "safety", "advanced"),
    ("doc:93", "Red teaming LLMs: adversarial testing for harmful content and jailbreaks", "ethics", "safety", "advanced"),
    ("doc:94", "Privacy in ML: differential privacy, federated learning, and data anonymization", "ethics", "privacy", "advanced"),
    ("doc:95", "Hallucination detection and mitigation in large language models", "ethics", "reliability", "intermediate"),
    ("doc:96", "Copyright and intellectual property considerations in AI-generated content", "ethics", "legal", "intermediate"),
    ("doc:97", "Environmental impact of AI: compute costs, carbon footprint, and green AI", "ethics", "sustainability", "intermediate"),
    ("doc:98", "Responsible AI deployment: governance frameworks and audit procedures", "ethics", "governance", "advanced"),
    ("doc:99", "AI regulation: EU AI Act, US executive orders, and global policy landscape", "ethics", "regulation", "intermediate"),
]

# Knowledge graph edges: (src_idx, dst_idx, relationship)
EDGES = [
    # Foundations → Neural Nets
    (0, 1, "covers"), (0, 2, "covers"), (0, 3, "covers"),
    (1, 10, "prerequisite"), (7, 10, "prerequisite"),
    (10, 11, "prerequisite"), (10, 12, "prerequisite"),
    (10, 13, "enables"), (10, 14, "enables"), (10, 15, "enables"),
    # Neural Nets → Transformers
    (12, 20, "inspires"), (11, 20, "inspires"),
    (20, 21, "component_of"), (21, 22, "variant"), (21, 23, "variant"), (21, 24, "variant"),
    (22, 25, "technique"), (23, 25, "technique"),
    (25, 26, "followed_by"), (26, 27, "enables"),
    (23, 28, "exhibits"), (21, 29, "uses"),
    (23, 30, "optimized_by"), (23, 31, "optimized_by"), (23, 32, "optimized_by"),
    (21, 33, "extends"), (21, 34, "extends"),
    # Transformers → RAG
    (22, 36, "enables"), (40, 36, "uses"),
    (35, 36, "component"), (35, 37, "component"), (35, 38, "component"),
    (35, 39, "requires"), (35, 48, "technique"),
    (36, 41, "uses"), (40, 41, "uses"),
    (35, 42, "evaluated_by"), (35, 43, "extends"), (44, 35, "extends"),
    (45, 35, "extends"), (46, 35, "extends"), (46, 68, "related_to"),
    (47, 35, "optimizes"), (35, 49, "requires"),
    # RAG ↔ Vector DB
    (36, 50, "uses"), (50, 51, "implements"), (50, 52, "implements"),
    (51, 53, "extends"), (50, 54, "evaluated_by"),
    (50, 55, "feature"), (50, 56, "feature"), (50, 57, "feature"),
    (50, 58, "feature"), (50, 59, "feature"),
    # Knowledge Graphs
    (60, 61, "queried_via"), (60, 62, "built_by"), (62, 63, "includes"),
    (60, 64, "embedded_by"), (60, 65, "traversed_by"), (65, 66, "includes"),
    (60, 67, "extends"), (68, 60, "uses"), (68, 50, "uses"),
    (60, 69, "designed_with"),
    # Agents
    (70, 71, "uses"), (70, 72, "uses"), (70, 73, "extends"),
    (70, 74, "evaluated_by"), (75, 70, "specializes"), (76, 70, "specializes"),
    (70, 77, "implemented_by"), (70, 78, "constrained_by"), (71, 79, "requires"),
    (45, 70, "type_of"), (72, 47, "uses"),
    # MLOps
    (80, 81, "followed_by"), (82, 80, "feeds"), (83, 84, "integrates"),
    (84, 85, "manages"), (86, 83, "feeds"), (80, 87, "validated_by"),
    (80, 88, "deploys_to"), (89, 80, "optimizes"),
    # Ethics
    (90, 91, "requires"), (92, 93, "tested_by"), (92, 78, "related_to"),
    (94, 92, "supports"), (95, 42, "measured_by"), (95, 49, "mitigated_by"),
    (96, 98, "governed_by"), (97, 98, "governed_by"), (98, 99, "shaped_by"),
]


def vec_bytes(embedding: list[float]) -> bytes:
    return struct.pack(f"<{len(embedding)}f", *embedding)


def print_results(label: str, result: list, max_show: int = 5):
    count = int(result[0])
    print(f"  {label}: {count} result(s)")
    i = 1
    shown = 0
    while i < len(result) and shown < max_show:
        key = result[i].decode() if isinstance(result[i], bytes) else str(result[i])
        i += 1
        score = "?"
        if i < len(result) and isinstance(result[i], list):
            pairs = result[i]
            for j in range(0, len(pairs), 2):
                if pairs[j] == b"__vec_score":
                    score = pairs[j+1].decode() if isinstance(pairs[j+1], bytes) else str(pairs[j+1])
            i += 1
        elif i < len(result) and isinstance(result[i], bytes) and result[i] == b"__vec_score":
            score = result[i+1].decode() if isinstance(result[i+1], bytes) else str(result[i+1])
            i += 2
        # Look up title
        title = TITLES.get(key, "")
        print(f"    {key:12s}  score={score:12s}  {title[:60]}")
        shown += 1
    if count > max_show:
        print(f"    ... and {count - max_show} more")


def main():
    t_total = time.time()

    # ── Load Model ────────────────────────────────────────────────────────────
    print("Loading MiniLM model...")
    t0 = time.time()
    model = SentenceTransformer("all-MiniLM-L6-v2")
    dim = model.get_sentence_embedding_dimension()
    print(f"  Model loaded in {time.time()-t0:.1f}s — dimension={dim}")

    # ── Embed All Documents ───────────────────────────────────────────────────
    print(f"\nEmbedding {len(DOCUMENTS)} documents...")
    t0 = time.time()
    texts = [doc[1] for doc in DOCUMENTS]
    embeddings = model.encode(texts, show_progress_bar=False, normalize_embeddings=True)
    print(f"  Embedded in {time.time()-t0:.1f}s")

    # ── Connect to Moon ───────────────────────────────────────────────────────
    r = redis.Redis(host="localhost", port=MOON_PORT, decode_responses=False)
    r.ping()
    print(f"\nConnected to Moon on port {MOON_PORT}")

    # Build title lookup
    global TITLES
    TITLES = {doc[0]: doc[1] for doc in DOCUMENTS}

    # ╔══════════════════════════════════════════════════════╗
    # ║  1. Create Index & Ingest                            ║
    # ╚══════════════════════════════════════════════════════╝
    print("\n" + "=" * 70)
    print("1. CREATE INDEX + INGEST 100 DOCUMENTS (MiniLM 384-dim)")
    print("=" * 70)

    try:
        r.execute_command("FT.DROPINDEX", INDEX)
    except Exception:
        pass

    r.execute_command(
        "FT.CREATE", INDEX, "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
        "vec", "VECTOR", "HNSW", "6", "DIM", str(dim), "TYPE", "FLOAT32",
        "DISTANCE_METRIC", "COSINE",
    )

    t0 = time.time()
    for i, (key, text, domain, topic, level) in enumerate(DOCUMENTS):
        blob = vec_bytes(embeddings[i].tolist())
        r.hset(key, mapping={
            "vec": blob, "title": text, "domain": domain,
            "topic": topic, "level": level,
        })
    ingest_ms = (time.time() - t0) * 1000
    print(f"  Ingested 100 docs in {ingest_ms:.0f}ms ({100/max(ingest_ms/1000, 0.001):.0f} docs/sec)")

    time.sleep(1)  # Let auto-indexing settle

    info = r.execute_command("FT.INFO", INDEX)
    for i, v in enumerate(info):
        if v == b"num_docs":
            print(f"  Index num_docs: {int(info[i+1])}")
            break

    # ╔══════════════════════════════════════════════════════╗
    # ║  2. Build Knowledge Graph                            ║
    # ╚══════════════════════════════════════════════════════╝
    print("\n" + "=" * 70)
    print("2. BUILD KNOWLEDGE GRAPH (100 nodes, ~90 edges)")
    print("=" * 70)

    try:
        r.execute_command("GRAPH.CREATE", GRAPH)
    except Exception:
        pass

    t0 = time.time()
    nodes = {}
    for key, text, domain, topic, level in DOCUMENTS:
        nid = int(r.execute_command("GRAPH.ADDNODE", GRAPH, domain, "_key", key, "topic", topic))
        nodes[key] = nid

    for src_idx, dst_idx, rel in EDGES:
        src_key = DOCUMENTS[src_idx][0]
        dst_key = DOCUMENTS[dst_idx][0]
        r.execute_command("GRAPH.ADDEDGE", GRAPH, str(nodes[src_key]), str(nodes[dst_key]), rel)

    graph_ms = (time.time() - t0) * 1000
    print(f"  Built graph in {graph_ms:.0f}ms — {len(nodes)} nodes, {len(EDGES)} edges")

    # ╔══════════════════════════════════════════════════════╗
    # ║  3. RAG — Semantic Search                            ║
    # ╚══════════════════════════════════════════════════════╝
    print("\n" + "=" * 70)
    print("3. RAG — SEMANTIC SEARCH")
    print("=" * 70)

    queries = [
        "How does retrieval augmented generation work?",
        "What are the best practices for deploying ML models in production?",
        "Explain the attention mechanism in transformers",
        "How to build a knowledge graph from text?",
        "What safety measures should AI agents have?",
    ]

    for query_text in queries:
        q_emb = model.encode(query_text, normalize_embeddings=True)
        q_blob = vec_bytes(q_emb.tolist())

        t0 = time.time()
        result = r.execute_command(
            "FT.SEARCH", INDEX, "*=>[KNN 5 @vec $q]",
            "PARAMS", "2", "q", q_blob,
        )
        latency_us = (time.time() - t0) * 1_000_000
        print(f"\n  Query: \"{query_text}\" ({latency_us:.0f}us)")
        count = int(result[0])
        i = 1
        shown = 0
        while i < len(result) and shown < 3:
            key = result[i].decode()
            i += 1
            score = "?"
            if i < len(result):
                if isinstance(result[i], list):
                    pairs = result[i]
                    for j in range(0, len(pairs), 2):
                        if pairs[j] == b"__vec_score":
                            score = pairs[j+1].decode()
                    i += 1
                elif result[i] == b"__vec_score":
                    score = result[i+1].decode()
                    i += 2
            print(f"    {key:12s} score={score:10s} {TITLES.get(key, '')[:55]}")
            shown += 1

    # ╔══════════════════════════════════════════════════════╗
    # ║  4. Filtered Search — Domain & Level                 ║
    # ╚══════════════════════════════════════════════════════╝
    print("\n" + "=" * 70)
    print("4. FILTERED SEARCH — by domain and level")
    print("=" * 70)

    q_emb = model.encode("How to optimize neural network training?", normalize_embeddings=True)
    q_blob = vec_bytes(q_emb.tolist())

    # Filter: only RAG domain
    result = r.execute_command(
        "FT.SEARCH", INDEX, "@domain:{rag}=>[KNN 5 @vec $q]",
        "PARAMS", "2", "q", q_blob,
    )
    print_results("domain=rag", result)

    # Filter: only beginner level
    result = r.execute_command(
        "FT.SEARCH", INDEX, "@level:{beginner}=>[KNN 5 @vec $q]",
        "PARAMS", "2", "q", q_blob,
    )
    print_results("level=beginner", result)

    # Filter: only advanced + agents domain
    result = r.execute_command(
        "FT.SEARCH", INDEX, "@domain:{agents}=>[KNN 5 @vec $q]",
        "PARAMS", "2", "q", q_blob,
    )
    print_results("domain=agents", result)

    # ╔══════════════════════════════════════════════════════╗
    # ║  5. GraphRAG — Vector + Graph Expansion              ║
    # ╚══════════════════════════════════════════════════════╝
    print("\n" + "=" * 70)
    print("5. GRAPHRAG — vector search + graph expansion")
    print("=" * 70)

    q_emb = model.encode("What is retrieval augmented generation?", normalize_embeddings=True)
    q_blob = vec_bytes(q_emb.tolist())

    # Pure KNN
    result = r.execute_command(
        "FT.SEARCH", INDEX, "*=>[KNN 3 @vec $q]",
        "PARAMS", "2", "q", q_blob,
    )
    print_results("Pure KNN (top 3)", result)

    # KNN + Graph expand
    result = r.execute_command(
        "FT.SEARCH", INDEX, "*=>[KNN 3 @vec $q]",
        "PARAMS", "2", "q", q_blob,
        "EXPAND", "GRAPH", GRAPH, "DEPTH", "2",
    )
    print_results("KNN + EXPAND GRAPH depth=2", result, max_show=8)

    # FT.EXPAND from a specific doc
    try:
        expand = r.execute_command("FT.EXPAND", INDEX, "doc:35", "DEPTH", "2", "GRAPH", GRAPH)
        if isinstance(expand, list):
            c = int(expand[0])
            keys = [expand[i].decode() for i in range(1, len(expand), 2) if isinstance(expand[i], bytes)]
            print(f"\n  FT.EXPAND from doc:35 (RAG overview) depth=2: {c} reachable")
            for k in keys[:8]:
                print(f"    → {k:12s} {TITLES.get(k, '')[:55]}")
            if c > 8:
                print(f"    ... and {c-8} more")
    except Exception as e:
        print(f"  FT.EXPAND: {e}")

    # ╔══════════════════════════════════════════════════════╗
    # ║  6. FT.RECOMMEND — Content-based recommendations     ║
    # ╚══════════════════════════════════════════════════════╝
    print("\n" + "=" * 70)
    print("6. FT.RECOMMEND — 'more like doc:35 (RAG), unlike doc:90 (ethics)'")
    print("=" * 70)

    result = r.execute_command(
        "FT.RECOMMEND", INDEX,
        "POSITIVE", "doc:35", "doc:36",  # RAG + dense retrieval
        "NEGATIVE", "doc:90",             # Not ethics
        "K", "5",
    )
    print_results("Recommend (RAG-like, not ethics)", result)

    # ╔══════════════════════════════════════════════════════╗
    # ║  7. Session-Aware Multi-Turn Retrieval               ║
    # ╚══════════════════════════════════════════════════════╝
    print("\n" + "=" * 70)
    print("7. SESSION — multi-turn conversation dedup")
    print("=" * 70)

    session = "session:user1:conv42"
    q_emb = model.encode("Tell me about transformer architectures", normalize_embeddings=True)
    q_blob = vec_bytes(q_emb.tolist())

    for turn in range(1, 5):
        result = r.execute_command(
            "FT.SEARCH", INDEX, "*=>[KNN 3 @vec $q]",
            "PARAMS", "2", "q", q_blob,
            "SESSION", session,
        )
        count = int(result[0])
        keys = [result[i].decode() for i in range(1, len(result), 2) if isinstance(result[i], bytes)]
        print(f"  Turn {turn}: {count} new results — {keys}")

    # Session state
    stype = r.type(session)
    if stype == b"zset":
        seen = r.zcard(session)
        print(f"  Session tracks {seen} seen docs (type=zset)")
    r.expire(session, 3600)
    print(f"  Session TTL set to 1h")

    # ╔══════════════════════════════════════════════════════╗
    # ║  8. FT.NAVIGATE — Multi-hop Knowledge Navigation     ║
    # ╚══════════════════════════════════════════════════════╝
    print("\n" + "=" * 70)
    print("8. FT.NAVIGATE — multi-hop retrieval")
    print("=" * 70)

    q_emb = model.encode("How to build production RAG systems?", normalize_embeddings=True)
    q_blob = vec_bytes(q_emb.tolist())

    try:
        result = r.execute_command(
            "FT.NAVIGATE", INDEX, "*=>[KNN 3 @vec $q]",
            "HOPS", "2",
            "PARAMS", "2", "q", q_blob,
        )
        print_results("NAVIGATE: KNN→graph→re-rank (2 hops)", result, max_show=8)
    except Exception as e:
        print(f"  FT.NAVIGATE: {e}")

    # ╔══════════════════════════════════════════════════════╗
    # ║  9. Semantic Cache                                   ║
    # ╚══════════════════════════════════════════════════════╝
    print("\n" + "=" * 70)
    print("9. SEMANTIC CACHE — cache check + fallback")
    print("=" * 70)

    # Store a cached response
    cache_q = model.encode("What is RAG?", normalize_embeddings=True)
    r.hset("cache:sem:rag_basic", mapping={
        "vec": vec_bytes(cache_q.tolist()),
        "response": "RAG combines retrieval with generation to ground LLM outputs in external knowledge.",
        "model": "gpt-4o", "tokens": "42",
    })
    r.expire("cache:sem:rag_basic", 3600)

    q_blob = vec_bytes(cache_q.tolist())
    try:
        result = r.execute_command(
            "FT.CACHESEARCH", INDEX, "cache:sem:",
            "*=>[KNN 3 @vec $q]",
            "PARAMS", "2", "q", q_blob,
            "THRESHOLD", "0.98",
            "FALLBACK", "KNN", "3",
        )
        count = int(result[0])
        if count > 0 and isinstance(result[2], list):
            meta = dict(zip(result[2][::2], result[2][1::2]))
            hit = meta.get(b"cache_hit", b"?").decode()
            print(f"  FT.CACHESEARCH: {count} results, cache_hit={hit}")
        else:
            print(f"  FT.CACHESEARCH: {count} results")
    except Exception as e:
        print(f"  FT.CACHESEARCH: {e}")

    # ╔══════════════════════════════════════════════════════╗
    # ║  10. Range Search                                    ║
    # ╚══════════════════════════════════════════════════════╝
    print("\n" + "=" * 70)
    print("10. RANGE SEARCH — distance threshold filter")
    print("=" * 70)

    q_emb = model.encode("BERT language model", normalize_embeddings=True)
    q_blob = vec_bytes(q_emb.tolist())

    result_all = r.execute_command(
        "FT.SEARCH", INDEX, "*=>[KNN 10 @vec $q]", "PARAMS", "2", "q", q_blob,
    )
    result_range = r.execute_command(
        "FT.SEARCH", INDEX, "*=>[KNN 10 @vec $q]", "PARAMS", "2", "q", q_blob,
        "RANGE", "0.5",  # Only results with cosine similarity >= 0.5
    )
    print(f"  KNN 10 (no range):  {int(result_all[0])} results")
    print(f"  KNN 10 (RANGE 0.5): {int(result_range[0])} results (similarity >= 0.5)")
    print_results("  Top results", result_range, max_show=5)

    # ╔══════════════════════════════════════════════════════╗
    # ║  Summary                                             ║
    # ╚══════════════════════════════════════════════════════╝
    total_time = time.time() - t_total
    print("\n" + "=" * 70)
    print(f"COMPLETE — {len(DOCUMENTS)} docs, {dim}-dim MiniLM, {len(EDGES)} graph edges")
    print(f"Total time: {total_time:.1f}s (includes model loading + embedding)")
    print("=" * 70)

    # Cleanup
    r.execute_command("FT.DROPINDEX", INDEX)
    r.delete(session, "cache:sem:rag_basic")


if __name__ == "__main__":
    main()
