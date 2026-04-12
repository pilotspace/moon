export interface SegmentInfo {
  type: "mutable" | "immutable";
  num_vectors: number;
  algorithm: string;
}

export interface VectorIndex {
  name: string;
  dimensions: number;
  metric: string; // "COSINE" | "L2" | "IP"
  num_docs: number;
  segments: SegmentInfo[];
  ef_runtime: number;
  compact_threshold: number;
}

export interface VectorPoint {
  id: string;
  key: string;
  vector: Float32Array;
  payload: Record<string, string>;
  segment: "mutable" | "immutable";
  // Filled after UMAP
  x?: number;
  y?: number;
  z?: number;
}

export type UmapWorkerRequest = {
  type: "run";
  vectors: Float32Array; // flattened [v0d0, v0d1, ..., v1d0, ...]
  dims: number;
  count: number;
  nNeighbors?: number; // default 15
  minDist?: number; // default 0.1
};

export type UmapWorkerResponse =
  | { type: "progress"; epoch: number; totalEpochs: number }
  | { type: "complete"; positions: Float32Array }; // flattened [x0,y0,z0, x1,y1,z1, ...]
