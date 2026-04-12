import { create } from "zustand";
import type { VectorIndex, VectorPoint, UmapWorkerResponse } from "@/types/vector";
import { fetchIndexList, fetchIndexInfo, fetchVectorData, searchKnn } from "@/lib/vector-api";
import type { KnnResult } from "@/lib/vector-api";

interface VectorState {
  // Index list
  indexes: string[];
  selectedIndex: string | null;
  indexInfo: VectorIndex | null;

  // Points and projection
  points: VectorPoint[];
  projectedPositions: Float32Array | null;
  umapProgress: { epoch: number; total: number } | null;

  // Loading
  loading: boolean;

  // Interaction
  hoveredPointId: string | null;
  selectedPointIds: Set<string>;
  colorBy: "segment" | "label" | "none";
  showHnsw: boolean;

  // KNN search
  knnResults: KnnResult[];
  knnQueryPointId: string | null;

  // Lasso selection
  lassoSelectedIds: Set<string>;

  // HNSW edges
  hnswEdges: Float32Array | null;

  // Actions
  loadIndexes: () => Promise<void>;
  selectIndex: (name: string) => Promise<void>;
  runUmap: () => void;
  setHovered: (id: string | null) => void;
  setSelected: (ids: Set<string>) => void;
  setColorBy: (mode: "segment" | "label" | "none") => void;
  setShowHnsw: (show: boolean) => void;
  searchFromPoint: (pointId: string, k?: number) => Promise<void>;
  performKnnSearch: (queryVector: number[], k?: number) => Promise<void>;
  setLassoSelected: (ids: Set<string>) => void;
  clearKnnResults: () => void;
}

let umapWorker: Worker | null = null;

export const useVectorStore = create<VectorState>((set, get) => ({
  indexes: [],
  selectedIndex: null,
  indexInfo: null,

  points: [],
  projectedPositions: null,
  umapProgress: null,

  loading: false,

  hoveredPointId: null,
  selectedPointIds: new Set<string>(),
  colorBy: "segment",
  showHnsw: false,

  knnResults: [],
  knnQueryPointId: null,
  lassoSelectedIds: new Set<string>(),
  hnswEdges: null,

  loadIndexes: async () => {
    try {
      const indexes = await fetchIndexList();
      set({ indexes });
    } catch {
      set({ indexes: [] });
    }
  },

  selectIndex: async (name: string) => {
    set({ loading: true, selectedIndex: name, projectedPositions: null, umapProgress: null });
    try {
      const [info, points] = await Promise.all([
        fetchIndexInfo(name),
        fetchVectorData(name),
      ]);
      set({ indexInfo: info, points, loading: false });
      // Auto-run UMAP if we have points with vectors
      if (points.length > 0 && points[0].vector.length > 0) {
        get().runUmap();
      }
    } catch {
      set({ indexInfo: null, points: [], loading: false });
    }
  },

  runUmap: () => {
    const { points } = get();
    if (points.length === 0) return;

    const dims = points[0].vector.length;
    if (dims === 0) return;

    // Terminate previous worker
    if (umapWorker) {
      umapWorker.terminate();
      umapWorker = null;
    }

    set({ umapProgress: { epoch: 0, total: 1 }, projectedPositions: null });

    // Flatten all vectors into a single Float32Array
    const count = points.length;
    const flat = new Float32Array(count * dims);
    for (let i = 0; i < count; i++) {
      flat.set(points[i].vector, i * dims);
    }

    umapWorker = new Worker(
      new URL("../workers/umap.worker.ts", import.meta.url),
      { type: "module" },
    );

    umapWorker.onmessage = (e: MessageEvent<UmapWorkerResponse>) => {
      const msg = e.data;
      if (msg.type === "progress") {
        set({ umapProgress: { epoch: msg.epoch, total: msg.totalEpochs } });
      } else if (msg.type === "complete") {
        set({
          projectedPositions: msg.positions,
          umapProgress: null,
        });
        if (umapWorker) {
          umapWorker.terminate();
          umapWorker = null;
        }
      }
    };

    // Transfer the buffer for zero-copy
    const transferable = flat.buffer;
    umapWorker.postMessage(
      { type: "run", vectors: flat, dims, count },
      { transfer: [transferable] },
    );
  },

  setHovered: (id) => set({ hoveredPointId: id }),
  setSelected: (ids) => set({ selectedPointIds: ids }),
  setColorBy: (mode) => set({ colorBy: mode }),
  setShowHnsw: (show) => set({ showHnsw: show }),

  searchFromPoint: async (pointId: string, k = 10) => {
    const { points, selectedIndex } = get();
    if (!selectedIndex) return;
    const point = points.find((p) => p.id === pointId);
    if (!point) return;
    set({ knnQueryPointId: pointId });
    await get().performKnnSearch(Array.from(point.vector), k);
  },

  performKnnSearch: async (queryVector: number[], k = 10) => {
    const { selectedIndex, points } = get();
    if (!selectedIndex) return;
    try {
      const results = await searchKnn(selectedIndex, queryVector, k);
      // Resolve pointIndex by matching keys
      const keyToIndex = new Map<string, number>();
      for (let i = 0; i < points.length; i++) {
        keyToIndex.set(points[i].key, i);
      }
      for (const r of results) {
        r.pointIndex = keyToIndex.get(r.key) ?? -1;
      }
      set({ knnResults: results });
    } catch {
      set({ knnResults: [] });
    }
  },

  setLassoSelected: (ids) => set({ lassoSelectedIds: ids }),

  clearKnnResults: () => set({ knnResults: [], knnQueryPointId: null }),
}));
