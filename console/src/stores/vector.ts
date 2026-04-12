import { create } from "zustand";
import type { VectorIndex, VectorPoint, UmapWorkerResponse } from "@/types/vector";
import { fetchIndexList, fetchIndexInfo, fetchVectorData } from "@/lib/vector-api";

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

  // Actions
  loadIndexes: () => Promise<void>;
  selectIndex: (name: string) => Promise<void>;
  runUmap: () => void;
  setHovered: (id: string | null) => void;
  setSelected: (ids: Set<string>) => void;
  setColorBy: (mode: "segment" | "label" | "none") => void;
  setShowHnsw: (show: boolean) => void;
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
}));
