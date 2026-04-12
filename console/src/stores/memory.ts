import { create } from "zustand";
import type { SlowlogEntry } from "@/types/metrics";
import type { MemoryNode, CommandStat } from "@/types/memory";
import { fetchMemoryTreemap, fetchSlowlog, fetchCommandStats } from "@/lib/api";

interface MemoryState {
  // Treemap
  treemapData: MemoryNode | null;
  treemapPath: string[];

  // Slowlog (separate from dashboard, fetches 128 entries)
  slowlog: SlowlogEntry[];

  // Command stats
  commandStats: CommandStat[];

  // Loading state
  loading: boolean;

  // Actions
  loadMemoryData: () => Promise<void>;
  loadSlowlog: () => Promise<void>;
  loadCommandStats: () => Promise<void>;
  drillDown: (namespace: string) => void;
  drillUp: () => void;
  resetDrill: () => void;

  // Computed
  currentTreemapNode: () => MemoryNode | null;
}

export const useMemoryStore = create<MemoryState>((set, get) => ({
  treemapData: null,
  treemapPath: [],
  slowlog: [],
  commandStats: [],
  loading: false,

  loadMemoryData: async () => {
    set({ loading: true });
    try {
      const data = await fetchMemoryTreemap(5000);
      set({ treemapData: data, loading: false });
    } catch {
      set({ loading: false });
    }
  },

  loadSlowlog: async () => {
    try {
      const entries = await fetchSlowlog(128);
      set({ slowlog: entries });
    } catch {
      // ignore
    }
  },

  loadCommandStats: async () => {
    try {
      const stats = await fetchCommandStats();
      set({ commandStats: stats });
    } catch {
      // ignore
    }
  },

  drillDown: (namespace: string) => {
    set((state) => ({ treemapPath: [...state.treemapPath, namespace] }));
  },

  drillUp: () => {
    set((state) => ({ treemapPath: state.treemapPath.slice(0, -1) }));
  },

  resetDrill: () => {
    set({ treemapPath: [] });
  },

  currentTreemapNode: () => {
    const { treemapData, treemapPath } = get();
    if (!treemapData) return null;
    let current = treemapData;
    for (const segment of treemapPath) {
      const child = current.children?.find((c) => c.name === segment);
      if (!child) return current;
      current = child;
    }
    return current;
  },
}));
