import { create } from "zustand";
import type { MetricDataPoint, MetricEvent, ServerInfo, SlowlogEntry } from "@/types/metrics";

const MAX_HISTORY = 60; // 60 data points = 1 minute at 1 Hz

interface MetricsState {
  // Connection
  connected: boolean;
  setConnected: (connected: boolean) => void;

  // Latest snapshot
  latest: MetricEvent | null;

  // Time-series history (rolling window)
  history: MetricDataPoint[];

  // Server INFO (fetched on load + periodically)
  serverInfo: ServerInfo | null;
  setServerInfo: (info: ServerInfo) => void;

  // Slowlog
  slowlog: SlowlogEntry[];
  setSlowlog: (entries: SlowlogEntry[]) => void;

  // Actions
  pushMetric: (event: MetricEvent) => void;
}

export const useMetricsStore = create<MetricsState>((set) => ({
  connected: false,
  setConnected: (connected) => set({ connected }),

  latest: null,
  history: [],

  serverInfo: null,
  setServerInfo: (info) => set({ serverInfo: info }),

  slowlog: [],
  setSlowlog: (entries) => set({ slowlog: entries }),

  pushMetric: (event) =>
    set((state) => {
      const point: MetricDataPoint = {
        time: Date.now(),
        ops_per_sec: event.ops_per_sec,
        total_memory: event.total_memory,
        connected_clients: event.connected_clients,
        total_keys: event.total_keys,
      };
      const history = [...state.history, point];
      if (history.length > MAX_HISTORY) {
        history.splice(0, history.length - MAX_HISTORY);
      }
      return { latest: event, history };
    }),
}));
