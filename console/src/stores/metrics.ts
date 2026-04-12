import { create } from "zustand";

interface MetricsState {
  connected: boolean;
  setConnected: (connected: boolean) => void;
}

export const useMetricsStore = create<MetricsState>((set) => ({
  connected: false,
  setConnected: (connected) => set({ connected }),
}));
