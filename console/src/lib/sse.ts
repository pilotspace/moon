import { useMetricsStore } from "@/stores/metrics";
import type { MetricEvent } from "@/types/metrics";

let eventSource: EventSource | null = null;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

const SSE_URL = "/events";
const RECONNECT_DELAY_MS = 3000;

export function connectSSE(): void {
  if (eventSource) return;

  const store = useMetricsStore.getState();
  eventSource = new EventSource(SSE_URL);

  eventSource.onopen = () => {
    store.setConnected(true);
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  };

  eventSource.onmessage = (ev: MessageEvent) => {
    try {
      const data = JSON.parse(ev.data) as MetricEvent;
      useMetricsStore.getState().pushMetric(data);
    } catch {
      // Ignore malformed events
    }
  };

  eventSource.onerror = () => {
    store.setConnected(false);
    eventSource?.close();
    eventSource = null;
    // Auto-reconnect
    if (!reconnectTimer) {
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectSSE();
      }, RECONNECT_DELAY_MS);
    }
  };
}

export function disconnectSSE(): void {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  if (eventSource) {
    eventSource.close();
    eventSource = null;
    useMetricsStore.getState().setConnected(false);
  }
}
