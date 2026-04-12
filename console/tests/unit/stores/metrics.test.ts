import { beforeEach, describe, expect, it } from "vitest";
import { useMetricsStore } from "@/stores/metrics";
import type { MetricEvent, ServerInfo, SlowlogEntry } from "@/types/metrics";

const reset = () =>
  useMetricsStore.setState({
    connected: false,
    latest: null,
    history: [],
    serverInfo: null,
    slowlog: [],
  });

const mkEvent = (ops: number): MetricEvent => ({
  event: "metrics",
  total_ops: ops * 10,
  ops_per_sec: ops,
  total_memory: 1024,
  connected_clients: 1,
  uptime_seconds: 10,
  total_keys: 10,
});

const mkServerInfo = (): ServerInfo => ({
  server: { version: "0.1.5" },
  memory: {},
  clients: {},
  stats: {},
  keyspace: {},
  persistence: {},
  replication: {},
  cpu: {},
});

const mkSlowEntry = (id: number): SlowlogEntry => ({
  id,
  timestamp: Date.now(),
  duration_us: 1000,
  command: "GET",
  args: ["key"],
});

describe("metrics store", () => {
  beforeEach(reset);

  it("pushMetric appends a data point and sets latest", () => {
    useMetricsStore.getState().pushMetric(mkEvent(100));
    const s = useMetricsStore.getState();
    expect(s.history).toHaveLength(1);
    expect(s.history[0].ops_per_sec).toBe(100);
    expect(s.latest?.ops_per_sec).toBe(100);
  });

  it("caps history at 60 points and evicts oldest", () => {
    const { pushMetric } = useMetricsStore.getState();
    for (let i = 0; i < 65; i++) pushMetric(mkEvent(i));
    const s = useMetricsStore.getState();
    expect(s.history).toHaveLength(60);
    expect(s.history[0].ops_per_sec).toBe(5); // oldest 5 evicted (0..4)
    expect(s.history[59].ops_per_sec).toBe(64);
  });

  it("setConnected toggles connected state", () => {
    useMetricsStore.getState().setConnected(true);
    expect(useMetricsStore.getState().connected).toBe(true);
    useMetricsStore.getState().setConnected(false);
    expect(useMetricsStore.getState().connected).toBe(false);
  });

  it("setServerInfo stores the ServerInfo payload", () => {
    const info = mkServerInfo();
    useMetricsStore.getState().setServerInfo(info);
    expect(useMetricsStore.getState().serverInfo).toEqual(info);
  });

  it("setSlowlog replaces slowlog array", () => {
    useMetricsStore.getState().setSlowlog([mkSlowEntry(1), mkSlowEntry(2)]);
    expect(useMetricsStore.getState().slowlog).toHaveLength(2);
    expect(useMetricsStore.getState().slowlog[0].id).toBe(1);
  });

  it("pushMetric stamps history point with a recent timestamp", () => {
    const before = Date.now();
    useMetricsStore.getState().pushMetric(mkEvent(1));
    const after = Date.now();
    const point = useMetricsStore.getState().history[0];
    expect(point.time).toBeGreaterThanOrEqual(before);
    expect(point.time).toBeLessThanOrEqual(after);
    expect(point.total_memory).toBe(1024);
    expect(point.connected_clients).toBe(1);
    expect(point.total_keys).toBe(10);
  });
});
