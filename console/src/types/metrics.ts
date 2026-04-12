/** Mirrors MetricEvent from src/admin/sse_stream.rs */
export interface MetricEvent {
  event: string;
  total_ops: number;
  ops_per_sec: number;
  total_memory: number;
  connected_clients: number;
  uptime_seconds: number;
  total_keys: number;
}

/** A timestamped metric data point for time-series charts */
export interface MetricDataPoint {
  time: number; // Date.now() when received
  ops_per_sec: number;
  total_memory: number;
  connected_clients: number;
  total_keys: number;
}

/** Server INFO response (from GET /api/v1/info) */
export interface ServerInfo {
  server: Record<string, string>;
  memory: Record<string, string>;
  clients: Record<string, string>;
  stats: Record<string, string>;
  keyspace: Record<string, string>;
  persistence: Record<string, string>;
  replication: Record<string, string>;
  cpu: Record<string, string>;
}

/** Slowlog entry */
export interface SlowlogEntry {
  id: number;
  timestamp: number;
  duration_us: number;
  command: string;
  args: string[];
}
