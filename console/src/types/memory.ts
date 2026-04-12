/** Treemap node for memory visualization */
export interface MemoryNode {
  name: string;           // key name or namespace prefix
  size: number;           // bytes from MEMORY USAGE
  type: string;           // "string" | "hash" | "list" | "set" | "zset" | "stream" | "namespace"
  children?: MemoryNode[];
  fullKey?: string;       // full Redis key (leaf nodes only)
}

/** Parsed INFO commandstats entry */
export interface CommandStat {
  command: string;
  calls: number;
  usec: number;           // total microseconds
  usec_per_call: number;
  rejected_calls: number;
  failed_calls: number;
}

/** Histogram bucket for latency distribution */
export interface LatencyBucket {
  range: string;          // e.g. "0-1ms", "1-10ms", "10-100ms", "100ms-1s", ">1s"
  count: number;
}
