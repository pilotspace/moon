/** Redis data type identifiers */
export type KeyType = "string" | "hash" | "list" | "set" | "zset" | "stream";

/** A key entry from SCAN with enriched metadata */
export interface KeyEntry {
  name: string;
  type: KeyType | null;      // null = not yet fetched
  ttl: number | null;        // null = not yet fetched, -1 = persistent, >0 = seconds
  memoryBytes: number | null; // null = not yet fetched
  selected: boolean;
}

/** Filter state for key list */
export interface KeyFilter {
  pattern: string;           // SCAN MATCH pattern, default "*"
  type: KeyType | "all";     // TYPE filter, default "all"
  ttlStatus: "all" | "persistent" | "expiring"; // TTL filter
}

/** Tree node for namespace hierarchy */
export interface NamespaceNode {
  name: string;              // segment name (e.g. "user")
  fullPrefix: string;        // full prefix (e.g. "user:session:")
  children: Map<string, NamespaceNode>;
  keyCount: number;          // aggregate key count
  expanded: boolean;
  loading: boolean;
}

/** SCAN response from REST API */
export interface ScanResult {
  cursor: string;
  keys: string[];
}

/** TTL urgency tier per KV-09 */
export type TtlTier = "green" | "amber" | "red" | "none";

export function getTtlTier(ttl: number | null): TtlTier {
  if (ttl === null || ttl === -1) return "none";
  if (ttl <= 30) return "red";
  if (ttl <= 300) return "amber";    // <5 minutes
  return "green";
}

/** Type badge color mapping */
export const TYPE_COLORS: Record<KeyType, string> = {
  string: "bg-blue-500/20 text-blue-400",
  hash: "bg-purple-500/20 text-purple-400",
  list: "bg-green-500/20 text-green-400",
  set: "bg-amber-500/20 text-amber-400",
  zset: "bg-pink-500/20 text-pink-400",
  stream: "bg-cyan-500/20 text-cyan-400",
};
