import type { ServerInfo, SlowlogEntry } from "@/types/metrics";
import type { ScanResult, KeyType } from "@/types/browser";
import type { CommandStat, MemoryNode } from "@/types/memory";

const API_BASE = "/api/v1";

async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`API ${path}: ${res.status}`);
  return res.json() as Promise<T>;
}

async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`API ${path}: ${res.status}`);
  return res.json() as Promise<T>;
}

/** Fetch full server INFO */
export async function fetchServerInfo(): Promise<ServerInfo> {
  return apiGet<ServerInfo>("/info");
}

/** Execute a RESP command via REST API. Returns the unwrapped result value. */
export async function execCommand(cmd: string, args: string[] = []): Promise<unknown> {
  const resp = await apiPost<{ result?: unknown; error?: string; type?: string }>("/command", { cmd, args });
  if (resp && typeof resp === "object" && "error" in resp && resp.error) {
    throw new Error(String(resp.error));
  }
  return resp && typeof resp === "object" && "result" in resp ? resp.result : resp;
}

/** SCAN keys with optional pattern, count, and type filter */
export async function scanKeys(
  cursor: string = "0",
  pattern: string = "*",
  count: number = 100,
  type?: KeyType,
): Promise<ScanResult> {
  const params = new URLSearchParams({ cursor, pattern, count: String(count) });
  if (type) params.set("type", type);
  return apiGet<ScanResult>(`/keys?${params}`);
}

/** Get key type via TYPE command */
export async function getKeyType(key: string): Promise<string> {
  const result = await execCommand("TYPE", [key]);
  return String(result);
}

/** Get key TTL */
export async function getKeyTtl(key: string): Promise<number> {
  const res = await apiGet<{ ttl: number }>(`/key/${encodeURIComponent(key)}/ttl`);
  return res.ttl;
}

/** Set key TTL. ttl=-1 to persist (remove TTL). */
export async function setKeyTtl(key: string, ttl: number): Promise<void> {
  await apiPost(`/key/${encodeURIComponent(key)}/ttl`, { ttl });
}

/** Get key value with type detection */
export async function getKeyValue(key: string): Promise<{ type: string; value: unknown }> {
  return apiGet<{ type: string; value: unknown }>(`/key/${encodeURIComponent(key)}`);
}

/** Set string key value */
export async function setKeyValue(key: string, value: string): Promise<void> {
  await fetch(`${API_BASE}/key/${encodeURIComponent(key)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ value }),
  });
}

/** Delete one or more keys */
export async function deleteKeys(keys: string[]): Promise<number> {
  let total = 0;
  for (const key of keys) {
    const res = await fetch(`${API_BASE}/key/${encodeURIComponent(key)}`, { method: "DELETE" });
    if (res.ok) {
      const data = await res.json();
      total += (data as { deleted: number }).deleted;
    }
  }
  return total;
}

/** Get memory usage for a key via MEMORY USAGE command */
export async function getKeyMemory(key: string): Promise<number> {
  const result = await execCommand("MEMORY", ["USAGE", key]);
  return typeof result === "number" ? result : 0;
}

/** SCAN with TYPE filter using the RESP command directly */
export async function scanKeysWithType(
  cursor: string,
  pattern: string,
  count: number,
  type: KeyType,
): Promise<ScanResult> {
  const result = await execCommand("SCAN", [cursor, "MATCH", pattern, "COUNT", String(count), "TYPE", type]);
  if (Array.isArray(result) && result.length === 2) {
    return { cursor: String(result[0]), keys: (result[1] as string[]).map(String) };
  }
  return { cursor: "0", keys: [] };
}

/** Fetch SLOWLOG GET entries */
export async function fetchSlowlog(count = 25): Promise<SlowlogEntry[]> {
  const result = await apiPost<unknown>("/command", { cmd: "SLOWLOG", args: ["GET", String(count)] });
  // Parse SLOWLOG response into structured entries
  if (!Array.isArray(result)) return [];
  return (result as unknown[][]).map((entry: unknown[]) => ({
    id: Number(entry[0]),
    timestamp: Number(entry[1]),
    duration_us: Number(entry[2]),
    command: String((entry[3] as string[])?.[0] ?? ""),
    args: ((entry[3] as string[])?.slice(1) ?? []).map(String),
  }));
}

/** Fetch INFO commandstats and parse into CommandStat[] */
export async function fetchCommandStats(): Promise<CommandStat[]> {
  const result = await execCommand("INFO", ["commandstats"]);
  const text = String(result);
  const stats: CommandStat[] = [];
  for (const line of text.split("\n")) {
    const match = line.match(
      /^cmdstat_(\w+):calls=(\d+),usec=(\d+),usec_per_call=([\d.]+),rejected_calls=(\d+),failed_calls=(\d+)/,
    );
    if (match) {
      stats.push({
        command: match[1],
        calls: Number(match[2]),
        usec: Number(match[3]),
        usec_per_call: Number(match[4]),
        rejected_calls: Number(match[5]),
        failed_calls: Number(match[6]),
      });
    }
  }
  return stats.sort((a, b) => b.usec - a.usec);
}

/** Build a MemoryNode tree from a flat list of keys with type and size */
function buildTreemapFromKeys(
  keys: { key: string; type: string; bytes: number }[],
): MemoryNode {
  const root: MemoryNode = { name: "keyspace", size: 0, type: "namespace", children: [] };

  for (const { key, type, bytes } of keys) {
    const parts = key.split(":");
    let current = root;

    for (let i = 0; i < parts.length - 1; i++) {
      const segment = parts[i];
      let child = current.children?.find((c) => c.name === segment && c.type === "namespace");
      if (!child) {
        child = { name: segment, size: 0, type: "namespace", children: [] };
        current.children ??= [];
        current.children.push(child);
      }
      current = child;
    }

    // Leaf node
    current.children ??= [];
    current.children.push({
      name: parts[parts.length - 1],
      size: bytes,
      type,
      fullKey: key,
    });
  }

  // Compute sizes bottom-up
  function computeSize(node: MemoryNode): number {
    if (!node.children || node.children.length === 0) return node.size;
    node.size = node.children.reduce((sum, c) => sum + computeSize(c), 0);
    return node.size;
  }
  computeSize(root);

  return root;
}

/** Fetch memory treemap data by scanning keys and getting their memory usage */
export async function fetchMemoryTreemap(maxKeys = 5000): Promise<MemoryNode> {
  const keys: { key: string; type: string; bytes: number }[] = [];
  let cursor = "0";
  do {
    const result = await scanKeys(cursor, "*", 500);
    cursor = result.cursor;
    for (const key of result.keys) {
      if (keys.length >= maxKeys) {
        cursor = "0";
        break;
      }
      const [typeResult, memResult] = await Promise.all([
        execCommand("TYPE", [key]),
        execCommand("MEMORY", ["USAGE", key]),
      ]);
      keys.push({
        key,
        type: String(typeResult).toLowerCase(),
        bytes: typeof memResult === "number" ? memResult : 0,
      });
    }
  } while (cursor !== "0");

  return buildTreemapFromKeys(keys);
}
