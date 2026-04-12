import type { ServerInfo, SlowlogEntry } from "@/types/metrics";
import type { ScanResult, KeyType } from "@/types/browser";

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

/** Execute a RESP command via REST API */
export async function execCommand(cmd: string, args: string[] = []): Promise<unknown> {
  return apiPost("/command", { cmd, args });
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
