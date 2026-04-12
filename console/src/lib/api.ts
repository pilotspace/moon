import type { ServerInfo, SlowlogEntry } from "@/types/metrics";

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
