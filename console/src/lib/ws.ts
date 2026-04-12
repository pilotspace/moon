import type { QueryResult } from "@/types/console";
import { execCommand } from "@/lib/api";

let ws: WebSocket | null = null;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
let requestId = 0;
const pending = new Map<number, { resolve: (r: QueryResult) => void; reject: (e: Error) => void }>();
const listeners = new Set<(data: unknown) => void>();

const RECONNECT_DELAY_MS = 3000;

function getWsUrl(): string {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}/ws`;
}

/** Connect to the Moon WebSocket endpoint with auto-reconnect */
export function connectWS(): void {
  if (ws) return;

  ws = new WebSocket(getWsUrl());

  ws.onopen = () => {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  };

  ws.onmessage = (ev: MessageEvent) => {
    try {
      const msg = JSON.parse(ev.data as string) as {
        id?: number;
        result?: unknown;
        error?: string;
        elapsed_ms?: number;
      };

      // Resolve pending request if this is a response
      if (msg.id !== undefined) {
        const entry = pending.get(msg.id);
        if (entry) {
          pending.delete(msg.id);
          entry.resolve({
            data: msg.result ?? null,
            raw: JSON.stringify(msg.result ?? msg.error ?? null, null, 2),
            elapsed_ms: msg.elapsed_ms ?? 0,
            error: msg.error,
          });
          return;
        }
      }

      // Broadcast to listeners for non-request messages
      for (const cb of listeners) {
        cb(msg);
      }
    } catch {
      // Ignore malformed messages
    }
  };

  ws.onerror = () => {
    // Will trigger onclose
  };

  ws.onclose = () => {
    ws = null;
    // Reject all pending requests
    for (const [, entry] of pending) {
      entry.reject(new Error("WebSocket closed"));
    }
    pending.clear();
    // Auto-reconnect
    if (!reconnectTimer) {
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectWS();
      }, RECONNECT_DELAY_MS);
    }
  };
}

/**
 * Send a raw command string. Uses WebSocket if connected, falls back to REST.
 * The raw string is split: first word = command, rest = args.
 */
export async function sendCommand(raw: string): Promise<QueryResult> {
  const trimmed = raw.trim();
  if (!trimmed) {
    return { data: null, raw: "", elapsed_ms: 0, error: "Empty command" };
  }

  // If WS is connected, send over WebSocket
  if (ws && ws.readyState === WebSocket.OPEN) {
    const id = ++requestId;
    return new Promise<QueryResult>((resolve, reject) => {
      pending.set(id, { resolve, reject });
      ws!.send(JSON.stringify({ id, command: trimmed }));
      // Timeout after 30s
      setTimeout(() => {
        if (pending.has(id)) {
          pending.delete(id);
          reject(new Error("Request timed out"));
        }
      }, 30000);
    });
  }

  // Fallback to REST API
  const parts = trimmed.split(/\s+/);
  const cmd = parts[0];
  const args = parts.slice(1);
  const start = performance.now();
  try {
    const data = await execCommand(cmd, args);
    const elapsed_ms = performance.now() - start;
    return {
      data,
      raw: JSON.stringify(data, null, 2),
      elapsed_ms,
    };
  } catch (err) {
    const elapsed_ms = performance.now() - start;
    return {
      data: null,
      raw: "",
      elapsed_ms,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

/** Register a listener for non-request WebSocket messages. Returns unsubscribe function. */
export function onMessage(cb: (data: unknown) => void): () => void {
  listeners.add(cb);
  return () => listeners.delete(cb);
}

/** Whether the WebSocket is currently connected */
export function isWsConnected(): boolean {
  return ws !== null && ws.readyState === WebSocket.OPEN;
}

/** Disconnect WebSocket and stop reconnection attempts */
export function disconnectWS(): void {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  if (ws) {
    ws.close();
    ws = null;
  }
}
