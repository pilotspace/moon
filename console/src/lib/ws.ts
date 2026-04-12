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
 * Parse a raw command line into cmd + args.
 * Splits on whitespace, but respects double-quoted strings so values with
 * spaces can be sent (e.g. `SET k "hello world"`).
 */
function parseCommandLine(raw: string): { cmd: string; args: string[] } {
  const tokens: string[] = [];
  let buf = "";
  let inQuote = false;
  for (let i = 0; i < raw.length; i++) {
    const c = raw[i];
    if (inQuote) {
      if (c === "\"") {
        inQuote = false;
      } else if (c === "\\" && i + 1 < raw.length) {
        buf += raw[++i];
      } else {
        buf += c;
      }
      continue;
    }
    if (c === "\"") {
      inQuote = true;
      continue;
    }
    if (/\s/.test(c)) {
      if (buf.length) {
        tokens.push(buf);
        buf = "";
      }
      continue;
    }
    buf += c;
  }
  if (buf.length) tokens.push(buf);
  const [cmd = "", ...args] = tokens;
  return { cmd, args };
}

/**
 * Send a raw command string. If the input contains multiple non-empty,
 * non-comment lines (comment = `#` or `//`), each line is sent as a separate
 * command and the collected results are returned. This matches redis-cli
 * multi-line paste behavior.
 *
 * If you need to execute a single multi-line query (e.g. a Cypher statement),
 * use sendSingleCommand() instead.
 */
export async function sendCommand(raw: string): Promise<QueryResult> {
  const trimmed = raw.trim();
  if (!trimmed) {
    return { data: null, raw: "", elapsed_ms: 0, error: "Empty command" };
  }

  const lines = trimmed
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0 && !l.startsWith("#") && !l.startsWith("//"));

  if (lines.length === 0) {
    return { data: null, raw: "", elapsed_ms: 0, error: "Empty command" };
  }

  if (lines.length === 1) {
    return sendSingleCommand(lines[0]);
  }

  // Multi-line: execute sequentially, collect results
  const start = performance.now();
  const results: Array<{ line: string; result: QueryResult }> = [];
  let firstError: string | undefined;
  for (const line of lines) {
    const r = await sendSingleCommand(line);
    results.push({ line, result: r });
    if (r.error && !firstError) firstError = `${line} → ${r.error}`;
  }
  const elapsed_ms = performance.now() - start;

  // Collate into one displayable result
  const collated = results.map(({ line, result }) => ({
    command: line,
    result: result.error ? `ERROR: ${result.error}` : result.data,
    elapsed_ms: Math.round(result.elapsed_ms * 100) / 100,
  }));

  return {
    data: collated,
    raw: collated
      .map((r) => `> ${r.command}\n${JSON.stringify(r.result, null, 2)}`)
      .join("\n\n"),
    elapsed_ms,
    error: firstError,
  };
}

/**
 * Send a single command. Use this for Cypher or any query that may legally
 * span multiple lines.
 */
export async function sendSingleCommand(raw: string): Promise<QueryResult> {
  const trimmed = raw.trim();
  if (!trimmed) {
    return { data: null, raw: "", elapsed_ms: 0, error: "Empty command" };
  }

  const { cmd, args } = parseCommandLine(trimmed);
  if (!cmd) {
    return { data: null, raw: "", elapsed_ms: 0, error: "Empty command" };
  }

  // If WS is connected, send over WebSocket
  if (ws && ws.readyState === WebSocket.OPEN) {
    const id = ++requestId;
    const start = performance.now();
    return new Promise<QueryResult>((resolve, reject) => {
      pending.set(id, {
        resolve: (r) => resolve({ ...r, elapsed_ms: r.elapsed_ms || performance.now() - start }),
        reject,
      });
      // Protocol: { id, cmd, args } — matches src/admin/ws_bridge.rs expectations
      ws!.send(JSON.stringify({ id, cmd, args }));
      setTimeout(() => {
        if (pending.has(id)) {
          pending.delete(id);
          reject(new Error("Request timed out"));
        }
      }, 30000);
    });
  }

  // Fallback to REST API
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
