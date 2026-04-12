import { useState, useMemo, useCallback } from "react";
import { Loader2 } from "lucide-react";
import type { QueryResult } from "@/types/console";

type ViewMode = "table" | "json" | "raw";

interface ResultPanelProps {
  result: QueryResult | null;
  executing: boolean;
}

function detectView(data: unknown): ViewMode {
  if (data === null || data === undefined) return "raw";
  if (typeof data === "string" || typeof data === "number" || typeof data === "boolean") return "raw";
  if (Array.isArray(data)) {
    if (
      data.length > 0 &&
      data.every((item) => typeof item === "object" && item !== null && !Array.isArray(item))
    ) {
      return "table";
    }
    if (data.every((item) => typeof item !== "object")) return "raw";
    return "json";
  }
  if (typeof data === "object") return "json";
  return "raw";
}

function formatElapsed(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function truncate(value: string, max: number): string {
  if (value.length <= max) return value;
  return value.slice(0, max) + "\u2026";
}

function cellToString(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

// ---- Table View ----

function TableView({ data }: { data: Record<string, unknown>[] }) {
  const columns = useMemo(() => {
    const keys = new Set<string>();
    for (const row of data) {
      for (const k of Object.keys(row)) keys.add(k);
    }
    return [...keys];
  }, [data]);

  return (
    <div className="overflow-auto">
      <table className="w-full text-sm font-mono border-collapse">
        <thead>
          <tr className="bg-zinc-900 sticky top-0">
            <th className="px-3 py-1.5 text-left text-zinc-400 font-medium border-b border-zinc-800 w-10">
              #
            </th>
            {columns.map((col) => (
              <th
                key={col}
                className="px-3 py-1.5 text-left text-zinc-400 font-medium border-b border-zinc-800"
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={i} className="hover:bg-zinc-800/50 border-b border-zinc-800/50">
              <td className="px-3 py-1 text-zinc-600">{i + 1}</td>
              {columns.map((col) => (
                <td key={col} className="px-3 py-1 text-zinc-300">
                  {truncate(cellToString(row[col]), 120)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      <div className="px-3 py-1 text-xs text-zinc-500">
        {data.length} row{data.length !== 1 ? "s" : ""}
      </div>
    </div>
  );
}

// ---- JSON View ----

function JsonNode({
  keyName,
  value,
  path,
  expanded,
  onToggle,
  depth,
}: {
  keyName?: string;
  value: unknown;
  path: string;
  expanded: Set<string>;
  onToggle: (path: string) => void;
  depth: number;
}) {
  if (depth > 10) {
    return (
      <div style={{ paddingLeft: depth * 16 }} className="text-zinc-500 text-sm font-mono">
        {keyName && <span className="text-blue-400">{keyName}: </span>}
        <span>...</span>
      </div>
    );
  }

  const isObject = typeof value === "object" && value !== null;
  const isArray = Array.isArray(value);
  const isExpanded = expanded.has(path);

  if (!isObject) {
    let colorClass = "text-zinc-300";
    let display = String(value);
    if (typeof value === "string") {
      colorClass = "text-green-400";
      display = `"${value}"`;
    } else if (typeof value === "number") {
      colorClass = "text-yellow-400";
    } else if (typeof value === "boolean") {
      colorClass = "text-red-400";
    } else if (value === null) {
      colorClass = "text-zinc-500";
      display = "null";
    }

    return (
      <div style={{ paddingLeft: depth * 16 }} className="text-sm font-mono py-px">
        {keyName !== undefined && <span className="text-blue-400">{keyName}: </span>}
        <span className={colorClass}>{display}</span>
      </div>
    );
  }

  const entries = isArray
    ? (value as unknown[]).map((v, i) => [String(i), v] as const)
    : Object.entries(value as Record<string, unknown>);

  const bracket = isArray ? ["[", "]"] : ["{", "}"];
  const preview = isArray ? `Array(${entries.length})` : `Object(${entries.length})`;

  return (
    <div>
      <div
        style={{ paddingLeft: depth * 16 }}
        className="text-sm font-mono py-px cursor-pointer hover:bg-zinc-800/50 select-none"
        onClick={() => onToggle(path)}
      >
        <span className="text-zinc-500 mr-1">{isExpanded ? "\u25BE" : "\u25B8"}</span>
        {keyName !== undefined && <span className="text-blue-400">{keyName}: </span>}
        {isExpanded ? (
          <span className="text-zinc-500">{bracket[0]}</span>
        ) : (
          <span className="text-zinc-500">
            {bracket[0]} {preview} {bracket[1]}
          </span>
        )}
      </div>
      {isExpanded && (
        <>
          {entries.map(([k, v]) => (
            <JsonNode
              key={k}
              keyName={isArray ? undefined : k}
              value={v}
              path={`${path}.${k}`}
              expanded={expanded}
              onToggle={onToggle}
              depth={depth + 1}
            />
          ))}
          <div style={{ paddingLeft: depth * 16 }} className="text-sm font-mono text-zinc-500">
            {bracket[1]}
          </div>
        </>
      )}
    </div>
  );
}

function JsonView({ data }: { data: unknown }) {
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set(["$"]));

  const onToggle = useCallback((path: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(path)) {
        next.delete(path);
      } else {
        next.add(path);
      }
      return next;
    });
  }, []);

  return (
    <div className="p-2 overflow-auto">
      <JsonNode keyName={undefined} value={data} path="$" expanded={expanded} onToggle={onToggle} depth={0} />
    </div>
  );
}

// ---- Raw View ----

function RawView({ data }: { data: unknown }) {
  let text: string;
  if (Array.isArray(data)) {
    text = data.map((item) => String(item)).join("\n");
  } else {
    text = String(data);
  }

  return <pre className="font-mono text-sm text-zinc-300 whitespace-pre-wrap p-4">{text}</pre>;
}

// ---- Main Component ----

const VIEW_LABELS: ViewMode[] = ["table", "json", "raw"];

export function ResultPanel({ result, executing }: ResultPanelProps) {
  const autoView = useMemo(() => (result ? detectView(result.data) : "raw"), [result]);
  const [override, setOverride] = useState<ViewMode | null>(null);

  // Reset override when result changes
  const currentView = override ?? autoView;

  if (executing) {
    return (
      <div className="flex items-center justify-center gap-2 p-8 text-zinc-400">
        <Loader2 size={16} className="animate-spin" />
        <span>Executing...</span>
      </div>
    );
  }

  if (!result) {
    return (
      <div className="flex items-center justify-center p-8 text-zinc-500 text-sm">
        Press Cmd+Enter to execute a query
      </div>
    );
  }

  if (result.error) {
    return (
      <div className="p-3">
        <div className="text-red-400 bg-red-950/30 border border-red-900 rounded p-3 font-mono text-sm">
          {result.error}
        </div>
        <div className="mt-2 text-xs text-zinc-500">{formatElapsed(result.elapsed_ms)}</div>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex-1 overflow-auto">
        {currentView === "table" && Array.isArray(result.data) ? (
          <TableView data={result.data as Record<string, unknown>[]} />
        ) : currentView === "json" ? (
          <JsonView data={result.data} />
        ) : (
          <RawView data={result.data} />
        )}
      </div>
      <div className="flex items-center justify-between px-3 py-1.5 border-t border-zinc-800 bg-zinc-900/50 text-xs text-zinc-500">
        <span>{formatElapsed(result.elapsed_ms)}</span>
        <div className="flex items-center gap-0.5">
          {VIEW_LABELS.map((mode) => (
            <button
              key={mode}
              onClick={() => setOverride(mode === autoView ? null : mode)}
              className={`px-2 py-0.5 rounded text-xs transition-colors ${
                currentView === mode
                  ? "bg-zinc-700 text-zinc-200"
                  : "text-zinc-500 hover:text-zinc-300"
              }`}
            >
              {mode.charAt(0).toUpperCase() + mode.slice(1)}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
