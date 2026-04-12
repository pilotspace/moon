import { useState, useEffect } from "react";
import { execCommand } from "@/lib/api";
import { ChevronDown, ChevronRight, Users } from "lucide-react";

interface StreamEntry {
  id: string;
  fields: Record<string, string>;
}

interface ConsumerGroup {
  name: string;
  consumers: number;
  pending: number;
  lastDeliveredId: string;
}

function parseStreamEntries(raw: unknown): StreamEntry[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((entry) => {
    if (!Array.isArray(entry) || entry.length < 2) return { id: String(entry), fields: {} };
    const id = String(entry[0]);
    const fieldArr = entry[1];
    const fields: Record<string, string> = {};
    if (Array.isArray(fieldArr)) {
      for (let i = 0; i < fieldArr.length; i += 2) {
        fields[String(fieldArr[i])] = String(fieldArr[i + 1] ?? "");
      }
    }
    return { id, fields };
  });
}

export function StreamViewer({ keyName }: { keyName: string; value: unknown }) {
  const [entries, setEntries] = useState<StreamEntry[]>([]);
  const [groups, setGroups] = useState<ConsumerGroup[]>([]);
  const [expandedId, setExpandedId] = useState<string | null>(null);

  useEffect(() => {
    // Load stream entries via XRANGE
    execCommand("XRANGE", [keyName, "-", "+", "COUNT", "100"])
      .then((result) => setEntries(parseStreamEntries(result)))
      .catch(() => {});

    // Load consumer groups
    execCommand("XINFO", ["GROUPS", keyName])
      .then((result) => {
        if (Array.isArray(result)) {
          const parsed = result.map((g) => {
            if (Array.isArray(g)) {
              const map: Record<string, unknown> = {};
              for (let i = 0; i < g.length; i += 2) map[String(g[i])] = g[i + 1];
              return {
                name: String(map["name"] ?? ""),
                consumers: Number(map["consumers"] ?? 0),
                pending: Number(map["pel-count"] ?? 0),
                lastDeliveredId: String(map["last-delivered-id"] ?? ""),
              };
            }
            return null;
          }).filter(Boolean) as ConsumerGroup[];
          setGroups(parsed);
        }
      })
      .catch(() => {});
  }, [keyName]);

  return (
    <div className="flex flex-col h-full">
      {/* Consumer groups */}
      {groups.length > 0 && (
        <div className="px-3 py-2 border-b border-border">
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground mb-1">
            <Users className="h-3 w-3" />
            Consumer Groups
          </div>
          <div className="flex flex-wrap gap-1.5">
            {groups.map((g) => (
              <span
                key={g.name}
                className="inline-flex items-center gap-1 rounded border border-border bg-zinc-900 px-2 py-0.5 text-[10px] font-mono text-zinc-300"
              >
                {g.name}
                <span className="text-muted-foreground">({g.consumers}c / {g.pending}p)</span>
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Timeline */}
      <div className="flex-1 overflow-auto">
        {entries.map((entry) => {
          const expanded = expandedId === entry.id;
          return (
            <div key={entry.id} className="border-b border-border/50">
              <button
                className="flex items-center gap-2 w-full px-3 py-1.5 text-left hover:bg-accent/30 transition-colors"
                onClick={() => setExpandedId(expanded ? null : entry.id)}
              >
                {expanded ? (
                  <ChevronDown className="h-3 w-3 text-muted-foreground shrink-0" />
                ) : (
                  <ChevronRight className="h-3 w-3 text-muted-foreground shrink-0" />
                )}
                <span className="text-xs font-mono text-cyan-400">{entry.id}</span>
                <span className="text-[10px] text-muted-foreground">
                  {Object.keys(entry.fields).length} fields
                </span>
              </button>
              {expanded && (
                <div className="px-8 pb-2 space-y-0.5">
                  {Object.entries(entry.fields).map(([k, v]) => (
                    <div key={k} className="flex gap-2 text-xs">
                      <span className="font-mono text-zinc-400 shrink-0">{k}:</span>
                      <span className="font-mono text-zinc-300 break-all">{v}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </div>

      <div className="px-3 py-1 border-t border-border text-[10px] text-muted-foreground">
        {entries.length} entries | {groups.length} consumer groups
      </div>
    </div>
  );
}
