import { useMemo, useState } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import { useMemoryStore } from "@/stores/memory";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { LatencyBucket } from "@/types/memory";

type SortField = "id" | "timestamp" | "duration_us" | "command";
type SortDir = "asc" | "desc";

function formatDuration(us: number): string {
  if (us >= 1_000_000) return `${(us / 1_000_000).toFixed(2)}s`;
  if (us >= 1_000) return `${(us / 1_000).toFixed(1)}ms`;
  return `${us}us`;
}

function formatTimestamp(ts: number): string {
  return new Date(ts * 1000).toLocaleTimeString();
}

function durationVariant(us: number) {
  if (us >= 100_000) return "destructive" as const;
  if (us >= 10_000) return "warning" as const;
  return "secondary" as const;
}

const BUCKET_COLORS = ["#22c55e", "#84cc16", "#eab308", "#f97316", "#ef4444"];

function computeLatencyBuckets(entries: { duration_us: number }[]): LatencyBucket[] {
  const buckets: LatencyBucket[] = [
    { range: "<1ms", count: 0 },
    { range: "1-10ms", count: 0 },
    { range: "10-100ms", count: 0 },
    { range: "100ms-1s", count: 0 },
    { range: ">1s", count: 0 },
  ];
  for (const entry of entries) {
    const us = entry.duration_us;
    if (us < 1_000) buckets[0].count++;
    else if (us < 10_000) buckets[1].count++;
    else if (us < 100_000) buckets[2].count++;
    else if (us < 1_000_000) buckets[3].count++;
    else buckets[4].count++;
  }
  return buckets;
}

export function SlowlogPanel() {
  const slowlog = useMemoryStore((s) => s.slowlog);
  const [sortField, setSortField] = useState<SortField>("duration_us");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const buckets = useMemo(() => computeLatencyBuckets(slowlog), [slowlog]);

  const sorted = useMemo(() => {
    const items = [...slowlog];
    items.sort((a, b) => {
      const av = a[sortField];
      const bv = b[sortField];
      if (typeof av === "number" && typeof bv === "number") {
        return sortDir === "asc" ? av - bv : bv - av;
      }
      return sortDir === "asc"
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
    return items;
  }, [slowlog, sortField, sortDir]);

  const toggleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDir("desc");
    }
  };

  const headers: { field: SortField; label: string; className?: string }[] = [
    { field: "id", label: "ID", className: "w-16" },
    { field: "timestamp", label: "Time", className: "w-24" },
    { field: "duration_us", label: "Duration", className: "w-24" },
    { field: "command", label: "Command" },
  ];

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle>Slowlog</CardTitle>
          {slowlog.length > 0 && (
            <Badge variant="secondary">{slowlog.length} entries</Badge>
          )}
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Latency Histogram */}
        {slowlog.length > 0 && (
          <div>
            <h4 className="text-xs font-medium text-muted-foreground mb-2">
              Latency Distribution
            </h4>
            <ResponsiveContainer width="100%" height={160}>
              <BarChart data={buckets} margin={{ top: 4, right: 4, bottom: 4, left: 4 }}>
                <XAxis
                  dataKey="range"
                  tick={{ fontSize: 11, fill: "#a1a1aa" }}
                  axisLine={false}
                  tickLine={false}
                />
                <YAxis
                  tick={{ fontSize: 11, fill: "#a1a1aa" }}
                  axisLine={false}
                  tickLine={false}
                  allowDecimals={false}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: "#27272a",
                    border: "1px solid #3f3f46",
                    borderRadius: "6px",
                    fontSize: "12px",
                  }}
                />
                <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                  {buckets.map((_, i) => (
                    <Cell key={i} fill={BUCKET_COLORS[i]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Sortable Table */}
        {slowlog.length === 0 ? (
          <div className="text-sm text-muted-foreground py-4 text-center">
            No slowlog entries. Configure slowlog-log-slower-than to capture slow commands.
          </div>
        ) : (
          <div className="overflow-x-auto max-h-80 overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-zinc-900">
                <tr className="border-b border-border">
                  {headers.map(({ field, label, className }) => (
                    <th
                      key={field}
                      onClick={() => toggleSort(field)}
                      className={cn(
                        "py-2 px-3 text-left text-xs font-medium text-muted-foreground cursor-pointer hover:text-zinc-300 select-none",
                        className,
                      )}
                    >
                      {label}
                      {sortField === field && (
                        <span className="ml-1">
                          {sortDir === "asc" ? "\u2191" : "\u2193"}
                        </span>
                      )}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sorted.map((entry) => (
                  <tr
                    key={entry.id}
                    className="border-b border-border/50 hover:bg-accent/30"
                  >
                    <td className="py-2 px-3 font-mono text-muted-foreground">
                      {entry.id}
                    </td>
                    <td className="py-2 px-3 text-muted-foreground">
                      {formatTimestamp(entry.timestamp)}
                    </td>
                    <td className="py-2 px-3">
                      <Badge variant={durationVariant(entry.duration_us)}>
                        {formatDuration(entry.duration_us)}
                      </Badge>
                    </td>
                    <td className="py-2 px-3 font-mono">
                      <span className="text-primary">{entry.command}</span>
                      {entry.args.length > 0 && (
                        <span className="text-muted-foreground ml-2">
                          {entry.args.slice(0, 3).join(" ")}
                          {entry.args.length > 3 && ` (+${entry.args.length - 3})`}
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
