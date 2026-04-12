import { useState, useMemo } from "react";
import { useMetricsStore } from "@/stores/metrics";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

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

export function SlowlogTable() {
  const slowlog = useMetricsStore((s) => s.slowlog);
  const [sortField, setSortField] = useState<SortField>("duration_us");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

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
        <CardTitle>Slowlog</CardTitle>
      </CardHeader>
      <CardContent>
        {slowlog.length === 0 ? (
          <div className="text-sm text-muted-foreground py-4 text-center">
            No slowlog entries. Configure slowlog-log-slower-than to capture slow commands.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border">
                  {headers.map(({ field, label, className }) => (
                    <th
                      key={field}
                      onClick={() => toggleSort(field)}
                      className={cn(
                        "py-2 px-3 text-left text-xs font-medium text-muted-foreground cursor-pointer hover:text-zinc-300 select-none",
                        className
                      )}
                    >
                      {label}
                      {sortField === field && (
                        <span className="ml-1">{sortDir === "asc" ? "\u2191" : "\u2193"}</span>
                      )}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sorted.map((entry) => (
                  <tr key={entry.id} className="border-b border-border/50 hover:bg-accent/30">
                    <td className="py-2 px-3 font-mono text-muted-foreground">{entry.id}</td>
                    <td className="py-2 px-3 text-muted-foreground">{formatTimestamp(entry.timestamp)}</td>
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
