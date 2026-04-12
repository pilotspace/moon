import { useMemo, useState } from "react";
import { useMemoryStore } from "@/stores/memory";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { CommandStat } from "@/types/memory";

type SortField = "command" | "calls" | "usec_per_call" | "usec" | "rejected_calls" | "failed_calls";
type SortDir = "asc" | "desc";

function formatUsec(us: number): string {
  if (us >= 1_000_000) return `${(us / 1_000_000).toFixed(2)}s`;
  if (us >= 1_000) return `${(us / 1_000).toFixed(1)}ms`;
  return `${us.toFixed(1)}us`;
}

function usecVariant(us: number) {
  if (us >= 1_000_000) return "destructive" as const;
  if (us >= 10_000) return "warning" as const;
  return "secondary" as const;
}

export function CommandStatsTable() {
  const commandStats = useMemoryStore((s) => s.commandStats);
  const [sortField, setSortField] = useState<SortField>("usec");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const hasRejected = useMemo(
    () => commandStats.some((s) => s.rejected_calls > 0),
    [commandStats],
  );
  const hasFailed = useMemo(
    () => commandStats.some((s) => s.failed_calls > 0),
    [commandStats],
  );

  const sorted = useMemo(() => {
    const items = [...commandStats];
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
  }, [commandStats, sortField, sortDir]);

  // Track top 3 by usec for highlighting
  const top3Usec = useMemo(() => {
    const byUsec = [...commandStats].sort((a, b) => b.usec - a.usec);
    return new Set(byUsec.slice(0, 3).map((s) => s.command));
  }, [commandStats]);

  const toggleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDir("desc");
    }
  };

  const baseHeaders: { field: SortField; label: string; className?: string }[] = [
    { field: "command", label: "Command", className: "w-32" },
    { field: "calls", label: "Calls", className: "w-28" },
    { field: "usec_per_call", label: "Avg Latency", className: "w-28" },
    { field: "usec", label: "Total CPU", className: "w-28" },
  ];

  const headers = [
    ...baseHeaders,
    ...(hasRejected ? [{ field: "rejected_calls" as SortField, label: "Rejected", className: "w-24" }] : []),
    ...(hasFailed ? [{ field: "failed_calls" as SortField, label: "Failed", className: "w-24" }] : []),
  ];

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle>Command Statistics</CardTitle>
          {commandStats.length > 0 && (
            <Badge variant="secondary">{commandStats.length} commands</Badge>
          )}
        </div>
      </CardHeader>
      <CardContent>
        {commandStats.length === 0 ? (
          <div className="text-sm text-muted-foreground py-4 text-center">
            No command statistics available.
          </div>
        ) : (
          <div className="overflow-x-auto max-h-96 overflow-y-auto">
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
                {sorted.map((stat: CommandStat) => (
                  <tr
                    key={stat.command}
                    className={cn(
                      "border-b border-border/50 hover:bg-accent/30",
                      top3Usec.has(stat.command) && "bg-amber-500/5",
                    )}
                  >
                    <td className="py-2 px-3 font-mono text-primary uppercase">
                      {stat.command}
                    </td>
                    <td className="py-2 px-3 text-zinc-300">
                      {stat.calls.toLocaleString()}
                    </td>
                    <td className="py-2 px-3">
                      <Badge variant={usecVariant(stat.usec_per_call)}>
                        {formatUsec(stat.usec_per_call)}
                      </Badge>
                    </td>
                    <td className="py-2 px-3">
                      <Badge variant={usecVariant(stat.usec)}>
                        {formatUsec(stat.usec)}
                      </Badge>
                    </td>
                    {hasRejected && (
                      <td className="py-2 px-3 text-muted-foreground">
                        {stat.rejected_calls > 0 ? stat.rejected_calls.toLocaleString() : "-"}
                      </td>
                    )}
                    {hasFailed && (
                      <td className="py-2 px-3 text-muted-foreground">
                        {stat.failed_calls > 0 ? (
                          <span className="text-destructive">
                            {stat.failed_calls.toLocaleString()}
                          </span>
                        ) : (
                          "-"
                        )}
                      </td>
                    )}
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
