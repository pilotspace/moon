import { useMemo } from "react";
import { useMetricsStore } from "@/stores/metrics";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";

const TYPE_COLORS: string[] = [
  "oklch(0.7 0.15 250)",   // blue
  "oklch(0.7 0.18 155)",   // green
  "oklch(0.75 0.15 75)",   // amber
  "oklch(0.7 0.15 320)",   // purple
  "oklch(0.6 0.2 25)",     // red
  "oklch(0.7 0.12 200)",   // teal
];

export function KeyspaceCard() {
  const serverInfo = useMetricsStore((s) => s.serverInfo);
  const latest = useMetricsStore((s) => s.latest);

  const data = useMemo(() => {
    const keyspace = serverInfo?.keyspace ?? {};
    // Parse keyspace info: db0: keys=100,expires=10,avg_ttl=1000
    const entries = Object.entries(keyspace).map(([db, info]) => {
      const keys = Number(info.match(/keys=(\d+)/)?.[1] ?? 0);
      return { name: db, keys };
    });
    if (entries.length === 0 && latest) {
      return [{ name: "total", keys: latest.total_keys }];
    }
    return entries;
  }, [serverInfo, latest]);

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle>Keyspace</CardTitle>
        <span className="text-2xl font-bold tabular-nums text-zinc-100">
          {latest?.total_keys?.toLocaleString() ?? "\u2014"}
        </span>
      </CardHeader>
      <CardContent>
        {data.length > 0 ? (
          <div className="h-32">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data} margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
                <XAxis
                  dataKey="name"
                  tick={{ fill: "#71717a", fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                />
                <YAxis
                  tick={{ fill: "#71717a", fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                  width={40}
                />
                <Tooltip
                  contentStyle={{ backgroundColor: "#18181b", border: "1px solid #3f3f46", borderRadius: 8 }}
                />
                <Bar dataKey="keys" radius={[4, 4, 0, 0]}>
                  {data.map((entry, i) => (
                    <Cell key={entry.name} fill={TYPE_COLORS[i % TYPE_COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="text-sm text-muted-foreground">No keyspace data</div>
        )}
      </CardContent>
    </Card>
  );
}
