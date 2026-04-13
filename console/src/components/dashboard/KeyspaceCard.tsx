import { useMemo } from "react";
import { useMetricsStore } from "@/stores/metrics";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";

const TYPE_COLORS: string[] = [
  "#6B93D4",   // blue
  "#5BBDAD",   // teal
  "#C4A55A",   // amber
  "#A88BD4",   // violet
  "#D4807A",   // rose
  "#62B87A",   // green
];

export function KeyspaceCard() {
  const serverInfo = useMetricsStore((s) => s.serverInfo);
  const latest = useMetricsStore((s) => s.latest);

  const data = useMemo(() => {
    const keyspace = serverInfo?.keyspace ?? {};
    // Parse keyspace info: "db0": "keys=100,expires=10,avg_ttl=1000"
    // Moon may also return plain numeric values or "keys=N" without other fields
    const entries = Object.entries(keyspace)
      .filter(([db]) => db.startsWith("db"))
      .map(([db, info]) => {
        // Try "keys=N" format first
        const match = info.match(/keys=(\d+)/);
        if (match) return { name: db, keys: Number(match[1]) };
        // Fallback: value might be a plain number
        const num = Number(info);
        if (!isNaN(num) && num > 0) return { name: db, keys: num };
        return { name: db, keys: 0 };
      })
      .filter((e) => e.keys > 0);
    if (entries.length === 0 && latest) {
      return [{ name: "db0", keys: latest.total_keys }];
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
                  tick={{ fill: "#a1a1aa", fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                />
                <YAxis
                  tick={{ fill: "#a1a1aa", fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                  width={40}
                />
                <Tooltip
                  contentStyle={{ backgroundColor: "#1E1E28", border: "1px solid #393944", borderRadius: 8 }}
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
