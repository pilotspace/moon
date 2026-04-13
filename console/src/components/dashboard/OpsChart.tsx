import { useMetricsStore } from "@/stores/metrics";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

function formatTime(ts: number): string {
  const d = new Date(ts);
  return `${d.getMinutes().toString().padStart(2, "0")}:${d.getSeconds().toString().padStart(2, "0")}`;
}

function formatOps(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export function OpsChart() {
  const history = useMetricsStore((s) => s.history);
  const latest = useMetricsStore((s) => s.latest);

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle>Operations / sec</CardTitle>
        <span className="text-2xl font-bold tabular-nums text-zinc-100">
          {latest ? formatOps(latest.ops_per_sec) : "\u2014"}
        </span>
      </CardHeader>
      <CardContent>
        <div className="h-48">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={history} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="opsGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="oklch(0.72 0.12 250)" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="oklch(0.72 0.12 250)" stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis
                dataKey="time"
                tickFormatter={formatTime}
                tick={{ fill: "#a1a1aa", fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                minTickGap={40}
              />
              <YAxis
                tickFormatter={formatOps}
                tick={{ fill: "#a1a1aa", fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                width={50}
              />
              <Tooltip
                contentStyle={{ backgroundColor: "#1E1E28", border: "1px solid #393944", borderRadius: 8 }}
                labelFormatter={formatTime}
                formatter={(v: number) => [formatOps(v), "ops/s"]}
              />
              <Area
                type="monotone"
                dataKey="ops_per_sec"
                stroke="oklch(0.72 0.12 250)"
                fill="url(#opsGrad)"
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
