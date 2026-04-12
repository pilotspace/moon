import { useMetricsStore } from "@/stores/metrics";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { AreaChart, Area, ResponsiveContainer } from "recharts";

export function ClientsCard() {
  const history = useMetricsStore((s) => s.history);
  const latest = useMetricsStore((s) => s.latest);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Connected Clients</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-3xl font-bold tabular-nums text-zinc-100">
          {latest?.connected_clients ?? "\u2014"}
        </div>
        <div className="h-16 mt-3">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={history.slice(-30)}>
              <Area
                type="monotone"
                dataKey="connected_clients"
                stroke="oklch(0.75 0.15 75)"
                fill="oklch(0.75 0.15 75)"
                fillOpacity={0.1}
                strokeWidth={1.5}
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
