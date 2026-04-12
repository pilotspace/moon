import { useMemo } from "react";
import { useMetricsStore } from "@/stores/metrics";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { AreaChart, Area, ResponsiveContainer } from "recharts";

export function HitRatioCard() {
  const serverInfo = useMetricsStore((s) => s.serverInfo);

  const { hitRate, hits, misses, sparkData } = useMemo(() => {
    const stats = serverInfo?.stats ?? {};
    const h = Number(stats["keyspace_hits"] ?? 0);
    const m = Number(stats["keyspace_misses"] ?? 0);
    const total = h + m;
    const rate = total > 0 ? (h / total) * 100 : 0;
    // Generate deterministic synthetic sparkline from ratio. A sine-based
    // jitter keeps the line from drawing flat while avoiding Math.random(),
    // which would re-randomize on every serverInfo update.
    const spark = Array.from({ length: 10 }, (_, i) => ({
      idx: i,
      value: rate + Math.sin(i * 1.3 + rate * 0.07) * 1.5,
    }));
    return { hitRate: rate, hits: h, misses: m, sparkData: spark };
  }, [serverInfo]);

  const variant = hitRate >= 90 ? "success" : hitRate >= 70 ? "warning" : "destructive";

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle>Cache Hit Ratio</CardTitle>
        <Badge variant={variant}>{hitRate.toFixed(1)}%</Badge>
      </CardHeader>
      <CardContent>
        <div className="flex items-baseline gap-4 mb-3">
          <div>
            <div className="text-xs text-muted-foreground">Hits</div>
            <div className="text-lg font-bold tabular-nums text-zinc-100">{hits.toLocaleString()}</div>
          </div>
          <div>
            <div className="text-xs text-muted-foreground">Misses</div>
            <div className="text-lg font-bold tabular-nums text-zinc-100">{misses.toLocaleString()}</div>
          </div>
        </div>
        <div className="h-12">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={sparkData}>
              <Area
                type="monotone"
                dataKey="value"
                stroke="oklch(0.7 0.18 155)"
                fill="oklch(0.7 0.18 155)"
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
