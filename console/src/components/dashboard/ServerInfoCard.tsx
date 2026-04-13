import { useMetricsStore } from "@/stores/metrics";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";

function formatUptime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (h < 24) return `${h}h ${m}m`;
  const d = Math.floor(h / 24);
  return `${d}d ${h % 24}h`;
}

export function ServerInfoCard() {
  const serverInfo = useMetricsStore((s) => s.serverInfo);
  const latest = useMetricsStore((s) => s.latest);

  const server = serverInfo?.server ?? {};
  const version = server.moon_version || server.redis_version || "\u2014";
  const uptime = latest?.uptime_seconds
    ? formatUptime(latest.uptime_seconds)
    : server.uptime_in_seconds
      ? formatUptime(Number(server.uptime_in_seconds))
      : "\u2014";
  const shards = server.moon_shards || server.shard_count || "\u2014";
  const runtime = server.moon_runtime || server.runtime || "\u2014";

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle>Server</CardTitle>
        <span className="text-sm font-mono text-zinc-400">{version}</span>
      </CardHeader>
      <CardContent>
        <dl className="space-y-1.5">
          <div className="flex justify-between text-sm">
            <dt className="text-muted-foreground">Uptime</dt>
            <dd className="font-mono text-zinc-200">{uptime}</dd>
          </div>
          <div className="flex justify-between text-sm">
            <dt className="text-muted-foreground">Shards</dt>
            <dd className="font-mono text-zinc-200">{shards}</dd>
          </div>
          <div className="flex justify-between text-sm">
            <dt className="text-muted-foreground">Runtime</dt>
            <dd className="font-mono text-zinc-200">{runtime}</dd>
          </div>
          <div className="flex justify-between text-sm">
            <dt className="text-muted-foreground">Port</dt>
            <dd className="font-mono text-zinc-200">{server.tcp_port || "\u2014"}</dd>
          </div>
          <div className="flex justify-between text-sm">
            <dt className="text-muted-foreground">PID</dt>
            <dd className="font-mono text-zinc-200">{server.process_id || "\u2014"}</dd>
          </div>
        </dl>
      </CardContent>
    </Card>
  );
}
