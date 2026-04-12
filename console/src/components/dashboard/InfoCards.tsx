import { useMetricsStore } from "@/stores/metrics";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";

const INFO_SECTIONS = [
  { key: "server", label: "Server", fields: ["redis_version", "moon_version", "process_id", "uptime_in_seconds", "tcp_port"] },
  { key: "memory", label: "Memory", fields: ["used_memory_human", "used_memory_rss_human", "used_memory_peak_human", "mem_fragmentation_ratio"] },
  { key: "clients", label: "Clients", fields: ["connected_clients", "blocked_clients", "maxclients"] },
  { key: "stats", label: "Stats", fields: ["total_commands_processed", "instantaneous_ops_per_sec", "keyspace_hits", "keyspace_misses", "hit_rate"] },
  { key: "keyspace", label: "Keyspace", fields: [] as string[] },
  { key: "persistence", label: "Persistence", fields: ["aof_enabled", "rdb_last_save_time", "aof_rewrite_in_progress"] },
] as const;

export function InfoCards() {
  const serverInfo = useMetricsStore((s) => s.serverInfo);

  if (!serverInfo) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {INFO_SECTIONS.map(({ key, label }) => (
          <Card key={key}>
            <CardHeader><CardTitle>{label}</CardTitle></CardHeader>
            <CardContent>
              <div className="text-sm text-muted-foreground">Loading...</div>
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      {INFO_SECTIONS.map(({ key, label, fields }) => {
        const section = serverInfo[key as keyof typeof serverInfo] ?? {};
        const entries = fields.length > 0
          ? fields.map((f) => [f, section[f] ?? "\u2014"] as const)
          : Object.entries(section);

        return (
          <Card key={key}>
            <CardHeader><CardTitle>{label}</CardTitle></CardHeader>
            <CardContent>
              <dl className="space-y-1.5">
                {entries.map(([field, value]) => (
                  <div key={field} className="flex justify-between text-sm">
                    <dt className="text-muted-foreground truncate mr-2">{field}</dt>
                    <dd className="font-mono text-zinc-200 truncate">{String(value)}</dd>
                  </div>
                ))}
                {entries.length === 0 && (
                  <div className="text-sm text-muted-foreground">No data</div>
                )}
              </dl>
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}
