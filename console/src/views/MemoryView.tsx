import { useEffect } from "react";
import { useMemoryStore } from "@/stores/memory";
import { MemoryTreemap } from "@/components/memory/MemoryTreemap";
import { SlowlogPanel } from "@/components/memory/SlowlogPanel";
import { CommandStatsTable } from "@/components/memory/CommandStatsTable";

const REFRESH_INTERVAL_MS = 15_000; // Refresh every 15s (heavier than dashboard)

export function MemoryView() {
  useEffect(() => {
    const store = useMemoryStore.getState();
    const refresh = () => {
      store.loadMemoryData().catch(() => {});
      store.loadSlowlog().catch(() => {});
      store.loadCommandStats().catch(() => {});
    };
    refresh();
    const interval = setInterval(refresh, REFRESH_INTERVAL_MS);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold text-zinc-100">Memory & Observability</h1>

      {/* Memory treemap -- full width */}
      <MemoryTreemap />

      {/* Slowlog + Command Stats -- side by side on lg */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <SlowlogPanel />
        <CommandStatsTable />
      </div>
    </div>
  );
}
