import { useEffect } from "react";
import { useMetricsStore } from "@/stores/metrics";
import { fetchServerInfo, fetchSlowlog } from "@/lib/api";
import { InfoCards } from "@/components/dashboard/InfoCards";
import { OpsChart } from "@/components/dashboard/OpsChart";
import { MemoryChart } from "@/components/dashboard/MemoryChart";
import { ClientsCard } from "@/components/dashboard/ClientsCard";
import { HitRatioCard } from "@/components/dashboard/HitRatioCard";
import { KeyspaceCard } from "@/components/dashboard/KeyspaceCard";
import { SlowlogTable } from "@/components/dashboard/SlowlogTable";

const REFRESH_INTERVAL_MS = 10_000; // Refresh INFO + slowlog every 10s

export function Dashboard() {
  useEffect(() => {
    const refresh = () => {
      fetchServerInfo()
        .then((info) => useMetricsStore.getState().setServerInfo(info))
        .catch(() => {});
      fetchSlowlog(25)
        .then((entries) => useMetricsStore.getState().setSlowlog(entries))
        .catch(() => {});
    };

    refresh(); // Fetch immediately
    const interval = setInterval(refresh, REFRESH_INTERVAL_MS);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold text-zinc-100">Dashboard</h1>

      {/* Real-time charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <OpsChart />
        <MemoryChart />
      </div>

      {/* Metric cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <ClientsCard />
        <HitRatioCard />
        <KeyspaceCard />
      </div>

      {/* Server INFO sections */}
      <InfoCards />

      {/* Slowlog */}
      <SlowlogTable />
    </div>
  );
}
