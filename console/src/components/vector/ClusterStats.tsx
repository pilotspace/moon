import { useMemo } from "react";
import { useVectorStore } from "@/stores/vector";

export function ClusterStats() {
  const lassoSelectedIds = useVectorStore((s) => s.lassoSelectedIds);
  const points = useVectorStore((s) => s.points);
  const setLassoSelected = useVectorStore((s) => s.setLassoSelected);

  const stats = useMemo(() => {
    if (lassoSelectedIds.size === 0) return null;

    let mutableCount = 0;
    let immutableCount = 0;
    const labelCounts = new Map<string, number>();

    for (const id of lassoSelectedIds) {
      const point = points.find((p) => p.id === id);
      if (!point) continue;

      if (point.segment === "mutable") mutableCount++;
      else immutableCount++;

      const label = point.payload?.["label"] ?? point.payload?.["category"] ?? "unlabeled";
      labelCounts.set(label, (labelCounts.get(label) ?? 0) + 1);
    }

    const sortedLabels = [...labelCounts.entries()].sort((a, b) => b[1] - a[1]);

    return { mutableCount, immutableCount, sortedLabels };
  }, [lassoSelectedIds, points]);

  if (!stats) return null;

  return (
    <section>
      <h3 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500">
        Cluster Stats
      </h3>

      <div className="space-y-2 text-xs">
        <div className="flex items-center justify-between">
          <span className="text-zinc-500">Selected</span>
          <span className="font-mono text-zinc-200">{lassoSelectedIds.size}</span>
        </div>

        {/* Segment breakdown */}
        <div className="space-y-1">
          <div className="flex items-center justify-between">
            <span className="text-zinc-500">
              <span className="mr-1 inline-block h-2 w-2 rounded-full bg-orange-500" />
              Mutable
            </span>
            <span className="font-mono text-zinc-200">{stats.mutableCount}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-zinc-500">
              <span className="mr-1 inline-block h-2 w-2 rounded-full bg-indigo-500" />
              Immutable
            </span>
            <span className="font-mono text-zinc-200">{stats.immutableCount}</span>
          </div>
        </div>

        {/* Label distribution */}
        {stats.sortedLabels.length > 0 && (
          <div className="border-t border-zinc-800 pt-2">
            <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-zinc-500">
              Labels
            </div>
            <div className="max-h-32 space-y-0.5 overflow-y-auto">
              {stats.sortedLabels.map(([label, count]) => (
                <div key={label} className="flex items-center justify-between">
                  <span className="truncate text-zinc-400">{label}</span>
                  <span className="ml-2 shrink-0 font-mono text-zinc-300">{count}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        <button
          className="w-full rounded bg-zinc-800 px-2 py-1 text-xs text-zinc-400 hover:bg-zinc-700 hover:text-zinc-200"
          onClick={() => setLassoSelected(new Set())}
        >
          Clear selection
        </button>
      </div>
    </section>
  );
}
