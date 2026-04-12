import { useGraphStore } from "@/stores/graph";

const PALETTE = [
  "#6366f1",
  "#f97316",
  "#22c55e",
  "#ef4444",
  "#06b6d4",
  "#a855f7",
  "#eab308",
  "#ec4899",
];

export function LabelFilter() {
  const graphInfo = useGraphStore((s) => s.graphInfo);
  const visibleLabels = useGraphStore((s) => s.visibleLabels);
  const visibleRelTypes = useGraphStore((s) => s.visibleRelTypes);
  const toggleLabel = useGraphStore((s) => s.toggleLabel);
  const toggleRelType = useGraphStore((s) => s.toggleRelType);
  const setAllLabelsVisible = useGraphStore((s) => s.setAllLabelsVisible);
  const setAllRelTypesVisible = useGraphStore((s) => s.setAllRelTypesVisible);

  if (!graphInfo) return null;

  const labelEntries = Object.entries(graphInfo.labelCounts);
  const relEntries = Object.entries(graphInfo.relTypeCounts);

  if (labelEntries.length === 0 && relEntries.length === 0) return null;

  return (
    <section>
      {/* Node Labels */}
      {labelEntries.length > 0 && (
        <div>
          <div className="mb-1 flex items-center justify-between">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-zinc-500">
              Node Labels
            </h3>
            <span className="flex gap-2">
              <button
                className="text-[10px] text-indigo-400 hover:text-indigo-300"
                onClick={() => setAllLabelsVisible(true)}
              >
                All
              </button>
              <button
                className="text-[10px] text-indigo-400 hover:text-indigo-300"
                onClick={() => setAllLabelsVisible(false)}
              >
                None
              </button>
            </span>
          </div>
          <div className="space-y-0.5">
            {labelEntries.map(([label, count], i) => (
              <label
                key={label}
                className="flex cursor-pointer items-center gap-2 py-0.5 text-xs"
              >
                <input
                  type="checkbox"
                  className="accent-indigo-500"
                  checked={visibleLabels.has(label)}
                  onChange={() => toggleLabel(label)}
                />
                <span
                  className="inline-block h-2 w-2 rounded-full"
                  style={{ backgroundColor: PALETTE[i % PALETTE.length] }}
                />
                <span className="flex-1 text-zinc-300">{label}</span>
                <span className="font-mono text-[10px] text-zinc-500">
                  {count.toLocaleString()}
                </span>
              </label>
            ))}
          </div>
        </div>
      )}

      {/* Edge Types */}
      {relEntries.length > 0 && (
        <div className="mt-3">
          <div className="mb-1 flex items-center justify-between">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-zinc-500">
              Edge Types
            </h3>
            <span className="flex gap-2">
              <button
                className="text-[10px] text-indigo-400 hover:text-indigo-300"
                onClick={() => setAllRelTypesVisible(true)}
              >
                All
              </button>
              <button
                className="text-[10px] text-indigo-400 hover:text-indigo-300"
                onClick={() => setAllRelTypesVisible(false)}
              >
                None
              </button>
            </span>
          </div>
          <div className="space-y-0.5">
            {relEntries.map(([relType, count]) => (
              <label
                key={relType}
                className="flex cursor-pointer items-center gap-2 py-0.5 text-xs"
              >
                <input
                  type="checkbox"
                  className="accent-indigo-500"
                  checked={visibleRelTypes.has(relType)}
                  onChange={() => toggleRelType(relType)}
                />
                <span className="flex-1 text-zinc-300">{relType}</span>
                <span className="font-mono text-[10px] text-zinc-500">
                  {count.toLocaleString()}
                </span>
              </label>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}
