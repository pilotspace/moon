import { useGraphStore } from "@/stores/graph";

const PALETTE = [
  "#6B93D4", "#5BBDAD", "#A88BD4", "#CC8A5B",
  "#D4807A", "#C4A55A", "#62B87A", "#8E92D4",
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
      {labelEntries.length > 0 && (
        <div>
          <div className="mb-1 flex items-center justify-between">
            <h3 className="text-[11px] font-medium text-zinc-500">Filter Labels</h3>
            <span className="flex gap-1.5">
              <MiniBtn onClick={() => setAllLabelsVisible(true)}>all</MiniBtn>
              <MiniBtn onClick={() => setAllLabelsVisible(false)}>none</MiniBtn>
            </span>
          </div>
          <div className="space-y-px">
            {labelEntries.map(([label, count], i) => (
              <label
                key={label}
                className="flex cursor-pointer items-center gap-2 rounded px-1 py-0.5 text-xs hover:bg-zinc-800/40"
              >
                <input
                  type="checkbox"
                  className="accent-sky-500"
                  checked={visibleLabels.has(label)}
                  onChange={() => toggleLabel(label)}
                />
                <span
                  className="inline-block h-1.5 w-1.5 rounded-full"
                  style={{ backgroundColor: PALETTE[i % PALETTE.length] }}
                />
                <span className="flex-1 text-zinc-300">{label}</span>
                <span className="font-mono text-[10px] text-zinc-400">
                  {count.toLocaleString()}
                </span>
              </label>
            ))}
          </div>
        </div>
      )}

      {relEntries.length > 0 && (
        <div className="mt-3">
          <div className="mb-1 flex items-center justify-between">
            <h3 className="text-[11px] font-medium text-zinc-500">Filter Edges</h3>
            <span className="flex gap-1.5">
              <MiniBtn onClick={() => setAllRelTypesVisible(true)}>all</MiniBtn>
              <MiniBtn onClick={() => setAllRelTypesVisible(false)}>none</MiniBtn>
            </span>
          </div>
          <div className="space-y-px">
            {relEntries.map(([relType, count]) => (
              <label
                key={relType}
                className="flex cursor-pointer items-center gap-2 rounded px-1 py-0.5 text-xs hover:bg-zinc-800/40"
              >
                <input
                  type="checkbox"
                  className="accent-sky-500"
                  checked={visibleRelTypes.has(relType)}
                  onChange={() => toggleRelType(relType)}
                />
                <span className="flex-1 text-zinc-300">{relType}</span>
                <span className="font-mono text-[10px] text-zinc-400">
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

function MiniBtn({ onClick, children }: { onClick: () => void; children: React.ReactNode }) {
  return (
    <button className="text-[10px] text-zinc-400 hover:text-zinc-400" onClick={onClick}>
      {children}
    </button>
  );
}
