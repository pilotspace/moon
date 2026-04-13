import { useGraphStore } from "@/stores/graph";

const PALETTE = [
  "#6B93D4", "#5BBDAD", "#A88BD4", "#CC8A5B",
  "#D4807A", "#C4A55A", "#62B87A", "#8E92D4",
];

export function GraphInfoPanel() {
  const graphInfo = useGraphStore((s) => s.graphInfo);
  if (!graphInfo) return null;

  const labelEntries = Object.entries(graphInfo.labelCounts);
  const relEntries = Object.entries(graphInfo.relTypeCounts);

  return (
    <section>
      <h3 className="mb-1.5 text-[11px] font-medium text-zinc-500">Graph</h3>
      <dl className="space-y-0.5 text-xs">
        <Row label="Nodes" value={graphInfo.nodeCount.toLocaleString()} />
        <Row label="Edges" value={graphInfo.edgeCount.toLocaleString()} />
      </dl>

      {labelEntries.length > 0 && (
        <div className="mt-3">
          <div className="mb-1 text-[10px] font-medium text-zinc-400">Labels</div>
          <div className="space-y-px text-xs">
            {labelEntries.map(([label, count], i) => (
              <div key={label} className="flex items-center justify-between py-px">
                <span className="flex items-center gap-1.5">
                  <span
                    className="inline-block h-1.5 w-1.5 rounded-full"
                    style={{ backgroundColor: PALETTE[i % PALETTE.length] }}
                  />
                  <span className="text-zinc-400">{label}</span>
                </span>
                <span className="font-mono text-[10px] text-zinc-400">{count.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {relEntries.length > 0 && (
        <div className="mt-3">
          <div className="mb-1 text-[10px] font-medium text-zinc-400">Relationships</div>
          <div className="space-y-px text-xs">
            {relEntries.map(([relType, count]) => (
              <div key={relType} className="flex items-center justify-between py-px">
                <span className="text-zinc-400">{relType}</span>
                <span className="font-mono text-[10px] text-zinc-400">{count.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between">
      <dt className="text-zinc-500">{label}</dt>
      <dd className="font-mono text-zinc-200">{value}</dd>
    </div>
  );
}
