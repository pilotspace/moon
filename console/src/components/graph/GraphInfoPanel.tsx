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

export function GraphInfoPanel() {
  const graphInfo = useGraphStore((s) => s.graphInfo);

  if (!graphInfo) return null;

  const labelEntries = Object.entries(graphInfo.labelCounts);
  const relEntries = Object.entries(graphInfo.relTypeCounts);

  return (
    <section>
      <h3 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500">
        Graph Info
      </h3>
      <dl className="space-y-1 text-xs">
        <InfoRow label="Nodes" value={graphInfo.nodeCount.toLocaleString()} />
        <InfoRow label="Edges" value={graphInfo.edgeCount.toLocaleString()} />
      </dl>

      {labelEntries.length > 0 && (
        <div className="mt-3">
          <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-zinc-500">
            Labels
          </div>
          <div className="space-y-0.5 text-xs">
            {labelEntries.map(([label, count], i) => (
              <div key={label} className="flex items-center justify-between">
                <span className="flex items-center gap-1.5">
                  <span
                    className="inline-block h-2 w-2 rounded-full"
                    style={{ backgroundColor: PALETTE[i % PALETTE.length] }}
                  />
                  {label}
                </span>
                <span className="font-mono text-zinc-400">{count.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {relEntries.length > 0 && (
        <div className="mt-3">
          <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-zinc-500">
            Relationships
          </div>
          <div className="space-y-0.5 text-xs">
            {relEntries.map(([relType, count]) => (
              <div key={relType} className="flex items-center justify-between">
                <span className="text-zinc-300">{relType}</span>
                <span className="font-mono text-zinc-400">{count.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

function InfoRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between">
      <dt className="text-zinc-500">{label}</dt>
      <dd className="font-mono text-zinc-200">{value}</dd>
    </div>
  );
}
