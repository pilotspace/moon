import { useVectorStore } from "@/stores/vector";

export function PointInspector() {
  const hoveredPointId = useVectorStore((s) => s.hoveredPointId);
  const points = useVectorStore((s) => s.points);
  const knnResults = useVectorStore((s) => s.knnResults);
  const searchFromPoint = useVectorStore((s) => s.searchFromPoint);

  if (!hoveredPointId) return null;

  const point = points.find((p) => p.id === hoveredPointId);
  if (!point) return null;

  const knnMatch = knnResults.find((r) => r.key === point.key);
  const payloadEntries = Object.entries(point.payload);

  return (
    <div className="absolute bottom-3 left-3 z-10 w-56 rounded-lg border border-zinc-700/50 bg-zinc-900/95 p-2.5 shadow-xl backdrop-blur-sm">
      <div className="mb-1.5 truncate text-xs font-medium text-zinc-200">
        {point.key}
      </div>

      <div className="mb-1.5 space-y-0.5 text-[11px]">
        <Row label="Dims" value={String(point.vector.length)} />
        <div className="flex items-center justify-between">
          <span className="text-zinc-500">Segment</span>
          <span className={`text-[10px] font-medium ${point.segment === "immutable" ? "text-indigo-400" : "text-orange-400"}`}>
            {point.segment}
          </span>
        </div>
        {knnMatch && <Row label="Distance" value={knnMatch.score.toFixed(4)} accent />}
      </div>

      {payloadEntries.length > 0 && (
        <div className="mb-1.5 max-h-24 overflow-y-auto border-t border-zinc-800/60 pt-1.5">
          {payloadEntries.slice(0, 5).map(([k, v]) => (
            <div key={k} className="flex items-start justify-between gap-2 py-px text-[11px]">
              <span className="shrink-0 text-zinc-500">{k}</span>
              <span className="truncate text-right text-zinc-300">{String(v)}</span>
            </div>
          ))}
          {payloadEntries.length > 5 && (
            <div className="text-[10px] text-zinc-600">+{payloadEntries.length - 5} more</div>
          )}
        </div>
      )}

      <button
        className="w-full rounded-md bg-indigo-600/80 px-2 py-1 text-[11px] font-medium text-white hover:bg-indigo-500"
        onClick={() => searchFromPoint(hoveredPointId)}
      >
        Search KNN from here
      </button>
    </div>
  );
}

function Row({ label, value, accent }: { label: string; value: string; accent?: boolean }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-zinc-500">{label}</span>
      <span className={`font-mono ${accent ? "text-cyan-400" : "text-zinc-200"}`}>{value}</span>
    </div>
  );
}
