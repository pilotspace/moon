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
    <div className="absolute bottom-4 right-4 z-10 w-64 rounded-lg border border-zinc-700 bg-zinc-900/95 p-3 shadow-xl backdrop-blur-sm">
      <h3 className="mb-2 truncate text-xs font-semibold text-zinc-300">
        {point.key}
      </h3>

      <div className="mb-2 space-y-1 text-xs">
        <div className="flex items-center justify-between">
          <span className="text-zinc-500">Dimensions</span>
          <span className="font-mono text-zinc-200">{point.vector.length}</span>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-zinc-500">Segment</span>
          <span
            className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${
              point.segment === "immutable"
                ? "bg-indigo-500/20 text-indigo-300"
                : "bg-orange-500/20 text-orange-300"
            }`}
          >
            {point.segment}
          </span>
        </div>
        {knnMatch && (
          <div className="flex items-center justify-between">
            <span className="text-zinc-500">Distance</span>
            <span className="font-mono text-cyan-400">
              {knnMatch.score.toFixed(4)}
            </span>
          </div>
        )}
      </div>

      {/* Payload fields */}
      {payloadEntries.length > 0 && (
        <div className="mb-2 max-h-32 overflow-y-auto border-t border-zinc-800 pt-2">
          <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-zinc-500">
            Payload
          </div>
          {payloadEntries.map(([k, v]) => (
            <div key={k} className="flex items-start justify-between gap-2 py-0.5 text-xs">
              <span className="shrink-0 text-zinc-500">{k}</span>
              <span className="truncate text-right text-zinc-300">{v}</span>
            </div>
          ))}
        </div>
      )}

      <button
        className="w-full rounded bg-cyan-600 px-2 py-1.5 text-xs font-medium text-white hover:bg-cyan-500"
        onClick={() => searchFromPoint(hoveredPointId)}
      >
        Search KNN from here
      </button>
    </div>
  );
}
