import { useState } from "react";
import { useVectorStore } from "@/stores/vector";

export function KnnSearchPanel() {
  const hoveredPointId = useVectorStore((s) => s.hoveredPointId);
  const knnResults = useVectorStore((s) => s.knnResults);
  const knnQueryPointId = useVectorStore((s) => s.knnQueryPointId);
  const searchFromPoint = useVectorStore((s) => s.searchFromPoint);
  const performKnnSearch = useVectorStore((s) => s.performKnnSearch);
  const clearKnnResults = useVectorStore((s) => s.clearKnnResults);
  const setHovered = useVectorStore((s) => s.setHovered);
  const points = useVectorStore((s) => s.points);

  const [k, setK] = useState(10);
  const [showManual, setShowManual] = useState(false);
  const [manualVector, setManualVector] = useState("");

  const handleSearchFromSelected = () => {
    if (hoveredPointId) {
      searchFromPoint(hoveredPointId, k);
    }
  };

  const handleManualSearch = () => {
    const nums = manualVector
      .split(",")
      .map((s) => Number(s.trim()))
      .filter((n) => !isNaN(n));
    if (nums.length > 0) {
      performKnnSearch(nums, k);
    }
  };

  return (
    <section>
      <h3 className="mb-1.5 text-[11px] font-medium text-zinc-500">
        KNN Search
      </h3>

      <div className="space-y-2">
        <div className="flex items-center gap-2">
          <label className="text-xs text-zinc-500">k</label>
          <input
            type="number"
            min={1}
            max={100}
            value={k}
            onChange={(e) => setK(Math.max(1, Math.min(100, Number(e.target.value))))}
            className="w-14 rounded-md border border-zinc-700/50 bg-zinc-900 px-2 py-1 text-xs text-zinc-200 focus:border-indigo-500 focus:outline-none"
          />
        </div>

        <button
          disabled={!hoveredPointId}
          className="w-full rounded-md bg-indigo-600/80 px-2 py-1.5 text-xs font-medium text-white hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-30"
          onClick={handleSearchFromSelected}
        >
          Search from selected point
        </button>

        <button
          className="text-[10px] text-zinc-600 hover:text-zinc-400"
          onClick={() => setShowManual(!showManual)}
        >
          {showManual ? "Hide" : "Show"} manual query
        </button>

        {showManual && (
          <div className="space-y-1">
            <textarea
              className="h-14 w-full resize-none rounded-md border border-zinc-700/50 bg-zinc-900 px-2 py-1 font-mono text-[10px] text-zinc-200 placeholder:text-zinc-700 focus:border-indigo-500 focus:outline-none"
              placeholder="0.1, 0.2, 0.3, ..."
              value={manualVector}
              onChange={(e) => setManualVector(e.target.value)}
            />
            <button
              className="w-full rounded-md bg-zinc-800 px-2 py-1 text-xs text-zinc-300 hover:bg-zinc-700"
              onClick={handleManualSearch}
            >
              Search
            </button>
          </div>
        )}

        {knnResults.length > 0 && (
          <div className="border-t border-zinc-800/60 pt-2">
            <div className="mb-1 flex items-center justify-between">
              <span className="text-[11px] font-medium text-zinc-500">
                Results ({knnResults.length})
              </span>
              <button
                className="text-[10px] text-zinc-600 hover:text-zinc-400"
                onClick={clearKnnResults}
              >
                Clear
              </button>
            </div>
            {knnQueryPointId && (
              <div className="mb-1 truncate text-[10px] text-indigo-400">
                {points.find((p) => p.id === knnQueryPointId)?.key ?? knnQueryPointId}
              </div>
            )}
            <div className="max-h-40 space-y-px overflow-y-auto">
              {knnResults.map((r, i) => (
                <button
                  key={r.key}
                  className="flex w-full items-center justify-between rounded-md px-1.5 py-0.5 text-left text-xs hover:bg-zinc-800/60"
                  onClick={() => {
                    const pt = points.find((p) => p.key === r.key);
                    if (pt) setHovered(pt.id);
                  }}
                >
                  <span className="truncate text-zinc-300">
                    <span className="mr-1 text-zinc-600">{i + 1}.</span>
                    {r.key}
                  </span>
                  <span className="ml-2 shrink-0 font-mono text-[10px] text-cyan-400">
                    {r.score.toFixed(4)}
                  </span>
                </button>
              ))}
            </div>
          </div>
        )}
      </div>
    </section>
  );
}
