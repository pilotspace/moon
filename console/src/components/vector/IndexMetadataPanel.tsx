import { useEffect } from "react";
import { useVectorStore } from "@/stores/vector";

export function IndexMetadataPanel() {
  const indexes = useVectorStore((s) => s.indexes);
  const selectedIndex = useVectorStore((s) => s.selectedIndex);
  const indexInfo = useVectorStore((s) => s.indexInfo);
  const loading = useVectorStore((s) => s.loading);
  const points = useVectorStore((s) => s.points);
  const loadIndexes = useVectorStore((s) => s.loadIndexes);
  const selectIndex = useVectorStore((s) => s.selectIndex);
  const colorBy = useVectorStore((s) => s.colorBy);
  const setColorBy = useVectorStore((s) => s.setColorBy);

  useEffect(() => {
    loadIndexes();
  }, [loadIndexes]);

  return (
    <div className="w-72 shrink-0 overflow-y-auto border-l border-zinc-800 bg-zinc-950 p-4 text-sm text-zinc-300">
      <h2 className="mb-3 text-xs font-semibold uppercase tracking-wider text-zinc-500">
        Vector Explorer
      </h2>

      {/* Index selector */}
      <label className="mb-1 block text-xs text-zinc-500">Index</label>
      <select
        className="mb-4 w-full rounded border border-zinc-700 bg-zinc-900 px-2 py-1.5 text-sm text-zinc-200 focus:border-indigo-500 focus:outline-none"
        value={selectedIndex ?? ""}
        onChange={(e) => {
          if (e.target.value) selectIndex(e.target.value);
        }}
      >
        <option value="">
          {indexes.length === 0 ? "No indexes found" : "Select an index..."}
        </option>
        {indexes.map((name) => (
          <option key={name} value={name}>
            {name}
          </option>
        ))}
      </select>

      {loading && (
        <div className="mb-4 text-xs text-zinc-500">Loading index data...</div>
      )}

      {/* Index metadata */}
      {indexInfo && (
        <div className="space-y-4">
          <section>
            <h3 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500">
              Index Info
            </h3>
            <dl className="space-y-1">
              <InfoRow label="Name" value={indexInfo.name} />
              <InfoRow label="Dimensions" value={String(indexInfo.dimensions)} />
              <InfoRow label="Metric" value={indexInfo.metric} />
              <InfoRow
                label="Total Docs"
                value={indexInfo.num_docs.toLocaleString()}
              />
              <InfoRow
                label="Vectors Loaded"
                value={points.length.toLocaleString()}
              />
              <InfoRow
                label="EF Runtime"
                value={String(indexInfo.ef_runtime)}
              />
              <InfoRow
                label="Compact Threshold"
                value={indexInfo.compact_threshold.toLocaleString()}
              />
            </dl>
          </section>

          {/* Segments table */}
          {indexInfo.segments.length > 0 && (
            <section>
              <h3 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500">
                Segments
              </h3>
              <table className="w-full text-left text-xs">
                <thead>
                  <tr className="border-b border-zinc-800 text-zinc-500">
                    <th className="pb-1 pr-2 font-medium">Type</th>
                    <th className="pb-1 pr-2 font-medium">Algorithm</th>
                    <th className="pb-1 text-right font-medium">Vectors</th>
                  </tr>
                </thead>
                <tbody>
                  {indexInfo.segments.map((seg, i) => (
                    <tr key={i} className="border-b border-zinc-800/50">
                      <td className="py-1 pr-2">
                        <span
                          className={`inline-block h-2 w-2 rounded-full ${seg.type === "immutable" ? "bg-indigo-500" : "bg-orange-500"} mr-1.5`}
                        />
                        {seg.type}
                      </td>
                      <td className="py-1 pr-2 text-zinc-400">
                        {seg.algorithm}
                      </td>
                      <td className="py-1 text-right tabular-nums">
                        {seg.num_vectors.toLocaleString()}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
          )}

          {/* Color mode */}
          <section>
            <h3 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500">
              Color By
            </h3>
            <div className="flex gap-1">
              {(["segment", "label", "none"] as const).map((mode) => (
                <button
                  key={mode}
                  className={`rounded px-2 py-1 text-xs capitalize ${
                    colorBy === mode
                      ? "bg-indigo-600 text-white"
                      : "bg-zinc-800 text-zinc-400 hover:bg-zinc-700"
                  }`}
                  onClick={() => setColorBy(mode)}
                >
                  {mode}
                </button>
              ))}
            </div>
          </section>
        </div>
      )}
    </div>
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
