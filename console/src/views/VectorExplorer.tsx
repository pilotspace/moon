import { useEffect, useRef, useState, useCallback } from "react";
import * as THREE from "three";
import { useVectorStore } from "@/stores/vector";
import { PointCloudScene } from "@/components/vector/PointCloudScene";
import { UmapProgress } from "@/components/vector/UmapProgress";
import { PointInspector } from "@/components/vector/PointInspector";
import { KnnSearchPanel } from "@/components/vector/KnnSearchPanel";
import { LassoSelect } from "@/components/vector/LassoSelect";
import { ClusterStats } from "@/components/vector/ClusterStats";
import { ColorByControls } from "@/components/vector/ColorByControls";

export function VectorExplorer() {
  const indexes = useVectorStore((s) => s.indexes);
  const selectedIndex = useVectorStore((s) => s.selectedIndex);
  const indexInfo = useVectorStore((s) => s.indexInfo);
  const loading = useVectorStore((s) => s.loading);
  const showHnsw = useVectorStore((s) => s.showHnsw);
  const setShowHnsw = useVectorStore((s) => s.setShowHnsw);
  const loadIndexes = useVectorStore((s) => s.loadIndexes);
  const selectIndex = useVectorStore((s) => s.selectIndex);

  const [lassoActive, setLassoActive] = useState(false);
  const [camera, setCamera] = useState<THREE.Camera | null>(null);
  const canvasContainerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    loadIndexes();
  }, [loadIndexes]);

  const handleCameraReady = useCallback((cam: THREE.Camera) => {
    setCamera(cam);
  }, []);

  const canvasRect = canvasContainerRef.current?.getBoundingClientRect() ?? null;

  // Empty state
  if (!loading && indexes.length === 0 && !selectedIndex) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="text-center">
          <div className="mb-2 text-lg text-zinc-400">No vector indexes found</div>
          <div className="text-sm text-zinc-500">
            Create one with{" "}
            <code className="rounded bg-zinc-800 px-1.5 py-0.5 font-mono text-xs text-indigo-400">
              FT.CREATE
            </code>{" "}
            in the Console.
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col">
      {/* Top toolbar */}
      <div className="flex items-center gap-3 border-b border-zinc-800 bg-zinc-950 px-4 py-2">
        {/* Index selector */}
        <select
          className="rounded border border-zinc-700 bg-zinc-900 px-2 py-1.5 text-sm text-zinc-200 focus:border-indigo-500 focus:outline-none"
          value={selectedIndex ?? ""}
          onChange={(e) => {
            if (e.target.value) selectIndex(e.target.value);
          }}
        >
          <option value="">
            {indexes.length === 0 ? "No indexes" : "Select index..."}
          </option>
          {indexes.map((name) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </select>

        <div className="flex-1" />

        {/* Controls */}
        <ColorByControls />

        {/* HNSW toggle */}
        <button
          className={`rounded border px-2 py-1 text-[10px] ${
            showHnsw
              ? "border-cyan-500 bg-cyan-600/20 text-cyan-300"
              : "border-zinc-700 bg-zinc-800 text-zinc-400 hover:bg-zinc-700"
          }`}
          onClick={() => setShowHnsw(!showHnsw)}
          title="Toggle HNSW edges"
        >
          HNSW
        </button>

        {/* Lasso toggle */}
        <button
          className={`rounded border px-2 py-1 text-[10px] ${
            lassoActive
              ? "border-cyan-500 bg-cyan-600/20 text-cyan-300"
              : "border-zinc-700 bg-zinc-800 text-zinc-400 hover:bg-zinc-700"
          }`}
          onClick={() => setLassoActive(!lassoActive)}
          title="Toggle lasso selection"
        >
          Lasso
        </button>
      </div>

      {/* Main content */}
      <div className="flex flex-1 overflow-hidden">
        {/* Canvas area */}
        <div ref={canvasContainerRef} className="relative flex-1">
          {loading && (
            <div className="absolute inset-0 z-10 flex items-center justify-center bg-zinc-950/80">
              <div className="text-sm text-zinc-500">Loading index data...</div>
            </div>
          )}

          {selectedIndex && !loading && (
            <PointCloudScene onCameraReady={handleCameraReady} />
          )}

          {/* Overlays */}
          <UmapProgress />
          <PointInspector />
          <LassoSelect
            active={lassoActive}
            camera={camera}
            canvasRect={canvasRect}
          />
        </div>

        {/* Right sidebar */}
        {indexInfo && (
          <div className="w-72 shrink-0 overflow-y-auto border-l border-zinc-800 bg-zinc-950 p-4 text-sm text-zinc-300">
            <div className="space-y-6">
              {/* Index metadata (inline, without the selector since it's in toolbar) */}
              <section>
                <h3 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500">
                  Index Info
                </h3>
                <dl className="space-y-1 text-xs">
                  <InfoRow label="Name" value={indexInfo.name} />
                  <InfoRow label="Dimensions" value={String(indexInfo.dimensions)} />
                  <InfoRow label="Metric" value={indexInfo.metric} />
                  <InfoRow label="Documents" value={indexInfo.num_docs.toLocaleString()} />
                  <InfoRow label="EF Runtime" value={String(indexInfo.ef_runtime)} />
                </dl>
              </section>

              {/* Segments */}
              {indexInfo.segments.length > 0 && (
                <section>
                  <h3 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500">
                    Segments
                  </h3>
                  <div className="space-y-1 text-xs">
                    {indexInfo.segments.map((seg, i) => (
                      <div key={i} className="flex items-center justify-between">
                        <span>
                          <span
                            className={`mr-1.5 inline-block h-2 w-2 rounded-full ${
                              seg.type === "immutable" ? "bg-indigo-500" : "bg-orange-500"
                            }`}
                          />
                          {seg.type} ({seg.algorithm})
                        </span>
                        <span className="font-mono">{seg.num_vectors.toLocaleString()}</span>
                      </div>
                    ))}
                  </div>
                </section>
              )}

              {/* KNN Search */}
              <KnnSearchPanel />

              {/* Cluster Stats */}
              <ClusterStats />
            </div>
          </div>
        )}
      </div>
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
