import { useEffect, useRef, useState, useCallback } from "react";
import * as THREE from "three";
import { useVectorStore } from "@/stores/vector";
import { PointCloudScene } from "@/components/vector/PointCloudScene";
import { PointCloudCanvas2D } from "@/components/vector/PointCloudCanvas2D";
import { UmapProgress } from "@/components/vector/UmapProgress";
import { PointInspector } from "@/components/vector/PointInspector";
import { KnnSearchPanel } from "@/components/vector/KnnSearchPanel";
import { LassoSelect } from "@/components/vector/LassoSelect";
import { ClusterStats } from "@/components/vector/ClusterStats";
import { ColorByControls } from "@/components/vector/ColorByControls";

function hasWebGL(): boolean {
  try {
    const canvas = document.createElement("canvas");
    return !!(canvas.getContext("webgl2") || canvas.getContext("webgl"));
  } catch {
    return false;
  }
}

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
  const [webgl] = useState(() => hasWebGL());
  const [viewMode, setViewMode] = useState<"3d" | "2d">(webgl ? "3d" : "2d");

  useEffect(() => {
    loadIndexes();
  }, [loadIndexes]);

  // Auto-select when only 1 index exists
  useEffect(() => {
    if (!selectedIndex && indexes.length === 1) {
      selectIndex(indexes[0]);
    }
  }, [indexes, selectedIndex, selectIndex]);

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
      {/* Toolbar */}
      <div className="flex items-center gap-2 border-b border-zinc-800/60 bg-zinc-950 px-4 py-2">
        <select
          className="rounded-md border border-zinc-700/50 bg-zinc-900 px-2.5 py-1.5 text-sm text-zinc-200 focus:border-indigo-500 focus:outline-none"
          value={selectedIndex ?? ""}
          onChange={(e) => { if (e.target.value) selectIndex(e.target.value); }}
        >
          <option value="">
            {indexes.length === 0 ? "No indexes" : "Select index\u2026"}
          </option>
          {indexes.map((name) => (
            <option key={name} value={name}>{name}</option>
          ))}
        </select>

        <div className="mx-1 h-4 w-px bg-zinc-800" />
        <ColorByControls />
        <div className="flex-1" />

        {/* 2D / 3D toggle */}
        <ViewModeToggle mode={viewMode} webgl={webgl} onChange={setViewMode} />

        <div className="mx-1 h-4 w-px bg-zinc-800" />
        <TogglePill active={showHnsw} onClick={() => setShowHnsw(!showHnsw)} label="HNSW" />
        <TogglePill active={lassoActive} onClick={() => setLassoActive(!lassoActive)} label="Lasso" />
      </div>

      {/* Main content */}
      <div className="flex flex-1 overflow-hidden">
        <div ref={canvasContainerRef} className="relative flex-1">
          {loading && (
            <div className="absolute inset-0 z-10 flex items-center justify-center bg-zinc-950/80">
              <div className="text-sm text-zinc-500">Loading index data...</div>
            </div>
          )}

          {selectedIndex && !loading && (
            viewMode === "3d" && webgl
              ? <PointCloudScene onCameraReady={handleCameraReady} />
              : <PointCloudCanvas2D />
          )}

          {/* Overlays */}
          <UmapProgress />
          <PointInspector />
          {viewMode === "3d" && (
            <LassoSelect active={lassoActive} camera={camera} canvasRect={canvasRect} />
          )}
        </div>

        {/* Right sidebar */}
        {indexInfo && (
          <div className="w-64 shrink-0 overflow-y-auto border-l border-zinc-800/60 bg-zinc-950 px-3 py-3 text-sm text-zinc-300">
            <div className="space-y-4">
              <section>
                <SidebarHeading>Index</SidebarHeading>
                <dl className="space-y-1 text-xs">
                  <InfoRow label="Name" value={indexInfo.name} />
                  <InfoRow label="Dims" value={String(indexInfo.dimensions)} />
                  <InfoRow label="Metric" value={indexInfo.metric} />
                  <InfoRow label="Docs" value={indexInfo.num_docs.toLocaleString()} />
                  <InfoRow label="EF" value={String(indexInfo.ef_runtime)} />
                </dl>
              </section>

              {indexInfo.segments.length > 0 && (
                <section>
                  <SidebarHeading>Segments</SidebarHeading>
                  <div className="space-y-0.5 text-xs">
                    {indexInfo.segments.map((seg, i) => (
                      <div key={i} className="flex items-center justify-between py-0.5">
                        <span className="flex items-center gap-1.5">
                          <span className={`inline-block h-1.5 w-1.5 rounded-full ${seg.type === "immutable" ? "bg-indigo-400" : "bg-orange-400"}`} />
                          <span className="text-zinc-400">{seg.type}</span>
                          <span className="text-zinc-600">{seg.algorithm}</span>
                        </span>
                        <span className="font-mono text-zinc-300">{seg.num_vectors.toLocaleString()}</span>
                      </div>
                    ))}
                  </div>
                </section>
              )}

              <KnnSearchPanel />
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

function SidebarHeading({ children }: { children: React.ReactNode }) {
  return (
    <h3 className="mb-1.5 text-[11px] font-medium text-zinc-500">{children}</h3>
  );
}

function TogglePill({ active, onClick, label }: { active: boolean; onClick: () => void; label: string }) {
  return (
    <button
      className={`rounded-md px-2.5 py-1 text-xs transition-colors ${
        active
          ? "bg-indigo-500/15 text-indigo-300 ring-1 ring-indigo-500/30"
          : "text-zinc-500 hover:bg-zinc-800 hover:text-zinc-300"
      }`}
      onClick={onClick}
    >
      {label}
    </button>
  );
}

function ViewModeToggle({ mode, webgl, onChange }: { mode: "3d" | "2d"; webgl: boolean; onChange: (m: "3d" | "2d") => void }) {
  return (
    <div className="flex items-center gap-0.5 rounded-md bg-zinc-900 p-0.5">
      <button
        className={`rounded px-2 py-0.5 text-xs transition-colors ${
          mode === "2d" ? "bg-zinc-700 text-zinc-100" : "text-zinc-500 hover:text-zinc-300"
        }`}
        onClick={() => onChange("2d")}
      >
        2D
      </button>
      <button
        className={`rounded px-2 py-0.5 text-xs transition-colors ${
          mode === "3d" ? "bg-zinc-700 text-zinc-100" : "text-zinc-500 hover:text-zinc-300"
        } ${!webgl ? "cursor-not-allowed opacity-30" : ""}`}
        onClick={() => { if (webgl) onChange("3d"); }}
        title={!webgl ? "WebGL not available" : ""}
      >
        3D
      </button>
    </div>
  );
}
