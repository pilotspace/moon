import { useEffect, useState, Component, type ReactNode } from "react";
import { useGraphStore } from "@/stores/graph";
import { GraphScene } from "@/components/graph/GraphScene";
import { GraphCosmos } from "@/components/graph/GraphCosmos";
import { GraphCanvas2D } from "@/components/graph/GraphCanvas2D";
import { GraphInfoPanel } from "@/components/graph/GraphInfoPanel";
import { NodeInspector } from "@/components/graph/NodeInspector";
import { LabelFilter } from "@/components/graph/LabelFilter";
import { CypherInput } from "@/components/graph/CypherInput";

function hasWebGL2(): boolean {
  try {
    const c = document.createElement("canvas");
    return !!c.getContext("webgl2");
  } catch { return false; }
}

function hasWebGL(): boolean {
  try {
    const c = document.createElement("canvas");
    return !!(c.getContext("webgl2") || c.getContext("webgl"));
  } catch { return false; }
}

/** Error boundary: if cosmos.gl crashes (no GPU), fall back to Canvas2D */
class CosmosErrorBoundary extends Component<
  { children: ReactNode; fallback: ReactNode },
  { error: boolean }
> {
  state = { error: false };
  static getDerivedStateFromError() { return { error: true }; }
  render() { return this.state.error ? this.props.fallback : this.props.children; }
}

export function GraphExplorer() {
  const loading = useGraphStore((s) => s.loading);
  const nodes = useGraphStore((s) => s.nodes);
  const layoutProgress = useGraphStore((s) => s.layoutProgress);
  const loadGraphInfo = useGraphStore((s) => s.loadGraphInfo);
  const graphNames = useGraphStore((s) => s.graphNames);
  const selectedGraph = useGraphStore((s) => s.selectedGraph);
  const selectGraph = useGraphStore((s) => s.selectGraph);

  const [webgl] = useState(hasWebGL);
  const [webgl2] = useState(hasWebGL2);
  const [viewMode, setViewMode] = useState<"3d" | "2d">("2d");

  useEffect(() => { loadGraphInfo(); }, [loadGraphInfo]);

  return (
    <div className="flex h-full flex-col">
      {/* ── Toolbar ── */}
      <div className="flex items-center gap-2 border-b border-zinc-800/40 bg-zinc-950 px-4 py-2">
        {graphNames.length > 1 ? (
          <select
            className="rounded-md border border-zinc-700/50 bg-zinc-900 px-2.5 py-1.5 text-sm text-zinc-200 focus:border-cyan-500 focus:outline-none"
            value={selectedGraph ?? ""}
            onChange={(e) => { if (e.target.value) selectGraph(e.target.value); }}
          >
            {graphNames.map((n) => <option key={n} value={n}>{n}</option>)}
          </select>
        ) : graphNames.length === 1 ? (
          <span className="rounded-md bg-zinc-900 px-2.5 py-1.5 text-sm text-zinc-400">
            {graphNames[0]}
          </span>
        ) : null}

        <div className="flex-1"><CypherInput /></div>

        <DimensionToggle mode={viewMode} webgl={webgl} onChange={setViewMode} />
      </div>

      {/* ── Main ── */}
      <div className="flex flex-1 overflow-hidden">
        {/* Canvas */}
        <div className="relative flex-1">
          {loading && (
            <div className="absolute inset-0 z-10 flex items-center justify-center" style={{ background: "rgba(6,6,10,0.85)" }}>
              <div className="flex items-center gap-2 text-sm text-zinc-500">
                <Spinner /> Querying graph...
              </div>
            </div>
          )}

          {layoutProgress && (
            <div className="absolute left-3 top-3 z-10 rounded-md bg-black/60 px-3 py-1 text-[11px] tabular-nums text-zinc-500 backdrop-blur-sm">
              Layout converging\u2026 {(layoutProgress.alpha * 100).toFixed(0)}%
            </div>
          )}

          {nodes.length > 0 ? (
            viewMode === "3d" && webgl
              ? <GraphScene />
              : webgl2
                ? <CosmosErrorBoundary fallback={<GraphCanvas2D />}><GraphCosmos /></CosmosErrorBoundary>
                : <GraphCanvas2D />
          ) : (
            <div className="flex h-full items-center justify-center" style={{ background: "#06060a" }}>
              <div className="text-center">
                <div className="mb-3 text-4xl text-zinc-800">
                  <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1" className="mx-auto"><circle cx="5" cy="6" r="2"/><circle cx="19" cy="6" r="2"/><circle cx="12" cy="18" r="2"/><line x1="7" y1="6" x2="17" y2="6"/><line x1="6" y1="8" x2="11" y2="16"/><line x1="18" y1="8" x2="13" y2="16"/></svg>
                </div>
                <div className="mb-1 text-sm text-zinc-500">No graph data</div>
                <div className="text-xs text-zinc-600">
                  Run a Cypher query, e.g.{" "}
                  <code className="rounded bg-zinc-800/50 px-1.5 py-0.5 font-mono text-[11px] text-cyan-500/70">
                    MATCH (n)-[r]-&gt;(m) RETURN n,r,m
                  </code>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* ── Sidebar ── */}
        <div className="w-60 shrink-0 overflow-y-auto border-l border-zinc-800/40 bg-zinc-950 px-3 py-3 text-sm text-zinc-300">
          <div className="space-y-4">
            <GraphInfoPanel />
            <LabelFilter />
            <NodeInspector />
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Sub-components ──

function DimensionToggle({ mode, webgl, onChange }: {
  mode: "3d" | "2d"; webgl: boolean; onChange: (m: "3d" | "2d") => void;
}) {
  return (
    <div className="flex items-center gap-px rounded-md bg-zinc-900 p-0.5">
      <Pill active={mode === "2d"} onClick={() => onChange("2d")}>2D</Pill>
      <Pill
        active={mode === "3d"}
        onClick={() => { if (webgl) onChange("3d"); }}
        disabled={!webgl}
        title={!webgl ? "WebGL unavailable" : undefined}
      >
        3D
      </Pill>
    </div>
  );
}

function Pill({ active, onClick, disabled, title, children }: {
  active: boolean; onClick: () => void; disabled?: boolean; title?: string;
  children: React.ReactNode;
}) {
  return (
    <button
      className={`rounded px-2 py-0.5 text-xs transition-colors ${
        active ? "bg-zinc-700 text-zinc-100" : "text-zinc-500 hover:text-zinc-300"
      } ${disabled ? "cursor-not-allowed opacity-30" : ""}`}
      onClick={onClick}
      title={title}
    >
      {children}
    </button>
  );
}

function Spinner() {
  return (
    <div className="h-3.5 w-3.5 animate-spin rounded-full border border-zinc-700 border-t-cyan-500" />
  );
}
