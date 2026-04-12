import { useEffect } from "react";
import { useGraphStore } from "@/stores/graph";
import { GraphScene } from "@/components/graph/GraphScene";
import { GraphInfoPanel } from "@/components/graph/GraphInfoPanel";
import { NodeInspector } from "@/components/graph/NodeInspector";
import { LabelFilter } from "@/components/graph/LabelFilter";
import { CypherInput } from "@/components/graph/CypherInput";

export function GraphExplorer() {
  const loading = useGraphStore((s) => s.loading);
  const nodes = useGraphStore((s) => s.nodes);
  const layoutProgress = useGraphStore((s) => s.layoutProgress);
  const loadGraphInfo = useGraphStore((s) => s.loadGraphInfo);

  useEffect(() => {
    loadGraphInfo();
  }, [loadGraphInfo]);

  return (
    <div className="flex h-full flex-col">
      {/* Top toolbar with Cypher input */}
      <div className="border-b border-zinc-800 bg-zinc-950 px-4 py-2">
        <CypherInput />
      </div>

      {/* Main content */}
      <div className="flex flex-1 overflow-hidden">
        {/* Canvas area */}
        <div className="relative flex-1">
          {loading && (
            <div className="absolute inset-0 z-10 flex items-center justify-center bg-zinc-950/80">
              <div className="text-sm text-zinc-500">Querying graph...</div>
            </div>
          )}

          {layoutProgress && (
            <div className="absolute left-4 top-4 z-10 rounded bg-zinc-900/90 px-3 py-1.5 text-xs text-zinc-400">
              Layout converging... alpha={layoutProgress.alpha.toFixed(3)}
            </div>
          )}

          {nodes.length > 0 ? (
            <GraphScene />
          ) : (
            <div className="flex h-full items-center justify-center">
              <div className="text-center">
                <div className="mb-2 text-lg text-zinc-400">No graph data</div>
                <div className="text-sm text-zinc-500">
                  Run a Cypher query above, e.g.{" "}
                  <code className="rounded bg-zinc-800 px-1.5 py-0.5 font-mono text-xs text-indigo-400">
                    MATCH (n)-[r]-&gt;(m) RETURN n, r, m LIMIT 200
                  </code>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Right sidebar */}
        <div className="w-72 shrink-0 overflow-y-auto border-l border-zinc-800 bg-zinc-950 p-4 text-sm text-zinc-300">
          <div className="space-y-6">
            <GraphInfoPanel />
            <LabelFilter />
            <NodeInspector />
          </div>
        </div>
      </div>
    </div>
  );
}
