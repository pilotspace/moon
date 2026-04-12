import { useMemo } from "react";
import { useGraphStore } from "@/stores/graph";

export function NodeInspector() {
  const selectedNodeId = useGraphStore((s) => s.selectedNodeId);
  const nodes = useGraphStore((s) => s.nodes);
  const edges = useGraphStore((s) => s.edges);
  const selectNode = useGraphStore((s) => s.selectNode);

  const node = useMemo(
    () => nodes.find((n) => n.id === selectedNodeId),
    [nodes, selectedNodeId],
  );

  const { inDegree, outDegree, connectedEdges } = useMemo(() => {
    if (!selectedNodeId) return { inDegree: 0, outDegree: 0, connectedEdges: [] };
    const connected = edges.filter(
      (e) => e.source === selectedNodeId || e.target === selectedNodeId,
    );
    return {
      inDegree: edges.filter((e) => e.target === selectedNodeId).length,
      outDegree: edges.filter((e) => e.source === selectedNodeId).length,
      connectedEdges: connected.slice(0, 10),
    };
  }, [edges, selectedNodeId]);

  if (!selectedNodeId || !node) return null;

  const propertyEntries = Object.entries(node.properties);

  return (
    <section>
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-xs font-semibold uppercase tracking-wider text-zinc-500">
          Node Inspector
        </h3>
        <button
          className="text-xs text-zinc-500 hover:text-zinc-300"
          onClick={() => selectNode(null)}
        >
          x
        </button>
      </div>

      {/* Node ID */}
      <div className="mb-2 truncate font-mono text-xs text-zinc-200">
        {node.id}
      </div>

      {/* Labels as badges */}
      {node.labels.length > 0 && (
        <div className="mb-2 flex flex-wrap gap-1">
          {node.labels.map((label) => (
            <span
              key={label}
              className="rounded bg-indigo-600/20 px-1.5 py-0.5 text-[10px] text-indigo-300"
            >
              {label}
            </span>
          ))}
        </div>
      )}

      {/* Degree info */}
      <div className="mb-2 space-y-0.5 text-xs">
        <div className="flex items-center justify-between">
          <span className="text-zinc-500">In-degree</span>
          <span className="font-mono text-zinc-200">{inDegree}</span>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-zinc-500">Out-degree</span>
          <span className="font-mono text-zinc-200">{outDegree}</span>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-zinc-500">Total degree</span>
          <span className="font-mono text-zinc-200">{inDegree + outDegree}</span>
        </div>
      </div>

      {/* Properties */}
      {propertyEntries.length > 0 && (
        <div className="mb-2 max-h-40 overflow-y-auto border-t border-zinc-800 pt-2">
          <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-zinc-500">
            Properties
          </div>
          <dl className="space-y-0.5 text-xs">
            {propertyEntries.map(([key, value]) => (
              <div key={key} className="flex items-start justify-between gap-2">
                <dt className="shrink-0 text-zinc-500">{key}</dt>
                <dd className="truncate text-right font-mono text-zinc-300">
                  {String(value)}
                </dd>
              </div>
            ))}
          </dl>
        </div>
      )}

      {/* Connected edges */}
      {connectedEdges.length > 0 && (
        <div className="border-t border-zinc-800 pt-2">
          <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-zinc-500">
            Connected Edges ({inDegree + outDegree})
          </div>
          <div className="max-h-32 space-y-0.5 overflow-y-auto text-xs">
            {connectedEdges.map((edge) => {
              const isOutgoing = edge.source === selectedNodeId;
              const otherId = isOutgoing ? edge.target : edge.source;
              return (
                <div key={edge.id} className="flex items-center gap-1 text-zinc-400">
                  <span className="text-[10px]">{isOutgoing ? "\u2192" : "\u2190"}</span>
                  <span className="text-indigo-400">{edge.type}</span>
                  <span className="truncate font-mono text-zinc-500">{otherId}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </section>
  );
}
