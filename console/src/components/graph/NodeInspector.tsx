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

  const { inDeg, outDeg, connected } = useMemo(() => {
    if (!selectedNodeId) return { inDeg: 0, outDeg: 0, connected: [] as typeof edges };
    const c = edges.filter(
      (e) => e.source === selectedNodeId || e.target === selectedNodeId,
    );
    return {
      inDeg: edges.filter((e) => e.target === selectedNodeId).length,
      outDeg: edges.filter((e) => e.source === selectedNodeId).length,
      connected: c.slice(0, 10),
    };
  }, [edges, selectedNodeId]);

  if (!selectedNodeId || !node) return null;

  const props = Object.entries(node.properties);
  const displayId = node.id.replace("node:", "#");

  return (
    <section>
      <div className="mb-1.5 flex items-center justify-between">
        <h3 className="text-[11px] font-medium text-zinc-500">Inspector</h3>
        <button
          className="rounded px-1 text-[10px] text-zinc-400 hover:bg-zinc-800 hover:text-zinc-400"
          onClick={() => selectNode(null)}
        >
          \u2715
        </button>
      </div>

      <div className="mb-2 truncate font-mono text-xs text-zinc-200">{displayId}</div>

      {node.labels.length > 0 && (
        <div className="mb-2 flex flex-wrap gap-1">
          {node.labels.map((l) => (
            <span key={l} className="rounded bg-sky-500/10 px-1.5 py-px text-[10px] text-sky-400">
              {l}
            </span>
          ))}
        </div>
      )}

      <div className="mb-2 space-y-px text-xs">
        <Row label="In" value={String(inDeg)} />
        <Row label="Out" value={String(outDeg)} />
        <Row label="Total" value={String(inDeg + outDeg)} />
      </div>

      {props.length > 0 && (
        <div className="mb-2 max-h-36 overflow-y-auto border-t border-zinc-800/40 pt-1.5">
          <div className="mb-0.5 text-[10px] font-medium text-zinc-400">Properties</div>
          <dl className="space-y-px text-xs">
            {props.map(([k, v]) => (
              <div key={k} className="flex items-start justify-between gap-2">
                <dt className="shrink-0 text-zinc-500">{k}</dt>
                <dd className="truncate text-right font-mono text-zinc-300">{String(v)}</dd>
              </div>
            ))}
          </dl>
        </div>
      )}

      {connected.length > 0 && (
        <div className="border-t border-zinc-800/40 pt-1.5">
          <div className="mb-0.5 text-[10px] font-medium text-zinc-400">
            Edges ({inDeg + outDeg})
          </div>
          <div className="max-h-28 space-y-px overflow-y-auto text-xs">
            {connected.map((e) => {
              const out = e.source === selectedNodeId;
              const other = (out ? e.target : e.source).replace("node:", "#");
              return (
                <div key={e.id} className="flex items-center gap-1 text-zinc-500">
                  <span className="text-[10px]">{out ? "\u2192" : "\u2190"}</span>
                  <span className="text-cyan-500/70">{e.type}</span>
                  <span className="truncate font-mono text-zinc-400">{other}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </section>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between">
      <dt className="text-zinc-500">{label}</dt>
      <dd className="font-mono text-zinc-200">{value}</dd>
    </div>
  );
}
