import { useRef, useEffect, useCallback, useMemo, useState } from "react";
import { Graph } from "@cosmos.gl/graph";
import { useGraphStore } from "@/stores/graph";
import { GraphCanvas2D } from "./GraphCanvas2D";

// ── Palette — vibrant on dark, matching the Cosmograph aesthetic ──
const PALETTE: [number, number, number, number][] = [
  [0.42, 0.58, 0.83, 1], // blue     #6B93D4
  [0.36, 0.74, 0.68, 1], // teal     #5BBDAD
  [0.66, 0.55, 0.83, 1], // violet   #A88BD4
  [0.80, 0.54, 0.36, 1], // orange   #CC8A5B
  [0.83, 0.50, 0.48, 1], // rose     #D4807A
  [0.77, 0.65, 0.35, 1], // amber    #C4A55A
  [0.38, 0.72, 0.48, 1], // green    #62B87A
  [0.56, 0.57, 0.83, 1], // lavender #8E92D4
];

export function GraphCosmos() {
  const [initError, setInitError] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<Graph | null>(null);
  const prevDataRef = useRef<{ nodeCount: number; edgeCount: number }>({ nodeCount: 0, edgeCount: 0 });

  // If cosmos.gl failed to init, fall back to Canvas2D
  if (initError) return <GraphCanvas2D />;

  const nodes = useGraphStore((s) => s.nodes);
  const edges = useGraphStore((s) => s.edges);
  const positions = useGraphStore((s) => s.positions);
  const visibleLabels = useGraphStore((s) => s.visibleLabels);
  const selectedNodeId = useGraphStore((s) => s.selectedNodeId);
  const selectNode = useGraphStore((s) => s.selectNode);
  const setHoveredNode = useGraphStore((s) => s.setHoveredNode);

  // ── Build label → color map ──
  const labelColorMap = useMemo(() => {
    const all = new Set<string>();
    nodes.forEach((n) => n.labels.forEach((l) => all.add(l)));
    const m = new Map<string, [number, number, number, number]>();
    let i = 0;
    for (const l of all) m.set(l, PALETTE[i++ % PALETTE.length]);
    return m;
  }, [nodes]);

  // ── Compute degree for sizing ──
  const degree = useMemo(() => {
    const d = new Float32Array(nodes.length);
    const idxMap = new Map<string, number>();
    nodes.forEach((n, i) => idxMap.set(n.id, i));
    edges.forEach((e) => {
      const si = idxMap.get(e.source);
      const ti = idxMap.get(e.target);
      if (si != null) d[si]++;
      if (ti != null) d[ti]++;
    });
    return d;
  }, [nodes, edges]);

  // ── Node index lookup ──
  const nodeIdToIndex = useMemo(() => {
    const m = new Map<string, number>();
    nodes.forEach((n, i) => m.set(n.id, i));
    return m;
  }, [nodes]);

  // ── Interaction callbacks (stable refs) ──
  const onPointClick = useCallback((index: number | undefined) => {
    if (index != null && index < nodes.length) {
      selectNode(nodes[index].id);
    }
  }, [nodes, selectNode]);

  const onPointMouseOver = useCallback((index: number | undefined) => {
    if (index != null && index < nodes.length) {
      setHoveredNode(nodes[index].id);
    }
  }, [nodes, setHoveredNode]);

  const onPointMouseOut = useCallback(() => {
    setHoveredNode(null);
  }, [setHoveredNode]);

  const onBackgroundClick = useCallback(() => {
    selectNode(null);
  }, [selectNode]);

  // ── Create / destroy graph instance ──
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    let graph: Graph;
    try {
    graph = new Graph(container, {
      backgroundColor: "#06060a",
      // Simulation — spread nodes wide, organic feel
      enableSimulation: true,
      simulationFriction: 0.85,
      simulationRepulsion: 1.5,
      simulationGravity: 0.05,
      simulationLinkSpring: 0.6,
      simulationLinkDistance: 15,
      simulationDecay: 4000,
      // Points — SMALL dots (reference: tiny circles with glow)
      pointDefaultColor: "#6B93D4",
      pointDefaultSize: 1,
      pointOpacity: 0.85,
      pointSizeScale: 1,
      scalePointsOnZoom: false,
      pixelRatio: 1,
      renderHoveredPointRing: true,
      hoveredPointRingColor: "#e4e4e7",
      focusedPointRingColor: "#C4A55A",
      hoveredPointCursor: "pointer",
      // Links — thin but visible (reference: translucent hairlines)
      linkDefaultColor: "#6B93D4",
      linkOpacity: 0.2,
      linkDefaultWidth: 0.3,
      linkGreyoutOpacity: 0.03,
      scaleLinksOnZoom: true,
      curvedLinks: false,
      linkVisibilityDistanceRange: [50, 300],
      linkVisibilityMinTransparency: 0.1,
      // Viewport
      fitViewOnInit: true,
      fitViewDelay: 500,
      fitViewPadding: 0.1,
      fitViewDuration: 500,
      // Zoom & drag
      enableZoom: true,
      enableDrag: true,
      // Callbacks
      onPointClick,
      onPointMouseOver,
      onPointMouseOut,
      onBackgroundClick,
    });

    } catch {
      setInitError(true);
      return;
    }

    graphRef.current = graph;

    // Catch async init errors (WebGL context creation)
    graph.ready.catch(() => setInitError(true));

    return () => {
      try { graph.stop(); } catch { /* ignore cleanup errors */ }
      graphRef.current = null;
    };
  // Only recreate on mount/unmount — callbacks update via setConfig
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── Update callbacks when they change ──
  useEffect(() => {
    graphRef.current?.setConfigPartial({
      onPointClick,
      onPointMouseOver,
      onPointMouseOut,
      onBackgroundClick,
    });
  }, [onPointClick, onPointMouseOver, onPointMouseOut, onBackgroundClick]);

  // ── Feed data when nodes/edges/positions change ──
  useEffect(() => {
    const graph = graphRef.current;
    if (!graph || nodes.length === 0) return;

    const n = nodes.length;
    const dataChanged = prevDataRef.current.nodeCount !== n || prevDataRef.current.edgeCount !== edges.length;
    prevDataRef.current = { nodeCount: n, edgeCount: edges.length };

    // ── Point colors (RGBA Float32) ──
    // Use label color when available; if all nodes share one label, color by degree bucket
    const uniqueLabels = new Set<string>();
    nodes.forEach((nd) => nd.labels.forEach((l) => uniqueLabels.add(l)));
    const colorByDegree = uniqueLabels.size <= 1;

    const colors = new Float32Array(n * 4);
    for (let i = 0; i < n; i++) {
      const node = nodes[i];
      const visible = node.labels.some((l) => visibleLabels.has(l));
      let c: [number, number, number, number];
      if (colorByDegree) {
        // Hash node index for visual variety when all nodes share one label
        const hash = ((i * 2654435761) >>> 0) % PALETTE.length;
        c = PALETTE[hash];
      } else {
        c = labelColorMap.get(node.labels[0] ?? "") ?? PALETTE[0];
      }
      colors[i * 4] = c[0];
      colors[i * 4 + 1] = c[1];
      colors[i * 4 + 2] = c[2];
      colors[i * 4 + 3] = visible ? c[3] : 0;
    }
    graph.setPointColors(colors);

    // ── Point sizes (degree-scaled) ──
    const maxDeg = Math.max(1, ...Array.from(degree));
    const sizes = new Float32Array(n);
    for (let i = 0; i < n; i++) {
      const visible = nodes[i].labels.some((l) => visibleLabels.has(l));
      sizes[i] = visible ? 0.8 + (degree[i] / maxDeg) * 2 : 0;
    }
    graph.setPointSizes(sizes);

    // ── Links (index pairs) ──
    if (dataChanged) {
      const links = new Float32Array(edges.length * 2);
      for (let i = 0; i < edges.length; i++) {
        const si = nodeIdToIndex.get(edges[i].source) ?? 0;
        const ti = nodeIdToIndex.get(edges[i].target) ?? 0;
        links[i * 2] = si;
        links[i * 2 + 1] = ti;
      }
      graph.setLinks(links);
    }

    // ── Initial positions from force worker (if available) ──
    if (positions && dataChanged) {
      // cosmos expects 2D positions: [x1, y1, x2, y2, ...]
      const pos2d = new Float32Array(n * 2);
      for (let i = 0; i < n; i++) {
        pos2d[i * 2] = positions[i * 3];
        pos2d[i * 2 + 1] = positions[i * 3 + 1];
      }
      graph.setPointPositions(pos2d);
    }

    // ── Focused point (selected node) ──
    if (selectedNodeId) {
      const idx = nodeIdToIndex.get(selectedNodeId);
      if (idx != null) {
        graph.setConfigPartial({ focusedPointIndex: idx });
      }
    } else {
      graph.setConfigPartial({ focusedPointIndex: undefined });
    }

    graph.render();
  }, [nodes, edges, positions, visibleLabels, labelColorMap, degree, nodeIdToIndex, selectedNodeId]);

  return (
    <div
      ref={containerRef}
      className="h-full w-full"
      style={{ background: "#06060a" }}
    />
  );
}
