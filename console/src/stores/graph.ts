import { create } from "zustand";
import type {
  GraphNode,
  GraphEdge,
  GraphInfo,
  ForceWorkerResponse,
} from "@/types/graph";
import { fetchGraphInfo, queryGraph } from "@/lib/graph-api";

interface GraphState {
  // Metadata
  graphInfo: GraphInfo | null;

  // Graph data
  nodes: GraphNode[];
  edges: GraphEdge[];
  positions: Float32Array | null;

  // Layout progress
  layoutProgress: { alpha: number } | null;

  // Loading
  loading: boolean;

  // Interaction
  selectedNodeId: string | null;
  hoveredNodeId: string | null;

  // Filtering
  visibleLabels: Set<string>;
  visibleRelTypes: Set<string>;

  // Cypher
  cypherInput: string;
  cypherError: string | null;

  // Actions
  loadGraphInfo: () => Promise<void>;
  runQuery: (cypher: string) => Promise<void>;
  runForceLayout: (nodes: GraphNode[], edges: GraphEdge[]) => void;
  setCypherInput: (input: string) => void;
  selectNode: (id: string | null) => void;
  setHoveredNode: (id: string | null) => void;
  toggleLabel: (label: string) => void;
  toggleRelType: (relType: string) => void;
  setAllLabelsVisible: (visible: boolean) => void;
  setAllRelTypesVisible: (visible: boolean) => void;
}

let forceWorker: Worker | null = null;

export const useGraphStore = create<GraphState>((set, get) => ({
  graphInfo: null,
  nodes: [],
  edges: [],
  positions: null,
  layoutProgress: null,
  loading: false,
  selectedNodeId: null,
  hoveredNodeId: null,
  visibleLabels: new Set<string>(),
  visibleRelTypes: new Set<string>(),
  cypherInput: "MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 200",
  cypherError: null,

  loadGraphInfo: async () => {
    try {
      const info = await fetchGraphInfo();
      set({
        graphInfo: info,
        visibleLabels: new Set(Object.keys(info.labelCounts)),
        visibleRelTypes: new Set(Object.keys(info.relTypeCounts)),
      });
    } catch {
      set({ graphInfo: null });
    }
  },

  runQuery: async (cypher: string) => {
    set({
      loading: true,
      cypherError: null,
      selectedNodeId: null,
      positions: null,
    });
    try {
      const data = await queryGraph(cypher);
      if (data.nodes.length === 0) {
        set({
          nodes: [],
          edges: [],
          loading: false,
          cypherError: "Query returned no nodes",
        });
        return;
      }

      // Collect all labels and rel types from results
      const labels = new Set<string>();
      const relTypes = new Set<string>();
      for (const n of data.nodes) n.labels.forEach((l) => labels.add(l));
      for (const e of data.edges) relTypes.add(e.type);

      set({
        nodes: data.nodes,
        edges: data.edges,
        visibleLabels: labels,
        visibleRelTypes: relTypes,
        loading: false,
      });

      // Start force layout
      get().runForceLayout(data.nodes, data.edges);
    } catch (err) {
      set({ loading: false, cypherError: String(err) });
    }
  },

  runForceLayout: (nodes: GraphNode[], edges: GraphEdge[]) => {
    if (forceWorker) {
      forceWorker.terminate();
      forceWorker = null;
    }
    set({ layoutProgress: { alpha: 1.0 }, positions: null });

    forceWorker = new Worker(
      new URL("../workers/force-layout.worker.ts", import.meta.url),
      { type: "module" },
    );

    forceWorker.onmessage = (e: MessageEvent<ForceWorkerResponse>) => {
      const msg = e.data;
      if (msg.type === "tick") {
        set({ positions: msg.positions, layoutProgress: { alpha: msg.alpha } });
      } else if (msg.type === "complete") {
        set({ positions: msg.positions, layoutProgress: null });
        if (forceWorker) {
          forceWorker.terminate();
          forceWorker = null;
        }
      }
    };

    forceWorker.postMessage({
      type: "run",
      nodes: nodes.map((n) => ({ id: n.id })),
      edges: edges.map((e) => ({ source: e.source, target: e.target })),
    });
  },

  setCypherInput: (input) => set({ cypherInput: input }),
  selectNode: (id) => set({ selectedNodeId: id }),
  setHoveredNode: (id) => set({ hoveredNodeId: id }),

  toggleLabel: (label) => {
    const prev = get().visibleLabels;
    const next = new Set(prev);
    if (next.has(label)) next.delete(label);
    else next.add(label);
    set({ visibleLabels: next });
  },

  toggleRelType: (relType) => {
    const prev = get().visibleRelTypes;
    const next = new Set(prev);
    if (next.has(relType)) next.delete(relType);
    else next.add(relType);
    set({ visibleRelTypes: next });
  },

  setAllLabelsVisible: (visible) => {
    if (visible) {
      const labels = new Set<string>();
      get().nodes.forEach((n) => n.labels.forEach((l) => labels.add(l)));
      set({ visibleLabels: labels });
    } else {
      set({ visibleLabels: new Set() });
    }
  },

  setAllRelTypesVisible: (visible) => {
    if (visible) {
      const types = new Set<string>();
      get().edges.forEach((e) => types.add(e.type));
      set({ visibleRelTypes: types });
    } else {
      set({ visibleRelTypes: new Set() });
    }
  },
}));
