export interface GraphNode {
  id: string; // unique node ID from server
  labels: string[]; // e.g. ["Person", "Employee"]
  properties: Record<string, unknown>;
  // Layout positions (filled by force worker)
  x: number;
  y: number;
  z: number;
}

export interface GraphEdge {
  id: string; // unique edge ID
  source: string; // source node ID
  target: string; // target node ID
  type: string; // relationship type, e.g. "KNOWS"
  properties: Record<string, unknown>;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface GraphInfo {
  nodeCount: number;
  edgeCount: number;
  labelCounts: Record<string, number>; // label -> count
  relTypeCounts: Record<string, number>; // rel type -> count
}

// Force layout worker messages
export type ForceWorkerRequest = {
  type: "run";
  nodes: Array<{ id: string }>;
  edges: Array<{ source: string; target: string }>;
};

export type ForceWorkerResponse =
  | { type: "tick"; positions: Float32Array; alpha: number }
  | { type: "complete"; positions: Float32Array };
