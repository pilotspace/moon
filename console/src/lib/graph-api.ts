import { execCommand } from "@/lib/api";
import type { GraphInfo, GraphData, GraphNode, GraphEdge } from "@/types/graph";

/** Fetch graph metadata via GRAPH.INFO */
export async function fetchGraphInfo(): Promise<GraphInfo> {
  const result = await execCommand("GRAPH.INFO", []);
  // GRAPH.INFO returns flat key-value pairs
  const map = new Map<string, unknown>();
  if (Array.isArray(result)) {
    for (let i = 0; i < result.length - 1; i += 2) {
      map.set(String(result[i]).toLowerCase(), result[i + 1]);
    }
  }
  return {
    nodeCount: Number(map.get("node_count") ?? map.get("nodes") ?? 0),
    edgeCount: Number(map.get("edge_count") ?? map.get("edges") ?? 0),
    labelCounts: parseCounts(map.get("label_counts") ?? map.get("labels")),
    relTypeCounts: parseCounts(
      map.get("rel_type_counts") ?? map.get("relationship_types"),
    ),
  };
}

function parseCounts(raw: unknown): Record<string, number> {
  const counts: Record<string, number> = {};
  if (Array.isArray(raw)) {
    for (let i = 0; i < raw.length - 1; i += 2) {
      counts[String(raw[i])] = Number(raw[i + 1]);
    }
  } else if (raw && typeof raw === "object") {
    for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
      counts[k] = Number(v);
    }
  }
  return counts;
}

/** Execute a Cypher query and parse results into GraphData */
export async function queryGraph(cypher: string): Promise<GraphData> {
  const result = await execCommand("GRAPH.QUERY", [cypher]);
  return parseGraphResult(result);
}

/**
 * Parse GRAPH.QUERY result into nodes and edges.
 * GRAPH.QUERY returns: [headers[], row1[], row2[], ..., stats[]]
 * Each row cell can be a node [id, labels[], properties{}]
 * or edge [id, type, srcId, dstId, properties{}]
 */
function parseGraphResult(result: unknown): GraphData {
  const nodeMap = new Map<string, GraphNode>();
  const edgeMap = new Map<string, GraphEdge>();

  if (!Array.isArray(result) || result.length < 2)
    return { nodes: [], edges: [] };

  // Skip headers (result[0]) and stats (last element)
  const rows = result.slice(1, -1);

  for (const row of rows) {
    if (!Array.isArray(row)) continue;
    for (const cell of row) {
      if (!Array.isArray(cell) && typeof cell !== "object") continue;
      parseCell(cell, nodeMap, edgeMap);
    }
  }

  return {
    nodes: Array.from(nodeMap.values()),
    edges: Array.from(edgeMap.values()),
  };
}

function parseCell(
  cell: unknown,
  nodeMap: Map<string, GraphNode>,
  edgeMap: Map<string, GraphEdge>,
): void {
  if (!cell || typeof cell !== "object") return;

  // Array-encoded node: [id, labels[], properties{}]
  if (Array.isArray(cell)) {
    if (cell.length >= 3 && Array.isArray(cell[1])) {
      // Looks like a node
      const id = String(cell[0]);
      if (!nodeMap.has(id)) {
        nodeMap.set(id, {
          id,
          labels: (cell[1] as unknown[]).map(String),
          properties: (cell[2] as Record<string, unknown>) ?? {},
          x: 0,
          y: 0,
          z: 0,
        });
      }
    } else if (cell.length >= 5) {
      // Looks like an edge: [id, type, srcId, dstId, properties]
      const id = String(cell[0]);
      if (!edgeMap.has(id)) {
        const srcId = String(cell[2]);
        const dstId = String(cell[3]);
        edgeMap.set(id, {
          id,
          source: srcId,
          target: dstId,
          type: String(cell[1]),
          properties: (cell[4] as Record<string, unknown>) ?? {},
        });
      }
    }
    return;
  }

  // Object-encoded node/edge (JSON response format)
  const obj = cell as Record<string, unknown>;
  if ("labels" in obj) {
    const id = String(obj.id ?? obj.node_id ?? "");
    if (id && !nodeMap.has(id)) {
      nodeMap.set(id, {
        id,
        labels: Array.isArray(obj.labels) ? obj.labels.map(String) : [],
        properties: (obj.properties as Record<string, unknown>) ?? {},
        x: 0,
        y: 0,
        z: 0,
      });
    }
  } else if ("type" in obj && ("source" in obj || "src" in obj)) {
    const id = String(obj.id ?? obj.edge_id ?? "");
    if (id && !edgeMap.has(id)) {
      edgeMap.set(id, {
        id,
        source: String(obj.source ?? obj.src ?? ""),
        target: String(obj.target ?? obj.dst ?? obj.dest ?? ""),
        type: String(obj.type ?? ""),
        properties: (obj.properties as Record<string, unknown>) ?? {},
      });
    }
  }
}
