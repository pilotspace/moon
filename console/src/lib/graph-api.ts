import { execCommand } from "@/lib/api";
import type { GraphInfo, GraphData, GraphNode, GraphEdge } from "@/types/graph";

/** Get the list of available graph names */
export async function fetchGraphList(): Promise<string[]> {
  const result = await execCommand("GRAPH.LIST", []);
  if (Array.isArray(result)) return result.map(String);
  return [];
}

/** Fetch graph metadata via GRAPH.INFO */
export async function fetchGraphInfo(): Promise<GraphInfo> {
  // Auto-detect graph name from GRAPH.LIST
  const graphs = await fetchGraphList();
  const graphName = graphs[0];
  if (!graphName) return { nodeCount: 0, edgeCount: 0, labelCounts: {}, relTypeCounts: {} };
  const result = await execCommand("GRAPH.INFO", [graphName]);
  // GRAPH.INFO may return a Map (JSON object) or flat key-value array.
  let nodeCount = 0;
  let edgeCount = 0;
  let labelCounts: Record<string, number> = {};
  let relTypeCounts: Record<string, number> = {};

  if (result && typeof result === "object" && !Array.isArray(result)) {
    // Map/object response (Moon returns RESP3 Map → JSON object)
    const obj = result as Record<string, unknown>;
    nodeCount = Number(obj.node_count ?? obj.nodes ?? 0);
    edgeCount = Number(obj.edge_count ?? obj.edges ?? 0);
    labelCounts = parseCounts(obj.label_counts ?? obj.labels);
    relTypeCounts = parseCounts(obj.rel_type_counts ?? obj.relationship_types);
  } else if (Array.isArray(result)) {
    // Flat key-value pairs
    const map = new Map<string, unknown>();
    for (let i = 0; i < result.length - 1; i += 2) {
      map.set(String(result[i]).toLowerCase(), result[i + 1]);
    }
    nodeCount = Number(map.get("node_count") ?? map.get("nodes") ?? 0);
    edgeCount = Number(map.get("edge_count") ?? map.get("edges") ?? 0);
    labelCounts = parseCounts(map.get("label_counts") ?? map.get("labels"));
    relTypeCounts = parseCounts(map.get("rel_type_counts") ?? map.get("relationship_types"));
  }

  return { nodeCount, edgeCount, labelCounts, relTypeCounts };
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
  // Auto-detect graph name from GRAPH.LIST
  const graphs = await fetchGraphList();
  const graphName = graphs[0] ?? "default";
  const result = await execCommand("GRAPH.QUERY", [graphName, cypher]);
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

  // Skip headers (result[0]) and stats (last element which is a string).
  // Moon returns: [headers, [[row1], [row2], ...], stats_string]
  // Each row is an array of cells (strings, arrays, or objects).
  const body = result.slice(1, -1);

  for (const rowsOrRow of body) {
    if (!Array.isArray(rowsOrRow)) continue;
    // Check if this is a nested array of rows or a single row
    const rows = Array.isArray(rowsOrRow[0]) ? rowsOrRow : [rowsOrRow];
    for (const row of rows) {
      if (!Array.isArray(row)) continue;
      for (const cell of row) {
        parseCell(cell, nodeMap, edgeMap);
      }
      // If row has 3 cells like [node, edge/null, node], create an edge
      if (row.length === 3) {
        const srcId = typeof row[0] === "string" ? row[0] : null;
        const dstId = typeof row[2] === "string" ? row[2] : null;
        if (srcId?.startsWith("node:") && dstId?.startsWith("node:")) {
          const edgeId = `${srcId}->${dstId}`;
          if (!edgeMap.has(edgeId)) {
            edgeMap.set(edgeId, {
              id: edgeId,
              source: srcId,
              target: dstId,
              type: row[1] != null ? String(row[1]) : "RELATES_TO",
              properties: {},
            });
          }
        }
      }
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
  // Moon returns node references as "node:XXX" strings. Extract as node.
  if (typeof cell === "string" && cell.startsWith("node:")) {
    const id = cell;
    if (!nodeMap.has(id)) {
      nodeMap.set(id, {
        id,
        labels: [],
        properties: { id: cell.replace("node:", "") },
        x: 0, y: 0, z: 0,
      });
    }
    return;
  }
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
