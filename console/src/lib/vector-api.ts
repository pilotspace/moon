import { execCommand } from "@/lib/api";
import type { VectorIndex, VectorPoint } from "@/types/vector";

export async function fetchIndexList(): Promise<string[]> {
  const result = await execCommand("FT._LIST", []);
  if (Array.isArray(result)) return result.map(String);
  return [];
}

export async function fetchIndexInfo(
  indexName: string,
): Promise<VectorIndex> {
  const result = await execCommand("FT.INFO", [indexName]);
  // FT.INFO returns alternating key-value pairs as flat array
  const map = new Map<string, unknown>();
  if (Array.isArray(result)) {
    for (let i = 0; i < result.length - 1; i += 2) {
      map.set(String(result[i]).toLowerCase(), result[i + 1]);
    }
  }
  return {
    name: indexName,
    dimensions: Number(map.get("dimensions") ?? 0),
    metric: String(map.get("distance_metric") ?? "COSINE"),
    num_docs: Number(map.get("num_docs") ?? 0),
    segments: parseSegments(map.get("segments")),
    ef_runtime: Number(map.get("ef_runtime") ?? 200),
    compact_threshold: Number(map.get("compact_threshold") ?? 10000),
  };
}

function parseSegments(raw: unknown): VectorIndex["segments"] {
  if (!Array.isArray(raw)) return [];
  return raw.map((seg) => {
    const sm = new Map<string, unknown>();
    if (Array.isArray(seg)) {
      for (let i = 0; i < seg.length - 1; i += 2) {
        sm.set(String(seg[i]).toLowerCase(), seg[i + 1]);
      }
    }
    return {
      type: String(sm.get("type") ?? "mutable") as "mutable" | "immutable",
      num_vectors: Number(sm.get("num_vectors") ?? 0),
      algorithm: String(sm.get("algorithm") ?? "flat"),
    };
  });
}

export interface KnnResult {
  key: string;
  score: number;
  pointIndex: number;
}

export async function searchKnn(
  indexName: string,
  queryVector: number[],
  k: number = 10,
): Promise<KnnResult[]> {
  const vectorStr = queryVector.join(",");
  const result = await execCommand("FT.SEARCH", [
    indexName,
    `*=>[KNN ${k} @vector $BLOB]`,
    "PARAMS", "2", "BLOB", vectorStr,
    "SORTBY", "__vector_score",
    "LIMIT", "0", String(k),
  ]);
  if (!Array.isArray(result) || result.length < 2) return [];
  const results: KnnResult[] = [];
  for (let i = 1; i < result.length; i += 2) {
    const key = String(result[i]);
    const fields = result[i + 1];
    let score = 0;
    if (Array.isArray(fields)) {
      for (let j = 0; j < fields.length - 1; j += 2) {
        if (String(fields[j]) === "__vector_score") {
          score = Number(fields[j + 1]);
        }
      }
    }
    results.push({ key, score, pointIndex: -1 });
  }
  return results;
}

/** Fetch up to `limit` vectors from an index via FT.SEARCH */
export async function fetchVectorData(
  indexName: string,
  limit: number = 10000,
): Promise<VectorPoint[]> {
  // FT.SEARCH idx "*" LIMIT 0 <limit> returns [count, key1, [field, val, ...], key2, ...]
  const result = await execCommand("FT.SEARCH", [
    indexName,
    "*",
    "LIMIT",
    "0",
    String(limit),
  ]);
  if (!Array.isArray(result) || result.length < 1) return [];

  const points: VectorPoint[] = [];
  // result[0] = total count, then pairs of (key, fields[])
  for (let i = 1; i < result.length; i += 2) {
    const key = String(result[i]);
    const fields = result[i + 1];
    if (!Array.isArray(fields)) continue;

    const fieldMap = new Map<string, string>();
    for (let j = 0; j < fields.length - 1; j += 2) {
      fieldMap.set(String(fields[j]), String(fields[j + 1]));
    }

    // Find the vector field (typically "__vector" or "vector")
    let vector = new Float32Array(0);
    for (const [k, v] of fieldMap) {
      if (k.includes("vector") || k.startsWith("__")) {
        const nums = v
          .split(",")
          .map(Number)
          .filter((n) => !isNaN(n));
        if (nums.length > 1) {
          vector = new Float32Array(nums);
          fieldMap.delete(k);
          break;
        }
      }
    }

    const payload: Record<string, string> = {};
    for (const [k, v] of fieldMap) payload[k] = v;

    points.push({
      id: `${points.length}`,
      key,
      vector,
      payload,
      segment: "mutable",
    });
  }
  return points;
}
