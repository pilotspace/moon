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
  // Server key is "dimension" (singular), not "dimensions".
  const efRaw = map.get("ef_runtime");
  const efValue = efRaw === "auto" || efRaw == null ? 0 : Number(efRaw);
  return {
    name: indexName,
    dimensions: Number(map.get("dimension") ?? map.get("dimensions") ?? 0),
    metric: String(map.get("distance_metric") ?? "COSINE"),
    num_docs: Number(map.get("num_docs") ?? 0),
    segments: parseSegments(map.get("segments")),
    ef_runtime: Number.isNaN(efValue) ? 0 : efValue,
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

/** Decode a base64-encoded binary blob into a Float32Array. */
function decodeBase64Vector(b64: string): Float32Array {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return new Float32Array(bytes.buffer);
}

/** Fetch up to `limit` vectors from an index via SCAN + HGETALL.
 *
 * Moon's FT.SEARCH is KNN-only (no `"*"` match-all). So we SCAN for
 * keys matching the index prefix and HGETALL each one. Binary vector
 * blobs are returned as `{"base64": "..."}` by the JSON bridge —
 * decoded client-side to Float32Array.
 */
export async function fetchVectorData(
  indexName: string,
  limit: number = 10000,
): Promise<VectorPoint[]> {
  // Get the index prefix from FT.INFO
  const info = await execCommand("FT.INFO", [indexName]);
  let prefix = "doc:"; // default
  if (Array.isArray(info)) {
    for (let i = 0; i < info.length - 1; i += 2) {
      if (String(info[i]).toLowerCase() === "index_definition") {
        const def = info[i + 1];
        if (Array.isArray(def)) {
          for (let j = 0; j < def.length - 1; j += 2) {
            if (String(def[j]).toLowerCase() === "prefix" || String(def[j]).toLowerCase() === "prefixes") {
              prefix = String(def[j + 1]);
              break;
            }
          }
        }
      }
    }
  }

  // SCAN for keys matching the prefix
  const points: VectorPoint[] = [];
  let cursor = "0";
  do {
    const scanResult = await execCommand("SCAN", [cursor, "MATCH", `${prefix}*`, "COUNT", "200"]);
    if (!Array.isArray(scanResult) || scanResult.length < 2) break;
    cursor = String(scanResult[0]);
    const keys = scanResult[1] as string[];

    for (const key of keys) {
      if (points.length >= limit) { cursor = "0"; break; }
      const hgetall = await execCommand("HGETALL", [String(key)]);
      if (!Array.isArray(hgetall)) continue;

      const fieldMap = new Map<string, unknown>();
      for (let j = 0; j < hgetall.length - 1; j += 2) {
        fieldMap.set(String(hgetall[j]), hgetall[j + 1]);
      }

      // Find vector field — check for base64 blob or CSV string
      let vector = new Float32Array(0);
      const vectorFieldNames = ["v", "vector", "embedding", "emb", "__vector"];
      for (const fname of vectorFieldNames) {
        const val = fieldMap.get(fname);
        if (!val) continue;

        if (typeof val === "object" && val !== null && "base64" in val) {
          // Binary blob encoded as base64 by the JSON bridge
          vector = decodeBase64Vector((val as { base64: string }).base64);
          fieldMap.delete(fname);
          break;
        } else if (typeof val === "string" && val.includes(",")) {
          // CSV float string
          const nums = val.split(",").map(Number).filter((n) => !isNaN(n));
          if (nums.length > 1) {
            vector = new Float32Array(nums);
            fieldMap.delete(fname);
            break;
          }
        }
      }

      const payload: Record<string, string> = {};
      for (const [k, v] of fieldMap) {
        payload[k] = typeof v === "string" ? v : JSON.stringify(v);
      }

      points.push({
        id: `${points.length}`,
        key: String(key),
        vector,
        payload,
        segment: "mutable",
      });
    }
  } while (cursor !== "0");

  return points;
}
