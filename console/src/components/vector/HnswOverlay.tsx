import { useMemo } from "react";
import * as THREE from "three";
import { useVectorStore } from "@/stores/vector";

export function HnswOverlay() {
  const showHnsw = useVectorStore((s) => s.showHnsw);
  const hnswEdges = useVectorStore((s) => s.hnswEdges);
  const knnResults = useVectorStore((s) => s.knnResults);
  const projectedPositions = useVectorStore((s) => s.projectedPositions);

  const edgeGeometry = useMemo(() => {
    if (!hnswEdges || hnswEdges.length === 0) return null;
    const geo = new THREE.BufferGeometry();
    geo.setAttribute("position", new THREE.BufferAttribute(hnswEdges, 3));
    return geo;
  }, [hnswEdges]);

  // Build search path edges from KNN results (connect consecutive KNN result points)
  const searchPathGeometry = useMemo(() => {
    if (knnResults.length < 2 || !projectedPositions) return null;

    const validResults = knnResults.filter((r) => r.pointIndex >= 0);
    if (validResults.length < 2) return null;

    // Connect KNN results sequentially (by distance order) as a search path visualization
    const pathVerts = new Float32Array((validResults.length - 1) * 6);
    let offset = 0;
    for (let i = 0; i < validResults.length - 1; i++) {
      const a = validResults[i].pointIndex;
      const b = validResults[i + 1].pointIndex;
      pathVerts[offset++] = projectedPositions[a * 3];
      pathVerts[offset++] = projectedPositions[a * 3 + 1];
      pathVerts[offset++] = projectedPositions[a * 3 + 2];
      pathVerts[offset++] = projectedPositions[b * 3];
      pathVerts[offset++] = projectedPositions[b * 3 + 1];
      pathVerts[offset++] = projectedPositions[b * 3 + 2];
    }

    const geo = new THREE.BufferGeometry();
    geo.setAttribute("position", new THREE.BufferAttribute(pathVerts, 3));
    return geo;
  }, [knnResults, projectedPositions]);

  if (!showHnsw) return null;

  return (
    <group>
      {/* HNSW layer edges */}
      {edgeGeometry && (
        <lineSegments geometry={edgeGeometry}>
          <lineBasicMaterial
            color={0x06b6d4}
            transparent
            opacity={0.15}
            depthWrite={false}
          />
        </lineSegments>
      )}

      {/* Search path highlight */}
      {searchPathGeometry && (
        <lineSegments geometry={searchPathGeometry}>
          <lineBasicMaterial
            color={0x22d3ee}
            transparent
            opacity={0.8}
            linewidth={2}
            depthWrite={false}
          />
        </lineSegments>
      )}
    </group>
  );
}
