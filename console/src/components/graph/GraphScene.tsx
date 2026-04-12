import { useRef, useMemo, useCallback } from "react";
import { Canvas, useFrame, ThreeEvent } from "@react-three/fiber";
import { OrbitControls } from "@react-three/drei";
import * as THREE from "three";
import { useGraphStore } from "@/stores/graph";

// Categorical palette (same 8 colors as vector explorer)
const PALETTE = [
  0x6366f1, 0xf97316, 0x22c55e, 0xef4444, 0x06b6d4, 0xa855f7, 0xeab308,
  0xec4899,
];
const HIGHLIGHT_COLOR = new THREE.Color(0xfbbf24);
const EDGE_COLOR = new THREE.Color(0x3f3f46); // zinc-700
const EDGE_HIGHLIGHT = new THREE.Color(0x6366f1);

const NODE_RADIUS = 0.8;
const tempObject = new THREE.Object3D();
const tempColor = new THREE.Color();

function GraphNodes() {
  const meshRef = useRef<THREE.InstancedMesh>(null);
  const nodes = useGraphStore((s) => s.nodes);
  const positions = useGraphStore((s) => s.positions);
  const visibleLabels = useGraphStore((s) => s.visibleLabels);
  const selectedNodeId = useGraphStore((s) => s.selectedNodeId);
  const hoveredNodeId = useGraphStore((s) => s.hoveredNodeId);
  const selectNode = useGraphStore((s) => s.selectNode);

  // Build label-to-color map
  const labelColorMap = useMemo(() => {
    const allLabels = new Set<string>();
    nodes.forEach((n) => n.labels.forEach((l) => allLabels.add(l)));
    const map = new Map<string, number>();
    let idx = 0;
    for (const label of allLabels) {
      map.set(label, PALETTE[idx % PALETTE.length]);
      idx++;
    }
    return map;
  }, [nodes]);

  // Compute visible node indices
  const visibleIndices = useMemo(() => {
    const indices: number[] = [];
    for (let i = 0; i < nodes.length; i++) {
      if (nodes[i].labels.some((l) => visibleLabels.has(l))) {
        indices.push(i);
      }
    }
    return indices;
  }, [nodes, visibleLabels]);

  // Update instance matrices and colors each frame
  useFrame(() => {
    if (!meshRef.current || !positions || positions.length === 0) return;
    const mesh = meshRef.current;

    for (let vi = 0; vi < visibleIndices.length; vi++) {
      const i = visibleIndices[vi];
      const x = positions[i * 3];
      const y = positions[i * 3 + 1];
      const z = positions[i * 3 + 2];

      const isSelected = nodes[i].id === selectedNodeId;
      const isHovered = nodes[i].id === hoveredNodeId;
      const scale = isSelected ? 1.6 : isHovered ? 1.3 : 1.0;

      tempObject.position.set(x, y, z);
      tempObject.scale.setScalar(NODE_RADIUS * scale);
      tempObject.updateMatrix();
      mesh.setMatrixAt(vi, tempObject.matrix);

      if (isSelected || isHovered) {
        mesh.setColorAt(vi, HIGHLIGHT_COLOR);
      } else {
        const label = nodes[i].labels[0] ?? "";
        const hex = labelColorMap.get(label) ?? PALETTE[0];
        tempColor.setHex(hex);
        mesh.setColorAt(vi, tempColor);
      }
    }

    mesh.instanceMatrix.needsUpdate = true;
    if (mesh.instanceColor) mesh.instanceColor.needsUpdate = true;
    mesh.count = visibleIndices.length;
  });

  const handleClick = useCallback(
    (e: ThreeEvent<MouseEvent>) => {
      e.stopPropagation();
      const instanceId = e.instanceId;
      if (instanceId != null && instanceId < visibleIndices.length) {
        const nodeIdx = visibleIndices[instanceId];
        selectNode(nodes[nodeIdx].id);
      }
    },
    [visibleIndices, nodes, selectNode],
  );

  if (nodes.length === 0) return null;

  return (
    <instancedMesh
      ref={meshRef}
      args={[undefined, undefined, nodes.length]}
      onClick={handleClick}
    >
      <sphereGeometry args={[1, 16, 12]} />
      <meshStandardMaterial roughness={0.6} metalness={0.1} />
    </instancedMesh>
  );
}

function GraphEdges() {
  const lineRef = useRef<THREE.LineSegments>(null);
  const nodes = useGraphStore((s) => s.nodes);
  const edges = useGraphStore((s) => s.edges);
  const positions = useGraphStore((s) => s.positions);
  const visibleLabels = useGraphStore((s) => s.visibleLabels);
  const visibleRelTypes = useGraphStore((s) => s.visibleRelTypes);
  const selectedNodeId = useGraphStore((s) => s.selectedNodeId);

  const nodeIdToIndex = useMemo(() => {
    const map = new Map<string, number>();
    nodes.forEach((n, i) => map.set(n.id, i));
    return map;
  }, [nodes]);

  // Filter visible edges
  const visibleEdges = useMemo(() => {
    return edges.filter((e) => {
      if (!visibleRelTypes.has(e.type)) return false;
      const si = nodeIdToIndex.get(e.source);
      const ti = nodeIdToIndex.get(e.target);
      if (si == null || ti == null) return false;
      const srcVisible = nodes[si].labels.some((l) => visibleLabels.has(l));
      const tgtVisible = nodes[ti].labels.some((l) => visibleLabels.has(l));
      return srcVisible && tgtVisible;
    });
  }, [edges, visibleRelTypes, visibleLabels, nodes, nodeIdToIndex]);

  useFrame(() => {
    if (!lineRef.current || !positions || positions.length === 0) return;
    const geo = lineRef.current.geometry;

    const posArr = new Float32Array(visibleEdges.length * 6);
    const colArr = new Float32Array(visibleEdges.length * 6);

    for (let i = 0; i < visibleEdges.length; i++) {
      const edge = visibleEdges[i];
      const si = nodeIdToIndex.get(edge.source);
      const ti = nodeIdToIndex.get(edge.target);
      if (si == null || ti == null) continue;

      posArr[i * 6] = positions[si * 3];
      posArr[i * 6 + 1] = positions[si * 3 + 1];
      posArr[i * 6 + 2] = positions[si * 3 + 2];
      posArr[i * 6 + 3] = positions[ti * 3];
      posArr[i * 6 + 4] = positions[ti * 3 + 1];
      posArr[i * 6 + 5] = positions[ti * 3 + 2];

      const isConnected =
        edge.source === selectedNodeId || edge.target === selectedNodeId;
      const c = isConnected ? EDGE_HIGHLIGHT : EDGE_COLOR;
      colArr[i * 6] = c.r;
      colArr[i * 6 + 1] = c.g;
      colArr[i * 6 + 2] = c.b;
      colArr[i * 6 + 3] = c.r;
      colArr[i * 6 + 4] = c.g;
      colArr[i * 6 + 5] = c.b;
    }

    geo.setAttribute("position", new THREE.BufferAttribute(posArr, 3));
    geo.setAttribute("color", new THREE.BufferAttribute(colArr, 3));
    geo.computeBoundingSphere();
  });

  return (
    <lineSegments ref={lineRef}>
      <bufferGeometry />
      <lineBasicMaterial vertexColors transparent opacity={0.4} />
    </lineSegments>
  );
}

function SceneContent() {
  const selectNode = useGraphStore((s) => s.selectNode);
  return (
    <>
      <ambientLight intensity={0.6} />
      <directionalLight position={[10, 10, 5]} intensity={0.4} />
      <GraphNodes />
      <GraphEdges />
      <OrbitControls
        enableDamping
        dampingFactor={0.1}
        rotateSpeed={0.8}
        zoomSpeed={1.2}
      />
      {/* Click on empty space deselects */}
      <mesh visible={false} onClick={() => selectNode(null)}>
        <sphereGeometry args={[1000]} />
      </mesh>
    </>
  );
}

export function GraphScene() {
  return (
    <Canvas
      camera={{ position: [0, 0, 80], fov: 60, near: 0.1, far: 2000 }}
      style={{ background: "#0a0a0f" }}
      gl={{ antialias: true, alpha: false }}
    >
      <SceneContent />
    </Canvas>
  );
}
