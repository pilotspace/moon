import { useRef, useMemo, useCallback } from "react";
import { Canvas, useThree, useFrame } from "@react-three/fiber";
import { OrbitControls } from "@react-three/drei";
import * as THREE from "three";
import { useVectorStore } from "@/stores/vector";
import { HnswOverlay } from "./HnswOverlay";

// Unified desaturated palette — readable on #06060a
const PALETTE = [
  new THREE.Color(0x6b93d4), // blue
  new THREE.Color(0xcc8a5b), // orange
  new THREE.Color(0x62b87a), // green
  new THREE.Color(0xd4807a), // rose
  new THREE.Color(0x5bbdad), // teal
  new THREE.Color(0xa88bd4), // violet
  new THREE.Color(0xc4a55a), // amber
  new THREE.Color(0x8e92d4), // lavender
];

const CYAN = new THREE.Color(0x5bbdad);
const HIGHLIGHT = new THREE.Color(0xd4b96e);

const vertexShader = /* glsl */ `
  attribute vec3 aColor;
  attribute float aSize;
  varying vec3 vColor;
  void main() {
    vColor = aColor;
    vec4 mvPosition = modelViewMatrix * vec4(position, 1.0);
    gl_PointSize = aSize * (300.0 / -mvPosition.z);
    gl_Position = projectionMatrix * mvPosition;
  }
`;

const fragmentShader = /* glsl */ `
  varying vec3 vColor;
  void main() {
    float d = length(gl_PointCoord - vec2(0.5));
    if (d > 0.5) discard;
    float alpha = 1.0 - smoothstep(0.3, 0.5, d);
    gl_FragColor = vec4(vColor, alpha * 0.85);
  }
`;

function PointCloud() {
  const pointsRef = useRef<THREE.Points>(null);
  const raycasterRef = useRef(new THREE.Raycaster());
  const positions = useVectorStore((s) => s.projectedPositions);
  const points = useVectorStore((s) => s.points);
  const colorBy = useVectorStore((s) => s.colorBy);
  const hoveredPointId = useVectorStore((s) => s.hoveredPointId);
  const knnResults = useVectorStore((s) => s.knnResults);
  const lassoSelectedIds = useVectorStore((s) => s.lassoSelectedIds);
  const setHovered = useVectorStore((s) => s.setHovered);

  const { camera } = useThree();

  // Set raycaster threshold for points
  raycasterRef.current.params.Points = { threshold: 0.3 };

  const geometry = useMemo(() => {
    if (!positions || positions.length === 0) return null;
    const count = positions.length / 3;
    const geo = new THREE.BufferGeometry();
    geo.setAttribute("position", new THREE.BufferAttribute(positions, 3));

    // Build set of KNN point indices for fast lookup
    const knnIndexSet = new Set<number>();
    for (const r of knnResults) {
      if (r.pointIndex >= 0) knnIndexSet.add(r.pointIndex);
    }

    // Colors based on colorBy mode
    const colors = new Float32Array(count * 3);
    const sizes = new Float32Array(count);
    for (let i = 0; i < count; i++) {
      let color = PALETTE[0];
      let size = 3.0;

      // Base color by mode
      if (colorBy === "segment" && points[i]) {
        color = points[i].segment === "immutable" ? PALETTE[0] : PALETTE[1];
      } else if (colorBy === "label" && points[i]) {
        const labelVal =
          points[i].payload?.["label"] ??
          points[i].payload?.["category"] ??
          "";
        const hash = [...labelVal].reduce((a, c) => a + c.charCodeAt(0), 0);
        color = PALETTE[hash % PALETTE.length];
      }

      // Highlight overrides
      if (points[i] && points[i].id === hoveredPointId) {
        color = HIGHLIGHT;
        size = 8.0;
      } else if (knnIndexSet.has(i)) {
        color = CYAN;
        size = 6.0;
      } else if (points[i] && lassoSelectedIds.has(points[i].id)) {
        color = HIGHLIGHT;
        size = 5.0;
      }

      colors[i * 3] = color.r;
      colors[i * 3 + 1] = color.g;
      colors[i * 3 + 2] = color.b;
      sizes[i] = size;
    }
    geo.setAttribute("aColor", new THREE.BufferAttribute(colors, 3));
    geo.setAttribute("aSize", new THREE.BufferAttribute(sizes, 1));
    return geo;
  }, [positions, points, colorBy, hoveredPointId, knnResults, lassoSelectedIds]);

  const handleClick = useCallback(
    (e: THREE.Event & { pointer: THREE.Vector2 }) => {
      if (!pointsRef.current || !positions) return;
      const pointer = e.pointer ?? new THREE.Vector2();
      raycasterRef.current.setFromCamera(pointer, camera);
      const intersects = raycasterRef.current.intersectObject(pointsRef.current);
      if (intersects.length > 0 && intersects[0].index != null) {
        const idx = intersects[0].index;
        if (points[idx]) {
          setHovered(points[idx].id);
        }
      }
    },
    [camera, positions, points, setHovered],
  );

  // Keep raycaster threshold updated
  useFrame(() => {
    raycasterRef.current.params.Points = { threshold: 0.3 };
  });

  if (!geometry) return null;

  return (
    <points ref={pointsRef} geometry={geometry} onClick={handleClick}>
      <shaderMaterial
        vertexShader={vertexShader}
        fragmentShader={fragmentShader}
        transparent
        depthWrite={false}
        blending={THREE.AdditiveBlending}
      />
    </points>
  );
}

export interface PointCloudSceneHandle {
  getCamera: () => THREE.Camera | null;
  getCanvasRect: () => DOMRect | null;
}

function SceneContent({ onCameraReady }: { onCameraReady?: (camera: THREE.Camera) => void }) {
  const { camera } = useThree();

  useFrame(() => {
    if (onCameraReady) onCameraReady(camera);
  });

  return (
    <>
      <ambientLight intensity={0.5} />
      <PointCloud />
      <HnswOverlay />
      <OrbitControls
        enableDamping
        dampingFactor={0.1}
        rotateSpeed={0.8}
        zoomSpeed={1.2}
      />
    </>
  );
}

export function PointCloudScene({
  onCameraReady,
}: {
  onCameraReady?: (camera: THREE.Camera) => void;
}) {
  return (
    <Canvas
      camera={{ position: [0, 0, 20], fov: 60, near: 0.1, far: 1000 }}
      style={{ background: "#0a0a0f" }}
      gl={{ antialias: true, alpha: false }}
    >
      <SceneContent onCameraReady={onCameraReady} />
    </Canvas>
  );
}
