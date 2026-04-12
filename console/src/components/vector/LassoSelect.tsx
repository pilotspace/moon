import { useRef, useState, useCallback, useEffect } from "react";
import * as THREE from "three";
import { useVectorStore } from "@/stores/vector";

interface LassoSelectProps {
  active: boolean;
  camera: THREE.Camera | null;
  canvasRect: DOMRect | null;
}

function pointInPolygon(x: number, y: number, polygon: Array<[number, number]>): boolean {
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const xi = polygon[i][0], yi = polygon[i][1];
    const xj = polygon[j][0], yj = polygon[j][1];
    if ((yi > y) !== (yj > y) && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) {
      inside = !inside;
    }
  }
  return inside;
}

export function LassoSelect({ active, camera, canvasRect }: LassoSelectProps) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [drawing, setDrawing] = useState(false);
  const [lassoPath, setLassoPath] = useState<Array<[number, number]>>([]);
  const points = useVectorStore((s) => s.points);
  const projectedPositions = useVectorStore((s) => s.projectedPositions);
  const setLassoSelected = useVectorStore((s) => s.setLassoSelected);

  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      if (!active) return;
      e.preventDefault();
      e.stopPropagation();
      setDrawing(true);
      const rect = svgRef.current?.getBoundingClientRect();
      if (!rect) return;
      setLassoPath([[e.clientX - rect.left, e.clientY - rect.top]]);
    },
    [active],
  );

  const handleMouseMove = useCallback(
    (e: React.MouseEvent) => {
      if (!drawing || !active) return;
      const rect = svgRef.current?.getBoundingClientRect();
      if (!rect) return;
      setLassoPath((prev) => [...prev, [e.clientX - rect.left, e.clientY - rect.top]]);
    },
    [drawing, active],
  );

  const handleMouseUp = useCallback(() => {
    if (!drawing || !active) return;
    setDrawing(false);

    if (lassoPath.length < 3 || !camera || !canvasRect || !projectedPositions) {
      setLassoPath([]);
      return;
    }

    // Project 3D points to screen space and test against lasso polygon
    const selectedIds = new Set<string>();
    const count = projectedPositions.length / 3;
    const vec = new THREE.Vector3();

    for (let i = 0; i < count; i++) {
      vec.set(
        projectedPositions[i * 3],
        projectedPositions[i * 3 + 1],
        projectedPositions[i * 3 + 2],
      );
      vec.project(camera);

      const screenX = (vec.x * 0.5 + 0.5) * canvasRect.width;
      const screenY = (-vec.y * 0.5 + 0.5) * canvasRect.height;

      if (pointInPolygon(screenX, screenY, lassoPath) && points[i]) {
        selectedIds.add(points[i].id);
      }
    }

    setLassoSelected(selectedIds);
    setLassoPath([]);
  }, [drawing, active, lassoPath, camera, canvasRect, projectedPositions, points, setLassoSelected]);

  // Clear selection on escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setLassoSelected(new Set());
        setLassoPath([]);
        setDrawing(false);
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [setLassoSelected]);

  if (!active) return null;

  const pathStr = lassoPath.map(([x, y]) => `${x},${y}`).join(" ");

  return (
    <svg
      ref={svgRef}
      className="absolute inset-0 z-20 cursor-crosshair"
      style={{ pointerEvents: active ? "auto" : "none" }}
      onMouseDown={handleMouseDown}
      onMouseMove={handleMouseMove}
      onMouseUp={handleMouseUp}
    >
      {lassoPath.length > 1 && (
        <polygon
          points={pathStr}
          fill="rgba(6, 182, 212, 0.1)"
          stroke="rgba(6, 182, 212, 0.6)"
          strokeWidth={1.5}
          strokeDasharray="4 2"
        />
      )}
    </svg>
  );
}
