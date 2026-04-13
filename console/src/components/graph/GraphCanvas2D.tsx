import { useRef, useEffect, useCallback, useMemo } from "react";
import { useGraphStore } from "@/stores/graph";

// ── Cosmograph-inspired palette ──
// Muted, desaturated for base; full sat for highlight
const PALETTE = [
  "#6B93D4", // blue
  "#5BBDAD", // teal
  "#A88BD4", // violet
  "#CC8A5B", // orange
  "#D4807A", // rose
  "#C4A55A", // amber
  "#62B87A", // green
  "#8E92D4", // lavender
];

const BG = "#06060a";

export function GraphCanvas2D() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef({ x: 0, y: 0, scale: 1 });
  const dragRef = useRef<{
    sx: number; sy: number; vx: number; vy: number;
    moved: boolean;
  } | null>(null);
  const rafRef = useRef(0);
  const dirtyRef = useRef(true);
  const mouseWorldRef = useRef<{ x: number; y: number } | null>(null);

  const nodes = useGraphStore((s) => s.nodes);
  const edges = useGraphStore((s) => s.edges);
  const positions = useGraphStore((s) => s.positions);
  const visibleLabels = useGraphStore((s) => s.visibleLabels);
  const visibleRelTypes = useGraphStore((s) => s.visibleRelTypes);
  const selectedNodeId = useGraphStore((s) => s.selectedNodeId);
  const hoveredNodeId = useGraphStore((s) => s.hoveredNodeId);
  const selectNode = useGraphStore((s) => s.selectNode);
  const setHoveredNode = useGraphStore((s) => s.setHoveredNode);

  // ── Precomputed lookups ──
  const nodeIdToIndex = useMemo(() => {
    const m = new Map<string, number>();
    nodes.forEach((n, i) => m.set(n.id, i));
    return m;
  }, [nodes]);

  const labelColorMap = useMemo(() => {
    const all = new Set<string>();
    nodes.forEach((n) => n.labels.forEach((l) => all.add(l)));
    const m = new Map<string, string>();
    let i = 0;
    for (const l of all) { m.set(l, PALETTE[i++ % PALETTE.length]); }
    return m;
  }, [nodes]);

  const degree = useMemo(() => {
    const d = new Map<string, number>();
    nodes.forEach((n) => d.set(n.id, 0));
    edges.forEach((e) => {
      d.set(e.source, (d.get(e.source) ?? 0) + 1);
      d.set(e.target, (d.get(e.target) ?? 0) + 1);
    });
    return d;
  }, [nodes, edges]);

  const connectedSet = useMemo(() => {
    const s = new Set<string>();
    if (!hoveredNodeId && !selectedNodeId) return s;
    const focal = hoveredNodeId ?? selectedNodeId;
    edges.forEach((e) => {
      if (e.source === focal || e.target === focal) {
        s.add(e.source);
        s.add(e.target);
      }
    });
    return s;
  }, [edges, hoveredNodeId, selectedNodeId]);

  // ── Rendering ──
  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    const ctx = canvas?.getContext("2d");
    if (!canvas || !ctx || !positions || positions.length === 0) {
      // Draw empty state background
      if (canvas && ctx) {
        const dpr = devicePixelRatio || 1;
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.fillStyle = BG;
        ctx.fillRect(0, 0, canvas.width / dpr, canvas.height / dpr);
      }
      return;
    }

    const dpr = devicePixelRatio || 1;
    const w = canvas.width / dpr;
    const h = canvas.height / dpr;
    const { x: vx, y: vy, scale } = viewRef.current;
    const maxDeg = Math.max(1, ...Array.from(degree.values()));
    const focal = hoveredNodeId ?? selectedNodeId;

    // Clear
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.fillStyle = BG;
    ctx.fillRect(0, 0, w, h);
    ctx.save();
    ctx.translate(w / 2 + vx, h / 2 + vy);
    ctx.scale(scale, scale);

    // ─── Pass 1: Edges (hairline, nearly invisible) ───
    const baseEdgeAlpha = focal ? 0.03 : 0.07;
    for (const e of edges) {
      if (!visibleRelTypes.has(e.type)) continue;
      const si = nodeIdToIndex.get(e.source);
      const ti = nodeIdToIndex.get(e.target);
      if (si == null || ti == null) continue;

      const connected = e.source === focal || e.target === focal;
      const color = connected
        ? labelColorMap.get(nodes[si]?.labels[0] ?? "") ?? PALETTE[0]
        : "#ffffff";

      ctx.globalAlpha = connected ? 0.35 : baseEdgeAlpha;
      ctx.strokeStyle = color;
      ctx.lineWidth = connected ? 0.8 / scale : 0.4 / scale;
      ctx.beginPath();
      ctx.moveTo(positions[si * 3], positions[si * 3 + 1]);
      ctx.lineTo(positions[ti * 3], positions[ti * 3 + 1]);
      ctx.stroke();
    }
    ctx.globalAlpha = 1;

    // ─── Pass 2: Node glow (additive blend → nebula effect) ───
    ctx.globalCompositeOperation = "lighter";
    for (let i = 0; i < nodes.length; i++) {
      const n = nodes[i];
      if (!n.labels.some((l) => visibleLabels.has(l))) continue;
      const x = positions[i * 3];
      const y = positions[i * 3 + 1];
      const deg = degree.get(n.id) ?? 0;
      const normDeg = deg / maxDeg;
      const color = labelColorMap.get(n.labels[0] ?? "") ?? PALETTE[0];

      const isFocal = n.id === focal;
      const isConnected = connectedSet.has(n.id);
      const isSelected = n.id === selectedNodeId;

      // Only glow for: focal, connected, hub, or selected nodes
      const glowIntensity = isFocal ? 0.35
        : isSelected ? 0.25
        : isConnected ? 0.15
        : normDeg > 0.3 ? normDeg * 0.12
        : 0;

      if (glowIntensity > 0) {
        const baseR = 1.5 + normDeg * 3.5;
        const glowR = isFocal ? baseR * 6 : isSelected ? baseR * 5 : baseR * 3.5;
        const grad = ctx.createRadialGradient(x, y, 0, x, y, glowR);
        grad.addColorStop(0, color + alphaHex(glowIntensity));
        grad.addColorStop(0.4, color + alphaHex(glowIntensity * 0.4));
        grad.addColorStop(1, color + "00");
        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.arc(x, y, glowR, 0, Math.PI * 2);
        ctx.fill();
      }
    }

    // ─── Pass 3: Node cores (normal blend) ───
    ctx.globalCompositeOperation = "source-over";
    for (let i = 0; i < nodes.length; i++) {
      const n = nodes[i];
      if (!n.labels.some((l) => visibleLabels.has(l))) continue;
      const x = positions[i * 3];
      const y = positions[i * 3 + 1];
      const deg = degree.get(n.id) ?? 0;
      const normDeg = deg / maxDeg;
      const color = labelColorMap.get(n.labels[0] ?? "") ?? PALETTE[0];

      const isFocal = n.id === focal;
      const isSelected = n.id === selectedNodeId;
      const isConnected = connectedSet.has(n.id);
      const dimmed = focal && !isFocal && !isConnected;

      const baseR = 1.5 + normDeg * 3.5;
      const r = isFocal ? baseR * 2 : isSelected ? baseR * 1.6 : baseR;

      ctx.globalAlpha = dimmed ? 0.15 : isFocal ? 1 : isConnected ? 0.95 : 0.8;

      // Bright center dot
      ctx.fillStyle = isFocal ? "#ffffff" : color;
      ctx.beginPath();
      ctx.arc(x, y, r, 0, Math.PI * 2);
      ctx.fill();

      // Selection ring
      if (isSelected && !isFocal) {
        ctx.globalAlpha = 0.6;
        ctx.strokeStyle = "#ffffff";
        ctx.lineWidth = 0.6 / scale;
        ctx.beginPath();
        ctx.arc(x, y, r + 2 / scale, 0, Math.PI * 2);
        ctx.stroke();
      }
    }
    ctx.globalAlpha = 1;

    // ─── Pass 4: Hover label ───
    if (focal) {
      const fi = nodeIdToIndex.get(focal);
      if (fi != null) {
        const fx = positions[fi * 3];
        const fy = positions[fi * 3 + 1];
        const fDeg = degree.get(focal) ?? 0;
        const fR = 1.5 + (fDeg / maxDeg) * 3.5;
        const labelText = nodes[fi].properties?.["name"]
          ?? nodes[fi].properties?.["id"]
          ?? nodes[fi].id.replace("node:", "#");
        const labelY = fy - fR * 2 - 6 / scale;

        ctx.font = `${11 / scale}px -apple-system, system-ui, sans-serif`;
        ctx.textAlign = "center";
        ctx.textBaseline = "bottom";

        // Label bg pill
        const metrics = ctx.measureText(String(labelText));
        const px = 4 / scale;
        const py = 2 / scale;
        const lw = metrics.width + px * 2;
        const lh = 11 / scale + py * 2;
        ctx.fillStyle = "rgba(0,0,0,0.7)";
        roundRect(ctx, fx - lw / 2, labelY - lh, lw, lh, 3 / scale);
        ctx.fill();

        // Label text
        ctx.fillStyle = "#e4e4e7";
        ctx.fillText(String(labelText), fx, labelY - py);
      }
    }

    ctx.restore();
  }, [
    nodes, edges, positions, nodeIdToIndex, labelColorMap, degree,
    visibleLabels, visibleRelTypes, selectedNodeId, hoveredNodeId, connectedSet,
  ]);

  // Mark dirty when any dependency changes
  useEffect(() => { dirtyRef.current = true; }, [draw]);

  // RAF loop
  useEffect(() => {
    let prev = 0;
    const tick = (t: number) => {
      // Always redraw during layout animation, otherwise only on dirty
      if (dirtyRef.current || t - prev > 50) {
        draw();
        dirtyRef.current = false;
        prev = t;
      }
      rafRef.current = requestAnimationFrame(tick);
    };
    rafRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafRef.current);
  }, [draw]);

  // ── Resize ──
  useEffect(() => {
    const c = containerRef.current;
    const canvas = canvasRef.current;
    if (!c || !canvas) return;
    const resize = () => {
      const dpr = devicePixelRatio || 1;
      const r = c.getBoundingClientRect();
      canvas.width = r.width * dpr;
      canvas.height = r.height * dpr;
      canvas.style.width = `${r.width}px`;
      canvas.style.height = `${r.height}px`;
      dirtyRef.current = true;
    };
    const ro = new ResizeObserver(resize);
    ro.observe(c);
    resize();
    return () => ro.disconnect();
  }, []);

  // ── Auto-fit ──
  useEffect(() => {
    if (!positions || positions.length === 0 || !canvasRef.current) return;
    const dpr = devicePixelRatio || 1;
    const w = canvasRef.current.width / dpr;
    const h = canvasRef.current.height / dpr;
    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    for (let i = 0; i < nodes.length; i++) {
      const x = positions[i * 3], y = positions[i * 3 + 1];
      if (x < minX) minX = x; if (x > maxX) maxX = x;
      if (y < minY) minY = y; if (y > maxY) maxY = y;
    }
    const rangeX = maxX - minX || 1;
    const rangeY = maxY - minY || 1;
    const s = Math.min(w / rangeX, h / rangeY) * 0.7;
    const cx = (minX + maxX) / 2;
    const cy = (minY + maxY) / 2;
    viewRef.current = { x: -cx * s, y: -cy * s, scale: s };
    dirtyRef.current = true;
  }, [positions, nodes]);

  // ── Hit test ──
  const hitTest = useCallback((clientX: number, clientY: number): string | null => {
    const canvas = canvasRef.current;
    if (!canvas || !positions) return null;
    const dpr = devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    const w = canvas.width / dpr;
    const h = canvas.height / dpr;
    const { x: vx, y: vy, scale } = viewRef.current;
    const wx = (clientX - rect.left - w / 2 - vx) / scale;
    const wy = (clientY - rect.top - h / 2 - vy) / scale;
    const maxDeg = Math.max(1, ...Array.from(degree.values()));

    let best: string | null = null;
    let bestD = Infinity;
    for (let i = 0; i < nodes.length; i++) {
      const n = nodes[i];
      if (!n.labels.some((l) => visibleLabels.has(l))) continue;
      const dx = wx - positions[i * 3];
      const dy = wy - positions[i * 3 + 1];
      const d2 = dx * dx + dy * dy;
      const deg = degree.get(n.id) ?? 0;
      const r = (1.5 + (deg / maxDeg) * 3.5) * 3; // generous
      if (d2 < r * r && d2 < bestD) { bestD = d2; best = n.id; }
    }
    return best;
  }, [positions, nodes, degree, visibleLabels]);

  // ── Mouse handlers ──
  const onMouseDown = useCallback((e: React.MouseEvent) => {
    if (e.button !== 0) return;
    dragRef.current = {
      sx: e.clientX, sy: e.clientY,
      vx: viewRef.current.x, vy: viewRef.current.y,
      moved: false,
    };
  }, []);

  const onMouseMove = useCallback((e: React.MouseEvent) => {
    const d = dragRef.current;
    if (d) {
      const dx = e.clientX - d.sx;
      const dy = e.clientY - d.sy;
      if (Math.abs(dx) > 2 || Math.abs(dy) > 2) d.moved = true;
      viewRef.current.x = d.vx + dx;
      viewRef.current.y = d.vy + dy;
      dirtyRef.current = true;
      return;
    }
    const hit = hitTest(e.clientX, e.clientY);
    if (hit !== hoveredNodeId) {
      setHoveredNode(hit);
      dirtyRef.current = true;
    }
  }, [hitTest, hoveredNodeId, setHoveredNode]);

  const onMouseUp = useCallback((e: React.MouseEvent) => {
    const d = dragRef.current;
    dragRef.current = null;
    if (d && !d.moved) {
      const hit = hitTest(e.clientX, e.clientY);
      selectNode(hit);
      dirtyRef.current = true;
    }
  }, [hitTest, selectNode]);

  const onWheel = useCallback((e: React.WheelEvent) => {
    e.preventDefault();
    const canvas = canvasRef.current;
    if (!canvas) return;
    const dpr = devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    const w = canvas.width / dpr;
    const h = canvas.height / dpr;
    const mx = e.clientX - rect.left - w / 2;
    const my = e.clientY - rect.top - h / 2;

    const factor = e.deltaY > 0 ? 0.9 : 1.11;
    const v = viewRef.current;
    // Zoom toward cursor
    v.x = mx - (mx - v.x) * factor;
    v.y = my - (my - v.y) * factor;
    v.scale *= factor;
    dirtyRef.current = true;
  }, []);

  const onMouseLeave = useCallback(() => {
    dragRef.current = null;
    if (hoveredNodeId) { setHoveredNode(null); dirtyRef.current = true; }
  }, [hoveredNodeId, setHoveredNode]);

  return (
    <div
      ref={containerRef}
      className="h-full w-full"
      style={{ background: BG, cursor: dragRef.current?.moved ? "grabbing" : "default" }}
    >
      <canvas
        ref={canvasRef}
        onMouseDown={onMouseDown}
        onMouseMove={onMouseMove}
        onMouseUp={onMouseUp}
        onMouseLeave={onMouseLeave}
        onWheel={onWheel}
      />
    </div>
  );
}

// ── Helpers ──
function alphaHex(a: number): string {
  return Math.round(Math.min(1, Math.max(0, a)) * 255)
    .toString(16)
    .padStart(2, "0");
}

function roundRect(
  ctx: CanvasRenderingContext2D,
  x: number, y: number, w: number, h: number, r: number,
) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + w - r, y);
  ctx.quadraticCurveTo(x + w, y, x + w, y + r);
  ctx.lineTo(x + w, y + h - r);
  ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
  ctx.lineTo(x + r, y + h);
  ctx.quadraticCurveTo(x, y + h, x, y + h - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
}
