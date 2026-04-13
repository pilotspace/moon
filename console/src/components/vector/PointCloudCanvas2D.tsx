import { useRef, useEffect, useCallback, useMemo } from "react";
import { useVectorStore } from "@/stores/vector";

// Unified desaturated palette — readable on dark bg
const PALETTE: [number, number, number][] = [
  [107, 147, 212],  // blue     #6B93D4
  [204, 138, 91],   // orange   #CC8A5B
  [98, 184, 122],   // green    #62B87A
  [212, 128, 122],  // rose     #D4807A
  [91, 189, 173],   // teal     #5BBDAD
  [168, 139, 212],  // violet   #A88BD4
  [196, 165, 90],   // amber    #C4A55A
  [142, 146, 212],  // lavender #8E92D4
];
const BG = "#06060a";
const CYAN: [number, number, number] = [91, 189, 173];    // teal #5BBDAD
const HIGHLIGHT: [number, number, number] = [212, 185, 110]; // warm gold #D4B96E

export function PointCloudCanvas2D() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const camRef = useRef({ x: 0, y: 0, zoom: 1 });
  const dragRef = useRef<{ sx: number; sy: number; cx: number; cy: number } | null>(null);
  const rafRef = useRef(0);

  const positions = useVectorStore((s) => s.projectedPositions);
  const points = useVectorStore((s) => s.points);
  const colorBy = useVectorStore((s) => s.colorBy);
  const hoveredPointId = useVectorStore((s) => s.hoveredPointId);
  const knnResults = useVectorStore((s) => s.knnResults);
  const lassoSelectedIds = useVectorStore((s) => s.lassoSelectedIds);
  const setHovered = useVectorStore((s) => s.setHovered);

  const knnSet = useMemo(() => {
    const s = new Set<number>();
    for (const r of knnResults) if (r.pointIndex >= 0) s.add(r.pointIndex);
    return s;
  }, [knnResults]);

  const getColorRGB = useCallback((i: number): [number, number, number] => {
    const pt = points[i];
    if (!pt) return PALETTE[0];
    if (pt.id === hoveredPointId) return HIGHLIGHT;
    if (knnSet.has(i)) return CYAN;
    if (lassoSelectedIds.has(pt.id)) return HIGHLIGHT;
    if (colorBy === "segment") return pt.segment === "immutable" ? PALETTE[0] : PALETTE[1];
    if (colorBy === "label") {
      const lbl = String(pt.payload?.["label"] ?? pt.payload?.["category"] ?? "");
      const hash = [...lbl].reduce((a, c) => a + c.charCodeAt(0), 0);
      return PALETTE[hash % PALETTE.length];
    }
    return PALETTE[0];
  }, [points, colorBy, hoveredPointId, knnSet, lassoSelectedIds]);

  const isHighlighted = useCallback((i: number): boolean => {
    const pt = points[i];
    if (!pt) return false;
    return pt.id === hoveredPointId || knnSet.has(i) || lassoSelectedIds.has(pt.id);
  }, [points, hoveredPointId, knnSet, lassoSelectedIds]);

  // ── Draw ──
  const draw = useCallback(() => {
    const cvs = canvasRef.current;
    const ctx = cvs?.getContext("2d");
    if (!cvs || !ctx || !positions || positions.length === 0) return;

    const dpr = devicePixelRatio || 1;
    const W = cvs.width / dpr;
    const H = cvs.height / dpr;
    const { x: cx, y: cy, zoom } = camRef.current;
    const count = positions.length / 3;

    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.fillStyle = BG;
    ctx.fillRect(0, 0, W, H);

    ctx.save();
    ctx.translate(W / 2 + cx, H / 2 + cy);
    ctx.scale(zoom, zoom);

    // Scale-independent pixel sizes (divide by zoom so they stay constant on screen)
    const pxUnit = 1 / zoom;

    // ── Pass 1: glow layer (additive blend) ──
    ctx.globalCompositeOperation = "lighter";

    for (let i = 0; i < count; i++) {
      const x = positions[i * 3];
      const y = positions[i * 3 + 1];
      const rgb = getColorRGB(i);
      const hi = isHighlighted(i);

      const bloomR = (hi ? 14 : 4) * pxUnit;
      const alpha = hi ? 0.3 : 0.025;

      const grad = ctx.createRadialGradient(x, y, 0, x, y, bloomR);
      grad.addColorStop(0, `rgba(${rgb[0]},${rgb[1]},${rgb[2]},${alpha})`);
      grad.addColorStop(0.4, `rgba(${rgb[0]},${rgb[1]},${rgb[2]},${alpha * 0.25})`);
      grad.addColorStop(1, `rgba(${rgb[0]},${rgb[1]},${rgb[2]},0)`);
      ctx.fillStyle = grad;
      ctx.beginPath();
      ctx.arc(x, y, bloomR, 0, Math.PI * 2);
      ctx.fill();
    }

    // ── Pass 2: core dots (additive) ──
    for (let i = 0; i < count; i++) {
      const x = positions[i * 3];
      const y = positions[i * 3 + 1];
      const rgb = getColorRGB(i);
      const hi = isHighlighted(i);
      const r = (hi ? 3.5 : 1.4) * pxUnit;

      const core = ctx.createRadialGradient(x, y, 0, x, y, r);
      core.addColorStop(0, `rgba(${Math.min(255, rgb[0] + 100)},${Math.min(255, rgb[1] + 100)},${Math.min(255, rgb[2] + 100)},0.95)`);
      core.addColorStop(0.5, `rgba(${rgb[0]},${rgb[1]},${rgb[2]},0.6)`);
      core.addColorStop(1, `rgba(${rgb[0]},${rgb[1]},${rgb[2]},0)`);
      ctx.fillStyle = core;
      ctx.beginPath();
      ctx.arc(x, y, r, 0, Math.PI * 2);
      ctx.fill();
    }

    // ── Pass 3: hovered point ring ──
    ctx.globalCompositeOperation = "source-over";
    if (hoveredPointId) {
      const idx = points.findIndex((p) => p.id === hoveredPointId);
      if (idx >= 0) {
        const x = positions[idx * 3], y = positions[idx * 3 + 1];
        ctx.strokeStyle = `rgba(${HIGHLIGHT[0]},${HIGHLIGHT[1]},${HIGHLIGHT[2]},0.5)`;
        ctx.lineWidth = 0.6 * pxUnit;
        ctx.beginPath();
        ctx.arc(x, y, 7 * pxUnit, 0, Math.PI * 2);
        ctx.stroke();
      }
    }

    ctx.restore();
  }, [positions, points, getColorRGB, isHighlighted, hoveredPointId]);

  // RAF
  useEffect(() => {
    const tick = () => { draw(); rafRef.current = requestAnimationFrame(tick); };
    rafRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafRef.current);
  }, [draw]);

  // Resize
  useEffect(() => {
    const el = containerRef.current, cvs = canvasRef.current;
    if (!el || !cvs) return;
    const resize = () => {
      const dpr = devicePixelRatio || 1;
      const r = el.getBoundingClientRect();
      cvs.width = r.width * dpr; cvs.height = r.height * dpr;
      cvs.style.width = `${r.width}px`; cvs.style.height = `${r.height}px`;
    };
    const ro = new ResizeObserver(resize);
    ro.observe(el);
    resize();
    return () => ro.disconnect();
  }, []);

  // Auto-fit
  useEffect(() => {
    if (!positions || positions.length === 0 || !canvasRef.current) return;
    const dpr = devicePixelRatio || 1;
    const W = canvasRef.current.width / dpr, H = canvasRef.current.height / dpr;
    const count = positions.length / 3;
    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    for (let i = 0; i < count; i++) {
      const x = positions[i * 3], y = positions[i * 3 + 1];
      if (x < minX) minX = x; if (x > maxX) maxX = x;
      if (y < minY) minY = y; if (y > maxY) maxY = y;
    }
    const rX = maxX - minX || 1, rY = maxY - minY || 1;
    const zoom = Math.min(W / rX, H / rY) * 0.75;
    camRef.current = { x: -((minX + maxX) / 2) * zoom, y: -((minY + maxY) / 2) * zoom, zoom };
  }, [positions]);

  // Hit test
  const hitTest = useCallback((ex: number, ey: number): number => {
    const cvs = canvasRef.current;
    if (!cvs || !positions) return -1;
    const dpr = devicePixelRatio || 1;
    const rect = cvs.getBoundingClientRect();
    const W = cvs.width / dpr, H = cvs.height / dpr;
    const { x: cx, y: cy, zoom } = camRef.current;
    const wx = (ex - rect.left - W / 2 - cx) / zoom;
    const wy = (ey - rect.top - H / 2 - cy) / zoom;
    const count = positions.length / 3;
    let best = -1, bestD = Infinity;
    const hitR = 5 / zoom;
    for (let i = 0; i < count; i++) {
      const dx = wx - positions[i * 3], dy = wy - positions[i * 3 + 1];
      const d2 = dx * dx + dy * dy;
      if (d2 < hitR * hitR && d2 < bestD) { bestD = d2; best = i; }
    }
    return best;
  }, [positions]);

  const onMove = useCallback((e: React.MouseEvent) => {
    if (dragRef.current) {
      camRef.current.x = dragRef.current.cx + (e.clientX - dragRef.current.sx);
      camRef.current.y = dragRef.current.cy + (e.clientY - dragRef.current.sy);
      return;
    }
    const idx = hitTest(e.clientX, e.clientY);
    setHovered(idx >= 0 && points[idx] ? points[idx].id : null);
  }, [hitTest, setHovered, points]);

  const onDown = useCallback((e: React.MouseEvent) => {
    if (e.button === 0) dragRef.current = { sx: e.clientX, sy: e.clientY, cx: camRef.current.x, cy: camRef.current.y };
  }, []);

  const onUp = useCallback((e: React.MouseEvent) => {
    if (dragRef.current) {
      const dx = e.clientX - dragRef.current.sx, dy = e.clientY - dragRef.current.sy;
      dragRef.current = null;
      if (Math.abs(dx) < 4 && Math.abs(dy) < 4) {
        const idx = hitTest(e.clientX, e.clientY);
        setHovered(idx >= 0 && points[idx] ? points[idx].id : null);
      }
    }
  }, [hitTest, setHovered, points]);

  const onWheel = useCallback((e: React.WheelEvent) => {
    e.preventDefault();
    const f = e.deltaY > 0 ? 0.92 : 1.08;
    camRef.current.zoom *= f;
    camRef.current.x *= f;
    camRef.current.y *= f;
  }, []);

  return (
    <div ref={containerRef} className="h-full w-full" style={{ background: BG }}>
      <canvas
        ref={canvasRef}
        onMouseMove={onMove}
        onMouseDown={onDown}
        onMouseUp={onUp}
        onMouseLeave={() => { dragRef.current = null; setHovered(null); }}
        onWheel={onWheel}
        style={{ cursor: dragRef.current ? "grabbing" : "crosshair" }}
      />
    </div>
  );
}
