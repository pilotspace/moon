import { useCallback } from "react";
import { Treemap, ResponsiveContainer } from "recharts";
import { useMemoryStore } from "@/stores/memory";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import type { MemoryNode } from "@/types/memory";

const TYPE_COLORS: Record<string, string> = {
  string: "#6B93D4",
  hash: "#5BBDAD",
  list: "#C4A55A",
  set: "#A88BD4",
  zset: "#8E92D4",
  stream: "#62B87A",
  namespace: "#6B6B78",
};

function formatBytes(bytes: number): string {
  if (bytes >= 1_073_741_824) return `${(bytes / 1_073_741_824).toFixed(1)} GB`;
  if (bytes >= 1_048_576) return `${(bytes / 1_048_576).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
}

interface TreemapContentProps {
  x?: number;
  y?: number;
  width?: number;
  height?: number;
  name?: string;
  type?: string;
  size?: number;
}

function CustomContent(props: TreemapContentProps) {
  const { x = 0, y = 0, width = 0, height = 0, name, type, size } = props;
  if (width < 4 || height < 4) return null;

  const color = TYPE_COLORS[type ?? "namespace"] ?? TYPE_COLORS.namespace;
  const showLabel = width > 36 && height > 20;
  const showSize = width > 60 && height > 36;
  const maxChars = Math.max(1, Math.floor((width - 12) / 7));
  const fontSize = width < 60 ? 10 : 12;
  const truncated = name && name.length > maxChars ? name.slice(0, maxChars - 1) + "\u2026" : name;

  return (
    <g>
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={color}
        fillOpacity={0.85}
        stroke="#18181b"
        strokeWidth={1.5}
        rx={2}
        style={{ cursor: "pointer" }}
      />
      {showLabel && (
        <text
          x={x + 4}
          y={y + (fontSize === 10 ? 13 : 16)}
          fill="#fff"
          fontSize={fontSize}
          fontWeight={500}
          style={{ pointerEvents: "none" }}
        >
          {truncated}
        </text>
      )}
      {showSize && size != null && (
        <text
          x={x + 4}
          y={y + (fontSize === 10 ? 24 : 32)}
          fill="rgba(255,255,255,0.7)"
          fontSize={10}
          style={{ pointerEvents: "none" }}
        >
          {formatBytes(size)}
        </text>
      )}
    </g>
  );
}

export function MemoryTreemap() {
  const loading = useMemoryStore((s) => s.loading);
  const treemapPath = useMemoryStore((s) => s.treemapPath);
  const drillDown = useMemoryStore((s) => s.drillDown);
  const drillUp = useMemoryStore((s) => s.drillUp);
  const resetDrill = useMemoryStore((s) => s.resetDrill);
  const currentNode = useMemoryStore((s) => s.currentTreemapNode());

  const handleClick = useCallback(
    (node: MemoryNode) => {
      if (node.children && node.children.length > 0) {
        drillDown(node.name);
      }
    },
    [drillDown],
  );

  // Build recharts-compatible data: needs children with `size` for leaves
  const chartData = currentNode?.children?.map((child) => ({
    name: child.name,
    size: child.size,
    type: child.type,
    children: child.children,
    fullKey: child.fullKey,
  })) ?? [];

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle>Memory Distribution</CardTitle>
          <div className="flex items-center gap-2 flex-wrap">
            {Object.entries(TYPE_COLORS).filter(([k]) => k !== "namespace").map(([type, color]) => (
              <div key={type} className="flex items-center gap-1">
                <div className="w-3 h-3 rounded-sm" style={{ backgroundColor: color }} />
                <span className="text-xs text-muted-foreground">{type}</span>
              </div>
            ))}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {/* Breadcrumb navigation */}
        {treemapPath.length > 0 && (
          <div className="flex items-center gap-1 mb-4 text-sm">
            <button
              onClick={resetDrill}
              className="text-primary hover:underline cursor-pointer"
            >
              keyspace
            </button>
            {treemapPath.map((segment, i) => (
              <span key={i} className="flex items-center gap-1">
                <span className="text-muted-foreground">&gt;</span>
                {i < treemapPath.length - 1 ? (
                  <button
                    onClick={() => {
                      // Navigate to this level
                      const store = useMemoryStore.getState();
                      store.resetDrill();
                      for (let j = 0; j <= i; j++) {
                        store.drillDown(treemapPath[j]);
                      }
                    }}
                    className="text-primary hover:underline cursor-pointer"
                  >
                    {segment}
                  </button>
                ) : (
                  <span className="text-zinc-100 font-medium">{segment}</span>
                )}
              </span>
            ))}
            <button
              onClick={drillUp}
              className="ml-2 text-xs text-muted-foreground hover:text-zinc-300 cursor-pointer"
            >
              <Badge variant="outline">Back</Badge>
            </button>
          </div>
        )}

        {loading ? (
          <div className="flex items-center justify-center h-64 text-muted-foreground">
            <div className="animate-spin w-6 h-6 border-2 border-primary border-t-transparent rounded-full mr-3" />
            Scanning keyspace...
          </div>
        ) : chartData.length === 0 ? (
          <div className="flex items-center justify-center h-64 text-sm text-muted-foreground">
            No keys found. Add data to see memory distribution.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={400}>
            <Treemap
              data={chartData}
              dataKey="size"
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              content={<CustomContent /> as any}
              onClick={(node) => {
                if (node && typeof node === "object" && "name" in node) {
                  handleClick(node as unknown as MemoryNode);
                }
              }}
              animationDuration={300}
            />
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}
