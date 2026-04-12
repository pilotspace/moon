import { useRef, useEffect, useCallback } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useBrowserStore } from "@/stores/browser";
import { cn } from "@/lib/utils";
import type { KeyType } from "@/types/browser";
import { getTtlTier, TYPE_COLORS } from "@/types/browser";
import { Loader2, CheckSquare, Square } from "lucide-react";

const ROW_HEIGHT = 36;

function formatBytes(bytes: number | null): string {
  if (bytes === null) return "--";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatTtl(ttl: number | null): string {
  if (ttl === null) return "--";
  if (ttl === -1) return "persistent";
  if (ttl <= 0) return "expired";
  if (ttl < 60) return `${ttl}s`;
  if (ttl < 3600) return `${Math.floor(ttl / 60)}m ${ttl % 60}s`;
  return `${Math.floor(ttl / 3600)}h ${Math.floor((ttl % 3600) / 60)}m`;
}

function TypeBadge({ type }: { type: KeyType | null }) {
  if (!type) return <span className="text-xs text-muted-foreground">...</span>;
  return (
    <span className={cn("inline-flex items-center rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase", TYPE_COLORS[type])}>
      {type}
    </span>
  );
}

function TtlDisplay({ ttl }: { ttl: number | null }) {
  const tier = getTtlTier(ttl);
  const text = formatTtl(ttl);
  return (
    <span
      className={cn(
        "text-xs tabular-nums",
        tier === "green" && "text-green-400",
        tier === "amber" && "text-amber-400",
        tier === "red" && "text-red-400 animate-pulse",
        tier === "none" && "text-muted-foreground",
      )}
    >
      {text}
    </span>
  );
}

export function KeyList() {
  const parentRef = useRef<HTMLDivElement>(null);
  const keys = useBrowserStore((s) => s.keys);
  const loading = useBrowserStore((s) => s.loading);
  const hasMore = useBrowserStore((s) => s.hasMore);
  const selectedKey = useBrowserStore((s) => s.selectedKey);
  const selectedKeys = useBrowserStore((s) => s.selectedKeys);
  const loadKeys = useBrowserStore((s) => s.loadKeys);
  const selectKey = useBrowserStore((s) => s.selectKey);
  const toggleKeySelection = useBrowserStore((s) => s.toggleKeySelection);
  const enrichKey = useBrowserStore((s) => s.enrichKey);

  const virtualizer = useVirtualizer({
    count: keys.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });

  // Load more when scrolled near bottom
  const handleScroll = useCallback(() => {
    const el = parentRef.current;
    if (!el || loading || !hasMore) return;
    const { scrollTop, scrollHeight, clientHeight } = el;
    if (scrollHeight - scrollTop - clientHeight < 200) {
      loadKeys();
    }
  }, [loading, hasMore, loadKeys]);

  // Initial load
  useEffect(() => {
    if (keys.length === 0) loadKeys(true);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Enrich visible keys with type/TTL/memory
  const virtualItems = virtualizer.getVirtualItems();
  useEffect(() => {
    for (const vItem of virtualItems) {
      const entry = keys[vItem.index];
      if (entry && entry.type === null) {
        enrichKey(entry.name);
      }
    }
  }, [virtualItems, keys, enrichKey]);

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center gap-2 px-3 py-2 border-b border-border text-xs text-muted-foreground font-medium">
        <span className="w-6" />
        <span className="flex-1">Key</span>
        <span className="w-16 text-right">Type</span>
        <span className="w-20 text-right">TTL</span>
        <span className="w-16 text-right">Size</span>
      </div>

      {/* Virtual list */}
      <div
        ref={parentRef}
        className="flex-1 overflow-auto"
        onScroll={handleScroll}
      >
        <div
          style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}
        >
          {virtualItems.map((vItem) => {
            const entry = keys[vItem.index];
            if (!entry) return null;
            const isActive = entry.name === selectedKey;
            const isChecked = selectedKeys.has(entry.name);

            return (
              <div
                key={entry.name}
                data-index={vItem.index}
                ref={virtualizer.measureElement}
                className={cn(
                  "absolute left-0 right-0 flex items-center gap-2 px-3 py-1 cursor-pointer border-b border-border/50 text-sm transition-colors",
                  isActive ? "bg-accent text-accent-foreground" : "hover:bg-accent/30",
                )}
                style={{
                  top: `${vItem.start}px`,
                  height: `${ROW_HEIGHT}px`,
                }}
                onClick={() => selectKey(entry.name)}
              >
                <button
                  className="w-6 flex items-center justify-center"
                  onClick={(e) => {
                    e.stopPropagation();
                    toggleKeySelection(entry.name);
                  }}
                >
                  {isChecked ? (
                    <CheckSquare className="h-3.5 w-3.5 text-primary" />
                  ) : (
                    <Square className="h-3.5 w-3.5 text-muted-foreground/50" />
                  )}
                </button>
                <span className="flex-1 truncate font-mono text-xs">{entry.name}</span>
                <span className="w-16 text-right"><TypeBadge type={entry.type} /></span>
                <span className="w-20 text-right"><TtlDisplay ttl={entry.ttl} /></span>
                <span className="w-16 text-right text-xs text-muted-foreground tabular-nums">{formatBytes(entry.memoryBytes)}</span>
              </div>
            );
          })}
        </div>

        {loading && (
          <div className="flex items-center justify-center py-4">
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
          </div>
        )}
      </div>

      {/* Footer */}
      <div className="flex items-center justify-between px-3 py-1.5 border-t border-border text-xs text-muted-foreground">
        <span>{keys.length} keys loaded</span>
        {hasMore && <span className="text-primary">scroll for more</span>}
      </div>
    </div>
  );
}
