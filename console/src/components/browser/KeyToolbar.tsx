import { useState } from "react";
import { useBrowserStore } from "@/stores/browser";
import { Search, Filter, Trash2, RefreshCw, X } from "lucide-react";
import type { KeyType } from "@/types/browser";

const KEY_TYPES: Array<{ value: KeyType | "all"; label: string }> = [
  { value: "all", label: "All Types" },
  { value: "string", label: "String" },
  { value: "hash", label: "Hash" },
  { value: "list", label: "List" },
  { value: "set", label: "Set" },
  { value: "zset", label: "Sorted Set" },
  { value: "stream", label: "Stream" },
];

const TTL_OPTIONS = [
  { value: "all" as const, label: "Any TTL" },
  { value: "persistent" as const, label: "Persistent" },
  { value: "expiring" as const, label: "Expiring" },
];

export function KeyToolbar() {
  const filter = useBrowserStore((s) => s.filter);
  const setFilter = useBrowserStore((s) => s.setFilter);
  const selectedKeys = useBrowserStore((s) => s.selectedKeys);
  const selectAllKeys = useBrowserStore((s) => s.selectAllKeys);
  const clearSelection = useBrowserStore((s) => s.clearSelection);
  const deleteSelected = useBrowserStore((s) => s.deleteSelected);
  const loadKeys = useBrowserStore((s) => s.loadKeys);
  const keys = useBrowserStore((s) => s.keys);
  const [patternInput, setPatternInput] = useState(filter.pattern);
  const [confirmDelete, setConfirmDelete] = useState(false);

  const handlePatternSubmit = () => {
    setFilter({ pattern: patternInput || "*" });
  };

  const handleDelete = async () => {
    if (!confirmDelete) {
      setConfirmDelete(true);
      return;
    }
    await deleteSelected();
    setConfirmDelete(false);
  };

  return (
    <div className="flex flex-col gap-2 px-3 py-2 border-b border-border">
      {/* Search bar */}
      <div className="flex items-center gap-2">
        <div className="flex-1 flex items-center gap-1.5 rounded-md border border-border bg-zinc-900 px-2 py-1">
          <Search className="h-3.5 w-3.5 text-muted-foreground" />
          <input
            type="text"
            value={patternInput}
            onChange={(e) => setPatternInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handlePatternSubmit()}
            placeholder="Filter pattern (e.g. user:*)"
            className="flex-1 bg-transparent text-xs text-zinc-100 outline-none placeholder:text-muted-foreground/50"
          />
          {patternInput !== "*" && (
            <button onClick={() => { setPatternInput("*"); setFilter({ pattern: "*" }); }}>
              <X className="h-3 w-3 text-muted-foreground hover:text-zinc-100" />
            </button>
          )}
        </div>
        <button
          onClick={() => loadKeys(true)}
          className="p-1.5 rounded-md border border-border hover:bg-accent/50 transition-colors"
          title="Refresh"
        >
          <RefreshCw className="h-3.5 w-3.5 text-muted-foreground" />
        </button>
      </div>

      {/* Filter row */}
      <div className="flex items-center gap-2">
        <Filter className="h-3 w-3 text-muted-foreground" />
        <select
          value={filter.type}
          onChange={(e) => setFilter({ type: e.target.value as KeyType | "all" })}
          className="text-xs bg-zinc-900 border border-border rounded px-1.5 py-0.5 text-zinc-300 outline-none"
        >
          {KEY_TYPES.map((t) => (
            <option key={t.value} value={t.value}>{t.label}</option>
          ))}
        </select>
        <select
          value={filter.ttlStatus}
          onChange={(e) => setFilter({ ttlStatus: e.target.value as "all" | "persistent" | "expiring" })}
          className="text-xs bg-zinc-900 border border-border rounded px-1.5 py-0.5 text-zinc-300 outline-none"
        >
          {TTL_OPTIONS.map((t) => (
            <option key={t.value} value={t.value}>{t.label}</option>
          ))}
        </select>

        <div className="flex-1" />

        {/* Bulk actions */}
        {selectedKeys.size > 0 ? (
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted-foreground">{selectedKeys.size} selected</span>
            <button
              onClick={clearSelection}
              className="text-xs text-muted-foreground hover:text-zinc-100 transition-colors"
            >
              Clear
            </button>
            <button
              onClick={handleDelete}
              className={`flex items-center gap-1 text-xs px-2 py-0.5 rounded transition-colors ${
                confirmDelete
                  ? "bg-destructive text-destructive-foreground"
                  : "text-destructive hover:bg-destructive/20"
              }`}
            >
              <Trash2 className="h-3 w-3" />
              {confirmDelete ? "Confirm Delete" : "Delete"}
            </button>
          </div>
        ) : (
          <button
            onClick={selectAllKeys}
            className="text-xs text-muted-foreground hover:text-zinc-100 transition-colors"
          >
            Select All ({keys.length})
          </button>
        )}
      </div>
    </div>
  );
}
