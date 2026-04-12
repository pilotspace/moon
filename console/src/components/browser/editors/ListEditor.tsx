import { useState, useCallback } from "react";
import { execCommand } from "@/lib/api";
import { toastError } from "@/lib/toast";
import { ArrowUpToLine, ArrowDownToLine, Trash2 } from "lucide-react";

export function ListEditor({ keyName, value }: { keyName: string; value: unknown }) {
  const [items, setItems] = useState<string[]>(() => (Array.isArray(value) ? value.map(String) : []));
  const [newValue, setNewValue] = useState("");

  const handleLPush = useCallback(async () => {
    if (!newValue.trim()) return;
    try {
      await execCommand("LPUSH", [keyName, newValue]);
      setItems((prev) => [newValue, ...prev]);
      setNewValue("");
    } catch (err) {
      toastError(`LPUSH ${keyName} failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }, [keyName, newValue]);

  const handleRPush = useCallback(async () => {
    if (!newValue.trim()) return;
    try {
      await execCommand("RPUSH", [keyName, newValue]);
      setItems((prev) => [...prev, newValue]);
      setNewValue("");
    } catch (err) {
      toastError(`RPUSH ${keyName} failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }, [keyName, newValue]);

  const handleRemove = useCallback(async (index: number) => {
    const val = items[index];
    // LREM removes first occurrence
    try {
      await execCommand("LREM", [keyName, "1", val]);
      setItems((prev) => prev.filter((_, i) => i !== index));
    } catch (err) {
      toastError(`LREM ${keyName} failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }, [keyName, items]);

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center gap-2 px-3 py-1.5 border-b border-border text-[10px] font-medium text-muted-foreground uppercase">
        <span className="w-10">Index</span>
        <span className="flex-1">Value</span>
        <span className="w-8" />
      </div>

      {/* Items */}
      <div className="flex-1 overflow-auto">
        {items.map((item, i) => (
          <div
            key={`${i}-${item}`}
            className="flex items-center gap-2 px-3 py-1 border-b border-border/50"
          >
            <span className="w-10 text-[10px] text-muted-foreground tabular-nums">{i}</span>
            <span className="flex-1 text-xs font-mono text-zinc-300 truncate">{item}</span>
            <button
              onClick={() => handleRemove(i)}
              className="w-8 flex justify-center text-muted-foreground/50 hover:text-destructive"
            >
              <Trash2 className="h-3 w-3" />
            </button>
          </div>
        ))}
      </div>

      {/* Push controls */}
      <div className="flex items-center gap-2 px-3 py-2 border-t border-border">
        <input
          value={newValue}
          onChange={(e) => setNewValue(e.target.value)}
          placeholder="new element"
          onKeyDown={(e) => e.key === "Enter" && handleRPush()}
          className="flex-1 text-xs font-mono bg-zinc-900 border border-border rounded px-1.5 py-0.5 text-zinc-100 outline-none placeholder:text-muted-foreground/40"
        />
        <button
          onClick={handleLPush}
          className="flex items-center gap-1 text-xs text-primary hover:text-primary/80"
          title="LPUSH (prepend)"
        >
          <ArrowUpToLine className="h-3 w-3" />
          L
        </button>
        <button
          onClick={handleRPush}
          className="flex items-center gap-1 text-xs text-primary hover:text-primary/80"
          title="RPUSH (append)"
        >
          <ArrowDownToLine className="h-3 w-3" />
          R
        </button>
      </div>

      <div className="px-3 py-1 border-t border-border text-[10px] text-muted-foreground">
        {items.length} elements
      </div>
    </div>
  );
}
