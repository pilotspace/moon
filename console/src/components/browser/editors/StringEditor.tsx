import { useState, useEffect } from "react";
import { execCommand } from "@/lib/api";
import { Save, FileJson, FileText, Binary } from "lucide-react";
import { cn } from "@/lib/utils";

type ViewMode = "raw" | "json" | "hex";

function isValidJson(str: string): boolean {
  try { JSON.parse(str); return true; } catch { return false; }
}

function toHex(str: string): string {
  return Array.from(new TextEncoder().encode(str))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join(" ");
}

export function StringEditor({ keyName, value }: { keyName: string; value: string }) {
  const detectedJson = isValidJson(value);
  const [mode, setMode] = useState<ViewMode>(detectedJson ? "json" : "raw");
  const [editValue, setEditValue] = useState(
    detectedJson ? JSON.stringify(JSON.parse(value), null, 2) : value
  );
  const [saving, setSaving] = useState(false);
  const [dirty, setDirty] = useState(false);

  useEffect(() => {
    const formatted = detectedJson ? JSON.stringify(JSON.parse(value), null, 2) : value;
    setEditValue(mode === "json" && detectedJson ? formatted : value);
    setDirty(false);
  }, [keyName, value]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleSave = async () => {
    setSaving(true);
    try {
      const saveVal = mode === "json" ? JSON.stringify(JSON.parse(editValue)) : editValue;
      await execCommand("SET", [keyName, saveVal]);
      setDirty(false);
    } catch (err) {
      console.error("Save failed:", err);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Mode tabs */}
      <div className="flex items-center gap-1 px-3 py-1.5 border-b border-border">
        {([
          { id: "raw" as const, icon: FileText, label: "Raw" },
          { id: "json" as const, icon: FileJson, label: "JSON" },
          { id: "hex" as const, icon: Binary, label: "Hex" },
        ]).map(({ id, icon: Icon, label }) => (
          <button
            key={id}
            onClick={() => setMode(id)}
            className={cn(
              "flex items-center gap-1 px-2 py-0.5 text-xs rounded transition-colors",
              mode === id ? "bg-accent text-accent-foreground" : "text-muted-foreground hover:text-zinc-100",
            )}
          >
            <Icon className="h-3 w-3" />
            {label}
          </button>
        ))}
        <div className="flex-1" />
        {dirty && (
          <button
            onClick={handleSave}
            disabled={saving}
            className="flex items-center gap-1 px-2 py-0.5 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
          >
            <Save className="h-3 w-3" />
            {saving ? "Saving..." : "Save"}
          </button>
        )}
      </div>

      {/* Editor */}
      {mode === "hex" ? (
        <pre className="flex-1 overflow-auto p-3 text-xs font-mono text-zinc-300 whitespace-pre-wrap break-all">
          {toHex(value)}
        </pre>
      ) : (
        <textarea
          value={editValue}
          onChange={(e) => { setEditValue(e.target.value); setDirty(true); }}
          className="flex-1 resize-none bg-transparent p-3 text-xs font-mono text-zinc-100 outline-none"
          spellCheck={false}
        />
      )}

      {/* Footer */}
      <div className="px-3 py-1 border-t border-border text-[10px] text-muted-foreground">
        {value.length} bytes {detectedJson && " | valid JSON detected"}
      </div>
    </div>
  );
}
