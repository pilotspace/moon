import { useState, useCallback } from "react";
import { execCommand } from "@/lib/api";
import { toastError } from "@/lib/toast";
import { Plus, Trash2, Loader2 } from "lucide-react";

interface HashField {
  field: string;
  value: string;
  editing: boolean;
  editValue: string;
}

function parseHashArray(arr: unknown): HashField[] {
  if (!Array.isArray(arr)) return [];
  const fields: HashField[] = [];
  for (let i = 0; i < arr.length; i += 2) {
    fields.push({
      field: String(arr[i]),
      value: String(arr[i + 1] ?? ""),
      editing: false,
      editValue: String(arr[i + 1] ?? ""),
    });
  }
  return fields;
}

export function HashEditor({ keyName, value }: { keyName: string; value: unknown }) {
  const [fields, setFields] = useState<HashField[]>(() => parseHashArray(value));
  const [newField, setNewField] = useState("");
  const [newValue, setNewValue] = useState("");
  const [saving, setSaving] = useState<string | null>(null);

  const handleSave = useCallback(async (field: string, val: string) => {
    // Guard against double-save: Enter key triggers handleSave, then setting
    // editing=false unmounts the input which fires onBlur → handleSave again.
    // Using functional setState prevents state staleness; the saving flag
    // prevents two concurrent HSETs for the same field.
    if (saving === field) return;
    setSaving(field);
    try {
      await execCommand("HSET", [keyName, field, val]);
      setFields((prev) =>
        prev.map((f) =>
          f.field === field ? { ...f, value: val, editValue: val, editing: false } : f
        )
      );
    } catch (err) {
      toastError(`HSET ${keyName} ${field} failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setSaving(null);
    }
  }, [keyName, saving]);

  const handleDelete = useCallback(async (field: string) => {
    try {
      await execCommand("HDEL", [keyName, field]);
      setFields((prev) => prev.filter((f) => f.field !== field));
    } catch (err) {
      toastError(`HDEL ${keyName} ${field} failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }, [keyName]);

  const handleAdd = async () => {
    if (!newField.trim()) return;
    try {
      await execCommand("HSET", [keyName, newField, newValue]);
      setFields((prev) => [...prev, { field: newField, value: newValue, editing: false, editValue: newValue }]);
      setNewField("");
      setNewValue("");
    } catch (err) {
      toastError(`HSET ${keyName} ${newField} failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="grid grid-cols-[1fr_1fr_60px] gap-2 px-3 py-1.5 border-b border-border text-[10px] font-medium text-muted-foreground uppercase">
        <span>Field</span>
        <span>Value</span>
        <span />
      </div>

      {/* Fields */}
      <div className="flex-1 overflow-auto">
        {fields.map((f) => (
          <div
            key={f.field}
            className="grid grid-cols-[1fr_1fr_60px] gap-2 px-3 py-1 border-b border-border/50 items-center"
          >
            <span className="text-xs font-mono text-zinc-300 truncate">{f.field}</span>
            {f.editing ? (
              <input
                value={f.editValue}
                onChange={(e) =>
                  setFields((prev) =>
                    prev.map((x) => (x.field === f.field ? { ...x, editValue: e.target.value } : x))
                  )
                }
                onKeyDown={(e) => e.key === "Enter" && handleSave(f.field, f.editValue)}
                onBlur={() => handleSave(f.field, f.editValue)}
                className="text-xs font-mono bg-zinc-900 border border-border rounded px-1.5 py-0.5 text-zinc-100 outline-none"
                autoFocus
              />
            ) : (
              <span
                className="text-xs font-mono text-zinc-400 truncate cursor-pointer hover:text-zinc-100"
                onClick={() =>
                  setFields((prev) =>
                    prev.map((x) => (x.field === f.field ? { ...x, editing: true } : x))
                  )
                }
              >
                {f.value}
              </span>
            )}
            <div className="flex items-center gap-1 justify-end">
              {saving === f.field && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
              <button onClick={() => handleDelete(f.field)} className="text-muted-foreground/50 hover:text-destructive">
                <Trash2 className="h-3 w-3" />
              </button>
            </div>
          </div>
        ))}
      </div>

      {/* Add new field */}
      <div className="grid grid-cols-[1fr_1fr_60px] gap-2 px-3 py-2 border-t border-border items-center">
        <input
          value={newField}
          onChange={(e) => setNewField(e.target.value)}
          placeholder="field"
          className="text-xs font-mono bg-zinc-900 border border-border rounded px-1.5 py-0.5 text-zinc-100 outline-none placeholder:text-muted-foreground/40"
        />
        <input
          value={newValue}
          onChange={(e) => setNewValue(e.target.value)}
          placeholder="value"
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
          className="text-xs font-mono bg-zinc-900 border border-border rounded px-1.5 py-0.5 text-zinc-100 outline-none placeholder:text-muted-foreground/40"
        />
        <button
          onClick={handleAdd}
          className="flex items-center justify-center gap-1 text-xs text-primary hover:text-primary/80"
        >
          <Plus className="h-3 w-3" />
        </button>
      </div>

      <div className="px-3 py-1 border-t border-border text-[10px] text-muted-foreground">
        {fields.length} fields
      </div>
    </div>
  );
}
