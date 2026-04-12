import { useState, useCallback } from "react";
import { execCommand } from "@/lib/api";
import { Plus, X } from "lucide-react";
import { cn } from "@/lib/utils";

export function SetEditor({ keyName, value }: { keyName: string; value: unknown }) {
  const [members, setMembers] = useState<string[]>(() => (Array.isArray(value) ? value.map(String) : []));
  const [newMember, setNewMember] = useState("");

  const handleAdd = useCallback(async () => {
    if (!newMember.trim()) return;
    await execCommand("SADD", [keyName, newMember]);
    setMembers((prev) => [...prev, newMember]);
    setNewMember("");
  }, [keyName, newMember]);

  const handleRemove = useCallback(async (member: string) => {
    await execCommand("SREM", [keyName, member]);
    setMembers((prev) => prev.filter((m) => m !== member));
  }, [keyName]);

  return (
    <div className="flex flex-col h-full">
      {/* Tag cloud */}
      <div className="flex-1 overflow-auto p-3">
        <div className="flex flex-wrap gap-1.5">
          {members.map((member) => (
            <span
              key={member}
              className={cn(
                "inline-flex items-center gap-1 rounded-md border border-border bg-zinc-900 px-2 py-0.5 text-xs font-mono text-zinc-300",
                "group hover:border-destructive/50"
              )}
            >
              {member}
              <button
                onClick={() => handleRemove(member)}
                className="text-muted-foreground/40 group-hover:text-destructive transition-colors"
              >
                <X className="h-2.5 w-2.5" />
              </button>
            </span>
          ))}
        </div>
      </div>

      {/* Add member */}
      <div className="flex items-center gap-2 px-3 py-2 border-t border-border">
        <input
          value={newMember}
          onChange={(e) => setNewMember(e.target.value)}
          placeholder="new member"
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
          className="flex-1 text-xs font-mono bg-zinc-900 border border-border rounded px-1.5 py-0.5 text-zinc-100 outline-none placeholder:text-muted-foreground/40"
        />
        <button
          onClick={handleAdd}
          className="flex items-center gap-1 text-xs text-primary hover:text-primary/80"
        >
          <Plus className="h-3 w-3" /> Add
        </button>
      </div>

      <div className="px-3 py-1 border-t border-border text-[10px] text-muted-foreground">
        {members.length} members
      </div>
    </div>
  );
}
