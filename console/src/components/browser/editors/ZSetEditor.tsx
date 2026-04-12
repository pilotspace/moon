import { useState, useCallback } from "react";
import { execCommand } from "@/lib/api";
import { Plus, Trash2, Loader2 } from "lucide-react";

interface ZSetMember {
  member: string;
  score: number;
  editingScore: boolean;
  editScore: string;
}

function parseZSetArray(arr: unknown): ZSetMember[] {
  if (!Array.isArray(arr)) return [];
  const members: ZSetMember[] = [];
  for (let i = 0; i < arr.length; i += 2) {
    members.push({
      member: String(arr[i]),
      score: parseFloat(String(arr[i + 1] ?? "0")),
      editingScore: false,
      editScore: String(arr[i + 1] ?? "0"),
    });
  }
  // Sort by score ascending (rank order)
  members.sort((a, b) => a.score - b.score);
  return members;
}

export function ZSetEditor({ keyName, value }: { keyName: string; value: unknown }) {
  const [members, setMembers] = useState<ZSetMember[]>(() => parseZSetArray(value));
  const [newMember, setNewMember] = useState("");
  const [newScore, setNewScore] = useState("0");
  const [saving, setSaving] = useState<string | null>(null);

  const handleScoreSave = useCallback(async (member: string, scoreStr: string) => {
    const score = parseFloat(scoreStr);
    if (isNaN(score)) return;
    setSaving(member);
    try {
      await execCommand("ZADD", [keyName, String(score), member]);
      setMembers((prev) =>
        prev
          .map((m) => (m.member === member ? { ...m, score, editingScore: false, editScore: String(score) } : m))
          .sort((a, b) => a.score - b.score)
      );
    } finally {
      setSaving(null);
    }
  }, [keyName]);

  const handleRemove = useCallback(async (member: string) => {
    await execCommand("ZREM", [keyName, member]);
    setMembers((prev) => prev.filter((m) => m.member !== member));
  }, [keyName]);

  const handleAdd = async () => {
    if (!newMember.trim()) return;
    const score = parseFloat(newScore) || 0;
    await execCommand("ZADD", [keyName, String(score), newMember]);
    setMembers((prev) =>
      [...prev, { member: newMember, score, editingScore: false, editScore: String(score) }]
        .sort((a, b) => a.score - b.score)
    );
    setNewMember("");
    setNewScore("0");
  };

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="grid grid-cols-[40px_1fr_80px_40px] gap-2 px-3 py-1.5 border-b border-border text-[10px] font-medium text-muted-foreground uppercase">
        <span>Rank</span>
        <span>Member</span>
        <span className="text-right">Score</span>
        <span />
      </div>

      {/* Members */}
      <div className="flex-1 overflow-auto">
        {members.map((m, i) => (
          <div
            key={m.member}
            className="grid grid-cols-[40px_1fr_80px_40px] gap-2 px-3 py-1 border-b border-border/50 items-center"
          >
            <span className="text-[10px] text-muted-foreground tabular-nums">#{i + 1}</span>
            <span className="text-xs font-mono text-zinc-300 truncate">{m.member}</span>
            {m.editingScore ? (
              <input
                value={m.editScore}
                onChange={(e) =>
                  setMembers((prev) =>
                    prev.map((x) => (x.member === m.member ? { ...x, editScore: e.target.value } : x))
                  )
                }
                onKeyDown={(e) => e.key === "Enter" && handleScoreSave(m.member, m.editScore)}
                onBlur={() => handleScoreSave(m.member, m.editScore)}
                className="text-xs font-mono bg-zinc-900 border border-border rounded px-1.5 py-0.5 text-right text-zinc-100 outline-none w-full"
                autoFocus
              />
            ) : (
              <span
                className="text-xs font-mono text-amber-400 text-right cursor-pointer hover:text-amber-300 tabular-nums"
                onClick={() =>
                  setMembers((prev) =>
                    prev.map((x) => (x.member === m.member ? { ...x, editingScore: true } : x))
                  )
                }
              >
                {m.score}
              </span>
            )}
            <div className="flex items-center justify-end">
              {saving === m.member ? (
                <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
              ) : (
                <button onClick={() => handleRemove(m.member)} className="text-muted-foreground/50 hover:text-destructive">
                  <Trash2 className="h-3 w-3" />
                </button>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Add new */}
      <div className="grid grid-cols-[40px_1fr_80px_40px] gap-2 px-3 py-2 border-t border-border items-center">
        <span />
        <input
          value={newMember}
          onChange={(e) => setNewMember(e.target.value)}
          placeholder="member"
          className="text-xs font-mono bg-zinc-900 border border-border rounded px-1.5 py-0.5 text-zinc-100 outline-none placeholder:text-muted-foreground/40"
        />
        <input
          value={newScore}
          onChange={(e) => setNewScore(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
          placeholder="0"
          className="text-xs font-mono bg-zinc-900 border border-border rounded px-1.5 py-0.5 text-right text-zinc-100 outline-none placeholder:text-muted-foreground/40"
        />
        <button onClick={handleAdd} className="flex justify-center text-primary hover:text-primary/80">
          <Plus className="h-3 w-3" />
        </button>
      </div>

      <div className="px-3 py-1 border-t border-border text-[10px] text-muted-foreground">
        {members.length} members
      </div>
    </div>
  );
}
