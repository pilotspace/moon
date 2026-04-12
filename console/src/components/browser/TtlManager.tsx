import { useState, useEffect, useRef } from "react";
import { getKeyTtl, setKeyTtl } from "@/lib/api";
import { getTtlTier } from "@/types/browser";
import { Clock, Save, X } from "lucide-react";
import { cn } from "@/lib/utils";

function formatTtlLive(ttl: number): string {
  if (ttl <= 0) return "expired";
  const h = Math.floor(ttl / 3600);
  const m = Math.floor((ttl % 3600) / 60);
  const s = ttl % 60;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

export function TtlManager({ keyName }: { keyName: string }) {
  const [ttl, setTtl] = useState<number | null>(null);
  const [editing, setEditing] = useState(false);
  const [inputValue, setInputValue] = useState("");
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Fetch TTL on mount and start countdown
  useEffect(() => {
    let mounted = true;

    getKeyTtl(keyName).then((t) => {
      if (mounted) setTtl(t);
    });

    return () => { mounted = false; };
  }, [keyName]);

  // Live countdown
  useEffect(() => {
    if (ttl !== null && ttl > 0) {
      intervalRef.current = setInterval(() => {
        setTtl((prev) => (prev !== null && prev > 0 ? prev - 1 : prev));
      }, 1000);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [ttl !== null && ttl > 0]); // eslint-disable-line react-hooks/exhaustive-deps

  const tier = getTtlTier(ttl);

  const handleSave = async () => {
    const seconds = parseInt(inputValue, 10);
    if (isNaN(seconds) || seconds <= 0) return;
    await setKeyTtl(keyName, seconds);
    setTtl(seconds);
    setEditing(false);
  };

  const handlePersist = async () => {
    await setKeyTtl(keyName, -1);
    setTtl(-1);
  };

  return (
    <div className="flex items-center gap-2 text-xs">
      <Clock className="h-3.5 w-3.5 text-muted-foreground" />

      {editing ? (
        <div className="flex items-center gap-1">
          <input
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSave()}
            placeholder="seconds"
            className="w-20 bg-zinc-900 border border-border rounded px-1.5 py-0.5 text-xs text-zinc-100 outline-none"
            autoFocus
          />
          <button onClick={handleSave} className="text-primary hover:text-primary/80">
            <Save className="h-3 w-3" />
          </button>
          <button onClick={() => setEditing(false)} className="text-muted-foreground hover:text-zinc-100">
            <X className="h-3 w-3" />
          </button>
        </div>
      ) : (
        <>
          <span
            className={cn(
              "tabular-nums font-mono",
              tier === "green" && "text-green-400",
              tier === "amber" && "text-amber-400",
              tier === "red" && "text-red-400 animate-pulse",
              tier === "none" && "text-muted-foreground",
            )}
          >
            {ttl === null ? "..." : ttl === -1 ? "persistent" : formatTtlLive(ttl)}
          </span>
          <button
            onClick={() => { setEditing(true); setInputValue(ttl !== null && ttl > 0 ? String(ttl) : ""); }}
            className="text-[10px] text-muted-foreground hover:text-zinc-100 transition-colors"
          >
            Set TTL
          </button>
          {ttl !== null && ttl > 0 && (
            <button
              onClick={handlePersist}
              className="text-[10px] text-muted-foreground hover:text-zinc-100 transition-colors"
            >
              Persist
            </button>
          )}
        </>
      )}
    </div>
  );
}
