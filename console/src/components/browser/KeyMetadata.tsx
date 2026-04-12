import { useEffect, useState } from "react";
import { getKeyMemory } from "@/lib/api";
import { TYPE_COLORS } from "@/types/browser";
import type { KeyType } from "@/types/browser";
import { HardDrive, MemoryStick } from "lucide-react";
import { cn } from "@/lib/utils";

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function KeyMetadata({ keyName, keyType }: { keyName: string; keyType: KeyType }) {
  const [memory, setMemory] = useState<number | null>(null);

  useEffect(() => {
    getKeyMemory(keyName).then(setMemory).catch(() => setMemory(null));
  }, [keyName]);

  return (
    <div className="flex items-center gap-3 text-xs">
      {/* Type badge */}
      <span className={cn("inline-flex items-center rounded px-2 py-0.5 text-[10px] font-semibold uppercase", TYPE_COLORS[keyType])}>
        {keyType}
      </span>

      {/* Memory usage */}
      <div className="flex items-center gap-1 text-muted-foreground">
        <MemoryStick className="h-3 w-3" />
        <span className="tabular-nums">{memory !== null ? formatBytes(memory) : "..."}</span>
      </div>

      {/* Storage tier (always "memory" for now) */}
      <div className="flex items-center gap-1 text-muted-foreground">
        <HardDrive className="h-3 w-3" />
        <span>memory</span>
      </div>
    </div>
  );
}
