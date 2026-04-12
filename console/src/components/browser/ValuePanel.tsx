import { useEffect, useState } from "react";
import { getKeyValue } from "@/lib/api";
import { TtlManager } from "@/components/browser/TtlManager";
import { KeyMetadata } from "@/components/browser/KeyMetadata";
import { StringEditor } from "@/components/browser/editors/StringEditor";
import { HashEditor } from "@/components/browser/editors/HashEditor";
import { ListEditor } from "@/components/browser/editors/ListEditor";
import { SetEditor } from "@/components/browser/editors/SetEditor";
import { ZSetEditor } from "@/components/browser/editors/ZSetEditor";
import { StreamViewer } from "@/components/browser/editors/StreamViewer";
import type { KeyType } from "@/types/browser";
import { Loader2 } from "lucide-react";

export function ValuePanel({ keyName }: { keyName: string }) {
  const [loading, setLoading] = useState(true);
  const [keyType, setKeyType] = useState<KeyType | null>(null);
  const [value, setValue] = useState<unknown>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    getKeyValue(keyName)
      .then((res) => {
        setKeyType(res.type as KeyType);
        setValue(res.value);
      })
      .catch((err) => setError(String(err)))
      .finally(() => setLoading(false));
  }, [keyName]);

  if (loading) {
    return (
      <div className="flex flex-col h-full">
        <div className="flex-1 flex items-center justify-center">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col h-full">
        <div className="flex-1 flex items-center justify-center text-sm text-destructive">
          {error}
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      {/* Key header */}
      <div className="px-4 py-3 border-b border-border space-y-2">
        <h2 className="text-sm font-medium text-zinc-100 font-mono truncate">{keyName}</h2>
        {keyType && <KeyMetadata keyName={keyName} keyType={keyType} />}
        <TtlManager keyName={keyName} />
      </div>

      {/* Type-dispatched editor */}
      <div className="flex-1 min-h-0 overflow-hidden">
        {keyType === "string" && <StringEditor keyName={keyName} value={String(value ?? "")} />}
        {keyType === "hash" && <HashEditor keyName={keyName} value={value} />}
        {keyType === "list" && <ListEditor keyName={keyName} value={value} />}
        {keyType === "set" && <SetEditor keyName={keyName} value={value} />}
        {keyType === "zset" && <ZSetEditor keyName={keyName} value={value} />}
        {keyType === "stream" && <StreamViewer keyName={keyName} value={value} />}
      </div>
    </div>
  );
}
