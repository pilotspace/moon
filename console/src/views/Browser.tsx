import { KeyList } from "@/components/browser/KeyList";
import { NamespaceTree } from "@/components/browser/NamespaceTree";
import { KeyToolbar } from "@/components/browser/KeyToolbar";
import { useBrowserStore } from "@/stores/browser";
import { Card } from "@/components/ui/card";

export function Browser() {
  const selectedKey = useBrowserStore((s) => s.selectedKey);

  return (
    <div className="flex flex-col h-[calc(100vh-3rem)]">
      <h1 className="text-xl font-semibold text-zinc-100 mb-4">Data Browser</h1>

      <div className="flex-1 flex gap-4 min-h-0">
        {/* Left: Namespace tree */}
        <Card className="w-56 shrink-0 flex flex-col overflow-hidden">
          <NamespaceTree />
        </Card>

        {/* Center: Key list */}
        <Card className="flex-1 flex flex-col overflow-hidden">
          <KeyToolbar />
          <KeyList />
        </Card>

        {/* Right: Value editor panel (placeholder for Plan 02) */}
        {selectedKey && (
          <Card className="w-[420px] shrink-0 flex flex-col overflow-hidden">
            <div className="px-4 py-3 border-b border-border">
              <h2 className="text-sm font-medium text-zinc-100 truncate">{selectedKey}</h2>
            </div>
            <div className="flex-1 flex items-center justify-center text-sm text-muted-foreground">
              Value editor - Phase 130 Plan 02
            </div>
          </Card>
        )}
      </div>
    </div>
  );
}
