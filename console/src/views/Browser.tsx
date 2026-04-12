import { KeyList } from "@/components/browser/KeyList";
import { NamespaceTree } from "@/components/browser/NamespaceTree";
import { KeyToolbar } from "@/components/browser/KeyToolbar";
import { ValuePanel } from "@/components/browser/ValuePanel";
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

        {/* Right: Value editor panel */}
        {selectedKey && (
          <Card className="w-[420px] shrink-0 flex flex-col overflow-hidden">
            <ValuePanel keyName={selectedKey} />
          </Card>
        )}
      </div>
    </div>
  );
}
