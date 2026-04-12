import { useEffect } from "react";
import { useBrowserStore } from "@/stores/browser";
import { cn } from "@/lib/utils";
import { ChevronRight, ChevronDown, FolderOpen, Folder } from "lucide-react";
import type { NamespaceNode } from "@/types/browser";

function TreeNode({
  node,
  depth,
  activePrefix,
  onSelect,
}: {
  node: NamespaceNode;
  depth: number;
  activePrefix: string;
  onSelect: (prefix: string) => void;
}) {
  const isActive = activePrefix === node.fullPrefix;
  const hasChildren = node.children.size > 0;

  const toggle = () => {
    node.expanded = !node.expanded;
    onSelect(node.fullPrefix);
  };

  return (
    <>
      <button
        className={cn(
          "flex items-center gap-1.5 w-full text-left px-2 py-1 text-xs rounded transition-colors",
          isActive ? "bg-accent text-accent-foreground" : "text-muted-foreground hover:bg-accent/30",
        )}
        style={{ paddingLeft: `${depth * 16 + 8}px` }}
        onClick={toggle}
      >
        {hasChildren ? (
          node.expanded ? (
            <ChevronDown className="h-3 w-3 shrink-0" />
          ) : (
            <ChevronRight className="h-3 w-3 shrink-0" />
          )
        ) : (
          <span className="w-3" />
        )}
        {node.expanded ? (
          <FolderOpen className="h-3.5 w-3.5 text-primary shrink-0" />
        ) : (
          <Folder className="h-3.5 w-3.5 shrink-0" />
        )}
        <span className="truncate">{node.name}</span>
        <span className="ml-auto text-[10px] text-muted-foreground/60 tabular-nums">
          {node.keyCount}
        </span>
      </button>
      {node.expanded &&
        [...node.children.values()].map((child) => (
          <TreeNode
            key={child.fullPrefix}
            node={child}
            depth={depth + 1}
            activePrefix={activePrefix}
            onSelect={onSelect}
          />
        ))}
    </>
  );
}

export function NamespaceTree() {
  const treeRoot = useBrowserStore((s) => s.treeRoot);
  const activePrefix = useBrowserStore((s) => s.activePrefix);
  const setActivePrefix = useBrowserStore((s) => s.setActivePrefix);
  const buildTree = useBrowserStore((s) => s.buildTree);
  const keys = useBrowserStore((s) => s.keys);

  // Rebuild tree when keys change
  useEffect(() => {
    if (keys.length > 0) buildTree();
  }, [keys.length, buildTree]);

  return (
    <div className="flex flex-col h-full">
      <div className="px-3 py-2 text-xs font-medium text-muted-foreground border-b border-border">
        Namespaces
      </div>
      <div className="flex-1 overflow-auto py-1">
        {/* All keys root */}
        <button
          className={cn(
            "flex items-center gap-1.5 w-full text-left px-2 py-1 text-xs rounded transition-colors",
            activePrefix === "" ? "bg-accent text-accent-foreground" : "text-muted-foreground hover:bg-accent/30",
          )}
          onClick={() => setActivePrefix("")}
        >
          <FolderOpen className="h-3.5 w-3.5 text-primary" />
          <span>All Keys</span>
        </button>

        {treeRoot &&
          [...treeRoot.children.values()].map((child) => (
            <TreeNode
              key={child.fullPrefix}
              node={child}
              depth={1}
              activePrefix={activePrefix}
              onSelect={setActivePrefix}
            />
          ))}
      </div>
    </div>
  );
}
