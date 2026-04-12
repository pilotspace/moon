import { useConsoleStore } from "@/stores/console";
import { Plus, X } from "lucide-react";
import type { QueryLanguage } from "@/types/console";

const LANGUAGE_DOTS: Record<QueryLanguage, string> = {
  resp: "bg-blue-400",
  cypher: "bg-purple-400",
  ftsearch: "bg-green-400",
};

export function TabBar() {
  const tabs = useConsoleStore((s) => s.tabs);
  const activeTabId = useConsoleStore((s) => s.activeTabId);
  const setActiveTab = useConsoleStore((s) => s.setActiveTab);
  const closeTab = useConsoleStore((s) => s.closeTab);
  const addTab = useConsoleStore((s) => s.addTab);

  return (
    <div className="flex items-center bg-zinc-900 border-b border-zinc-800 overflow-x-auto">
      {tabs.map((tab) => {
        const isActive = tab.id === activeTabId;
        return (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`
              group flex items-center gap-2 px-3 py-2 text-sm whitespace-nowrap
              border-r border-zinc-800 transition-colors
              ${isActive
                ? "bg-zinc-800 text-white"
                : "text-zinc-400 hover:text-white hover:bg-zinc-800/50"
              }
            `}
          >
            <span className={`inline-block w-2 h-2 rounded-full ${LANGUAGE_DOTS[tab.language]}`} />
            <span>{tab.name}</span>
            {tab.executing && (
              <span className="inline-block w-2 h-2 rounded-full bg-amber-400 animate-pulse" />
            )}
            <span
              role="button"
              onClick={(e) => {
                e.stopPropagation();
                closeTab(tab.id);
              }}
              className="ml-1 p-0.5 rounded hover:bg-zinc-700 opacity-0 group-hover:opacity-100 transition-opacity"
            >
              <X size={14} />
            </span>
          </button>
        );
      })}

      <button
        onClick={addTab}
        className="flex items-center justify-center px-3 py-2 text-zinc-400 hover:text-white hover:bg-zinc-800/50 transition-colors"
        title="New tab"
      >
        <Plus size={16} />
      </button>
    </div>
  );
}
