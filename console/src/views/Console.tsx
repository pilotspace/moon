import { useEffect, useState, useCallback } from "react";
import { useConsoleStore } from "@/stores/console";
import { connectWS, disconnectWS } from "@/lib/ws";
import { TabBar } from "@/components/console/TabBar";
import { Editor } from "@/components/console/Editor";
import { ResultPanel } from "@/components/console/ResultPanel";
import { HistoryPanel } from "@/components/console/HistoryPanel";
import { History } from "lucide-react";

export function Console() {
  const [showHistory, setShowHistory] = useState(false);
  const [resultHeight, setResultHeight] = useState(40); // percentage

  const tabs = useConsoleStore((s) => s.tabs);
  const activeTabId = useConsoleStore((s) => s.activeTabId);
  const addTab = useConsoleStore((s) => s.addTab);
  const closeTab = useConsoleStore((s) => s.closeTab);
  const navigateHistory = useConsoleStore((s) => s.navigateHistory);

  const activeTab = tabs.find((t) => t.id === activeTabId);

  // WebSocket lifecycle
  useEffect(() => {
    connectWS();
    return () => disconnectWS();
  }, []);

  // Drag resize handler for result panel
  const handleDragStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      const startY = e.clientY;
      const container = (e.target as HTMLElement).closest("[data-console-root]");
      if (!container) return;
      const containerRect = container.getBoundingClientRect();
      const startHeight = resultHeight;

      const onMove = (ev: MouseEvent) => {
        const deltaY = startY - ev.clientY;
        const deltaPct = (deltaY / containerRect.height) * 100;
        const newHeight = Math.min(80, Math.max(10, startHeight + deltaPct));
        setResultHeight(newHeight);
      };

      const onUp = () => {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
      };

      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    },
    [resultHeight],
  );

  // Global keyboard shortcuts
  // Cmd+Enter is handled in Editor.tsx via Monaco keybinding
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (!e.metaKey && !e.ctrlKey) return;

      switch (e.key) {
        case "t":
          // Cmd+T: new tab
          e.preventDefault();
          addTab();
          break;
        case "w":
          // Cmd+W: close current tab
          e.preventDefault();
          closeTab(activeTabId);
          break;
        case "ArrowUp":
          // Cmd+Up: previous history entry
          e.preventDefault();
          navigateHistory(activeTabId, "up");
          break;
        case "ArrowDown":
          // Cmd+Down: next history entry
          e.preventDefault();
          navigateHistory(activeTabId, "down");
          break;
        case "h":
          // Cmd+H: toggle history panel
          e.preventDefault();
          setShowHistory((prev) => !prev);
          break;
      }
    };

    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [activeTabId, addTab, closeTab, navigateHistory]);

  return (
    <div className="flex flex-col h-full -m-6" data-console-root>
      {/* Tab bar */}
      <div className="flex items-center border-b border-zinc-800">
        <div className="flex-1">
          <TabBar />
        </div>
        <button
          onClick={() => setShowHistory((prev) => !prev)}
          className={`px-3 py-2 text-sm transition-colors ${
            showHistory ? "text-white bg-zinc-800" : "text-zinc-400 hover:text-white"
          }`}
          title="Toggle history (Cmd+H)"
        >
          <History size={16} />
        </button>
      </div>

      {/* Main content area */}
      <div className="flex flex-1 min-h-0">
        {/* Editor + Result column */}
        <div className="flex flex-col flex-1 min-w-0">
          {/* Editor area */}
          <div className="flex-1 min-h-0" style={{ flex: `1 1 ${100 - resultHeight}%` }}>
            <Editor />
          </div>

          {/* Resize handle */}
          <div
            className="h-1 bg-zinc-800 hover:bg-zinc-600 cursor-row-resize flex-shrink-0 transition-colors"
            onMouseDown={handleDragStart}
          />

          {/* Result panel */}
          <div
            className="min-h-0 overflow-hidden border-t border-zinc-800"
            style={{ flex: `0 0 ${resultHeight}%` }}
          >
            <ResultPanel
              result={activeTab?.result ?? null}
              executing={activeTab?.executing ?? false}
            />
          </div>
        </div>

        {/* History sidebar */}
        {showHistory && (
          <div className="w-64 flex-shrink-0">
            <HistoryPanel />
          </div>
        )}
      </div>
    </div>
  );
}
