import { useState, useMemo, useCallback } from "react";
import { useConsoleStore } from "@/stores/console";
import type { HistoryEntry, QueryLanguage } from "@/types/console";

const LANGUAGE_LABELS: Record<QueryLanguage, { text: string; color: string }> = {
  resp: { text: "RESP", color: "bg-blue-400/20 text-blue-400" },
  cypher: { text: "Cypher", color: "bg-purple-400/20 text-purple-400" },
  ftsearch: { text: "FT", color: "bg-green-400/20 text-green-400" },
};

function formatRelativeTime(timestamp: number): string {
  const delta = Date.now() - timestamp;
  const seconds = Math.floor(delta / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function truncate(text: string, max: number): string {
  if (text.length <= max) return text;
  return text.slice(0, max) + "\u2026";
}

const VISIBLE_WINDOW = 50;
const BUFFER = 10;

export function HistoryPanel() {
  const tabs = useConsoleStore((s) => s.tabs);
  const activeTabId = useConsoleStore((s) => s.activeTabId);
  const updateTabContent = useConsoleStore((s) => s.updateTabContent);
  const searchHistory = useConsoleStore((s) => s.searchHistory);

  const [searchQuery, setSearchQuery] = useState("");
  const [scrollOffset, setScrollOffset] = useState(0);

  const activeTab = tabs.find((t) => t.id === activeTabId);

  const entries: HistoryEntry[] = useMemo(() => {
    if (!activeTab) return [];
    if (searchQuery.trim()) {
      return searchHistory(activeTabId, searchQuery);
    }
    return activeTab.history;
  }, [activeTab, activeTabId, searchQuery, searchHistory]);

  const visibleEntries = useMemo(() => {
    if (entries.length <= VISIBLE_WINDOW + BUFFER) return entries;
    const start = Math.max(0, scrollOffset - BUFFER);
    const end = Math.min(entries.length, scrollOffset + VISIBLE_WINDOW + BUFFER);
    return entries.slice(start, end);
  }, [entries, scrollOffset]);

  const handleScroll = useCallback(
    (e: React.UIEvent<HTMLDivElement>) => {
      const target = e.currentTarget;
      const itemHeight = 56; // approximate item height
      const newOffset = Math.floor(target.scrollTop / itemHeight);
      if (Math.abs(newOffset - scrollOffset) > 5) {
        setScrollOffset(newOffset);
      }
    },
    [scrollOffset],
  );

  const handleRestore = useCallback(
    (entry: HistoryEntry) => {
      if (activeTab) {
        updateTabContent(activeTab.id, entry.query);
      }
    },
    [activeTab, updateTabContent],
  );

  if (!activeTab) return null;

  return (
    <div className="flex flex-col h-full bg-zinc-950 border-l border-zinc-800">
      <div className="px-3 py-2 border-b border-zinc-800">
        <input
          type="text"
          placeholder="Search history..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full bg-zinc-900 border border-zinc-800 rounded px-3 py-1.5 text-sm text-zinc-300 placeholder:text-zinc-600 focus:outline-none focus:border-zinc-600"
        />
      </div>

      <div className="flex-1 overflow-y-auto" onScroll={handleScroll}>
        {entries.length === 0 ? (
          <div className="p-4 text-sm text-zinc-500 text-center">
            {searchQuery
              ? "No matching queries found."
              : "No history yet. Execute a query with Cmd+Enter."}
          </div>
        ) : (
          <div>
            {visibleEntries.map((entry, i) => {
              const lang = LANGUAGE_LABELS[entry.language];
              return (
                <button
                  key={`${entry.timestamp}-${i}`}
                  onClick={() => handleRestore(entry)}
                  className="w-full text-left px-3 py-2 border-b border-zinc-800/50 hover:bg-zinc-800/50 transition-colors group"
                >
                  <div className="flex items-center gap-2 mb-0.5">
                    <span
                      className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${lang.color}`}
                    >
                      {lang.text}
                    </span>
                    <span className="text-[10px] text-zinc-600">
                      {formatRelativeTime(entry.timestamp)}
                    </span>
                    {entry.result.error && (
                      <span className="inline-block w-1.5 h-1.5 rounded-full bg-red-500" />
                    )}
                  </div>
                  <div className="font-mono text-sm text-zinc-400 group-hover:text-zinc-200 transition-colors">
                    {truncate(entry.query, 80)}
                  </div>
                </button>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
