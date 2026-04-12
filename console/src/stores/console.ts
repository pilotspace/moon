import { create } from "zustand";
import { persist } from "zustand/middleware";
import type { ConsoleTab, HistoryEntry, QueryResult, QueryLanguage } from "@/types/console";

interface ConsoleState {
  tabs: ConsoleTab[];
  activeTabId: string;

  // Tab actions
  addTab: () => void;
  closeTab: (id: string) => void;
  setActiveTab: (id: string) => void;
  updateTabContent: (id: string, content: string) => void;
  setTabLanguage: (id: string, language: QueryLanguage) => void;

  // Execution
  setExecuting: (id: string, executing: boolean) => void;
  setResult: (id: string, result: QueryResult) => void;

  // History — per-tab
  addToHistory: (id: string, entry: HistoryEntry) => void;
  navigateHistory: (id: string, direction: "up" | "down") => void;

  // History search
  searchHistory: (id: string, query: string) => HistoryEntry[];
}

const MAX_HISTORY = 100;

function createDefaultTab(name: string): ConsoleTab {
  return {
    id: crypto.randomUUID(),
    name,
    language: "resp",
    content: "",
    result: null,
    history: [],
    historyIndex: -1,
    executing: false,
  };
}

/** Detect query language from content prefix */
function detectLanguage(content: string): QueryLanguage {
  const trimmed = content.trimStart().toUpperCase();
  if (
    trimmed.startsWith("MATCH ") ||
    trimmed.startsWith("CREATE ") ||
    trimmed.startsWith("MERGE ") ||
    trimmed.startsWith("GRAPH.")
  ) {
    return "cypher";
  }
  if (trimmed.startsWith("FT.")) {
    return "ftsearch";
  }
  return "resp";
}

const defaultTab = createDefaultTab("Query 1");

export const useConsoleStore = create<ConsoleState>()(
  persist(
    (set, get) => ({
      tabs: [defaultTab],
      activeTabId: defaultTab.id,

      addTab: () => {
        const { tabs } = get();
        const num = tabs.length + 1;
        const tab = createDefaultTab(`Query ${num}`);
        set({ tabs: [...tabs, tab], activeTabId: tab.id });
      },

      closeTab: (id) => {
        const { tabs, activeTabId } = get();
        if (tabs.length === 1) {
          // Always keep at least one tab
          const fresh = createDefaultTab("Query 1");
          set({ tabs: [fresh], activeTabId: fresh.id });
          return;
        }
        const idx = tabs.findIndex((t) => t.id === id);
        const next = tabs.filter((t) => t.id !== id);
        let newActive = activeTabId;
        if (activeTabId === id) {
          // Switch to adjacent tab
          const newIdx = Math.min(idx, next.length - 1);
          newActive = next[newIdx].id;
        }
        set({ tabs: next, activeTabId: newActive });
      },

      setActiveTab: (id) => set({ activeTabId: id }),

      updateTabContent: (id, content) => {
        const language = detectLanguage(content);
        set((s) => ({
          tabs: s.tabs.map((t) =>
            t.id === id ? { ...t, content, language } : t,
          ),
        }));
      },

      setTabLanguage: (id, language) => {
        set((s) => ({
          tabs: s.tabs.map((t) =>
            t.id === id ? { ...t, language } : t,
          ),
        }));
      },

      setExecuting: (id, executing) => {
        set((s) => ({
          tabs: s.tabs.map((t) =>
            t.id === id ? { ...t, executing } : t,
          ),
        }));
      },

      setResult: (id, result) => {
        set((s) => ({
          tabs: s.tabs.map((t) =>
            t.id === id ? { ...t, result, executing: false } : t,
          ),
        }));
      },

      addToHistory: (id, entry) => {
        set((s) => ({
          tabs: s.tabs.map((t) => {
            if (t.id !== id) return t;
            const history = [entry, ...t.history].slice(0, MAX_HISTORY);
            return { ...t, history, historyIndex: -1 };
          }),
        }));
      },

      navigateHistory: (id, direction) => {
        set((s) => ({
          tabs: s.tabs.map((t) => {
            if (t.id !== id) return t;
            if (t.history.length === 0) return t;

            if (direction === "up") {
              const newIndex = t.historyIndex === -1 ? 0 : Math.min(t.historyIndex + 1, t.history.length - 1);
              return {
                ...t,
                historyIndex: newIndex,
                content: t.history[newIndex].query,
              };
            } else {
              // direction === "down"
              if (t.historyIndex <= 0) {
                return { ...t, historyIndex: -1, content: "" };
              }
              const newIndex = t.historyIndex - 1;
              return {
                ...t,
                historyIndex: newIndex,
                content: t.history[newIndex].query,
              };
            }
          }),
        }));
      },

      searchHistory: (id, query) => {
        const tab = get().tabs.find((t) => t.id === id);
        if (!tab) return [];
        const lower = query.toLowerCase();
        return tab.history.filter((e) => e.query.toLowerCase().includes(lower));
      },
    }),
    {
      name: "moon-console",
      partialize: (state) => ({
        tabs: state.tabs.map((t) => ({
          ...t,
          // Don't persist transient state
          executing: false,
          result: null,
        })),
        activeTabId: state.activeTabId,
      }),
    },
  ),
);
