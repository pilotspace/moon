import { beforeEach, describe, expect, it } from "vitest";
import { useConsoleStore } from "@/stores/console";

// Ensure localStorage is fresh between tests (persist middleware writes here).
const reset = () => {
  try {
    window.localStorage.removeItem("moon-console");
  } catch {
    // ignore — jsdom should always have localStorage
  }
};

describe("console store", () => {
  beforeEach(reset);

  it("exposes all expected action functions", () => {
    const s = useConsoleStore.getState();
    expect(typeof s.addTab).toBe("function");
    expect(typeof s.closeTab).toBe("function");
    expect(typeof s.setActiveTab).toBe("function");
    expect(typeof s.updateTabContent).toBe("function");
    expect(typeof s.setTabLanguage).toBe("function");
    expect(typeof s.setExecuting).toBe("function");
    expect(typeof s.setResult).toBe("function");
    expect(typeof s.addToHistory).toBe("function");
    expect(typeof s.navigateHistory).toBe("function");
    expect(typeof s.searchHistory).toBe("function");
  });

  it("addTab appends a new tab and makes it active", () => {
    const before = useConsoleStore.getState().tabs.length;
    useConsoleStore.getState().addTab();
    const after = useConsoleStore.getState().tabs;
    expect(after.length).toBe(before + 1);
    expect(useConsoleStore.getState().activeTabId).toBe(after[after.length - 1].id);
  });

  it("updateTabContent autodetects cypher language for MATCH prefix", () => {
    const id = useConsoleStore.getState().activeTabId;
    useConsoleStore.getState().updateTabContent(id, "MATCH (n) RETURN n");
    const tab = useConsoleStore.getState().tabs.find((t) => t.id === id)!;
    expect(tab.content).toBe("MATCH (n) RETURN n");
    expect(tab.language).toBe("cypher");
  });

  it("updateTabContent autodetects ftsearch language for FT. prefix", () => {
    const id = useConsoleStore.getState().activeTabId;
    useConsoleStore.getState().updateTabContent(id, "FT.SEARCH idx *");
    const tab = useConsoleStore.getState().tabs.find((t) => t.id === id)!;
    expect(tab.language).toBe("ftsearch");
  });

  it("updateTabContent defaults to resp language", () => {
    const id = useConsoleStore.getState().activeTabId;
    useConsoleStore.getState().updateTabContent(id, "GET mykey");
    const tab = useConsoleStore.getState().tabs.find((t) => t.id === id)!;
    expect(tab.language).toBe("resp");
  });

  it("closeTab always keeps at least one tab open", () => {
    // Start with a known state: only one tab
    const firstId = useConsoleStore.getState().tabs[0].id;
    useConsoleStore.getState().closeTab(firstId);
    const tabs = useConsoleStore.getState().tabs;
    expect(tabs.length).toBe(1); // new fresh tab created
    expect(tabs[0].id).not.toBe(firstId);
  });

  it("addToHistory caps at 100 entries and prepends newest", () => {
    const id = useConsoleStore.getState().activeTabId;
    const mkResult = () => ({ data: null, raw: "", elapsed_ms: 1 });
    for (let i = 0; i < 105; i++) {
      useConsoleStore.getState().addToHistory(id, {
        query: `cmd ${i}`,
        language: "resp",
        timestamp: Date.now(),
        result: mkResult(),
      });
    }
    const tab = useConsoleStore.getState().tabs.find((t) => t.id === id)!;
    expect(tab.history).toHaveLength(100);
    expect(tab.history[0].query).toBe("cmd 104"); // newest first
  });

  it("searchHistory filters entries by case-insensitive substring", () => {
    const id = useConsoleStore.getState().activeTabId;
    const mkResult = () => ({ data: null, raw: "", elapsed_ms: 1 });
    useConsoleStore.getState().addToHistory(id, {
      query: "GET users:1",
      language: "resp",
      timestamp: 1,
      result: mkResult(),
    });
    useConsoleStore.getState().addToHistory(id, {
      query: "HSET session:42 name x",
      language: "resp",
      timestamp: 2,
      result: mkResult(),
    });
    const hits = useConsoleStore.getState().searchHistory(id, "hset");
    expect(hits).toHaveLength(1);
    expect(hits[0].query).toContain("HSET");
  });
});
