import { describe, expect, it } from "vitest";
import { COMMANDS, createCompletionProvider } from "@/lib/completions";

// Minimal Monaco stub covering only what createCompletionProvider reaches into.
const fakeMonaco = {
  languages: {
    CompletionItemKind: { Function: 1 },
  },
} as never;

function mkModel(line: string) {
  return {
    getWordUntilPosition: (_pos: unknown) => {
      // Match Monaco's "word" extraction: the trailing alphanumeric/underscore run.
      const match = line.match(/([A-Za-z0-9_]*)$/);
      const word = match ? match[1] : "";
      const startColumn = line.length - word.length + 1;
      return {
        word,
        startColumn,
        endColumn: line.length + 1,
      };
    },
    getLineContent: (_ln: number) => line,
  } as never;
}

function callProvider(line: string): string[] {
  const provider = createCompletionProvider(fakeMonaco);
  const pos = { lineNumber: 1, column: line.length + 1 } as never;
  // provideCompletionItems is declared on the provider interface; we registered it.
  const result = provider.provideCompletionItems!(
    mkModel(line),
    pos,
    {} as never,
    {} as never,
  );
  const list = (result as { suggestions: { label: string }[] }).suggestions;
  return list.map((s) => s.label);
}

describe("completion provider", () => {
  it("ships >= 206 commands", () => {
    expect(COMMANDS.length).toBeGreaterThanOrEqual(206);
  });

  it("every command has name/args/summary/group", () => {
    for (const cmd of COMMANDS) {
      expect(cmd.name).toBeTruthy();
      expect(cmd.summary).toBeTruthy();
      expect(cmd.group).toBeTruthy();
      expect(typeof cmd.args).toBe("string");
    }
  });

  it("all command groups are represented", () => {
    const groups = new Set(COMMANDS.map((c) => c.group));
    for (const g of [
      "string", "hash", "list", "set", "zset", "key", "server",
      "pubsub", "stream", "script", "transaction", "graph", "search",
      "hyperloglog", "geo", "bitmap",
    ]) {
      expect(groups.has(g)).toBe(true);
    }
  });

  it("filters commands by GE prefix (GET, GETSET, GETDEL, GEOADD)", () => {
    const labels = callProvider("GE");
    expect(labels).toContain("GET");
    expect(labels).toContain("GETSET");
    expect(labels).toContain("GETDEL");
    expect(labels).toContain("GEOADD");
    // Must NOT return unrelated commands.
    expect(labels).not.toContain("PING");
    expect(labels).not.toContain("HSET");
  });

  it("filters by HS prefix for hash commands", () => {
    const labels = callProvider("HS");
    expect(labels).toContain("HSET");
    expect(labels).toContain("HSETNX");
    expect(labels).toContain("HSCAN");
  });

  it("filters by ZR prefix for sorted set range commands", () => {
    const labels = callProvider("ZR");
    expect(labels).toContain("ZRANGE");
    expect(labels).toContain("ZRANK");
    expect(labels).toContain("ZREM");
    expect(labels).toContain("ZREVRANK");
  });

  it("returns GRAPH.QUERY when typing GRAPH.Q", () => {
    const labels = callProvider("GRAPH.Q");
    expect(labels).toContain("GRAPH.QUERY");
  });

  it("returns FT.SEARCH when typing FT.S", () => {
    const labels = callProvider("FT.S");
    expect(labels).toContain("FT.SEARCH");
  });

  it("emits suggestion objects with Monaco completion shape", () => {
    const provider = createCompletionProvider(fakeMonaco);
    const pos = { lineNumber: 1, column: 4 } as never;
    const result = provider.provideCompletionItems!(
      mkModel("GET"),
      pos,
      {} as never,
      {} as never,
    );
    const list = (result as {
      suggestions: {
        label: string;
        kind: number;
        insertText: string;
        detail: string;
        documentation: string;
      }[];
    }).suggestions;
    const getEntry = list.find((s) => s.label === "GET")!;
    expect(getEntry).toBeDefined();
    expect(getEntry.kind).toBe(1); // CompletionItemKind.Function stub value
    expect(getEntry.insertText).toBe("GET ");
    expect(getEntry.detail).toBe("key");
    expect(getEntry.documentation).toContain("string");
  });
});
