import { describe, expect, it } from "vitest";
import {
  respLanguageId,
  respLanguage,
  respThemeRules,
} from "@/lib/monarch-resp";

describe("Monarch RESP grammar", () => {
  it("uses the 'resp' language id", () => {
    expect(respLanguageId).toBe("resp");
  });

  it("defines a tokenizer with a 'root' state", () => {
    expect(respLanguage.tokenizer).toBeTypeOf("object");
    expect(respLanguage.tokenizer).toHaveProperty("root");
    expect(Array.isArray(respLanguage.tokenizer.root)).toBe(true);
    expect((respLanguage.tokenizer.root as unknown[]).length).toBeGreaterThan(0);
  });

  it("is case-insensitive and covers all major command groups", () => {
    expect(respLanguage.ignoreCase).toBe(true);
    const kws = respLanguage.keywords as string[];
    expect(Array.isArray(kws)).toBe(true);
    // Sample coverage across groups
    for (const cmd of [
      "GET", "SET", "HGET", "LPUSH", "SADD", "ZADD", "DEL", "PING",
      "SUBSCRIBE", "XADD", "EVAL", "MULTI", "GRAPH.QUERY", "FT.SEARCH",
    ]) {
      expect(kws).toContain(cmd);
    }
  });

  it("ships non-empty theme rules with {token, foreground} shape", () => {
    expect(Array.isArray(respThemeRules)).toBe(true);
    expect(respThemeRules.length).toBeGreaterThan(0);
    for (const rule of respThemeRules) {
      expect(rule).toHaveProperty("token");
      expect(rule).toHaveProperty("foreground");
      expect(typeof rule.token).toBe("string");
      expect(typeof rule.foreground).toBe("string");
    }
  });
});
