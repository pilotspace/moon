import { describe, expect, it } from "vitest";
import {
  cypherLanguageId,
  cypherLanguage,
  cypherThemeRules,
} from "@/lib/monarch-cypher";

describe("Monarch Cypher grammar", () => {
  it("exports a non-empty language id", () => {
    expect(cypherLanguageId).toBeTypeOf("string");
    expect(cypherLanguageId.length).toBeGreaterThan(0);
  });

  it("defines a tokenizer with a 'root' state", () => {
    expect(cypherLanguage.tokenizer).toBeTypeOf("object");
    expect(cypherLanguage.tokenizer).toHaveProperty("root");
    expect(Array.isArray(cypherLanguage.tokenizer.root)).toBe(true);
  });

  it("covers core Cypher keywords", () => {
    const kws = cypherLanguage.keywords as string[];
    for (const kw of ["MATCH", "WHERE", "RETURN", "CREATE", "MERGE", "DELETE", "WITH", "LIMIT"]) {
      expect(kws).toContain(kw);
    }
  });

  it("declares Node/Relationship/Path as typeKeywords", () => {
    const types = cypherLanguage.typeKeywords as string[];
    expect(types).toContain("Node");
    expect(types).toContain("Relationship");
    expect(types).toContain("Path");
  });

  it("ships theme rules with token + foreground", () => {
    expect(Array.isArray(cypherThemeRules)).toBe(true);
    expect(cypherThemeRules.length).toBeGreaterThan(0);
    for (const rule of cypherThemeRules) {
      expect(rule.token).toBeTypeOf("string");
      expect(rule.foreground).toMatch(/^[0-9A-Fa-f]{6}$/);
    }
  });
});
