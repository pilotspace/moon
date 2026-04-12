import type { languages } from "monaco-editor";

export const cypherLanguageId = "cypher";

export const cypherLanguage: languages.IMonarchLanguage = {
  ignoreCase: true,
  keywords: [
    "MATCH", "OPTIONAL", "WHERE", "RETURN", "WITH", "ORDER", "BY",
    "SKIP", "LIMIT", "CREATE", "MERGE", "DELETE", "DETACH", "SET",
    "REMOVE", "ON", "AS", "AND", "OR", "NOT", "IN", "IS", "NULL",
    "TRUE", "FALSE", "DISTINCT", "CASE", "WHEN", "THEN", "ELSE",
    "END", "UNWIND", "UNION", "ALL", "COUNT", "SUM", "AVG", "MIN",
    "MAX", "COLLECT", "EXISTS", "NONE", "ANY", "SINGLE", "ASC",
    "DESC", "ASCENDING", "DESCENDING", "CALL", "YIELD", "XOR",
    "STARTS", "ENDS", "CONTAINS", "FOREACH", "EXPLAIN", "PROFILE",
  ],
  typeKeywords: ["Node", "Relationship", "Path"],
  operators: ["=", "<>", "<", ">", "<=", ">=", "=~", "+", "-", "*", "/", "%"],
  tokenizer: {
    root: [
      [/\/\/.*$/, "comment"],
      [/"[^"]*"/, "string"],
      [/'[^']*'/, "string"],
      [/\$[a-zA-Z_]\w*/, "variable"],
      [/\b\d+(\.\d+)?\b/, "number"],
      [/\(/, "delimiter.parenthesis"],
      [/\)/, "delimiter.parenthesis"],
      [/\[/, "delimiter.bracket"],
      [/\]/, "delimiter.bracket"],
      [/\{/, "delimiter.brace"],
      [/\}/, "delimiter.brace"],
      [/->|<-|-/, "operator.arrow"],
      [/:([A-Z]\w*)/, "type"],
      [/[A-Za-z_]\w*/, { cases: { "@keywords": "keyword", "@typeKeywords": "type", "@default": "identifier" } }],
    ],
  },
};

export const cypherThemeRules: { token: string; foreground: string }[] = [
  { token: "keyword", foreground: "C586C0" },
  { token: "string", foreground: "CE9178" },
  { token: "number", foreground: "B5CEA8" },
  { token: "comment", foreground: "6A9955" },
  { token: "variable", foreground: "9CDCFE" },
  { token: "type", foreground: "4EC9B0" },
  { token: "operator.arrow", foreground: "D4D4D4" },
];
