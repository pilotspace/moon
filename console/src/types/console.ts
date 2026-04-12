/** Language mode for the query editor */
export type QueryLanguage = "resp" | "cypher" | "ftsearch";

/** A single editor tab with independent state */
export interface ConsoleTab {
  id: string;
  name: string;
  language: QueryLanguage;
  content: string;
  result: QueryResult | null;
  history: HistoryEntry[];
  historyIndex: number; // -1 = current editor content (not navigating history)
  executing: boolean;
}

/** A completed query stored in history */
export interface HistoryEntry {
  query: string;
  language: QueryLanguage;
  result: QueryResult;
  timestamp: number;
}

/** Result returned from query execution */
export interface QueryResult {
  data: unknown;
  raw: string;
  elapsed_ms: number;
  error?: string;
}

/** Metadata for a Moon command (used by auto-complete) */
export interface CommandInfo {
  name: string;
  summary: string;
  args: string; // e.g. "key value [EX seconds]"
  group: string;
}
