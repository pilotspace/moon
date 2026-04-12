import { useCallback } from "react";
import { useGraphStore } from "@/stores/graph";

export function CypherInput() {
  const cypherInput = useGraphStore((s) => s.cypherInput);
  const cypherError = useGraphStore((s) => s.cypherError);
  const loading = useGraphStore((s) => s.loading);
  const setCypherInput = useGraphStore((s) => s.setCypherInput);
  const runQuery = useGraphStore((s) => s.runQuery);

  const handleRun = useCallback(() => {
    const trimmed = cypherInput.trim();
    if (trimmed.length > 0 && !loading) {
      runQuery(trimmed);
    }
  }, [cypherInput, loading, runQuery]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        handleRun();
      }
    },
    [handleRun],
  );

  return (
    <div className="flex items-start gap-2">
      <div className="flex-1">
        <textarea
          className="w-full resize-none rounded border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm font-mono text-zinc-200 placeholder:text-zinc-600 focus:border-indigo-500 focus:outline-none"
          rows={2}
          value={cypherInput}
          onChange={(e) => setCypherInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 200"
          spellCheck={false}
        />
        {cypherError && (
          <div className="mt-1 text-xs text-red-400">{cypherError}</div>
        )}
      </div>
      <button
        className="shrink-0 rounded bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-50"
        onClick={handleRun}
        disabled={loading || cypherInput.trim().length === 0}
      >
        {loading ? "Running..." : "Run"}
      </button>
    </div>
  );
}
