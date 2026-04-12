import { useVectorStore } from "@/stores/vector";

const MODES = ["segment", "label", "none"] as const;

export function ColorByControls() {
  const colorBy = useVectorStore((s) => s.colorBy);
  const setColorBy = useVectorStore((s) => s.setColorBy);

  return (
    <div className="flex items-center gap-1">
      <span className="mr-1 text-[10px] text-zinc-500">Color:</span>
      {MODES.map((mode) => (
        <button
          key={mode}
          className={`rounded px-2 py-1 text-[10px] capitalize ${
            colorBy === mode
              ? "border border-indigo-500 bg-indigo-600/20 text-indigo-300"
              : "border border-zinc-700 bg-zinc-800 text-zinc-400 hover:bg-zinc-700"
          }`}
          onClick={() => setColorBy(mode)}
        >
          {mode}
        </button>
      ))}
    </div>
  );
}
