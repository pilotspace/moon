import { useVectorStore } from "@/stores/vector";

const MODES = ["segment", "label", "none"] as const;

export function ColorByControls() {
  const colorBy = useVectorStore((s) => s.colorBy);
  const setColorBy = useVectorStore((s) => s.setColorBy);

  return (
    <div className="flex items-center gap-0.5 rounded-md bg-zinc-900 p-0.5">
      {MODES.map((mode) => (
        <button
          key={mode}
          className={`rounded-md px-2 py-1 text-xs capitalize transition-colors ${
            colorBy === mode
              ? "bg-zinc-700 text-zinc-100"
              : "text-zinc-500 hover:text-zinc-300"
          }`}
          onClick={() => setColorBy(mode)}
        >
          {mode}
        </button>
      ))}
    </div>
  );
}
