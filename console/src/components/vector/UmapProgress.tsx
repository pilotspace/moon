import { useVectorStore } from "@/stores/vector";

export function UmapProgress() {
  const progress = useVectorStore((s) => s.umapProgress);

  if (!progress) return null;

  const pct = progress.total > 0 ? (progress.epoch / progress.total) * 100 : 0;

  return (
    <div className="absolute bottom-4 left-4 z-10 rounded-lg border border-zinc-700 bg-zinc-900/90 px-4 py-2.5 shadow-lg backdrop-blur-sm">
      <div className="mb-1.5 text-xs text-zinc-400">
        Projecting vectors... {progress.epoch}/{progress.total}
      </div>
      <div className="h-1.5 w-48 overflow-hidden rounded-full bg-zinc-700">
        <div
          className="h-full rounded-full bg-indigo-500 transition-all duration-150"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}
