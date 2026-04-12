import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    // Report compressed sizes so CI can gate on UX-02 budget.
    reportCompressedSize: true,
    chunkSizeWarningLimit: 400, // KB — warn if any chunk exceeds
    rollupOptions: {
      output: {
        manualChunks: {
          // Heavy 3D stack (Vectors + Graph views). Loaded lazily via
          // React.lazy in App.tsx — not in Dashboard's critical path.
          three: ["three", "@react-three/fiber", "@react-three/drei"],
          // Monaco (Query Console) — ~2 MB raw, ~600 KB gzipped.
          monaco: ["monaco-editor", "@monaco-editor/react"],
          // Charting for Dashboard + Memory. Kept separate so Graph/Vectors
          // don't pay for recharts.
          recharts: ["recharts"],
          // TanStack table + virtual (Browser + Memory).
          tanstack: ["@tanstack/react-table", "@tanstack/react-virtual"],
          // UMAP worker already separate (see workers/); no change.
        },
      },
    },
  },
  server: {
    proxy: {
      "/api": "http://localhost:9100",
      "/events": "http://localhost:9100",
      "/ws": { target: "ws://localhost:9100", ws: true },
    },
  },
});
