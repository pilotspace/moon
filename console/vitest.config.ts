/// <reference types="vitest" />
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./tests/unit/setup.ts"],
    include: ["tests/unit/**/*.test.{ts,tsx}"],
    exclude: ["tests/e2e/**", "node_modules/**", "dist/**"],
    passWithNoTests: true,
    css: false,
    coverage: {
      provider: "v8",
      reporter: ["text", "html"],
      include: ["src/stores/**", "src/lib/**", "src/components/**"],
      exclude: ["src/**/*.d.ts", "src/main.tsx", "src/vite-env.d.ts"],
    },
  },
});
