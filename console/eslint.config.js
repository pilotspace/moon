// ESLint 9 flat config for the moon console.
//
// ESLint 9 reads only flat config, so without this file `pnpm run lint`
// exits with a missing-config error before it lints anything. It wires the
// plugins already pinned in package.json: @eslint/js and typescript-eslint
// recommended, react-hooks recommended, and react-refresh's Vite rule.
// CI runs it in the `unit` job of .github/workflows/console-integration.yml.
import js from "@eslint/js";
import globals from "globals";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["dist", "coverage", "playwright-report", "test-results"] },
  {
    extends: [js.configs.recommended, ...tseslint.configs.recommended],
    files: ["**/*.{ts,tsx}"],
    languageOptions: {
      ecmaVersion: 2020,
      globals: globals.browser,
    },
    plugins: {
      "react-hooks": reactHooks,
      "react-refresh": reactRefresh,
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
      "react-refresh/only-export-components": [
        "warn",
        { allowConstantExport: true },
      ],
      // Same convention tsc already applies under noUnusedParameters: a
      // leading underscore marks a parameter that is unused on purpose (a
      // stub matching a callback signature).
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_", caughtErrorsIgnorePattern: "^_" },
      ],
    },
  },
  {
    // Node-side files: build config and the Playwright/vitest harnesses.
    files: ["*.config.{js,ts}", "tests/**/*.{ts,tsx}"],
    languageOptions: {
      globals: { ...globals.node, ...globals.browser },
    },
  },
);
