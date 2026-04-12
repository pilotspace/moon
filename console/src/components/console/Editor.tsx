import { lazy, Suspense, useRef, useCallback } from "react";
import { useConsoleStore } from "@/stores/console";
import { respLanguageId, respLanguage, respThemeRules } from "@/lib/monarch-resp";
import { cypherLanguageId, cypherLanguage, cypherThemeRules } from "@/lib/monarch-cypher";
import { createCompletionProvider } from "@/lib/completions";
import { sendCommand } from "@/lib/ws";
import type { editor as EditorTypes } from "monaco-editor";

const MonacoEditor = lazy(() => import("@monaco-editor/react"));

let languagesRegistered = false;

export function Editor() {
  const editorRef = useRef<EditorTypes.IStandaloneCodeEditor | null>(null);

  const tabs = useConsoleStore((s) => s.tabs);
  const activeTabId = useConsoleStore((s) => s.activeTabId);
  const updateTabContent = useConsoleStore((s) => s.updateTabContent);

  const activeTab = tabs.find((t) => t.id === activeTabId);

  const handleBeforeMount = useCallback((monaco: typeof import("monaco-editor")) => {
    if (languagesRegistered) return;
    languagesRegistered = true;

    // Register RESP language
    monaco.languages.register({ id: respLanguageId });
    monaco.languages.setMonarchTokensProvider(respLanguageId, respLanguage);

    // Register Cypher language
    monaco.languages.register({ id: cypherLanguageId });
    monaco.languages.setMonarchTokensProvider(cypherLanguageId, cypherLanguage);

    // Register completion provider for RESP
    monaco.languages.registerCompletionItemProvider(
      respLanguageId,
      createCompletionProvider(monaco),
    );

    // Also register completions for cypher (graph commands)
    monaco.languages.registerCompletionItemProvider(
      cypherLanguageId,
      createCompletionProvider(monaco),
    );

    // Define moon-dark theme
    monaco.editor.defineTheme("moon-dark", {
      base: "vs-dark",
      inherit: true,
      rules: [...respThemeRules, ...cypherThemeRules],
      colors: {
        "editor.background": "#09090B", // zinc-950
      },
    });
  }, []);

  const handleMount = useCallback(
    (editor: EditorTypes.IStandaloneCodeEditor, monaco: typeof import("monaco-editor")) => {
      editorRef.current = editor;

      // Cmd/Ctrl+Enter to execute
      editor.addAction({
        id: "moon-execute",
        label: "Execute Query",
        keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
        run: async () => {
          const store = useConsoleStore.getState();
          const tab = store.tabs.find((t) => t.id === store.activeTabId);
          if (!tab || tab.executing || !tab.content.trim()) return;

          store.setExecuting(tab.id, true);
          const result = await sendCommand(tab.content);
          store.setResult(tab.id, result);
          store.addToHistory(tab.id, {
            query: tab.content,
            language: tab.language,
            result,
            timestamp: Date.now(),
          });
        },
      });

      editor.focus();
    },
    [],
  );

  const handleChange = useCallback(
    (value: string | undefined) => {
      if (activeTab) {
        updateTabContent(activeTab.id, value ?? "");
      }
    },
    [activeTab, updateTabContent],
  );

  if (!activeTab) return null;

  const editorLanguage = activeTab.language === "cypher" ? cypherLanguageId : respLanguageId;

  return (
    <Suspense
      fallback={
        <div className="flex items-center justify-center h-full text-zinc-500">
          Loading editor...
        </div>
      }
    >
      <MonacoEditor
        height="100%"
        language={editorLanguage}
        theme="moon-dark"
        value={activeTab.content}
        onChange={handleChange}
        beforeMount={handleBeforeMount}
        onMount={handleMount}
        options={{
          minimap: { enabled: false },
          fontSize: 14,
          lineNumbers: "on",
          scrollBeyondLastLine: false,
          wordWrap: "on",
          automaticLayout: true,
          tabSize: 2,
          padding: { top: 12 },
          renderLineHighlight: "gutter",
          overviewRulerLanes: 0,
          hideCursorInOverviewRuler: true,
          scrollbar: {
            verticalScrollbarSize: 6,
            horizontalScrollbarSize: 6,
          },
        }}
      />
    </Suspense>
  );
}
