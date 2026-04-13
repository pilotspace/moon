import { lazy, Suspense, useRef, useCallback } from "react";
import { useConsoleStore } from "@/stores/console";
import { respLanguageId, respLanguage, respThemeRules } from "@/lib/monarch-resp";
import { cypherLanguageId, cypherLanguage, cypherThemeRules } from "@/lib/monarch-cypher";
import { createCompletionProvider } from "@/lib/completions";
import { sendCommand, sendSingleCommand } from "@/lib/ws";
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

      // Cmd/Ctrl+Enter to execute.
      //
      // Selection → execute just the selected text.
      // Cypher tab → execute entire content as a single multi-line query.
      // RESP tab, no selection → execute the current line only (redis-cli style).
      // Cmd/Ctrl+Shift+Enter → execute ALL lines sequentially (batch mode).
      const runQuery = async (input: string, multiLine: boolean) => {
        const store = useConsoleStore.getState();
        const tab = store.tabs.find((t) => t.id === store.activeTabId);
        if (!tab || tab.executing || !input.trim()) return;

        store.setExecuting(tab.id, true);
        const result =
          multiLine || tab.language === "cypher"
            ? await sendCommand(input)
            : await sendSingleCommand(input);
        store.setResult(tab.id, result);
        store.addToHistory(tab.id, {
          query: input,
          language: tab.language,
          result,
          timestamp: Date.now(),
        });
      };

      editor.addAction({
        id: "moon-execute",
        label: "Execute Current Line or Selection",
        keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
        run: async (ed) => {
          const selection = ed.getSelection();
          const model = ed.getModel();
          if (!model) return;

          const hasSelection =
            selection &&
            (selection.startLineNumber !== selection.endLineNumber ||
              selection.startColumn !== selection.endColumn);

          let text: string;
          let multiLine: boolean;
          if (hasSelection) {
            text = model.getValueInRange(selection);
            multiLine = text.includes("\n");
          } else {
            // Execute the current line only
            const lineNumber = selection?.positionLineNumber ?? 1;
            text = model.getLineContent(lineNumber);
            multiLine = false;
          }

          await runQuery(text, multiLine);
        },
      });

      editor.addAction({
        id: "moon-execute-all",
        label: "Execute All Lines",
        keybindings: [
          monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.Enter,
        ],
        run: async (ed) => {
          const model = ed.getModel();
          if (!model) return;
          await runQuery(model.getValue(), true);
        },
      });

      // Add placeholder watermark when editor is empty
      const updatePlaceholder = () => {
        const model = editor.getModel();
        const container = editor.getDomNode();
        if (!model || !container) return;
        let placeholder = container.querySelector(".moon-placeholder") as HTMLDivElement | null;
        if (model.getValue().trim() === "") {
          if (!placeholder) {
            placeholder = document.createElement("div");
            placeholder.className = "moon-placeholder";
            placeholder.style.cssText =
              "position:absolute;top:12px;left:64px;color:#52525b;font-size:14px;pointer-events:none;z-index:1;font-family:inherit;";
            placeholder.textContent = "Type PING and press \u2318+Enter";
            container.style.position = "relative";
            container.appendChild(placeholder);
          }
          placeholder.style.display = "";
        } else if (placeholder) {
          placeholder.style.display = "none";
        }
      };
      updatePlaceholder();
      editor.onDidChangeModelContent(updatePlaceholder);

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
