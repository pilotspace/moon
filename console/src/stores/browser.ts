import { create } from "zustand";
import type { KeyEntry, KeyFilter, NamespaceNode, KeyType } from "@/types/browser";
import { scanKeys, scanKeysWithType, getKeyTtl, getKeyType, getKeyMemory } from "@/lib/api";

interface BrowserState {
  // Key list
  keys: KeyEntry[];
  cursor: string;
  loading: boolean;
  hasMore: boolean;

  // Selection
  selectedKey: string | null;
  selectedKeys: Set<string>;

  // Filters (per KV-11)
  filter: KeyFilter;
  setFilter: (filter: Partial<KeyFilter>) => void;

  // Namespace tree
  treeRoot: NamespaceNode | null;
  activePrefix: string;
  setActivePrefix: (prefix: string) => void;

  // Actions
  loadKeys: (reset?: boolean) => Promise<void>;
  selectKey: (key: string | null) => void;
  toggleKeySelection: (key: string) => void;
  selectAllKeys: () => void;
  clearSelection: () => void;
  enrichKey: (keyName: string) => Promise<void>;
  buildTree: () => void;
  deleteSelected: () => Promise<number>;
}

const BATCH_SIZE = 100;

export const useBrowserStore = create<BrowserState>((set, get) => ({
  keys: [],
  cursor: "0",
  loading: false,
  hasMore: true,

  selectedKey: null,
  selectedKeys: new Set(),

  filter: { pattern: "*", type: "all", ttlStatus: "all" },
  setFilter: (partial) => {
    set((s) => ({ filter: { ...s.filter, ...partial } }));
    // Reset and reload with new filter
    set({ keys: [], cursor: "0", hasMore: true });
    get().loadKeys(true);
  },

  treeRoot: null,
  activePrefix: "",
  setActivePrefix: (prefix) => {
    set({ activePrefix: prefix });
    // Update filter pattern to match prefix
    const pattern = prefix ? `${prefix}*` : "*";
    set((s) => ({ filter: { ...s.filter, pattern }, keys: [], cursor: "0", hasMore: true }));
    get().loadKeys(true);
  },

  loadKeys: async (reset = false) => {
    const state = get();
    if (state.loading) return;
    if (!reset && !state.hasMore) return;

    set({ loading: true });

    const cursor = reset ? "0" : state.cursor;
    const { filter } = state;

    try {
      let result;
      if (filter.type !== "all") {
        result = await scanKeysWithType(cursor, filter.pattern, BATCH_SIZE, filter.type as KeyType);
      } else {
        result = await scanKeys(cursor, filter.pattern, BATCH_SIZE);
      }

      const newEntries: KeyEntry[] = result.keys.map((name) => ({
        name,
        type: null,
        ttl: null,
        memoryBytes: null,
        selected: false,
      }));

      set((s) => ({
        keys: reset ? newEntries : [...s.keys, ...newEntries],
        cursor: result.cursor,
        hasMore: result.cursor !== "0",
        loading: false,
      }));
    } catch {
      set({ loading: false });
    }
  },

  selectKey: (key) => set({ selectedKey: key }),

  toggleKeySelection: (key) =>
    set((s) => {
      const next = new Set(s.selectedKeys);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return { selectedKeys: next };
    }),

  selectAllKeys: () =>
    set((s) => ({
      selectedKeys: new Set(s.keys.map((k) => k.name)),
    })),

  clearSelection: () => set({ selectedKeys: new Set() }),

  enrichKey: async (keyName) => {
    const state = get();
    const idx = state.keys.findIndex((k) => k.name === keyName);
    if (idx === -1) return;
    const entry = state.keys[idx];
    if (entry.type !== null) return; // Already enriched

    try {
      const [typeStr, ttl, mem] = await Promise.all([
        getKeyType(keyName),
        getKeyTtl(keyName),
        getKeyMemory(keyName),
      ]);

      set((s) => {
        const updated = [...s.keys];
        updated[idx] = {
          ...updated[idx],
          type: typeStr as KeyType,
          ttl,
          memoryBytes: mem,
        };
        return { keys: updated };
      });
    } catch {
      // Silently skip enrichment failures
    }
  },

  buildTree: () => {
    const { keys } = get();
    const root: NamespaceNode = {
      name: "root",
      fullPrefix: "",
      children: new Map(),
      keyCount: keys.length,
      expanded: true,
      loading: false,
    };

    for (const key of keys) {
      const parts = key.name.split(":");
      if (parts.length <= 1) continue; // No namespace

      let current = root;
      let prefix = "";
      for (let i = 0; i < parts.length - 1; i++) {
        prefix += (i > 0 ? ":" : "") + parts[i];
        const childPrefix = prefix + ":";
        if (!current.children.has(parts[i])) {
          current.children.set(parts[i], {
            name: parts[i],
            fullPrefix: childPrefix,
            children: new Map(),
            keyCount: 0,
            expanded: false,
            loading: false,
          });
        }
        const child = current.children.get(parts[i])!;
        child.keyCount++;
        current = child;
      }
    }

    set({ treeRoot: root });
  },

  deleteSelected: async () => {
    const { selectedKeys } = get();
    if (selectedKeys.size === 0) return 0;

    const { deleteKeys: apiDeleteKeys } = await import("@/lib/api");
    const deleted = await apiDeleteKeys([...selectedKeys]);

    // Remove deleted keys from list
    set((s) => ({
      keys: s.keys.filter((k) => !selectedKeys.has(k.name)),
      selectedKeys: new Set(),
      selectedKey: selectedKeys.has(s.selectedKey ?? "") ? null : s.selectedKey,
    }));

    return deleted;
  },
}));
