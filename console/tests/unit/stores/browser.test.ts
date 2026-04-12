import { beforeEach, describe, expect, it } from "vitest";
import { useBrowserStore } from "@/stores/browser";
import type { KeyEntry } from "@/types/browser";

const mkKey = (name: string): KeyEntry => ({
  name,
  type: "string",
  ttl: -1,
  memoryBytes: null,
  selected: false,
});

const resetStore = () => {
  useBrowserStore.setState({
    keys: [],
    cursor: "0",
    loading: false,
    hasMore: true,
    selectedKey: null,
    selectedKeys: new Set<string>(),
    treeRoot: null,
    activePrefix: "",
    filter: { pattern: "*", type: "all", ttlStatus: "all" },
  });
};

describe("browser store / namespace trie", () => {
  beforeEach(resetStore);

  it("buildTree groups colon-delimited keys and skips single-segment keys", () => {
    useBrowserStore.setState({
      keys: [mkKey("a:b:c"), mkKey("a:b:d"), mkKey("x"), mkKey("y:z")],
    });
    useBrowserStore.getState().buildTree();
    const root = useBrowserStore.getState().treeRoot!;
    expect(root).not.toBeNull();
    expect(root.children.has("a")).toBe(true);
    expect(root.children.has("y")).toBe(true);
    // Single-segment "x" never creates a child (loop runs parts.length - 1 = 0 times).
    expect(root.children.has("x")).toBe(false);

    const a = root.children.get("a")!;
    expect(a.keyCount).toBe(2);
    expect(a.fullPrefix).toBe("a:");

    const ab = a.children.get("b")!;
    expect(ab.keyCount).toBe(2);
    expect(ab.fullPrefix).toBe("a:b:");

    const y = root.children.get("y")!;
    expect(y.keyCount).toBe(1);
    expect(y.fullPrefix).toBe("y:");
  });

  it("buildTree with empty key list yields a root with no children", () => {
    useBrowserStore.getState().buildTree();
    const root = useBrowserStore.getState().treeRoot!;
    expect(root.children.size).toBe(0);
    expect(root.keyCount).toBe(0);
  });

  it("selectKey / toggleKeySelection / clearSelection mutate state", () => {
    useBrowserStore.getState().selectKey("foo");
    expect(useBrowserStore.getState().selectedKey).toBe("foo");

    useBrowserStore.getState().toggleKeySelection("a");
    useBrowserStore.getState().toggleKeySelection("b");
    expect(useBrowserStore.getState().selectedKeys.size).toBe(2);

    // Toggling "a" again removes it.
    useBrowserStore.getState().toggleKeySelection("a");
    expect(useBrowserStore.getState().selectedKeys.has("a")).toBe(false);
    expect(useBrowserStore.getState().selectedKeys.has("b")).toBe(true);

    useBrowserStore.getState().clearSelection();
    expect(useBrowserStore.getState().selectedKeys.size).toBe(0);
  });

  it("selectAllKeys selects every current key", () => {
    useBrowserStore.setState({ keys: [mkKey("a"), mkKey("b"), mkKey("c")] });
    useBrowserStore.getState().selectAllKeys();
    const ids = useBrowserStore.getState().selectedKeys;
    expect(ids.size).toBe(3);
    expect(ids.has("a")).toBe(true);
    expect(ids.has("c")).toBe(true);
  });
});
