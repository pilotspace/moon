import { beforeEach, describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { NamespaceTree } from "@/components/browser/NamespaceTree";
import { useBrowserStore } from "@/stores/browser";
import type { KeyEntry } from "@/types/browser";

const mkKey = (name: string): KeyEntry => ({
  name,
  type: "string",
  ttl: -1,
  memoryBytes: null,
  selected: false,
});

const resetStore = () =>
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

describe("<NamespaceTree />", () => {
  beforeEach(resetStore);

  it("renders the Namespaces header and All Keys root button", () => {
    render(<NamespaceTree />);
    expect(screen.getByText("Namespaces")).toBeInTheDocument();
    expect(screen.getByText("All Keys")).toBeInTheDocument();
  });

  it("renders namespace children after buildTree runs", () => {
    useBrowserStore.setState({
      keys: [mkKey("user:1:name"), mkKey("user:2:name"), mkKey("session:abc")],
    });
    useBrowserStore.getState().buildTree();
    render(<NamespaceTree />);
    expect(screen.getByText("user")).toBeInTheDocument();
    expect(screen.getByText("session")).toBeInTheDocument();
  });

  it("shows aggregate key counts on each namespace button", () => {
    useBrowserStore.setState({
      keys: [mkKey("user:1"), mkKey("user:2"), mkKey("user:3")],
    });
    useBrowserStore.getState().buildTree();
    render(<NamespaceTree />);
    // "user" appears once with keyCount 3
    expect(screen.getByText("user")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
  });
});
