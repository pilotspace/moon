import { beforeEach, describe, expect, it } from "vitest";
import { useMemoryStore } from "@/stores/memory";
import type { MemoryNode } from "@/types/memory";

const reset = () =>
  useMemoryStore.setState({
    treemapData: null,
    treemapPath: [],
    slowlog: [],
    commandStats: [],
    loading: false,
  });

const mkTree = (): MemoryNode => ({
  name: "root",
  size: 100,
  type: "namespace",
  children: [
    {
      name: "user",
      size: 60,
      type: "namespace",
      children: [
        { name: "1", size: 30, type: "string" },
        { name: "2", size: 30, type: "string" },
      ],
    },
    { name: "session", size: 40, type: "namespace" },
  ],
});

describe("memory store", () => {
  beforeEach(reset);

  it("exposes expected action functions", () => {
    const s = useMemoryStore.getState();
    expect(typeof s.loadMemoryData).toBe("function");
    expect(typeof s.loadSlowlog).toBe("function");
    expect(typeof s.loadCommandStats).toBe("function");
    expect(typeof s.drillDown).toBe("function");
    expect(typeof s.drillUp).toBe("function");
    expect(typeof s.resetDrill).toBe("function");
    expect(typeof s.currentTreemapNode).toBe("function");
  });

  it("drillDown / drillUp / resetDrill walk the path stack", () => {
    useMemoryStore.getState().drillDown("user");
    useMemoryStore.getState().drillDown("1");
    expect(useMemoryStore.getState().treemapPath).toEqual(["user", "1"]);

    useMemoryStore.getState().drillUp();
    expect(useMemoryStore.getState().treemapPath).toEqual(["user"]);

    useMemoryStore.getState().resetDrill();
    expect(useMemoryStore.getState().treemapPath).toEqual([]);
  });

  it("currentTreemapNode returns root when path empty", () => {
    useMemoryStore.setState({ treemapData: mkTree() });
    const node = useMemoryStore.getState().currentTreemapNode();
    expect(node?.name).toBe("root");
  });

  it("currentTreemapNode descends via path segments", () => {
    useMemoryStore.setState({ treemapData: mkTree(), treemapPath: ["user"] });
    const node = useMemoryStore.getState().currentTreemapNode();
    expect(node?.name).toBe("user");
    expect(node?.size).toBe(60);
  });

  it("currentTreemapNode returns null when treemapData is null", () => {
    useMemoryStore.setState({ treemapData: null, treemapPath: [] });
    expect(useMemoryStore.getState().currentTreemapNode()).toBeNull();
  });

  it("currentTreemapNode stops at last valid segment when path drifts", () => {
    useMemoryStore.setState({
      treemapData: mkTree(),
      treemapPath: ["user", "nonexistent"],
    });
    // Drills into user, fails to find "nonexistent" → returns "user" node.
    const node = useMemoryStore.getState().currentTreemapNode();
    expect(node?.name).toBe("user");
  });
});
