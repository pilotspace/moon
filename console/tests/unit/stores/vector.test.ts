import { beforeEach, describe, expect, it } from "vitest";
import { useVectorStore } from "@/stores/vector";

const reset = () =>
  useVectorStore.setState({
    indexes: [],
    selectedIndex: null,
    indexInfo: null,
    points: [],
    projectedPositions: null,
    umapProgress: null,
    loading: false,
    hoveredPointId: null,
    selectedPointIds: new Set<string>(),
    colorBy: "segment",
    showHnsw: false,
    knnResults: [],
    knnQueryPointId: null,
    lassoSelectedIds: new Set<string>(),
    hnswEdges: null,
  });

describe("vector store", () => {
  beforeEach(reset);

  it("exposes expected action functions", () => {
    const s = useVectorStore.getState();
    expect(typeof s.loadIndexes).toBe("function");
    expect(typeof s.selectIndex).toBe("function");
    expect(typeof s.runUmap).toBe("function");
    expect(typeof s.setHovered).toBe("function");
    expect(typeof s.setSelected).toBe("function");
    expect(typeof s.setColorBy).toBe("function");
    expect(typeof s.setShowHnsw).toBe("function");
    expect(typeof s.searchFromPoint).toBe("function");
    expect(typeof s.performKnnSearch).toBe("function");
    expect(typeof s.clearKnnResults).toBe("function");
  });

  it("setHovered + setColorBy + setShowHnsw mutate state", () => {
    useVectorStore.getState().setHovered("p42");
    expect(useVectorStore.getState().hoveredPointId).toBe("p42");

    useVectorStore.getState().setColorBy("label");
    expect(useVectorStore.getState().colorBy).toBe("label");

    useVectorStore.getState().setShowHnsw(true);
    expect(useVectorStore.getState().showHnsw).toBe(true);
  });

  it("setSelected / setLassoSelected accept a Set of ids", () => {
    useVectorStore.getState().setSelected(new Set(["a", "b"]));
    expect(useVectorStore.getState().selectedPointIds.size).toBe(2);

    useVectorStore.getState().setLassoSelected(new Set(["c"]));
    expect(useVectorStore.getState().lassoSelectedIds.has("c")).toBe(true);
  });

  it("clearKnnResults resets knn state", () => {
    useVectorStore.setState({
      knnResults: [
        { key: "vec:1", score: 0.9, pointIndex: 0 },
      ] as never,
      knnQueryPointId: "vec:1",
    });
    useVectorStore.getState().clearKnnResults();
    expect(useVectorStore.getState().knnResults).toHaveLength(0);
    expect(useVectorStore.getState().knnQueryPointId).toBeNull();
  });
});
