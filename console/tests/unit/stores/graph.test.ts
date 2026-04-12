import { beforeEach, describe, expect, it } from "vitest";
import { useGraphStore } from "@/stores/graph";

const reset = () =>
  useGraphStore.setState({
    graphInfo: null,
    nodes: [],
    edges: [],
    positions: null,
    layoutProgress: null,
    loading: false,
    selectedNodeId: null,
    hoveredNodeId: null,
    visibleLabels: new Set<string>(),
    visibleRelTypes: new Set<string>(),
    cypherInput: "MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 200",
    cypherError: null,
  });

describe("graph store", () => {
  beforeEach(reset);

  it("exposes expected action functions", () => {
    const s = useGraphStore.getState();
    expect(typeof s.loadGraphInfo).toBe("function");
    expect(typeof s.runQuery).toBe("function");
    expect(typeof s.runForceLayout).toBe("function");
    expect(typeof s.setCypherInput).toBe("function");
    expect(typeof s.selectNode).toBe("function");
    expect(typeof s.setHoveredNode).toBe("function");
    expect(typeof s.toggleLabel).toBe("function");
    expect(typeof s.toggleRelType).toBe("function");
    expect(typeof s.setAllLabelsVisible).toBe("function");
    expect(typeof s.setAllRelTypesVisible).toBe("function");
  });

  it("setCypherInput updates cypher text", () => {
    useGraphStore.getState().setCypherInput("MATCH (n:Person) RETURN n");
    expect(useGraphStore.getState().cypherInput).toBe("MATCH (n:Person) RETURN n");
  });

  it("selectNode and setHoveredNode mutate the right fields", () => {
    useGraphStore.getState().selectNode("node-1");
    expect(useGraphStore.getState().selectedNodeId).toBe("node-1");

    useGraphStore.getState().setHoveredNode("node-2");
    expect(useGraphStore.getState().hoveredNodeId).toBe("node-2");

    useGraphStore.getState().selectNode(null);
    expect(useGraphStore.getState().selectedNodeId).toBeNull();
  });

  it("toggleLabel adds/removes labels from visibleLabels set", () => {
    useGraphStore.getState().toggleLabel("Person");
    expect(useGraphStore.getState().visibleLabels.has("Person")).toBe(true);

    useGraphStore.getState().toggleLabel("Movie");
    expect(useGraphStore.getState().visibleLabels.size).toBe(2);

    useGraphStore.getState().toggleLabel("Person");
    expect(useGraphStore.getState().visibleLabels.has("Person")).toBe(false);
    expect(useGraphStore.getState().visibleLabels.has("Movie")).toBe(true);
  });

  it("toggleRelType mirrors toggleLabel behavior", () => {
    useGraphStore.getState().toggleRelType("KNOWS");
    useGraphStore.getState().toggleRelType("ACTED_IN");
    expect(useGraphStore.getState().visibleRelTypes.size).toBe(2);
    useGraphStore.getState().toggleRelType("KNOWS");
    expect(useGraphStore.getState().visibleRelTypes.has("KNOWS")).toBe(false);
  });

  it("setAllLabelsVisible(false) clears labels; (true) rebuilds from nodes", () => {
    useGraphStore.setState({
      nodes: [
        { id: "1", labels: ["Person"], props: {} },
        { id: "2", labels: ["Movie", "Thing"], props: {} },
      ] as never,
    });
    useGraphStore.getState().setAllLabelsVisible(false);
    expect(useGraphStore.getState().visibleLabels.size).toBe(0);

    useGraphStore.getState().setAllLabelsVisible(true);
    const labels = useGraphStore.getState().visibleLabels;
    expect(labels.has("Person")).toBe(true);
    expect(labels.has("Movie")).toBe(true);
    expect(labels.has("Thing")).toBe(true);
  });

  it("setAllRelTypesVisible(true) rebuilds from edges", () => {
    useGraphStore.setState({
      edges: [
        { id: "e1", source: "1", target: "2", type: "KNOWS", props: {} },
        { id: "e2", source: "2", target: "3", type: "ACTED_IN", props: {} },
      ] as never,
    });
    useGraphStore.getState().setAllRelTypesVisible(true);
    const types = useGraphStore.getState().visibleRelTypes;
    expect(types.has("KNOWS")).toBe(true);
    expect(types.has("ACTED_IN")).toBe(true);
  });
});
