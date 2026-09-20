import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";

// cosmos.gl needs WebGL, which jsdom does not have. Make the constructor throw
// the way a real WebGL context failure does, so the component takes its
// Canvas2D fallback path.
vi.mock("@cosmos.gl/graph", () => ({
  Graph: class {
    constructor() {
      throw new Error("WebGL unavailable");
    }
  },
}));

vi.mock("@/components/graph/GraphCanvas2D", () => ({
  GraphCanvas2D: () => <div data-testid="canvas2d-fallback" />,
}));

import { GraphCosmos } from "@/components/graph/GraphCosmos";

describe("<GraphCosmos />", () => {
  // The fallback used to be an early return placed before the component's
  // hooks, so the re-render after a failed init called fewer hooks than the
  // first render and React threw. Only the parent's error boundary hid it.
  it("falls back to Canvas2D when cosmos.gl cannot initialise, without a hooks-order error", () => {
    render(<GraphCosmos />);
    expect(screen.getByTestId("canvas2d-fallback")).toBeInTheDocument();
  });
});
