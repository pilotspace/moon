import {
  forceSimulation,
  forceLink,
  forceManyBody,
  forceCenter,
  forceCollide,
} from "d3-force-3d";
import type { ForceWorkerRequest, ForceWorkerResponse } from "@/types/graph";

self.onmessage = (e: MessageEvent<ForceWorkerRequest>) => {
  const { nodes, edges } = e.data;
  const count = nodes.length;

  // Create simulation nodes with random initial positions
  const simNodes = nodes.map((n) => ({
    id: n.id,
    x: (Math.random() - 0.5) * 100,
    y: (Math.random() - 0.5) * 100,
    z: (Math.random() - 0.5) * 100,
  }));

  const simLinks = edges.map((e) => ({ source: e.source, target: e.target }));

  const simulation = forceSimulation(simNodes, 3)
    .force(
      "link",
      forceLink(simLinks)
        .id((d: { id?: string }) => d.id ?? "")
        .distance(8)
        .strength(0.3),
    )
    .force("charge", forceManyBody().strength(-30).distanceMax(150))
    .force("center", forceCenter(0, 0, 0))
    .force("collide", forceCollide(1.5))
    .alphaDecay(0.01)
    .stop();

  // Run 300 ticks manually, reporting every 30
  const totalTicks = 300;
  for (let i = 0; i < totalTicks; i++) {
    simulation.tick();
    if (i % 30 === 0 || i === totalTicks - 1) {
      const positions = new Float32Array(count * 3);
      for (let j = 0; j < count; j++) {
        positions[j * 3] = simNodes[j].x ?? 0;
        positions[j * 3 + 1] = simNodes[j].y ?? 0;
        positions[j * 3 + 2] = simNodes[j].z ?? 0;
      }
      if (i < totalTicks - 1) {
        const msg: ForceWorkerResponse = {
          type: "tick",
          positions,
          alpha: simulation.alpha(),
        };
        self.postMessage(msg);
      } else {
        const msg: ForceWorkerResponse = { type: "complete", positions };
        self.postMessage(msg, { transfer: [positions.buffer] });
      }
    }
  }
};
