import { UMAP } from "umap-js";
import type { UmapWorkerRequest, UmapWorkerResponse } from "@/types/vector";

self.onmessage = (e: MessageEvent<UmapWorkerRequest>) => {
  const { vectors, dims, count, nNeighbors = 15, minDist = 0.1 } = e.data;

  // Convert flattened Float32Array to array-of-arrays for umap-js
  const data: number[][] = [];
  for (let i = 0; i < count; i++) {
    data.push(Array.from(vectors.slice(i * dims, (i + 1) * dims)));
  }

  const umap = new UMAP({
    nNeighbors: Math.min(nNeighbors, count - 1),
    minDist,
    nComponents: 3,
    nEpochs: Math.min(200, Math.max(50, Math.floor(count / 10))),
  });

  // Fit with epoch-by-epoch stepping for progress reporting
  const totalEpochs = umap.initializeFit(data);

  for (let epoch = 0; epoch < totalEpochs; epoch++) {
    umap.step();
    if (epoch % 10 === 0) {
      const msg: UmapWorkerResponse = {
        type: "progress",
        epoch,
        totalEpochs,
      };
      self.postMessage(msg);
    }
  }

  const embedding = umap.getEmbedding();
  // Flatten to Float32Array [x0,y0,z0, x1,y1,z1, ...]
  const positions = new Float32Array(count * 3);
  for (let i = 0; i < count; i++) {
    positions[i * 3] = embedding[i][0];
    positions[i * 3 + 1] = embedding[i][1];
    positions[i * 3 + 2] = embedding[i][2];
  }

  const msg: UmapWorkerResponse = { type: "complete", positions };
  self.postMessage(msg, { transfer: [positions.buffer] });
};
