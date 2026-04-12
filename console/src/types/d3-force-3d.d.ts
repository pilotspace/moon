declare module "d3-force-3d" {
  interface SimulationNode {
    id?: string;
    x?: number;
    y?: number;
    z?: number;
    vx?: number;
    vy?: number;
    vz?: number;
    fx?: number | null;
    fy?: number | null;
    fz?: number | null;
    index?: number;
  }

  interface SimulationLink<N extends SimulationNode = SimulationNode> {
    source: string | N;
    target: string | N;
    index?: number;
  }

  interface ForceSimulation<N extends SimulationNode = SimulationNode> {
    tick(): this;
    stop(): this;
    alpha(): number;
    alpha(value: number): this;
    alphaDecay(): number;
    alphaDecay(value: number): this;
    force(name: string): unknown;
    force(name: string, force: unknown): this;
    nodes(): N[];
    nodes(nodes: N[]): this;
  }

  interface ForceLink<N extends SimulationNode = SimulationNode> {
    id(fn: (d: N) => string): this;
    distance(value: number): this;
    strength(value: number): this;
  }

  interface ForceManyBody {
    strength(value: number): this;
    distanceMax(value: number): this;
  }

  interface ForceCenter {
    x(value: number): this;
    y(value: number): this;
    z(value: number): this;
  }

  interface ForceCollide {
    radius(value: number): this;
  }

  export function forceSimulation<N extends SimulationNode = SimulationNode>(
    nodes?: N[],
    numDimensions?: number,
  ): ForceSimulation<N>;

  export function forceLink<N extends SimulationNode = SimulationNode>(
    links?: SimulationLink<N>[],
  ): ForceLink<N>;

  export function forceManyBody(): ForceManyBody;

  export function forceCenter(
    x?: number,
    y?: number,
    z?: number,
  ): ForceCenter;

  export function forceCollide(radius?: number): ForceCollide;
}
