import type { GraphData, GraphNode, GraphEdge, ServiceGraph } from "./types";
import { SERVICE_NODE_COLORS } from "./colors";

/**
 * Convert a ServiceGraph (from Go tools) into GraphData for the existing
 * GraphScene / NodeCloud / EdgeLines pipeline.
 *
 * Uses a simple force-directed layout (~150 iterations) with no external deps.
 */
export function layoutServiceGraph(sg: ServiceGraph): GraphData {
  /* ── 1. Assign sequential numeric IDs ────────────────── */
  const idMap = new Map<string, number>();
  let nextId = 1;

  const allNodes: { stringId: string; kind: string; label: string }[] = [];

  for (const s of sg.services) {
    idMap.set(s.id, nextId++);
    allNodes.push({ stringId: s.id, kind: "Service", label: s.id });
  }
  for (const t of sg.topics) {
    idMap.set(t.id, nextId++);
    allNodes.push({ stringId: t.id, kind: "Topic", label: t.id });
  }
  for (const g of sg.graphqlEndpoints) {
    idMap.set(g.id, nextId++);
    allNodes.push({ stringId: g.id, kind: "GraphQL", label: g.id });
  }
  for (const d of sg.tables) {
    idMap.set(d.id, nextId++);
    allNodes.push({ stringId: d.id, kind: "Table", label: d.id });
  }

  /* ── 2. Build edges (only those whose endpoints exist) ─ */
  const edges: GraphEdge[] = [];
  const degree = new Map<number, number>();

  for (const e of sg.edges) {
    const src = idMap.get(e.source);
    const tgt = idMap.get(e.target);
    if (src === undefined || tgt === undefined) continue;
    edges.push({ source: src, target: tgt, type: e.type });
    degree.set(src, (degree.get(src) ?? 0) + 1);
    degree.set(tgt, (degree.get(tgt) ?? 0) + 1);
  }

  /* ── 3. Initial positions: services on outer ring ────── */
  const N = allNodes.length;
  const positions: { x: number; y: number; z: number }[] = [];

  const serviceCount = sg.services.length;
  let sIdx = 0;
  const radius = Math.max(80, N * 4);

  for (let i = 0; i < N; i++) {
    const kind = allNodes[i].kind;
    if (kind === "Service") {
      const angle = (2 * Math.PI * sIdx) / Math.max(serviceCount, 1);
      positions.push({
        x: Math.cos(angle) * radius,
        y: Math.sin(angle) * radius,
        z: (Math.random() - 0.5) * 20,
      });
      sIdx++;
    } else {
      positions.push({
        x: (Math.random() - 0.5) * radius * 0.6,
        y: (Math.random() - 0.5) * radius * 0.6,
        z: (Math.random() - 0.5) * 20,
      });
    }
  }

  /* ── 4. Force-directed iterations ───────────────────── */
  const ITERATIONS = 150;
  const REPULSION = 2000;
  const SPRING_K = 0.01;
  const DAMPING = 0.9;

  const vx = new Float64Array(N);
  const vy = new Float64Array(N);

  // Build adjacency from edges for spring forces
  const adjSrc: number[] = [];
  const adjTgt: number[] = [];
  for (const e of edges) {
    // Map numeric IDs back to array indices
    const si = e.source - 1;
    const ti = e.target - 1;
    adjSrc.push(si);
    adjTgt.push(ti);
  }

  for (let iter = 0; iter < ITERATIONS; iter++) {
    const temp = 1 - iter / ITERATIONS; // cooling

    // Repulsion (all pairs — fine for small graphs <200 nodes)
    for (let i = 0; i < N; i++) {
      for (let j = i + 1; j < N; j++) {
        let dx = positions[i].x - positions[j].x;
        let dy = positions[i].y - positions[j].y;
        const dist2 = dx * dx + dy * dy + 1;
        const isService = allNodes[i].kind === "Service" || allNodes[j].kind === "Service";
        const rep = (isService ? REPULSION * 3 : REPULSION) / dist2;
        const dist = Math.sqrt(dist2);
        dx /= dist;
        dy /= dist;
        vx[i] += dx * rep * temp;
        vy[i] += dy * rep * temp;
        vx[j] -= dx * rep * temp;
        vy[j] -= dy * rep * temp;
      }
    }

    // Attraction (spring along edges)
    for (let k = 0; k < adjSrc.length; k++) {
      const i = adjSrc[k];
      const j = adjTgt[k];
      const dx = positions[j].x - positions[i].x;
      const dy = positions[j].y - positions[i].y;
      const dist = Math.sqrt(dx * dx + dy * dy + 1);
      const force = SPRING_K * dist * temp;
      vx[i] += (dx / dist) * force;
      vy[i] += (dy / dist) * force;
      vx[j] -= (dx / dist) * force;
      vy[j] -= (dy / dist) * force;
    }

    // Apply velocities with damping
    for (let i = 0; i < N; i++) {
      vx[i] *= DAMPING;
      vy[i] *= DAMPING;
      positions[i].x += vx[i];
      positions[i].y += vy[i];
    }
  }

  /* ── 5. Build GraphNode array ───────────────────────── */
  const maxDeg = Math.max(1, ...Array.from(degree.values()));
  const nodes: GraphNode[] = allNodes.map((n, i) => {
    const numId = idMap.get(n.stringId)!;
    const d = degree.get(numId) ?? 0;
    return {
      id: numId,
      x: positions[i].x,
      y: positions[i].y,
      z: positions[i].z,
      label: n.kind,
      name: n.label,
      size: 2 + (d / maxDeg) * 6,
      color: SERVICE_NODE_COLORS[n.kind] ?? "#94a3b8",
    };
  });

  return { nodes, edges, total_nodes: nodes.length };
}
