/* Region level of detail — adapters between the level=regions payload
 * (layout_regions.c) and the node/edge shapes the 3D scene renders. */
import { messages, type UiMessages } from "./i18n";
import type { GraphData, GraphNode, Region, RegionsPayload } from "./types";

/* The scene renders GraphNodes; a region becomes one body. Region ids live in
 * a separate id space from node ids — the scene never mixes the two levels,
 * so collisions are impossible by construction. */
export function regionToNode(region: Region): GraphNode {
  return {
    id: region.id,
    x: region.x,
    y: region.y,
    z: region.z,
    label: "Region",
    name: region.name,
    size: region.size,
    color: region.color,
    status: "structural",
    in_calls: 0,
  };
}

/* Duplicate display names (two Leiden communities dominated by the same
 * folder) get their hub appended so the human can tell them apart. */
export function disambiguateRegionNames(regions: Region[]): Region[] {
  const counts = new Map<string, number>();
  for (const region of regions) {
    counts.set(region.name, (counts.get(region.name) ?? 0) + 1);
  }
  return regions.map((region) =>
    (counts.get(region.name) ?? 0) > 1 && region.hub
      ? { ...region, name: `${region.name} · ${region.hub}` }
      : region,
  );
}

/* GraphData for the scene: bodies + weighted arcs (type "REGION"). */
export function regionsToGraphData(payload: RegionsPayload): GraphData {
  const regions = disambiguateRegionNames(payload.regions);
  return {
    nodes: regions.map(regionToNode),
    edges: payload.edges.map((edge) => ({
      source: edge.source,
      target: edge.target,
      type: "REGION",
    })),
    total_nodes: payload.total_nodes,
  };
}

/* Below this project size the region level adds a hop without teaching
 * anything — the full galaxy is already readable and loads fast. */
export const REGIONS_MIN_TOTAL_NODES = 5000;

export function regionsViewWorthwhile(payload: RegionsPayload): boolean {
  return (
    payload.total_nodes > REGIONS_MIN_TOTAL_NODES && payload.regions.length > 1
  );
}

/* The product-code lens: test regions teach how the code is exercised, not
 * how it works — hide them by default, one toggle away. A region is
 * test-ish when its display name lives under a test folder. */
export function isTestRegion(name: string): boolean {
  return /(^|\/)(tests?|__tests__|spec|specs)($|\/|\b)/i.test(name.split(" · ")[0] ?? name);
}

export function lensRegionsPayload(
  payload: RegionsPayload,
  includeTests: boolean,
): RegionsPayload {
  if (includeTests) return payload;
  const kept = new Set(
    payload.regions.filter((region) => !isTestRegion(region.name)).map((r) => r.id),
  );
  return {
    ...payload,
    regions: payload.regions.filter((region) => kept.has(region.id)),
    edges: payload.edges.filter((edge) => kept.has(edge.source) && kept.has(edge.target)),
  };
}

/* region.why arrives from layout_regions.c as one of three English
 * templates; recompose it in the viewer's locale. The en messages
 * reproduce the server strings byte-for-byte, and an unrecognized string
 * (a future server template) passes through verbatim — honest English
 * beats a wrong translation. */
const REGION_WHY_CALL_COMMUNITY = /^call community: (\d+) files, (\d+)% under (.+)$/;

export function localizeRegionWhy(
  why: string,
  m: UiMessages["regions"] = messages.en.regions,
): string {
  const match = REGION_WHY_CALL_COMMUNITY.exec(why);
  if (match) return m.whyCallCommunity(Number(match[1]), Number(match[2]), match[3]);
  if (why === "folder group (not explained by a kept call community)")
    return m.whyFolderGroup;
  if (why === "files outside every kept community and folder group") return m.whyMisc;
  return why;
}

/* Hub lists must never headline builtins — a `len` hub reads as a bug and
 * costs trust in everything else on the page. */
export function isBuiltinQn(qn: string): boolean {
  return /(^|\.)builtins?\./.test(qn) || /(^|\.)__builtins__\./.test(qn);
}

/* Score → word: numbers alone don't teach. Thresholds match the
 * moderately-connected convention (0.7 / 0.4). */
export function cohesionWord(cohesion: number): string {
  if (cohesion >= 0.7) return "tightly connected";
  if (cohesion >= 0.4) return "moderately connected";
  return "loosely connected";
}
