/* Change-impact data layer: /api/impact reverse reachability — who could
 * notice a change to one symbol, at what distance, in which regions, and
 * which tests reach it. Counts are floors: static CALLS edges only, and
 * the walk is capped in depth and volume. */
import { messages, type UiMessages } from "./i18n";
import { fetchJsonFrom } from "./whyfetch";

export interface ImpactNode {
  id: number;
  name: string;
  file_path?: string;
}

export interface ImpactReacher extends ImpactNode {
  distance: number;
}

export interface ImpactPayload {
  basis: string;
  max_depth: number;
  visit_cap: number;
  callable_total: number;
  node: ImpactNode;
  reachable: number;
  max_distance: number;
  truncated: boolean;
  depth_capped: boolean;
  /* Reachers per distance; index 0 = distance 1 (direct callers). */
  by_distance: number[];
  /* Non-test reachers by region, sorted desc, capped. */
  regions: { name: string; count: number }[];
  regions_more: number;
  unregioned: number;
  /* Nearest non-test reachers in BFS order, capped. */
  nearest: ImpactReacher[];
  tests: { count: number; nearest: ImpactReacher[] };
}

export async function fetchImpact(
  project: string,
  ref: { id?: number; qn?: string },
): Promise<ImpactPayload> {
  const params = new URLSearchParams({ project });
  params.set("node", ref.id !== undefined ? `#${ref.id}` : (ref.qn ?? ""));
  const payload = await fetchJsonFrom<ImpactPayload & { error?: string }>(
    `/api/impact?${params}`,
  );
  if (payload.error) throw new Error(payload.error);
  return payload;
}

/* The headline sentence. Region count includes the rows the payload capped
 * away (regions_more); a capped walk says so — the count is then a floor
 * of a floor. Pass the active locale's impact messages to localize. */
export function impactSentence(
  payload: ImpactPayload,
  m: UiMessages["impact"] = messages.en.impact,
): string {
  if (payload.reachable === 0) return m.nothingCalls;
  const regionTotal = payload.regions.length + payload.regions_more;
  const sentence = m.sentence(payload.reachable, payload.callable_total, regionTotal);
  if (payload.truncated || payload.depth_capped) return `${sentence}${m.cappedSuffix}`;
  return sentence;
}
