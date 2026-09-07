/* The "first read": deterministic teaching artifacts computed from data the
 * graph already holds — every insight carries the reason it is interesting.
 * No model, no scores without provenance. */
import type { Region, RegionsPayload } from "./types";
import { messages, type UiMessages } from "./i18n";
import { disambiguateRegionNames } from "./regions";

export interface SurprisingCoupling {
  source: Region;
  target: Region;
  weight: number;
  reasons: string[];
}

/* Cross-region couplings ranked by an explained score: heavier links, links
 * that cross top-level directories, and links touching a low-cohesion region
 * rank higher. `misc` never surprises anyone. Pass the active locale's
 * firstread messages to localize the reasons. */
export function surprisingCouplings(
  payload: RegionsPayload,
  limit = 5,
  m: UiMessages["firstread"] = messages.en.firstread,
): SurprisingCoupling[] {
  const regions = disambiguateRegionNames(payload.regions);
  const byId = new Map(regions.map((region) => [region.id, region]));
  const top = (name: string) => name.split("/")[0] ?? name;
  const scored: (SurprisingCoupling & { score: number })[] = [];
  for (const edge of payload.edges) {
    const source = byId.get(edge.source);
    const target = byId.get(edge.target);
    if (!source || !target) continue;
    if (source.name === "misc" || target.name === "misc") continue;
    const reasons: string[] = [];
    let score = Math.log2(1 + edge.weight);
    reasons.push(m.edgesCross(edge.weight));
    if (top(source.name) !== top(target.name)) {
      score += 2;
      reasons.push(m.linksAreas(top(source.name), top(target.name)));
    }
    if (source.cohesion < 0.3 || target.cohesion < 0.3) {
      score += 1;
      const loose = source.cohesion < target.cohesion ? source : target;
      reasons.push(m.holdsLoosely(loose.name, loose.cohesion));
    }
    scored.push({ source, target, weight: edge.weight, reasons, score });
  }
  scored.sort(
    (a, b) => b.score - a.score || b.weight - a.weight || a.source.id - b.source.id,
  );
  return scored.slice(0, limit).map(({ score: _score, ...rest }) => rest);
}

export interface SuggestedQuestion {
  question: string;
  why: string;
  /* Seed for the prompt composer: the entities the question is about. */
  about: string[];
}

export interface FirstReadStats {
  deadCount?: number;
  unresolvedShare?: number; /* USAGE / (CALLS+CALL_REFERENCE+USAGE), 0..1 */
}

/* Question templates keyed to structural signals — each one is a prompt
 * starter the human can hand to their agent. Pass the active locale's
 * firstread messages to localize question and evidence; the `about` seeds
 * stay English (they feed the composer, not the page). */
export function suggestedQuestions(
  payload: RegionsPayload,
  stats: FirstReadStats = {},
  limit = 5,
  m: UiMessages["firstread"] = messages.en.firstread,
): SuggestedQuestion[] {
  const regions = disambiguateRegionNames(payload.regions).filter(
    (region) => region.name !== "misc",
  );
  const questions: SuggestedQuestion[] = [];

  const couplings = surprisingCouplings(payload, 2, m);
  for (const coupling of couplings) {
    questions.push({
      question: m.questionWhyDepend(coupling.source.name, coupling.target.name),
      why: coupling.reasons.join(m.reasonSeparator),
      about: [coupling.source.name, coupling.target.name],
    });
  }

  const loose = [...regions]
    .filter((region) => region.members >= 50 && region.cohesion > 0 && region.cohesion < 0.25)
    .sort((a, b) => a.cohesion - b.cohesion)[0];
  if (loose) {
    questions.push({
      question: m.questionSplit(loose.name),
      why: m.whySplit((loose.cohesion * 100).toFixed(0), loose.members),
      about: [loose.name],
    });
  }

  if (stats.deadCount && stats.deadCount > 0) {
    questions.push({
      question: m.questionDeadSafe(stats.deadCount),
      why: m.whyDeadSafe,
      about: ["dead code"],
    });
  }

  if (stats.unresolvedShare !== undefined && stats.unresolvedShare > 0.3) {
    questions.push({
      question: m.questionUnresolved,
      why: m.whyUnresolved((stats.unresolvedShare * 100).toFixed(0)),
      about: ["resolution certainty"],
    });
  }

  return questions.slice(0, limit);
}
