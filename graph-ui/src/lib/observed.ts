/* Observed executions — runtime evidence on trace hops and flow steps from
 * ingested traces. Presence means the exact caller→callee pair fired in a
 * recorded run; absence means "never recorded", which must never be read
 * as dead or unused — a run only covers what it exercised. */
import type { ObservedCall } from "./atlas";
import { messages, type UiMessages } from "./i18n";

/* True when every hop beyond the first is observed (the first entry is the
 * origin, not a call). False when there is no hop to judge. */
export function allHopsObserved(steps: { observed?: ObservedCall }[]): boolean {
  if (steps.length < 2) return false;
  return steps.slice(1).every((step) => step.observed !== undefined);
}

/* The most recently seen observation across steps — names the freshness
 * footer. Null when nothing was observed. */
export function newestObserved(
  steps: { observed?: ObservedCall }[],
): ObservedCall | null {
  let newest: ObservedCall | null = null;
  for (const step of steps) {
    if (step.observed && (!newest || step.observed.last_seen > newest.last_seen)) {
      newest = step.observed;
    }
  }
  return newest;
}

/* Title text for the marker chip: "ran in pytest 2026-08-27 · last 2026-08-27".
 * Pass the active locale's observed messages to localize. */
export function observedTitle(
  observed: ObservedCall,
  m: UiMessages["observed"] = messages.en.observed,
): string {
  return m.title(observed.label, observed.last_seen.slice(0, 10));
}
