/* Tier → chip tone, shared by the wiki panel and the hover preview so the
 * two surfaces can never drift apart. */
import type { WikiTier } from "../wiki/entries";

export const TIER_TONE: Record<WikiTier, string> = {
  "first-class": "bg-emerald-400/10 text-emerald-300/80",
  caveated: "bg-amber-400/10 text-amber-300/80",
  refused: "bg-surface-3 text-foreground/40 border border-border/40",
};
