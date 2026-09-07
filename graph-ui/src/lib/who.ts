/* Who-can-help data layer: /api/who — the people with recorded history in
 * one file, as contacts with evidence: commits here, breadth repo-wide,
 * last touched. Never a ranking — no shares, no "top", no comparisons;
 * every number is an evidence statement. */
import { messages, type UiMessages } from "./i18n";
import { fetchJsonFrom } from "./whyfetch";

export interface WhoPerson {
  name: string;
  commits_here: number;
  files_repo_wide: number;
  /* Epoch seconds; present only when the retained newest commits prove
   * it — its absence is honest, never an error. */
  last_seen?: number;
}

export interface WhoPayload {
  window: string;
  /* false = no git history readable for this project. */
  available: boolean;
  commits_1y?: number;
  /* Sorted by commits_here desc, at most 8; may be empty (a file with no
   * recorded commits in the window). */
  people?: WhoPerson[];
  /* Everyone with recorded history here — may exceed people.length. */
  people_total?: number;
}

export async function fetchWho(project: string, file: string): Promise<WhoPayload> {
  const params = new URLSearchParams({ project, file });
  const payload = await fetchJsonFrom<WhoPayload & { error?: string }>(
    `/api/who?${params}`,
  );
  if (payload.error) throw new Error(payload.error);
  return payload;
}

/* The evidence line under a name: "23 commits to this file this year ·
 * active in 47 files repo-wide · last touched 2026-08-12". The last part
 * appears only when last_seen is proven. Pass the active locale's who
 * messages to localize. */
export function personEvidence(
  person: WhoPerson,
  m: UiMessages["who"] = messages.en.who,
): string {
  const parts = [m.commitsHere(person.commits_here), m.breadth(person.files_repo_wide)];
  if (person.last_seen !== undefined)
    parts.push(m.lastTouched(new Date(person.last_seen * 1000).toISOString().slice(0, 10)));
  return parts.join(" · ");
}
