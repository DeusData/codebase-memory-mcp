/* Rationale-evidence data layer: /api/symbol-history — the recorded "why"
 * for one symbol: git log -L over its own line range, followed through
 * renames. Each call costs one git subprocess on the server, so it is
 * fetched on demand (a button), never automatically. */
import { fetchJsonFrom } from "./whyfetch";

export interface SymbolCommit {
  hash: string;
  time: number;
  author: string;
  subject: string;
}

export interface SymbolHistoryPayload {
  max_commits: number;
  /* Normalized https forge base (scheme, host, owner and repository, no
   * trailing slash); absent when the repo has no usable remote — then
   * hashes render as plain text and #refs stay unlinked. */
  remote_url?: string;
  available: boolean;
  /* Newest first, at most max_commits; absent on the unavailable paths. */
  commits?: SymbolCommit[];
  truncated: boolean;
}

export async function fetchSymbolHistory(
  project: string,
  file: string,
  start: number,
  end: number,
): Promise<SymbolHistoryPayload> {
  const params = new URLSearchParams({
    project,
    file,
    start: String(start),
    end: String(end),
  });
  const payload = await fetchJsonFrom<SymbolHistoryPayload & { error?: string }>(
    `/api/symbol-history?${params}`,
  );
  if (payload.error) throw new Error(payload.error);
  return payload;
}

/* One piece of a commit subject: plain text, or a "#123" issue/PR ref
 * (then ref carries the number). */
export interface SubjectSegment {
  text: string;
  ref?: number;
}

/* Split a commit subject into plain segments and linkable "#123" refs.
 * A bare "#" without digits stays plain text. */
export function linkifyRefs(subject: string): SubjectSegment[] {
  const segments: SubjectSegment[] = [];
  const pattern = /#(\d+)/g;
  let cursor = 0;
  for (let match = pattern.exec(subject); match; match = pattern.exec(subject)) {
    if (match.index > cursor) segments.push({ text: subject.slice(cursor, match.index) });
    segments.push({ text: match[0], ref: Number(match[1]) });
    cursor = match.index + match[0].length;
  }
  if (cursor < subject.length) segments.push({ text: subject.slice(cursor) });
  return segments;
}

export function commitUrl(remote: string, hash: string): string {
  return `${remote}/commit/${hash}`;
}

/* Forges route /issues/N to the pull request when N is a PR, so one link
 * shape covers both kinds of ref. */
export function refUrl(remote: string, ref: number): string {
  return `${remote}/issues/${ref}`;
}
