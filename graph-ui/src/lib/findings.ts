/* Findings machinery: knee-point cutoffs, the cost sentence, and finding
 * dismissals — the difference between a dashboard and a number museum. */
import { messages, type UiMessages } from "./i18n";

/* Knee of a descending score list — how many head items deserve emphasis:
 * "the head that matters" (CodeScene's refusal to show 5,000 items).
 * Kneedle-style: normalize ranks and values to the unit square, smooth y
 * with a centered 3-point moving average, and put the knee at the maximum
 * of the difference curve against the descending chord y = 1 − x (the
 * decreasing-data form of Kneedle's transform). A largest-relative-drop
 * rule sat here before and collapsed to the rank 1→2 gap on power-law
 * data — the head became "top 1". Guard rails: the head stays within
 * [3, 15], never exceeds half the list, and n < 5 falls back to min(3, n). */
export function kneeCount(values: number[]): number {
  const n = values.length;
  if (n < 5) return Math.min(3, n);
  const max = Math.max(...values);
  if (max <= 0) return Math.min(3, n);
  const y = values.map((value) => value / max);
  const smoothed = y.map((_, i) => {
    const lo = Math.max(0, i - 1);
    const hi = Math.min(n - 1, i + 1);
    let sum = 0;
    for (let j = lo; j <= hi; j++) sum += y[j];
    return sum / (hi - lo + 1);
  });
  /* The maximum sits where the curve has fallen furthest below the chord —
   * the first rank of the flat tail, i.e. the size of the head. */
  let knee = 0;
  let best = -Infinity;
  for (let i = 0; i < n; i++) {
    const difference = 1 - i / (n - 1) - smoothed[i];
    if (difference > best) {
      best = difference;
      knee = i;
    }
  }
  return Math.min(Math.max(knee, 3), Math.min(15, Math.ceil(n / 2)));
}

/* "9 files — 1.1% of the codebase, 34% of all commits this year." The
 * shares are computed here so every locale interpolates identical numbers.
 * Pass the active locale's dashboard messages to localize. */
export function costSentence(
  headFiles: number,
  headCommits: number,
  totalFiles: number,
  totalCommits: number,
  m: UiMessages["dashboard"] = messages.en.dashboard,
): string | null {
  if (headFiles <= 0 || totalFiles <= 0 || totalCommits <= 0) return null;
  const filesShare = ((headFiles / totalFiles) * 100).toFixed(1);
  const commitShare = ((headCommits / totalCommits) * 100).toFixed(0);
  return m.costSentence(headFiles, filesShare, commitShare);
}

/* Dismissals: a finding stays dismissed until its magnitude changes bucket
 * (an order-of-magnitude jump resurfaces it — regression re-alerts). */
const DISMISS_KEY = "cbm-atlas-dismissed";

export function findingKey(kind: string, subject: string, magnitude: number): string {
  const bucket = magnitude > 0 ? Math.floor(Math.log10(magnitude)) : 0;
  return `${kind}:${subject}:${bucket}`;
}

function readDismissed(): Set<string> {
  try {
    const raw = localStorage.getItem(DISMISS_KEY);
    return new Set(raw ? (JSON.parse(raw) as string[]) : []);
  } catch {
    return new Set();
  }
}

export function isDismissed(key: string): boolean {
  return readDismissed().has(key);
}

export function dismiss(key: string): void {
  try {
    const set = readDismissed();
    set.add(key);
    localStorage.setItem(DISMISS_KEY, JSON.stringify([...set]));
  } catch {
    /* private mode — dismissals just don't persist */
  }
}

export function undismissAll(): void {
  try {
    localStorage.removeItem(DISMISS_KEY);
  } catch {
    /* ignore */
  }
}
