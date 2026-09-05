/**
 * Whether this browser has already asked one project's reader why they are here.
 *
 * The reference keeps this flag in the workspace, not on the machine
 * (`codeatlas-views/src/browser/welcome-service.ts`): there, opening the same
 * repository in a second window is the same reader with the same answer. This
 * frontend has no workspace to write into and a server that offers nothing that
 * stores it, so the flag is a preference of this browser and the panel says so
 * in its own subline. The difference is real and is stated rather than papered
 * over: the same project opened in another browser asks again.
 *
 * The decline is stored exactly like an answer. "Not now" is a reader saying
 * they do not want to be asked, and asking again on the next load would be
 * treating it as if they had said nothing.
 */

import type { KeyValueStore } from '../checklist/understanding-store';
import type { WhyIntent } from './why-model';

/** Prefix of the key one project's answer lives under. */
export const WHY_KEY_PREFIX = 'atlas-why:';

/** The key one project's answer lives under. */
export function whyKey(project: string): string {
    return `${WHY_KEY_PREFIX}${project}`;
}

/**
 * What is stored: that the question was put, and the answer when there was one.
 *
 * `asked` and `intent` are two fields rather than one nullable field, because
 * "asked and declined" and "never asked" are different states and only the first
 * one means the panel stays shut.
 */
export interface WhyAnswer {
    asked: boolean;
    intent?: WhyIntent;
}

const NEVER_ASKED: WhyAnswer = { asked: false };

const INTENTS: readonly string[] = ['bug', 'change', 'understand', 'entry'];

/**
 * What this browser has recorded for one project.
 *
 * An unreadable or unknown value counts as never asked, which errs towards
 * asking a question the reader can decline in one click rather than towards
 * silently withholding the only way into the modes.
 */
export function readWhyAnswer(store: KeyValueStore, project: string): WhyAnswer {
    if (project.length === 0) {
        return NEVER_ASKED;
    }
    let raw: string | null;
    try {
        raw = store.getItem(whyKey(project));
    } catch {
        return NEVER_ASKED;
    }
    if (raw === null) {
        return NEVER_ASKED;
    }
    try {
        const parsed = JSON.parse(raw) as Record<string, unknown>;
        if (parsed['asked'] !== true) {
            return NEVER_ASKED;
        }
        const intent = parsed['intent'];
        return typeof intent === 'string' && INTENTS.includes(intent)
            ? { asked: true, intent: intent as WhyIntent }
            : { asked: true };
    } catch {
        return NEVER_ASKED;
    }
}

/** Record the answer, or the decline. Returns what now stands. */
export function recordWhyAnswer(store: KeyValueStore, project: string, intent?: WhyIntent): WhyAnswer {
    const answer: WhyAnswer = intent === undefined ? { asked: true } : { asked: true, intent };
    if (project.length === 0) {
        return answer;
    }
    try {
        store.setItem(whyKey(project), JSON.stringify(answer));
    } catch {
        // A refused storage costs the reader one repeated question on the next
        // load, which is a better failure than a panel that will not close.
    }
    return answer;
}
