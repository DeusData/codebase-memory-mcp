/**
 * Which reader the panel is speaking to, remembered between visits.
 *
 * ## Why this is stored at all
 *
 * Until W13 the slider was a detail level, and losing it on reload was a small
 * annoyance: the reader moved it again. It is now a statement about the person
 * in front of the screen, and a surface that forgets who is reading and quietly
 * goes back to the middle setting is telling that person their answer did not
 * matter. So it is stored, and it survives the reload and the symbol change.
 *
 * ## Why the key carries no project
 *
 * Every other preference in this frontend is keyed per project
 * (`atlas-llm:<project>`, `atlas-why:<project>`, `atlas-understanding:<project>`)
 * because every other preference is an answer about that codebase. This one is
 * not. A junior does not become a senior by opening a different repository, and
 * a key per project would mean the same person answering the same question once
 * per codebase. One key, deliberately.
 *
 * ## Why every doubt ends in the middle and not at zero
 *
 * A missing value, unreadable JSON, a storage that refuses to answer, a number
 * outside the ladder: all of them mean "nobody has said". The fallback is the
 * medior level, which is the one that shows the recorded facts as they are, and
 * that is the honest thing to show someone who has not told you who they are.
 * Falling back to the vibe coder would be guessing that a new reader is a
 * beginner; falling back to the architect would be guessing the opposite.
 */

import type { KeyValueStore } from '../checklist/understanding-store';
import { clampDepth, MAX_DEPTH } from './presentation-profile';
import type { DepthLevel } from './presentation-profile';

/** The one key. See the note above on why it carries no project. */
export const READER_LEVEL_KEY = 'atlas-reader';

/** What is shown to someone who has not said who they are. */
export const READER_LEVEL_DEFAULT: DepthLevel = 2;

/** What this browser last said, or the default when it said nothing usable. */
export function readReaderLevel(store: KeyValueStore): DepthLevel {
    let raw: string | null;
    try {
        raw = store.getItem(READER_LEVEL_KEY);
    } catch {
        return READER_LEVEL_DEFAULT;
    }
    // An empty entry is not a zero. `Number('')` is 0, and a browser that
    // wrote a blank would otherwise hand every reader the vibe coder's level
    // and call it their choice.
    if (raw === null || raw.trim().length === 0) {
        return READER_LEVEL_DEFAULT;
    }
    const value = Number(raw);
    if (!Number.isInteger(value) || value < 0 || value > MAX_DEPTH) {
        return READER_LEVEL_DEFAULT;
    }
    return clampDepth(value);
}

/**
 * Remember the level. A storage that refuses is not an error worth showing.
 *
 * The same rule the understanding checklist follows: a browser with site data
 * switched off is a browser that forgets, and a panel that popped a warning
 * about it would be making the reader's privacy setting into a fault.
 */
export function writeReaderLevel(store: KeyValueStore, level: DepthLevel): void {
    try {
        store.setItem(READER_LEVEL_KEY, String(level));
    } catch {
        // Nothing to do and nothing to say: the level still holds for this session.
    }
}

/** The browser's own storage, when this build is running in one. */
export function browserStore(): KeyValueStore | undefined {
    try {
        return typeof localStorage === 'undefined' ? undefined : localStorage;
    } catch {
        return undefined;
    }
}
