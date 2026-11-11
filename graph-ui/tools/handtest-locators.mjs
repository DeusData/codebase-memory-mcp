import { literalPattern } from './literal-pattern.mjs';

/** Match one architecture chip by its graph node ID. */
export function architectureNodeSelector(id) {
    // CSS strings need escapes for backslashes, quotes, and control characters.
    // Terminate each hex escape so the next character cannot extend its code point.
    const escaped = id.replace(/[\x00-\x1f\x7f"\\]/g, char => `\\${char.codePointAt(0).toString(16)} `);
    return `button.architecture-node-label[data-node-id="${escaped}"]`;
}

/** Match a complete project name in the project chooser. */
export function projectNamePattern(name) {
    return literalPattern(name, { start: true, end: true });
}
