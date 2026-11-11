import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const css = readFileSync(new URL('./browser-chat.css', import.meta.url), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '');

/** The declarations of the last rule whose selector list names `selector` exactly. */
function declarations(selector: string): Record<string, string> {
    const result: Record<string, string> = {};
    for (const match of css.matchAll(/([^{}]+)\{([^}]*)\}/g)) {
        const selectors = match[1].split(',').map(item => item.trim());
        if (!selectors.includes(selector)) continue;
        for (const part of match[2].split(';')) {
            const [name, ...value] = part.split(':');
            if (name.trim() && value.length) result[name.trim()] = value.join(':').trim();
        }
    }
    return result;
}

describe('the expanded source block of an answer', () => {
    it('wraps long fact and code lines instead of clipping them at the panel edge', () => {
        for (const selector of ['.cbm-chat-source-content pre', '.cbm-chat-source-content .cbm-chat-attachment pre']) {
            const rule = declarations(selector);
            expect(rule['white-space'], selector).toBe('pre-wrap');
            expect(rule['overflow-wrap'], selector).toBe('anywhere');
        }
    });
});
