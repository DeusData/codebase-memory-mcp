import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const css = readFileSync(new URL('./browser-chat.css', import.meta.url), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '');
const rule = (selector: string) => [...css.matchAll(/([^{}]+)\{([^}]*)\}/g)].filter(match => match[1].split(',').map(item => item.trim()).includes(selector)).map(match => match[2]).join(';');

/** The lines read from JSONBAgg ended mid-template at the panel edge, behind a hidden scrollbar (B2). */
describe('code blocks in a chat answer', () => {
    it('wrap long lines instead of clipping them at the panel edge', () => {
        expect(rule('.cbm-chat-markdown .cbm-chat-code-block pre')).toMatch(/white-space:\s*pre-wrap/);
        expect(rule('.cbm-chat-markdown .cbm-chat-code-block pre')).toMatch(/overflow-wrap:\s*anywhere/);
        expect(rule('.cbm-chat-code-block pre > code')).not.toMatch(/width:\s*max-content/);
    });
});
