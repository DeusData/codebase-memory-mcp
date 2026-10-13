// @vitest-environment jsdom
import { afterEach, describe, expect, it } from 'vitest';

import { fullscreenIsolationRequired, isolateFullscreenBackground } from './fullscreen-isolation';

afterEach(() => {
    document.body.replaceChildren();
});

function fixture(): Record<string, HTMLElement> {
    const shell = document.createElement('div');
    shell.className = 'atlas-shell';
    const header = document.createElement('header');
    const tabs = document.createElement('div');
    const body = document.createElement('div');
    const main = document.createElement('main');
    const side = document.createElement('aside');
    const twin = document.createElement('div');
    const galaxy = document.createElement('section');
    shell.append(header, tabs, body);
    body.append(main, side);
    side.append(twin, galaxy);
    document.body.appendChild(shell);
    return { shell, header, tabs, body, main, side, twin, galaxy };
}

describe('fullscreen background isolation', () => {
    it('inerts only siblings along the galaxy-to-shell path', () => {
        const node = fixture();
        const release = isolateFullscreenBackground(node.galaxy);

        expect(node.header.hasAttribute('inert')).toBe(true);
        expect(node.tabs.hasAttribute('inert')).toBe(true);
        expect(node.main.hasAttribute('inert')).toBe(true);
        expect(node.twin.hasAttribute('inert')).toBe(true);
        expect(node.body.hasAttribute('inert')).toBe(false);
        expect(node.side.hasAttribute('inert')).toBe(false);
        expect(node.galaxy.hasAttribute('inert')).toBe(false);

        release();
    });

    it('restores exactly and preserves an independently inert sibling', () => {
        const node = fixture();
        node.main.setAttribute('inert', 'already-isolated');
        const release = isolateFullscreenBackground(node.galaxy);

        release();

        expect(node.header.hasAttribute('inert')).toBe(false);
        expect(node.main.getAttribute('inert')).toBe('already-isolated');
    });

    it('does not request isolation when a higher overlay owns Escape', () => {
        expect(fullscreenIsolationRequired(true, false)).toBe(true);
        expect(fullscreenIsolationRequired(true, true)).toBe(false);
        expect(fullscreenIsolationRequired(false, false)).toBe(false);
    });
});
