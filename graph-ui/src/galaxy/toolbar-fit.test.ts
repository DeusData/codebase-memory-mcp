// @vitest-environment jsdom
/*
 * Review zu K3: die Leiste eines Ausschnitts blieb einzeilig, indem sie
 * abschnitt. Bei offenem Chat und 1.494 px Fensterbreite (der des Handtests)
 * lagen Zaehler und Menue "⋯" mit den Limits jenseits des Randes. Jetzt
 * wird gemessen: erst die volle Leiste, dann die knappe Beschriftung, und erst
 * wenn auch die nicht passt, bricht die Zeile um. Nichts ragt mehr hinaus.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { chooseToolbarFit, fitToolbar, type ToolbarFit } from './toolbar-fit';

describe('chooseToolbarFit', () => {
    it('takes the first level at which nothing sticks out, and wraps as the last resort', () => {
        const tried: ToolbarFit[] = [];
        expect(chooseToolbarFit(fit => { tried.push(fit); return false; })).toBe('full');
        expect(tried).toEqual(['full']);
        expect(chooseToolbarFit(fit => fit === 'full')).toBe('compact');
        expect(chooseToolbarFit(fit => fit !== 'tight')).toBe('tight');
        expect(chooseToolbarFit(() => true)).toBe('wrap');
    });
});

describe('fitToolbar', () => {
    afterEach(() => { document.body.innerHTML = ''; });
    /** A bar 1,000 px wide whose last item ends where `rightAt` says for the level the bar is set to. */
    const bar = (rightAt: Record<ToolbarFit, number>) => {
        const element = document.createElement('div');
        element.style.paddingRight = '10px';
        const rect = (left: number, right: number) => ({ left, right, width: right - left, top: 0, bottom: 40, height: 40, x: left, y: 0, toJSON: () => ({}) }) as DOMRect;
        element.getBoundingClientRect = () => rect(0, 1000);
        for (const at of [0, 1]) {
            const child = document.createElement('button');
            child.getBoundingClientRect = () => at === 0 ? rect(10, 200) : rect(210, rightAt[(element.dataset.fit ?? 'full') as ToolbarFit]);
            element.append(child);
        }
        document.body.append(element);
        return element;
    };

    it('keeps the full bar while it fits inside the padding', () => {
        const element = bar({ full: 990, compact: 900, tight: 900, wrap: 900 });
        expect(fitToolbar(element)).toBe('full');
        expect(element.dataset.fit).toBe('full');
    });

    it('turns to the short labels when the full bar sticks out, before it wraps', () => {
        const element = bar({ full: 1072, compact: 960, tight: 940, wrap: 600 });
        expect(fitToolbar(element)).toBe('compact');
        expect(element.dataset.fit).toBe('compact');
    });

    it('shortens the count next, and wraps when even that sticks out, so every control stays reachable', () => {
        expect(fitToolbar(bar({ full: 1134, compact: 1003, tight: 985, wrap: 600 }))).toBe('tight');
        const element = bar({ full: 1134, compact: 1003, tight: 995, wrap: 600 });
        expect(fitToolbar(element)).toBe('wrap');
        expect(element.dataset.fit).toBe('wrap');
        // A wider bar later goes back to the full labels.
        element.getBoundingClientRect = () => ({ left: 0, right: 1200, width: 1200, top: 0, bottom: 40, height: 40, x: 0, y: 0, toJSON: () => ({}) }) as DOMRect;
        expect(fitToolbar(element)).toBe('full');
    });

    /*
     * Hand test K2 seen in the browser: with "← Back" and "Forward →" in words
     * the root name shrank to "JS…" with the chat open, while nothing stuck
     * out. The full level now also needs the root name whole.
     */
    it('leaves the full level when the root name would be squeezed below its text, but not for a name only its maximum width clips', () => {
        const element = bar({ full: 990, compact: 900, tight: 900, wrap: 900 });
        const name = document.createElement('button');
        name.dataset.fitWhole = '';
        name.style.maxWidth = '176px';
        name.style.boxSizing = 'border-box';
        let widths = { full: 40, compact: 100 } as Record<string, number>, content = 100;
        Object.defineProperty(name, 'scrollWidth', { get: () => content });
        Object.defineProperty(name, 'clientWidth', { get: () => widths[element.dataset.fit ?? 'full'] ?? 100 });
        element.append(name);
        expect(fitToolbar(element)).toBe('compact');
        widths = { full: 100, compact: 100 };
        expect(fitToolbar(element)).toBe('full');
        // A long name is cut by its maximum width at every level: that alone is no reason to leave the full labels.
        content = 300;
        widths = { full: 176, compact: 176 };
        expect(fitToolbar(element)).toBe('full');
    });
});
