/*
 * Der Cache vor dem Server: was er behaelt, was er zusammenlegt, und was er
 * zaehlt.
 *
 * Die dritte Frage ist die wichtigste. Der Beweislauf misst an genau diesem
 * Zaehler, ob eine Caret-Bewegung innerhalb desselben Symbols nachgeladen hat.
 * Wuerde er Wartende oder Treffer mitzaehlen, waere die Messung wertlos, und
 * "kein Refetch" waere eine Behauptung statt eines Befunds.
 */

import { describe, expect, it, vi } from 'vitest';

import type { SymbolRef } from '../core/focus-protocol';
import type { SemanticIR } from '../core/semantic-ir';
import { CREATE_USER_IR } from '../test-support/twin-fixtures';
import { IrCache, irCacheKey } from './ir-cache';

function symbol(qualifiedName: string | undefined, name = 'irgendwas'): SymbolRef {
    return {
        name,
        ...(qualifiedName === undefined ? {} : { qualifiedName }),
        kind: 'function',
        uri: 'file:///workspace/src/a.ts',
        range: { start: { line: 0, character: 0 }, end: { line: 1, character: 0 } },
    };
}

const entryFor = (ir: SemanticIR = CREATE_USER_IR) => ({ ir, warnings: [] as string[] });

/** Ein Versprechen, das der Test selbst einloest. */
function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>((done) => {
        resolve = done;
    });
    return { promise, resolve };
}

describe('irCacheKey', () => {
    it('ist der qualifizierte Name', () => {
        expect(irCacheKey(symbol('p.src.a.f'))).toBe('p.src.a.f');
    });

    it('ist undefined ohne qualifizierten Namen, damit kein leerer Schluessel entsteht', () => {
        expect(irCacheKey(symbol(undefined))).toBeUndefined();
        expect(irCacheKey(symbol(''))).toBeUndefined();
    });
});

describe('IrCache', () => {
    it('fragt beim ersten Mal und zaehlt genau eine Anfrage', async () => {
        const onFetch = vi.fn();
        const fetcher = vi.fn(async () => entryFor());
        const cache = new IrCache(fetcher, onFetch);
        await cache.load(symbol('p.src.a.f'));
        expect(fetcher).toHaveBeenCalledTimes(1);
        expect(onFetch).toHaveBeenCalledTimes(1);
    });

    it('fragt beim zweiten Mal nicht und zaehlt nichts dazu', async () => {
        const onFetch = vi.fn();
        const fetcher = vi.fn(async () => entryFor());
        const cache = new IrCache(fetcher, onFetch);
        await cache.load(symbol('p.src.a.f'));
        const again = await cache.load(symbol('p.src.a.f'));
        expect(fetcher).toHaveBeenCalledTimes(1);
        expect(onFetch).toHaveBeenCalledTimes(1);
        expect(again.ir).toBe(CREATE_USER_IR);
    });

    it('schluesselt ueber den qualifizierten Namen, nicht ueber die Zeile', async () => {
        const fetcher = vi.fn(async () => entryFor());
        const cache = new IrCache(fetcher);
        const inSameSymbol = { ...symbol('p.src.a.f'), range: { start: { line: 40, character: 0 }, end: { line: 41, character: 0 } } };
        await cache.load(symbol('p.src.a.f'));
        await cache.load(inSameSymbol);
        expect(fetcher).toHaveBeenCalledTimes(1);
    });

    it('legt zwei gleichzeitige Anfragen auf dasselbe Symbol zusammen', async () => {
        const gate = deferred<{ ir: SemanticIR; warnings: string[] }>();
        const onFetch = vi.fn();
        const fetcher = vi.fn(() => gate.promise);
        const cache = new IrCache(fetcher, onFetch);
        const first = cache.load(symbol('p.src.a.f'));
        const second = cache.load(symbol('p.src.a.f'));
        gate.resolve(entryFor());
        expect(await first).toBe(await second);
        expect(fetcher).toHaveBeenCalledTimes(1);
        expect(onFetch).toHaveBeenCalledTimes(1);
    });

    it('merkt sich eine gescheiterte Anfrage nicht', async () => {
        let calls = 0;
        const fetcher = vi.fn(async () => {
            calls += 1;
            if (calls === 1) {
                throw new Error('Server weg');
            }
            return entryFor();
        });
        const cache = new IrCache(fetcher);
        await expect(cache.load(symbol('p.src.a.f'))).rejects.toThrow('Server weg');
        await cache.load(symbol('p.src.a.f'));
        expect(fetcher).toHaveBeenCalledTimes(2);
        expect(cache.size).toBe(1);
    });

    it('wirft den am laengsten ungenutzten Eintrag hinaus, wenn der Deckel greift', async () => {
        const cache = new IrCache(async () => entryFor(), () => undefined, 2);
        await cache.load(symbol('p.a'));
        await cache.load(symbol('p.b'));
        await cache.load(symbol('p.c'));
        expect(cache.keys()).toEqual(['p.b', 'p.c']);
        expect(cache.size).toBe(2);
    });

    it('macht einen Treffer wieder zum juengsten Eintrag', async () => {
        const cache = new IrCache(async () => entryFor(), () => undefined, 2);
        await cache.load(symbol('p.a'));
        await cache.load(symbol('p.b'));
        await cache.load(symbol('p.a'));
        await cache.load(symbol('p.c'));
        expect(cache.keys()).toEqual(['p.a', 'p.c']);
    });

    it('beantwortet ein Symbol ohne qualifizierten Namen, ohne es abzulegen', async () => {
        const onFetch = vi.fn();
        const fetcher = vi.fn(async () => entryFor());
        const cache = new IrCache(fetcher, onFetch);
        await cache.load(symbol(undefined));
        await cache.load(symbol(undefined));
        expect(fetcher).toHaveBeenCalledTimes(2);
        expect(onFetch).toHaveBeenCalledTimes(2);
        expect(cache.size).toBe(0);
    });

    it('peek fragt nie nach', () => {
        const fetcher = vi.fn(async () => entryFor());
        const cache = new IrCache(fetcher);
        expect(cache.peek(symbol('p.src.a.f'))).toBeUndefined();
        expect(fetcher).not.toHaveBeenCalled();
    });
});
