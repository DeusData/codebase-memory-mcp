import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { sha1Hex, sha256Hex, sha256HexSync } from './hash';

/*
 * Der Massstab ist node:crypto, also genau die Bibliothek, gegen die das
 * Referenzprojekt seine Ids und Hashes gebaut hat. Eine eigene Rechnung ist
 * nur dann ein Ersatz, wenn sie dieselben Ziffern liefert; alles andere waere
 * ein stiller Bruch, der erst auffaellt, wenn ein Haken in der IDE gesetzt und
 * hier nicht wiedergefunden wird.
 */

const SAMPLES = [
    '',
    'abc',
    'export function createUser() {}',
    'core-logic|codeatlas-atlas-sample.src.util.validate.validateUser',
    'Umlaute und ein Emoji: aeoeue \u{1F600}',
    'x'.repeat(1000),
];

describe('sha1, wie der Checklisten-Generator sie braucht', () => {

    it('stimmt fuer jede Probe mit node:crypto ueberein', () => {
        for (const sample of SAMPLES) {
            expect(sha1Hex(sample)).toBe(createHash('sha1').update(sample).digest('hex'));
        }
    });

    it('liefert 40 Hexziffern, auch fuer die leere Eingabe', () => {
        expect(sha1Hex('')).toMatch(/^[0-9a-f]{40}$/);
        expect(sha1Hex('')).toBe('da39a3ee5e6b4b0d3255bfef95601890afd80709');
    });
});

describe('sha256, wie der IR-Builder sie braucht', () => {

    it('stimmt fuer jede Probe mit node:crypto ueberein', async () => {
        for (const sample of SAMPLES) {
            const expected = createHash('sha256').update(sample).digest('hex');
            expect(await sha256Hex(sample)).toBe(expected);
        }
    });

    it('liefert dieselben Ziffern ohne WebCrypto wie mit', async () => {
        for (const sample of SAMPLES) {
            expect(sha256HexSync(sample)).toBe(await sha256Hex(sample));
        }
    });

    it('liefert 64 Hexziffern', async () => {
        expect(await sha256Hex('abc')).toMatch(/^[0-9a-f]{64}$/);
    });
});
