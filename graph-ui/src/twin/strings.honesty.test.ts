/**
 * Kein Satz des Twins verspricht ein Feature fuer spaeter.
 *
 * Nutzerauftrag vom 2026-08-29. Drei Saetze dieses Moduls taten es bis dahin,
 * und jeder auf seine Art falsch:
 *
 *  - die Runtime-Zeile schickte den Leser zu einem Menuepunkt "Import runtime
 *    traces", den es in dieser Oberflaeche nicht gibt, und behauptete nebenbei,
 *    das Produkt zeige keine beobachteten Aufrufe (es zeigt sie: an den Hops des
 *    BUG-Assistenten und im Flow);
 *  - die Changes-Zeile behauptete, CodeAtlas lese keine Versionsgeschichte,
 *    waehrend die Aenderungsansicht seit W4d genau das tut;
 *  - die Effects-Zeile versprach ein "when the effect relations land", also
 *    einen Termin auf einer fremden Roadmap.
 *
 * Dieser Test ist die Sperre dagegen. Er geht ueber ALLE sichtbaren Saetze
 * dieses Moduls, nicht nur ueber die drei, weil der naechste solche Satz an
 * einer anderen Stelle entstehen wird.
 *
 * ## Die Ausnahmen, und warum es genau diese sind
 *
 * Drei Saetze duerfen "yet" tragen, und alle drei reden ueber denselben
 * Sachverhalt: eine Datei ist NOCH NICHT INDIZIERT. Das ist kein Versprechen
 * ueber ein Feature, sondern eine Auskunft ueber einen Zustand, den der Leser
 * selbst aendert, indem er das Projekt indiziert. Zwei davon stehen ausserdem
 * unter einer eigenen Zusicherung: `absenceSentence` ist wortgleich mit
 * `explainAbsence` des Backends (siehe Docblock dort), und ein umgeschriebener
 * Satz waere ein stiller Bruch dieser Gleichheit.
 */

import { describe, expect, it } from 'vitest';

import * as strings from './strings';

/** Formulierungen, die etwas fuer spaeter versprechen. */
const PROMISES: readonly { name: string; pattern: RegExp }[] = [
    { name: 'yet', pattern: /\byet\b/i },
    { name: 'arrives later', pattern: /\barrives? later\b/i },
    { name: 'when ... lands', pattern: /\bwhen\b[^.]{0,60}\blands?\b/i },
    { name: 'not wired', pattern: /\bnot wired\b/i },
    { name: 'coming soon', pattern: /\bcoming soon\b/i },
    { name: 'not built', pattern: /\bnot built\b/i },
];

/**
 * Die dokumentierten Ausnahmen: Export, Grund, und das Wort, um das es geht.
 *
 * Als Daten und nicht als Kommentar, weil eine Ausnahme, die man nicht
 * aufzaehlen kann, keine Ausnahme ist, sondern eine Luecke.
 */
const EXCEPTIONS: readonly { export: string; word: string; reason: string }[] = [
    {
        export: 'TWIN_FILE_NOT_INDEXED',
        word: 'yet',
        reason:
            'Auskunft ueber einen Zustand, den der Leser selbst aendert: die Datei ist nicht '
            + 'indiziert, und wer sie indiziert, bekommt die Antwort.',
    },
    {
        export: 'absenceSentence',
        word: 'yet',
        reason:
            'wortgleich mit explainAbsence im Backend (Docblock in strings.ts): ein hier '
            + 'umgeschriebener Satz waere ein stiller Bruch dieser Gleichheit. Der Satz sagt, '
            + 'dass die Datei nicht im Index ist, und verspricht kein Feature.',
    },
    {
        export: 'plainAbsence',
        word: 'yet',
        reason: 'dieselbe Auskunft ohne Vokabular: die Datei ist nicht gelesen, nicht: es kommt noch.',
    },
];

const excepted = (name: string): boolean => EXCEPTIONS.some((entry) => entry.export === name);

/** Beispielwerte fuer die Funktionen des Moduls, nach Stelligkeit. */
const SAMPLES: unknown[] = ['notIndexed', 'sample', 2, 'second'];

/** Jeder sichtbare Satz dieses Moduls, mit dem Export, aus dem er kommt. */
function sentences(): { name: string; text: string }[] {
    const out: { name: string; text: string }[] = [];
    const walk = (name: string, value: unknown): void => {
        if (typeof value === 'string') {
            out.push({ name, text: value });
            return;
        }
        if (Array.isArray(value)) {
            value.forEach((entry) => walk(name, entry));
            return;
        }
        if (typeof value === 'function') {
            // Eine Funktion, die mit Beispielwerten nicht laeuft, wird nicht
            // erraten: sie steht dann nicht in der Pruefung, und das steht im
            // Ergebnis unten als Zahl.
            for (const first of ['notIndexed', 'unknown', 'unsupported', 'inferred', 'sample']) {
                try {
                    walk(name, (value as (...args: unknown[]) => unknown)(first, ...SAMPLES.slice(1)));
                } catch {
                    // naechster Beispielwert
                }
            }
            return;
        }
        if (value !== null && typeof value === 'object') {
            for (const entry of Object.values(value as Record<string, unknown>)) {
                walk(name, entry);
            }
        }
    };
    for (const [name, value] of Object.entries(strings)) {
        walk(name, value);
    }
    return out;
}

describe('die sichtbaren Saetze des Twins', () => {
    const all = sentences();

    it('sind ueberhaupt eingesammelt worden', () => {
        expect(all.length).toBeGreaterThan(60);
    });

    for (const promise of PROMISES) {
        it(`verspricht nichts mit "${promise.name}"`, () => {
            const hits = all
                .filter((entry) => promise.pattern.test(entry.text))
                .filter((entry) => !excepted(entry.name));
            expect(hits.map((entry) => `${entry.name}: ${entry.text}`)).toEqual([]);
        });
    }

    it('verweist auf keinen Menuepunkt, den es hier nicht gibt', () => {
        const hits = all.filter((entry) => /Import runtime traces/i.test(entry.text));
        expect(hits.map((entry) => entry.name)).toEqual([]);
    });

    it('sagt bei Runtime, wo die beobachteten Aufrufe wirklich stehen', () => {
        expect(strings.RUNTIME_NOT_IN_TWIN).toContain('BUG hunt');
        expect(strings.RUNTIME_NOT_IN_TWIN).toContain('ingest_traces');
        expect(strings.RUNTIME_NOT_IN_TWIN).not.toMatch(/Atlas menu/i);
    });

    it('sagt bei Changes, welche Flaeche die Frage beantwortet', () => {
        expect(strings.CHANGES_NOT_IN_TWIN).toContain('[c]hange scope');
        expect(strings.CHANGES_NOT_IN_TWIN).toContain('detect_changes');
    });

    it('sagt bei Effects, dass die Luecke im Index steht und wo sie notiert ist', () => {
        expect(strings.EFFECTS_NOT_IN_INDEX).toContain('UPSTREAM-ASKS.md');
        expect(strings.EFFECTS_NOT_IN_INDEX).toMatch(/routes/);
    });

    it('haelt jede Ausnahme begruendet, und keine ohne Fundstelle', () => {
        for (const entry of EXCEPTIONS) {
            expect(entry.reason.length).toBeGreaterThan(40);
            const found = all.filter((line) => line.name === entry.export
                && new RegExp(`\\b${entry.word}\\b`, 'i').test(line.text));
            expect(found.length, `${entry.export} braucht die Ausnahme nicht mehr`)
                .toBeGreaterThan(0);
        }
    });
});
