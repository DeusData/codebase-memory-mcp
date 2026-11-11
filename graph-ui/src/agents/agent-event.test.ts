/*
 * Zwei Fragen an das Ereignismodell, und beide sind Ehrlichkeitsfragen.
 *
 * **Was liest es, und was erfindet es dabei?** Eine fremde JSON-Zeile darf
 * nachsichtig gelesen werden; sie darf nicht ergaenzt werden. Ein fehlender
 * Pfad bleibt leer, ein fehlender Bereich fehlt, eine fehlende Absicht ist
 * keine.
 *
 * **Woran erkennt es die Art der Arbeit?** Am Werkzeugnamen, und fuer den
 * Testlauf am Befehl. Alles andere waere eine Deutung des Befehls, und die
 * einzige Deutung, die dieses Modul sich erlaubt, ist die an einem Programm,
 * das nur eines tut.
 */

import { describe, expect, it } from 'vitest';

import {
    READ_TOOLS,
    WORK_KIND_LETTERS,
    eventKey,
    looksLikeTestRun,
    readAgentEvent,
    readLineSpan,
    sinceParameter,
    workKindOf,
} from './agent-event';

const LINE = {
    ts: 1788038561000,
    agent: 'implementer',
    run: 'a1f2c3d4',
    seq: 3,
    phase: 'end',
    tool: 'Edit',
    path: 'src/services/userService.ts',
    lines: [24, 30],
    detail: 'replaced the validation branch',
};

describe('readAgentEvent', () => {
    it('liest die Felder des Formats, so wie die Bruecke sie schickt', () => {
        const event = readAgentEvent(LINE);
        expect(event).toBeDefined();
        expect(event?.agent).toBe('implementer');
        expect(event?.run).toBe('a1f2c3d4');
        expect(event?.seq).toBe(3);
        expect(event?.tool).toBe('Edit');
        expect(event?.path).toBe('src/services/userService.ts');
        expect(event?.lines).toEqual([24, 30]);
        expect(event?.replay).toBe(false);
    });

    it('nimmt eine Zeile mit unbekannten Feldern und uebergeht sie', () => {
        const event = readAgentEvent({ ...LINE, whatIsThis: 42, source: 'fs' });
        expect(event).toBeDefined();
        expect(event?.source).toBe('fs');
        expect(event).not.toHaveProperty('whatIsThis');
    });

    it('laesst eine Zeile fallen, der eine der vier Pflichtangaben fehlt', () => {
        for (const missing of ['ts', 'agent', 'run', 'tool']) {
            const broken: Record<string, unknown> = { ...LINE };
            delete broken[missing];
            expect(readAgentEvent(broken), missing).toBeUndefined();
        }
        expect(readAgentEvent(null)).toBeUndefined();
        expect(readAgentEvent('a line')).toBeUndefined();
    });

    it('erfindet keine Absicht, wo keine steht', () => {
        expect(readAgentEvent(LINE)?.intent).toBeUndefined();
        expect(readAgentEvent({ ...LINE, intent: '' })?.intent).toBeUndefined();
        expect(readAgentEvent({ ...LINE, intent: 'tightening the check' })?.intent)
            .toBe('tightening the check');
    });

    it('erfindet keinen halben Zeilenbereich', () => {
        expect(readLineSpan([24, 30])).toEqual([24, 30]);
        expect(readLineSpan([30, 24])).toEqual([24, 30]);
        expect(readLineSpan([24])).toBeUndefined();
        expect(readLineSpan([24, 30, 40])).toBeUndefined();
        expect(readLineSpan('24-30')).toBeUndefined();
        expect(readLineSpan([0, 30])).toBeUndefined();
        expect(readAgentEvent({ ...LINE, lines: [24] })?.lines).toBeUndefined();
    });

    it('merkt sich die aufgezeichnete Zeit, wenn die Wiedergabe sie verschoben hat', () => {
        const event = readAgentEvent({ ...LINE, replay: true, ts_recorded: 1788038000000 });
        expect(event?.replay).toBe(true);
        expect(event?.recordedTs).toBe(1788038000000);
    });
});

describe('workKindOf', () => {
    it('liest die vier Arten am Werkzeugnamen ab', () => {
        expect(workKindOf('Read')).toBe('read');
        expect(workKindOf('Edit')).toBe('write');
        expect(workKindOf('Write')).toBe('write');
        expect(workKindOf('Grep')).toBe('search');
        expect(workKindOf('Glob')).toBe('search');
    });

    it('zaehlt das Oeffnen eines Symbols als Lesen', () => {
        expect(READ_TOOLS).toContain('Open');
        expect(workKindOf('Open')).toBe('read');
    });

    it('nennt einen Befehl nur dann einen Testlauf, wenn ein Testprogramm darin steht', () => {
        expect(workKindOf('Bash', 'npx vitest run test/userService.test.ts')).toBe('test');
        expect(workKindOf('Bash', 'node --test tests/scaffold/')).toBe('test');
        expect(workKindOf('Bash', 'npm run test:unit')).toBe('test');
        // Ein Pfad, in dem "test" vorkommt, ist kein Testlauf.
        expect(workKindOf('Bash', 'cat test/userService.test.ts')).toBe('other');
        expect(workKindOf('Bash', 'ls -la')).toBe('other');
        expect(looksLikeTestRun('pytest -q')).toBe(true);
        expect(looksLikeTestRun('grep -rn latest src/')).toBe(false);
    });

    it('sagt bei einem unbekannten Werkzeug, dass es die Art nicht kennt', () => {
        expect(workKindOf('SomeNewTool')).toBe('other');
        expect(WORK_KIND_LETTERS.other).toBe('O');
        expect(new Set(Object.values(WORK_KIND_LETTERS)).size).toBe(5);
    });
});

describe('die Wiederaufnahme', () => {
    it('nennt jede Laufnummer, in einer festen Ordnung', () => {
        const seen = new Map([['zzz', 4], ['aaa', 9]]);
        expect(sinceParameter(seen)).toBe('aaa:9,zzz:4');
        expect(sinceParameter(new Map())).toBe('');
    });

    it('macht aus Lauf und Nummer eine Identitaet', () => {
        expect(eventKey(readAgentEvent(LINE)!)).toBe('a1f2c3d4:3');
    });
});
