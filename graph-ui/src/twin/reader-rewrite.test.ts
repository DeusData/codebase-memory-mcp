/*
 * Der Waechter ueber der Umformulierung: was das Modell aendern darf und was
 * es nicht darf.
 *
 * Die Zusicherungen sind nach den Fehlern geordnet, die ein Modell wirklich
 * macht, und nicht nach den Zweigen der Funktion. Ein Modell, das einen Namen
 * neu schreibt, ist der teuerste Fall dieses Zyklus: die Zeile sieht danach
 * genauso vertrauenswuerdig aus wie vorher und zeigt auf ein Symbol, das es
 * nicht gibt.
 */

import { describe, expect, it } from 'vitest';

import {
    applyReaderRewrite,
    factsOf,
    lengthCeiling,
    readerLines,
    readerMaxTokens,
    readerSubjectText,
    rewriteMap,
} from './reader-rewrite';

const LINES = readerLines([
    { id: 'p0', text: 'This is a function. It hands work to 6 other pieces of code.' },
    { id: 'p1', text: 'First it hands work to validateUser (validate.ts:19).' },
    { id: 'n0', text: 'Nothing that looks like a test calls this.' },
]);

describe('factsOf', () => {
    it('haelt Namen, Dateien und Zahlen fest, in der Reihenfolge des Satzes', () => {
        expect(factsOf('First it hands work to validateUser (validate.ts:19).'))
            .toEqual(['validateUser', 'validate.ts', '19']);
    });

    it('erkennt einen Pfad als eine Sache und nicht als zwei', () => {
        expect(factsOf('It sits on src/services/userService.ts.'))
            .toEqual(['src/services/userService.ts']);
    });

    it('haelt einen Namen mit Grossbuchstaben im Wort fest', () => {
        expect(factsOf('It can raise ValidationError.')).toEqual(['ValidationError']);
        expect(factsOf('CodeAtlas put it together.')).toEqual(['CodeAtlas']);
    });

    it('haelt ein gewoehnliches Wort NICHT fest', () => {
        // Sonst waere jede Umformulierung verboten, die "failures" statt
        // "errors" sagt, und genau diese Freiheit ist der ganze Zweck.
        expect(factsOf('It reads nothing from the environment it runs in.')).toEqual([]);
    });

    it('haelt eine Kommazahl als eine Zahl fest', () => {
        expect(factsOf('confidence 0.90 via construction')).toEqual(['0.90']);
    });
});

describe('applyReaderRewrite laesst durch', () => {
    it('eine Umformulierung, die jeden Fakt an seiner Stelle laesst', () => {
        const outcome = applyReaderRewrite(LINES, [
            'This is a function, and it leans on 6 other bits of code to do its job.',
            'It starts by handing work to validateUser, over in validate.ts:19.',
            'No test seems to call this one.',
        ].join('\n'));
        expect(outcome.kind).toBe('applied');
        if (outcome.kind === 'applied') {
            expect(outcome.lines[1].text).toContain('validateUser');
            expect(rewriteMap(outcome.lines)['n0']).toBe('No test seems to call this one.');
        }
    });

    it('eine Antwort in einem Codezaun, weil das nur Verpackung ist', () => {
        const outcome = applyReaderRewrite(readerLines([{ id: 'p0', text: 'It calls 2 things.' }]),
            '```\nIt reaches out to 2 things.\n```');
        expect(outcome.kind).toBe('applied');
    });
});

describe('applyReaderRewrite verwirft', () => {
    const refuse = (answer: string, lines = LINES): string => {
        const outcome = applyReaderRewrite(lines, answer);
        expect(outcome.kind).toBe('refused');
        return outcome.kind === 'refused' ? outcome.reason : '';
    };

    it('einen umgeschriebenen Namen', () => {
        const reason = refuse([
            'This is a function. It hands work to 6 other pieces of code.',
            'It starts with validateUsers, over in validate.ts:19.',
            'Nothing that looks like a test calls this.',
        ].join('\n'));
        expect(reason).toContain('validateUsers');
        expect(reason).toContain('validateUser');
        expect(reason).toContain('sentence 2');
    });

    it('eine geaenderte Zahl', () => {
        const reason = refuse([
            'This is a function. It hands work to 7 other pieces of code.',
            'First it hands work to validateUser (validate.ts:19).',
            'Nothing that looks like a test calls this.',
        ].join('\n'));
        expect(reason).toContain('"7"');
        expect(reason).toContain('"6"');
    });

    it('eine geaenderte Datei', () => {
        const reason = refuse([
            'This is a function. It hands work to 6 other pieces of code.',
            'First it hands work to validateUser (validate.js:19).',
            'Nothing that looks like a test calls this.',
        ].join('\n'));
        expect(reason).toContain('validate.js');
    });

    it('eine geaenderte Zeile', () => {
        const reason = refuse([
            'This is a function. It hands work to 6 other pieces of code.',
            'First it hands work to validateUser (validate.ts:21).',
            'Nothing that looks like a test calls this.',
        ].join('\n'));
        expect(reason).toContain('"21"');
    });

    it('eine vertauschte Reihenfolge der Aussagen', () => {
        const reason = refuse([
            'First it hands work to validateUser (validate.ts:19).',
            'This is a function. It hands work to 6 other pieces of code.',
            'Nothing that looks like a test calls this.',
        ].join('\n'));
        expect(reason).toContain('sentence 1');
    });

    it('zwei vertauschte Saetze, die gar keinen Fakt tragen', () => {
        /*
         * Die Luecke, die eine reine Faktenpruefung nicht sehen kann: zwei
         * Saetze ohne Namen und ohne Zahl sehen ihr gleich aus, und ein Tausch
         * wuerde die Aussage der einen Ueberschrift unter die andere stellen.
         */
        const plain = readerLines([
            { id: 'a', text: 'Nothing here was recorded as stopping with an error.' },
            { id: 'b', text: 'It reads nothing from the environment it runs in.' },
        ]);
        const reason = refuse([
            'It reads nothing from the environment it runs in.',
            'Nothing here was recorded as stopping with an error.',
        ].join('\n'), plain);
        expect(reason).toContain('different order');
    });

    it('aber nicht eine ehrliche Umformulierung, die ihre Woerter wechselt', () => {
        const plain = readerLines([
            { id: 'a', text: 'Nothing here was recorded as stopping with an error.' },
            { id: 'b', text: 'It reads nothing from the environment it runs in.' },
        ]);
        const outcome = applyReaderRewrite(plain, [
            'No failure was ever written down for this one.',
            'It takes no configuration from around it.',
        ].join('\n'));
        expect(outcome.kind).toBe('applied');
    });

    it('einen dazuerfundenen Namen', () => {
        const reason = refuse([
            'This is a function. It hands work to 6 other pieces of code.',
            'First it hands work to validateUser (validate.ts:19), which calls saveUser.',
            'Nothing that looks like a test calls this.',
        ].join('\n'));
        expect(reason).toContain('added "saveUser"');
    });

    it('einen weggelassenen Namen', () => {
        const reason = refuse([
            'This is a function. It hands work to 6 other pieces of code.',
            'First it hands work to validateUser, over in validate.ts.',
            'Nothing that looks like a test calls this.',
        ].join('\n'));
        expect(reason).toContain('dropped "19"');
    });

    it('eine falsche Zahl von Saetzen', () => {
        expect(refuse('Just the one line.')).toContain('1 sentences instead of 3');
    });

    it('eine leere Antwort', () => {
        expect(refuse('   \n  \n')).toContain('answered with nothing');
    });

    it('einen Aufsatz, der jeden Fakt behaelt und die Seite sprengt', () => {
        const padded = 'It hands work to 6 other pieces of code, '
            + 'and here is a great deal of additional prose that nobody asked for, '
            + 'stretching on and on well past anything a sentence should be, '
            + 'because a model that has been told to reword sometimes decides to teach.';
        const reason = refuse([
            `This is a function. ${padded}`,
            'First it hands work to validateUser (validate.ts:19).',
            'Nothing that looks like a test calls this.',
        ].join('\n'));
        expect(reason).toContain('over the ceiling');
    });

    it('eine Umformulierung, wenn es auf der Stufe gar nichts zu formulieren gab', () => {
        expect(refuse('anything at all', [])).toContain('nothing on this level');
    });
});

describe('was der Aufrufer braucht', () => {
    it('gibt die Saetze als einen Text, einer je Zeile', () => {
        expect(readerSubjectText(LINES).split('\n')).toHaveLength(3);
    });

    it('bemisst das Token-Budget am Text und nicht an einer festen Zahl', () => {
        expect(readerMaxTokens(LINES)).toBeGreaterThanOrEqual(256);
        expect(readerMaxTokens(readerLines([{ id: 'p0', text: 'x'.repeat(4000) }]))).toBe(2000);
    });

    it('laesst einen leeren Satz gar nicht erst in die Liste', () => {
        expect(readerLines([{ id: 'p0', text: '' }, { id: 'p1', text: 'Something.' }]))
            .toHaveLength(1);
    });

    it('gibt der Decke eine Zahl, die vom Satz abhaengt', () => {
        expect(lengthCeiling('abc')).toBe(46);
    });
});
