/**
 * Die Beispiele der Kommandozeile: dass sie aus dem Index kommen und dass sie
 * beide Jobs der Zeile zeigen.
 *
 * Die eine Eigenschaft, an der alles haengt: ein Beispiel mit einem Symbol, das
 * dieses Projekt nicht hat, waere eine Einladung ins Leere. Der Beweislauf
 * haelt die Namen im Browser gegen den geladenen Index (examplesUseRealSymbols);
 * hier steht, dass die Wahl ordinal und damit wiederholbar ist und dass sie
 * ohne geladenen Index gar keine Beispiele erfindet.
 */

import { describe, expect, it } from 'vitest';

import {
    COMMAND_EXAMPLES_LABEL,
    EXAMPLE_SEARCH_LETTERS,
    commandExamplesFor,
    commandPlaceholderFor,
    exampleSymbolOf,
} from './command-examples';
import type { ExampleSymbol } from './command-examples';

const pool: ExampleSymbol[] = [
    { name: 'UserEntity', kind: 'class' },
    { name: 'validateUser', kind: 'function' },
    { name: 'createUser', kind: 'function' },
    { name: 'toUser', kind: 'method' },
];

describe('exampleSymbolOf', () => {
    it('waehlt ordinal und bevorzugt etwas Aufrufbares', () => {
        expect(exampleSymbolOf(pool)).toBe('createUser');
    });

    it('waehlt dasselbe, egal in welcher Reihenfolge der Server geantwortet hat', () => {
        expect(exampleSymbolOf([...pool].reverse())).toBe('createUser');
    });

    it('nimmt notfalls auch etwas nicht Aufrufbares', () => {
        expect(exampleSymbolOf([{ name: 'UserEntity', kind: 'class' }])).toBe('UserEntity');
    });

    it('erfindet nichts, wenn nichts geladen ist', () => {
        expect(exampleSymbolOf([])).toBeUndefined();
    });

    /*
     * Der Fall, an dem es aufgefallen ist: der erste Lauf von smoke-w8b waehlte
     * `constructor`, weil jede Klasse dieses Fixtures einen hat und der Name
     * ordinal weit vorn steht. "Who calls constructor?" ist eine Frage, deren
     * Antwort niemandem hilft, und ein Beispiel soll zeigen, wie die Zeile
     * benutzt wird.
     */
    it('nimmt keinen Namen, den jede Klasse hat', () => {
        expect(exampleSymbolOf([
            { name: 'constructor', kind: 'method' },
            { name: 'validateUser', kind: 'function' },
        ])).toBe('validateUser');
    });

    it('laesst Namen aus, die man nicht abtippen kann', () => {
        expect(exampleSymbolOf([
            { name: 'a', kind: 'function' },
            { name: 'has space', kind: 'function' },
            { name: '<anonymous>', kind: 'function' },
        ])).toBeUndefined();
    });
});

describe('commandExamplesFor', () => {
    it('zeigt beide Jobs der Zeile: eine Suche und zwei Fragen', () => {
        const examples = commandExamplesFor('createUser');
        expect(examples).toHaveLength(3);
        expect(examples.map((entry) => entry.id)).toEqual(['search', 'at', 'question']);
        expect(examples[0].text).toBe('cr');
        expect(examples[0].text.length).toBe(EXAMPLE_SEARCH_LETTERS);
        expect(examples[1].text).toBe('@createUser what does it do?');
        expect(examples[2].text).toBe('Who calls createUser?');
    });

    it('haengt jedes Beispiel an den Namen, aus dem es kommt', () => {
        for (const example of commandExamplesFor('createUser')) {
            expect(example.symbol).toBe('createUser');
            expect(example.note.length).toBeGreaterThan(0);
        }
    });
});

describe('commandPlaceholderFor', () => {
    it('nennt ein echtes Beispiel, sobald es eines gibt', () => {
        expect(commandPlaceholderFor('createUser', 'fallback'))
            .toBe('@createUser what does it do?');
    });

    it('faellt auf die Gattung zurueck, statt einen Namen zu erfinden', () => {
        expect(commandPlaceholderFor(undefined, 'type a command or ask the atlas...'))
            .toBe('type a command or ask the atlas...');
    });

    it('sagt ueber der Liste, worum es geht', () => {
        expect(COMMAND_EXAMPLES_LABEL.length).toBeGreaterThan(0);
    });
});
