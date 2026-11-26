/*
 * Die Farbe und der Buchstabe eines Akteurs: zwei Zusicherungen, beide
 * pruefbar ohne einen gerenderten Pixel.
 *
 * **Dieselbe Kennung, dieselbe Farbe.** Sonst tauschen zwei Agenten die Farbe,
 * sobald einer frueher eintrifft, und der Leser sieht eine Bewegung, die es
 * nicht gab.
 *
 * **Der Farbton liegt NIE im warmen Viertel.** Dort liegen die Knotenfarben
 * dieses Graphen (Sternklassen). Ein Agentenkoerper in einer Knotenfarbe waere
 * genau die Verwechslung, gegen die die eigene Ebene gebaut ist.
 */

import { describe, expect, it } from 'vitest';

import {
    AGENT_HUE_END,
    AGENT_HUE_START,
    YOU_COLOR,
    YOU_ID,
    agentColor,
    agentHue,
    agentLetters,
    hashOf,
} from './agent-colors';

const NAMES = [
    'implementer', 'checker', 'explorer', 'reviewer', 'auditor', 'orchestrator',
    'agent', 'file-writes', 'a', 'zzz', 'Agent 7', 'run-1234',
];

describe('die Farbe', () => {
    it('haengt nur an der Kennung und an nichts sonst', () => {
        for (const name of NAMES) {
            expect(agentColor(name)).toBe(agentColor(name));
            expect(hashOf(name)).toBe(hashOf(name));
        }
    });

    it('liegt immer im kuehlen Band und nie bei den Sternfarben der Knoten', () => {
        for (const name of NAMES) {
            const hue = agentHue(name);
            expect(hue, name).toBeGreaterThanOrEqual(AGENT_HUE_START);
            expect(hue, name).toBeLessThan(AGENT_HUE_END);
        }
        // Das warme Viertel, in dem die Knoten dieser Fixture liegen (#ff6050
        // bis #fff0c0, also Farbton 15 bis 45), ist ausgeschlossen.
        expect(AGENT_HUE_START).toBeGreaterThan(45);
    });

    it('gibt dem Leser eine eigene Farbe ausserhalb des Bandes', () => {
        expect(agentColor(YOU_ID)).toBe(YOU_COLOR);
        expect(NAMES.map(agentColor)).not.toContain(YOU_COLOR);
    });
});

describe('der Buchstabe', () => {
    it('nimmt den Anfang des Namens', () => {
        const letters = agentLetters([
            { id: 'implementer', name: 'implementer' },
            { id: 'checker', name: 'checker' },
            { id: 'explorer', name: 'explorer' },
        ]);
        expect(letters.get('implementer')).toBe('I');
        expect(letters.get('checker')).toBe('C');
        expect(letters.get('explorer')).toBe('E');
    });

    it('gibt zwei Akteuren mit demselben Anfang NIE denselben Buchstaben', () => {
        const letters = agentLetters([
            { id: 'checker', name: 'checker' },
            { id: 'compiler', name: 'compiler' },
            { id: 'collector', name: 'collector' },
        ]);
        const seen = [...letters.values()];
        expect(new Set(seen).size).toBe(seen.length);
    });

    it('haelt das Y des Lesers frei, auch neben einem Agenten mit Y', () => {
        const letters = agentLetters([
            { id: 'yara', name: 'yara' },
            { id: YOU_ID, name: 'you' },
        ]);
        expect(letters.get(YOU_ID)).toBe('Y');
        expect(letters.get('yara')).not.toBe('Y');
    });

    it('vergibt in der Ordnung der Kennungen, damit der Reload nichts vertauscht', () => {
        const actors = [
            { id: 'checker', name: 'checker' },
            { id: 'compiler', name: 'compiler' },
        ];
        const first = agentLetters(actors);
        const second = agentLetters([...actors].reverse());
        expect([...second.entries()].sort()).toEqual([...first.entries()].sort());
    });

    it('bleibt bei einem Namen ohne Buchstaben trotzdem eindeutig', () => {
        const letters = agentLetters([
            { id: '1', name: '???' },
            { id: '2', name: '???' },
        ]);
        expect(new Set(letters.values()).size).toBe(2);
    });
});
