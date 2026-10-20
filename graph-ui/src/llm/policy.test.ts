/**
 * Die Vorrangregel: eine committete Policy schlaegt jede Praeferenz.
 *
 * Geprueft wird beides getrennt, weil es zwei Aussagen sind. Erstens: was in der
 * Datei steht, wird richtig gelesen, samt der beiden Faelle, in denen sie da ist
 * und trotzdem nichts Verstaendliches sagt. Zweitens: die Auswertung von Policy
 * und Praeferenz zusammen kommt in jeder Kombination auf dieselbe Antwort, und
 * in keiner davon hebt eine Praeferenz ein `deny` auf.
 */

import { describe, expect, it } from 'vitest';

import { blocksLlm, POLICY_PATH, readPolicySource } from './policy';
import type { PolicyVerdict } from './policy';
import { resolveLlmState } from './llm-state';

describe('was in .codeatlas/policy.json steht', () => {
    it('liegt an dem Pfad, den auch das Referenzprojekt benutzt', () => {
        expect(POLICY_PATH).toBe('.codeatlas/policy.json');
    });

    it('sperrt bei "llm": "deny"', () => {
        expect(readPolicySource('{"llm": "deny"}').verdict).toBe('deny');
    });

    it('erlaubt bei "llm": "allow"', () => {
        expect(readPolicySource('{"llm": "allow"}').verdict).toBe('allow');
    });

    it('ist ohne den Schluessel keine Aussage ueber das LLM', () => {
        expect(readPolicySource('{"telemetry": "deny"}').verdict).toBe('absent');
    });

    it('liest weitere Schluessel nicht als Sperre', () => {
        const reading = readPolicySource('{"chat": "deny", "llm": "allow"}');
        expect(reading.verdict).toBe('allow');
    });

    it('sperrt, wenn die Datei kein lesbares JSON ist', () => {
        const reading = readPolicySource('{ "llm": deny');
        expect(reading.verdict).toBe('unreadable');
        expect(reading.detail).toMatch(/kein lesbares JSON/);
    });

    it('sperrt, wenn dort ein Wert steht, den dieses Programm nicht kennt', () => {
        const reading = readPolicySource('{"llm": "ask-me-later"}');
        expect(reading.verdict).toBe('unreadable');
        expect(reading.detail).toMatch(/ask-me-later/);
    });

    it('sperrt, wenn die Datei kein Objekt enthaelt', () => {
        expect(readPolicySource('["deny"]').verdict).toBe('unreadable');
    });

    it('nennt in jedem Ausgang die Datei, um die es geht', () => {
        for (const source of ['{"llm":"deny"}', '{"llm":"allow"}', '{}', 'kaputt']) {
            expect(readPolicySource(source).path).toBe(POLICY_PATH);
        }
    });
});

describe('welche Urteile sperren', () => {
    it('sind genau deny und unreadable', () => {
        const verdicts: PolicyVerdict[] = ['deny', 'allow', 'absent', 'unreadable'];
        expect(verdicts.filter(blocksLlm)).toEqual(['deny', 'unreadable']);
    });
});

describe('Policy und Praeferenz zusammen', () => {
    it('sperrt bei deny, egal was der Leser eingestellt hat', () => {
        expect(resolveLlmState('deny', true)).toBe('disabled-by-policy');
        expect(resolveLlmState('deny', false)).toBe('disabled-by-policy');
    });

    it('sperrt auch bei einer vorhandenen, aber unlesbaren Policy', () => {
        expect(resolveLlmState('unreadable', true)).toBe('disabled-by-policy');
    });

    it('laesst ohne Policy die Praeferenz entscheiden', () => {
        expect(resolveLlmState('absent', true)).toBe('on');
        expect(resolveLlmState('absent', false)).toBe('off');
    });

    it('laesst auch bei ausdruecklichem allow die Praeferenz entscheiden', () => {
        expect(resolveLlmState('allow', true)).toBe('on');
        expect(resolveLlmState('allow', false)).toBe('off');
    });

    it('ist ohne Antwort der Policy noch nicht an', () => {
        // Solange niemand gefragt hat, ist die Lage nicht "erlaubt", sondern
        // unbekannt, und unbekannt heisst bei einem Opt-out: aus.
        expect(resolveLlmState(undefined, true)).toBe('off');
        expect(resolveLlmState(undefined, false)).toBe('off');
    });
});
