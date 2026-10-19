// W14 correction acceptance: a missing resolved call is not complete behavior.
// Run: node --test tests/scaffold/w14-leaf-honesty.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const artifact = () => JSON.parse(readFileSync(
    join(ROOT, 'verification', 'w14', 'symbols.json'), 'utf8',
));
const LEVELS = ['vibe coder', 'junior', 'medior', 'senior', 'architect'];
const FORBIDDEN = /works alone|complete recorded behavior|complete[^.]{0,80}inside|does its work by itself/i;

test('AC1: every leaf lead states kind, line count and file without claiming completeness', () => {
    const a = artifact();
    assert.ok(Array.isArray(a.leafLeads) && a.leafLeads.length >= 3,
        'die direkt gemessenen Blatt-Einstiege fehlen');
    for (const lead of a.leafLeads) {
        assert.match(lead.text, new RegExp(`^${lead.symbol}\\s+is\\s+a\\s+`, 'i'),
            `${lead.symbol}: der positive Symbolart-Einstieg fehlt`);
        assert.match(lead.text, /\b\d+\s+lines\b/i,
            `${lead.symbol}: der Einstieg nennt keine Zeilenzahl`);
        assert.match(lead.text, /\.(?:ts|tsx|js|jsx|mjs|cjs)\b/i,
            `${lead.symbol}: der Einstieg nennt keine Datei`);
        assert.doesNotMatch(lead.text, FORBIDDEN,
            `${lead.symbol}: der Einstieg behauptet weiterhin Vollstaendigkeit`);
    }
});

test('AC2: query and insert keep shared rows visible on all levels without contradiction', () => {
    const a = artifact();
    for (const symbol of ['query', 'insert']) {
        for (const level of LEVELS) {
            const text = a.symbols?.[symbol]?.levels?.[level]?.text ?? '';
            assert.match(text, /\brows\b/,
                `${symbol}/${level}: der geteilte Zustand rows fehlt`);
            assert.doesNotMatch(text, FORBIDDEN,
                `${symbol}/${level}: Blatttext widerspricht dem geteilten Zustand`);
        }
    }
});

test('AC3: the junior leaf text describes only the resolved-call boundary', () => {
    const text = artifact().symbols?.query?.levels?.junior?.text ?? '';
    assert.match(text, /index/i, 'die Grenze wird nicht dem Index zugeschrieben');
    assert.match(text, /outgoing call/i,
        'der Leertext nennt nicht die fehlende aufgeloeste ausgehende Kante');
    assert.doesNotMatch(text, /works? by itself|does its work by itself|nothing else|no shared state/i,
        'der Leertext macht weiter eine Aussage ueber das gesamte Verhalten');
});

test('AC4: help explains built questions and the explicit optional AI action', () => {
    const text = artifact().helpText ?? '';
    assert.ok(text.length > 100, 'der frisch gerenderte Hilfetext fehlt im W14-Artefakt');
    assert.doesNotMatch(text, /sent to (?:the )?local model|local model as a question/i,
        'die Hilfe behauptet weiterhin einen automatischen Modellrequest');
    assert.match(text, /question mark/i, 'der Fragezeichenweg fehlt in der Hilfe');
    assert.match(text, /(?:indexed|index) cards/i,
        'die gebaute Antwort aus Indexkarten fehlt in der Hilfe');
    assert.match(text, /AI button/i,
        'die optionale Modellfassung wird nicht als ausdruecklicher AI-Knopf erklaert');
});
