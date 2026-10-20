/*
 * Die Coverage-Saetze: geprueft wird, was sie NICHT sagen.
 *
 * Ein Satz, der einen Grund erfindet, faellt in einem Beweisbild nicht auf; er
 * liest sich sogar besser. Darum steht hier zu jedem Satz ein Fall ohne Grund
 * daneben, und der Test verlangt, dass die Formulierung dann ohne Grund endet.
 */
import { describe, expect, it } from 'vitest';
import {
    COVERAGE_DESCRIPTIONS,
    COVERAGE_LABELS,
    COVERAGE_MARKS,
    COVERAGE_SOURCE_NOTE,
    coverageTooltip,
    folderTooltip,
    freshnessNote,
    freshnessNoteNeeded,
    partialFileNote,
    readerUnavailableNote,
} from './coverage-strings';
import { COVERAGE_ORDER } from './tree-model';

describe('die Tabellen decken jede Stufe ab', () => {

    it('hat zu jeder Stufe ein Wort, ein Zeichen und einen Satz', () => {
        for (const state of COVERAGE_ORDER) {
            expect(COVERAGE_LABELS[state].length).toBeGreaterThan(0);
            expect(COVERAGE_DESCRIPTIONS[state].length).toBeGreaterThan(0);
            expect(typeof COVERAGE_MARKS[state]).toBe('string');
        }
    });

    it('laesst genau die vollstaendige Stufe ohne Zeichen', () => {
        expect(COVERAGE_MARKS.indexed).toBe('');
        expect(COVERAGE_MARKS.partial).toBe('!');
    });
});

describe('COVERAGE_SOURCE_NOTE', () => {

    it('benennt die Grenze des Baums und nicht seinen Inhalt', () => {
        expect(COVERAGE_SOURCE_NOTE).toContain('as the index discovery saw them');
        expect(COVERAGE_SOURCE_NOTE).toContain('never met are invisible');
    });
});

describe('coverageTooltip', () => {

    it('nennt bei partial Zeilenbereiche und keine Symbolzahl', () => {
        const text = coverageTooltip('partial', '12-18,24');
        expect(text).toContain('partially parsed');
        expect(text).toContain('12-18,24');
        // Der Server meldet Bereiche, keine Zahl. Eine Zahl waere gezaehlt,
        // und gezaehlt hat sie niemand.
        expect(text).not.toMatch(/\d+ symbols/);
    });

    it('endet ohne Grund, wenn der Server keinen genannt hat', () => {
        expect(coverageTooltip('skipped')).toContain('named no reason');
        expect(coverageTooltip('not-indexed')).toContain('named no rule');
    });

    it('sagt bei einem Grund den Grund des Servers', () => {
        expect(coverageTooltip('skipped', 'unsupported extension'))
            .toContain('unsupported extension');
        expect(coverageTooltip('not-indexed', '.gitignore')).toContain('.gitignore');
    });

    it('behauptet fuer indexed keine Vollstaendigkeit, nur Befundfreiheit', () => {
        expect(coverageTooltip('indexed')).toContain('no source recorded an issue');
    });
});

describe('folderTooltip', () => {

    it('spricht ueber den Inhalt und nicht ueber den Ordner selbst', () => {
        expect(folderTooltip('skipped')).toContain('worst stage below this folder');
        expect(folderTooltip('indexed')).toContain('every file below it');
    });
});

describe('readerUnavailableNote', () => {

    const note = readerUnavailableNote('assets/beleg.png', 'skipped', 'unsupported extension');

    it('erklaert die Grenze der Server-Flaeche statt einen Fehler zu melden', () => {
        expect(note).toContain('get_code_snippet');
        expect(note).toContain('Upstream-Ask 1');
        expect(note).not.toMatch(/error|failed/i);
    });

    it('sagt ausdruecklich, dass die Datei existiert', () => {
        expect(note).toContain('is on disk');
    });

    it('nennt den Grund des Servers, wenn es einen gibt, und sonst keinen', () => {
        expect(note).toContain('unsupported extension');
        expect(readerUnavailableNote('a.bin', 'not-indexed')).toContain('named no rule');
    });
});

describe('partialFileNote', () => {

    it('qualifiziert den Text, statt ihn zu verdaechtigen', () => {
        const text = partialFileNote('src/broken.ts', '12-18');
        expect(text).toContain('12-18');
        expect(text).toContain('The text below is the file');
    });
});

describe('freshnessNoteNeeded', () => {

    it('schweigt genau dann, wenn der Server nichts zu melden hat', () => {
        expect(freshnessNoteNeeded('no_recorded_issue', 'metadata_match')).toBe(false);
    });

    it('meldet eine geaenderte Datei und einen aufgezeichneten Befund', () => {
        expect(freshnessNoteNeeded('no_recorded_issue', 'metadata_changed')).toBe(true);
        expect(freshnessNoteNeeded('partial', 'metadata_match')).toBe(true);
        expect(freshnessNoteNeeded('coverage_unavailable', 'unavailable')).toBe(true);
    });
});

describe('freshnessNote', () => {

    it('nennt Frische, Befund und Empfehlung in dieser Reihenfolge', () => {
        expect(freshnessNote('partial', 'metadata_changed', 'read_source_and_reindex'))
            .toBe('metadata_changed / partial / read_source_and_reindex');
    });

    it('laesst weg, was der Server nicht geschrieben hat', () => {
        expect(freshnessNote('', 'metadata_changed', '')).toBe('metadata_changed');
        expect(freshnessNote('', '', '')).toBe('unknown');
    });
});
