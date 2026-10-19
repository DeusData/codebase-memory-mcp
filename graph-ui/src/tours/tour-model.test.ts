/**
 * Der portierte Lesestand einer Fuehrung.
 *
 * Die drei Eigenschaften, um derentwillen tour-model.ts eine eigene Datei ist:
 * dass die Abdeckung abrundet und nie eine unvollstaendige Liste als
 * vollstaendig zeigt, dass ein betretener Schritt genau ein Item vermerkt und
 * nicht die ganze Liste, und dass ein Wiederaufnahme-Angebot drei Faelle
 * ablehnt statt sie zu erraten.
 */

import { describe, expect, it } from 'vitest';

import type { ChecklistItemState, UnderstandingState } from '../checklist/checklist-model';
import type { TourDto, TourProgressDto, TourStepDto } from '../core/tour-protocol';
import { toEditorRange } from '../core/positions';
import {
    TOUR_MARK_CATEGORY,
    coveragePercent,
    emptyCoverage,
    markableItemId,
    resumeStep,
    stepSymbol,
    sumCoverage,
} from './tour-model';

const item = (
    id: string,
    category: ChecklistItemState['category'],
    visited = false,
): ChecklistItemState => ({ id, category, label: id, visited, confirmed: false });

const stateOf = (visited: number, total: number): UnderstandingState => ({
    symbolQualifiedName: 'p.s',
    items: [],
    exploration: { visited, total },
    verification: { confirmed: 0, total },
});

describe('coverage', () => {
    it('starts at nothing counted, which is not nothing to count', () => {
        expect(emptyCoverage()).toEqual({ visited: 0, total: 0, percent: 0, symbols: 0 });
    });

    it('rounds down, so a nearly finished list is never shown as finished', () => {
        expect(coveragePercent(249, 250)).toBe(99);
        expect(coveragePercent(1, 3)).toBe(33);
    });

    it('reaches a hundred only on exactness', () => {
        expect(coveragePercent(250, 250)).toBe(100);
        expect(coveragePercent(251, 250)).toBe(100);
    });

    it('is zero when there is nothing to divide by', () => {
        expect(coveragePercent(4, 0)).toBe(0);
    });

    it('adds up only the symbols that had a checklist at all', () => {
        expect(sumCoverage([stateOf(1, 4), undefined, stateOf(2, 6)])).toEqual({
            visited: 3,
            total: 10,
            percent: 30,
            symbols: 2,
        });
    });
});

describe('what a step marks', () => {
    it('takes the first core-logic item', () => {
        expect(markableItemId([item('a', 'callers'), item('b', TOUR_MARK_CATEGORY), item('c', TOUR_MARK_CATEGORY)]))
            .toBe('b');
    });

    it('falls back to the first item of any category', () => {
        expect(markableItemId([item('a', 'tests'), item('b', 'callers')])).toBe('a');
    });

    it('marks nothing when there is nothing to mark', () => {
        expect(markableItemId([])).toBeUndefined();
    });
});

describe('resuming', () => {
    const tour = (steps: number): TourDto => ({
        schemaVersion: 1,
        id: 'getting-started',
        title: 'Getting started',
        generated: { strategy: 'topsort' },
        steps: Array.from({ length: steps }, (_unused, order) => ({
            id: `s${order}`,
            title: `s${order}`,
            description: '',
            order,
            primary: { kind: 'file', filePath: `f${order}.ts`, uri: `file:///f${order}.ts` },
        })),
        path: '',
    });
    const progress = (tourId: string, stepIndex: number): TourProgressDto => ({
        tourId,
        stepIndex,
        updatedAt: '2026-08-28T00:00:00.000Z',
    });

    it('offers the step the reader last arrived at', () => {
        expect(resumeStep(tour(6), progress('getting-started', 3))).toBe(3);
    });

    it('refuses progress against another walk', () => {
        expect(resumeStep(tour(6), progress('entry-p-s', 3))).toBeUndefined();
    });

    it('refuses the first step, which is what starting already does', () => {
        expect(resumeStep(tour(6), progress('getting-started', 0))).toBeUndefined();
    });

    it('refuses a step a shorter walk no longer has', () => {
        expect(resumeStep(tour(2), progress('getting-started', 5))).toBeUndefined();
    });

    it('refuses when there is nothing to resume into', () => {
        expect(resumeStep(undefined, progress('getting-started', 2))).toBeUndefined();
        expect(resumeStep(tour(6), undefined)).toBeUndefined();
    });
});

describe('what a step points at', () => {
    const symbolStep: TourStepDto = {
        id: 's',
        title: 's',
        description: '',
        order: 0,
        primary: {
            kind: 'symbol',
            filePath: 'src/config.ts',
            symbol: {
                name: 'loadConfig',
                qualifiedName: 'p.src.config.loadConfig',
                kind: 'function',
                uri: 'file:///workspace/src/config.ts',
                range: toEditorRange(11, 11),
            },
        },
    };

    it('hands back the symbol of a symbol step', () => {
        expect(stepSymbol(symbolStep)?.name).toBe('loadConfig');
    });

    it('invents nothing for a file step', () => {
        expect(
            stepSymbol({
                id: 'f',
                title: 'f',
                description: '',
                order: 0,
                primary: { kind: 'file', filePath: 'a.md', uri: 'file:///a.md' },
            }),
        ).toBeUndefined();
        expect(stepSymbol(undefined)).toBeUndefined();
    });
});
