/**
 * Die vier Karten, ihre Wortwahl und was eine Antwort am Twin bewegt.
 *
 * Der Wortlaut wird hier geprueft und nicht nur im Browser-Lauf: der Lauf
 * findet ein verbotenes Wort erst, wenn es schon gezeichnet ist, und diese
 * Suite findet es beim Schreiben. Beide pruefen dieselbe Liste, damit sie nicht
 * auseinanderlaufen koennen.
 */

import { describe, expect, it } from 'vitest';

import { Facet } from '../twin/presentation-profile';
import type { PresentationProfile } from '../twin/presentation-profile';
import {
    AVOIDED_WORDS,
    WHY_CARDS,
    WHY_DECLINE_LABEL,
    WHY_HEADLINE,
    WHY_MENU_LABEL,
    WHY_SUBLINE,
    avoidedWordsIn,
    profileFor,
} from './why-model';

const FALLBACK: PresentationProfile = {
    id: 'custom:test',
    label: 'Test',
    depth: 2,
    facets: [Facet.Logic, Facet.Calls, Facet.Data, Facet.Errors, Facet.Tests],
    terminology: 'technical',
    conceptCallouts: false,
    panels: [],
    aiChangeWatcher: false,
};

describe('the cards', () => {
    it('are four, in the order of a working day', () => {
        expect(WHY_CARDS.map((card) => card.intent)).toEqual(['bug', 'change', 'understand', 'entry']);
    });

    it('carry no sentence about doing less than they say, because all four open a panel now', () => {
        // The field stays on the type for the next card that lands before its
        // panel does; what must not stay is a note that stopped being true.
        expect(WHY_CARDS.filter((card) => card.stub !== undefined)).toEqual([]);
    });

    it('give every card a sentence about what the answer is for', () => {
        for (const card of WHY_CARDS) {
            expect(card.label.length).toBeGreaterThan(0);
            expect(card.detail.length).toBeGreaterThan(20);
        }
    });
});

describe('the wording', () => {
    const visible = [
        WHY_HEADLINE,
        WHY_SUBLINE,
        WHY_DECLINE_LABEL,
        WHY_MENU_LABEL,
        ...WHY_CARDS.flatMap((card) => [card.label, card.detail, card.stub ?? '']),
    ];

    it('uses none of the words this product does not use about its reader', () => {
        for (const text of visible) {
            expect(avoidedWordsIn(text), text).toEqual([]);
        }
    });

    it('would catch one if it appeared', () => {
        expect(avoidedWordsIn('Learn the codebase')).toEqual(['learn']);
        expect(avoidedWordsIn('lesson three of the tutorial')).toEqual(['lesson', 'tutorial']);
        expect(AVOIDED_WORDS).toContain('course');
    });

    it('still lets the ordinary word "understand" through', () => {
        expect(avoidedWordsIn('Understand the project')).toEqual([]);
    });
});

describe('what an answer sets', () => {
    it('gives a bug hunt the error and runtime lenses', () => {
        const profile = profileFor('bug', FALLBACK);
        expect(profile.depth).toBe(2);
        expect(profile.facets).toContain(Facet.Errors);
        expect(profile.facets).toContain(Facet.Runtime);
    });

    it('gives a change the lenses that say what would break', () => {
        const profile = profileFor('change', FALLBACK);
        expect(profile.facets).toContain(Facet.Changes);
        expect(profile.facets).toContain(Facet.Tests);
        expect(profile.facets).toContain(Facet.Data);
    });

    it('reads the project one step shallower', () => {
        const profile = profileFor('understand', FALLBACK);
        expect(profile.depth).toBe(1);
        expect(profile.facets).toEqual([Facet.Logic, Facet.Calls, Facet.Tests]);
    });

    it('leaves the workbench alone for somebody who named their own start', () => {
        expect(profileFor('entry', FALLBACK)).toBe(FALLBACK);
    });

    it('carries no panel layout, because there is one panel here', () => {
        for (const intent of ['bug', 'change', 'understand'] as const) {
            expect(profileFor(intent, FALLBACK).panels).toEqual([]);
        }
    });
});
