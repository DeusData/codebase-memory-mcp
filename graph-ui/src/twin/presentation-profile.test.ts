/*
 * Die Presentation: was der Regler klemmt, was ein Overlay wegwirft und was
 * aufgeloest herauskommt.
 *
 * Portiert mitsamt den Presets aus CodeAtlasIDE. Die Presets stehen hier nicht
 * als Zierde: sie sind die Startlage des Panels, und ein Preset, das sich
 * unbemerkt verschiebt, verschiebt die Tiefe, mit der ein Leser das Produkt zum
 * ersten Mal sieht.
 */

import { describe, expect, it } from 'vitest';

import {
    BUILTIN_PROFILES,
    DEPTH_NAMES,
    FACET_ORDER,
    Facet,
    clampDepth,
    hasOverrides,
    normalizeOverrides,
    resolvePresentation,
} from './presentation-profile';

const understanding = BUILTIN_PROFILES.find((profile) => profile.id === 'understanding')!;

describe('clampDepth', () => {
    it('rastet auf die fuenf Leser ein', () => {
        expect([0, 1, 2, 3, 4].map(clampDepth)).toEqual([0, 1, 2, 3, 4]);
    });

    it('faengt alles ausserhalb ab, statt einen sechsten Leser zu erfinden', () => {
        expect(clampDepth(-2)).toBe(0);
        expect(clampDepth(9)).toBe(4);
        expect(clampDepth(Number.NaN)).toBe(0);
    });

    it('rundet, weil ein Schieberegler Zwischenwerte liefern kann', () => {
        expect(clampDepth(1.4)).toBe(1);
        expect(clampDepth(1.6)).toBe(2);
    });
});

describe('die Facetten-Reihenfolge', () => {
    it('ist fest, weil der Leser die Schalter nach ihrer Stelle lernt', () => {
        expect(FACET_ORDER).toEqual([
            Facet.Logic,
            Facet.Calls,
            Facet.Data,
            Facet.Errors,
            Facet.Tests,
            Facet.Runtime,
            Facet.Changes,
        ]);
    });

    it('nennt jede Facette genau einmal', () => {
        expect(new Set(FACET_ORDER).size).toBe(FACET_ORDER.length);
        expect(FACET_ORDER).toHaveLength(Object.keys(Facet).length);
    });
});

describe('resolvePresentation', () => {
    it('nimmt Tiefe, Linsen und Terminologie des Profils, wenn niemand etwas aendert', () => {
        const resolved = resolvePresentation(understanding);
        expect(resolved.depth).toBe(1);
        expect(resolved.terminology).toBe('plain');
        expect([...resolved.facets].sort()).toEqual([Facet.Calls, Facet.Logic, Facet.Tests].sort());
    });

    it('laesst Entfernen gegen Hinzufuegen gewinnen', () => {
        const resolved = resolvePresentation(understanding, {
            facetsAdded: [Facet.Data],
            facetsRemoved: [Facet.Data, Facet.Tests],
        });
        expect(resolved.facets.has(Facet.Data)).toBe(false);
        expect(resolved.facets.has(Facet.Tests)).toBe(false);
    });

    it('traegt keine Profil-Identitaet, damit kein Renderer auf das Preset verzweigen kann', () => {
        const resolved = resolvePresentation(understanding, { depth: 3 });
        expect(Object.keys(resolved).sort()).toEqual([
            'conceptCallouts',
            'depth',
            'facets',
            'terminology',
        ]);
    });
});

describe('Overlays', () => {
    it('zaehlen als Aenderung nur, wenn sie etwas sagen', () => {
        expect(hasOverrides(undefined)).toBe(false);
        expect(hasOverrides({})).toBe(false);
        expect(hasOverrides({ depth: 0 })).toBe(true);
    });

    it('werfen weg, was das Profil ohnehin sagt', () => {
        const normalized = normalizeOverrides(understanding, {
            depth: 1,
            facetsAdded: [Facet.Logic],
            facetsRemoved: [Facet.Data],
        });
        expect(normalized).toEqual({});
        expect(hasOverrides(normalized)).toBe(false);
    });

    it('behalten, was das Profil nicht sagt', () => {
        const normalized = normalizeOverrides(understanding, {
            depth: 2,
            facetsAdded: [Facet.Data],
            facetsRemoved: [Facet.Tests],
        });
        expect(normalized).toEqual({ depth: 2, facetsAdded: [Facet.Data], facetsRemoved: [Facet.Tests] });
    });
});

describe('die eingebauten Profile', () => {
    it('sind die fuenf des Referenzprojekts, mit ihren Tiefen', () => {
        expect(BUILTIN_PROFILES.map((profile) => [profile.id, profile.depth])).toEqual([
            ['learning', 0],
            ['verification', 2],
            ['understanding', 1],
            ['debug-impact', 2],
            ['architecture', 4],
        ]);
    });

    it('benennen ihre Tiefe mit dem Namen, den DEPTH_NAMES fuehrt', () => {
        for (const profile of BUILTIN_PROFILES) {
            expect(DEPTH_NAMES[profile.depth]).toBeTypeOf('string');
        }
        // Die Namen des Nutzers, in seiner Schreibung. Sie sind seit W13 die
        // Antwort auf "fuer wen" und nicht mehr auf "wie viel".
        expect(DEPTH_NAMES).toEqual(['vibe coder', 'junior', 'medior', 'senior', 'architect']);
    });

    it('nennen jede Linse aus der festen Reihenfolge', () => {
        for (const profile of BUILTIN_PROFILES) {
            for (const facet of profile.facets) {
                expect(FACET_ORDER).toContain(facet);
            }
        }
    });
});
