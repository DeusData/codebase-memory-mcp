/**
 * Die Einstellungen, die Rechenzeit kosten: was gespeichert wird, was aus einem
 * unlesbaren Speicher wird, und wie sie sich auf die Vorgabe einer Ansicht
 * legen.
 *
 * Die drei Fragen haben je einen Fehler, gegen den sie geschnitten sind:
 *
 *  - Ein Speicher, dem man traut, macht aus einem alten oder fremden Eintrag
 *    eine Szene, die niemand eingestellt hat.
 *  - Eine Wahl, die die Vorgabe der Ansicht ERSETZT statt sich darauf zu legen,
 *    holt das Leuchten in die Hierarchie zurueck, wo es als Befund weggenommen
 *    wurde (Nutzerfeedback 2026-08-29, Screenshots).
 *  - Ein Sparprofil, das sich nicht vom Vorgabeprofil unterscheidet, waere ein
 *    Knopf ohne Wirkung.
 */

import { describe, expect, it } from 'vitest';

import {
    DEFAULT_DISPLAY_SETTINGS,
    DEFAULT_GRAPH_DISPLAY,
    EDGE_DENSITY_BRIGHTNESS,
    FRAME_CAPS,
    LABEL_DISTANCE_FACTORS,
    THRIFTY_GRAPH_DISPLAY,
    clampDisplaySettings,
    displayKey,
    displayWith,
    isDefaultDisplay,
    loadDisplaySettings,
    saveDisplaySettings,
} from './density';
import type { DisplayStore, GraphDisplaySettings } from './density';

function memoryStore(seed: Record<string, string> = {}): DisplayStore {
    const map = new Map<string, string>(Object.entries(seed));
    return {
        getItem: (key) => map.get(key) ?? null,
        setItem: (key, value) => {
            map.set(key, value);
        },
    };
}

describe('die Vorgabe und das Sparprofil', () => {
    it('ist die Vorgabe genau das Bild von vor dem Menue', () => {
        expect(DEFAULT_GRAPH_DISPLAY).toEqual({
            projection: 'spatial',
            halos: true,
            bloom: true,
            edges: 'full',
            labelDistanceFactor: 0,
            frameCap: 0,
            agents: true,
            agentTails: true,
            agentTrails: true,
            agentWaves: true,
            agentTimeline: true,
        });
        expect(isDefaultDisplay(DEFAULT_GRAPH_DISPLAY)).toBe(true);
    });

    it('unterscheidet sich das Sparprofil in jedem einzelnen Schalter', () => {
        for (const key of Object.keys(DEFAULT_GRAPH_DISPLAY) as (keyof GraphDisplaySettings)[]) {
            expect(THRIFTY_GRAPH_DISPLAY[key], `${key} ist im Sparprofil dasselbe`)
                .not.toBe(DEFAULT_GRAPH_DISPLAY[key]);
        }
        expect(isDefaultDisplay(THRIFTY_GRAPH_DISPLAY)).toBe(false);
    });

    it('sieht einen einzelnen umgelegten Schalter', () => {
        expect(isDefaultDisplay({ ...DEFAULT_GRAPH_DISPLAY, halos: false })).toBe(false);
        expect(isDefaultDisplay({ ...DEFAULT_GRAPH_DISPLAY, frameCap: 30 })).toBe(false);
    });

    it('bietet nur Werte an, die das Modul auch wieder annimmt', () => {
        for (const cap of FRAME_CAPS) {
            expect(clampDisplaySettings({ ...DEFAULT_GRAPH_DISPLAY, frameCap: cap }).frameCap).toBe(cap);
        }
        for (const factor of LABEL_DISTANCE_FACTORS) {
            expect(
                clampDisplaySettings({ ...DEFAULT_GRAPH_DISPLAY, labelDistanceFactor: factor })
                    .labelDistanceFactor,
            ).toBe(factor);
        }
    });
});

describe('was aus dem Speicher kommt', () => {
    it('liegt unter einem Schluessel, der das Projekt nennt', () => {
        expect(displayKey('atlas-sample')).toBe('atlas-display:atlas-sample');
    });

    it('ist beim Erststart die Vorgabe', () => {
        expect(loadDisplaySettings(memoryStore(), 'p')).toEqual(DEFAULT_GRAPH_DISPLAY);
    });

    it('ueberlebt das Schreiben und Nachlesen', () => {
        const store = memoryStore();
        saveDisplaySettings(store, 'p', THRIFTY_GRAPH_DISPLAY);
        expect(loadDisplaySettings(store, 'p')).toEqual(THRIFTY_GRAPH_DISPLAY);
    });

    it('haelt zwei Projekte auseinander', () => {
        const store = memoryStore();
        saveDisplaySettings(store, 'eins', THRIFTY_GRAPH_DISPLAY);
        expect(loadDisplaySettings(store, 'zwei')).toEqual(DEFAULT_GRAPH_DISPLAY);
    });

    it('faellt bei unlesbarem JSON auf die Vorgabe zurueck', () => {
        expect(loadDisplaySettings(memoryStore({ 'atlas-display:p': '{kaputt' }), 'p'))
            .toEqual(DEFAULT_GRAPH_DISPLAY);
    });

    it('faellt bei einem Speicher, der wirft, auf die Vorgabe zurueck', () => {
        const angry: DisplayStore = {
            getItem: () => {
                throw new Error('site data off');
            },
            setItem: () => {
                throw new Error('site data off');
            },
        };
        expect(loadDisplaySettings(angry, 'p')).toEqual(DEFAULT_GRAPH_DISPLAY);
        // Und das Schreiben scheitert lautlos: der Preis ist ein Leser, der die
        // Einstellung noch einmal umlegt, und nicht ein Schalter, der klemmt.
        expect(() => saveDisplaySettings(angry, 'p', THRIFTY_GRAPH_DISPLAY)).not.toThrow();
    });

    it('nimmt ohne Projekt nichts an und merkt nichts', () => {
        const store = memoryStore();
        expect(loadDisplaySettings(store, '')).toEqual(DEFAULT_GRAPH_DISPLAY);
        saveDisplaySettings(store, '', THRIFTY_GRAPH_DISPLAY);
        expect(store.getItem('atlas-display:')).toBeNull();
    });

    it('stutzt einen unbekannten Wert auf die Vorgabe und nicht auf den naechsten', () => {
        const clamped = clampDisplaySettings({
            projection: 'isometric',
            halos: 'ja',
            bloom: 1,
            edges: 'medium',
            labelDistanceFactor: 7,
            frameCap: 144,
        });
        expect(clamped).toEqual(DEFAULT_GRAPH_DISPLAY);
    });

    it('behaelt jeden Wert, den es kennt, auch neben einem, den es nicht kennt', () => {
        const clamped = clampDisplaySettings({ projection: 'flat', frameCap: 30, edges: 'was' });
        expect(clamped.projection).toBe('flat');
        expect(clamped.frameCap).toBe(30);
        expect(clamped.edges).toBe('full');
    });
});

describe('die Wahl legt sich auf die Vorgabe der Ansicht', () => {
    it('laesst die Vorgabe unveraendert, wenn nichts umgelegt ist', () => {
        expect(displayWith(DEFAULT_DISPLAY_SETTINGS, DEFAULT_GRAPH_DISPLAY))
            .toEqual(DEFAULT_DISPLAY_SETTINGS);
    });

    it('multipliziert die Kantenhelligkeit statt sie zu setzen', () => {
        const dimmed = displayWith(DEFAULT_DISPLAY_SETTINGS, {
            ...DEFAULT_GRAPH_DISPLAY,
            edges: 'dim',
        });
        expect(dimmed.edgeBrightness).toBe(EDGE_DENSITY_BRIGHTNESS.dim);
    });

    it('holt ein eingeschaltetes Bloom NICHT in eine Ansicht zurueck, die keins hat', () => {
        // Genau der Befund, gegen den `displayWith` multiplikativ rechnet: die
        // Hierarchie setzt bloom auf 0, weil das Leuchten dort aus sechzig
        // Namen Flecken macht.
        const hierarchy = { ...DEFAULT_DISPLAY_SETTINGS, bloom: 0, nodeGlow: 0.5 };
        expect(displayWith(hierarchy, DEFAULT_GRAPH_DISPLAY).bloom).toBe(0);
        expect(displayWith(hierarchy, DEFAULT_GRAPH_DISPLAY).nodeGlow).toBe(0.5);
    });

    it('nimmt ein ausgeschaltetes Bloom ueberall weg', () => {
        expect(displayWith(DEFAULT_DISPLAY_SETTINGS, { ...DEFAULT_GRAPH_DISPLAY, bloom: false }).bloom)
            .toBe(0);
    });
});
