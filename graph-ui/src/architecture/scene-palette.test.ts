import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { edgeColor } from '../graph/edge-style';
import { layoutContainers } from './container-layout';
import { journeyEdgeColor, SCENE_PALETTE, scenePaletteStyle } from './scene-palette';

const here = dirname(fileURLToPath(import.meta.url));
const read = (name: string) => readFileSync(join(here, name), 'utf8');
/** Every file that draws or styles an Architecture scene: Overview, Routes, Hotspots, System structure, Behavior. */
const SCENE_FILES = ['ArchitectureScene.tsx', 'SystemArchitectureScene.tsx', 'architecture-scene.css', 'spatial-architecture.css',
    'system-architecture.css', 'behavior-journey.css', 'container-map.css'];
/** The blue-grey scheme System structure and Behavior had next to the green Overview (Korrekturplan K19). */
const BLUE_GREY = ['#0b1118', '#1c3440', '#15242e', '#536977', '#6e8a98', '#91a6b4', '#27333e', '#0e1925', '#111923', '#14212e',
    '#101820', '#0b1219', '#080f17', '#0b1525', '#12252d', '#080f14', '#101b23', '#638db3', '#719ec5', '#8ca5bc', '#9bb5ca', '#b6c7d5'];
const rgb = (hex: string) => [1, 3, 5].map(at => parseInt(hex.slice(at, at + 2), 16));
/** Blue the strongest channel: the hue the user called "blau statt grün". */
const bluish = (hex: string) => { const [red, green, blue] = rgb(hex); return blue > green && blue > red; };
/**
 * Blue literals that encode data, not scenery: the kind colours of the
 * Overview (the reference scheme) and the fill light it was lit with.
 */
const ENCODINGS: Record<string, string> = {
    '#8ccff1': 'Overview: file kind', '#c2afff': 'Overview: symbol kind', '#554d70': 'Overview: symbol kind, chip border', '#a899ff': 'fill light of every scene, Overview included',
};

describe('architecture scene palette', () => {
    it('leaves no blue-grey scene colour in any Architecture scene', () => {
        const found = SCENE_FILES.flatMap(name => BLUE_GREY.filter(color => read(name).toLowerCase().includes(color)).map(color => `${name}: ${color}`));
        expect(found).toEqual([]);
    });
    it('draws every scene surface in the green of the IDE', () => {
        for (const key of ['background', 'gridMajor', 'gridMinor', 'plate', 'plateEdge', 'body', 'node', 'panel', 'panelRaised', 'line', 'label', 'labelBorder', 'labelSelected', 'code'] as const) {
            const [red, green, blue] = rgb(SCENE_PALETTE[key]);
            expect(green, key).toBeGreaterThanOrEqual(blue);
            expect(green, key).toBeGreaterThanOrEqual(red);
        }
    });
    it('hands the same values to the DOM labels as CSS variables, so there is one table', () => {
        const style = scenePaletteStyle() as Record<string, string>;
        expect(style['--arch-background']).toBe(SCENE_PALETTE.background);
        expect(style['--arch-label-border']).toBe(SCENE_PALETTE.labelBorder);
        expect(Object.keys(style)).toHaveLength(Object.keys(SCENE_PALETTE).length);
    });
    it('uses the palette in both WebGL scenes and the variables in every scene stylesheet', () => {
        for (const name of ['ArchitectureScene.tsx', 'SystemArchitectureScene.tsx']) expect(read(name), name).toContain('SCENE_PALETTE.background');
        for (const name of ['architecture-scene.css', 'spatial-architecture.css', 'system-architecture.css', 'behavior-journey.css', 'container-map.css']) {
            expect(read(name), name).toContain('var(--arch-');
        }
    });
    it('draws no scenery in blue: every blue literal in a scene file is a named data encoding', () => {
        const found = [...SCENE_FILES, 'container-layout.ts', 'scene-palette.ts'].flatMap(name => [...read(name).matchAll(/#[0-9a-f]{6}(?:[0-9a-f]{2})?\b/gi)]
            .map(match => match[0].slice(0, 7).toLowerCase()).filter(color => bluish(color) && !ENCODINGS[color]).map(color => `${name}: ${color}`));
        expect(found).toEqual([]);
        for (const [key, value] of Object.entries(SCENE_PALETTE)) if (key !== 'fillLight') expect(bluish(value), key).toBe(false);
    });
    it('draws the calls of Behavior and the Service map in the green of the scheme, other relationships in their own hue', () => {
        // Behavior shows only invocations, so its plain call needs no hue to tell it from imports or usage.
        expect(bluish(edgeColor('CALLS'))).toBe(true);
        expect(journeyEdgeColor('CALLS')).toBe(SCENE_PALETTE.call);
        expect(journeyEdgeColor('HTTP_CALLS')).toBe(edgeColor('HTTP_CALLS'));
        const [red, green, blue] = rgb(SCENE_PALETTE.call);
        expect(green).toBeGreaterThan(Math.max(red, blue));
        const service = { project: 'demo', manifest: 'compose.yml', line: 1, networks: [], ports: [] };
        const { graph } = layoutContainers({ services: [{ ...service, id: 'api', name: 'api', sourcePaths: ['api'] }, { ...service, id: 'db', name: 'db', image: 'postgres:16', sourcePaths: [] }],
            connections: [{ id: 'api->db', source: 'api', target: 'db', kind: 'call', protocol: 'postgres', evidence: [] }], warnings: [], unresolved: [] });
        expect(graph.nodes.map(node => node.tint)).toEqual([SCENE_PALETTE.serviceSource, SCENE_PALETTE.serviceImage]);
        expect(graph.edges.map(edge => [edge.type, edge.tint])).toEqual([['SERVICE_CALLS', SCENE_PALETTE.call]]);
        expect(read('SystemArchitectureScene.tsx')).toContain('journeyEdgeColor');
        expect(read('container-map.css')).toContain('.container-call-key { width:18px; height:2px; background:var(--arch-call); }');
    });
});
