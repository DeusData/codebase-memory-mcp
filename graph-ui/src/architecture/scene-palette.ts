import type { CSSProperties } from 'react';
import { edgeColor, normalizeEdgeType } from '../graph/edge-style';

/**
 * One colour scheme for every Architecture scene: Overview, Routes (Service
 * map and Endpoints), Hotspots, System structure and Behavior.
 *
 * The base is the green of the IDE (styles/tokens.css: --atlas-bg,
 * --atlas-panel, --atlas-line) and the accent of the Overview
 * (spatial-architecture.css). System structure and Behavior had a blue-grey
 * scheme of their own and read like another product. WebGL needs real colour
 * values, so they live here; the labels, panels and source blocks in the DOM
 * read the same values as CSS variables (scenePaletteStyle on the Architecture
 * root), so there is a single table.
 *
 * Relationship hues (graph/edge-style.ts), language tints and the amber of
 * hotspots and routes are encodings, not scenery, and stay as they are. The
 * one exception is the plain call where it is the only kind of line: in
 * Behavior and in the Service map it takes `call` (journeyEdgeColor,
 * container-layout.ts).
 */
export const SCENE_PALETTE = {
    /** Canvas clear colour and the ground of every scene container. */
    background: '#0b1311',
    gridMajor: '#1e3b31',
    gridMinor: '#13261f',
    /** Folder platforms, directory lanes and component lanes. */
    plate: '#4f7a68',
    plateEdge: '#6c9a86',
    /** The dark body a box colour is mixed into. */
    body: '#24322c',
    /** A box without its own tint: a group, component or operation. */
    node: '#8fc0aa',
    remainder: '#a6a68f',
    /** The plain call where it is the only kind of line: Behavior, and the service links of the Service map. */
    call: '#62d2a2',
    /** Service map: a service built from source, and one that runs a published image. */
    serviceSource: '#79c9b0',
    serviceImage: '#b6a087',
    selected: '#effff8',
    keyLight: '#dcfff3',
    fillLight: '#a899ff',
    /** Map containers and their inspectors. */
    panel: '#0c1613',
    panelRaised: '#0e1a16',
    line: '#27443e',
    text: '#deece7',
    muted: '#91ada7',
    accent: '#6be7ba',
    accentStrong: '#85f0c3',
    accentLine: '#449e7b',
    accentBg: '#17332e',
    /** Chips on the map. */
    label: '#0e1a16ed',
    labelBorder: '#2f4d42',
    labelSelected: '#12291f',
    labelText: '#e1ede7',
    labelMuted: '#9ab5a9',
    labelFocus: '#83e4c3',
    folderLabel: '#94b4a7',
    edgeLabel: '#0d1714f5',
    /** Source excerpts beside a scene. */
    code: '#08110e',
    codeLine: '#6be7ba1f',
} as const;

/**
 * The line colour of a Behavior call. Behavior draws only invocations, so a
 * plain call needs no hue to tell it from imports or usage and takes the
 * green of the scheme; HTTP, async and the other invocation kinds keep their
 * hues. Overview and System structure keep every hue: there calls run beside
 * green usage lines.
 */
export function journeyEdgeColor(type: string, types?: readonly string[]): string {
    const kinds = [...new Set((types?.length ? types : [type]).map(normalizeEdgeType))];
    return kinds.length === 1 && kinds[0] === 'CALLS' ? SCENE_PALETTE.call : edgeColor(type, types);
}

/** The lowest the camera may tilt: a map turned edge-on reads as a flat line. */
export const SCENE_MAX_TILT = Math.PI * 7 / 18;

const variable = (key: string) => `--arch-${key.replace(/[A-Z]/g, letter => `-${letter.toLowerCase()}`)}`;

/** The palette as CSS variables for everything inside the Architecture workspace. */
export function scenePaletteStyle(): CSSProperties {
    return Object.fromEntries(Object.entries(SCENE_PALETTE).map(([key, value]) => [variable(key), value])) as CSSProperties;
}
