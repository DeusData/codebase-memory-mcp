/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-core/src/common/presentation-profile.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * DepthLevel, DEPTH_NAMES, Facet, FACET_ORDER, TerminologyLevel,
 * NavigatorProfileId, PresentationProfile, PresentationOverrides,
 * hasOverrides, normalizeOverrides, ResolvedPresentation, BUILTIN_PROFILES
 * und resolvePresentation.
 *
 * Zwei Abweichungen, beide genannt statt versteckt:
 *
 * 1. `PanelSpec` und das Feld `panels` der Profile beschreiben, wohin Theia
 *    seine Widgets andockt. Diese Oberflaeche hat keine Andockbereiche, also
 *    waere eine Liste von `widgetId`/`area`/`rank` hier eine Behauptung ueber
 *    ein Layout, das es nicht gibt. Die Felder sind mitportiert und tragen die
 *    Werte des Originals, damit ein spaeterer Abgleich der beiden Dateien ein
 *    Diff bleibt und keine Rekonstruktion; gelesen wird davon hier nichts.
 * 2. `clampDepth` stammt nicht aus dieser Datei, sondern aus
 *    theia-extensions/codeatlas-views/src/browser/twin/twin-toolbar.tsx
 *    (Zeile 90). Es steht hier, weil es zum Wertebereich von DepthLevel gehoert
 *    und nicht zur Toolbar: der Regler dieses Projekts ist ein anderes Stueck
 *    JSX, und die Klemmregel darf davon nicht abhaengen. Der Rest von
 *    twin-toolbar.tsx ist bewusst nicht mitgekommen.
 *
 * Dritte Abweichung, seit W13 und auf Nutzerauftrag: die Leiter hat FUENF
 * Rasten statt vier, und sie fragt etwas anderes. Das Original regelt die
 * Menge (narrative, guided, technical, dense); hier steht der Leser
 * (vibe coder, junior, medior, senior, architect). Ein Abgleich der beiden
 * Dateien bleibt trotzdem ein Diff und keine Rekonstruktion: die zweite
 * Raste des Originals liegt unveraendert auf der dritten von hier (medior ist
 * technical, Wort fuer Wort), und die zwei neuen Rasten stehen daneben, statt
 * eine der alten zu ersetzen.
 */

/**
 * Presentation profiles: how much to show and which lenses to show it through.
 *
 * A profile is an authored preset. What the UI consumes is the resolved result
 * of profile plus user overrides. The resolved value deliberately carries no
 * profile identity, so no widget can branch on "which preset am I in" and
 * quietly reintroduce mode-specific behaviour.
 */

/**
 * Which reader the panel is speaking to. 0 vibe coder to 4 architect.
 *
 * The scale is still called a depth because every surface that reads it calls
 * it that, and renaming the type would have been a rename of forty call sites
 * for no gain. What it means changed: it is not an amount of text but the
 * question the body answers, and the five names below are the answer to "for
 * whom".
 */
export type DepthLevel = 0 | 1 | 2 | 3 | 4;

/**
 * Human labels for DepthLevel, indexed by the level itself.
 *
 * The reader's own words, chosen by the person this product is built for and
 * kept in their spelling. Lower case throughout, because these are the names of
 * people and not the names of settings.
 */
export const DEPTH_NAMES = ['vibe coder', 'junior', 'medior', 'senior', 'architect'] as const;

/** The highest reader on the ladder. Everything that clamps clamps to this. */
export const MAX_DEPTH = 4;

/** The lenses a surface can render. A facet is either on or off; there is no ordering. */
export enum Facet {
    Logic = 'logic',
    Calls = 'calls',
    Data = 'data',
    Errors = 'errors',
    Tests = 'tests',
    Runtime = 'runtime',
    Changes = 'changes'
}

/**
 * The order facets are offered in, everywhere they are offered.
 *
 * A fixed order is not decoration. The facet chips are a row of toggles the
 * reader learns by position, and a set iteration order that depends on which
 * preset happens to be active would move them under the reader's hand.
 */
export const FACET_ORDER: readonly Facet[] = [
    Facet.Logic,
    Facet.Calls,
    Facet.Data,
    Facet.Errors,
    Facet.Tests,
    Facet.Runtime,
    Facet.Changes
];

/** Whether to name things the way the domain does or the way the code does. */
export type TerminologyLevel = 'plain' | 'technical';

export type NavigatorProfileId =
    | 'learning'
    | 'verification'
    | 'understanding'
    | 'debug-impact'
    | 'architecture';

/** Where a widget sits when a profile is applied. */
export interface PanelSpec {
    widgetId: string;
    area: 'left' | 'right' | 'bottom' | 'main';
    /** Lower ranks sit closer to the top of their area. */
    rank: number;
    /** Whether applying the profile opens the widget or only registers its slot. */
    open: boolean;
}

/** An authored preset. Immutable: overrides produce a new resolved value, not a mutation. */
export interface PresentationProfile {
    readonly id: NavigatorProfileId | `custom:${string}`;
    readonly label: string;
    readonly depth: DepthLevel;
    readonly facets: readonly Facet[];
    readonly terminology?: TerminologyLevel;
    /** Show short concept explanations next to unfamiliar constructs. */
    readonly conceptCallouts: boolean;
    readonly panels: readonly PanelSpec[];
    /** Watch for AI-authored edits and surface them for review. */
    readonly aiChangeWatcher: boolean;
}

/** User adjustments layered on top of a profile. */
export interface PresentationOverrides {
    depth?: DepthLevel;
    facetsAdded?: Facet[];
    facetsRemoved?: Facet[];
}

/**
 * Anything the input can produce, clamped onto the five detents.
 *
 * From `twin-toolbar.tsx` in the reference; the ceiling is the only thing that
 * moved, and it moved with the ladder.
 */
export function clampDepth(value: number): DepthLevel {
    const rounded = Math.round(value);
    if (!Number.isFinite(rounded) || rounded <= 0) {
        return 0;
    }
    return (rounded >= MAX_DEPTH ? MAX_DEPTH : rounded) as DepthLevel;
}

/**
 * Whether an overlay says anything at all.
 *
 * Used to decide whether a surface should tell the reader that what they are
 * looking at is their own adjustment rather than the preset as authored. An
 * overlay that has been normalised back to nothing is not a modification, which
 * is why turning a facet off and on again clears the marker instead of leaving
 * it lit for the rest of the session.
 */
export function hasOverrides(overrides: PresentationOverrides | undefined): boolean {
    if (!overrides) {
        return false;
    }
    return overrides.depth !== undefined
        || (overrides.facetsAdded?.length ?? 0) > 0
        || (overrides.facetsRemoved?.length ?? 0) > 0;
}

/**
 * Drop everything in an overlay that the profile already says.
 *
 * Keeping "add Errors" in the overlay of a profile that already has Errors
 * would be harmless to render and wrong to report: the reader would be told
 * they had changed something they had only toggled back.
 */
export function normalizeOverrides(
    profile: PresentationProfile,
    overrides: PresentationOverrides
): PresentationOverrides {
    const authored = new Set<Facet>(profile.facets);
    const normalized: PresentationOverrides = {};
    if (overrides.depth !== undefined && overrides.depth !== profile.depth) {
        normalized.depth = overrides.depth;
    }
    const added = (overrides.facetsAdded ?? []).filter(facet => !authored.has(facet));
    if (added.length > 0) {
        normalized.facetsAdded = added;
    }
    const removed = (overrides.facetsRemoved ?? []).filter(facet => authored.has(facet));
    if (removed.length > 0) {
        normalized.facetsRemoved = removed;
    }
    return normalized;
}

/**
 * What widgets actually read. Carries no profile id by design: rendering must
 * depend only on depth, facets, terminology and callouts.
 */
export interface ResolvedPresentation {
    readonly depth: DepthLevel;
    readonly facets: ReadonlySet<Facet>;
    readonly terminology: TerminologyLevel;
    readonly conceptCallouts: boolean;
}

const TWIN = 'codeatlas.twin';
const CHECKLIST = 'codeatlas.checklist';
const NAVIGATOR = 'codeatlas.navigator';
const MAP = 'codeatlas.map';
const IMPACT = 'codeatlas.impact';

/**
 * The five presets, and the layout each of them means.
 *
 * The panel lists are not decoration around the depth and the facets: they are
 * half of what a profile *is*. A reader who says they are here to fix a bug and
 * a reader who says they are here to learn the codebase should not be looking at
 * the same four panels with a different slider position, and until the panels
 * moved with the preset they were.
 *
 * Two conventions run through all five.
 *
 * **Rank one is what the profile leads with.** Several of these panels dock to
 * the same side, which makes them tabs of one stack where only one is visible,
 * so the lowest rank in an area is a statement about which question this profile
 * thinks the reader is asking first. Debugging leads with the calls, verifying
 * leads with the checklist, architecture leads with the map.
 *
 * **A widget keeps its side across every profile.** The navigator is beside the
 * twin in all five, not on the left in three of them, because a preset that
 * threw a panel across the window on every switch would make choosing a mode
 * feel like opening a different application.
 */
export const BUILTIN_PROFILES: readonly PresentationProfile[] = [
    {
        id: 'learning',
        label: 'Learning',
        depth: 0,
        facets: [Facet.Logic, Facet.Errors],
        terminology: 'plain',
        conceptCallouts: true,
        panels: [
            { widgetId: TWIN, area: 'right', rank: 100, open: true },
            { widgetId: CHECKLIST, area: 'right', rank: 200, open: true },
            { widgetId: NAVIGATOR, area: 'right', rank: 300, open: true }
        ],
        aiChangeWatcher: false
    },
    {
        id: 'verification',
        label: 'Verification',
        depth: 2,
        facets: [Facet.Changes, Facet.Data, Facet.Tests, Facet.Errors],
        terminology: 'technical',
        conceptCallouts: false,
        panels: [
            { widgetId: CHECKLIST, area: 'right', rank: 100, open: true },
            { widgetId: TWIN, area: 'right', rank: 200, open: true },
            { widgetId: IMPACT, area: 'bottom', rank: 300, open: true }
        ],
        aiChangeWatcher: true
    },
    {
        id: 'understanding',
        label: 'Understanding',
        depth: 1,
        facets: [Facet.Logic, Facet.Calls, Facet.Tests],
        terminology: 'plain',
        conceptCallouts: true,
        panels: [
            { widgetId: TWIN, area: 'right', rank: 100, open: true },
            { widgetId: CHECKLIST, area: 'right', rank: 200, open: true },
            { widgetId: NAVIGATOR, area: 'right', rank: 300, open: true },
            { widgetId: MAP, area: 'main', rank: 400, open: false }
        ],
        aiChangeWatcher: false
    },
    {
        id: 'debug-impact',
        label: 'Debug and impact',
        depth: 2,
        facets: [Facet.Calls, Facet.Errors, Facet.Runtime, Facet.Changes],
        terminology: 'technical',
        conceptCallouts: false,
        panels: [
            { widgetId: NAVIGATOR, area: 'right', rank: 100, open: true },
            { widgetId: TWIN, area: 'right', rank: 200, open: true },
            { widgetId: IMPACT, area: 'bottom', rank: 300, open: true },
            { widgetId: MAP, area: 'main', rank: 400, open: false }
        ],
        aiChangeWatcher: false
    },
    {
        id: 'architecture',
        // Moved from 3 to 4 with the W13 ladder, and it is the same statement it
        // always was: this preset is the one whose reader is the architect, and
        // the architect is now a name on the scale rather than a position on it.
        label: 'Architecture',
        depth: 4,
        facets: [Facet.Calls, Facet.Data, Facet.Changes],
        terminology: 'technical',
        conceptCallouts: false,
        panels: [
            { widgetId: MAP, area: 'main', rank: 100, open: true },
            { widgetId: TWIN, area: 'right', rank: 200, open: true },
            { widgetId: NAVIGATOR, area: 'right', rank: 300, open: true },
            { widgetId: IMPACT, area: 'bottom', rank: 400, open: false }
        ],
        aiChangeWatcher: false
    }
];

/**
 * Apply overrides to a profile. Removals win over additions, so a user who
 * turns a facet off keeps it off even if the same facet is also added.
 */
export function resolvePresentation(
    profile: PresentationProfile,
    overrides?: PresentationOverrides
): ResolvedPresentation {
    const depth = overrides?.depth ?? profile.depth;
    const facets = new Set<Facet>(profile.facets);
    for (const facet of overrides?.facetsAdded ?? []) {
        facets.add(facet);
    }
    for (const facet of overrides?.facetsRemoved ?? []) {
        facets.delete(facet);
    }
    return {
        depth,
        facets,
        terminology: profile.terminology ?? (depth <= 1 ? 'plain' : 'technical'),
        conceptCallouts: profile.conceptCallouts
    };
}
