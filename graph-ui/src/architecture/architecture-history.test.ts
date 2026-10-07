/*
 * Handtest K27: der Architecture-Eintrag im gemeinsamen Verlaufsmodell
 * (src/graph/navigation-history.ts). Welcher Ort ein Schritt ist, wie er im
 * Tooltip heisst und welcher Ort in der Liste der letzten Orte steht.
 */
import { describe, expect, it } from 'vitest';
import {
    NAVIGATION_HISTORY_LIMIT, emptyNavigationHistory, moveNavigation, peekNavigation, pushNavigation, replaceNavigation,
    type NavigationHistory,
} from '../graph/navigation-history';
import {
    architectureEntryDetail, architectureEntryLabel, architectureEntryName, architectureHistoryOptions, initialArchitecturePlace,
    type ArchitectureHistoryEntry,
} from './architecture-history';
import type { SystemSymbol } from './system-architecture-source';

const start = initialArchitecturePlace('overview');
const at = (change: Partial<ArchitectureHistoryEntry>): ArchitectureHistoryEntry => ({ ...start, ...change });
const key = architectureHistoryOptions.key;
const recentKey = (entry: ArchitectureHistoryEntry) => architectureHistoryOptions.recentKey!(entry);
const symbol = (id: number, name: string): SystemSymbol => ({ id, name, qualified_name: `django.${name}`, label: 'Function', file_path: `django/${name}.py`, component_id: 'django' });
const behavior = (id: number, name: string, extra: Partial<NonNullable<ArchitectureHistoryEntry['system']['behavior']>> = {}) =>
    at({ view: 'behavior', system: { expanded: [], behavior: { project: 'django-demo', id, name, ...extra } } });

describe('the Architecture history entry (K27)', () => {
    it('starts on the saved subtab with nothing opened', () => {
        expect(initialArchitecturePlace('routes')).toEqual({ view: 'routes', spatial: { planar: false }, routes: 'services', filter: '', system: { expanded: [] } });
        expect(initialArchitecturePlace('dependencies').view).toBe('overview');
    });

    it('is a new step for every subtab and for what each subtab opens', () => {
        const views = ['overview', 'entryPoints', 'routes', 'hotspots', 'structure', 'behavior'] as const;
        expect(new Set(views.map(view => key(at({ view }))))).toHaveProperty('size', views.length);
        // Overview: the opened area and file, and Plan or 3D.
        const django = at({ spatial: { planar: false, areaPath: 'django' } });
        expect(key(django)).not.toBe(key(start));
        expect(key(at({ spatial: { planar: false, areaPath: 'django', filePath: 'django/shortcuts.py' } }))).not.toBe(key(django));
        expect(key(at({ spatial: { planar: true, areaPath: 'django' } }))).not.toBe(key(django));
        // Routes: the perspective and the filter an opened route group sets.
        const routes = at({ view: 'routes' });
        expect(key({ ...routes, routes: 'endpoints' })).not.toBe(key(routes));
        expect(key({ ...routes, routes: 'endpoints', filter: '/edit' })).not.toBe(key({ ...routes, routes: 'endpoints' }));
        // System structure: the focus and the expanded groups, in any order.
        const structure = at({ view: 'structure' });
        const focused = { ...structure, system: { expanded: [], focus: { id: 'g:django', label: 'django' } } };
        expect(key(focused)).not.toBe(key(structure));
        expect(key({ ...structure, system: { expanded: ['g:a', 'g:b'] } })).toBe(key({ ...structure, system: { expanded: ['g:b', 'g:a'] } }));
        expect(key({ ...structure, system: { expanded: ['g:a'] } })).not.toBe(key(structure));
        // Behavior: the start and the destination ("Reach").
        expect(key(behavior(7, 'handle'))).not.toBe(key(behavior(8, 'get_autocommit')));
        expect(key(behavior(7, 'handle', { targetId: 9, targetName: 'save' }))).not.toBe(key(behavior(7, 'handle')));
    });

    it('merges what the screen does not show: other subtabs, names and where a hop came from', () => {
        // What another subtab holds does not split an Overview step.
        expect(key(at({ filter: '/edit' }))).toBe(key(start));
        expect(key(at({ routes: 'endpoints' }))).toBe(key(start));
        expect(key(at({ system: { expanded: ['g:a'], focus: { id: 'g:a', label: 'a' } } }))).toBe(key(start));
        expect(key(at({ view: 'structure', spatial: { planar: true, areaPath: 'django' } }))).toBe(key(at({ view: 'structure' })));
        // Names are for the tooltip; identity is the numeric start of this analysis.
        expect(key(behavior(7, 'handle'))).toBe(key(behavior(7, 'renamed', { from: symbol(3, 'main'), generation: 'g2' })));
        // A Service map does not draw Plan or 3D.
        expect(key(at({ view: 'routes', spatial: { planar: true } }))).toBe(key(at({ view: 'routes' })));
        expect(key(at({ view: 'routes', routes: 'endpoints', spatial: { planar: true } }))).not.toBe(key(at({ view: 'routes', routes: 'endpoints' })));
    });

    it('names the target of Back and Forward', () => {
        expect(architectureEntryLabel(start)).toBe('Overview');
        expect(architectureEntryLabel(at({ spatial: { planar: false, areaPath: 'django' } }))).toBe('Overview · django');
        expect(architectureEntryLabel(at({ spatial: { planar: true, areaPath: 'django', filePath: 'django/shortcuts.py' } }))).toBe('Overview · django/shortcuts.py · Plan');
        expect(architectureEntryLabel(at({ view: 'entryPoints' }))).toBe('Overview · Entry points');
        expect(architectureEntryLabel(at({ view: 'routes' }))).toBe('Routes · Service map');
        expect(architectureEntryLabel(at({ view: 'routes', routes: 'endpoints', filter: '/edit' }))).toBe('Routes · Endpoints · /edit');
        expect(architectureEntryLabel(at({ view: 'hotspots', spatial: { planar: false, hotspotArea: 'django/db' } }))).toBe('Hotspots · django/db');
        expect(architectureEntryLabel(at({ view: 'structure', system: { expanded: ['g:django'], focus: { id: 'g:django', label: 'django' } } })))
            .toBe('System structure · django · 1 group open');
        expect(architectureEntryLabel(at({ view: 'structure', system: { expanded: ['g:a', 'g:b'] } }))).toBe('System structure · 2 groups open');
        expect(architectureEntryLabel(at({ view: 'behavior' }))).toBe('Behavior');
        expect(architectureEntryLabel(behavior(7, 'handle', { targetId: 9, targetName: 'save' }))).toBe('Behavior · handle → save');
        expect(architectureEntryLabel(behavior(8, 'get_autocommit', { from: symbol(7, 'handle') }))).toBe('Behavior · get_autocommit · followed from handle');
        // Without a requested start the tooltip names the start the journey showed.
        expect(architectureEntryLabel(at({ view: 'behavior', system: { expanded: [], shown: 'main', behavior: { project: 'django-demo' } } }))).toBe('Behavior · main');
        expect(architectureEntryLabel(at({ view: 'behavior', system: { expanded: [], shown: 'main', behavior: { project: 'django-demo', id: 7, name: 'handle' } } }))).toBe('Behavior · handle');
        expect(key(at({ view: 'behavior', system: { expanded: [], shown: 'main' } }))).toBe(key(at({ view: 'behavior' })));
        expect(architectureEntryName(behavior(7, 'handle'))).toBe('Behavior');
        expect(architectureEntryDetail(behavior(7, 'handle'))).toBe('handle');
        expect(architectureEntryDetail(start)).toBe('');
    });

    it('holds Plan or 3D in System structure and Behavior, and where a call chain stands (K27)', () => {
        const structure = at({ view: 'structure' });
        // Plan or 3D is a step in every view that draws a scene, each with its own camera as before.
        expect(key({ ...structure, system: { expanded: [], structurePlanar: true } })).not.toBe(key(structure));
        expect(key({ ...structure, system: { expanded: [], behaviorPlanar: true } })).toBe(key(structure));
        const reach = behavior(1, 'main', { targetId: 3, targetName: 'save' });
        expect(key({ ...reach, system: { ...reach.system, behaviorPlanar: true } })).not.toBe(key(reach));
        expect(key({ ...reach, system: { ...reach.system, structurePlanar: true } })).toBe(key(reach));
        // Another indexed path to the destination is a step; the operation on it and the page of direct calls are not.
        const second = { ...reach, system: { ...reach.system, behavior: { ...reach.system.behavior!, position: { path: 1 } } } };
        expect(key(second)).not.toBe(key(reach));
        expect(key({ ...reach, system: { ...reach.system, behavior: { ...reach.system.behavior!, position: { path: 0, step: 2, page: 1 } } } })).toBe(key(reach));
        expect(recentKey(second)).toBe(recentKey(reach));
        expect(architectureEntryLabel({ ...structure, system: { expanded: [], structurePlanar: true } })).toBe('System structure · Plan');
        expect(architectureEntryLabel({ ...second, system: { ...second.system, behaviorPlanar: true } })).toBe('Behavior · main → save · Path 2 · Plan');
        expect(architectureEntryLabel({ ...reach, system: { ...reach.system, behavior: { ...reach.system.behavior!, position: { step: 2 } } } })).toBe('Behavior · main → save');
    });

    it('keeps one recent place per subtab location, whatever its Plan or expanded groups', () => {
        expect(recentKey(at({ spatial: { planar: true } }))).toBe(recentKey(start));
        expect(recentKey(at({ spatial: { planar: false, areaPath: 'django' } }))).not.toBe(recentKey(start));
        const structure = at({ view: 'structure', system: { expanded: [], focus: { id: 'g:django', label: 'django' } } });
        expect(recentKey({ ...structure, system: { ...structure.system, expanded: ['g:django'] } })).toBe(recentKey(structure));
        expect(recentKey(behavior(7, 'handle'))).not.toBe(recentKey(behavior(8, 'get_autocommit')));
    });

    it('runs on the one shared model: bounded, merged, forward branch dropped, recent places', () => {
        const options = architectureHistoryOptions;
        const push = (history: NavigationHistory<ArchitectureHistoryEntry>, ...entries: ArchitectureHistoryEntry[]) =>
            entries.reduce((next, entry) => pushNavigation(next, entry, options), history);
        const django = at({ spatial: { planar: false, areaPath: 'django' } });
        let history = push(emptyNavigationHistory<ArchitectureHistoryEntry>(), start, django, django, at({ view: 'routes' }), at({ view: 'routes', filter: '' }));
        expect(history.entries.map(architectureEntryLabel)).toEqual(['Overview', 'Overview · django', 'Routes · Service map']);
        history = moveNavigation(moveNavigation(history, -1, options), -1, options);
        history = push(history, at({ view: 'structure' }));
        expect(history.entries.map(architectureEntryLabel)).toEqual(['Overview', 'System structure']);
        expect(peekNavigation(history, 1)).toBeUndefined();
        const many = Array.from({ length: NAVIGATION_HISTORY_LIMIT + 10 }, (_, index) => behavior(index + 1, `operation${index + 1}`));
        history = push(history, ...many);
        expect(history.entries).toHaveLength(NAVIGATION_HISTORY_LIMIT);
        expect(history.recent).toHaveLength(8);
        expect(architectureEntryLabel(history.recent[0]!)).toBe(`Behavior · operation${NAVIGATION_HISTORY_LIMIT + 10}`);
    });

    it('lets an automatic pick replace the step it completes instead of adding one', () => {
        const options = architectureHistoryOptions;
        const empty = at({ view: 'behavior' });
        let history = pushNavigation(pushNavigation(emptyNavigationHistory<ArchitectureHistoryEntry>(), start, options), empty, options);
        history = replaceNavigation(history, behavior(1, 'main'), options);
        expect(history.entries.map(architectureEntryLabel)).toEqual(['Overview', 'Behavior · main']);
        expect(history.recent.map(architectureEntryLabel)).toEqual(['Behavior · main', 'Overview']);
    });
});
