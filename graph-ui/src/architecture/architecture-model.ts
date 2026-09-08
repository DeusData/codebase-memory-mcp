import type { ArchitectureBoundary, ArchitectureOverviewDto } from '../core/intelligence-provider';

export const ARCHITECTURE_VIEWS = ['overview', 'dependencies', 'entryPoints', 'routes', 'hotspots'] as const;
export type ArchitectureView = typeof ARCHITECTURE_VIEWS[number];
export interface ArchitectureConfig { view: ArchitectureView; filter: string }
export interface ConfigStorage { getItem(key: string): string | null; setItem(key: string, value: string): void }
export interface BoundaryMap { groups: string[]; boundaries: ArchitectureBoundary[]; omittedGroups: number }
export const DEFAULT_ARCHITECTURE_CONFIG: ArchitectureConfig = { view: 'overview', filter: '' };

function configKey(project: string): string {
    return `atlas.architecture.v1:${project}`;
}

export function readArchitectureConfig(storage: ConfigStorage | undefined, project: string): ArchitectureConfig {
    try {
        const raw = project ? storage?.getItem(configKey(project)) : undefined;
        if (!raw) return { ...DEFAULT_ARCHITECTURE_CONFIG };
        const value: unknown = JSON.parse(raw);
        if (value === null || typeof value !== 'object') return { ...DEFAULT_ARCHITECTURE_CONFIG };
        const candidate = value as Record<string, unknown>;
        if (candidate.version !== 1 || typeof candidate.filter !== 'string'
            || !ARCHITECTURE_VIEWS.includes(candidate.view as ArchitectureView)) {
            return { ...DEFAULT_ARCHITECTURE_CONFIG };
        }
        return { view: candidate.view as ArchitectureView, filter: candidate.filter };
    } catch {
        return { ...DEFAULT_ARCHITECTURE_CONFIG };
    }
}

export function saveArchitectureConfig(storage: ConfigStorage | undefined, project: string, config: ArchitectureConfig): boolean {
    if (!storage || !project) return false;
    try {
        storage.setItem(configKey(project), JSON.stringify({ version: 1, ...config }));
        return true;
    } catch {
        return false;
    }
}

/** Filter the visible lists; project totals and individual findings stay untouched. */
export function matchingArchitecture(overview: ArchitectureOverviewDto, filter: string): ArchitectureOverviewDto {
    const query = filter.trim().toLocaleLowerCase();
    if (!query) return overview;
    const matches = (...values: (string | undefined)[]) => values.some(value => value?.toLocaleLowerCase().includes(query));
    return {
        ...overview,
        groups: overview.groups.filter(group => matches(group.name)),
        boundaries: overview.boundaries.filter(boundary => matches(boundary.from, boundary.to)),
        layers: overview.layers.filter(layer => matches(layer.group, layer.layer, layer.reason)),
        clusters: overview.clusters.filter(cluster => matches(cluster.label, ...cluster.topMembers)),
        entryPoints: overview.entryPoints.filter(entry => matches(entry.name, entry.qualifiedName, entry.filePath)),
        routes: overview.routes.filter(route => matches(route.path, route.method, route.handler, route.filePath)),
        hotspots: overview.hotspots.filter(hotspot => matches(hotspot.name, hotspot.qualifiedName, hotspot.filePath)),
        files: overview.files.filter(file => matches(file)),
    };
}

/** A bounded drawing, backed only by reported group-to-group call edges. */
export function boundaryMap(boundaries: ArchitectureBoundary[], limit = 8): BoundaryMap {
    const weights = new Map<string, number>();
    for (const boundary of boundaries) {
        weights.set(boundary.from, (weights.get(boundary.from) ?? 0) + boundary.callCount);
        weights.set(boundary.to, (weights.get(boundary.to) ?? 0) + boundary.callCount);
    }
    const groups = [...weights.keys()]
        .sort((a, b) => (weights.get(b) ?? 0) - (weights.get(a) ?? 0) || (a < b ? -1 : a > b ? 1 : 0))
        .slice(0, Math.max(0, limit));
    const included = new Set(groups);
    return {
        groups,
        boundaries: boundaries.filter(boundary => included.has(boundary.from) && included.has(boundary.to)),
        omittedGroups: weights.size - groups.length,
    };
}
