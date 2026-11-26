import { layoutOrganicClusters, type OrganicClusterLayout, type OrganicClusterOptions } from './organic-clusters';
import type { GraphData } from './types';

export interface OrganicLayoutRequest {
    sequence: number;
    data: GraphData;
    options: OrganicClusterOptions;
}
export type OrganicLayoutResponse = { sequence: number; result: OrganicClusterLayout }
    | { sequence: number; error: string };

// A persistent worker owns the layout module's bounded geometry cache.
const context = globalThis as unknown as {
    onmessage: ((event: MessageEvent<OrganicLayoutRequest>) => void) | null;
    postMessage: (response: OrganicLayoutResponse) => void;
};
context.onmessage = ({ data: request }) => {
    try {
        context.postMessage({ sequence: request.sequence, result: layoutOrganicClusters(request.data, request.options) });
    } catch (error: unknown) {
        context.postMessage({ sequence: request.sequence, error: error instanceof Error ? error.message : String(error) });
    }
};
