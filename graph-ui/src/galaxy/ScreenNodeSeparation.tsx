import { useEffect, useRef } from 'react';
import { useFrame, useThree } from '@react-three/fiber';
import { Matrix4 } from 'three';
import type { GraphNode } from './types';
import { separateProjectedNodes } from './projected-node-spacing';

/** Arrange a new graph or viewport after its camera fit settles. Once committed,
 * positions stay fixed while zooming, panning or orbiting the same graph.
 * Pinned nodes are never moved, so a marked scope root stays where it is. */
export function ScreenNodeSeparation({ nodes, active, onChange, onBusyChange, pinned }: {
    nodes: GraphNode[];
    active: boolean;
    pinned?: ReadonlySet<number>;
    onChange: (source: GraphNode[], positioned: GraphNode[]) => void;
    onBusyChange?: (busy: boolean) => void;
}) {
    const { camera, size, invalidate } = useThree();
    const state = useRef({ matrix: new Matrix4(), projection: new Matrix4(), source: undefined as GraphNode[] | undefined,
        width: 0, height: 0, changedAt: 0, dirty: true, queued: 0, busy: false });
    const callbacks = useRef({ onChange, onBusyChange, pinned });
    callbacks.current = { onChange, onBusyChange, pinned };
    useEffect(() => () => {
        cancelAnimationFrame(state.current.queued);
        callbacks.current.onBusyChange?.(false);
    }, []);
    useFrame(() => {
        if (!active) return;
        const current = state.current;
        const layoutChanged = current.source !== nodes || current.width !== size.width || current.height !== size.height;
        // Navigation only changes the camera; it must not rearrange the graph
        // or restart the loading indicator after a layout has been committed.
        if (!layoutChanged && !current.dirty) return;
        camera.updateMatrixWorld();
        const cameraChanged = current.matrix.elements.some((value, index) => Math.abs(value - camera.matrixWorld.elements[index]!) > .00001)
            || !current.projection.equals(camera.projectionMatrix);
        if (layoutChanged || cameraChanged) {
            current.matrix.copy(camera.matrixWorld); current.projection.copy(camera.projectionMatrix);
            current.source = nodes; current.width = size.width; current.height = size.height;
            current.changedAt = performance.now(); current.dirty = true;
            cancelAnimationFrame(current.queued); current.queued = 0;
            if (!current.busy) { current.busy = true; callbacks.current.onBusyChange?.(true); }
        }
        if (!current.dirty || current.queued || performance.now() - current.changedAt < 100) return;
        // Yield a paint before computing so delayed render feedback can appear.
        current.queued = requestAnimationFrame(() => {
            current.queued = requestAnimationFrame(() => {
                current.queued = 0;
                callbacks.current.onChange(nodes, separateProjectedNodes(nodes, camera, size.width, size.height, callbacks.current.pinned));
                current.dirty = false; current.busy = false;
                callbacks.current.onBusyChange?.(false);
                invalidate();
            });
        });
    });
    return null;
}
