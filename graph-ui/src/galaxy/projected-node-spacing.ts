import { Camera, Vector3 } from 'three';
import type { GraphNode } from './types';
import { separateScreenNodes } from './screen-node-spacing';

/** Reserve the visible body plus a glow margin, rather than comparing centers.
 * Both the normal and highlighted sphere render at no more than size / 2. */
export function projectNodeDisks(nodes: readonly GraphNode[], camera: Camera, width: number, height: number) {
    camera.updateMatrixWorld();
    const view = new Vector3(), projected = new Vector3();
    const focal = Math.max(Math.abs(camera.projectionMatrix.elements[0]!) * width,
        Math.abs(camera.projectionMatrix.elements[5]!) * height) / 2;
    const perspective = camera.projectionMatrix.elements[15] === 0;
    return nodes.flatMap(node => {
        view.set(node.x, node.y, node.z).applyMatrix4(camera.matrixWorldInverse);
        projected.set(node.x, node.y, node.z).project(camera);
        if (view.z >= 0 || ![projected.x, projected.y, projected.z].every(Number.isFinite) || projected.z < -1 || projected.z > 1) return [];
        const worldRadius = Math.max(0, node.size) * .5;
        const depth = -view.z;
        // Perspective enlarges an off-axis sphere too. This conservative
        // enclosing radius includes its depth, unlike radius / depth alone.
        const denom = Math.max(.001, depth * depth - worldRadius * worldRadius);
        const offAxis = Math.hypot(view.x, view.y);
        const radius = perspective
            ? focal * (worldRadius * Math.sqrt(denom + offAxis * offAxis) / denom
                + worldRadius * worldRadius * offAxis / (depth * denom))
            : focal * worldRadius;
        return [{ id: node.id, x: (projected.x + 1) * width / 2, y: (1 - projected.y) * height / 2,
            z: projected.z, radius: Math.max(.75, radius * 1.35 + 1.5), node }];
    });
}

/** Display-only offsets: index coordinates, identities and relationships stay
 * intact. Keep depth and let the drawing grow outside the viewport if needed.
 * Pinned nodes (a scope root) keep their place; the others move around them. */
export function separateProjectedNodes(nodes: GraphNode[], camera: Camera, width: number, height: number,
    pinned: ReadonlySet<number> = new Set()): GraphNode[] {
    if (width <= 0 || height <= 0 || nodes.length < 2) return nodes;
    const view = new Vector3();
    const perspective = camera.projectionMatrix.elements[15] === 0;
    // A sphere intersecting the eye plane has no finite projected silhouette.
    // Keep its display glyph in front of the camera at extreme zoom; source
    // size and coordinates remain untouched in the graph/layout cache.
    let positioned = perspective ? nodes.map(node => {
        const depth = -view.set(node.x, node.y, node.z).applyMatrix4(camera.matrixWorldInverse).z;
        return depth > 0 && depth <= node.size * .55 ? { ...node, size: depth * .5 } : node;
    }) : nodes;
    for (let pass = 0; pass < 4; pass++) {
        const disks = projectNodeDisks(positioned, camera, width, height);
        const separated = separateScreenNodes(disks, 7, pinned);
        const moved = new Map<number, GraphNode>(), point = new Vector3();
        for (let index = 0; index < separated.length; index++) {
            const disk = separated[index]!, original = disks[index]!;
            if (disk.x === original.x && disk.y === original.y) continue;
            point.set(disk.x / width * 2 - 1, 1 - disk.y / height * 2, disk.z).unproject(camera);
            moved.set(disk.id, { ...disk.node, x: point.x, y: point.y, z: point.z });
        }
        if (!moved.size) break;
        positioned = positioned.map(node => moved.get(node.id) ?? node);
        // Lateral movement can enlarge perspective silhouettes. Verify against
        // the actual reprojected bodies, not only the input circle estimates.
        const actual = projectNodeDisks(positioned, camera, width, height);
        if (separateScreenNodes(actual, 5, pinned).every((disk, index) => disk === actual[index])) break;
    }
    return positioned;
}
