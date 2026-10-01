import { describe, expect, it } from 'vitest';
import { PerspectiveCamera, OrthographicCamera, Vector3 } from 'three';
import { projectNodeDisks, separateProjectedNodes } from './projected-node-spacing';
import type { GraphNode } from './types';

const node = (id: number, x: number, y: number, z: number, size = 8): GraphNode =>
    ({ id, x, y, z, size, label: 'Function', name: `node${id}`, color: '#abcdef' });

describe('visible node separation', () => {
    for (const orthographic of [false, true]) it(`separates nodes aligned along the viewing ray (${orthographic ? 'orthographic' : 'perspective'})`, () => {
        const camera = orthographic ? new OrthographicCamera(-150, 150, 150, -150, .1, 2000)
            : new PerspectiveCamera(50, 1, .1, 2000);
        camera.position.set(0, 0, 500); camera.lookAt(0, 0, 0); camera.updateMatrixWorld();
        const nodes = [node(1, 0, 0, 0), node(2, 0, 0, -200), node(3, 1, 1, -70, 15)];
        const before = projectNodeDisks(nodes, camera, 600, 600);
        expect(before[0]!.x).toBe(before[1]!.x);
        const result = separateProjectedNodes(nodes, camera, 600, 600);
        const disks = projectNodeDisks(result, camera, 600, 600);
        for (let a = 0; a < disks.length; a++) for (let b = a + 1; b < disks.length; b++) {
            expect(Math.hypot(disks[a]!.x - disks[b]!.x, disks[a]!.y - disks[b]!.y)
                - disks[a]!.radius - disks[b]!.radius).toBeGreaterThan(4.9);
        }
        expect(result.map(n => n.id)).toEqual(nodes.map(n => n.id));
        for (let i = 0; i < result.length; i++) {
            const originalDepth = new Vector3(nodes[i]!.x, nodes[i]!.y, nodes[i]!.z).applyMatrix4(camera.matrixWorldInverse).z;
            const movedDepth = new Vector3(result[i]!.x, result[i]!.y, result[i]!.z).applyMatrix4(camera.matrixWorldInverse).z;
            expect(movedDepth).toBeCloseTo(originalDepth, 7);
        }
    });
});
