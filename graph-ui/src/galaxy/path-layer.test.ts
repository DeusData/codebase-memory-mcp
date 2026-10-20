// @vitest-environment jsdom
/*
 * Handtest 2026-10-04 (G2): in der Hierarchie lag das Label "DEFINES" eines
 * Pfades (JSONBAgg <-DEFINES- general.py -DEFINES-> __init__) auf der
 * Ueberschrift des Bandes "Mixed directions · 55 nodes", und beide Texte
 * waren nicht mehr zu lesen. Die Labels des Pfades wichen nur den Namen aus,
 * nicht der Ueberschrift. Jetzt weichen alle Kantenlabels der Szene denselben
 * Texten aus, und dazu gehoert die Ueberschrift.
 */
import { afterEach, expect, it } from 'vitest';
import { LABEL_BLOCKER_SELECTOR, screenBlockers } from './PathLayer';
import { placeAlongSegment, type ScreenRect } from './path-frame';

const overlaps = (a: ScreenRect, b: ScreenRect) => !(a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top);
function element(host: HTMLElement, html: string, rect: ScreenRect): HTMLElement {
    host.insertAdjacentHTML('beforeend', html);
    const added = host.lastElementChild as HTMLElement;
    const target = added.querySelector('b') ?? added;
    target.getBoundingClientRect = () => ({ ...rect, x: rect.left, y: rect.top, width: rect.right - rect.left, height: rect.bottom - rect.top, toJSON: () => ({}) });
    return target;
}

afterEach(() => { document.body.innerHTML = ''; });

it('G2: path labels keep clear of the band heading of the hierarchy, as of the path names', () => {
    const host = document.createElement('div'); document.body.append(host);
    // Measured in the browser (CSS px): general.py, __init__ and the band heading in between.
    const heading = { left: 834, right: 1268, top: 652, bottom: 687 };
    element(host, '<span class="atlas-hierarchy-band-label"><strong>Mixed directions · 55 nodes</strong></span>', heading);
    element(host, '<span class="atlas-galaxy-path-node"><i></i><b>general.py</b></span>', { left: 778, right: 838, top: 540, bottom: 557 });
    element(host, '<span class="atlas-galaxy-path-node"><i></i><b>__init__</b></span>', { left: 1135, right: 1185, top: 725, bottom: 742 });
    // Hidden or not laid out: no blocker.
    element(host, '<span class="atlas-galaxy-root-marker"><i></i><b>JSONBAgg</b></span>', { left: 0, right: 0, top: 0, bottom: 0 });
    expect(host.querySelectorAll(LABEL_BLOCKER_SELECTOR)).toHaveLength(4);

    const blockers = screenBlockers(host);
    expect(blockers).toHaveLength(3);
    expect(blockers).toContainEqual(heading);
    const placed = placeAlongSegment({ x: 808, y: 572 }, { x: 1160, y: 757 }, { width: 50, height: 14 }, blockers, []);
    expect(placed.overlap).toBe(0);
    expect(overlaps(placed.rect, heading)).toBe(false);
});
