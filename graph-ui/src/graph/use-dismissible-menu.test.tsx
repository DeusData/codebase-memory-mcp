// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { useDismissibleMenu } from './use-dismissible-menu';

/*
 * Hand test 2026-10-04 (A2): the Recent menus of Galaxy and Architecture stayed open over the content
 * through several steps and while scrolling. Both use this hook.
 */
let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });

function Fixture({ place, onPick }: { place: string; onPick: () => void }) {
    const menu = useDismissibleMenu(place);
    return <div>
        <details ref={menu.ref}><summary>Recent</summary>
            <ul><li><button type="button" onClick={() => { menu.close(); onPick(); }}>Overview</button></li></ul></details>
        <p>Content</p><input aria-label="Elsewhere" />
    </div>;
}
async function setup(place = 'a') {
    const pick = vi.fn();
    await act(async () => root.render(<Fixture place={place} onPick={pick} />));
    const details = host.querySelector('details')!;
    await act(async () => { details.open = true; });
    return { pick, details };
}
const fire = (target: EventTarget, type: string) => act(async () => { target.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true })); });

it('closes on a pointer press outside the menu and stays open for one inside it', async () => {
    const { details } = await setup();
    await fire(details.querySelector('summary')!, 'pointerdown');
    await fire(details.querySelector('li button')!, 'pointerdown');
    expect(details.open).toBe(true);
    await fire(host.querySelector('p')!, 'pointerdown');
    expect(details.open).toBe(false);
});

it('closes when the page scrolls under it or focus moves elsewhere', async () => {
    const { details } = await setup();
    await act(async () => { host.querySelector('p')!.dispatchEvent(new WheelEvent('wheel', { bubbles: true, deltaY: 120 })); });
    expect(details.open).toBe(false);
    await act(async () => { details.open = true; });
    // The list scrolls itself without closing.
    await act(async () => { details.querySelector('ul')!.dispatchEvent(new WheelEvent('wheel', { bubbles: true, deltaY: 120 })); });
    expect(details.open).toBe(true);
    await act(async () => { host.querySelector('input')!.focus(); });
    expect(details.open).toBe(false);
});

it('closes on Escape, takes the key from the page and returns focus to its summary', async () => {
    const { details } = await setup();
    const entry = details.querySelector<HTMLButtonElement>('li button')!;
    entry.focus();
    const event = new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true });
    const page = vi.fn();
    window.addEventListener('keydown', page);
    await act(async () => { entry.dispatchEvent(event); });
    window.removeEventListener('keydown', page);
    expect(details.open).toBe(false);
    expect(event.defaultPrevented).toBe(true);
    expect(page).not.toHaveBeenCalled();
    expect(document.activeElement).toBe(details.querySelector('summary'));
    // Closed, Escape belongs to the page again.
    const later = new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true });
    await act(async () => { document.body.dispatchEvent(later); });
    expect(later.defaultPrevented).toBe(false);
});

it('closes when an entry is chosen and whenever the place changes', async () => {
    const { pick, details } = await setup();
    await act(async () => details.querySelector<HTMLButtonElement>('li button')!.click());
    expect(pick).toHaveBeenCalledOnce();
    expect(details.open).toBe(false);
    await act(async () => { details.open = true; });
    await act(async () => root.render(<Fixture place="a" onPick={pick} />));
    expect(details.open).toBe(true);
    // Back, Forward, Alt+Left or any other step leads to another place.
    await act(async () => root.render(<Fixture place="b" onPick={pick} />));
    expect(details.open).toBe(false);
});
