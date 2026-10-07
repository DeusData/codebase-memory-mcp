import { useCallback, useEffect, useRef } from 'react';

/**
 * A `<details>` menu that closes once the reader is done with it, as the
 * Recent menus of Galaxy and Architecture do (hand test 2026-10-04, A2: the
 * menu stayed open over the content through several steps and while
 * scrolling).
 *
 * It closes on a pointer press, a wheel turn or a focus outside it, on Escape
 * (which then belongs to the menu alone: the page neither leaves its scope nor
 * clears its selection), when the caller closes it after an entry was chosen,
 * and whenever `place` changes: Back, Forward, Alt+Left and every other step
 * lead somewhere else, and the list would name the wrong current place.
 *
 * The listeners stay on the document for the life of the menu and do nothing
 * while it is closed, so opening it needs no React state and no render.
 */
export function useDismissibleMenu(place: string) {
    const ref = useRef<HTMLDetailsElement>(null);
    const close = useCallback(() => {
        const menu = ref.current;
        if (menu?.open) menu.open = false;
    }, []);
    useEffect(() => {
        const outside = (event: Event) => {
            const menu = ref.current;
            if (menu?.open && !(event.target instanceof Node && menu.contains(event.target))) menu.open = false;
        };
        const escape = (event: KeyboardEvent) => {
            const menu = ref.current;
            if (event.key !== 'Escape' || !menu?.open) return;
            event.preventDefault();
            event.stopPropagation();
            const inside = document.activeElement instanceof Node && menu.contains(document.activeElement);
            menu.open = false;
            // An entry that had focus is hidden now; the summary keeps the reader in place.
            if (inside) menu.querySelector('summary')?.focus();
        };
        document.addEventListener('pointerdown', outside, true);
        document.addEventListener('wheel', outside, { capture: true, passive: true });
        document.addEventListener('focusin', outside, true);
        document.addEventListener('keydown', escape, true);
        return () => {
            document.removeEventListener('pointerdown', outside, true);
            document.removeEventListener('wheel', outside, { capture: true });
            document.removeEventListener('focusin', outside, true);
            document.removeEventListener('keydown', escape, true);
        };
    }, []);
    const shown = useRef(place);
    useEffect(() => {
        if (shown.current === place) return;
        shown.current = place;
        close();
    }, [place, close]);
    return { ref, close };
}
