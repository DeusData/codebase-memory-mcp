/**
 * Isolate everything a fullscreen galaxy covers without making assumptions
 * about the shell's exact number of layout wrappers.
 *
 * Walking from the galaxy to the shell and inerting only each branch's siblings
 * keeps the graph's own ancestor path alive. Attribute values are retained, not
 * merely boolean state, so an independently inert background stays exactly as
 * it was when fullscreen ends.
 */
export function isolateFullscreenBackground(galaxy: HTMLElement): () => void {
    const previous = new Map<HTMLElement, string | null>();
    let branch: HTMLElement | null = galaxy;

    while (branch !== null) {
        const parent: HTMLElement | null = branch.parentElement;
        if (parent === null) {
            break;
        }
        for (const sibling of parent.children) {
            if (!(sibling instanceof HTMLElement) || sibling === branch) {
                continue;
            }
            if (!previous.has(sibling)) {
                previous.set(sibling, sibling.getAttribute('inert'));
                sibling.setAttribute('inert', '');
            }
        }
        if (parent.classList.contains('atlas-shell')) {
            break;
        }
        branch = parent;
    }

    return () => {
        for (const [node, value] of previous) {
            if (value === null) {
                node.removeAttribute('inert');
            } else {
                node.setAttribute('inert', value);
            }
        }
    };
}

/** A higher overlay owns keyboard interaction instead of the fullscreen graph. */
export function fullscreenIsolationRequired(fullscreen: boolean, escapeTaken: boolean): boolean {
    return fullscreen && !escapeTaken;
}
