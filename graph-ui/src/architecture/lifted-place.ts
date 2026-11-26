import { useCallback, useEffect, useRef, useState } from 'react';
import type { PlaceChange } from './architecture-history';

/**
 * A view's part of the Architecture place (K27). The workspace passes it in so
 * that Back and Forward can restore it; a view rendered on its own keeps it as
 * its own state. Either way the view reads one value and reports changes one way.
 */
export function useLiftedPlace<T extends object>(lifted: T | undefined, onChange: PlaceChange<T> | undefined, initial: () => T): [T, PlaceChange<T>] {
    const [own, setOwn] = useState(initial);
    const change = useCallback<PlaceChange<T>>((partial, automatic) => {
        if (onChange) onChange(partial, automatic);
        else setOwn(current => ({ ...current, ...partial }));
    }, [onChange]);
    return [lifted ?? own, change];
}

/**
 * Runs `reset` when the identity changes after the first render, never on mount.
 * A view that mounts again (switching subtabs, or Back into it) must keep the
 * place it is handed instead of clearing it as if the index had changed.
 */
export function useOnIdentityChange(identity: string, reset: () => void): void {
    const seen = useRef(identity);
    const latest = useRef(reset);
    latest.current = reset;
    useEffect(() => {
        if (seen.current === identity) return;
        seen.current = identity;
        latest.current();
    }, [identity]);
}
