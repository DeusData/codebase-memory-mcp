import { useRef, type PointerEvent as ReactPointerEvent } from 'react';

/** A missed raycast counts as an empty-canvas click only after a primary,
 * stationary canvas gesture. Tracking the entire gesture also excludes drags
 * that return to their start. What the click does is the caller's decision:
 * inside a Galaxy scope it only clears highlights and never leaves the scope
 * (hand test K9, GalaxyPanel `handleBackgroundClick`). */
export function useGraphBackgroundReset(onReset?: () => void) {
    const gesture = useRef<{ id: number; x: number; y: number; moved: boolean } | undefined>(undefined);
    const onPointerDownCapture = (event: ReactPointerEvent) => {
        if (event.button !== 0 || event.isPrimary === false || !(event.target instanceof HTMLCanvasElement)) {
            gesture.current = undefined;
            return;
        }
        gesture.current = { id: event.pointerId, x: event.clientX, y: event.clientY, moved: false };
    };
    const onPointerMoveCapture = (event: ReactPointerEvent) => {
        const start = gesture.current;
        if (start && (event.pointerId !== start.id || Math.hypot(event.clientX - start.x, event.clientY - start.y) > 4)) start.moved = true;
    };
    const onPointerCancelCapture = () => { gesture.current = undefined; };
    const onPointerMissed = (event: MouseEvent) => {
        const start = gesture.current;
        gesture.current = undefined;
        if (event.type !== 'click' || event.button !== 0 || !(event.target instanceof HTMLCanvasElement)
            || !start || start.moved || Math.hypot(event.clientX - start.x, event.clientY - start.y) > 4) return;
        onReset?.();
    };
    return { onPointerDownCapture, onPointerMoveCapture, onPointerCancelCapture, onPointerMissed };
}
